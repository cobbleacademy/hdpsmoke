"""
Structured audit logger with optional Splunk HEC output.

All encryption/decryption/rotation events flow through `audit_log()`.
Splunk delivery is async-batched; failures fall back to stdout so no
event is silently dropped.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
from collections import deque
from contextlib import suppress
from typing import Any

import httpx
import structlog
from structlog.types import EventDict, WrappedLogger

from app.config import Settings


# ── stdlib root logger → structlog ───────────────────────────────────────────

def _add_hostname(logger: WrappedLogger, method: str, event_dict: EventDict) -> EventDict:
    event_dict["host"] = socket.gethostname()
    return event_dict


def _add_timestamp(logger: WrappedLogger, method: str, event_dict: EventDict) -> EventDict:
    event_dict["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return event_dict


def configure_structlog(log_level: str) -> None:
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            _add_hostname,
            _add_timestamp,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper(), logging.INFO),
    )


# ── Splunk HEC async batcher ──────────────────────────────────────────────────

class SplunkHECBatcher:
    """
    Collects audit events and ships them to Splunk HEC in batches.
    Falls back to stdout on any delivery failure — no silent drops.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queue: deque[dict[str, Any]] = deque()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._client = httpx.AsyncClient(
            verify=settings.splunk_verify_ssl,
            headers={
                "Authorization": f"Splunk {settings.splunk_hec_token}",
                "Content-Type": "application/json",
            },
            timeout=10.0,
        )
        self._logger = structlog.get_logger("splunk_batcher")

    def enqueue(self, event: dict[str, Any]) -> None:
        self._queue.append(event)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        await self._flush()
        await self._client.aclose()

    async def _flush_loop(self) -> None:
        interval = self._settings.splunk_flush_interval_seconds
        while True:
            await asyncio.sleep(interval)
            await self._flush()

    async def _flush(self) -> None:
        if not self._queue:
            return
        async with self._lock:
            batch_size = self._settings.splunk_batch_size
            batch, remaining = [], list(self._queue)
            to_send, leftover = remaining[:batch_size], remaining[batch_size:]
            self._queue.clear()
            self._queue.extend(leftover)
            batch = to_send

        if not batch:
            return

        payload = "\n".join(
            json.dumps({
                "time": event.pop("_epoch", time.time()),
                "host": event.get("host", socket.gethostname()),
                "source": self._settings.splunk_source,
                "sourcetype": self._settings.splunk_sourcetype,
                "index": self._settings.splunk_index,
                "event": event,
            })
            for event in batch
        )

        try:
            resp = await self._client.post(self._settings.splunk_hec_url, content=payload)
            resp.raise_for_status()
        except Exception as exc:
            self._logger.error("splunk_hec_delivery_failed", error=str(exc), batch_size=len(batch))
            # Re-queue so events aren't lost; stdout already has them
            async with self._lock:
                self._queue.extendleft(reversed(batch))


# ── Module-level singleton (initialised in main.py lifespan) ─────────────────

_splunk_batcher: SplunkHECBatcher | None = None
_log = structlog.get_logger("audit")
_recent_events: deque[dict[str, Any]] = deque(maxlen=200)


def init_audit(settings: Settings) -> None:
    """Call once at startup."""
    global _splunk_batcher
    configure_structlog(settings.log_level)
    if settings.splunk_enabled:
        _splunk_batcher = SplunkHECBatcher(settings)


async def start_splunk_batcher() -> None:
    if _splunk_batcher:
        await _splunk_batcher.start()


async def stop_splunk_batcher() -> None:
    if _splunk_batcher:
        await _splunk_batcher.stop()


def audit_log(event_type: str, **kwargs: Any) -> None:
    """
    Emit one audit event to stdout (always) and Splunk HEC (if enabled).
    Never include plaintext or DEK material in kwargs.
    """
    record: dict[str, Any] = {
        "event_type": event_type,
        "_epoch": time.time(),
        **kwargs,
    }
    _log.info(event_type, **{k: v for k, v in record.items() if k != "_epoch"})
    _recent_events.append(record.copy())

    if _splunk_batcher is not None:
        _splunk_batcher.enqueue(record.copy())


def get_recent_events(limit: int = 50) -> list[dict[str, Any]]:
    """Most recent audit events, newest first. Used by the demo UI."""
    return list(_recent_events)[-limit:][::-1]
