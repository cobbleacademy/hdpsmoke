"""
CEK Rotation Service — entry point.

Runs rotate_cek() immediately on startup, then repeats every
rotation_interval_hours.  A single SIGTERM (or SIGINT) sets a stop event
and lets the current sleep or rotation finish before the process exits
cleanly — no forceful kill needed.
"""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

from cek_rotation.config import Settings
from cek_rotation.rotator import rotate_cek

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger("cek_rotation.main")


async def run() -> None:
    config = Settings()
    stop_event = asyncio.Event()

    def _handle_signal(sig: int, _frame) -> None:  # noqa: ANN001
        log.info("shutdown_signal_received", signal=signal.Signals(sig).name)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    interval_seconds = config.rotation_interval_hours * 3600

    credential = DefaultAzureCredential()
    secret_client = SecretClient(
        vault_url=config.azure_keyvault_secret_url,
        credential=credential,
    )

    redis_client = None
    if config.redis_url and config.redis_post_rotation_mode != "none":
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(
            config.redis_url,
            decode_responses=False,
            socket_connect_timeout=2,
            socket_timeout=2,
        )

    log.info(
        "cek_rotation_service_started",
        vault_url=config.azure_keyvault_secret_url,
        interval_hours=config.rotation_interval_hours,
        redis_mode=config.redis_post_rotation_mode if config.redis_url else "disabled",
    )

    try:
        while not stop_event.is_set():
            try:
                result = await rotate_cek(secret_client, config, redis_client)
                log.info("rotation_cycle_done", **result)
            except Exception as exc:
                # Log and wait for the next interval — do not crash the service.
                log.error(
                    "rotation_cycle_failed",
                    error=str(exc),
                    exc_info=True,
                )

            # Sleep in small increments so SIGTERM wakes us promptly.
            deadline = asyncio.get_event_loop().time() + interval_seconds
            while not stop_event.is_set():
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, 5.0))

    finally:
        log.info("cek_rotation_service_stopping")
        if redis_client is not None:
            await redis_client.aclose()
        await secret_client.close()
        await credential.close()
        log.info("cek_rotation_service_stopped")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
