"""
PlainID PBAC adapter — Policy Enforcement Point shim.

The HSM service is the PEP; PlainID is the PDP.  This module's only
contract with the rest of the codebase is a single coroutine:

    permitted: bool = await client.check(end_user_id, action, resource_context)

Everything about the PlainID wire format — endpoint path, request field
names, response field path, auth header shape, resource string templates —
is read from an external JSON config file at startup.  Operators can adapt
to any PlainID API version or tenant without a code change or image rebuild.

Config file format (default: config/pbac_integration.json)
----------------------------------------------------------
{
  "endpoint_path": "/v2/isPermitted",
  "auth": {
    "header_name": "Authorization",
    "header_value_template": "Bearer {api_key}"
  },
  "request": {
    "principal_field": "principal",
    "action_field":    "action",
    "resource_field":  "resource",
    "context_field":   "context"
  },
  "response": {
    "permitted_path": "permitted"       ← dot-notation: "result.allowed", "data.decision"
  },
  "resource_templates": {
    "encrypt": "hsm:encrypt:{data_classification}",
    "decrypt": "hsm:decrypt:{data_classification}"
  }
}

permitted_path dot-notation examples
-------------------------------------
  "permitted"              → body["permitted"]
  "result.allowed"         → body["result"]["allowed"]
  "data.decision.permit"   → body["data"]["decision"]["permit"]

Decision cache
--------------
Decisions are cached in-process per (end_user_id, action, resource) for
pbac_cache_ttl_seconds.  SHA-256 key; plain dict; no lock (asyncio
single-threaded event loop).

Fail behaviour
--------------
pbac_fail_open=False (default) → deny on PlainID error (safer).
pbac_fail_open=True            → allow on error (use only when availability
                                 must beat strict enforcement during outages).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

_log = logging.getLogger(__name__)

# ── Integration config schema defaults ───────────────────────────────────────
# Used when a key is absent from the JSON file so adding new fields is
# backwards-compatible — existing config files don't need to list every key.

_DEFAULTS: dict[str, Any] = {
    "endpoint_path": "/v2/isPermitted",
    "auth": {
        "header_name": "Authorization",
        "header_value_template": "Bearer {api_key}",
    },
    "request": {
        "principal_field": "principal",
        "action_field": "action",
        "resource_field": "resource",
        "context_field": "context",
    },
    "response": {
        "permitted_path": "permitted",
    },
    "resource_templates": {
        "encrypt": "hsm:encrypt:{data_classification}",
        "decrypt": "hsm:decrypt:{data_classification}",
    },
}


def load_integration_config(config_path: str | None) -> dict[str, Any]:
    """
    Load the PBAC integration config from a JSON file and merge with defaults.

    If config_path is empty or the file does not exist, returns the defaults.
    This means the service starts fine even when no file is mounted — operators
    only need the file when they want to override the defaults.
    """
    cfg = json.loads(json.dumps(_DEFAULTS))  # deep copy

    if not config_path:
        return cfg

    path = Path(config_path)
    if not path.exists():
        _log.warning("pbac_integration_config_not_found", path=str(path))
        return cfg

    try:
        with path.open() as f:
            overrides = json.load(f)
        _deep_merge(cfg, overrides)
        _log.info("pbac_integration_config_loaded", path=str(path))
    except Exception as exc:
        _log.error("pbac_integration_config_load_failed", path=str(path), error=str(exc))

    return cfg


def _deep_merge(base: dict, override: dict) -> None:
    """Recursively merge override into base in-place (override wins on conflict)."""
    for key, value in override.items():
        if key.startswith("_"):
            continue  # skip comment keys
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


# ── Response navigation ───────────────────────────────────────────────────────

def _get_nested(body: dict[str, Any], dot_path: str) -> Any:
    """
    Navigate a nested dict via a dot-notation path.

    Examples:
        "permitted"            → body["permitted"]
        "result.allowed"       → body["result"]["allowed"]
        "data.decision.permit" → body["data"]["decision"]["permit"]

    Returns None if any segment is missing or not a dict.
    """
    current: Any = body
    for segment in dot_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    return current


# ── Main client ───────────────────────────────────────────────────────────────

class PBACClient:
    def __init__(
        self,
        plainid_url: str,
        api_key: str,
        integration_config: dict[str, Any],
        cache_ttl_seconds: int = 30,
        fail_open: bool = False,
        http_timeout: float = 3.0,
    ) -> None:
        self._base_url = plainid_url.rstrip("/")
        self._api_key = api_key
        self._cfg = integration_config
        self._cache_ttl = cache_ttl_seconds
        self._fail_open = fail_open
        self._timeout = http_timeout
        # {cache_key: (permitted, expires_monotonic)}
        self._cache: dict[str, tuple[bool, float]] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def check(
        self,
        end_user_id: str,
        action: str,
        data_classification: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        """
        Return True if end_user_id is permitted to perform action.

        Parameters
        ----------
        end_user_id        : the logged-in user identity from the request.
        action             : "encrypt" or "decrypt".
        data_classification: used to build the resource string from the
                             configured resource_templates.
        context            : extra attributes forwarded to PlainID
                             (app_id, caller_ip, owner_app_id, etc.).
        """
        resource = self._build_resource(action, data_classification)
        cache_key = self._make_cache_key(end_user_id, action, resource)

        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            permitted = await self._call_plainid(end_user_id, action, resource, context or {})
        except Exception as exc:
            _log.warning(
                "pbac_check_failed",
                extra={
                    "end_user_id": end_user_id,
                    "action": action,
                    "resource": resource,
                    "error": str(exc),
                    "fail_open": self._fail_open,
                },
            )
            return self._fail_open

        self._set_cached(cache_key, permitted)
        return permitted

    # ── Resource string builder ───────────────────────────────────────────────

    def _build_resource(self, action: str, data_classification: str | None) -> str:
        templates: dict = self._cfg.get("resource_templates", {})
        template = templates.get(action, "hsm:{action}:{data_classification}")
        return template.format(
            action=action,
            data_classification=data_classification or "default",
        )

    # ── Cache helpers ─────────────────────────────────────────────────────────

    def _make_cache_key(self, end_user_id: str, action: str, resource: str) -> str:
        raw = f"{end_user_id}\x00{action}\x00{resource}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _get_cached(self, key: str) -> bool | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        permitted, expires = entry
        if time.monotonic() < expires:
            return permitted
        del self._cache[key]
        return None

    def _set_cached(self, key: str, permitted: bool) -> None:
        self._cache[key] = (permitted, time.monotonic() + self._cache_ttl)

    # ── HTTP call ─────────────────────────────────────────────────────────────

    async def _call_plainid(
        self,
        end_user_id: str,
        action: str,
        resource: str,
        context: dict[str, Any],
    ) -> bool:
        req_cfg: dict = self._cfg.get("request", {})
        auth_cfg: dict = self._cfg.get("auth", {})

        payload = {
            req_cfg.get("principal_field", "principal"): end_user_id,
            req_cfg.get("action_field", "action"): action,
            req_cfg.get("resource_field", "resource"): resource,
        }
        context_field = req_cfg.get("context_field", "context")
        if context_field:
            payload[context_field] = context

        header_name = auth_cfg.get("header_name", "Authorization")
        header_value = auth_cfg.get("header_value_template", "Bearer {api_key}").format(
            api_key=self._api_key
        )

        endpoint = self._base_url + self._cfg.get("endpoint_path", "/v2/isPermitted")

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                endpoint,
                headers={header_name: header_value, "Content-Type": "application/json"},
                json=payload,
            )
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()

        return self._extract_permitted(body)

    # ── Response normalization ────────────────────────────────────────────────

    def _extract_permitted(self, body: dict[str, Any]) -> bool:
        """
        Navigate to the boolean using the configured permitted_path (dot-notation).
        Returns False (deny) if the path is missing or the value is not truthy.
        """
        resp_cfg: dict = self._cfg.get("response", {})
        path = resp_cfg.get("permitted_path", "permitted")
        value = _get_nested(body, path)

        if value is None:
            _log.warning(
                "pbac_permitted_path_not_found",
                extra={"path": path, "body_keys": list(body.keys())},
            )
            return False  # deny-unknown

        # Normalize: bool True/False, string "true"/"ALLOW"/"PERMITTED", int 1
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.upper() in ("TRUE", "ALLOW", "ALLOWED", "PERMIT", "PERMITTED", "YES", "1")
        return bool(value)


# ── No-op client ─────────────────────────────────────────────────────────────

class NullPBACClient:
    """
    No-op — always permits.  Used in demo mode and when pbac_enabled=False.
    Identical call signature to PBACClient so the service layer needs no
    conditionals.
    """

    async def check(
        self,
        end_user_id: str,
        action: str,
        data_classification: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        return True
