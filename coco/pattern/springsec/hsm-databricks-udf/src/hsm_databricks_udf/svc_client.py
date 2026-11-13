"""
HTTP client for hsm-core-service's POST /dek/issue and POST /dek/unwrap --
the same two endpoints hsm-bulk-client uses (java/docs/BULK_OPERATIONS.md
Tier 3). No new server-side surface; this is a Python client for an
already-shipped, already-tested API.

Field names follow Config.api_version: snake_case on V1, camelCase on V2
(hsm-core-service rejects unknown fields on V2 with 400, so a mix-up fails
loudly). This module builds and reads snake_case dicts either way; only the
wire format changes, in _post.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass

import certifi
import requests
from requests.adapters import HTTPAdapter

from . import transport
from .auth import SelfSignedJwtTokenProvider, StaticTokenProvider, TokenProvider
from .config import Config, ConfigError


class SvcClientError(RuntimeError):
    """Raised for a non-2xx response, with the server's own detail message when available."""


@dataclass(slots=True)
class IssueResult:
    edek_id: str
    wrapped_dek_b64: str
    owner_app_id: str   # the record's true, permanent owner -- NOT always this caller; see cache.py
    reused: bool


@dataclass(slots=True)
class UnwrapResult:
    edek_id: str
    wrapped_dek_b64: str
    owner_app_id: str   # the record's true, permanent owner -- required as the AES-GCM AAD; see cache.py


class CaBundleAdapter(HTTPAdapter):
    """
    HTTPS adapter that trusts the public CAs requests normally uses (certifi)
    plus an extra CA bundle -- for an hsm-core-service endpoint whose TLS
    certificate is issued by an internal/corporate CA. The bundle is loaded from
    memory (no file written; Unity Catalog's sandbox gives no reliable place to
    put one). Verification and hostname checking stay on: there is deliberately
    no way to disable them here.
    """

    def __init__(self, ca_bundle_pem: str, **kwargs) -> None:
        context = ssl.create_default_context(cafile=certifi.where())
        context.load_verify_locations(cadata=ca_bundle_pem)
        self._ssl_context = context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().proxy_manager_for(*args, **kwargs)


class SvcClient:
    def __init__(self, config: Config, session: requests.Session | None = None,
                 token_provider: TokenProvider | None = None) -> None:
        self._config = config
        self._session = session or requests.Session()
        if config.ca_cert_pem:
            self._session.mount("https://", CaBundleAdapter(config.ca_cert_pem))
        self._token_provider = token_provider or _build_token_provider(config)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token_provider.get_bearer_token()}",
            "X-App-ID": self._config.app_id,
            "Content-Type": "application/json",
        }

    def issue_dek(self, dek_name: str, data_classification: str | None = None) -> IssueResult:
        """
        POST /dek/issue with a single item. Batched multi-item calls aren't
        exposed here -- one DEK is fetched per distinct dek_name, then cached
        for the worker process's lifetime (see cache.py), so batching this
        specific call adds complexity without a real throughput win.
        """
        body = {
            "items": [
                {"key": "1", "name": dek_name, "data_classification": data_classification}
            ]
        }
        data = self._post("/dek/issue", body)
        item = _first_result_or_raise(data, "items")
        return IssueResult(
            edek_id=item["edek_id"],
            wrapped_dek_b64=item["wrapped_dek_b64"],
            owner_app_id=item["owner_app_id"],
            reused=item.get("reused", False),
        )

    def unwrap_dek(self, edek_id: str) -> UnwrapResult:
        """POST /dek/unwrap with a single item, for decrypting a row whose edek_id is already known."""
        body = {"items": [{"key": "1", "edek_id": edek_id}]}
        data = self._post("/dek/unwrap", body)
        item = _first_result_or_raise(data, "items")
        return UnwrapResult(edek_id=item["edek_id"], wrapped_dek_b64=item["wrapped_dek_b64"], owner_app_id=item["owner_app_id"])

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self._config.base_url}{path}"
        v2 = self._config.api_version == "V2"
        resp = self._session.post(url, json=_rename_keys(body, _camel) if v2 else body, headers=self._headers(),
                                   timeout=self._config.request_timeout_seconds)
        if not resp.ok:
            detail = _extract_detail(resp)
            raise SvcClientError(f"{path} -> {resp.status_code}: {detail}")
        data = resp.json()
        return _rename_keys(data, _snake) if v2 else data


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


def _snake(name: str) -> str:
    return "".join("_" + c.lower() if c.isupper() else c for c in name)


def _rename_keys(node, rename):
    """Every object key in a JSON tree renamed (values untouched): API v2's camelCase <-> this module's snake_case."""
    if isinstance(node, dict):
        return {rename(k): _rename_keys(v, rename) for k, v in node.items()}
    if isinstance(node, list):
        return [_rename_keys(v, rename) for v in node]
    return node


def _build_token_provider(config: Config) -> TokenProvider:
    if config.auth_mode == "STATIC":
        if not config.bearer_token:
            raise ConfigError("HSM_AUTH_MODE=STATIC requires HSM_BEARER_TOKEN")
        return StaticTokenProvider(config.bearer_token)
    elif config.auth_mode == "SELF_SIGNED_JWT":
        if not config.signing_private_key_pem:
            raise ConfigError("HSM_AUTH_MODE=SELF_SIGNED_JWT requires HSM_SIGNING_PRIVATE_KEY_PEM (or HSM_PRIVATE_KEY_PEM as a fallback)")
        signing_key = transport.parse_private_key_pem(config.signing_private_key_pem)
        return SelfSignedJwtTokenProvider(signing_key, config.app_id, config.self_signed_audience)
    else:
        raise ConfigError(f"Unknown auth_mode: {config.auth_mode}")


def _first_result_or_raise(data: dict, items_key: str) -> dict:
    items = data.get(items_key) or []
    if not items:
        raise SvcClientError(f"empty '{items_key}' in response: {data}")
    item = items[0]
    if item.get("status") != "success":
        raise SvcClientError(item.get("detail") or f"item failed with no detail: {item}")
    return item


def _extract_detail(resp: requests.Response) -> str:
    try:
        return resp.json().get("detail", resp.text)
    except ValueError:
        return resp.text
