"""
HTTP client for hsm-core-service's POST /dek/issue and POST /dek/unwrap --
the same two endpoints hsm-bulk-client uses (java/docs/BULK_OPERATIONS.md
Tier 3). No new server-side surface; this is a Python client for an
already-shipped, already-tested API.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests

from .config import Config


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


class SvcClient:
    def __init__(self, config: Config, session: requests.Session | None = None) -> None:
        self._config = config
        self._session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._config.bearer_token}",
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
        resp = self._session.post(url, json=body, headers=self._headers(),
                                   timeout=self._config.request_timeout_seconds)
        if not resp.ok:
            detail = _extract_detail(resp)
            raise SvcClientError(f"{path} -> {resp.status_code}: {detail}")
        return resp.json()


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
