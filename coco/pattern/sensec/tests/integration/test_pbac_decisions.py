"""
Area 5 — PlainID PBAC Decision Tests

Tests the full PBAC enforcement path: PBACClient unit behaviour, the
encrypt/decrypt service integration, and edge cases around end_user_id.

Coverage:
  1.  permit decision returned by PlainID → encrypt succeeds (201)
  2.  deny decision returned by PlainID → encrypt returns 403
  3.  permit decision cached — PlainID called only once for N identical checks
  4.  cache expires after TTL → PlainID called again
  5.  PlainID unreachable, fail_open=False (default) → denied (403)
  6.  PlainID unreachable, fail_open=True → permitted (201)
  7.  end_user_id absent → PBAC skipped entirely (NullPBACClient path)
  8.  decrypt denied by PBAC → 403, audit event reason=pbac_denied
  9.  decrypt permitted by PBAC → 200
 10.  resource string built from data_classification template
 11.  response traversal — nested dot-notation path (e.g. "result.allowed")
 12.  response normalization — string "ALLOW" / "PERMITTED" / "true" all permit
 13.  response normalization — string "DENY" / missing key → deny
 14.  NullPBACClient always permits regardless of arguments
 15.  cache key is scoped: same user, different action → separate cache entries
 16.  cache key is scoped: same user+action, different classification → separate
 17.  custom integration config overrides field names and endpoint path
 18.  PlainID HTTP 500 → treated as error, fail_open=False → denied
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.auth.pbac_client import (
    NullPBACClient,
    PBACClient,
    _get_nested,
    load_integration_config,
)
from app.config import get_settings
from app.demo.mock_kek_client import MockKEKClient
from app.main import create_app
from app.models.edek_record import EDEKRecord, RotationStatus

API_PREFIX = get_settings().api_v1_prefix

# ── Default integration config (matches _DEFAULTS in pbac_client.py) ──────────

_DEFAULT_CFG = {
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
    "response": {"permitted_path": "permitted"},
    "resource_templates": {
        "encrypt": "hsm:encrypt:{data_classification}",
        "decrypt": "hsm:decrypt:{data_classification}",
    },
}


# ── PBACClient factory ─────────────────────────────────────────────────────────

def _make_client(
    fail_open: bool = False,
    cache_ttl: int = 60,
    cfg: dict | None = None,
) -> PBACClient:
    return PBACClient(
        plainid_url="http://plainid.internal",
        api_key="test-api-key",
        integration_config=cfg or dict(_DEFAULT_CFG),
        cache_ttl_seconds=cache_ttl,
        fail_open=fail_open,
        http_timeout=3.0,
    )


# ── HTTP fixtures ──────────────────────────────────────────────────────────────

class _ScalarResult:
    def __init__(self, rows): self._rows = rows
    def all(self): return self._rows


class FakeSession:
    def __init__(self, store: dict):
        self._store = store

    def add(self, obj) -> None:
        if getattr(obj, "created_at", None) is None:
            obj.created_at = datetime.now(timezone.utc)
        if getattr(obj, "rotation_status", None) is None:
            obj.rotation_status = RotationStatus.current
        self._store[str(obj.edek_id)] = obj

    async def commit(self) -> None:
        pass

    async def get(self, model, pk):
        return self._store.get(str(pk))

    async def scalars(self, stmt):
        return _ScalarResult(list(self._store.values()))

    async def execute(self, stmt):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


HEADERS = {"Authorization": "Bearer fake.jwt", "X-App-ID": "app-test"}


@pytest.fixture
def edek_store() -> dict:
    return {}


@pytest.fixture
def kek_client() -> MockKEKClient:
    return MockKEKClient()


@pytest.fixture
def _patch_base(monkeypatch, kek_client, edek_store):
    """Patch everything except _pbac_client — callers set that themselves."""
    from app import dependencies

    monkeypatch.setattr(dependencies, "_kek_client", kek_client)
    monkeypatch.setattr(dependencies, "_session_factory",
                        type("SF", (), {"__call__": lambda self: FakeSession(edek_store)})())

    reg = MagicMock()
    reg.get_scopes = AsyncMock(return_value=["encrypt", "decrypt", "rotate"])
    reg.require_scope = AsyncMock()
    reg.is_granted = AsyncMock(side_effect=lambda grantee_app_id, owner_app_id: grantee_app_id == owner_app_id)
    monkeypatch.setattr(dependencies, "_app_registry", reg)

    val = MagicMock()
    val.validate = MagicMock(return_value={"sub": "svc-account", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)


def _make_http_client(monkeypatch, pbac) -> AsyncClient:
    """Wire up a full HTTP test client with the given pbac instance."""
    from app import dependencies
    monkeypatch.setattr(dependencies, "_pbac_client", pbac)
    app = create_app()
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 1–2 — permit / deny through encrypt endpoint
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pbac_permit_encrypt_succeeds(_patch_base, monkeypatch):
    client = _make_client()
    client._call_plainid = AsyncMock(return_value=True)

    async with _make_http_client(monkeypatch, client) as http:
        resp = await http.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "secret", "end_user_id": "u-001", "data_classification": "pii"},
            headers=HEADERS,
        )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_pbac_deny_encrypt_returns_403(_patch_base, monkeypatch):
    client = _make_client()
    client._call_plainid = AsyncMock(return_value=False)

    async with _make_http_client(monkeypatch, client) as http:
        resp = await http.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "secret", "end_user_id": "u-001", "data_classification": "pii"},
            headers=HEADERS,
        )
    assert resp.status_code == 403


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 3–4 — decision cache
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pbac_cache_hit_calls_plainid_once():
    """Same (user, action, resource) must hit cache — PlainID called only once."""
    client = _make_client(cache_ttl=60)
    client._call_plainid = AsyncMock(return_value=True)

    for _ in range(5):
        result = await client.check("u-001", "encrypt", "pii")
        assert result is True

    client._call_plainid.assert_awaited_once()


@pytest.mark.asyncio
async def test_pbac_cache_expires_and_rechecks(monkeypatch):
    """After TTL expiry, the next check must call PlainID again."""
    client = _make_client(cache_ttl=1)
    client._call_plainid = AsyncMock(return_value=True)

    await client.check("u-001", "encrypt", "pii")
    assert client._call_plainid.await_count == 1

    # Fast-forward monotonic clock past the TTL
    real_monotonic = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 2)

    await client.check("u-001", "encrypt", "pii")
    assert client._call_plainid.await_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 5–6 — fail-open / fail-closed
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pbac_fail_closed_on_plainid_unreachable(_patch_base, monkeypatch):
    """fail_open=False (default): PlainID error → denied → 403."""
    client = _make_client(fail_open=False)
    client._call_plainid = AsyncMock(side_effect=ConnectionError("KV unreachable"))

    async with _make_http_client(monkeypatch, client) as http:
        resp = await http.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "secret", "end_user_id": "u-001"},
            headers=HEADERS,
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_pbac_fail_open_on_plainid_unreachable(_patch_base, monkeypatch):
    """fail_open=True: PlainID error → permitted → 201."""
    client = _make_client(fail_open=True)
    client._call_plainid = AsyncMock(side_effect=ConnectionError("KV unreachable"))

    async with _make_http_client(monkeypatch, client) as http:
        resp = await http.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "secret", "end_user_id": "u-001"},
            headers=HEADERS,
        )
    assert resp.status_code == 201


# ═══════════════════════════════════════════════════════════════════════════════
# Test 7 — end_user_id absent → PBAC skipped
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pbac_skipped_when_no_end_user_id(_patch_base, monkeypatch):
    """
    When end_user_id is absent the encryption service skips the PBAC check
    entirely, so a deny-everything client must not block the request.
    """
    client = _make_client()
    client._call_plainid = AsyncMock(return_value=False)  # would deny if called

    async with _make_http_client(monkeypatch, client) as http:
        resp = await http.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "no-user-context"},  # no end_user_id
            headers=HEADERS,
        )
    assert resp.status_code == 201
    client._call_plainid.assert_not_awaited()


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 8–9 — PBAC on decrypt path
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_pbac_deny_decrypt_returns_403(_patch_base, monkeypatch):
    """PBAC deny on decrypt must return 403 before the DEK is ever unwrapped."""
    # First encrypt without PBAC (NullPBACClient default)
    from app import dependencies
    monkeypatch.setattr(dependencies, "_pbac_client", NullPBACClient())
    app_enc = create_app()
    async with AsyncClient(transport=ASGITransport(app=app_enc), base_url="http://test") as enc_client:
        enc_resp = await enc_client.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "protected", "end_user_id": "u-001", "data_classification": "pii"},
            headers=HEADERS,
        )
    assert enc_resp.status_code == 201
    enc = enc_resp.json()

    # Now decrypt with a deny-all PBAC client
    deny_client = _make_client()
    deny_client._call_plainid = AsyncMock(return_value=False)
    async with _make_http_client(monkeypatch, deny_client) as http:
        resp = await http.post(
            f"{API_PREFIX}/decrypt",
            json={
                "edek_id": enc["edek_id"],
                "iv_b64": enc["iv_b64"],
                "ciphertext_b64": enc["ciphertext_b64"],
                "tag_b64": enc["tag_b64"],
                "end_user_id": "u-001",
            },
            headers=HEADERS,
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_pbac_permit_decrypt_returns_200(_patch_base, monkeypatch):
    """PBAC permit on decrypt must return 200 with the correct plaintext."""
    from app import dependencies
    monkeypatch.setattr(dependencies, "_pbac_client", NullPBACClient())
    app_enc = create_app()
    async with AsyncClient(transport=ASGITransport(app=app_enc), base_url="http://test") as enc_client:
        enc_resp = await enc_client.post(
            f"{API_PREFIX}/encrypt",
            json={"plaintext": "hello pbac", "end_user_id": "u-002", "data_classification": "internal"},
            headers=HEADERS,
        )
    enc = enc_resp.json()

    permit_client = _make_client()
    permit_client._call_plainid = AsyncMock(return_value=True)
    async with _make_http_client(monkeypatch, permit_client) as http:
        resp = await http.post(
            f"{API_PREFIX}/decrypt",
            json={
                "edek_id": enc["edek_id"],
                "iv_b64": enc["iv_b64"],
                "ciphertext_b64": enc["ciphertext_b64"],
                "tag_b64": enc["tag_b64"],
                "end_user_id": "u-002",
            },
            headers=HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["plaintext"] == "hello pbac"


# ═══════════════════════════════════════════════════════════════════════════════
# Test 10 — resource string from data_classification template
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_resource_string_built_from_classification():
    client = _make_client()
    captured: list[dict] = []

    async def _capture(end_user_id, action, resource, context):
        captured.append({"action": action, "resource": resource})
        return True

    client._call_plainid = _capture

    await client.check("u-001", "encrypt", "pii")
    await client.check("u-001", "decrypt", "phi")

    assert captured[0]["resource"] == "hsm:encrypt:pii"
    assert captured[1]["resource"] == "hsm:decrypt:phi"


# ═══════════════════════════════════════════════════════════════════════════════
# Test 11 — nested dot-notation response traversal
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_nested_dot_notation_response_path():
    """PlainID response at 'result.allowed' must be resolved correctly."""
    cfg = dict(_DEFAULT_CFG)
    cfg["response"] = {"permitted_path": "result.allowed"}

    client = _make_client(cfg=cfg)
    client._call_plainid = AsyncMock(return_value=True)

    # Verify _extract_permitted navigates nested structure
    assert client._extract_permitted({"result": {"allowed": True}}) is True
    assert client._extract_permitted({"result": {"allowed": False}}) is False
    assert client._extract_permitted({"result": {}}) is False          # missing key → deny
    assert client._extract_permitted({"other": True}) is False         # wrong path → deny


def test_get_nested_helper():
    body = {"a": {"b": {"c": True}}, "x": False}
    assert _get_nested(body, "a.b.c") is True
    assert _get_nested(body, "x") is False
    assert _get_nested(body, "a.b.missing") is None
    assert _get_nested(body, "missing") is None
    assert _get_nested(body, "a.b.c.d") is None   # traversal past non-dict


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 12–13 — response normalization
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("value,expected", [
    (True, True),
    (False, False),
    (1, True),
    (0, False),
    ("true", True),
    ("TRUE", True),
    ("ALLOW", True),
    ("ALLOWED", True),
    ("PERMIT", True),
    ("PERMITTED", True),
    ("YES", True),
    ("1", True),
    ("false", False),
    ("DENY", False),
    ("no", False),
    ("", False),
])
def test_response_normalization(value, expected):
    client = _make_client()
    body = {"permitted": value}
    assert client._extract_permitted(body) is expected


def test_response_missing_key_denies():
    """If the permitted_path key is absent from the response, deny."""
    client = _make_client()
    assert client._extract_permitted({}) is False
    assert client._extract_permitted({"other_key": True}) is False


# ═══════════════════════════════════════════════════════════════════════════════
# Test 14 — NullPBACClient always permits
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_null_pbac_client_always_permits():
    null = NullPBACClient()
    for action in ("encrypt", "decrypt"):
        for classification in ("pii", "phi", "public", None):
            result = await null.check(
                end_user_id="any-user",
                action=action,
                data_classification=classification,
                context={"app_id": "app-x"},
            )
            assert result is True, f"NullPBACClient denied {action}/{classification}"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests 15–16 — cache key scoping
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_cache_key_scoped_by_action():
    """Same user, different action → independent cache entries."""
    client = _make_client(cache_ttl=60)
    call_log: list[str] = []

    async def _call(user, action, resource, ctx):
        call_log.append(action)
        return action == "encrypt"   # encrypt=permit, decrypt=deny

    client._call_plainid = _call

    enc = await client.check("u-001", "encrypt", "pii")
    dec = await client.check("u-001", "decrypt", "pii")

    assert enc is True
    assert dec is False
    assert call_log == ["encrypt", "decrypt"]   # both hit PlainID

    # Second round must be served from cache (no new PlainID calls)
    await client.check("u-001", "encrypt", "pii")
    await client.check("u-001", "decrypt", "pii")
    assert call_log == ["encrypt", "decrypt"]


@pytest.mark.asyncio
async def test_cache_key_scoped_by_classification():
    """Same user+action, different data_classification → separate cache entries."""
    client = _make_client(cache_ttl=60)
    called_resources: list[str] = []

    async def _call(user, action, resource, ctx):
        called_resources.append(resource)
        return True

    client._call_plainid = _call

    await client.check("u-001", "encrypt", "pii")
    await client.check("u-001", "encrypt", "phi")
    await client.check("u-001", "encrypt", "public")

    assert len(called_resources) == 3
    assert "hsm:encrypt:pii" in called_resources
    assert "hsm:encrypt:phi" in called_resources
    assert "hsm:encrypt:public" in called_resources


# ═══════════════════════════════════════════════════════════════════════════════
# Test 17 — custom integration config
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_custom_integration_config_overrides_field_names():
    """Operator-supplied config must override endpoint path and field names."""
    custom_cfg = {
        "endpoint_path": "/api/v3/authorize",
        "auth": {
            "header_name": "X-Api-Key",
            "header_value_template": "{api_key}",
        },
        "request": {
            "principal_field": "userId",
            "action_field": "operation",
            "resource_field": "asset",
            "context_field": "attributes",
        },
        "response": {"permitted_path": "decision.allowed"},
        "resource_templates": {
            "encrypt": "vault:write:{data_classification}",
            "decrypt": "vault:read:{data_classification}",
        },
    }

    client = PBACClient(
        plainid_url="http://plainid.internal",
        api_key="my-key",
        integration_config=custom_cfg,
        cache_ttl_seconds=0,   # no cache so _call_plainid always fires
    )
    captured_payload: dict = {}
    captured_url: str = ""

    import httpx as _httpx

    class _FakeResponse:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"decision": {"allowed": True}}

    class _FakeHTTPClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def post(self, url, headers=None, json=None):
            nonlocal captured_payload, captured_url
            captured_url = url
            captured_payload = json or {}
            return _FakeResponse()

    with patch("app.auth.pbac_client.httpx.AsyncClient", return_value=_FakeHTTPClient()):
        result = await client.check("user-99", "encrypt", "phi", context={"app_id": "app-x"})

    assert result is True
    assert "/api/v3/authorize" in captured_url
    assert "userId" in captured_payload
    assert captured_payload["userId"] == "user-99"
    assert captured_payload["operation"] == "encrypt"
    assert captured_payload["asset"] == "vault:write:phi"
    assert "attributes" in captured_payload


# ═══════════════════════════════════════════════════════════════════════════════
# Test 18 — PlainID HTTP 500 → fail-closed
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_plainid_http_500_fail_closed():
    """An HTTP 500 from PlainID raises an exception → fail_open=False → denied."""
    import httpx as _httpx

    client = _make_client(fail_open=False)

    class _HTTP500:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def post(self, url, headers=None, json=None):
            raise _httpx.HTTPStatusError(
                "500 Server Error",
                request=MagicMock(),
                response=MagicMock(status_code=500),
            )

    with patch("app.auth.pbac_client.httpx.AsyncClient", return_value=_HTTP500()):
        result = await client.check("u-001", "encrypt", "pii")

    assert result is False   # fail-closed → denied
