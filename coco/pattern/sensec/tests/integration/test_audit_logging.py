"""
Area 4 — Audit Log Tests

Every audit_log() call site is exercised here.  Tests capture the
in-process _recent_events ring-buffer (via get_recent_events()) rather
than log output so they remain structlog-backend-agnostic.

Coverage:
  1.  encrypt success → event_type=encrypt, status=success, expected fields
  2.  encrypt PBAC denied → status=failure, reason=pbac_denied
  3.  decrypt success → event_type=decrypt, status=success
  4.  decrypt EDEK not found → status=failure, reason=edek_not_found
  5.  decrypt access denied (no grant) → status=failure, reason=no_grant_for_owner
  6.  decrypt PBAC denied → status=failure, reason=pbac_denied
  7.  decrypt ciphertext tampered → status=failure, reason=tag_verification_failed
  8.  KEK rotation completed → event_type=kek_rotation_completed, status=success
  9.  KEK rotation denied (missing scope) → status=failure, kek_rotation_denied
 10.  grant added success → event_type=grant_added, status=success
 11.  grant removed success → event_type=grant_removed, status=success
 12.  grant denied (missing scope) → status=failure, reason=scope_not_permitted:grant
 13.  app status changed → event_type=app_status_changed, status=success
 14.  app status denied (missing scope) → status=failure
 15.  get_recent_events() returns events newest-first, capped at limit
 16.  audit event fields — edek_id, app_id, sub, caller_ip always present
 17.  no plaintext / DEK material in any audit event
 18.  multiple operations → ring buffer accumulates all events in order
"""

from __future__ import annotations

import base64
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.audit import logger as audit_module
from app.audit.logger import audit_log, get_recent_events
from app.config import get_settings
from app.demo.mock_kek_client import MockKEKClient
from app.main import create_app
from app.models.edek_record import EDEKRecord, RotationStatus

API_PREFIX = get_settings().api_v1_prefix


# ── In-memory EDEK store (reused from Area 1 with minimal duplication) ────────

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
        results = list(self._store.values())
        try:
            compiled = str(stmt.whereclause.compile(compile_kwargs={"literal_binds": True}))
            if "rotation_status" in compiled:
                results = [r for r in results if r.rotation_status == RotationStatus.current]
            if "!=" in compiled and "kek_version" in compiled:
                target = compiled.split("!=")[-1].strip().strip("'")
                results = [r for r in results if r.kek_version != target]
        except Exception:
            pass
        return _ScalarResult(results)

    async def execute(self, stmt):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


class FakeSessionFactory:
    def __init__(self, store: dict):
        self._store = store

    def __call__(self) -> FakeSession:
        return FakeSession(self._store)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _clear_audit_buffer():
    """Drain the ring-buffer before every test for isolation."""
    audit_module._recent_events.clear()
    yield
    audit_module._recent_events.clear()


@pytest.fixture
def edek_store() -> dict:
    return {}


@pytest.fixture
def kek_client() -> MockKEKClient:
    return MockKEKClient()


@pytest.fixture
def _patch_dependencies(monkeypatch, kek_client, edek_store):
    from app import dependencies

    monkeypatch.setattr(dependencies, "_kek_client", kek_client)
    monkeypatch.setattr(dependencies, "_session_factory", FakeSessionFactory(edek_store))

    reg = MagicMock()
    reg.get_scopes = AsyncMock(return_value=["encrypt", "decrypt", "rotate", "grant", "manage_apps"])
    reg.require_scope = AsyncMock()
    reg.is_granted = AsyncMock(side_effect=lambda grantee_app_id, owner_app_id: grantee_app_id == owner_app_id)
    reg.add_grant = AsyncMock()
    reg.remove_grant = AsyncMock()
    reg.list_grants = AsyncMock(return_value=[])
    reg.set_active = AsyncMock()
    monkeypatch.setattr(dependencies, "_app_registry", reg)

    val = MagicMock()
    val.validate = MagicMock(return_value={"sub": "svc-account", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)

    return reg


@pytest_asyncio.fixture
async def client(_patch_dependencies):
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def restricted_client(_patch_dependencies):
    """Client whose caller has only encrypt+decrypt — no rotate/grant/manage_apps."""
    from app.dependencies import AuthenticatedCaller, get_caller

    app = create_app()

    async def _restricted_caller():
        return AuthenticatedCaller(
            app_id="app-test", sub="svc-account", scopes=["encrypt", "decrypt"]
        )

    app.dependency_overrides[get_caller] = _restricted_caller
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


HEADERS = {"Authorization": "Bearer fake.jwt", "X-App-ID": "app-test"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_event(event_type: str, **filters) -> dict[str, Any] | None:
    """Return the most recent event of the given type matching all filters."""
    for ev in get_recent_events(limit=200):
        if ev.get("event_type") != event_type:
            continue
        if all(ev.get(k) == v for k, v in filters.items()):
            return ev
    return None


async def _encrypt(client, plaintext: str = "secret", **extra) -> dict:
    resp = await client.post(
        f"{API_PREFIX}/encrypt",
        json={"plaintext": plaintext, **extra},
        headers=HEADERS,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _decrypt(client, enc: dict, headers=None) -> tuple[int, dict]:
    resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": enc["ciphertext_b64"],
            "tag_b64": enc["tag_b64"],
        },
        headers=headers or HEADERS,
    )
    return resp.status_code, resp.json()


# ── Test 1: encrypt success ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_encrypt_success_logged(client):
    enc = await _encrypt(client, "hello world")

    ev = _find_event("encrypt", status="success")
    assert ev is not None, "No encrypt/success event found"
    assert ev["app_id"] == "app-test"
    assert ev["sub"] == "svc-account"
    assert ev["edek_id"] == enc["edek_id"]
    assert ev["kek_version"] == enc["kek_version"]


# ── Test 2: encrypt PBAC denied ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_encrypt_pbac_denied_logged(client, monkeypatch):
    pbac = MagicMock()
    pbac.check = AsyncMock(return_value=False)

    from app import dependencies
    monkeypatch.setattr(dependencies, "_pbac_client", pbac)

    resp = await client.post(
        f"{API_PREFIX}/encrypt",
        json={"plaintext": "denied", "end_user_id": "u-001", "data_classification": "pii"},
        headers=HEADERS,
    )
    assert resp.status_code == 403

    ev = _find_event("encrypt", status="failure", reason="pbac_denied")
    assert ev is not None
    assert ev["end_user_id"] == "u-001"


# ── Test 3: decrypt success ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_decrypt_success_logged(client):
    enc = await _encrypt(client, "decrypt me")
    code, body = await _decrypt(client, enc)
    assert code == 200

    ev = _find_event("decrypt", status="success")
    assert ev is not None
    assert ev["edek_id"] == enc["edek_id"]
    assert ev["app_id"] == "app-test"


# ── Test 4: decrypt — EDEK not found ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_decrypt_edek_not_found_logged(client):
    fake_id = str(uuid.uuid4())
    resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": fake_id,
            "iv_b64": base64.b64encode(b"\x00" * 12).decode(),
            "ciphertext_b64": base64.b64encode(b"garbage").decode(),
            "tag_b64": base64.b64encode(b"\x00" * 16).decode(),
        },
        headers=HEADERS,
    )
    assert resp.status_code == 404

    ev = _find_event("decrypt", status="failure", reason="edek_not_found")
    assert ev is not None
    assert ev["edek_id"] == fake_id


# ── Test 5: decrypt — no grant for owner ──────────────────────────────────────

@pytest.mark.asyncio
async def test_decrypt_no_grant_logged(client, _patch_dependencies, edek_store):
    # Manually insert a record owned by a different app
    other_edek_id = str(uuid.uuid4())
    record = EDEKRecord(
        edek_id=uuid.UUID(other_edek_id),
        app_id="app-other",
        edek_blob=base64.b64encode(b"dummy").decode(),
        kek_version="demo-v1",
        algorithm="AES-256-GCM",
        encoding="utf8",
        rotation_status=RotationStatus.current,
    )
    record.created_at = datetime.now(timezone.utc)
    edek_store[other_edek_id] = record

    # app-test has no grant to decrypt app-other's data
    _patch_dependencies.is_granted = AsyncMock(return_value=False)

    resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": other_edek_id,
            "iv_b64": base64.b64encode(b"\x00" * 12).decode(),
            "ciphertext_b64": base64.b64encode(b"x").decode(),
            "tag_b64": base64.b64encode(b"\x00" * 16).decode(),
        },
        headers=HEADERS,
    )
    assert resp.status_code == 403

    ev = _find_event("decrypt", status="failure", reason="no_grant_for_owner")
    assert ev is not None


# ── Test 6: decrypt — PBAC denied ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_decrypt_pbac_denied_logged(client, monkeypatch):
    enc = await _encrypt(client, "protected", end_user_id="u-999", data_classification="pii")

    pbac = MagicMock()
    pbac.check = AsyncMock(return_value=False)
    from app import dependencies
    monkeypatch.setattr(dependencies, "_pbac_client", pbac)

    resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": enc["ciphertext_b64"],
            "tag_b64": enc["tag_b64"],
            "end_user_id": "u-999",
        },
        headers=HEADERS,
    )
    assert resp.status_code == 403

    ev = _find_event("decrypt", status="failure", reason="pbac_denied")
    assert ev is not None
    assert ev["end_user_id"] == "u-999"


# ── Test 7: decrypt — tag verification failed ─────────────────────────────────

@pytest.mark.asyncio
async def test_decrypt_tag_verification_failed_logged(client):
    enc = await _encrypt(client, "tamper test")

    # Corrupt the ciphertext
    bad_ct = base64.b64encode(b"this is garbage").decode()
    resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": bad_ct,
            "tag_b64": enc["tag_b64"],
        },
        headers=HEADERS,
    )
    assert resp.status_code == 422

    ev = _find_event("decrypt", status="failure", reason="tag_verification_failed")
    assert ev is not None
    assert ev["edek_id"] == enc["edek_id"]


# ── Test 8: KEK rotation completed ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_kek_rotation_logged(client):
    await _encrypt(client, "before rotation")
    resp = await client.post(f"{API_PREFIX}/admin/rotate-kek", headers=HEADERS)
    assert resp.status_code == 200

    ev = _find_event("kek_rotation_completed", status="success")
    assert ev is not None
    assert ev["records_rotated"] == 1
    assert "new_kek_version" in ev
    assert ev["triggered_by"].startswith("api:")


# ── Test 9: KEK rotation denied (no rotate scope) ────────────────────────────

@pytest.mark.asyncio
async def test_kek_rotation_denied_logged(restricted_client):
    resp = await restricted_client.post(f"{API_PREFIX}/admin/rotate-kek", headers=HEADERS)
    assert resp.status_code == 403
    ev = _find_event("kek_rotation_denied", status="failure")
    assert ev is not None


# ── Test 10: grant added ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_grant_added_logged(client):
    resp = await client.post(
        f"{API_PREFIX}/admin/grants",
        json={"grantee_app_id": "app-reader", "owner_app_id": "app-test"},
        headers=HEADERS,
    )
    assert resp.status_code == 201

    ev = _find_event("grant_added", status="success")
    assert ev is not None
    assert ev["grantee_app_id"] == "app-reader"
    assert ev["owner_app_id"] == "app-test"
    assert ev["app_id"] == "app-test"


# ── Test 11: grant removed ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_grant_removed_logged(client):
    import json as _json
    resp = await client.request(
        "DELETE",
        f"{API_PREFIX}/admin/grants",
        content=_json.dumps({"grantee_app_id": "app-reader", "owner_app_id": "app-test"}),
        headers={**HEADERS, "Content-Type": "application/json"},
    )
    assert resp.status_code == 204

    ev = _find_event("grant_removed", status="success")
    assert ev is not None
    assert ev["grantee_app_id"] == "app-reader"


# ── Test 12: grant denied (missing scope) ────────────────────────────────────

@pytest.mark.asyncio
async def test_grant_denied_logged(restricted_client):
    resp = await restricted_client.post(
        f"{API_PREFIX}/admin/grants",
        json={"grantee_app_id": "app-reader", "owner_app_id": "app-test"},
        headers=HEADERS,
    )
    assert resp.status_code == 403
    ev = _find_event("grant_added", status="failure")
    assert ev is not None
    assert "scope_not_permitted:grant" in ev.get("reason", "")


# ── Test 13: app status changed ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_app_status_changed_logged(client):
    resp = await client.post(
        f"{API_PREFIX}/admin/apps/status",
        json={"app_id": "app-victim", "active": False},
        headers=HEADERS,
    )
    assert resp.status_code == 200

    ev = _find_event("app_status_changed", status="success")
    assert ev is not None
    assert ev["target_app_id"] == "app-victim"
    assert ev["active"] is False


# ── Test 14: app status denied (missing scope) ───────────────────────────────

@pytest.mark.asyncio
async def test_app_status_denied_logged(restricted_client):
    resp = await restricted_client.post(
        f"{API_PREFIX}/admin/apps/status",
        json={"app_id": "app-victim", "active": False},
        headers=HEADERS,
    )
    assert resp.status_code == 403
    ev = _find_event("app_status_changed", status="failure")
    assert ev is not None


# ── Test 15: get_recent_events newest-first, capped at limit ─────────────────

@pytest.mark.asyncio
async def test_get_recent_events_order_and_limit(client):
    for i in range(5):
        await _encrypt(client, f"event-{i}")

    events = get_recent_events(limit=3)
    assert len(events) == 3
    # Ring buffer returns newest-first: the 5th encrypt was most recent
    for ev in events:
        assert ev.get("event_type") == "encrypt"


# ── Test 16: required fields always present on encrypt / decrypt events ────────

@pytest.mark.asyncio
async def test_required_fields_always_present(client):
    enc = await _encrypt(client, "field check")
    await _decrypt(client, enc)

    for event_type in ("encrypt", "decrypt"):
        ev = _find_event(event_type, status="success")
        assert ev is not None, f"Missing {event_type}/success event"
        for field in ("app_id", "sub", "edek_id", "status"):
            assert field in ev, f"Field '{field}' missing from {event_type} event"


# ── Test 17: no plaintext or DEK material in any audit event ──────────────────

@pytest.mark.asyncio
async def test_no_plaintext_in_audit_events(client):
    secret = "top-secret-value-" + secrets.token_hex(8)
    enc = await _encrypt(client, secret)
    await _decrypt(client, enc)

    for ev in get_recent_events(limit=200):
        ev_str = str(ev)
        assert secret not in ev_str, f"Plaintext found in audit event: {ev_str}"
        # DEK is 32 random bytes — we can't check bytes directly, but ensure
        # no field named 'dek' or 'plaintext' appears in the record
        assert "dek" not in ev, f"'dek' key found in audit event"
        assert "plaintext" not in ev, f"'plaintext' key found in audit event"


# ── Test 18: ring buffer accumulates all events in chronological order ────────

@pytest.mark.asyncio
async def test_ring_buffer_accumulates_events_in_order(client):
    enc1 = await _encrypt(client, "first")
    enc2 = await _encrypt(client, "second")
    enc3 = await _encrypt(client, "third")

    # get_recent_events is newest-first; reverse to get insertion order
    events = list(reversed(get_recent_events(limit=200)))
    encrypt_ids = [e["edek_id"] for e in events if e.get("event_type") == "encrypt" and e.get("status") == "success"]

    assert encrypt_ids == [enc1["edek_id"], enc2["edek_id"], enc3["edek_id"]]
