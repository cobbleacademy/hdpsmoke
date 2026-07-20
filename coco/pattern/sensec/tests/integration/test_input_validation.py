"""
Area 8 — Input Validation, Size Limits, and Element Mix-up Detection

Tests:
  Size limits (encrypt)
  1.  1-byte plaintext → 201 (below minimum still passes — min_length=1)
  2.  Plaintext at exactly 64 KiB UTF-8 bytes → 201
  3.  Plaintext at 1 MiB + 1 byte → 422 (byte-level hard ceiling enforced)
  4.  Multibyte Unicode within byte limit → 201
  5.  Empty plaintext → 422 (min_length=1)

  Element integrity (decrypt)
  6.  Correct elements from same response → 200
  7.  iv_b64 wrong byte length → 422, reason in message
  8.  tag_b64 wrong byte length → 422, reason in message
  9.  iv_b64 and tag_b64 swapped between two different responses → 422, mismatch message
 10.  ciphertext_b64 swapped with another response (iv+tag correct) → 422, mismatch message
 11.  edek_id from A, everything else from B → 422, mismatch message
 12.  Pre-existing record (fingerprint=None) decrypts without fingerprint check
 13.  Genuinely tampered ciphertext (same edek, same iv/tag, flipped byte) → 422, "corrupt or tampered"
 14.  Invalid base64 on any field → 422 pydantic validation error
 15.  audit event carries the specific failure reason for each rejection path
"""

from __future__ import annotations

import base64
import secrets
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

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
HEADERS = {"Authorization": "Bearer fake.jwt", "X-App-ID": "app-test"}


# ── In-memory store ───────────────────────────────────────────────────────────

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


class FakeSessionFactory:
    def __init__(self, store: dict):
        self._store = store
    def __call__(self):
        return FakeSession(self._store)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _clear_audit():
    audit_module._recent_events.clear()
    yield
    audit_module._recent_events.clear()


@pytest.fixture
def edek_store():
    return {}


@pytest.fixture
def kek_client():
    return MockKEKClient()


@pytest.fixture
def _patch(monkeypatch, kek_client, edek_store):
    from app import dependencies
    monkeypatch.setattr(dependencies, "_kek_client", kek_client)
    monkeypatch.setattr(dependencies, "_session_factory", FakeSessionFactory(edek_store))

    reg = MagicMock()
    reg.get_scopes = AsyncMock(return_value=["encrypt", "decrypt", "rotate"])
    reg.require_scope = AsyncMock()
    reg.is_granted = AsyncMock(side_effect=lambda grantee_app_id, owner_app_id: grantee_app_id == owner_app_id)
    monkeypatch.setattr(dependencies, "_app_registry", reg)

    val = MagicMock()
    val.validate = MagicMock(return_value={"sub": "svc", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)


@pytest_asyncio.fixture
async def client(_patch):
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _encrypt(client, plaintext: str) -> dict:
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": plaintext}, headers=HEADERS)
    assert r.status_code == 201, r.text
    return r.json()


async def _decrypt_raw(client, payload: dict) -> tuple[int, dict]:
    r = await client.post(f"{API_PREFIX}/decrypt", json=payload, headers=HEADERS)
    return r.status_code, r.json()


def _last_failure(reason: str) -> dict | None:
    for ev in get_recent_events(limit=100):
        if ev.get("status") == "failure" and ev.get("reason") == reason:
            return ev
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SIZE LIMIT TESTS
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_single_byte_plaintext_accepted(client):
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": "x"}, headers=HEADERS)
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_64kib_plaintext_accepted(client):
    payload = "A" * 65_536
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": payload}, headers=HEADERS)
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_over_1mib_plaintext_rejected(client):
    payload = "A" * (1_048_576 + 1)
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": payload}, headers=HEADERS)
    assert r.status_code == 422
    body = r.json()
    detail = str(body)
    assert "1048576" in detail or "limit" in detail.lower() or "max_length" in detail.lower()


@pytest.mark.asyncio
async def test_multibyte_unicode_within_limit_accepted(client):
    # Japanese characters — 3 bytes each in UTF-8; 1000 chars = 3000 bytes (well within limit)
    payload = "あいうえお" * 200
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": payload}, headers=HEADERS)
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_empty_plaintext_rejected(client):
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": ""}, headers=HEADERS)
    assert r.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════════
# ELEMENT INTEGRITY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_correct_elements_decrypt_200(client):
    enc = await _encrypt(client, "valid decrypt")
    code, body = await _decrypt_raw(client, {
        "edek_id": enc["edek_id"],
        "iv_b64": enc["iv_b64"],
        "ciphertext_b64": enc["ciphertext_b64"],
        "tag_b64": enc["tag_b64"],
    })
    assert code == 200
    assert body["plaintext"] == "valid decrypt"


@pytest.mark.asyncio
async def test_wrong_iv_length_rejected(client):
    enc = await _encrypt(client, "test")
    # 8 bytes instead of required 12
    bad_iv = base64.b64encode(b"\x00" * 8).decode()
    code, body = await _decrypt_raw(client, {
        "edek_id": enc["edek_id"],
        "iv_b64": bad_iv,
        "ciphertext_b64": enc["ciphertext_b64"],
        "tag_b64": enc["tag_b64"],
    })
    assert code == 422
    detail = str(body)
    assert "iv_b64" in detail
    assert "12" in detail      # tells client the required length


@pytest.mark.asyncio
async def test_wrong_tag_length_rejected(client):
    enc = await _encrypt(client, "test")
    # 8 bytes instead of required 16
    bad_tag = base64.b64encode(b"\x00" * 8).decode()
    code, body = await _decrypt_raw(client, {
        "edek_id": enc["edek_id"],
        "iv_b64": enc["iv_b64"],
        "ciphertext_b64": enc["ciphertext_b64"],
        "tag_b64": bad_tag,
    })
    assert code == 422
    detail = str(body)
    assert "tag_b64" in detail
    assert "16" in detail      # tells client the required length


@pytest.mark.asyncio
async def test_iv_and_tag_swapped_between_responses_rejected(client):
    """
    edek_id from A, iv+tag from B, ciphertext from A.
    Fingerprint(iv_B, tag_B) != stored fingerprint(iv_A, tag_A) → element_mismatch.
    """
    enc_a = await _encrypt(client, "response A")
    enc_b = await _encrypt(client, "response B")

    code, body = await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],
        "iv_b64": enc_b["iv_b64"],          # ← from B
        "ciphertext_b64": enc_a["ciphertext_b64"],
        "tag_b64": enc_b["tag_b64"],         # ← from B
    })
    assert code == 422
    detail = str(body)
    assert "edek_id" in detail or "mix" in detail.lower() or "belong" in detail.lower()

    ev = _last_failure("element_mismatch")
    assert ev is not None


@pytest.mark.asyncio
async def test_ciphertext_swapped_between_responses_rejected(client):
    """
    edek_id/iv/tag from A, ciphertext from B.
    iv+tag fingerprint matches (they're from A), but AES-GCM fails because
    ciphertext is from a different encryption.
    """
    enc_a = await _encrypt(client, "response A")
    enc_b = await _encrypt(client, "response B")

    code, body = await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],
        "iv_b64": enc_a["iv_b64"],
        "ciphertext_b64": enc_b["ciphertext_b64"],   # ← from B
        "tag_b64": enc_a["tag_b64"],
    })
    # iv+tag match the fingerprint so fingerprint check passes;
    # AES-GCM then catches the wrong ciphertext via tag verification
    assert code == 422
    detail = str(body)
    assert "tampered" in detail.lower() or "authentication" in detail.lower() or "corrupt" in detail.lower()


@pytest.mark.asyncio
async def test_edek_id_from_a_everything_else_from_b_rejected(client):
    """
    edek_id from A → DEK_A unwrapped.
    iv/tag from B → fingerprint(iv_B, tag_B) != stored fingerprint(iv_A, tag_A) → mismatch.
    """
    enc_a = await _encrypt(client, "response A")
    enc_b = await _encrypt(client, "response B")

    code, body = await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],          # ← edek from A
        "iv_b64": enc_b["iv_b64"],             # ← everything else from B
        "ciphertext_b64": enc_b["ciphertext_b64"],
        "tag_b64": enc_b["tag_b64"],
    })
    assert code == 422
    ev = _last_failure("element_mismatch")
    assert ev is not None


@pytest.mark.asyncio
async def test_legacy_record_without_fingerprint_decrypts(client, edek_store):
    """
    Records written before the fingerprint column was added have fingerprint=None.
    They must still decrypt — the pre-flight check must be skipped for NULL fingerprints.
    """
    from app.crypto.dek_manager import make_fingerprint
    enc = await _encrypt(client, "legacy record")

    # Simulate a pre-migration record: clear the fingerprint
    stored = edek_store[enc["edek_id"]]
    stored.fingerprint = None

    code, body = await _decrypt_raw(client, {
        "edek_id": enc["edek_id"],
        "iv_b64": enc["iv_b64"],
        "ciphertext_b64": enc["ciphertext_b64"],
        "tag_b64": enc["tag_b64"],
    })
    assert code == 200
    assert body["plaintext"] == "legacy record"


@pytest.mark.asyncio
async def test_tampered_ciphertext_byte_rejected(client):
    """
    Flip one byte in the ciphertext (iv+tag are correct → fingerprint passes).
    AES-GCM must reject it with tag_verification_failed, not a confusing generic error.
    """
    enc = await _encrypt(client, "tamper me")

    ct = bytearray(base64.b64decode(enc["ciphertext_b64"]))
    ct[0] ^= 0xFF   # flip first byte
    bad_ct = base64.b64encode(bytes(ct)).decode()

    code, body = await _decrypt_raw(client, {
        "edek_id": enc["edek_id"],
        "iv_b64": enc["iv_b64"],
        "ciphertext_b64": bad_ct,
        "tag_b64": enc["tag_b64"],
    })
    assert code == 422
    detail = str(body)
    assert "tampered" in detail.lower() or "corrupt" in detail.lower() or "authentication" in detail.lower()

    ev = _last_failure("tag_verification_failed")
    assert ev is not None


@pytest.mark.asyncio
async def test_invalid_base64_field_rejected(client):
    """Non-base64 characters in any field must be rejected by Pydantic (422)."""
    enc = await _encrypt(client, "base64 test")

    for bad_field, bad_value in [
        ("iv_b64", "!!!not-base64!!!"),
        ("ciphertext_b64", "not+valid==garbage"),
        ("tag_b64", "@#$%"),
    ]:
        payload = {
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": enc["ciphertext_b64"],
            "tag_b64": enc["tag_b64"],
        }
        payload[bad_field] = bad_value
        code, body = await _decrypt_raw(client, payload)
        assert code == 422, f"Expected 422 for {bad_field}={bad_value!r}, got {code}: {body}"


@pytest.mark.asyncio
async def test_audit_event_carries_specific_failure_reason(client):
    """Each rejection path must produce an audit event with a distinct reason."""
    enc_a = await _encrypt(client, "A")
    enc_b = await _encrypt(client, "B")

    # element_mismatch
    await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],
        "iv_b64": enc_b["iv_b64"],
        "ciphertext_b64": enc_b["ciphertext_b64"],
        "tag_b64": enc_b["tag_b64"],
    })
    assert _last_failure("element_mismatch") is not None

    # invalid_iv_length
    bad_iv = base64.b64encode(b"\x00" * 6).decode()
    await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],
        "iv_b64": bad_iv,
        "ciphertext_b64": enc_a["ciphertext_b64"],
        "tag_b64": enc_a["tag_b64"],
    })
    assert _last_failure("invalid_iv_length") is not None

    # invalid_tag_length
    bad_tag = base64.b64encode(b"\x00" * 4).decode()
    await _decrypt_raw(client, {
        "edek_id": enc_a["edek_id"],
        "iv_b64": enc_a["iv_b64"],
        "ciphertext_b64": enc_a["ciphertext_b64"],
        "tag_b64": bad_tag,
    })
    assert _last_failure("invalid_tag_length") is not None
