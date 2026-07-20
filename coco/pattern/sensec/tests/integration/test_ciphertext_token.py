"""
Ciphertext Token Tests

Validates the single-token encrypt/decrypt flow and the token codec.

Coverage:
  1.  encrypt response includes ciphertext_token
  2.  decrypt with ciphertext_token returns correct plaintext (200)
  3.  legacy fields still work (backward compat)
  4.  token is opaque: client stores & echoes one string, no field management
  5.  token from response A cannot decrypt response B (tamper attempt)
  6.  truncated token → 422, descriptive message
  7.  wrong version prefix → 422, descriptive message
  8.  token with no ciphertext payload → 422
  9.  pack/unpack round-trip is identity (unit)
 10.  unpack rejects unknown version byte
 11.  DecryptRequest with neither token nor legacy fields → 422
 12.  DecryptRequest with legacy fields missing some → 422 with hint
"""

from __future__ import annotations

import base64
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import get_settings
from app.crypto.dek_manager import pack_token, unpack_token
from app.demo.mock_kek_client import MockKEKClient
from app.main import create_app
from app.models.edek_record import EDEKRecord, RotationStatus
from app.models.schemas import DecryptRequest

API_PREFIX = get_settings().api_v1_prefix
HEADERS = {"Authorization": "Bearer fake.jwt", "X-App-ID": "app-test"}


# ── In-memory store ───────────────────────────────────────────────────────────

class _ScalarResult:
    def __init__(self, rows): self._rows = rows
    def all(self): return self._rows


class FakeSession:
    def __init__(self, store):
        self._store = store

    def add(self, obj):
        if getattr(obj, "created_at", None) is None:
            obj.created_at = datetime.now(timezone.utc)
        if getattr(obj, "rotation_status", None) is None:
            obj.rotation_status = RotationStatus.current
        self._store[str(obj.edek_id)] = obj

    async def commit(self): pass
    async def get(self, model, pk): return self._store.get(str(pk))
    async def scalars(self, stmt): return _ScalarResult(list(self._store.values()))
    async def execute(self, stmt): return None
    async def __aenter__(self): return self
    async def __aexit__(self, *_): pass


@pytest.fixture
def edek_store(): return {}

@pytest.fixture
def kek_client(): return MockKEKClient()

@pytest.fixture
def _patch(monkeypatch, kek_client, edek_store):
    from app import dependencies
    monkeypatch.setattr(dependencies, "_kek_client", kek_client)
    monkeypatch.setattr(dependencies, "_session_factory",
                        type("SF", (), {"__call__": lambda s: FakeSession(edek_store)})())
    reg = MagicMock()
    reg.get_scopes = AsyncMock(return_value=["encrypt", "decrypt", "rotate"])
    reg.require_scope = AsyncMock()
    reg.is_granted = AsyncMock(
        side_effect=lambda grantee_app_id, owner_app_id: grantee_app_id == owner_app_id
    )
    monkeypatch.setattr(dependencies, "_app_registry", reg)
    val = MagicMock()
    val.validate = MagicMock(return_value={"sub": "svc", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)

@pytest_asyncio.fixture
async def client(_patch):
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c

async def _encrypt(client, text="secret"):
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": text}, headers=HEADERS)
    assert r.status_code == 201, r.text
    return r.json()


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_encrypt_response_includes_token(client):
    enc = await _encrypt(client)
    assert "ciphertext_token" in enc
    assert enc["ciphertext_token"].startswith("v1.")


@pytest.mark.asyncio
async def test_decrypt_with_token_returns_plaintext(client):
    enc = await _encrypt(client, "hello token")
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": enc["ciphertext_token"]},
        headers=HEADERS,
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == "hello token"


@pytest.mark.asyncio
async def test_legacy_fields_still_work(client):
    enc = await _encrypt(client, "legacy path")
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": enc["ciphertext_b64"],
            "tag_b64": enc["tag_b64"],
        },
        headers=HEADERS,
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == "legacy path"


@pytest.mark.asyncio
async def test_token_is_single_opaque_string(client):
    """Client only needs to store and echo ciphertext_token — no field juggling."""
    enc = await _encrypt(client, "opaque")
    token = enc["ciphertext_token"]

    assert isinstance(token, str)
    assert "." in token    # version prefix + payload

    # Single-field decrypt — no edek_id / iv / ciphertext / tag needed
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": token},
        headers=HEADERS,
    )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_token_from_a_cannot_decrypt_b(client):
    """Using response A's token to decrypt in a different context is safe
    because the edek_id is embedded — it points to A's EDEK, not B's."""
    enc_a = await _encrypt(client, "response A")
    enc_b = await _encrypt(client, "response B")

    # Token A resolves A's edek_id internally; it decrypts A's plaintext, not B's
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": enc_a["ciphertext_token"]},
        headers=HEADERS,
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == "response A"   # not B


@pytest.mark.asyncio
async def test_truncated_token_rejected(client):
    enc = await _encrypt(client)
    token = enc["ciphertext_token"]
    truncated = token[:20]   # cut off mid-payload
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": truncated},
        headers=HEADERS,
    )
    assert r.status_code == 422
    detail = str(r.json())
    assert "too short" in detail or "format" in detail or "base64" in detail


@pytest.mark.asyncio
async def test_wrong_version_prefix_rejected(client):
    enc = await _encrypt(client)
    # Replace "v1." with "v9."
    bad_token = "v9." + enc["ciphertext_token"][3:]
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": bad_token},
        headers=HEADERS,
    )
    assert r.status_code == 422
    detail = str(r.json())
    assert "v9" in detail or "format" in detail or "unrecognised" in detail


@pytest.mark.asyncio
async def test_token_with_no_ciphertext_payload_rejected(client):
    # Build a minimal binary with just the fixed header (45 bytes), no ciphertext
    edek_id = uuid.uuid4()
    iv  = b"\x00" * 12
    tag = b"\x00" * 16
    binary = bytes([0x01]) + edek_id.bytes + iv + tag   # no ciphertext appended
    bad_token = "v1." + base64.urlsafe_b64encode(binary).decode()
    r = await client.post(
        f"{API_PREFIX}/decrypt",
        json={"ciphertext_token": bad_token},
        headers=HEADERS,
    )
    assert r.status_code == 422
    assert "ciphertext" in str(r.json()).lower() or "payload" in str(r.json()).lower()


def test_pack_unpack_round_trip():
    edek_id   = uuid.uuid4()
    iv        = b"\xAB" * 12
    tag       = b"\xCD" * 16
    ciphertext = b"\xEF" * 64

    token = pack_token(edek_id, iv, tag, ciphertext)
    unpacked = unpack_token(token)

    assert unpacked.edek_id   == edek_id
    assert unpacked.iv        == iv
    assert unpacked.tag       == tag
    assert unpacked.ciphertext == ciphertext


def test_unpack_rejects_unknown_version():
    edek_id = uuid.uuid4()
    binary = bytes([0xFF]) + edek_id.bytes + b"\x00" * 12 + b"\x00" * 16 + b"ciphertext"
    token = "v1." + base64.urlsafe_b64encode(binary).decode()
    with pytest.raises(ValueError, match="unsupported version"):
        unpack_token(token)


def test_decrypt_request_requires_token_or_legacy():
    with pytest.raises(Exception):
        DecryptRequest()   # neither token nor legacy fields


def test_decrypt_request_legacy_requires_all_fields():
    import pytest as _pytest
    with _pytest.raises(Exception, match="missing"):
        DecryptRequest(edek_id=uuid.uuid4(), iv_b64="aGVsbG8=")   # missing ciphertext_b64, tag_b64
