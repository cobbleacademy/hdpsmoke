"""
Integration tests — run against a live (or mocked) Azure Key Vault.
Set AZURE_KEYVAULT_URL, DATABASE_URL, etc. in environment before running.

These tests mock the KEK client so the suite can run in CI without
a real HSM. For end-to-end validation, remove the mock fixture and
point to a real Managed HSM.
"""

from __future__ import annotations

import base64
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import get_settings
from app.main import create_app


# ── Fixtures ──────────────────────────────────────────────────────────────────

FAKE_KEK_VERSION = "abc123"

API_PREFIX = get_settings().api_v1_prefix


@pytest.fixture(autouse=True)
def _patch_dependencies(monkeypatch):
    """Stub out Azure and DB so tests are hermetic."""
    from app import dependencies

    # KEK client — wrap/unwrap must actually round-trip the real DEK that
    # dek_manager generates per call. Using fixed FAKE_DEK/FAKE_EDEK values
    # here would mean encrypt() always GCM-encrypts under one random DEK
    # while decrypt() always unwraps to a different fixed one — guaranteed
    # tag mismatch, unrelated to whatever the test is actually checking.
    kek = MagicMock()
    kek.wrap_dek = AsyncMock(side_effect=lambda dek: (dek, FAKE_KEK_VERSION))
    kek.unwrap_dek = AsyncMock(side_effect=lambda edek, version: edek)
    kek.get_current_kek_version = AsyncMock(return_value=FAKE_KEK_VERSION)
    kek.close = AsyncMock()
    monkeypatch.setattr(dependencies, "_kek_client", kek)

    # In-memory EDEK store
    _store: dict[str, object] = {}

    class FakeSession:
        def __init__(self): self._added = []
        def add(self, obj): self._added.append(obj); _store[str(obj.edek_id)] = obj
        async def commit(self): pass
        async def get(self, model, pk):
            return _store.get(str(pk))
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass

    class FakeFactory:
        def __call__(self): return FakeSession()

    monkeypatch.setattr(dependencies, "_session_factory", FakeFactory())

    # App registry
    reg = MagicMock()
    reg.get_scopes = AsyncMock(return_value=["encrypt", "decrypt"])
    reg.require_scope = AsyncMock()
    reg.is_granted = AsyncMock(side_effect=lambda grantee_app_id, owner_app_id: grantee_app_id == owner_app_id)
    monkeypatch.setattr(dependencies, "_app_registry", reg)

    # JWT validator — always accepts
    val = MagicMock()
    val.validate = MagicMock(return_value={"sub": "test-user", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)


@pytest_asyncio.fixture
async def client():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


HEADERS = {"Authorization": "Bearer fake.jwt.token", "X-App-ID": "app-test"}


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_encrypt_returns_200(client):
    resp = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": "hello world"}, headers=HEADERS)
    assert resp.status_code == 201
    body = resp.json()
    assert "edek_id" in body
    assert "iv_b64" in body
    assert "ciphertext_b64" in body
    assert "tag_b64" in body


@pytest.mark.asyncio
async def test_encrypt_then_decrypt_roundtrip(client):
    enc_resp = await client.post(
        f"{API_PREFIX}/encrypt", json={"plaintext": "round-trip secret"}, headers=HEADERS
    )
    assert enc_resp.status_code == 201
    enc = enc_resp.json()

    dec_resp = await client.post(
        f"{API_PREFIX}/decrypt",
        json={
            "edek_id": enc["edek_id"],
            "iv_b64": enc["iv_b64"],
            "ciphertext_b64": enc["ciphertext_b64"],
            "tag_b64": enc["tag_b64"],
        },
        headers=HEADERS,
    )
    assert dec_resp.status_code == 200
    assert dec_resp.json()["plaintext"] == "round-trip secret"


@pytest.mark.asyncio
async def test_same_plaintext_different_ciphertext(client):
    """IV randomisation must produce different ciphertext for identical inputs."""
    payload = {"plaintext": "determinism test"}
    r1 = await client.post(f"{API_PREFIX}/encrypt", json=payload, headers=HEADERS)
    r2 = await client.post(f"{API_PREFIX}/encrypt", json=payload, headers=HEADERS)
    assert r1.json()["ciphertext_b64"] != r2.json()["ciphertext_b64"]
    assert r1.json()["iv_b64"] != r2.json()["iv_b64"]


@pytest.mark.asyncio
async def test_missing_auth_header_rejected(client):
    resp = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": "x"})
    assert resp.status_code in (401, 422)


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get(f"{API_PREFIX}/admin/health")
    assert resp.status_code == 200
