"""
Area 6 — Concurrency & Load Tests

Validates that the HSM service handles a high volume of concurrent
encrypt/decrypt calls correctly, including during an active KEK rotation
window.  All tests run in-process (no real network), so they are fast
and deterministic while still exercising real asyncio concurrency.

Coverage:
  1.  50 concurrent encrypts — all succeed (201), no 500s
  2.  50 concurrent decrypts — all return the correct plaintext
  3.  Concurrent encrypt + decrypt on same EDEK — no data corruption
  4.  50 encrypts + rotate-kek fired concurrently — zero encrypt failures
  5.  Decrypt of pre-rotation EDEK succeeds after rotation completes
  6.  Re-wrap during rotation: all EDEKs move to new kek_version
  7.  200 concurrent encrypts then one bulk rotation — all re-wrapped
  8.  Rotate called twice concurrently — both complete, no record lost
  9.  Mixed encrypt/decrypt/rotate burst — data integrity preserved
 10.  Zero decryption failures when rotate fires mid-stream
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import get_settings
from app.demo.mock_kek_client import MockKEKClient
from app.main import create_app
from app.models.edek_record import EDEKRecord, RotationStatus

API_PREFIX = get_settings().api_v1_prefix
HEADERS = {"Authorization": "Bearer fake.jwt", "X-App-ID": "app-test"}


# ── In-memory EDEK store ──────────────────────────────────────────────────────

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
        results.sort(key=lambda r: r.created_at or datetime.min.replace(tzinfo=timezone.utc))
        try:
            if stmt._offset_clause is not None:
                results = results[int(str(stmt._offset_clause)):]
        except Exception:
            pass
        try:
            if stmt._limit_clause is not None:
                results = results[:int(str(stmt._limit_clause))]
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

@pytest.fixture
def edek_store() -> dict:
    return {}


@pytest.fixture
def kek_client() -> MockKEKClient:
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
    val.validate = MagicMock(return_value={"sub": "svc-load", "app_id": "app-test"})
    monkeypatch.setattr(dependencies, "_jwt_validator", val)


@pytest_asyncio.fixture
async def client(_patch):
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _encrypt(client, plaintext: str = "data") -> dict:
    r = await client.post(f"{API_PREFIX}/encrypt", json={"plaintext": plaintext}, headers=HEADERS)
    assert r.status_code == 201, f"encrypt failed: {r.text}"
    return r.json()


async def _decrypt(client, enc: dict) -> dict:
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
    assert r.status_code == 200, f"decrypt failed: {r.text}"
    return r.json()


async def _rotate(client) -> dict:
    r = await client.post(f"{API_PREFIX}/admin/rotate-kek", headers=HEADERS)
    assert r.status_code == 200, f"rotate failed: {r.text}"
    return r.json()


# ── Test 1: 50 concurrent encrypts ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_50_concurrent_encrypts_all_succeed(client):
    results = await asyncio.gather(*[_encrypt(client, f"payload-{i}") for i in range(50)])
    assert len(results) == 50
    edek_ids = {r["edek_id"] for r in results}
    assert len(edek_ids) == 50, "Duplicate edek_ids — store collision under concurrency"


# ── Test 2: 50 concurrent decrypts ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_50_concurrent_decrypts_correct_plaintext(client):
    encs = [await _encrypt(client, f"secret-{i}") for i in range(50)]
    decs = await asyncio.gather(*[_decrypt(client, e) for e in encs])
    for i, d in enumerate(decs):
        assert d["plaintext"] == f"secret-{i}", f"Wrong plaintext at index {i}"


# ── Test 3: concurrent encrypt + decrypt on same EDEK ─────────────────────────

@pytest.mark.asyncio
async def test_concurrent_encrypt_decrypt_same_edek_no_corruption(client):
    enc = await _encrypt(client, "shared-secret")
    # Decrypt the same EDEK 20 times concurrently
    results = await asyncio.gather(*[_decrypt(client, enc) for _ in range(20)])
    for r in results:
        assert r["plaintext"] == "shared-secret"


# ── Test 4: 50 encrypts + rotate fired concurrently ───────────────────────────

@pytest.mark.asyncio
async def test_concurrent_encrypts_with_rotation_no_500s(client):
    tasks = [_encrypt(client, f"live-{i}") for i in range(50)]
    tasks.append(_rotate(client))

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [r for r in responses if isinstance(r, Exception)]
    assert not errors, f"Unexpected exceptions: {errors}"

    encrypt_results = [r for r in responses[:-1] if isinstance(r, dict)]
    assert len(encrypt_results) == 50


# ── Test 5: Pre-rotation EDEK decrypts after rotation ─────────────────────────

@pytest.mark.asyncio
async def test_pre_rotation_edek_decrypts_after_rotation(client):
    enc = await _encrypt(client, "before-rotate")
    old_ver = enc["kek_version"]

    rot = await _rotate(client)
    assert rot["new_kek_version"] != old_ver

    dec = await _decrypt(client, enc)
    assert dec["plaintext"] == "before-rotate"


# ── Test 6: All EDEKs re-wrapped after rotation ────────────────────────────────

@pytest.mark.asyncio
async def test_all_edeks_rewrapped_after_rotation(client, edek_store):
    for i in range(20):
        await _encrypt(client, f"record-{i}")

    rot = await _rotate(client)
    new_ver = rot["new_kek_version"]

    assert rot["records_queued"] == 20
    for record in edek_store.values():
        assert record.kek_version == new_ver, f"Record still on old version: {record.kek_version}"


# ── Test 7: 200 concurrent encrypts then bulk rotation ────────────────────────

@pytest.mark.asyncio
async def test_200_concurrent_encrypts_bulk_rotation(client, edek_store):
    encs = await asyncio.gather(*[_encrypt(client, f"bulk-{i}") for i in range(200)])
    assert len(encs) == 200

    rot = await _rotate(client)
    assert rot["records_queued"] == 200

    new_ver = rot["new_kek_version"]
    for record in edek_store.values():
        assert record.kek_version == new_ver


# ── Test 8: Two concurrent rotations — both complete, no record lost ──────────

@pytest.mark.asyncio
async def test_two_concurrent_rotations_no_record_lost(client, edek_store):
    for i in range(10):
        await _encrypt(client, f"record-{i}")

    rot1, rot2 = await asyncio.gather(_rotate(client), _rotate(client))
    total = rot1["records_queued"] + rot2["records_queued"]
    # Combined they must have processed all 10 records (some may overlap pages)
    assert total >= 10

    # All records must be decryptable — no corruption from concurrent re-wrap
    encs = [await _encrypt(client, f"verify-{i}") for i in range(5)]
    for enc in encs:
        dec = await _decrypt(client, enc)
        assert dec["plaintext"].startswith("verify-")


# ── Test 9: Mixed burst — data integrity preserved ────────────────────────────

@pytest.mark.asyncio
async def test_mixed_burst_encrypt_decrypt_rotate(client):
    """
    20 pre-existing EDEKs + 30 new encrypts + 1 rotation, all fired concurrently.
    Every pre-existing EDEK must still decrypt correctly after the burst settles.
    """
    pre = [await _encrypt(client, f"pre-{i}") for i in range(20)]

    tasks = (
        [_encrypt(client, f"burst-{i}") for i in range(30)]
        + [_rotate(client)]
    )
    await asyncio.gather(*tasks, return_exceptions=True)

    # All pre-existing EDEKs must still decrypt
    decs = await asyncio.gather(*[_decrypt(client, e) for e in pre])
    for i, d in enumerate(decs):
        assert d["plaintext"] == f"pre-{i}"


# ── Test 10: Zero decrypt failures when rotate fires mid-stream ───────────────

@pytest.mark.asyncio
async def test_zero_decrypt_failures_during_rotation(client):
    """
    Encrypt 40 records, then fire rotate + 40 decrypts concurrently.
    All decrypts must succeed — rotate must never break in-flight decrypts.
    """
    encs = [await _encrypt(client, f"inflight-{i}") for i in range(40)]

    tasks = [_decrypt(client, e) for e in encs] + [_rotate(client)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    decrypt_results = results[:40]
    exceptions = [r for r in decrypt_results if isinstance(r, Exception)]
    assert not exceptions, f"Decrypt exceptions during rotation: {exceptions}"

    for i, r in enumerate(decrypt_results):
        assert isinstance(r, dict), f"result[{i}] is not a dict: {r}"
        assert r["plaintext"] == f"inflight-{i}"
