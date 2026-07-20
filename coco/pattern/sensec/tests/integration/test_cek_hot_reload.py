"""
Area 3 — Pod CEK Hot-Reload Tests (no pod restart)

Tests the background poll loop (_cek_reload_loop) that detects CEK slot
changes from Azure KV Secrets and calls DEKCache.rotate() in-process.

Coverage:
  1.  Single pod: slot change detected within one poll interval
  2.  Single pod: encrypt before + after poll — decrypt always succeeds
  3.  kv_version change on same slot triggers rotate (same slot, new bytes)
  4.  No rotate when version unchanged — cache state untouched
  5.  Multi-pod: encrypt on pod-A, decrypt on pod-B after both converge
  6.  Decrypt during convergence window — fallback prev_cek serves old entries
  7.  Poll failure (KV unreachable) — warning logged, no crash, loop continues
  8.  Multiple consecutive slot changes — cache tracks latest version
  9.  rotate() called exactly once per detected change, not on every poll
 10.  Cache version matches KV current_key after convergence
"""

from __future__ import annotations

import asyncio
import base64
import logging
import secrets
from dataclasses import dataclass, field
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio

from app.crypto.dek_cache import DEKCache
from app.dependencies import _cek_reload_loop


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_cek() -> bytes:
    return secrets.token_bytes(32)


def _cek_b64(cek: bytes) -> str:
    return base64.b64encode(cek).decode()


def _make_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


def _make_cache(
    redis,
    cek: bytes,
    version: str,
    prev_cek: bytes | None = None,
    prev_version: str | None = None,
    ttl: int = 60,
) -> DEKCache:
    return DEKCache(
        redis_client=redis,
        cek=cek,
        version=version,
        ttl_seconds=ttl,
        excluded_classifications=set(),
        prev_cek=prev_cek,
        prev_version=prev_version,
    )


@dataclass
class FakeKVState:
    """Simulates Azure KV Secrets — current slot and per-slot CEK bytes."""
    current_slot: str
    slots: dict[str, tuple[bytes, str]]  # slot → (cek_bytes, kv_version)
    fail_next: bool = False

    def cek_bytes(self, slot: str) -> bytes:
        return self.slots[slot][0]

    def kv_version(self, slot: str) -> str:
        return self.slots[slot][1]

    def composite(self) -> str:
        return f"{self.current_slot}:{self.kv_version(self.current_slot)}"


def _make_kek_client(kv: FakeKVState) -> MagicMock:
    """Build a MockKEKClient whose fetch_secret / fetch_secret_with_version
    read from the shared FakeKVState so tests can mutate it mid-poll."""

    async def fetch_secret(name: str) -> str:
        if kv.fail_next:
            kv.fail_next = False
            raise ConnectionError("KV unreachable")
        if "current" in name:
            return kv.current_slot
        slot = "alpha" if "alpha" in name else "beta"
        return _cek_b64(kv.cek_bytes(slot))

    async def fetch_secret_with_version(name: str):
        if kv.fail_next:
            kv.fail_next = False
            raise ConnectionError("KV unreachable")
        slot = "alpha" if "alpha" in name else "beta"
        cek_b64 = _cek_b64(kv.cek_bytes(slot))
        kv_ver = kv.kv_version(slot)
        return cek_b64, kv_ver

    client = MagicMock()
    client.fetch_secret = AsyncMock(side_effect=fetch_secret)
    client.fetch_secret_with_version = AsyncMock(side_effect=fetch_secret_with_version)
    return client


class FakeSettings:
    cek_current_key_secret_name = "cek-current-key"
    cek_alpha_secret_name = "cek-alpha"
    cek_beta_secret_name = "cek-beta"
    dek_cache_reload_interval_seconds = 0.05   # 50ms — fast for tests


async def _run_poll(cache: DEKCache, kek_client, n_polls: int = 1) -> None:
    """
    Drive the reload loop for exactly n_polls body iterations then cancel.

    The loop structure is: sleep → body → sleep → body → ...
    We patch asyncio.sleep to return instantly and raise CancelledError
    only AFTER n_polls iterations so the body always runs n_polls times.
    """
    poll_count = 0

    async def instant_sleep(_delay):
        nonlocal poll_count
        poll_count += 1
        if poll_count > n_polls:
            raise asyncio.CancelledError
        # else return immediately — body runs after this

    # Patch the asyncio reference inside app.dependencies, not the global one
    with patch("app.dependencies.asyncio.sleep", side_effect=instant_sleep):
        try:
            await _cek_reload_loop(cache, kek_client, FakeSettings())
        except asyncio.CancelledError:
            pass


# ── Test 1: Slot change detected within one poll interval ─────────────────────

@pytest.mark.asyncio
async def test_slot_change_detected_within_one_poll():
    """After one poll, cache.current_version must match the new KV slot."""
    alpha_cek = _make_cek()
    beta_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (beta_cek, "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    # Switch slot before poll fires
    kv.current_slot = "beta"

    await _run_poll(cache, kek_client, n_polls=1)

    assert cache.current_version == "beta:v1"


# ── Test 2: Encrypt before poll, decrypt after — always succeeds ──────────────

@pytest.mark.asyncio
async def test_encrypt_before_poll_decrypt_after():
    """
    DEK cached under alpha before poll must be decryptable after poll
    rotates cache to beta (via fallback prev_cek path).
    """
    alpha_cek = _make_cek()
    beta_cek = _make_cek()
    dek = secrets.token_bytes(32)
    edek_id = "edek-pre-poll"

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (beta_cek, "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    # Write DEK under alpha:v1
    await cache.set(edek_id, dek, data_classification=None)
    assert await redis.exists(f"dek:alpha:v1:{edek_id}")

    # Switch slot and run poll
    kv.current_slot = "beta"
    await _run_poll(cache, kek_client, n_polls=1)

    assert cache.current_version == "beta:v1"

    # DEK must still be retrievable via prev_cek fallback
    result = await cache.get(edek_id)
    assert result == dek


# ── Test 3: kv_version change on same slot triggers rotate ───────────────────

@pytest.mark.asyncio
async def test_kv_version_change_triggers_rotate():
    """
    Same slot (alpha) but new kv_version (v1→v2) must trigger rotate.
    This happens when Rotation SVC writes new bytes to the same slot secret.
    """
    alpha_v1_cek = _make_cek()
    alpha_v2_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_v1_cek, "v1"), "beta": (_make_cek(), "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_v1_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    # Simulate Rotation SVC writing new CEK bytes to alpha slot
    kv.slots["alpha"] = (alpha_v2_cek, "v2")

    await _run_poll(cache, kek_client, n_polls=1)

    assert cache.current_version == "alpha:v2"


# ── Test 4: No rotate when version unchanged ──────────────────────────────────

@pytest.mark.asyncio
async def test_no_rotate_when_version_unchanged():
    """Poll must not call rotate() when KV reports the same version."""
    alpha_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (_make_cek(), "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    original_rotate = cache.rotate
    rotate_calls = []
    cache.rotate = lambda cek, ver: rotate_calls.append(ver) or original_rotate(cek, ver)

    # No slot or version change
    await _run_poll(cache, kek_client, n_polls=2)

    assert len(rotate_calls) == 0
    assert cache.current_version == "alpha:v1"


# ── Test 5: Multi-pod: encrypt on pod-A, decrypt on pod-B after convergence ───

@pytest.mark.asyncio
async def test_multi_pod_encrypt_pod_a_decrypt_pod_b():
    """
    Simulate two pods sharing Redis. Encrypt on pod-A (alpha), rotate both
    pods to beta, decrypt on pod-B — must succeed via fallback.
    """
    alpha_cek = _make_cek()
    beta_cek = _make_cek()
    dek = secrets.token_bytes(32)
    edek_id = "edek-multi-pod"

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (beta_cek, "v1")},
    )

    # Shared Redis — both pods use the same instance
    redis = _make_redis()

    pod_a = _make_cache(redis, alpha_cek, "alpha:v1")
    pod_b = _make_cache(redis, alpha_cek, "alpha:v1")

    # Pod-A encrypts on alpha:v1
    await pod_a.set(edek_id, dek, data_classification=None)

    # Switch KV to beta
    kv.current_slot = "beta"

    # Both pods poll and converge to beta:v1
    kek_a = _make_kek_client(kv)
    kek_b = _make_kek_client(kv)
    await asyncio.gather(
        _run_poll(pod_a, kek_a, n_polls=1),
        _run_poll(pod_b, kek_b, n_polls=1),
    )

    assert pod_a.current_version == "beta:v1"
    assert pod_b.current_version == "beta:v1"

    # Pod-B decrypts — entry is under alpha:v1 (prev), fallback must serve it
    result = await pod_b.get(edek_id)
    assert result == dek


# ── Test 6: Decrypt during convergence window via prev_cek ───────────────────

@pytest.mark.asyncio
async def test_decrypt_during_convergence_window():
    """
    Pod has rotated to beta but Redis still has entries from alpha.
    get() must fall back to prev_cek (alpha) and return the correct DEK.
    """
    alpha_cek = _make_cek()
    beta_cek = _make_cek()
    dek = secrets.token_bytes(32)
    edek_id = "edek-convergence"

    redis = _make_redis()

    # Write under alpha
    alpha_cache = _make_cache(redis, alpha_cek, "alpha:v1")
    await alpha_cache.set(edek_id, dek, data_classification=None)

    # Pod now has beta as current, alpha as prev (post-rotation state)
    cache = _make_cache(redis, beta_cek, "beta:v1", prev_cek=alpha_cek, prev_version="alpha:v1")

    # No beta key exists yet — must fall back to alpha
    result = await cache.get(edek_id)
    assert result == dek


# ── Test 7: Poll failure — warning logged, loop continues ─────────────────────

@pytest.mark.asyncio
async def test_poll_failure_logs_warning_and_continues(caplog):
    """KV unreachable during poll must log a warning and not crash the loop."""
    alpha_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (_make_cek(), "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    # First poll fails, second succeeds
    kv.fail_next = True

    with caplog.at_level(logging.WARNING):
        await _run_poll(cache, kek_client, n_polls=2)

    assert any("CEK reload poll failed" in r.message for r in caplog.records)
    # Cache must still be functional after failure
    assert cache.current_version == "alpha:v1"


# ── Test 8: Multiple consecutive slot changes — cache tracks latest ───────────

@pytest.mark.asyncio
async def test_multiple_consecutive_slot_changes():
    """
    alpha→beta→alpha (new kv_version): cache must track the latest version
    after each poll, never getting stuck on a stale intermediate state.
    """
    alpha_v1_cek = _make_cek()
    beta_cek = _make_cek()
    alpha_v2_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_v1_cek, "v1"), "beta": (beta_cek, "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_v1_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    # Poll 1: alpha→beta
    kv.current_slot = "beta"
    await _run_poll(cache, kek_client, n_polls=1)
    assert cache.current_version == "beta:v1"

    # Poll 2: beta→alpha:v2 (slot reuse with new CEK bytes)
    kv.current_slot = "alpha"
    kv.slots["alpha"] = (alpha_v2_cek, "v2")
    await _run_poll(cache, kek_client, n_polls=1)
    assert cache.current_version == "alpha:v2"


# ── Test 9: rotate() called exactly once per detected change ──────────────────

@pytest.mark.asyncio
async def test_rotate_called_exactly_once_per_change():
    """
    Over 3 polls where only the first detects a change, rotate() must be
    called exactly once — not on every poll tick.
    """
    alpha_cek = _make_cek()
    beta_cek = _make_cek()

    kv = FakeKVState(
        current_slot="alpha",
        slots={"alpha": (alpha_cek, "v1"), "beta": (beta_cek, "v1")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    kek_client = _make_kek_client(kv)

    rotate_calls = []
    original_rotate = cache.rotate

    def tracking_rotate(cek, ver):
        rotate_calls.append(ver)
        original_rotate(cek, ver)

    cache.rotate = tracking_rotate

    # Change slot before first poll, leave stable for remaining polls
    kv.current_slot = "beta"
    await _run_poll(cache, kek_client, n_polls=3)

    assert len(rotate_calls) == 1
    assert rotate_calls[0] == "beta:v1"


# ── Test 10: Cache version matches KV after convergence ──────────────────────

@pytest.mark.asyncio
async def test_cache_version_matches_kv_after_convergence():
    """
    After convergence, cache.current_version must exactly equal
    '{slot}:{kv_version}' from the KV state.
    """
    alpha_cek = _make_cek()
    beta_cek = _make_cek()

    kv = FakeKVState(
        current_slot="beta",
        slots={"alpha": (alpha_cek, "abc123"), "beta": (beta_cek, "def456")},
    )
    redis = _make_redis()
    cache = _make_cache(redis, alpha_cek, "alpha:abc123")
    kek_client = _make_kek_client(kv)

    await _run_poll(cache, kek_client, n_polls=1)

    expected = kv.composite()  # "beta:def456"
    assert cache.current_version == expected
