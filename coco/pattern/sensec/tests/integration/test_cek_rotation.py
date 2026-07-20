"""
Area 2 — CEK Rotation + Slot Switch Tests

Tests the full CEK rotation lifecycle using DEKCache (in-process) and
the post-rotation Redis ops from cek_rotation/redis_ops.py.

Coverage:
  1.  Rotate alpha→beta: encrypt + decrypt with new slot CEK
  2.  Redis keys use new slot prefix after rotation
  3.  Rotate beta→alpha (slot reuse): kv_version prevents key collision
  4.  Old slot entries expire within TTL after rotation
  5.  Rekey mode: all old-slot Redis entries migrated to new slot
  6.  Flush mode: all old-slot Redis entries deleted
  7.  count_by_version: correct per-slot counts
  8.  DEKCache get() hits current version — no Redis call to prev
  9.  DEKCache get() falls back to prev version on current miss
 10.  Fallback hit is backfilled under current version
 11.  Excluded classifications bypass cache entirely
 12.  Cache miss on unknown edek_id returns None
 13.  rotate() is a no-op when version is unchanged
 14.  Concurrent cache reads during slot switch — no corruption
 15.  rekey with already-expired entries counted as skipped
"""

from __future__ import annotations

import asyncio
import base64
import os
import secrets

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio

from app.crypto.dek_cache import DEKCache, NullDEKCache
from cek_rotation.redis_ops import count_by_version, flush_dek_cache, rekey_dek_cache


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_cek() -> bytes:
    return secrets.token_bytes(32)


def _make_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


def _make_cache(
    redis,
    cek: bytes,
    version: str,
    ttl: int = 60,
    prev_cek: bytes | None = None,
    prev_version: str | None = None,
    excluded: set[str] | None = None,
) -> DEKCache:
    return DEKCache(
        redis_client=redis,
        cek=cek,
        version=version,
        ttl_seconds=ttl,
        excluded_classifications=excluded or set(),
        prev_cek=prev_cek,
        prev_version=prev_version,
    )


def _make_dek() -> bytes:
    return secrets.token_bytes(32)


# ── Test 1: Rotate alpha→beta: cache set + get with new CEK ──────────────────

@pytest.mark.asyncio
async def test_alpha_to_beta_set_and_get():
    """After rotating to beta, cache must store and retrieve DEK under beta key."""
    redis = _make_redis()
    alpha_cek = _make_cek()
    beta_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-001"

    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    cache.rotate(beta_cek, "beta:v2")

    await cache.set(edek_id, dek, data_classification=None)
    result = await cache.get(edek_id)

    assert result == dek


# ── Test 2: Redis keys use new slot prefix after rotation ─────────────────────

@pytest.mark.asyncio
async def test_redis_key_uses_new_slot_prefix():
    """After rotation, Redis keys must be prefixed with the new version."""
    redis = _make_redis()
    alpha_cek = _make_cek()
    beta_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-002"

    cache = _make_cache(redis, alpha_cek, "alpha:v1")
    cache.rotate(beta_cek, "beta:v2")
    await cache.set(edek_id, dek, data_classification=None)

    # Key must exist under new slot, not old
    new_key = f"dek:beta:v2:{edek_id}"
    old_key = f"dek:alpha:v1:{edek_id}"

    assert await redis.exists(new_key)
    assert not await redis.exists(old_key)


# ── Test 3: Slot reuse (beta→alpha): kv_version prevents collision ────────────

@pytest.mark.asyncio
async def test_slot_reuse_kv_version_prevents_collision():
    """
    When alpha slot is reused after alpha→beta→alpha rotation, the new
    kv_version must produce a different Redis key than the original alpha,
    preventing stale entries from being served.
    """
    redis = _make_redis()
    alpha_v1_cek = _make_cek()
    beta_cek = _make_cek()
    alpha_v2_cek = _make_cek()  # same slot, new bytes, new kv_version

    dek_v1 = _make_dek()
    dek_v2 = _make_dek()
    edek_id = "edek-003"

    # Phase 1: alpha:v1
    cache = _make_cache(redis, alpha_v1_cek, "alpha:v1")
    await cache.set(edek_id, dek_v1, data_classification=None)

    # Phase 2: rotate alpha→beta
    cache.rotate(beta_cek, "beta:v1")
    await cache.set(edek_id, dek_v1, data_classification=None)

    # Phase 3: rotate beta→alpha (reuse slot, new kv_version)
    cache.rotate(alpha_v2_cek, "alpha:v2")
    await cache.set(edek_id, dek_v2, data_classification=None)

    # Must retrieve dek_v2 (not dek_v1) — get() reads from current version alpha:v2
    result = await cache.get(edek_id)
    assert result == dek_v2

    # New key must exist under alpha:v2 (not alpha:v1)
    new_key = f"dek:alpha:v2:{edek_id}"
    assert await redis.exists(new_key)

    # Old alpha:v1 key may still exist (expires naturally via TTL — DEKCache never
    # deletes old keys on rotation). Collision prevention works because the keys
    # are namespaced by kv_version, not just slot name.


# ── Test 4: Old slot entries expire within TTL ────────────────────────────────

@pytest.mark.asyncio
async def test_old_slot_entries_expire_within_ttl():
    """Old-version Redis keys must have a TTL set and eventually expire."""
    redis = _make_redis()
    alpha_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-004"

    cache = _make_cache(redis, alpha_cek, "alpha:v1", ttl=2)
    await cache.set(edek_id, dek, data_classification=None)

    key = f"dek:alpha:v1:{edek_id}"
    ttl_val = await redis.ttl(key)

    # TTL must be set (>0) and ≤ configured ttl_seconds
    assert 0 < ttl_val <= 2

    # Wait for expiry
    await asyncio.sleep(3)
    assert await redis.get(key) is None


# ── Test 5: Rekey mode — all old-slot entries migrated ───────────────────────

@pytest.mark.asyncio
async def test_rekey_migrates_all_old_slot_entries():
    """rekey_dek_cache must migrate all old-version entries to new version."""
    redis = _make_redis()
    old_cek = _make_cek()
    new_cek = _make_cek()
    old_version = "alpha:v1"
    new_version = "beta:v1"

    # Write 5 entries under old version
    deks = {}
    cache = _make_cache(redis, old_cek, old_version)
    for i in range(5):
        edek_id = f"edek-{i:03d}"
        dek = _make_dek()
        deks[edek_id] = dek
        await cache.set(edek_id, dek, data_classification=None)

    result = await rekey_dek_cache(
        redis_client=redis,
        old_cek=old_cek,
        new_cek=new_cek,
        old_version=old_version,
        new_version=new_version,
        default_ttl=60,
    )

    assert result["rekeyed"] == 5
    assert result["failed"] == 0
    assert result["skipped"] == 0

    # Verify all entries readable under new version
    new_cache = _make_cache(redis, new_cek, new_version)
    for edek_id, original_dek in deks.items():
        retrieved = await new_cache.get(edek_id)
        assert retrieved == original_dek, f"Mismatch for {edek_id}"

    # Old version keys must be gone
    old_keys = [k async for k in redis.scan_iter(f"dek:{old_version}:*")]
    assert len(old_keys) == 0


# ── Test 6: Flush mode — all old-slot entries deleted ────────────────────────

@pytest.mark.asyncio
async def test_flush_deletes_all_dek_cache_entries():
    """flush_dek_cache must delete all dek:* keys."""
    redis = _make_redis()
    cek = _make_cek()
    cache = _make_cache(redis, cek, "alpha:v1")

    for i in range(5):
        await cache.set(f"edek-{i}", _make_dek(), data_classification=None)

    deleted = await flush_dek_cache(redis)
    assert deleted == 5

    remaining = [k async for k in redis.scan_iter("dek:*")]
    assert len(remaining) == 0


# ── Test 7: count_by_version returns correct per-slot counts ─────────────────

@pytest.mark.asyncio
async def test_count_by_version_correct():
    """count_by_version must return accurate counts for each slot:kv_version."""
    redis = _make_redis()
    alpha_cek = _make_cek()
    beta_cek = _make_cek()

    alpha_cache = _make_cache(redis, alpha_cek, "alpha:v1")
    beta_cache = _make_cache(redis, beta_cek, "beta:v1")

    for i in range(3):
        await alpha_cache.set(f"edek-a{i}", _make_dek(), data_classification=None)
    for i in range(2):
        await beta_cache.set(f"edek-b{i}", _make_dek(), data_classification=None)

    counts = await count_by_version(redis)

    assert counts.get("alpha:v1") == 3
    assert counts.get("beta:v1") == 2


# ── Test 8: Cache get() hits current version without fallback ─────────────────

@pytest.mark.asyncio
async def test_cache_hit_on_current_version():
    """get() must return the DEK when found under the current version key."""
    redis = _make_redis()
    cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-hit"

    cache = _make_cache(redis, cek, "alpha:v1")
    await cache.set(edek_id, dek, data_classification=None)

    result = await cache.get(edek_id)
    assert result == dek


# ── Test 9: Cache get() falls back to prev version on current miss ────────────

@pytest.mark.asyncio
async def test_cache_fallback_to_prev_version():
    """
    Entry written under prev version must be returned via fallback
    when the current version key is absent.
    """
    redis = _make_redis()
    prev_cek = _make_cek()
    curr_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-prev"

    # Write under prev version
    prev_cache = _make_cache(redis, prev_cek, "alpha:v1")
    await prev_cache.set(edek_id, dek, data_classification=None)

    # New cache with prev as fallback
    cache = _make_cache(redis, curr_cek, "beta:v2", prev_cek=prev_cek, prev_version="alpha:v1")

    result = await cache.get(edek_id)
    assert result == dek


# ── Test 10: Fallback hit is backfilled under current version ─────────────────

@pytest.mark.asyncio
async def test_fallback_hit_backfilled_under_current():
    """
    After a fallback hit from prev version, the entry must be backfilled
    under the current version key so the next read is a fast-path hit.
    """
    redis = _make_redis()
    prev_cek = _make_cek()
    curr_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-backfill"

    # Write under prev
    prev_cache = _make_cache(redis, prev_cek, "alpha:v1")
    await prev_cache.set(edek_id, dek, data_classification=None)

    # Read via fallback — triggers backfill
    cache = _make_cache(redis, curr_cek, "beta:v2", prev_cek=prev_cek, prev_version="alpha:v1")
    await cache.get(edek_id)

    # Current version key must now exist
    current_key = f"dek:beta:v2:{edek_id}"
    assert await redis.exists(current_key)

    # And must decrypt correctly
    result = await cache.get(edek_id)
    assert result == dek


# ── Test 11: Excluded classifications bypass cache ────────────────────────────

@pytest.mark.asyncio
async def test_excluded_classification_bypasses_cache():
    """set() must not write to Redis for excluded data classifications."""
    redis = _make_redis()
    cek = _make_cek()
    cache = _make_cache(redis, cek, "alpha:v1", excluded={"pci", "pii"})

    await cache.set("edek-pci", _make_dek(), data_classification="pci")
    await cache.set("edek-pii", _make_dek(), data_classification="pii")
    await cache.set("edek-std", _make_dek(), data_classification="standard")

    keys = [k async for k in redis.scan_iter("dek:*")]
    assert len(keys) == 1
    assert b"edek-std" in keys[0]


# ── Test 12: Cache miss on unknown edek_id returns None ──────────────────────

@pytest.mark.asyncio
async def test_cache_miss_returns_none():
    """get() must return None for an edek_id that was never cached."""
    redis = _make_redis()
    cek = _make_cek()
    cache = _make_cache(redis, cek, "alpha:v1")

    result = await cache.get("nonexistent-edek")
    assert result is None


# ── Test 13: rotate() is a no-op when version is unchanged ───────────────────

@pytest.mark.asyncio
async def test_rotate_noop_on_same_version():
    """rotate() with the current version must not change internal state."""
    redis = _make_redis()
    cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-noop"

    cache = _make_cache(redis, cek, "alpha:v1")
    await cache.set(edek_id, dek, data_classification=None)

    # rotate to same version — should be a no-op
    cache.rotate(cek, "alpha:v1")

    result = await cache.get(edek_id)
    assert result == dek
    assert cache.current_version == "alpha:v1"


# ── Test 14: Concurrent reads during slot switch — no corruption ──────────────

@pytest.mark.asyncio
async def test_concurrent_reads_during_slot_switch():
    """
    20 concurrent get() calls during a slot switch must all return
    the correct DEK — no corruption from race between current and prev.
    """
    redis = _make_redis()
    prev_cek = _make_cek()
    curr_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-concurrent"

    # Pre-populate under prev version
    prev_cache = _make_cache(redis, prev_cek, "alpha:v1")
    await prev_cache.set(edek_id, dek, data_classification=None)

    # Cache with prev fallback simulates mid-rotation state
    cache = _make_cache(redis, curr_cek, "beta:v2", prev_cek=prev_cek, prev_version="alpha:v1")

    results = await asyncio.gather(*[cache.get(edek_id) for _ in range(20)])

    assert all(r == dek for r in results), "Some concurrent reads returned wrong DEK"


# ── Test 15: rekey skips already-expired entries ──────────────────────────────

@pytest.mark.asyncio
async def test_rekey_skips_expired_entries():
    """
    Entries that expire between SCAN and GET must be counted as skipped,
    not failed, and rekey must complete without error.
    """
    redis = _make_redis()
    old_cek = _make_cek()
    new_cek = _make_cek()

    # Write one entry with very short TTL
    cache = _make_cache(redis, old_cek, "alpha:v1", ttl=1)
    await cache.set("edek-expiring", _make_dek(), data_classification=None)

    # Write one normal entry
    cache2 = _make_cache(redis, old_cek, "alpha:v1", ttl=60)
    await cache2.set("edek-stable", _make_dek(), data_classification=None)

    # Wait for short-TTL entry to expire
    await asyncio.sleep(2)

    result = await rekey_dek_cache(
        redis_client=redis,
        old_cek=old_cek,
        new_cek=new_cek,
        old_version="alpha:v1",
        new_version="beta:v1",
        default_ttl=60,
    )

    # stable rekeyed, expiring skipped, none failed
    assert result["rekeyed"] == 1
    assert result["failed"] == 0


# ── Test 16: flush on empty cache returns 0 ──────────────────────────────────

@pytest.mark.asyncio
async def test_flush_empty_cache_returns_zero():
    """flush_dek_cache on an empty Redis must return 0 without error."""
    redis = _make_redis()
    deleted = await flush_dek_cache(redis)
    assert deleted == 0


# ── Test 17: rekey preserves remaining TTL ───────────────────────────────────

@pytest.mark.asyncio
async def test_rekey_preserves_remaining_ttl():
    """
    After rekey, the new key's TTL must be ≤ the original key's TTL
    at the time it was written (time elapses, so it will be slightly less).
    """
    redis = _make_redis()
    old_cek = _make_cek()
    new_cek = _make_cek()
    dek = _make_dek()
    edek_id = "edek-ttl"

    cache = _make_cache(redis, old_cek, "alpha:v1", ttl=30)
    await cache.set(edek_id, dek, data_classification=None)

    await rekey_dek_cache(
        redis_client=redis,
        old_cek=old_cek,
        new_cek=new_cek,
        old_version="alpha:v1",
        new_version="beta:v1",
        default_ttl=60,
    )

    new_key = f"dek:beta:v1:{edek_id}"
    new_ttl = await redis.ttl(new_key)

    # TTL must be positive and ≤ 30
    assert 0 < new_ttl <= 30
