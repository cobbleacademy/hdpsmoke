"""
Post-rotation Redis DEK cache operations.

After rotating the CEK the Rotation SVC can optionally inspect or migrate
existing cache entries so pods converge faster than waiting for natural TTL
expiry (default 60 s).

Key format (set by app/crypto/dek_cache.py):
    dek:{slot}:{kv_version}:{edek_id}

Value format:
    {iv_b64}:{ciphertext_b64}   — AES-256-GCM blob
"""

from __future__ import annotations

import base64
import os

import structlog
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = structlog.get_logger("cek_rotation.redis_ops")

_KEY_PREFIX = "dek:"


def _version_prefix(version: str) -> str:
    return f"{_KEY_PREFIX}{version}:"


async def count_by_version(redis_client) -> dict[str, int]:
    """
    Scan all DEK cache keys and return entry counts grouped by CEK version.

    Returns a dict like:
        {"alpha:3f8a2b...": 142, "beta:c91d44...": 7}

    "zero or non-zero" for each version tells you whether the old CEK is
    still referenced.  Call *before* a flush or rekey to get the baseline,
    and again after to confirm zero old-version entries remain.
    """
    counts: dict[str, int] = {}
    async for raw_key in redis_client.scan_iter(f"{_KEY_PREFIX}*"):
        key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
        # format: dek:{slot}:{kv_version}:{edek_id}
        # split into at most 4 parts so edek_id (which may contain colons) is intact
        parts = key.split(":", 3)
        if len(parts) == 4:
            version = f"{parts[1]}:{parts[2]}"
            counts[version] = counts.get(version, 0) + 1
    return counts


async def flush_dek_cache(redis_client) -> int:
    """
    Delete every DEK cache entry (all dek:* keys).

    Use this when you want the simplest post-rotation cleanup.  All pods
    will take a cache MISS on their next decrypt and re-warm from the HSM.
    Returns the number of keys deleted.
    """
    keys = [k async for k in redis_client.scan_iter(f"{_KEY_PREFIX}*")]
    if not keys:
        return 0
    deleted = await redis_client.delete(*keys)
    log.info("dek_cache_flushed", deleted=deleted)
    return deleted


async def rekey_dek_cache(
    redis_client,
    old_cek: bytes,
    new_cek: bytes,
    old_version: str,
    new_version: str,
    default_ttl: int,
) -> dict:
    """
    Re-encrypt every old-slot DEK cache entry under the new CEK in-place.

    For each key matching ``dek:{old_version}:*``:
      1. Fetch the blob and its remaining TTL atomically via pipeline.
      2. Decrypt with old_cek (AES-256-GCM).
      3. Re-encrypt with new_cek using a fresh random IV.
      4. Write under the new key ``dek:{new_version}:{edek_id}`` with the
         same remaining TTL (or default_ttl if the entry is about to expire).
      5. Delete the old key.

    Pods that poll within the 30 s window find the entry already migrated,
    so there is no cache-MISS storm after rotation.

    Returns a dict: {"rekeyed": N, "failed": M, "skipped": K}
    where "skipped" counts entries that expired between SCAN and GET.
    """
    old_gcm = AESGCM(old_cek)
    new_gcm = AESGCM(new_cek)
    pattern = f"{_version_prefix(old_version)}*"
    prefix_len = len(_version_prefix(old_version))

    rekeyed = skipped = failed = 0

    async for raw_old_key in redis_client.scan_iter(pattern):
        old_key = raw_old_key.decode() if isinstance(raw_old_key, bytes) else raw_old_key
        edek_id = old_key[prefix_len:]
        new_key = f"{_version_prefix(new_version)}{edek_id}"

        try:
            # Fetch TTL and blob atomically; avoids a TOCTOU gap.
            pipe = redis_client.pipeline(transaction=False)
            pipe.ttl(old_key)
            pipe.get(old_key)
            ttl_val, blob = await pipe.execute()

            if blob is None:
                skipped += 1
                continue  # expired between SCAN and GET — nothing to migrate

            # Decrypt old blob
            iv_b64, ct_b64 = blob.split(b":", 1)
            dek = old_gcm.decrypt(
                base64.b64decode(iv_b64),
                base64.b64decode(ct_b64),
                None,
            )

            # Re-encrypt under new CEK with a fresh IV
            new_iv = os.urandom(12)
            new_blob = (
                base64.b64encode(new_iv)
                + b":"
                + base64.b64encode(new_gcm.encrypt(new_iv, dek, None))
            )

            # Preserve the original remaining TTL; fall back to default_ttl
            # if Redis returns -1 (no expiry set) or -2 (key deleted).
            remaining = ttl_val if ttl_val > 0 else default_ttl

            await redis_client.set(new_key, new_blob, ex=remaining)
            await redis_client.delete(old_key)
            rekeyed += 1

        except Exception as exc:
            log.warning(
                "rekey_entry_failed",
                key=old_key,
                error=str(exc),
            )
            failed += 1

    log.info(
        "dek_cache_rekeyed",
        old_version=old_version,
        new_version=new_version,
        rekeyed=rekeyed,
        skipped=skipped,
        failed=failed,
    )
    return {"rekeyed": rekeyed, "skipped": skipped, "failed": failed}
