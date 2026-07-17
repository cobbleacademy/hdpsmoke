"""
Redis-backed DEK cache with versioned CEK support.

After Azure Key Vault unwraps a DEK, the raw DEK bytes are AES-256-GCM
encrypted with a Cache Encryption Key (CEK) and stored in Redis for a
short TTL window. Subsequent decrypts of the same edek_id within the
TTL window skip the KV round-trip entirely.

CEK rotation (hot-reload, no pod restart):
  The CEK is versioned. Redis keys are namespaced by CEK version:
      dek:{cek_version}:{edek_id}
  When rotation is detected (background poll, every 30s), DEKCache.rotate()
  atomically promotes current→prev and installs the new CEK as current.
  During the transition window:
    - get() tries the current version first.
    - On MISS, falls back to the previous version.
    - A fallback hit is immediately backfilled under the current version.
  Old entries expire naturally via TTL — no cache flush needed, no MISS
  storm, no cross-pod key collisions during a rolling update.

The DEK is NEVER stored as plaintext. The Redis value format is:
    {iv_b64}:{ciphertext_b64}
where iv is a fresh random 12-byte nonce per write.
"""

from __future__ import annotations

import asyncio
import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class DEKCache:
    def __init__(
        self,
        redis_client,               # redis.asyncio.Redis — typed loosely to avoid hard import
        cek: bytes,
        version: str,               # e.g. "v1"
        ttl_seconds: int,
        excluded_classifications: set[str],
        prev_cek: bytes | None = None,
        prev_version: str | None = None,
    ) -> None:
        if len(cek) != 32:
            raise ValueError(f"CEK must be exactly 32 bytes, got {len(cek)}")
        if prev_cek is not None and len(prev_cek) != 32:
            raise ValueError(f"prev_cek must be exactly 32 bytes, got {len(prev_cek)}")
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._excluded = excluded_classifications
        self._lock = asyncio.Lock()

        self._current_version = version
        self._current_aesgcm = AESGCM(cek)
        self._prev_version = prev_version
        self._prev_aesgcm = AESGCM(prev_cek) if prev_cek else None

    @property
    def current_version(self) -> str:
        return self._current_version

    def rotate(self, new_cek: bytes, new_version: str) -> None:
        """
        Atomically promote current CEK → prev and install new_cek as current.
        Called by the background reload task when a new version is detected in
        Azure KV. Thread-safe via GIL; asyncio-safe because it never awaits.
        """
        if new_version == self._current_version:
            return
        self._prev_version = self._current_version
        self._prev_aesgcm = self._current_aesgcm
        self._current_version = new_version
        self._current_aesgcm = AESGCM(new_cek)

    def _key(self, version: str, edek_id: str) -> str:
        return f"dek:{version}:{edek_id}"

    async def get(self, edek_id: str) -> bytes | None:
        """
        Return raw DEK bytes on cache hit, None on miss or any Redis error.
        Read order:
          1. current CEK version → fast path, most requests end here.
          2. previous CEK version → grace-period fallback during rotation.
             A hit here is backfilled under the current version so the next
             read takes the fast path.
        """
        try:
            blob = await self._redis.get(self._key(self._current_version, edek_id))
            if blob is not None:
                return self._decrypt_blob(blob, self._current_aesgcm)

            # Grace-period fallback: entry was written by a pod still on prev CEK
            if self._prev_aesgcm and self._prev_version:
                blob = await self._redis.get(self._key(self._prev_version, edek_id))
                if blob is not None:
                    dek = self._decrypt_blob(blob, self._prev_aesgcm)
                    # Backfill under current version — next read will be a fast-path HIT
                    await self._write_blob(edek_id, dek)
                    return dek

            return None
        except Exception:
            return None  # cache miss on any error — never block the decrypt path

    async def set(
        self,
        edek_id: str,
        dek: bytes,
        data_classification: str | None,
    ) -> None:
        """Encrypt dek with current CEK and store in Redis. Skips excluded classifications."""
        if data_classification and data_classification.lower() in self._excluded:
            return
        await self._write_blob(edek_id, dek)

    async def _write_blob(self, edek_id: str, dek: bytes) -> None:
        try:
            blob = self._encrypt_blob(dek, self._current_aesgcm)
            await self._redis.set(self._key(self._current_version, edek_id), blob, ex=self._ttl)
        except Exception:
            pass  # cache write failure is non-fatal

    async def delete(self, edek_id: str) -> None:
        """Explicitly evict cached DEK entries across both current and previous versions."""
        try:
            keys = [self._key(self._current_version, edek_id)]
            if self._prev_version:
                keys.append(self._key(self._prev_version, edek_id))
            await self._redis.delete(*keys)
        except Exception:
            pass

    def _encrypt_blob(self, dek: bytes, aesgcm: AESGCM) -> bytes:
        iv = os.urandom(12)
        ciphertext = aesgcm.encrypt(iv, dek, None)
        return base64.b64encode(iv) + b":" + base64.b64encode(ciphertext)

    def _decrypt_blob(self, blob: bytes, aesgcm: AESGCM) -> bytes:
        iv_b64, ct_b64 = blob.split(b":", 1)
        return aesgcm.decrypt(base64.b64decode(iv_b64), base64.b64decode(ct_b64), None)


class NullDEKCache:
    """Drop-in no-op used when cache is disabled (demo mode or dek_cache_enabled=false)."""

    current_version: str = "null"

    async def get(self, edek_id: str) -> bytes | None:
        return None

    async def set(self, edek_id: str, dek: bytes, data_classification: str | None) -> None:
        pass

    async def delete(self, edek_id: str) -> None:
        pass

    def rotate(self, new_cek: bytes, new_version: str) -> None:
        pass
