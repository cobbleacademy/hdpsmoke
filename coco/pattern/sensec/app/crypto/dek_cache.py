"""
Redis-backed DEK cache.

After Azure Key Vault unwraps a DEK, the raw DEK bytes are AES-256-GCM
encrypted with a Cache Encryption Key (CEK) and stored in Redis for a
short TTL window. Subsequent decrypts of the same edek_id within the
TTL window skip the KV round-trip entirely.

The CEK is a 32-byte secret stored in Azure Key Vault (regular vault,
not Managed HSM) and loaded once at service startup. All pod replicas
load the same CEK so their Redis entries are interoperable.

The DEK is NEVER stored as plaintext. The Redis value format is:
    {iv_b64}:{ciphertext_b64}
where iv is a fresh random 12-byte nonce per write.
"""

from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


_REDIS_KEY_PREFIX = "dek:v1:"


class DEKCache:
    def __init__(
        self,
        redis_client,           # redis.asyncio.Redis — typed loosely to avoid hard import at module level
        cek: bytes,
        ttl_seconds: int,
        excluded_classifications: set[str],
    ) -> None:
        if len(cek) != 32:
            raise ValueError(f"CEK must be exactly 32 bytes, got {len(cek)}")
        self._redis = redis_client
        self._aesgcm = AESGCM(cek)
        self._ttl = ttl_seconds
        self._excluded = excluded_classifications

    async def get(self, edek_id: str) -> bytes | None:
        """Return raw DEK bytes on cache hit, None on miss or any Redis error."""
        try:
            blob = await self._redis.get(f"{_REDIS_KEY_PREFIX}{edek_id}")
            if blob is None:
                return None
            return self._decrypt_blob(blob)
        except Exception:
            return None  # cache miss on any error — never block the decrypt path

    async def set(
        self,
        edek_id: str,
        dek: bytes,
        data_classification: str | None,
    ) -> None:
        """Encrypt dek with CEK and store in Redis. Skips excluded classifications."""
        if data_classification and data_classification.lower() in self._excluded:
            return
        try:
            blob = self._encrypt_blob(dek)
            await self._redis.set(f"{_REDIS_KEY_PREFIX}{edek_id}", blob, ex=self._ttl)
        except Exception:
            pass  # cache write failure is non-fatal

    async def delete(self, edek_id: str) -> None:
        """Explicitly evict a cached DEK (e.g. on record deletion)."""
        try:
            await self._redis.delete(f"{_REDIS_KEY_PREFIX}{edek_id}")
        except Exception:
            pass

    def _encrypt_blob(self, dek: bytes) -> bytes:
        iv = os.urandom(12)
        ciphertext = self._aesgcm.encrypt(iv, dek, None)
        iv_b64 = base64.b64encode(iv)
        ct_b64 = base64.b64encode(ciphertext)
        return iv_b64 + b":" + ct_b64

    def _decrypt_blob(self, blob: bytes) -> bytes:
        iv_b64, ct_b64 = blob.split(b":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ct_b64)
        return self._aesgcm.decrypt(iv, ciphertext, None)


class NullDEKCache:
    """Drop-in no-op used when cache is disabled (demo mode or dek_cache_enabled=false)."""

    async def get(self, edek_id: str) -> bytes | None:
        return None

    async def set(self, edek_id: str, dek: bytes, data_classification: str | None) -> None:
        pass

    async def delete(self, edek_id: str) -> None:
        pass
