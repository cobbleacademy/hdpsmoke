"""
In-memory stand-in for Azure Key Vault Managed HSM — DEMO MODE ONLY.

Implements the same interface as app.crypto.kek_client.KEKClient so the
rest of the service (routers, encryption_service, rotation_service) runs
completely unmodified. The "master key" here is just a process-memory
AES-256 key — there is no hardware boundary, no FIPS guarantee, and the
key is lost on restart. Never use this in anything but a local demo.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone

# Fixed demo key — never changes across restarts so the SQLite DB remains valid.
# This is intentional: demo mode has no security guarantees whatsoever.
_DEMO_V1_KEY = bytes.fromhex("d8a954a65371f88768f06712205a323bcbe8d30b103d742809d8f9e88dfcb841")

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class MockKEKClient:
    def __init__(self) -> None:
        self._versions: dict[str, bytes] = {}
        self._created_at: dict[str, str] = {}
        self._current_version = "demo-v1"
        self._versions[self._current_version] = _DEMO_V1_KEY
        self._created_at[self._current_version] = datetime.now(timezone.utc).isoformat()

    async def wrap_dek(self, dek: bytes) -> tuple[bytes, str]:
        version = self._current_version
        key = self._versions[version]
        nonce = secrets.token_bytes(12)
        wrapped = AESGCM(key).encrypt(nonce, dek, None)
        return nonce + wrapped, version

    async def unwrap_dek(self, edek: bytes, kek_version: str) -> bytes:
        key = self._versions[kek_version]
        nonce, wrapped = edek[:12], edek[12:]
        return AESGCM(key).decrypt(nonce, wrapped, None)

    async def get_current_kek_version(self) -> str:
        return self._current_version

    async def rotate_to_new_version(self) -> str:
        """Demo-only: simulate Azure Key Vault promoting a new key version."""
        existing = [int(v.split("-v")[1]) for v in self._versions]
        next_n = max(existing) + 1
        new_version = f"demo-v{next_n}"
        self._versions[new_version] = secrets.token_bytes(32)
        self._created_at[new_version] = datetime.now(timezone.utc).isoformat()
        self._current_version = new_version
        return new_version

    async def fetch_secret(self, secret_name: str) -> str:
        return ""

    async def close(self) -> None:
        pass

    def get_state(self) -> dict:
        """
        Demo-only introspection — never exposes key bytes, only version
        metadata. A real HSM client has no equivalent call; key material
        is never readable by definition.
        """
        versions = [
            {
                "version": v,
                "created_at": self._created_at.get(v, ""),
                "is_current": v == self._current_version,
                "key_length_bits": len(key) * 8,
            }
            for v, key in self._versions.items()
        ]
        versions.sort(key=lambda x: x["version"])
        return {
            "current_version": self._current_version,
            "total_versions": len(self._versions),
            "versions": versions,
        }
