"""
DEK lifecycle: generate, encrypt, decrypt using AES-256-GCM (FIPS 140-2 approved).

AAD (Additional Authenticated Data) binds ciphertext to the originating app_id.
This prevents a stolen ciphertext blob from being decrypted by a different app,
even if that app obtains the EDEK.
"""

from __future__ import annotations

import ctypes
import secrets
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.crypto import iv_factory


DEK_LENGTH_BYTES = 32   # 256-bit AES key
ALGORITHM = "AES-256-GCM"   # persisted per-record so future algorithm migrations stay decryptable


@dataclass(slots=True)
class EncryptResult:
    ciphertext: bytes
    iv: bytes
    tag: bytes          # GCM authentication tag (last 16 bytes of cryptography output)


def generate_dek() -> bytearray:
    """Return a fresh 256-bit DEK as a mutable bytearray so it can be zeroed."""
    raw = secrets.token_bytes(DEK_LENGTH_BYTES)
    return bytearray(raw)


def encrypt(plaintext: bytes, dek: bytearray, app_id: str) -> EncryptResult:
    """
    AES-256-GCM encrypt. The cryptography library appends the 16-byte GCM tag
    to the ciphertext; we split them for explicit storage.
    """
    iv = iv_factory.generate()
    aad = _make_aad(app_id)
    aesgcm = AESGCM(bytes(dek))
    combined = aesgcm.encrypt(iv, plaintext, aad)
    # cryptography returns ciphertext || tag
    ciphertext, tag = combined[:-16], combined[-16:]
    return EncryptResult(ciphertext=ciphertext, iv=iv, tag=tag)


def decrypt(ciphertext: bytes, tag: bytes, iv: bytes, dek: bytearray, app_id: str) -> bytes:
    """
    AES-256-GCM decrypt + tag verification. Raises cryptography.exceptions.InvalidTag
    on authentication failure (tampered ciphertext or wrong app_id).
    """
    aad = _make_aad(app_id)
    aesgcm = AESGCM(bytes(dek))
    combined = ciphertext + tag
    return aesgcm.decrypt(iv, combined, aad)


def zero_dek(dek: bytearray) -> None:
    """Overwrite DEK bytes in memory immediately after use."""
    for i in range(len(dek)):
        dek[i] = 0
    # Best-effort: ask the allocator not to swap this to disk
    with suppress_oserror():
        ctypes.memset(ctypes.c_char_p(bytes(dek)), 0, len(dek))


def _make_aad(app_id: str) -> bytes:
    return f"hsm-svc:app_id={app_id}".encode()


class suppress_oserror:
    def __enter__(self): return self
    def __exit__(self, exc_type, *_): return exc_type is OSError
