"""
DEK lifecycle: generate, encrypt, decrypt using AES-256-GCM (FIPS 140-2 approved).

AAD (Additional Authenticated Data) binds ciphertext to the originating app_id.
This prevents a stolen ciphertext blob from being decrypted by a different app,
even if that app obtains the EDEK.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import struct
import uuid
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.crypto import iv_factory

# Expected byte lengths for AES-256-GCM parameters
IV_LENGTH = 12   # 96-bit nonce
TAG_LENGTH = 16  # 128-bit GCM authentication tag

# Token layout (binary, inside base64url wrapper):
#   1 byte  : format version (currently 0x01)
#  16 bytes : edek_id UUID (big-endian)
#  12 bytes : AES-GCM IV (nonce)
#  16 bytes : AES-GCM authentication tag
#   N bytes : ciphertext (variable)
#
# On-wire: "v1.<base64url(binary)>"
# The "v1." prefix lets future parsers detect the version before decoding.
_TOKEN_VERSION = 0x01
_TOKEN_PREFIX = "v1."
_TOKEN_FIXED_BYTES = 1 + 16 + IV_LENGTH + TAG_LENGTH   # 45 bytes


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


def pack_token(edek_id: uuid.UUID, iv: bytes, tag: bytes, ciphertext: bytes) -> str:
    """
    Encode all decrypt inputs into one opaque token the client stores and echoes back.

    Binary layout: version(1) | edek_id(16) | iv(12) | tag(16) | ciphertext(N)
    On-wire:       "v1.<base64url(binary)>"
    """
    binary = bytes([_TOKEN_VERSION]) + edek_id.bytes + iv + tag + ciphertext
    return _TOKEN_PREFIX + base64.urlsafe_b64encode(binary).decode()


@dataclass(slots=True)
class UnpackedToken:
    edek_id: uuid.UUID
    iv: bytes
    tag: bytes
    ciphertext: bytes


def unpack_token(token: str) -> UnpackedToken:
    """
    Decode a ciphertext_token produced by pack_token().
    Raises ValueError with a descriptive message on any format error.
    """
    if not token.startswith(_TOKEN_PREFIX):
        raise ValueError(
            f"ciphertext_token has unrecognised format: expected prefix '{_TOKEN_PREFIX}'"
        )
    b64_part = token[len(_TOKEN_PREFIX):]
    try:
        binary = base64.urlsafe_b64decode(b64_part + "==")   # pad-safe
    except Exception:
        raise ValueError("ciphertext_token contains invalid base64url data")

    if len(binary) < _TOKEN_FIXED_BYTES:
        raise ValueError(
            f"ciphertext_token is too short: {len(binary)} bytes "
            f"(minimum {_TOKEN_FIXED_BYTES})"
        )

    version = binary[0]
    if version != _TOKEN_VERSION:
        raise ValueError(
            f"ciphertext_token uses unsupported version 0x{version:02x}; "
            f"this service supports 0x{_TOKEN_VERSION:02x}"
        )

    offset = 1
    edek_id = uuid.UUID(bytes=binary[offset:offset + 16]);  offset += 16
    iv      = binary[offset:offset + IV_LENGTH];             offset += IV_LENGTH
    tag     = binary[offset:offset + TAG_LENGTH];            offset += TAG_LENGTH
    ciphertext = binary[offset:]

    if not ciphertext:
        raise ValueError("ciphertext_token contains no ciphertext payload")

    return UnpackedToken(edek_id=edek_id, iv=iv, tag=tag, ciphertext=ciphertext)


def make_fingerprint(iv: bytes, tag: bytes) -> str:
    """
    First 8 bytes of SHA-256(iv || tag) as a 16-char hex string.
    Stored with the EDEK record so decrypt can detect element mix-ups
    before AES-GCM decryption even runs. Not a secret — it's a consistency
    check, not a MAC; the actual authentication is AES-GCM's tag.
    """
    return hashlib.sha256(iv + tag).hexdigest()[:16]


def _make_aad(app_id: str) -> bytes:
    return f"hsm-svc:app_id={app_id}".encode()
