"""
The keys behind tokenize and surrogate (java/docs/DEIDENTIFICATION.md section 7),
as com.hsm.client.deid.TokenKeyProvider.

Each key is a named DEK issued by hsm-core and pinned by edek_id. For each one
needed, ``KeyProvider``:

1. unwraps it once through ``unwrap`` (``/dek/unwrap``);
2. derives each purpose key with HKDF-SHA256(DEK, salt = none, info = label, L = 32);
3. zeroes its copy of the DEK.

Derived keys are cached until ``close``, which zeroes them. Labels and outputs are
pinned by java/hsm-crypto-client/src/test/resources/deid-test-vectors.json.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import uuid
from typing import Callable

from .errors import DeidError

TOKENIZE_LABEL = "hsm-deid-tokenize-v1"
SURROGATE_LOOKUP_LABEL = "hsm-deid-surrogate-lookup-v1"
SURROGATE_ENC_LABEL = "hsm-deid-surrogate-enc-v1"

KEY_LENGTH = 32
DEK_LENGTH = 32
_HASH_LENGTH = 32


def hmac_sha256(key: bytes, *parts: bytes) -> bytes:
    """HMAC-SHA256 over the concatenation of ``parts``."""
    mac = hmac.new(bytes(key), digestmod=hashlib.sha256)
    for part in parts:
        mac.update(part)
    return mac.digest()


def hkdf_sha256(ikm: bytes, salt: bytes | None, info: bytes | None, length: int) -> bytes:
    """HKDF-SHA256 (RFC 5869). No salt means HashLen zero bytes, as the RFC specifies."""
    if length <= 0 or length > 255 * _HASH_LENGTH:
        raise ValueError("HKDF output length out of range")
    prk = hmac_sha256(salt if salt else bytes(_HASH_LENGTH), ikm)
    okm = bytearray()
    t = b""
    i = 1
    while len(okm) < length:
        t = hmac_sha256(prk, t, info or b"", bytes([i]))
        okm += t[: length - len(okm)]
        i += 1
    return bytes(okm)


class KeyProvider:
    """
    Derived purpose keys, one unwrap per DEK. ``unwrap(edek_id)`` returns the raw DEK
    (bytes or bytearray); production passes the hsm-core client's unwrap.
    Thread-safe; callers must not change the returned keys.
    """

    def __init__(self, unwrap: Callable[[uuid.UUID], bytes]):
        self._unwrap = unwrap
        self._derived: dict[str, bytearray] = {}
        self._lock = threading.Lock()
        self._closed = False

    def key(self, edek_id: uuid.UUID, label: str) -> bytes:
        return self.keys(edek_id, label)[0]

    def keys(self, edek_id: uuid.UUID, *labels: str) -> tuple:
        with self._lock:
            if self._closed:
                raise RuntimeError("KeyProvider is closed")
            out = []
            dek = None
            try:
                for label in labels:
                    cache_key = str(edek_id) + "/" + label
                    cached = self._derived.get(cache_key)
                    if cached is None:
                        if dek is None:
                            dek = self._unwrap_checked(edek_id)
                        cached = bytearray(hkdf_sha256(bytes(dek), None, label.encode("utf-8"), KEY_LENGTH))
                        self._derived[cache_key] = cached
                    out.append(bytes(cached))
                return tuple(out)
            finally:
                if isinstance(dek, bytearray):
                    dek[:] = bytes(len(dek))

    def _unwrap_checked(self, edek_id: uuid.UUID) -> bytearray:
        try:
            raw = self._unwrap(edek_id)
        except Exception as e:  # noqa: BLE001 - reported without the key material
            raise DeidError.bad_value("could not unwrap key edek_id=" + str(edek_id) + ": " + str(e)) from e
        if raw is None or len(raw) != DEK_LENGTH:
            raise DeidError.bad_value("could not unwrap key edek_id=" + str(edek_id) + ": unexpected DEK length")
        dek = bytearray(raw)
        if isinstance(raw, bytearray):
            raw[:] = bytes(len(raw))
        return dek

    def close(self) -> None:
        """Zeroes every derived key. Idempotent."""
        with self._lock:
            self._closed = True
            for k in self._derived.values():
                k[:] = bytes(len(k))
            self._derived.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
