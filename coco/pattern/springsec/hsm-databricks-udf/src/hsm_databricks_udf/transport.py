"""
RSA-OAEP-256 wrap/unwrap of a raw DEK for transport between hsm-core-service
and this package, matching com.hsm.core.crypto.TransportWrapper exactly:
transformation "RSA/ECB/OAEPWithSHA-256AndMGF1Padding" == OAEP with SHA-256
for both the hash and the MGF1 mask generation function, no label.

FIPS 140 scope note (see DATABRICKS_UDF_DESIGN.md §8): this is the one
narrower, still-open question in the design -- whether the once-per-DEK
unwrap operation itself needs a FIPS-validated module, or falls outside the
custody boundary the same way the repeated per-row AES-GCM work does. This
module implements it with the standard `cryptography` package pending that
confirmation; if the answer comes back "yes, unwrap needs a validated
module," only this one function needs to change, not the bulk crypto path.

hsm-core-service only ever calls wrap() (server-side, its private key never
leaves it). This package only ever calls unwrap() -- its own private key
never leaves the Databricks worker process, by the same design.
"""

from __future__ import annotations

import base64
import binascii

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

_OAEP_PADDING = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


def wrap(dek: bytes, public_key: rsa.RSAPublicKey) -> bytes:
    """RSA-OAEP-256 wrap -- included for symmetry and round-trip testing; hsm-core-service does this server-side today, not this package."""
    return public_key.encrypt(dek, _OAEP_PADDING)


def unwrap(wrapped_dek: bytes, private_key: rsa.RSAPrivateKey) -> bytes:
    """RSA-OAEP-256 unwrap -- this package's actual call path, once per DEK (see cache.py)."""
    return private_key.decrypt(wrapped_dek, _OAEP_PADDING)


def parse_public_key_pem(pem: str | bytes) -> rsa.RSAPublicKey:
    """Parse an RSA public key, as stored in app_registrations.public_key_pem -- either raw PEM or base64-encoded PEM, see _normalize_key_material."""
    key = serialization.load_pem_public_key(_normalize_key_material(pem))
    if not isinstance(key, rsa.RSAPublicKey):
        raise ValueError("public_key_pem is not an RSA key")
    return key


def parse_private_key_pem(pem: str | bytes, password: bytes | None = None) -> rsa.RSAPrivateKey:
    """Parse this package's own RSA private key -- either raw PEM or base64-encoded PEM, see _normalize_key_material. Never sent to hsm-core-service."""
    key = serialization.load_pem_private_key(_normalize_key_material(pem), password=password)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("private key PEM is not an RSA key")
    return key


def _normalize_key_material(pem: str | bytes) -> bytes:
    """
    Accepts either raw PEM text ('-----BEGIN ...-----...') or that same PEM
    base64-encoded as a single line -- a common, deliberately supported way
    to store multi-line key material in a secret manager's plain string
    field (e.g. Databricks `secrets put-secret --string-value "$(base64 -w0
    key.pem)"`), which sidesteps every newline-mangling risk a raw multi-line
    upload is exposed to across different CLI/UI paths. Detected by whether
    the PEM marker is present -- if not, the value is assumed to be
    base64-encoded PEM and decoded before parsing.
    """
    if isinstance(pem, str):
        pem = pem.encode()
    if b"-----BEGIN" in pem:
        return pem
    try:
        decoded = base64.b64decode(pem, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError(
            "key material is neither raw PEM (no '-----BEGIN' marker found) "
            f"nor valid base64: {e}"
        ) from e
    if b"-----BEGIN" not in decoded:
        raise ValueError("base64-decoded key material does not contain a PEM '-----BEGIN' marker")
    return decoded
