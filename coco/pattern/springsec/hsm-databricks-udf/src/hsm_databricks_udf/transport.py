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
    """Parse a PEM-encoded ('-----BEGIN PUBLIC KEY-----...') RSA public key, as stored in app_registrations.public_key_pem."""
    if isinstance(pem, str):
        pem = pem.encode()
    key = serialization.load_pem_public_key(pem)
    if not isinstance(key, rsa.RSAPublicKey):
        raise ValueError("public_key_pem is not an RSA key")
    return key


def parse_private_key_pem(pem: str | bytes, password: bytes | None = None) -> rsa.RSAPrivateKey:
    """Parse a PEM-encoded PKCS#8 RSA private key -- this package's own keypair, never sent to hsm-core-service."""
    if isinstance(pem, str):
        pem = pem.encode()
    key = serialization.load_pem_private_key(pem, password=password)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("private key PEM is not an RSA key")
    return key
