"""
IV generation — 96-bit (12 bytes) random IV per NIST SP 800-38D §8.2.
Each call to generate() produces a fresh cryptographically secure value.

Vendored from app/crypto/iv_factory.py — see DATABRICKS_UDF_DESIGN.md §5 for
why this is copied rather than imported: keeps this package's dependency
footprint to just `cryptography` + stdlib, decoupled from app/'s full
FastAPI/SQLAlchemy tree. Keep byte-for-byte identical to the source if that
file changes.
"""

import secrets


IV_LENGTH_BYTES = 12  # 96-bit recommended for AES-GCM


def generate() -> bytes:
    return secrets.token_bytes(IV_LENGTH_BYTES)
