"""
IV generation — 96-bit (12 bytes) random IV per NIST SP 800-38D §8.2.
Each call to generate() produces a fresh cryptographically secure value.
"""

import secrets


IV_LENGTH_BYTES = 12  # 96-bit recommended for AES-GCM


def generate() -> bytes:
    return secrets.token_bytes(IV_LENGTH_BYTES)
