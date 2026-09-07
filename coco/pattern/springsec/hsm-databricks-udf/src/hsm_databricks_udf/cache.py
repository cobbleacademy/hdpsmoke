"""
Per-worker-process DEK cache, mirroring HsmCryptoClient's design
(encryptCacheByName / decryptCacheByEdekId) -- see DATABRICKS_UDF_DESIGN.md §9.

Module-level dicts persist for the lifetime of the Python worker process
PySpark reuses across many rows within a partition, so a DEK is issued or
unwrapped once per distinct dek_name / edek_id, not once per row. No TTL,
unbounded for the process's lifetime -- the same tradeoff already accepted
and documented for the JVM client in AUTHORIZATION.md §1c (client-side DEK
memory exposure); that analysis applies identically here.

Each cache entry stores owner_app_id alongside the raw DEK -- not just for
convenience, but because it's required correctness: owner_app_id is the
AES-GCM AAD, and it is NOT always this worker's own app_id once a
grant-authorized cross-app reuse is in play. See udf.py and
DATABRICKS_UDF_DESIGN.md for the full reasoning (a real, confirmed bug on
the Java side of hsm-core-service until this same round -- see
EncryptionService.ResolvedDek's javadoc there).
"""

from __future__ import annotations

import base64

from . import transport
from .svc_client import SvcClient

# dek_name -> (edek_id, owner_app_id, raw_dek)
_encrypt_cache: dict[str, tuple[str, str, bytearray]] = {}
# edek_id -> (owner_app_id, raw_dek)
_decrypt_cache: dict[str, tuple[str, bytearray]] = {}


def get_or_issue_for_encrypt(dek_name: str, svc_client: SvcClient, private_key) -> tuple[str, str, bytearray]:
    """Returns (edek_id, owner_app_id, raw_dek) for a dek_name, issuing + unwrapping once, then cached."""
    cached = _encrypt_cache.get(dek_name)
    if cached is not None:
        return cached

    result = svc_client.issue_dek(dek_name)
    raw = bytearray(transport.unwrap(base64.b64decode(result.wrapped_dek_b64), private_key))
    entry = (result.edek_id, result.owner_app_id, raw)
    _encrypt_cache[dek_name] = entry
    # A dek_name reused for encrypt always resolves to the same edek_id, so
    # priming the decrypt cache too means a later decrypt of a row just
    # written in this same worker process skips an extra /dek/unwrap call.
    _decrypt_cache[result.edek_id] = (result.owner_app_id, raw)
    return entry


def get_or_unwrap_for_decrypt(edek_id: str, svc_client: SvcClient, private_key) -> tuple[str, bytearray]:
    """Returns (owner_app_id, raw_dek) for a known edek_id, unwrapping once, then cached."""
    cached = _decrypt_cache.get(edek_id)
    if cached is not None:
        return cached

    result = svc_client.unwrap_dek(edek_id)
    raw = bytearray(transport.unwrap(base64.b64decode(result.wrapped_dek_b64), private_key))
    entry = (result.owner_app_id, raw)
    _decrypt_cache[edek_id] = entry
    return entry
