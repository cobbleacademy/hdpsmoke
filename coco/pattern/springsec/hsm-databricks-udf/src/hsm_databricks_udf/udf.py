"""
hsm_encrypt / hsm_decrypt -- the actual Unity Catalog Function entry points
(see sql/create_functions.sql). Ties together config, svc_client, transport,
cache, and dek_manager exactly as sketched in DATABRICKS_UDF_DESIGN.md §6/§9.

Module-level Config/SvcClient/private_key are initialized lazily, once per
Python worker process, and reused across every row PySpark hands this worker
-- the same "issue/unwrap once, encrypt/decrypt many rows locally" shape
HsmCryptoClient already uses on the JVM side.
"""

from __future__ import annotations

import uuid

from . import cache, dek_manager, transport
from .config import Config
from .svc_client import SvcClient

_config: Config | None = None
_svc_client: SvcClient | None = None
_private_key = None


def _ensure_initialized() -> tuple[SvcClient, object]:
    global _config, _svc_client, _private_key
    if _config is None:
        _config = Config.from_env()
        _svc_client = SvcClient(_config)
        _private_key = transport.parse_private_key_pem(_config.private_key_pem)
    return _svc_client, _private_key


def encrypt(plaintext: str, dek_name: str, data_classification: str | None = None) -> str:
    """
    Returns a ciphertext_token in hsm-core-service's own wire format --
    decryptable through the ordinary /decrypt endpoint with zero awareness
    of how it was produced, same guarantee CoreBulkFileInteropTest holds the
    JVM bulk client to.

    dek_name is required (unlike /encrypt's optional dek_name): this UDF's
    whole point is bulk throughput via DEK reuse -- a fresh DEK per row would
    mean a network round-trip per row, defeating the reason this exists.
    """
    if not dek_name or not dek_name.strip():
        raise ValueError("dek_name is required -- hsm_encrypt is a bulk-reuse path, not a per-row /encrypt substitute")

    svc_client, private_key = _ensure_initialized()
    edek_id, owner_app_id, dek = cache.get_or_issue_for_encrypt(dek_name, svc_client, private_key)

    # owner_app_id is the record's true, permanent owner as returned by
    # /dek/issue -- NOT necessarily this worker's own app_id, once a
    # grant-authorized cross-app reuse is in play. It MUST be used as the
    # AES-GCM AAD, exactly matching what EncryptionService uses server-side
    # for /encrypt -- using this worker's own app_id instead would silently
    # produce a token nothing could ever decrypt again (a real, confirmed bug
    # on the Java side of hsm-core-service, fixed the same round this package
    # was built -- see EncryptionService.ResolvedDek's javadoc).
    result = dek_manager.encrypt(plaintext.encode("utf-8"), dek, owner_app_id)
    return dek_manager.pack_token(uuid.UUID(edek_id), result.iv, result.tag, result.ciphertext)


def decrypt(ciphertext_token: str) -> str:
    """
    Decrypts a token produced by hsm-core-service's /encrypt (or /encrypt/batch,
    or this package's own encrypt()) -- zero awareness of which produced it,
    same as /decrypt itself.
    """
    unpacked = dek_manager.unpack_token(ciphertext_token)
    edek_id = str(unpacked.edek_id)

    svc_client, private_key = _ensure_initialized()
    owner_app_id, dek = cache.get_or_unwrap_for_decrypt(edek_id, svc_client, private_key)

    plaintext = dek_manager.decrypt(unpacked.ciphertext, unpacked.tag, unpacked.iv, dek, owner_app_id)
    return plaintext.decode("utf-8")
