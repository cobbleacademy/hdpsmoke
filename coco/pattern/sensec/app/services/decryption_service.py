from __future__ import annotations

import base64

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import audit_log
from app.auth.app_registry import AppRegistry
from app.auth.pbac_client import NullPBACClient, PBACClient
from app.crypto import dek_manager
from app.crypto.dek_cache import DEKCache, NullDEKCache
from app.crypto.dek_manager import IV_LENGTH, TAG_LENGTH, make_fingerprint, unpack_token
from app.crypto.kek_client import KEKClient
from app.models.edek_record import EDEKRecord
from app.models.schemas import DecryptRequest, DecryptResponse


async def decrypt(
    request: DecryptRequest,
    app_id: str,
    caller_sub: str,
    caller_scopes: list[str],
    kek_client: KEKClient,
    session: AsyncSession,
    app_registry: AppRegistry,
    caller_ip: str = "",
    dek_cache: DEKCache | NullDEKCache | None = None,
    pbac_client: PBACClient | NullPBACClient | None = None,
) -> DecryptResponse:
    # ── Resolve inputs: token path (preferred) or legacy individual fields ────
    if request.ciphertext_token is not None:
        try:
            unpacked = unpack_token(request.ciphertext_token)
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc))
        resolved_edek_id = unpacked.edek_id
        resolved_iv  = unpacked.iv
        resolved_tag = unpacked.tag
        resolved_ct  = unpacked.ciphertext
    else:
        # Legacy path — all four fields are guaranteed present by schema validator
        resolved_edek_id = request.edek_id          # type: ignore[assignment]
        resolved_iv  = base64.b64decode(request.iv_b64)           # type: ignore[arg-type]
        resolved_tag = base64.b64decode(request.tag_b64)          # type: ignore[arg-type]
        resolved_ct  = base64.b64decode(request.ciphertext_b64)   # type: ignore[arg-type]

    record: EDEKRecord | None = await session.get(EDEKRecord, resolved_edek_id)

    edek_id_str = str(resolved_edek_id)

    if record is None:
        _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip, "edek_not_found",
                    end_user_id=request.end_user_id)
        raise HTTPException(status.HTTP_404_NOT_FOUND, "EDEK not found")

    owner_app_id = record.app_id
    # Governance SPN bypasses the per-record grant check — it may decrypt any record
    # for audit purposes. All other callers must have an explicit grant.
    if "governance" not in caller_scopes:
        if not await app_registry.is_granted(grantee_app_id=app_id, owner_app_id=owner_app_id):
            _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip,
                         "no_grant_for_owner", owner_app_id=owner_app_id, end_user_id=request.end_user_id)
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Access denied")

    if pbac_client is not None and request.end_user_id:
        permitted = await pbac_client.check(
            end_user_id=request.end_user_id,
            action="decrypt",
            data_classification=record.data_classification,
            context={"app_id": app_id, "owner_app_id": owner_app_id, "caller_ip": caller_ip},
        )
        if not permitted:
            _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip,
                        "pbac_denied", end_user_id=request.end_user_id)
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Access denied by policy")

    # ── Pre-flight: fixed-size parameter checks (legacy path only) ────────────
    # Token path: iv/tag sizes are guaranteed by pack_token — no check needed.
    # Legacy path: client-supplied base64 fields must be validated.
    iv_bytes  = resolved_iv
    tag_bytes = resolved_tag
    ct_bytes  = resolved_ct

    if request.ciphertext_token is None:
        # Legacy path: validate sizes that pack_token guarantees structurally
        if len(iv_bytes) != IV_LENGTH:
            _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip,
                        "invalid_iv_length", end_user_id=request.end_user_id)
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                f"iv_b64 is invalid: decoded to {len(iv_bytes)} bytes, "
                f"AES-GCM requires exactly {IV_LENGTH} bytes (96-bit nonce)",
            )
        if len(tag_bytes) != TAG_LENGTH:
            _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip,
                        "invalid_tag_length", end_user_id=request.end_user_id)
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                f"tag_b64 is invalid: decoded to {len(tag_bytes)} bytes, "
                f"AES-GCM requires exactly {TAG_LENGTH} bytes (128-bit tag)",
            )

    # ── Pre-flight: fingerprint cross-check ──────────────────────────────────
    # Only runs when the record has a fingerprint (records written before this
    # feature was added have fingerprint=None and skip this check).
    if record.fingerprint is not None:
        expected = make_fingerprint(iv_bytes, tag_bytes)
        if expected != record.fingerprint:
            _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip,
                        "element_mismatch", end_user_id=request.end_user_id)
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "iv_b64, ciphertext_b64, or tag_b64 do not belong to this edek_id. "
                "These fields must all come from the same encrypt response — "
                "mixing elements across different responses is not permitted.",
            )

    edek_bytes = base64.b64decode(record.edek_blob)

    cached_dek = await dek_cache.get(edek_id_str) if dek_cache else None
    if cached_dek is not None:
        dek = bytearray(cached_dek)
    else:
        raw = await kek_client.unwrap_dek(edek_bytes, record.kek_version)
        dek = bytearray(raw)
        if dek_cache:
            await dek_cache.set(edek_id_str, bytes(raw), record.data_classification)

    try:
        plaintext = dek_manager.decrypt(
            ciphertext=ct_bytes,
            tag=tag_bytes,
            iv=iv_bytes,
            dek=dek,
            app_id=owner_app_id,   # AAD must match what the owner used at encrypt time
        )
    except InvalidTag:
        _audit_fail("decrypt", app_id, caller_sub, edek_id_str, caller_ip, "tag_verification_failed",
                    end_user_id=request.end_user_id)
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "Ciphertext authentication failed: the data may be corrupt or tampered with",
        )
    finally:
        dek_manager.zero_dek(dek)

    audit_log(
        "decrypt",
        app_id=app_id,
        owner_app_id=owner_app_id,
        sub=caller_sub,
        end_user_id=request.end_user_id,
        edek_id=edek_id_str,
        kek_version=record.kek_version,
        caller_ip=caller_ip,
        status="success",
    )

    return DecryptResponse(
        plaintext=plaintext.decode(),
        owner_app_id=owner_app_id,
        algorithm=record.algorithm,
        encoding=record.encoding,
    )


def _audit_fail(event: str, app_id: str, sub: str, edek_id: str, ip: str, reason: str, **extra) -> None:
    audit_log(event, app_id=app_id, sub=sub, edek_id=edek_id, caller_ip=ip, status="failure", reason=reason, **extra)
