from __future__ import annotations

import base64
import uuid

from cryptography.exceptions import InvalidTag
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import audit_log
from app.auth.app_registry import AppRegistry
from app.crypto import dek_manager
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
) -> DecryptResponse:
    record: EDEKRecord | None = await session.get(EDEKRecord, request.edek_id)

    if record is None:
        _audit_fail("decrypt", app_id, caller_sub, str(request.edek_id), caller_ip, "edek_not_found",
                    end_user_id=request.end_user_id)
        raise HTTPException(status.HTTP_404_NOT_FOUND, "EDEK not found")

    owner_app_id = record.app_id
    # Governance SPN bypasses the per-record grant check — it may decrypt any record
    # for audit purposes. All other callers must have an explicit grant.
    if "governance" not in caller_scopes:
        if not await app_registry.is_granted(grantee_app_id=app_id, owner_app_id=owner_app_id):
            # Do not reveal whether the EDEK exists for a different app
            _audit_fail("decrypt", app_id, caller_sub, str(request.edek_id), caller_ip,
                         "no_grant_for_owner", owner_app_id=owner_app_id, end_user_id=request.end_user_id)
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Access denied")

    edek_bytes = base64.b64decode(record.edek_blob)
    dek = bytearray(await kek_client.unwrap_dek(edek_bytes, record.kek_version))

    try:
        plaintext = dek_manager.decrypt(
            ciphertext=base64.b64decode(request.ciphertext_b64),
            tag=base64.b64decode(request.tag_b64),
            iv=base64.b64decode(request.iv_b64),
            dek=dek,
            app_id=owner_app_id,   # AAD must match what the owner used at encrypt time
        )
    except InvalidTag:
        _audit_fail("decrypt", app_id, caller_sub, str(request.edek_id), caller_ip, "tag_verification_failed",
                    end_user_id=request.end_user_id)
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Ciphertext authentication failed")
    finally:
        dek_manager.zero_dek(dek)

    audit_log(
        "decrypt",
        app_id=app_id,
        owner_app_id=owner_app_id,
        sub=caller_sub,
        end_user_id=request.end_user_id,
        edek_id=str(request.edek_id),
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
