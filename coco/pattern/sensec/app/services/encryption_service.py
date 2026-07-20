from __future__ import annotations

import base64
import uuid

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import audit_log
from app.auth.pbac_client import NullPBACClient, PBACClient
from app.crypto import dek_manager
from app.crypto.dek_manager import make_fingerprint, pack_token
from app.crypto.kek_client import KEKClient
from app.models.edek_record import EDEKRecord, RotationStatus
from app.models.schemas import EncryptRequest, EncryptResponse


async def encrypt(
    request: EncryptRequest,
    app_id: str,
    caller_sub: str,
    kek_client: KEKClient,
    session: AsyncSession,
    caller_ip: str = "",
    pbac_client: PBACClient | NullPBACClient | None = None,
) -> EncryptResponse:
    if pbac_client is not None and request.end_user_id:
        permitted = await pbac_client.check(
            end_user_id=request.end_user_id,
            action="encrypt",
            data_classification=request.data_classification,
            context={"app_id": app_id, "caller_ip": caller_ip},
        )
        if not permitted:
            audit_log(
                "encrypt",
                app_id=app_id,
                sub=caller_sub,
                end_user_id=request.end_user_id,
                caller_ip=caller_ip,
                status="failure",
                reason="pbac_denied",
            )
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Access denied by policy")

    dek = dek_manager.generate_dek()
    try:
        result = dek_manager.encrypt(request.plaintext.encode(), dek, app_id)
        edek_bytes, kek_version = await kek_client.wrap_dek(bytes(dek))
    finally:
        dek_manager.zero_dek(dek)

    record = EDEKRecord(
        edek_id=uuid.uuid4(),
        app_id=app_id,
        edek_blob=base64.b64encode(edek_bytes).decode(),
        kek_version=kek_version,
        algorithm=dek_manager.ALGORITHM,
        encoding=request.encoding,
        data_classification=request.data_classification,
        rotation_status=RotationStatus.current,
        fingerprint=make_fingerprint(result.iv, result.tag),
    )
    session.add(record)
    await session.commit()

    audit_log(
        "encrypt",
        app_id=app_id,
        sub=caller_sub,
        end_user_id=request.end_user_id,
        edek_id=str(record.edek_id),
        kek_version=kek_version,
        data_classification=request.data_classification,
        caller_ip=caller_ip,
        context=request.context,
        status="success",
    )

    return EncryptResponse(
        ciphertext_token=pack_token(record.edek_id, result.iv, result.tag, result.ciphertext),
        edek_id=record.edek_id,
        owner_app_id=app_id,
        algorithm=dek_manager.ALGORITHM,
        encoding=request.encoding,
        kek_version=kek_version,
        # deprecated individual fields — kept for backward compatibility
        iv_b64=base64.b64encode(result.iv).decode(),
        ciphertext_b64=base64.b64encode(result.ciphertext).decode(),
        tag_b64=base64.b64encode(result.tag).decode(),
    )
