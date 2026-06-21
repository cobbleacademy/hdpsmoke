"""
Demo-only endpoints — exposed only when DEMO_MODE=true.
Lets the static UI populate an app picker and poll the audit feed
without requiring a real SIEM connection.
"""

from __future__ import annotations

import base64
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import get_recent_events
from app.demo.consumer_store import ConsumerAccount
from app.demo.mock_jwt_validator import DEMO_SCOPES, DEMO_TOKENS
from app.dependencies import get_app_registry, get_db_session, get_kek_client
from app.models.edek_record import EDEKRecord
from app.models.schemas import (
    ConsumerAccountCreateRequest,
    ConsumerAccountResponse,
    ConsumerRevealRequest,
    ConsumerRevealResponse,
    DecryptRequest,
    EncryptRequest,
)
from app.services import decryption_service, encryption_service

router = APIRouter(prefix="/demo", tags=["demo"])

# The consumer simulation always encrypts as this app — it owns the table.
CONSUMER_OWNER_APP_ID = "payments-svc"


@router.get("/apps")
async def list_demo_apps():
    apps = []
    for token, claims in DEMO_TOKENS.items():
        app_id = claims["app_id"]
        apps.append({
            "app_id": app_id,
            "token": token,
            "scopes": DEMO_SCOPES.get(app_id, []),
        })
    return {"apps": apps}


@router.get("/audit-log")
async def recent_audit_log(limit: int = 50):
    return {"events": get_recent_events(limit)}


@router.get("/hsm-state")
async def hsm_state(kek_client=Depends(get_kek_client)):
    """
    Simulated HSM introspection — version metadata only, never key bytes.
    A real Azure Managed HSM has no equivalent call; this exists purely
    so the demo UI can visualize what "rotation" actually changes.
    """
    if not hasattr(kek_client, "get_state"):
        return {"current_version": await kek_client.get_current_kek_version(), "versions": []}
    return kek_client.get_state()


@router.get("/edek-records")
async def recent_edek_records(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: int = 20,
):
    """
    Latest EDEK store rows, newest first. The wrapped key blob is shown
    truncated — it's ciphertext under the KEK either way, but truncating
    keeps the table readable and avoids implying the value is meaningful
    to display in full.
    """
    # idx_edek_created_at backs this ORDER BY ... LIMIT — without it this
    # degrades to a full table scan as records accumulate. The PK lookup
    # the actual decrypt path uses (session.get(EDEKRecord, edek_id)) is
    # unaffected by table size either way; only admin/listing queries
    # like this one need an index to stay fast at scale.
    rows = (
        await session.scalars(
            select(EDEKRecord).order_by(EDEKRecord.created_at.desc()).limit(limit)
        )
    ).all()
    return {
        "records": [
            {
                "edek_id": str(r.edek_id),
                "app_id": r.app_id,
                "kek_version": r.kek_version,
                "algorithm": r.algorithm,
                "encoding": r.encoding,
                "data_classification": r.data_classification,
                "rotation_status": r.rotation_status.value,
                "edek_blob_preview": r.edek_blob[:24] + "…",
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "rotated_at": r.rotated_at.isoformat() if r.rotated_at else None,
            }
            for r in rows
        ]
    }


# ── Consumer application simulation ──────────────────────────────────────────
#
# This block simulates payments-svc's OWN database, not this service's.
# In a real deployment these calls to encryption_service/decryption_service
# would be an HTTP round trip from payments-svc's backend to this service —
# here they're invoked in-process purely to avoid a fragile self-referential
# network call inside the demo, but the behavior (including the grant check
# inside decrypt) is identical either way.

@router.post("/consumer/accounts", response_model=ConsumerAccountResponse, status_code=201)
async def create_consumer_account(
    body: ConsumerAccountCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    kek_client=Depends(get_kek_client),
):
    enc = await encryption_service.encrypt(
        request=EncryptRequest(plaintext=body.account_number, data_classification="pci"),
        app_id=CONSUMER_OWNER_APP_ID,
        caller_sub="demo-consumer-app",
        kek_client=kek_client,
        session=session,
    )

    # Exactly the four fields a DecryptRequest needs — nothing more, and
    # critically, never the plaintext account_number itself.
    record = ConsumerAccount(
        customer_name=body.customer_name,
        email=body.email,
        edek_id=enc.edek_id,
        iv_b64=enc.iv_b64,
        ciphertext_b64=enc.ciphertext_b64,
        tag_b64=enc.tag_b64,
    )
    session.add(record)
    await session.commit()

    return ConsumerAccountResponse(
        id=record.id,
        customer_name=record.customer_name,
        email=record.email,
        account_number_ciphertext_preview=record.ciphertext_b64[:20] + "…",
        edek_id=record.edek_id,
        iv_b64=record.iv_b64,
        tag_b64=record.tag_b64,
        created_at=record.created_at.isoformat(),
    )


@router.get("/consumer/accounts")
async def list_consumer_accounts(session: Annotated[AsyncSession, Depends(get_db_session)]):
    rows = (
        await session.scalars(
            select(ConsumerAccount).order_by(ConsumerAccount.created_at.desc())
        )
    ).all()
    return {
        "accounts": [
            ConsumerAccountResponse(
                id=r.id,
                customer_name=r.customer_name,
                email=r.email,
                account_number_ciphertext_preview=r.ciphertext_b64[:20] + "…",
                edek_id=r.edek_id,
                iv_b64=r.iv_b64,
                tag_b64=r.tag_b64,
                created_at=r.created_at.isoformat() if r.created_at else "",
            )
            for r in rows
        ]
    }


@router.post("/consumer/accounts/{account_id}/reveal", response_model=ConsumerRevealResponse)
async def reveal_consumer_account(
    account_id: int,
    body: ConsumerRevealRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    kek_client=Depends(get_kek_client),
    app_registry=Depends(get_app_registry),
):
    record = await session.get(ConsumerAccount, account_id)
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found")

    # decryption_service.decrypt raises HTTPException itself on a missing
    # grant (403) or tag-verification failure (422) — no extra handling here.
    dec = await decryption_service.decrypt(
        request=DecryptRequest(
            edek_id=record.edek_id,
            iv_b64=record.iv_b64,
            ciphertext_b64=record.ciphertext_b64,
            tag_b64=record.tag_b64,
        ),
        app_id=body.reveal_as,
        caller_sub="demo-consumer-ui",
        kek_client=kek_client,
        session=session,
        app_registry=app_registry,
    )

    # Decrypted value is returned to the caller and never written back to
    # the table — the row on disk stays ciphertext-only either way.
    return ConsumerRevealResponse(id=account_id, account_number=dec.plaintext)
