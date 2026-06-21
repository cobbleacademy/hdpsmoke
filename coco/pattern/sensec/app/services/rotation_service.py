"""
KEK rotation service.

Strategy:
  1. Create a new KEK version in Azure Key Vault (or it was auto-rotated by AKV policy).
  2. Page through all EDEK records with status=current.
  3. For each: unwrap with old version → re-wrap with new version → update record.
  4. Mark old records as 'rotated' only after successful re-wrap.
  5. Old KEK version remains in AKV (disabled) so in-flight decrypts still work
     during the rotation window.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.audit.logger import audit_log
from app.crypto.kek_client import KEKClient
from app.models.edek_record import EDEKRecord, RotationStatus
from app.models.schemas import RotateKEKResponse

log = structlog.get_logger("rotation_service")

PAGE_SIZE = 200


async def rotate_kek(
    kek_client: KEKClient,
    session_factory: async_sessionmaker,
    triggered_by: str = "scheduler",
) -> RotateKEKResponse:
    new_version = await kek_client.get_current_kek_version()
    log.info("kek_rotation_started", new_kek_version=new_version, triggered_by=triggered_by)

    total = 0
    offset = 0

    while True:
        async with session_factory() as session:
            rows = (
                await session.scalars(
                    select(EDEKRecord)
                    .where(EDEKRecord.rotation_status == RotationStatus.current)
                    .where(EDEKRecord.kek_version != new_version)
                    .order_by(EDEKRecord.created_at)
                    .limit(PAGE_SIZE)
                    .offset(offset)
                )
            ).all()

            if not rows:
                break

            for record in rows:
                await _rewrap_record(record, new_version, kek_client, session)
                total += 1

            await session.commit()
            offset += PAGE_SIZE

    audit_log(
        "kek_rotation_completed",
        new_kek_version=new_version,
        records_rotated=total,
        triggered_by=triggered_by,
        status="success",
    )

    return RotateKEKResponse(new_kek_version=new_version, records_queued=total)


async def _rewrap_record(
    record: EDEKRecord,
    new_version: str,
    kek_client: KEKClient,
    session: AsyncSession,
) -> None:
    old_edek = base64.b64decode(record.edek_blob)
    dek_bytes = await kek_client.unwrap_dek(old_edek, record.kek_version)
    new_edek, _ = await kek_client.wrap_dek(dek_bytes)

    record.edek_blob = base64.b64encode(new_edek).decode()
    record.kek_version = new_version
    record.rotation_status = RotationStatus.current
    record.rotated_at = datetime.now(timezone.utc)
