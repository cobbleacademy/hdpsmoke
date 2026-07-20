from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Enum, Index, String, Text, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import enum

class Base(DeclarativeBase):
    pass


class RotationStatus(str, enum.Enum):
    current = "current"
    pending = "pending"
    rotated = "rotated"


class EDEKRecord(Base):
    __tablename__ = "edek_records"

    edek_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    app_id: Mapped[str] = mapped_column(String(128), nullable=False)
    edek_blob: Mapped[str] = mapped_column(Text, nullable=False)      # base64-encoded wrapped DEK
    kek_version: Mapped[str] = mapped_column(String(64), nullable=False)
    algorithm: Mapped[str] = mapped_column(String(32), nullable=False, default="AES-256-GCM")
    encoding: Mapped[str] = mapped_column(String(16), nullable=False, default="utf8")
    data_classification: Mapped[str | None] = mapped_column(String(32), nullable=True)
    rotation_status: Mapped[RotationStatus] = mapped_column(
        Enum(RotationStatus, name="rotation_status", schema=os.environ.get("DB_SCHEMA") or None),
        nullable=False,
        default=RotationStatus.current,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # First 8 bytes of SHA-256(iv || tag) encoded as 16 hex chars.
    # Nullable so pre-existing records (written before this column existed) still decrypt.
    # Lets pre-flight catch element mix-ups before AES-GCM even runs.
    fingerprint: Mapped[str | None] = mapped_column(String(16), nullable=True)

    __table_args__ = (
        Index("idx_edek_app_id", "app_id"),
        Index("idx_edek_rotation_status", "rotation_status"),
        Index("idx_edek_kek_version", "kek_version"),
        Index("idx_edek_classification", "data_classification"),
        Index("idx_edek_created_at", "created_at"),
        {"schema": os.environ.get("DB_SCHEMA") or None},
    )
