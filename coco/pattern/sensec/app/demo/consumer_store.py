"""
Simulates a CONSUMING application's own database — DEMO MODE ONLY.

This is the other half of the architecture that's easy to lose sight of:
this service never stores ciphertext. The calling app (here, payments-svc)
owns its own schema, calls /encrypt for the sensitive column only, and
persists the result in its own table, right next to its non-sensitive
columns. This service comes back into the picture only when that app needs
to decrypt later.

Of the full encrypt response, exactly four fields are required to decrypt
later — edek_id, iv_b64, ciphertext_b64, tag_b64 — because those are the
entire contents of a DecryptRequest. owner_app_id/algorithm/encoding are
recoverable from the EDEK record server-side, so a consumer doesn't have to
persist them, though some teams do anyway purely for local debugging
without a round trip. This table stores only the required four.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Text, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ConsumerBase(DeclarativeBase):
    pass


class ConsumerAccount(ConsumerBase):
    """payments-svc's own customer_accounts table — not part of this service's schema."""
    __tablename__ = "consumer_customer_accounts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Non-sensitive — stored as plain columns, exactly like any normal app data
    customer_name: Mapped[str] = mapped_column(String(128), nullable=False)
    email: Mapped[str] = mapped_column(String(256), nullable=False)

    # Sensitive — account_number itself is NEVER stored. Only the ciphertext
    # and the minimum metadata required to ask this service to decrypt it.
    edek_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    iv_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    ciphertext_b64: Mapped[str] = mapped_column(Text, nullable=False)
    tag_b64: Mapped[str] = mapped_column(String(64), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
