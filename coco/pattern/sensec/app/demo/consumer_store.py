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

from datetime import datetime, timezone

from sqlalchemy import DateTime, String
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

    # Sensitive — account_number itself is NEVER stored.
    # The entire encrypt response is packed into one opaque token: store it,
    # echo it back to decrypt. No field juggling, no mix-up risk.
    # Format: "v1.<base64url(version|edek_id|iv|tag|ciphertext)>"
    # Size:   ~128 chars for short fields like account numbers.
    ciphertext_token: Mapped[str] = mapped_column(String(512), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
