"""Add fingerprint column to edek_records for element mix-up detection.

Stores first 8 bytes of SHA-256(iv || tag) as a 16-char hex string.
Nullable so pre-existing records (written before this column existed)
continue to decrypt normally — the pre-flight fingerprint check is
skipped when fingerprint IS NULL.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "edek_records",
        sa.Column("fingerprint", sa.String(16), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("edek_records", "fingerprint")
