"""Initial schema: edek_records, app_registrations, app_decrypt_grants.

This is the first Alembic migration in this repo — demo mode bypasses
Alembic entirely (uses metadata.create_all against SQLite), so there is
no prior production migration history to preserve. The schema below
already includes algorithm/encoding/data_classification rather than
adding them in a separate follow-up revision.

Revision ID: 0001
Revises:
Create Date: 2026-06-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

rotation_status_enum = sa.Enum("current", "pending", "rotated", name="rotationstatus")


def upgrade() -> None:
    op.create_table(
        "edek_records",
        sa.Column("edek_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("app_id", sa.String(128), nullable=False),
        sa.Column("edek_blob", sa.Text(), nullable=False),
        sa.Column("kek_version", sa.String(64), nullable=False),
        sa.Column("algorithm", sa.String(32), nullable=False, server_default="AES-256-GCM"),
        sa.Column("encoding", sa.String(16), nullable=False, server_default="utf8"),
        sa.Column("data_classification", sa.String(32), nullable=True),
        sa.Column("rotation_status", rotation_status_enum, nullable=False, server_default="current"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_edek_app_id", "edek_records", ["app_id"])
    op.create_index("idx_edek_rotation_status", "edek_records", ["rotation_status"])
    op.create_index("idx_edek_kek_version", "edek_records", ["kek_version"])
    op.create_index("idx_edek_classification", "edek_records", ["data_classification"])
    op.create_index("idx_edek_created_at", "edek_records", ["created_at"])

    op.create_table(
        "app_registrations",
        sa.Column("app_id", sa.String(128), primary_key=True),
        sa.Column("allowed_scopes", sa.String(512), nullable=False),
        sa.Column("description", sa.String(512), nullable=False, server_default=""),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
    )

    op.create_table(
        "app_decrypt_grants",
        sa.Column("grantee_app_id", sa.String(128), primary_key=True),
        sa.Column("owner_app_id", sa.String(128), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("app_decrypt_grants")
    op.drop_table("app_registrations")
    op.drop_index("idx_edek_created_at", table_name="edek_records")
    op.drop_index("idx_edek_classification", table_name="edek_records")
    op.drop_index("idx_edek_kek_version", table_name="edek_records")
    op.drop_index("idx_edek_rotation_status", table_name="edek_records")
    op.drop_index("idx_edek_app_id", table_name="edek_records")
    op.drop_table("edek_records")
    rotation_status_enum.drop(op.get_bind(), checkfirst=True)
