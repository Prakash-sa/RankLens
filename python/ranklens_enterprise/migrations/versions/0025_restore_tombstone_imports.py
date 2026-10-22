"""Audit authenticated deletion-tombstone restore reconciliation."""

from __future__ import annotations
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "0025_restore_tombstone_imports"
down_revision: Union[str, Sequence[str], None] = "0024_integrity_notification_redrives"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "restore_tombstone_imports",
        sa.Column("bundle_sha256", sa.String(64), primary_key=True),
        sa.Column("source_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entry_count", sa.Integer(), nullable=False),
        sa.Column("applied_count", sa.Integer(), nullable=False),
        sa.Column("stale_count", sa.Integer(), nullable=False),
        sa.Column("imported_by", sa.String(128), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_restore_tombstone_import_history",
        "restore_tombstone_imports",
        ["imported_at", "bundle_sha256"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_restore_tombstone_import_history",
        table_name="restore_tombstone_imports",
    )
    op.drop_table("restore_tombstone_imports")
