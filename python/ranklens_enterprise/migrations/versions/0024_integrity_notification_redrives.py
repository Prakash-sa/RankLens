"""Audit operator redrives of quarantined integrity notifications."""

from __future__ import annotations
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "0024_integrity_notification_redrives"
down_revision: Union[str, Sequence[str], None] = "0023_integrity_notification_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "integrity_notification_redrives",
        sa.Column("redrive_id", sa.String(36), primary_key=True),
        sa.Column("notification_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("cluster_id", sa.String(128), nullable=False),
        sa.Column("requested_by_credential_id", sa.String(128), nullable=False),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_integrity_notification_redrive_history",
        "integrity_notification_redrives",
        ["notification_id", "requested_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_integrity_notification_redrive_history",
        table_name="integrity_notification_redrives",
    )
    op.drop_table("integrity_notification_redrives")
