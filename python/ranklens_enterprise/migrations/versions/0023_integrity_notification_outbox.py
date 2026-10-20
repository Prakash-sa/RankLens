"""Add a leased integrity-incident notification outbox."""

from __future__ import annotations
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "0023_integrity_notification_outbox"
down_revision: Union[str, Sequence[str], None] = "0022_integrity_incidents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "integrity_notifications",
        sa.Column("notification_id", sa.String(36), primary_key=True),
        sa.Column("incident_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("cluster_id", sa.String(128), nullable=False),
        sa.Column("occurrence", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("state", sa.String(24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("incident_id", "occurrence", "event_type", name="uq_integrity_notification_event"),
    )
    op.create_index("ix_integrity_notification_due", "integrity_notifications", ["state", "next_attempt_at", "notification_id"])
    op.create_index("ix_integrity_notification_scope", "integrity_notifications", ["tenant_id", "cluster_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_integrity_notification_scope", table_name="integrity_notifications")
    op.drop_index("ix_integrity_notification_due", table_name="integrity_notifications")
    op.drop_table("integrity_notifications")
