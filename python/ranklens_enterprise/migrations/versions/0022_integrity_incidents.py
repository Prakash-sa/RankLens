"""Add durable protected-object integrity incidents."""

from __future__ import annotations
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "0022_integrity_incidents"
down_revision: Union[str, Sequence[str], None] = "0021_retention_policy_migration_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "integrity_incidents",
        sa.Column("incident_id", sa.String(36), primary_key=True),
        sa.Column("candidate_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("cluster_id", sa.String(128), nullable=False),
        sa.Column("attempt_id", sa.String(128), nullable=True),
        sa.Column("object_key", sa.String(512), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("category", sa.String(24), nullable=False),
        sa.Column("state", sa.String(24), nullable=False),
        sa.Column("details", sa.String(512), nullable=False),
        sa.Column("occurrences", sa.BigInteger(), nullable=False),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by_credential_id", sa.String(128), nullable=True),
        sa.Column("resolution", sa.String(512), nullable=True),
        sa.UniqueConstraint("candidate_id", "tenant_id", "cluster_id", "category", name="uq_integrity_incident_identity"),
    )
    op.create_index("ix_integrity_incident_queue", "integrity_incidents", ["tenant_id", "cluster_id", "state", "last_detected_at"])
    op.create_table(
        "integrity_incident_resolutions",
        sa.Column("resolution_id", sa.String(36), primary_key=True),
        sa.Column("incident_id", sa.String(36), nullable=False),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("cluster_id", sa.String(128), nullable=False),
        sa.Column("resolved_by_credential_id", sa.String(128), nullable=False),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_integrity_incident_resolution_history", "integrity_incident_resolutions", ["incident_id", "resolved_at"])


def downgrade() -> None:
    op.drop_index("ix_integrity_incident_resolution_history", table_name="integrity_incident_resolutions")
    op.drop_table("integrity_incident_resolutions")
    op.drop_index("ix_integrity_incident_queue", table_name="integrity_incidents")
    op.drop_table("integrity_incidents")
