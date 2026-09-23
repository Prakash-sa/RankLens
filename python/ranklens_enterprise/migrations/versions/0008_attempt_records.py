"""Add durable attempt identities and independent scheduler state."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0008_attempt_records"
down_revision: Union[str, Sequence[str], None] = "0007_object_gc_candidates"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "attempt_records",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("workflow_execution_id", sa.String(length=128), nullable=True),
        sa.Column("logical_case_id", sa.String(length=128), nullable=True),
        sa.Column("scheduler_source_identity", sa.String(length=256), nullable=True),
        sa.Column("scheduler_state", sa.String(length=32), nullable=False),
        sa.Column("scheduler_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_admitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_admitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("segment_count", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "attempt_id"),
    )
    op.create_index(
        "ix_attempt_record_list",
        "attempt_records",
        ["tenant_id", "cluster_id", "attempt_id"],
    )
    op.create_index(
        "ix_attempt_record_scheduler",
        "attempt_records",
        ["tenant_id", "cluster_id", "scheduler_source_identity"],
    )
    op.execute(
        """
        INSERT INTO attempt_records (
            tenant_id, cluster_id, attempt_id, workflow_execution_id,
            logical_case_id, scheduler_source_identity, scheduler_state,
            scheduler_observed_at, first_admitted_at, last_admitted_at,
            segment_count, record_count
        )
        SELECT tenant_id, cluster_id, attempt_id, NULL, NULL, NULL, 'unknown',
               NULL, MIN(committed_at), MAX(committed_at), COUNT(*), SUM(record_count)
        FROM segment_manifests
        GROUP BY tenant_id, cluster_id, attempt_id
        """
    )


def downgrade() -> None:
    op.drop_index("ix_attempt_record_scheduler", table_name="attempt_records")
    op.drop_index("ix_attempt_record_list", table_name="attempt_records")
    op.drop_table("attempt_records")
