"""Add durable generation-fenced attempt erasure requests."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0010_attempt_deletions"
down_revision: Union[str, Sequence[str], None] = "0009_attempt_scheduler_identity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "attempt_deletions",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("deletion_generation", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_segment_objects", sa.Integer(), nullable=False),
        sa.Column("retained_shared_objects", sa.Integer(), nullable=False),
        sa.Column("deleted_report_objects", sa.Integer(), nullable=False),
        sa.Column("deleted_catalog_rows", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(length=512), nullable=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "attempt_id"),
    )
    op.create_index(
        "ix_attempt_deletion_pending",
        "attempt_deletions",
        ["state", "requested_at"],
    )
    op.execute(
        """
        INSERT INTO attempt_deletions (
            tenant_id, cluster_id, attempt_id, deletion_generation, state,
            attempts, lease_generation, deleted_segment_objects,
            retained_shared_objects, deleted_report_objects,
            deleted_catalog_rows, requested_at
        )
        SELECT tenant_id, cluster_id, attempt_id, generation, 'pending',
               0, 0, 0, 0, 0, 0, deleted_at
        FROM attempt_generations
        WHERE deleted_at IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ix_attempt_deletion_pending", table_name="attempt_deletions")
    op.drop_table("attempt_deletions")
