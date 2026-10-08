"""Audit explicit retention-policy backfills for legacy attempts."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0016_retention_policy_backfills"
down_revision: Union[str, Sequence[str], None] = "0015_retention_policy_revisions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "retention_policy_backfills",
        sa.Column("backfill_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("policy_version", sa.BigInteger(), nullable=False),
        sa.Column("eligible_attempts", sa.BigInteger(), nullable=False),
        sa.Column("due_attempts", sa.BigInteger(), nullable=False),
        sa.Column("updated_attempts", sa.BigInteger(), nullable=False),
        sa.Column("requested_by_credential_id", sa.String(length=128), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("backfill_id"),
    )
    op.create_index(
        "ix_retention_policy_backfill_history",
        "retention_policy_backfills",
        ["tenant_id", "cluster_id", "requested_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_retention_policy_backfill_history",
        table_name="retention_policy_backfills",
    )
    op.drop_table("retention_policy_backfills")
