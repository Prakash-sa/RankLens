"""Audit reviewed migration of existing retention assignments."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0021_retention_policy_migration_audit"
down_revision: Union[str, Sequence[str], None] = "0020_machine_credential_revocations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("retention_policy_backfills") as batch:
        batch.add_column(sa.Column("source_policy_version", sa.BigInteger(), nullable=True))
        batch.add_column(sa.Column("reason", sa.String(length=512), nullable=True))
        batch.add_column(sa.Column("candidate_digest", sa.String(length=64), nullable=True))
    op.create_table(
        "retention_policy_backfill_items",
        sa.Column("backfill_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("previous_policy_version", sa.BigInteger(), nullable=True),
        sa.Column("previous_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("new_policy_version", sa.BigInteger(), nullable=False),
        sa.Column("new_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("backfill_id", "attempt_id"),
    )
    op.create_index(
        "ix_retention_policy_backfill_item_scope",
        "retention_policy_backfill_items",
        ["tenant_id", "cluster_id", "attempt_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_retention_policy_backfill_item_scope",
        table_name="retention_policy_backfill_items",
    )
    op.drop_table("retention_policy_backfill_items")
    with op.batch_alter_table("retention_policy_backfills") as batch:
        batch.drop_column("candidate_digest")
        batch.drop_column("reason")
        batch.drop_column("source_policy_version")
