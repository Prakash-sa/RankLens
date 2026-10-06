"""Add append-only cluster retention policies and applied-policy evidence."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0015_retention_policy_revisions"
down_revision: Union[str, Sequence[str], None] = "0014_hold_credential_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "retention_policy_revisions",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("retention_seconds", sa.BigInteger(), nullable=False),
        sa.Column("created_by_credential_id", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "version"),
    )
    op.create_index(
        "ix_retention_policy_latest",
        "retention_policy_revisions",
        ["tenant_id", "cluster_id", "version"],
    )
    op.add_column(
        "attempt_records",
        sa.Column("retention_policy_version", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("attempt_records", "retention_policy_version")
    op.drop_index(
        "ix_retention_policy_latest", table_name="retention_policy_revisions"
    )
    op.drop_table("retention_policy_revisions")
