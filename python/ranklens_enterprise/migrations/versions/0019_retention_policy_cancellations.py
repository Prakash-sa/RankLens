"""Audit cancellation of scheduled retention-policy revisions."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0019_retention_policy_cancellations"
down_revision: Union[str, Sequence[str], None] = "0018_retention_policy_case_scope"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "retention_policy_cancellations",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("policy_version", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("cancelled_by_credential_id", sa.String(length=128), nullable=False),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "policy_version"),
    )
    op.create_index(
        "ix_retention_policy_cancellation_history",
        "retention_policy_cancellations",
        ["tenant_id", "cluster_id", "cancelled_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_retention_policy_cancellation_history",
        table_name="retention_policy_cancellations",
    )
    op.drop_table("retention_policy_cancellations")
