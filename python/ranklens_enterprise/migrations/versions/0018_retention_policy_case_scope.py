"""Add optional logical-case scope to retention-policy revisions."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0018_retention_policy_case_scope"
down_revision: Union[str, Sequence[str], None] = "0017_retention_policy_effective_at"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index(
        "ix_retention_policy_effective",
        table_name="retention_policy_revisions",
    )
    op.add_column(
        "retention_policy_revisions",
        sa.Column("logical_case_id", sa.String(length=128), nullable=True),
    )
    op.create_index(
        "ix_retention_policy_effective",
        "retention_policy_revisions",
        ["tenant_id", "cluster_id", "logical_case_id", "effective_at", "version"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_retention_policy_effective",
        table_name="retention_policy_revisions",
    )
    op.drop_column("retention_policy_revisions", "logical_case_id")
    op.create_index(
        "ix_retention_policy_effective",
        "retention_policy_revisions",
        ["tenant_id", "cluster_id", "effective_at", "version"],
    )
