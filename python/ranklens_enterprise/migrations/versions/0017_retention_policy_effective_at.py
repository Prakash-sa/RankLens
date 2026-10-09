"""Schedule append-only retention-policy revisions by effective time."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0017_retention_policy_effective_at"
down_revision: Union[str, Sequence[str], None] = "0016_retention_policy_backfills"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "retention_policy_revisions",
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE retention_policy_revisions "
            "SET effective_at = created_at WHERE effective_at IS NULL"
        )
    )
    with op.batch_alter_table("retention_policy_revisions") as batch_op:
        batch_op.alter_column(
            "effective_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )
        batch_op.create_index(
            "ix_retention_policy_effective",
            ["tenant_id", "cluster_id", "effective_at", "version"],
        )


def downgrade() -> None:
    with op.batch_alter_table("retention_policy_revisions") as batch_op:
        batch_op.drop_index("ix_retention_policy_effective")
        batch_op.drop_column("effective_at")
