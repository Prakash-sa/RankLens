"""Add durable attempt retention holds."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0011_attempt_holds"
down_revision: Union[str, Sequence[str], None] = "0010_attempt_deletions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "attempt_holds",
        sa.Column("hold_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("placed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("hold_id"),
    )
    op.create_index(
        "ix_attempt_hold_active",
        "attempt_holds",
        ["tenant_id", "cluster_id", "attempt_id", "state"],
    )


def downgrade() -> None:
    op.drop_index("ix_attempt_hold_active", table_name="attempt_holds")
    op.drop_table("attempt_holds")
