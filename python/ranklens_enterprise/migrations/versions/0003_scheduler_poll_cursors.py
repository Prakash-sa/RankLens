"""Add durable leased scheduler poll cursors."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0003_scheduler_poll_cursors"
down_revision: Union[str, Sequence[str], None] = "0002_scheduler_observations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheduler_poll_cursors",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("adapter", sa.String(length=64), nullable=False),
        sa.Column("cursor_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id"),
    )
    op.create_index(
        "ix_scheduler_poll_due",
        "scheduler_poll_cursors",
        ["next_poll_at", "tenant_id", "cluster_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_scheduler_poll_due", table_name="scheduler_poll_cursors")
    op.drop_table("scheduler_poll_cursors")
