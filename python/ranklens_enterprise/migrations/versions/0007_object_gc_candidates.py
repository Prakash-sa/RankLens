"""Add delayed, auditable orphan-object reclamation candidates."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0007_object_gc_candidates"
down_revision: Union[str, Sequence[str], None] = "0006_reservation_expiry_index"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "object_gc_candidates",
        sa.Column("candidate_id", sa.String(length=36), nullable=False),
        sa.Column("object_key", sa.String(length=512), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("candidate_id"),
        sa.UniqueConstraint("object_key", name="uq_object_gc_candidate_key"),
    )
    op.create_index(
        "ix_object_gc_due",
        "object_gc_candidates",
        ["state", "not_before", "candidate_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_object_gc_due", table_name="object_gc_candidates")
    op.drop_table("object_gc_candidates")
