"""Add immutable report revisions and atomic report heads."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0004_report_revisions"
down_revision: Union[str, Sequence[str], None] = "0003_scheduler_poll_cursors"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "report_revisions",
        sa.Column("revision_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("deletion_generation", sa.BigInteger(), nullable=False),
        sa.Column("revision_number", sa.BigInteger(), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("segment_count", sa.Integer(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.Column("report_object_key", sa.String(length=512), nullable=False),
        sa.Column("report_sha256", sa.String(length=64), nullable=False),
        sa.Column("builder_version", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("revision_id"),
        sa.UniqueConstraint(
            "tenant_id", "cluster_id", "attempt_id", "deletion_generation", "revision_number",
            name="uq_report_revision_number",
        ),
        sa.UniqueConstraint(
            "tenant_id", "cluster_id", "attempt_id", "deletion_generation", "input_fingerprint",
            name="uq_report_revision_input",
        ),
    )
    op.create_index(
        "ix_report_revision_attempt",
        "report_revisions",
        ["tenant_id", "cluster_id", "attempt_id", "deletion_generation", "revision_number"],
    )
    op.create_table(
        "report_heads",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("deletion_generation", sa.BigInteger(), nullable=False),
        sa.Column("revision_number", sa.BigInteger(), nullable=False),
        sa.Column("revision_id", sa.String(length=36), nullable=False),
        sa.Column("input_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "attempt_id"),
    )


def downgrade() -> None:
    op.drop_table("report_heads")
    op.drop_index("ix_report_revision_attempt", table_name="report_revisions")
    op.drop_table("report_revisions")
