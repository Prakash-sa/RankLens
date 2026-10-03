"""Add optional fixed retention expiry to durable attempts."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0012_attempt_retention"
down_revision: Union[str, Sequence[str], None] = "0011_attempt_holds"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "attempt_records",
        sa.Column("retention_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_attempt_record_retention",
        "attempt_records",
        ["retention_expires_at", "tenant_id", "cluster_id", "attempt_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_attempt_record_retention", table_name="attempt_records")
    op.drop_column("attempt_records", "retention_expires_at")
