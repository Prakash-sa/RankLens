"""Persist tenant-scoped machine credential revocations."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0020_machine_credential_revocations"
down_revision: Union[str, Sequence[str], None] = "0019_retention_policy_cancellations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "machine_credential_revocations",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("credential_id", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("revoked_by_credential_id", sa.String(length=128), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "credential_id"),
    )
    op.create_index(
        "ix_machine_credential_revocation_history",
        "machine_credential_revocations",
        ["tenant_id", "revoked_at", "credential_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_machine_credential_revocation_history",
        table_name="machine_credential_revocations",
    )
    op.drop_table("machine_credential_revocations")
