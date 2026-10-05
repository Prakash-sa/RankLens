"""Attribute retention-hold changes to stable machine credential IDs."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0014_hold_credential_audit"
down_revision: Union[str, Sequence[str], None] = "0013_admission_credential_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "attempt_holds",
        sa.Column("placed_by_credential_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "attempt_holds",
        sa.Column("released_by_credential_id", sa.String(length=128), nullable=True),
    )
    op.execute(
        "UPDATE attempt_holds SET placed_by_credential_id = 'legacy-unknown' "
        "WHERE placed_by_credential_id IS NULL"
    )
    with op.batch_alter_table("attempt_holds") as batch:
        batch.alter_column(
            "placed_by_credential_id",
            existing_type=sa.String(length=128),
            nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("attempt_holds") as batch:
        batch.drop_column("released_by_credential_id")
        batch.drop_column("placed_by_credential_id")
