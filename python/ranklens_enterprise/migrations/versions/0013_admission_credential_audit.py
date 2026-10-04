"""Attribute durable admission state to a stable machine credential ID."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0013_admission_credential_audit"
down_revision: Union[str, Sequence[str], None] = "0012_attempt_retention"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _add_credential_id(table_name: str) -> None:
    op.add_column(
        table_name,
        sa.Column("credential_id", sa.String(length=128), nullable=True),
    )
    op.execute(
        sa.text(
            f"UPDATE {table_name} SET credential_id = 'legacy-unknown' "
            "WHERE credential_id IS NULL"
        )
    )
    with op.batch_alter_table(table_name) as batch:
        batch.alter_column(
            "credential_id",
            existing_type=sa.String(length=128),
            nullable=False,
        )


def upgrade() -> None:
    _add_credential_id("admission_reservations")
    _add_credential_id("segment_manifests")


def downgrade() -> None:
    with op.batch_alter_table("segment_manifests") as batch:
        batch.drop_column("credential_id")
    with op.batch_alter_table("admission_reservations") as batch:
        batch.drop_column("credential_id")
