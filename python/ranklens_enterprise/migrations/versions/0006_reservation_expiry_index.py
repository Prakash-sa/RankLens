"""Index active admission reservations for bounded expiry sweeps."""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0006_reservation_expiry_index"
down_revision: Union[str, Sequence[str], None] = "0005_report_parquet_artifacts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_reservation_expiry",
        "admission_reservations",
        ["state", "created_at", "reservation_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_reservation_expiry", table_name="admission_reservations")
