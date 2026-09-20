"""Track versioned Parquet artifacts for report revisions."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0005_report_parquet_artifacts"
down_revision: Union[str, Sequence[str], None] = "0004_report_revisions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "report_revisions", sa.Column("parquet_object_key", sa.String(length=512), nullable=True)
    )
    op.add_column(
        "report_revisions", sa.Column("parquet_sha256", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "report_revisions",
        sa.Column("parquet_schema_version", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "report_revisions", sa.Column("parquet_row_count", sa.BigInteger(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("report_revisions", "parquet_row_count")
    op.drop_column("report_revisions", "parquet_schema_version")
    op.drop_column("report_revisions", "parquet_sha256")
    op.drop_column("report_revisions", "parquet_object_key")
