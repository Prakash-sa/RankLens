"""Audit authorized report evidence downloads."""

from __future__ import annotations
from typing import Sequence, Union
import sqlalchemy as sa
from alembic import op

revision: str = "0026_evidence_download_audits"
down_revision: Union[str, Sequence[str], None] = "0025_restore_tombstone_imports"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "evidence_download_audits",
        sa.Column("download_id", sa.String(36), primary_key=True),
        sa.Column("tenant_id", sa.String(128), nullable=False),
        sa.Column("cluster_id", sa.String(128), nullable=False),
        sa.Column("attempt_id", sa.String(128), nullable=False),
        sa.Column("revision_id", sa.String(36), nullable=False),
        sa.Column("revision_number", sa.BigInteger(), nullable=False),
        sa.Column("artifact_kind", sa.String(16), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column("downloaded_by_credential_id", sa.String(128), nullable=False),
        sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_evidence_download_audit_scope",
        "evidence_download_audits",
        ["tenant_id", "cluster_id", "attempt_id", "downloaded_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_evidence_download_audit_scope",
        table_name="evidence_download_audits",
    )
    op.drop_table("evidence_download_audits")
