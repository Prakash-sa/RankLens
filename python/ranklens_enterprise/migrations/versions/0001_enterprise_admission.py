"""Enterprise admission, receipt, deletion, and outbox catalog."""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0001_enterprise_admission"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "attempt_generations",
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("tenant_id", "cluster_id", "attempt_id"),
    )
    op.create_table(
        "admission_reservations",
        sa.Column("reservation_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("producer_id", sa.String(length=128), nullable=False),
        sa.Column("transport_epoch", sa.String(length=128), nullable=False),
        sa.Column("stream_id", sa.String(length=128), nullable=False),
        sa.Column("first_sequence", sa.BigInteger(), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("object_key", sa.String(length=512), nullable=False),
        sa.Column("deletion_generation", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("reservation_id"),
        sa.UniqueConstraint(
            "tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id",
            "first_sequence", "last_sequence", name="uq_reservation_stream_range",
        ),
    )
    op.create_index(
        "ix_reservation_stream_lookup", "admission_reservations",
        ["tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id", "first_sequence", "last_sequence"],
    )
    op.create_table(
        "segment_manifests",
        sa.Column("receipt_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("cluster_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("producer_id", sa.String(length=128), nullable=False),
        sa.Column("transport_epoch", sa.String(length=128), nullable=False),
        sa.Column("stream_id", sa.String(length=128), nullable=False),
        sa.Column("first_sequence", sa.BigInteger(), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("object_key", sa.String(length=512), nullable=False),
        sa.Column("deletion_generation", sa.BigInteger(), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("receipt_id"),
        sa.UniqueConstraint(
            "tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id",
            "first_sequence", "last_sequence", name="uq_manifest_stream_range",
        ),
    )
    op.create_index(
        "ix_manifest_stream_overlap", "segment_manifests",
        ["tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id", "first_sequence", "last_sequence"],
    )
    op.create_index(
        "ix_manifest_attempt", "segment_manifests",
        ["tenant_id", "cluster_id", "attempt_id", "committed_at"],
    )
    op.create_table(
        "outbox_records",
        sa.Column("outbox_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.String(length=128), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("outbox_id"),
    )
    op.create_index("ix_outbox_pending", "outbox_records", ["state", "created_at"])
    op.create_table(
        "normalized_segments",
        sa.Column("receipt_id", sa.String(length=36), nullable=False),
        sa.Column("tenant_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_id", sa.String(length=128), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("normalizer_version", sa.String(length=32), nullable=False),
        sa.Column("normalized_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("receipt_id"),
    )
    op.create_index("ix_normalized_attempt", "normalized_segments", ["tenant_id", "attempt_id"])


def downgrade() -> None:
    op.drop_index("ix_normalized_attempt", table_name="normalized_segments")
    op.drop_table("normalized_segments")
    op.drop_index("ix_outbox_pending", table_name="outbox_records")
    op.drop_table("outbox_records")
    op.drop_index("ix_manifest_attempt", table_name="segment_manifests")
    op.drop_index("ix_manifest_stream_overlap", table_name="segment_manifests")
    op.drop_table("segment_manifests")
    op.drop_index("ix_reservation_stream_lookup", table_name="admission_reservations")
    op.drop_table("admission_reservations")
    op.drop_table("attempt_generations")
