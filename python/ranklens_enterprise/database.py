"""Transactional catalog for admission receipts and asynchronous work."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text, UniqueConstraint, create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class AttemptGeneration(Base):
    __tablename__ = "attempt_generations"

    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AdmissionReservation(Base):
    __tablename__ = "admission_reservations"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id",
            "first_sequence", "last_sequence", name="uq_reservation_stream_range",
        ),
        Index(
            "ix_reservation_stream_lookup", "tenant_id", "cluster_id", "producer_id",
            "transport_epoch", "stream_id", "first_sequence", "last_sequence",
        ),
        Index("ix_reservation_expiry", "state", "created_at", "reservation_id"),
    )

    reservation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    producer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    transport_epoch: Mapped[str] = mapped_column(String(128), nullable=False)
    stream_id: Mapped[str] = mapped_column(String(128), nullable=False)
    first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    deletion_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SegmentManifest(Base):
    __tablename__ = "segment_manifests"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "cluster_id", "producer_id", "transport_epoch", "stream_id",
            "first_sequence", "last_sequence", name="uq_manifest_stream_range",
        ),
        Index(
            "ix_manifest_stream_overlap", "tenant_id", "cluster_id", "producer_id",
            "transport_epoch", "stream_id", "first_sequence", "last_sequence",
        ),
        Index("ix_manifest_attempt", "tenant_id", "cluster_id", "attempt_id", "committed_at"),
    )

    receipt_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    producer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    transport_epoch: Mapped[str] = mapped_column(String(128), nullable=False)
    stream_id: Mapped[str] = mapped_column(String(128), nullable=False)
    first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    deletion_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OutboxRecord(Base):
    __tablename__ = "outbox_records"
    __table_args__ = (Index("ix_outbox_pending", "state", "created_at"),)

    outbox_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_owner: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    lease_generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class NormalizedSegment(Base):
    __tablename__ = "normalized_segments"
    __table_args__ = (Index("ix_normalized_attempt", "tenant_id", "attempt_id"),)

    receipt_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    normalizer_version: Mapped[str] = mapped_column(String(32), nullable=False)
    normalized_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReportRevision(Base):
    __tablename__ = "report_revisions"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "cluster_id", "attempt_id", "deletion_generation", "revision_number",
            name="uq_report_revision_number",
        ),
        UniqueConstraint(
            "tenant_id", "cluster_id", "attempt_id", "deletion_generation", "input_fingerprint",
            name="uq_report_revision_input",
        ),
        Index(
            "ix_report_revision_attempt",
            "tenant_id", "cluster_id", "attempt_id", "deletion_generation", "revision_number",
        ),
    )

    revision_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_id: Mapped[str] = mapped_column(String(128), nullable=False)
    deletion_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    segment_count: Mapped[int] = mapped_column(Integer, nullable=False)
    record_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    report_object_key: Mapped[str] = mapped_column(String(512), nullable=False)
    report_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    parquet_object_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    parquet_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parquet_schema_version: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    parquet_row_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    builder_version: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ReportHead(Base):
    __tablename__ = "report_heads"

    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    deletion_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision_id: Mapped[str] = mapped_column(String(36), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SchedulerObservation(Base):
    __tablename__ = "scheduler_observations"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "cluster_id", "source_identity", "observed_at",
            name="uq_scheduler_observation_seen",
        ),
        Index(
            "ix_scheduler_observation_attempt",
            "tenant_id", "cluster_id", "source_identity", "observed_at",
        ),
        Index(
            "ix_scheduler_observation_job",
            "tenant_id", "cluster_id", "job_id", "observed_at",
        ),
    )

    observation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    adapter: Mapped[str] = mapped_column(String(64), nullable=False)
    adapter_version: Mapped[str] = mapped_column(String(64), nullable=False)
    source_identity: Mapped[str] = mapped_column(String(256), nullable=False)
    job_id: Mapped[str] = mapped_column(String(128), nullable=False)
    array_job_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    array_task_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    step_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    restart_count: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    state_reason: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    allocated_nodes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    allocated_cpus: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    account: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    partition: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    user_ref: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SchedulerPollCursor(Base):
    """Durable ownership and progress for one tenant/cluster poll stream."""

    __tablename__ = "scheduler_poll_cursors"
    __table_args__ = (Index("ix_scheduler_poll_due", "next_poll_at", "tenant_id", "cluster_id"),)

    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    adapter: Mapped[str] = mapped_column(String(64), nullable=False)
    cursor_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_poll_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    lease_generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def build_engine(database_url: str):
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, pool_pre_ping=True, future=True, connect_args=connect_args)


def build_session_factory(engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, class_=Session)


def initialize_schema(engine) -> None:
    """Create the development schema.

    Production deployments use reviewed Alembic migrations; this bootstrap is
    deliberately small so a new installation cannot run without tables.
    """

    Base.metadata.create_all(engine)


def assert_schema_ready(engine) -> None:
    """Fail closed when a service starts without the reviewed migration chain."""

    available = set(inspect(engine).get_table_names())
    required = {"alembic_version", *Base.metadata.tables.keys()}
    missing = required.difference(available)
    if missing:
        raise RuntimeError(
            "database migrations are not current; missing tables: "
            + ", ".join(sorted(missing))
        )


def lock_stream(session: Session, identity: str) -> None:
    """Serialize overlap checks on PostgreSQL for one logical stream."""

    if session.bind is None or session.bind.dialect.name != "postgresql":
        return
    digest = hashlib.blake2b(identity.encode("utf-8"), digest_size=8).digest()
    signed_key = int.from_bytes(digest, byteorder="big", signed=True)
    session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": signed_key})
