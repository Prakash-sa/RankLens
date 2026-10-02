"""Leased, idempotent outbox worker for admitted telemetry."""

from __future__ import annotations

import hashlib
import json
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from .database import (
    AdmissionReservation,
    AttemptDeletion,
    AttemptGeneration,
    AttemptHold,
    AttemptRecord,
    NormalizedSegment,
    ObjectGcCandidate,
    OutboxRecord,
    ReportHead,
    ReportRevision,
    SchedulerObservation,
    SegmentManifest,
    lock_stream,
    utc_now,
)
from .parquet import ParquetSegment, build_normalized_parquet
from .storage import ImmutableObjectStore, ObjectIntegrityError


@dataclass(frozen=True)
class WorkLease:
    outbox_id: str
    generation: int
    owner: str


@dataclass(frozen=True)
class DeletionLease:
    tenant_id: str
    cluster_id: str
    attempt_id: str
    deletion_generation: int
    lease_generation: int
    owner: str


class AttemptDeletionWorker:
    """Physically erase one fenced attempt while retaining its minimal tombstone."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        owner: Optional[str] = None,
        lease_seconds: int = 60,
    ):
        self._sessions = sessions
        self._objects = objects
        self._owner = owner or f"deletion-worker-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds

    def claim(self) -> Optional[DeletionLease]:
        now = utc_now()
        with self._sessions.begin() as session:
            query = (
                select(AttemptDeletion)
                .where(
                    (AttemptDeletion.state == "pending")
                    | (
                        (AttemptDeletion.state == "processing")
                        & (AttemptDeletion.lease_expires_at < now)
                    )
                )
                .order_by(AttemptDeletion.requested_at)
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            request = session.scalar(query)
            if request is None:
                return None
            request.state = "processing"
            request.attempts += 1
            request.lease_owner = self._owner
            request.lease_generation += 1
            request.lease_expires_at = now + timedelta(seconds=self._lease_seconds)
            request.last_error = None
            return DeletionLease(
                tenant_id=request.tenant_id,
                cluster_id=request.cluster_id,
                attempt_id=request.attempt_id,
                deletion_generation=request.deletion_generation,
                lease_generation=request.lease_generation,
                owner=self._owner,
            )

    @staticmethod
    def _matches(lease: DeletionLease, request: AttemptDeletion) -> bool:
        return (
            request.state == "processing"
            and request.lease_owner == lease.owner
            and request.lease_generation == lease.lease_generation
            and request.deletion_generation == lease.deletion_generation
        )

    @staticmethod
    def _deleted_rows(result) -> int:
        return max(result.rowcount or 0, 0)

    def execute(self, lease: DeletionLease) -> bool:
        identity = (lease.tenant_id, lease.cluster_id, lease.attempt_id)
        try:
            with self._sessions.begin() as session:
                request = session.get(AttemptDeletion, identity)
                if request is None or not self._matches(lease, request):
                    return False
                lock_stream(
                    session,
                    f"attempt:{lease.tenant_id}:{lease.cluster_id}:{lease.attempt_id}",
                )
                session.refresh(request)
                if not self._matches(lease, request):
                    return False
                active_hold = session.scalar(
                    select(AttemptHold.hold_id)
                    .where(
                        AttemptHold.tenant_id == lease.tenant_id,
                        AttemptHold.cluster_id == lease.cluster_id,
                        AttemptHold.attempt_id == lease.attempt_id,
                        AttemptHold.state == "active",
                    )
                    .limit(1)
                )
                if active_hold is not None:
                    request.state = "held"
                    request.lease_owner = None
                    request.lease_expires_at = None
                    request.last_error = "physical erasure blocked by retention hold"
                    return False
                generation = session.get(AttemptGeneration, identity)
                if (
                    generation is None
                    or generation.deleted_at is None
                    or generation.generation != lease.deletion_generation
                ):
                    raise ValueError("attempt deletion generation is no longer current")

                manifests = list(
                    session.scalars(
                        select(SegmentManifest).where(
                            SegmentManifest.tenant_id == lease.tenant_id,
                            SegmentManifest.cluster_id == lease.cluster_id,
                            SegmentManifest.attempt_id == lease.attempt_id,
                        )
                    )
                )
                revisions = list(
                    session.scalars(
                        select(ReportRevision).where(
                            ReportRevision.tenant_id == lease.tenant_id,
                            ReportRevision.cluster_id == lease.cluster_id,
                            ReportRevision.attempt_id == lease.attempt_id,
                        )
                    )
                )
                attempt = session.get(AttemptRecord, identity)
                receipt_ids = [manifest.receipt_id for manifest in manifests]
                segment_objects = {
                    manifest.object_key: manifest.payload_sha256 for manifest in manifests
                }
                shared_objects = set()
                if segment_objects:
                    keys = list(segment_objects)
                    for key in sorted(keys):
                        lock_stream(session, f"object:{key}")
                    manifest_references = session.execute(
                        select(
                            SegmentManifest.object_key,
                            SegmentManifest.tenant_id,
                            SegmentManifest.cluster_id,
                            SegmentManifest.attempt_id,
                        ).where(SegmentManifest.object_key.in_(keys))
                    ).all()
                    reservation_references = session.execute(
                        select(
                            AdmissionReservation.object_key,
                            AdmissionReservation.tenant_id,
                            AdmissionReservation.cluster_id,
                            AdmissionReservation.attempt_id,
                        ).where(
                            AdmissionReservation.object_key.in_(keys),
                            AdmissionReservation.state.in_(("reserved", "committed")),
                        )
                    ).all()
                    for key, tenant_id, cluster_id, attempt_id in (
                        manifest_references + reservation_references
                    ):
                        if (tenant_id, cluster_id, attempt_id) != identity:
                            shared_objects.add(key)

                report_objects = {}
                for revision in revisions:
                    report_objects[revision.report_object_key] = revision.report_sha256
                    if revision.parquet_object_key and revision.parquet_sha256:
                        report_objects[revision.parquet_object_key] = revision.parquet_sha256
                for key in sorted(report_objects):
                    lock_stream(session, f"object:{key}")

                deletable_segments = {
                    key: digest for key, digest in segment_objects.items()
                    if key not in shared_objects
                }
                for key, digest in sorted(
                    {**deletable_segments, **report_objects}.items()
                ):
                    self._objects.delete_verified(key, digest)

                deleted_rows = 0
                if receipt_ids:
                    deleted_rows += self._deleted_rows(
                        session.execute(
                            delete(OutboxRecord).where(
                                OutboxRecord.event_type == "segment.admitted",
                                OutboxRecord.aggregate_id.in_(receipt_ids),
                            )
                        )
                    )
                    deleted_rows += self._deleted_rows(
                        session.execute(
                            delete(NormalizedSegment).where(
                                NormalizedSegment.receipt_id.in_(receipt_ids)
                            )
                        )
                    )

                report_requests = list(
                    session.scalars(
                        select(OutboxRecord).where(
                            OutboxRecord.tenant_id == lease.tenant_id,
                            OutboxRecord.event_type == "attempt.report.requested",
                            OutboxRecord.aggregate_id == lease.attempt_id,
                        )
                    )
                )
                for record in report_requests:
                    try:
                        payload = json.loads(record.payload_json)
                    except json.JSONDecodeError:
                        continue
                    if (
                        isinstance(payload, dict)
                        and payload.get("cluster_id") == lease.cluster_id
                    ):
                        session.delete(record)
                        deleted_rows += 1

                deleted_rows += self._deleted_rows(
                    session.execute(
                        delete(ReportRevision).where(
                            ReportRevision.tenant_id == lease.tenant_id,
                            ReportRevision.cluster_id == lease.cluster_id,
                            ReportRevision.attempt_id == lease.attempt_id,
                        )
                    )
                )
                deleted_rows += self._deleted_rows(
                    session.execute(
                        delete(ReportHead).where(
                            ReportHead.tenant_id == lease.tenant_id,
                            ReportHead.cluster_id == lease.cluster_id,
                            ReportHead.attempt_id == lease.attempt_id,
                        )
                    )
                )
                deleted_rows += self._deleted_rows(
                    session.execute(
                        delete(SegmentManifest).where(
                            SegmentManifest.tenant_id == lease.tenant_id,
                            SegmentManifest.cluster_id == lease.cluster_id,
                            SegmentManifest.attempt_id == lease.attempt_id,
                        )
                    )
                )
                deleted_rows += self._deleted_rows(
                    session.execute(
                        delete(AdmissionReservation).where(
                            AdmissionReservation.tenant_id == lease.tenant_id,
                            AdmissionReservation.cluster_id == lease.cluster_id,
                            AdmissionReservation.attempt_id == lease.attempt_id,
                        )
                    )
                )
                if attempt is not None and attempt.scheduler_source_identity:
                    deleted_rows += self._deleted_rows(
                        session.execute(
                            delete(SchedulerObservation).where(
                                SchedulerObservation.tenant_id == lease.tenant_id,
                                SchedulerObservation.cluster_id == lease.cluster_id,
                                SchedulerObservation.source_identity
                                == attempt.scheduler_source_identity,
                            )
                        )
                    )
                deleted_rows += self._deleted_rows(
                    session.execute(
                        delete(AttemptRecord).where(
                            AttemptRecord.tenant_id == lease.tenant_id,
                            AttemptRecord.cluster_id == lease.cluster_id,
                            AttemptRecord.attempt_id == lease.attempt_id,
                        )
                    )
                )
                deleted_object_keys = [*deletable_segments, *report_objects]
                if deleted_object_keys:
                    deleted_rows += self._deleted_rows(
                        session.execute(
                            delete(ObjectGcCandidate).where(
                                ObjectGcCandidate.object_key.in_(deleted_object_keys)
                            )
                        )
                    )

                request.state = "completed"
                request.lease_owner = None
                request.lease_expires_at = None
                request.deleted_segment_objects = len(deletable_segments)
                request.retained_shared_objects = len(shared_objects)
                request.deleted_report_objects = len(report_objects)
                request.deleted_catalog_rows = deleted_rows
                request.last_error = None
                request.completed_at = utc_now()
                return True
        except (ObjectIntegrityError, ValueError) as exc:
            self.fail(lease, str(exc))
            return False

    def fail(self, lease: DeletionLease, message: str) -> None:
        identity = (lease.tenant_id, lease.cluster_id, lease.attempt_id)
        with self._sessions.begin() as session:
            request = session.get(AttemptDeletion, identity)
            if request is None or not self._matches(lease, request):
                return
            request.state = "failed" if request.attempts >= 5 else "pending"
            request.lease_owner = None
            request.lease_expires_at = None
            request.last_error = message[:512]

    def run_once(self) -> bool:
        lease = self.claim()
        if lease is None:
            return False
        return self.execute(lease)


class ReservationSweeper:
    """Expires abandoned pre-admission reservations in bounded transactions."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        ttl_seconds: int = 900,
        batch_size: int = 100,
        gc_grace_seconds: int = 3600,
        clock=utc_now,
    ):
        if ttl_seconds < 60 or ttl_seconds > 86400:
            raise ValueError("ttl_seconds must be within [60, 86400]")
        if batch_size < 1 or batch_size > 1000:
            raise ValueError("batch_size must be within [1, 1000]")
        if gc_grace_seconds < 300 or gc_grace_seconds > 604800:
            raise ValueError("gc_grace_seconds must be within [300, 604800]")
        self._sessions = sessions
        self._ttl_seconds = ttl_seconds
        self._batch_size = batch_size
        self._gc_grace_seconds = gc_grace_seconds
        self._clock = clock

    def run_once(self) -> int:
        cutoff = self._clock() - timedelta(seconds=self._ttl_seconds)
        with self._sessions.begin() as session:
            query = (
                select(AdmissionReservation)
                .where(
                    AdmissionReservation.state == "reserved",
                    AdmissionReservation.created_at < cutoff,
                )
                .order_by(AdmissionReservation.created_at, AdmissionReservation.reservation_id)
                .limit(self._batch_size)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            reservations = list(session.scalars(query))
            for reservation in reservations:
                reservation.state = "expired"
                existing = session.scalar(
                    select(ObjectGcCandidate.candidate_id).where(
                        ObjectGcCandidate.object_key == reservation.object_key
                    )
                )
                if existing is None:
                    now = self._clock()
                    session.add(
                        ObjectGcCandidate(
                            candidate_id=str(uuid.uuid4()),
                            object_key=reservation.object_key,
                            payload_sha256=reservation.payload_sha256,
                            state="pending",
                            not_before=now + timedelta(seconds=self._gc_grace_seconds),
                            attempts=0,
                            last_error=None,
                            created_at=now,
                            completed_at=None,
                        )
                    )
            return len(reservations)


class ObjectGcWorker:
    """Reclaims only unreferenced, checksum-verified objects after a grace period."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        clock=utc_now,
    ):
        self._sessions = sessions
        self._objects = objects
        self._clock = clock

    def run_once(self) -> Optional[str]:
        now = self._clock()
        with self._sessions.begin() as session:
            query = (
                select(ObjectGcCandidate)
                .where(
                    ObjectGcCandidate.state == "pending",
                    ObjectGcCandidate.not_before <= now,
                )
                .order_by(ObjectGcCandidate.not_before, ObjectGcCandidate.candidate_id)
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            candidate = session.scalar(query)
            if candidate is None:
                return None
            candidate.attempts += 1
            lock_stream(session, f"object:{candidate.object_key}")
            if _object_has_reference(session, candidate.object_key):
                candidate.state = "protected"
                candidate.completed_at = now
                candidate.last_error = None
                return "protected"
            try:
                deleted = self._objects.delete_verified(
                    candidate.object_key, candidate.payload_sha256
                )
            except ObjectIntegrityError as exc:
                candidate.last_error = str(exc)[:512]
                if candidate.attempts >= 5:
                    candidate.state = "failed"
                    candidate.completed_at = now
                else:
                    delay = min(3600, 30 * (2 ** (candidate.attempts - 1)))
                    candidate.not_before = now + timedelta(seconds=delay)
                return "failed" if candidate.state == "failed" else "retry"
            candidate.state = "deleted" if deleted else "missing"
            candidate.completed_at = now
            candidate.last_error = None
            return candidate.state


def _object_has_reference(session: Session, object_key: str) -> bool:
    manifest_reference = session.scalar(
        select(SegmentManifest.receipt_id)
        .where(SegmentManifest.object_key == object_key)
        .limit(1)
    )
    if manifest_reference is not None:
        return True
    reservation_reference = session.scalar(
        select(AdmissionReservation.reservation_id)
        .where(
            AdmissionReservation.object_key == object_key,
            AdmissionReservation.state.in_(("reserved", "committed")),
        )
        .limit(1)
    )
    if reservation_reference is not None:
        return True
    report_reference = session.scalar(
        select(ReportRevision.revision_id)
        .where(
            (ReportRevision.report_object_key == object_key)
            | (ReportRevision.parquet_object_key == object_key)
        )
        .limit(1)
    )
    return report_reference is not None


class ObjectGcAuditWorker:
    """Periodically reconciles protected and quarantined objects with catalog truth."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        gc_grace_seconds: int = 3600,
        audit_interval_seconds: int = 86400,
        batch_size: int = 100,
        clock=utc_now,
    ):
        if not 300 <= gc_grace_seconds <= 604800:
            raise ValueError("gc_grace_seconds must be within [300, 604800]")
        if not 300 <= audit_interval_seconds <= 604800:
            raise ValueError("audit_interval_seconds must be within [300, 604800]")
        if not 1 <= batch_size <= 10_000:
            raise ValueError("batch_size must be within [1, 10000]")
        self._sessions = sessions
        self._objects = objects
        self._gc_grace_seconds = gc_grace_seconds
        self._audit_interval_seconds = audit_interval_seconds
        self._batch_size = batch_size
        self._clock = clock

    def run_once(self) -> dict[str, int]:
        now = self._clock()
        outcomes: dict[str, int] = {}
        with self._sessions.begin() as session:
            query = (
                select(ObjectGcCandidate)
                .where(
                    (
                        (ObjectGcCandidate.state == "protected")
                        & (ObjectGcCandidate.not_before <= now)
                    )
                    | (
                        (ObjectGcCandidate.state == "failed")
                        & ObjectGcCandidate.completed_at.is_not(None)
                        & (
                            ObjectGcCandidate.completed_at
                            <= now - timedelta(seconds=self._audit_interval_seconds)
                        )
                    )
                )
                .order_by(ObjectGcCandidate.not_before, ObjectGcCandidate.candidate_id)
                .limit(self._batch_size)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            candidates = list(session.scalars(query))
            for candidate in candidates:
                lock_stream(session, f"object:{candidate.object_key}")
                referenced = _object_has_reference(session, candidate.object_key)
                try:
                    exists = self._objects.exists_verified(
                        candidate.object_key, candidate.payload_sha256
                    )
                except ObjectIntegrityError as exc:
                    candidate.state = "failed"
                    candidate.last_error = f"audit integrity failure: {exc}"[:512]
                    candidate.completed_at = now
                    candidate.not_before = now + timedelta(
                        seconds=self._audit_interval_seconds
                    )
                else:
                    if referenced and exists:
                        candidate.state = "protected"
                        candidate.last_error = None
                        candidate.completed_at = now
                        candidate.not_before = now + timedelta(
                            seconds=self._audit_interval_seconds
                        )
                    elif referenced:
                        candidate.state = "failed"
                        candidate.last_error = "audit found a referenced object missing"
                        candidate.completed_at = now
                        candidate.not_before = now + timedelta(
                            seconds=self._audit_interval_seconds
                        )
                    elif exists:
                        candidate.state = "pending"
                        candidate.attempts = 0
                        candidate.last_error = None
                        candidate.completed_at = None
                        candidate.not_before = now + timedelta(
                            seconds=self._gc_grace_seconds
                        )
                    else:
                        candidate.state = "missing"
                        candidate.last_error = None
                        candidate.completed_at = now
                outcomes[candidate.state] = outcomes.get(candidate.state, 0) + 1
        return outcomes


class OutboxWorker:
    """Processes small outbox references; raw payloads remain in object storage."""

    NORMALIZER_VERSION = "ndjson-v1"

    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        owner: Optional[str] = None,
        lease_seconds: int = 60,
    ):
        self._sessions = sessions
        self._objects = objects
        self._owner = owner or f"worker-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds

    def claim(self) -> Optional[WorkLease]:
        now = utc_now()
        with self._sessions.begin() as session:
            query = (
                select(OutboxRecord)
                .where(
                    OutboxRecord.event_type == "segment.admitted",
                    (OutboxRecord.state == "pending")
                    | (
                        (OutboxRecord.state == "processing")
                        & (OutboxRecord.lease_expires_at < now)
                    )
                )
                .order_by(OutboxRecord.created_at, OutboxRecord.outbox_id)
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            record = session.scalar(query)
            if record is None:
                return None
            record.state = "processing"
            record.lease_owner = self._owner
            record.lease_generation += 1
            record.lease_expires_at = now + timedelta(seconds=self._lease_seconds)
            record.attempts += 1
            record.last_error = None
            return WorkLease(record.outbox_id, record.lease_generation, self._owner)

    def _parse_payload(self, payload: bytes, expected_records: int) -> int:
        count = 0
        for line in payload.splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("normalized NDJSON record must be an object")
            count += 1
        if count != expected_records:
            raise ValueError("normalized record count differs from admitted manifest")
        return count

    def execute(self, lease: WorkLease) -> bool:
        """Apply one exactly-once effect and fence stale workers at publication."""

        try:
            with self._sessions() as session:
                outbox = session.get(OutboxRecord, lease.outbox_id)
                if outbox is None or outbox.aggregate_id is None:
                    return False
                manifest = session.get(SegmentManifest, outbox.aggregate_id)
                if manifest is None:
                    raise ValueError("outbox references a missing manifest")
                payload = self._objects.read_verified(manifest.object_key, manifest.payload_sha256)
                record_count = self._parse_payload(payload, manifest.record_count)
                publication = {
                    "receipt_id": manifest.receipt_id,
                    "tenant_id": manifest.tenant_id,
                    "cluster_id": manifest.cluster_id,
                    "attempt_id": manifest.attempt_id,
                    "record_count": record_count,
                    "payload_sha256": manifest.payload_sha256,
                    "deletion_generation": manifest.deletion_generation,
                }

            with self._sessions.begin() as session:
                outbox = session.get(OutboxRecord, lease.outbox_id)
                if (
                    outbox is None
                    or outbox.state != "processing"
                    or outbox.lease_owner != lease.owner
                    or outbox.lease_generation != lease.generation
                ):
                    return False
                state = session.get(
                    AttemptGeneration,
                    (
                        publication["tenant_id"],
                        publication["cluster_id"],
                        publication["attempt_id"],
                    ),
                )
                if (
                    state is None
                    or state.deleted_at is not None
                    or state.generation != publication["deletion_generation"]
                ):
                    outbox.state = "suppressed"
                    outbox.lease_owner = None
                    outbox.lease_expires_at = None
                    outbox.last_error = "attempt deletion generation changed"
                    return False

                existing = session.get(NormalizedSegment, publication["receipt_id"])
                if existing is None:
                    session.add(
                        NormalizedSegment(
                            receipt_id=publication["receipt_id"],
                            tenant_id=publication["tenant_id"],
                            attempt_id=publication["attempt_id"],
                            record_count=publication["record_count"],
                            payload_sha256=publication["payload_sha256"],
                            normalizer_version=self.NORMALIZER_VERSION,
                            normalized_at=utc_now(),
                        )
                    )
                    session.add(
                        OutboxRecord(
                            outbox_id=str(uuid.uuid4()),
                            tenant_id=publication["tenant_id"],
                            event_type="attempt.report.requested",
                            aggregate_id=publication["attempt_id"],
                            payload_json=json.dumps(
                                {
                                    "cluster_id": publication["cluster_id"],
                                    "attempt_id": publication["attempt_id"],
                                    "deletion_generation": publication["deletion_generation"],
                                },
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            state="pending",
                            attempts=0,
                            lease_owner=None,
                            lease_generation=0,
                            lease_expires_at=None,
                            last_error=None,
                            created_at=utc_now(),
                        )
                    )
                elif existing.payload_sha256 != publication["payload_sha256"]:
                    raise ValueError("normalized receipt digest conflict")
                outbox.state = "completed"
                outbox.lease_owner = None
                outbox.lease_expires_at = None
                outbox.last_error = None
                return True
        except (ObjectIntegrityError, json.JSONDecodeError, ValueError) as exc:
            self.fail(lease, str(exc))
            return False

    def fail(self, lease: WorkLease, message: str) -> None:
        with self._sessions.begin() as session:
            outbox = session.get(OutboxRecord, lease.outbox_id)
            if (
                outbox is None
                or outbox.lease_owner != lease.owner
                or outbox.lease_generation != lease.generation
            ):
                return
            outbox.state = "failed" if outbox.attempts >= 5 else "pending"
            outbox.lease_owner = None
            outbox.lease_expires_at = None
            outbox.last_error = message[:512]

    def run_once(self) -> bool:
        lease = self.claim()
        if lease is None:
            return False
        self.execute(lease)
        return True


@dataclass(frozen=True)
class ReportInput:
    receipt_id: str
    payload_sha256: str
    object_key: str
    record_count: int
    normalizer_version: str


class ReportRevisionWorker:
    """Publishes deterministic attempt manifests and atomically advances their head."""

    BUILDER_VERSION = "attempt-manifest-v1"

    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        owner: Optional[str] = None,
        lease_seconds: int = 60,
        max_segments: int = 10_000,
        max_records: int = 10_000_000,
        max_input_bytes: int = 64 * 1024 * 1024,
        orphan_gc_grace_seconds: int = 3600,
    ):
        if not 300 <= orphan_gc_grace_seconds <= 604800:
            raise ValueError("orphan_gc_grace_seconds must be within [300, 604800]")
        self._sessions = sessions
        self._objects = objects
        self._owner = owner or f"report-worker-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds
        self._max_segments = max_segments
        self._max_records = max_records
        self._max_input_bytes = max_input_bytes
        self._orphan_gc_grace_seconds = orphan_gc_grace_seconds

    def claim(self) -> Optional[WorkLease]:
        now = utc_now()
        with self._sessions.begin() as session:
            query = (
                select(OutboxRecord)
                .where(
                    OutboxRecord.event_type == "attempt.report.requested",
                    (OutboxRecord.state == "pending")
                    | (
                        (OutboxRecord.state == "processing")
                        & (OutboxRecord.lease_expires_at < now)
                    ),
                )
                .order_by(OutboxRecord.created_at, OutboxRecord.outbox_id)
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            record = session.scalar(query)
            if record is None:
                return None
            record.state = "processing"
            record.lease_owner = self._owner
            record.lease_generation += 1
            record.lease_expires_at = now + timedelta(seconds=self._lease_seconds)
            record.attempts += 1
            record.last_error = None
            return WorkLease(record.outbox_id, record.lease_generation, self._owner)

    @staticmethod
    def _request(record: OutboxRecord) -> tuple[str, str, str, int]:
        payload = json.loads(record.payload_json)
        if not isinstance(payload, dict) or set(payload) != {
            "cluster_id", "attempt_id", "deletion_generation"
        }:
            raise ValueError("report request payload is invalid")
        cluster_id = payload["cluster_id"]
        attempt_id = payload["attempt_id"]
        generation = payload["deletion_generation"]
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ValueError("report request cluster_id is invalid")
        if not isinstance(attempt_id, str) or not attempt_id:
            raise ValueError("report request attempt_id is invalid")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
            raise ValueError("report request deletion_generation is invalid")
        return record.tenant_id, cluster_id, attempt_id, generation

    @staticmethod
    def _inputs(
        session: Session,
        tenant_id: str,
        cluster_id: str,
        attempt_id: str,
        generation: int,
    ) -> list[ReportInput]:
        statement = (
            select(NormalizedSegment, SegmentManifest)
            .join(SegmentManifest, SegmentManifest.receipt_id == NormalizedSegment.receipt_id)
            .where(
                SegmentManifest.tenant_id == tenant_id,
                SegmentManifest.cluster_id == cluster_id,
                SegmentManifest.attempt_id == attempt_id,
                SegmentManifest.deletion_generation == generation,
            )
            .order_by(
                SegmentManifest.producer_id,
                SegmentManifest.transport_epoch,
                SegmentManifest.stream_id,
                SegmentManifest.first_sequence,
                SegmentManifest.receipt_id,
            )
        )
        return [
            ReportInput(
                receipt_id=manifest.receipt_id,
                payload_sha256=manifest.payload_sha256,
                object_key=manifest.object_key,
                record_count=normalized.record_count,
                normalizer_version=normalized.normalizer_version,
            )
            for normalized, manifest in session.execute(statement).all()
        ]

    @staticmethod
    def _input_fingerprint(inputs: list[ReportInput]) -> str:
        encoded = json.dumps(
            [
                {
                    "normalizer_version": item.normalizer_version,
                    "payload_sha256": item.payload_sha256,
                    "receipt_id": item.receipt_id,
                    "record_count": item.record_count,
                }
                for item in inputs
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _report_payload(
        cls,
        tenant_id: str,
        cluster_id: str,
        attempt_id: str,
        generation: int,
        fingerprint: str,
        inputs: list[ReportInput],
        parquet_sha256: str,
        parquet_schema_version: str,
        parquet_row_count: int,
    ) -> bytes:
        value = {
            "attempt_id": attempt_id,
            "builder_version": cls.BUILDER_VERSION,
            "cluster_id": cluster_id,
            "deletion_generation": generation,
            "input_fingerprint": fingerprint,
            "parquet": {
                "row_count": parquet_row_count,
                "schema_version": parquet_schema_version,
                "sha256": parquet_sha256,
            },
            "record_count": sum(item.record_count for item in inputs),
            "schema_major": 1,
            "segment_count": len(inputs),
            "segments": [
                {
                    "normalizer_version": item.normalizer_version,
                    "payload_sha256": item.payload_sha256,
                    "receipt_id": item.receipt_id,
                    "record_count": item.record_count,
                }
                for item in inputs
            ],
            "tenant_id": tenant_id,
            "type": "ranklens-attempt-manifest",
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    @staticmethod
    def _object_key(
        tenant_id: str,
        cluster_id: str,
        attempt_id: str,
        digest: str,
        suffix: str,
    ) -> str:
        scope = "/".join(
            hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
            for value in (tenant_id, cluster_id, attempt_id)
        )
        return f"reports/v1/{scope}/{digest}.{suffix}"

    @staticmethod
    def _ensure_artifact_candidate(
        session: Session,
        object_key: str,
        digest: str,
        now,
        not_before,
    ) -> ObjectGcCandidate:
        candidate = session.scalar(
            select(ObjectGcCandidate).where(ObjectGcCandidate.object_key == object_key)
        )
        if candidate is None:
            candidate = ObjectGcCandidate(
                candidate_id=str(uuid.uuid4()),
                object_key=object_key,
                payload_sha256=digest,
                state="pending",
                not_before=not_before,
                attempts=0,
                last_error=None,
                created_at=now,
                completed_at=None,
            )
            session.add(candidate)
            return candidate
        if candidate.payload_sha256 != digest:
            raise ValueError("artifact garbage-collection digest does not match")
        if candidate.state in ("deleted", "missing", "failed"):
            candidate.state = "pending"
            candidate.not_before = not_before
            candidate.attempts = 0
            candidate.last_error = None
            candidate.completed_at = None
        return candidate

    def _reserve_artifacts(self, artifacts: dict[str, str]) -> None:
        now = utc_now()
        not_before = now + timedelta(seconds=self._orphan_gc_grace_seconds)
        with self._sessions.begin() as session:
            for object_key, digest in sorted(artifacts.items()):
                lock_stream(session, f"object:{object_key}")
                self._ensure_artifact_candidate(
                    session, object_key, digest, now, not_before
                )

    @staticmethod
    def _protect_artifacts(
        session: Session, artifacts: dict[str, str], now
    ) -> None:
        for object_key, digest in artifacts.items():
            candidate = session.scalar(
                select(ObjectGcCandidate).where(
                    ObjectGcCandidate.object_key == object_key
                )
            )
            if candidate is None or candidate.payload_sha256 != digest:
                raise ValueError("artifact garbage-collection reservation is missing")
            candidate.state = "protected"
            candidate.last_error = None
            candidate.completed_at = now

    def execute(self, lease: WorkLease) -> bool:
        try:
            with self._sessions() as session:
                record = session.get(OutboxRecord, lease.outbox_id)
                if (
                    record is None
                    or record.state != "processing"
                    or record.lease_owner != lease.owner
                    or record.lease_generation != lease.generation
                ):
                    return False
                tenant_id, cluster_id, attempt_id, generation = self._request(record)
                inputs = self._inputs(session, tenant_id, cluster_id, attempt_id, generation)
                if not inputs:
                    raise ValueError("report request has no normalized inputs")

            fingerprint = self._input_fingerprint(inputs)
            parquet_segments = []
            total_input_bytes = 0
            for item in inputs:
                segment_payload = self._objects.read_verified(
                    item.object_key, item.payload_sha256
                )
                total_input_bytes += len(segment_payload)
                if total_input_bytes > self._max_input_bytes:
                    raise ValueError("Parquet input byte limit exceeded")
                parquet_segments.append(
                    ParquetSegment(
                        receipt_id=item.receipt_id,
                        payload_sha256=item.payload_sha256,
                        expected_records=item.record_count,
                        payload=segment_payload,
                    )
                )
            parquet = build_normalized_parquet(
                tenant_id=tenant_id,
                cluster_id=cluster_id,
                attempt_id=attempt_id,
                segments=parquet_segments,
                max_segments=self._max_segments,
                max_records=self._max_records,
                max_input_bytes=self._max_input_bytes,
            )
            parquet_digest = hashlib.sha256(parquet.payload).hexdigest()
            parquet_key = self._object_key(
                tenant_id, cluster_id, attempt_id, parquet_digest, "parquet"
            )
            payload = self._report_payload(
                tenant_id,
                cluster_id,
                attempt_id,
                generation,
                fingerprint,
                inputs,
                parquet_digest,
                parquet.schema_version,
                parquet.row_count,
            )
            report_digest = hashlib.sha256(payload).hexdigest()
            object_key = self._object_key(
                tenant_id, cluster_id, attempt_id, report_digest, "json"
            )
            artifacts = {
                object_key: report_digest,
                parquet_key: parquet_digest,
            }
            self._reserve_artifacts(artifacts)

            with self._sessions.begin() as session:
                record = session.get(OutboxRecord, lease.outbox_id)
                if (
                    record is None
                    or record.state != "processing"
                    or record.lease_owner != lease.owner
                    or record.lease_generation != lease.generation
                ):
                    return False
                # Serialize generation validation and publication with admission/deletion.
                # A deletion that wins this lock is observed below; one that loses it
                # must discover and erase this revision before completing.
                lock_stream(session, f"attempt:{tenant_id}:{cluster_id}:{attempt_id}")
                lock_stream(session, f"report:{tenant_id}:{cluster_id}:{attempt_id}")
                generation_state = session.get(
                    AttemptGeneration, (tenant_id, cluster_id, attempt_id)
                )
                if (
                    generation_state is None
                    or generation_state.deleted_at is not None
                    or generation_state.generation != generation
                ):
                    record.state = "suppressed"
                    record.lease_owner = None
                    record.lease_expires_at = None
                    record.last_error = "attempt deletion generation changed"
                    return False
                current_inputs = self._inputs(
                    session, tenant_id, cluster_id, attempt_id, generation
                )
                if self._input_fingerprint(current_inputs) != fingerprint:
                    record.state = "pending"
                    record.lease_owner = None
                    record.lease_expires_at = None
                    record.last_error = "normalized inputs changed during report build"
                    return False

                now = utc_now()
                not_before = now + timedelta(seconds=self._orphan_gc_grace_seconds)
                for artifact_key, digest in sorted(artifacts.items()):
                    lock_stream(session, f"object:{artifact_key}")
                    self._ensure_artifact_candidate(
                        session, artifact_key, digest, now, not_before
                    )
                self._objects.put_verified(parquet_key, parquet.payload, parquet_digest)
                self._objects.put_verified(object_key, payload, report_digest)

                head_key = (tenant_id, cluster_id, attempt_id)
                head = session.get(ReportHead, head_key)
                if (
                    head is not None
                    and head.deletion_generation == generation
                    and head.input_fingerprint == fingerprint
                ):
                    record.state = "completed"
                    record.lease_owner = None
                    record.lease_expires_at = None
                    record.last_error = None
                    self._protect_artifacts(session, artifacts, now)
                    return True

                existing = session.scalar(
                    select(ReportRevision).where(
                        ReportRevision.tenant_id == tenant_id,
                        ReportRevision.cluster_id == cluster_id,
                        ReportRevision.attempt_id == attempt_id,
                        ReportRevision.deletion_generation == generation,
                        ReportRevision.input_fingerprint == fingerprint,
                    )
                )
                if existing is None:
                    revision_number = (
                        head.revision_number + 1
                        if head is not None and head.deletion_generation == generation
                        else 1
                    )
                    existing = ReportRevision(
                        revision_id=str(uuid.uuid4()),
                        tenant_id=tenant_id,
                        cluster_id=cluster_id,
                        attempt_id=attempt_id,
                        deletion_generation=generation,
                        revision_number=revision_number,
                        input_fingerprint=fingerprint,
                        segment_count=len(inputs),
                        record_count=sum(item.record_count for item in inputs),
                        report_object_key=object_key,
                        report_sha256=report_digest,
                        parquet_object_key=parquet_key,
                        parquet_sha256=parquet_digest,
                        parquet_schema_version=parquet.schema_version,
                        parquet_row_count=parquet.row_count,
                        builder_version=self.BUILDER_VERSION,
                        created_at=now,
                    )
                    session.add(existing)
                    session.flush()
                elif (
                    existing.report_object_key != object_key
                    or existing.report_sha256 != report_digest
                    or existing.parquet_object_key != parquet_key
                    or existing.parquet_sha256 != parquet_digest
                ):
                    raise ValueError("existing report revision artifacts differ")
                if head is None:
                    head = ReportHead(
                        tenant_id=tenant_id,
                        cluster_id=cluster_id,
                        attempt_id=attempt_id,
                        deletion_generation=generation,
                        revision_number=existing.revision_number,
                        revision_id=existing.revision_id,
                        input_fingerprint=fingerprint,
                        updated_at=now,
                    )
                    session.add(head)
                else:
                    head.deletion_generation = generation
                    head.revision_number = existing.revision_number
                    head.revision_id = existing.revision_id
                    head.input_fingerprint = fingerprint
                    head.updated_at = now
                record.state = "completed"
                record.lease_owner = None
                record.lease_expires_at = None
                record.last_error = None
                self._protect_artifacts(session, artifacts, now)
                return True
        except (ObjectIntegrityError, json.JSONDecodeError, ValueError) as exc:
            self.fail(lease, str(exc))
            return False

    def fail(self, lease: WorkLease, message: str) -> None:
        with self._sessions.begin() as session:
            record = session.get(OutboxRecord, lease.outbox_id)
            if (
                record is None
                or record.lease_owner != lease.owner
                or record.lease_generation != lease.generation
            ):
                return
            record.state = "failed" if record.attempts >= 5 else "pending"
            record.lease_owner = None
            record.lease_expires_at = None
            record.last_error = message[:512]

    def run_once(self) -> bool:
        lease = self.claim()
        if lease is None:
            return False
        self.execute(lease)
        return True


def main() -> int:
    from .database import assert_schema_ready, build_engine, build_session_factory, initialize_schema
    from .settings import Settings
    from .storage import LocalObjectStore

    settings = Settings.from_environment()
    engine = build_engine(settings.database_url)
    if settings.bootstrap_schema:
        initialize_schema(engine)
    else:
        assert_schema_ready(engine)
    sessions = build_session_factory(engine)
    objects = LocalObjectStore(settings.object_root)
    deletion_worker = AttemptDeletionWorker(sessions, objects)
    worker = OutboxWorker(sessions, objects)
    report_worker = ReportRevisionWorker(
        sessions,
        objects,
        orphan_gc_grace_seconds=settings.object_gc_grace_seconds,
    )
    reservation_sweeper = ReservationSweeper(
        sessions,
        ttl_seconds=settings.reservation_ttl_seconds,
        gc_grace_seconds=settings.object_gc_grace_seconds,
    )
    object_gc = ObjectGcWorker(sessions, objects)
    object_gc_audit = ObjectGcAuditWorker(
        sessions,
        objects,
        gc_grace_seconds=settings.object_gc_grace_seconds,
        audit_interval_seconds=settings.object_gc_audit_interval_seconds,
        batch_size=settings.object_gc_audit_batch_size,
    )
    next_reservation_sweep = 0.0
    next_object_gc_sweep = 0.0
    next_object_gc_audit_sweep = 0.0
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping:
            worked = deletion_worker.run_once()
            worked = worker.run_once() or worked
            worked = report_worker.run_once() or worked
            monotonic_now = time.monotonic()
            if monotonic_now >= next_reservation_sweep:
                worked = reservation_sweeper.run_once() > 0 or worked
                next_reservation_sweep = monotonic_now + settings.reservation_sweep_seconds
            if monotonic_now >= next_object_gc_sweep:
                worked = object_gc.run_once() is not None or worked
                next_object_gc_sweep = monotonic_now + settings.object_gc_sweep_seconds
            if monotonic_now >= next_object_gc_audit_sweep:
                worked = bool(object_gc_audit.run_once()) or worked
                next_object_gc_audit_sweep = (
                    monotonic_now + settings.object_gc_audit_sweep_seconds
                )
            if not worked:
                time.sleep(0.5)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
