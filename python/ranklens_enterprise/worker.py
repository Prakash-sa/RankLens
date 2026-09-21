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

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .database import (
    AdmissionReservation,
    AttemptGeneration,
    NormalizedSegment,
    OutboxRecord,
    ReportHead,
    ReportRevision,
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


class ReservationSweeper:
    """Expires abandoned pre-admission reservations in bounded transactions."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        ttl_seconds: int = 900,
        batch_size: int = 100,
        clock=utc_now,
    ):
        if ttl_seconds < 60 or ttl_seconds > 86400:
            raise ValueError("ttl_seconds must be within [60, 86400]")
        if batch_size < 1 or batch_size > 1000:
            raise ValueError("batch_size must be within [1, 1000]")
        self._sessions = sessions
        self._ttl_seconds = ttl_seconds
        self._batch_size = batch_size
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
            return len(reservations)


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
    ):
        self._sessions = sessions
        self._objects = objects
        self._owner = owner or f"report-worker-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds
        self._max_segments = max_segments
        self._max_records = max_records
        self._max_input_bytes = max_input_bytes

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
            self._objects.put_verified(parquet_key, parquet.payload, parquet_digest)
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
            self._objects.put_verified(object_key, payload, report_digest)

            with self._sessions.begin() as session:
                record = session.get(OutboxRecord, lease.outbox_id)
                if (
                    record is None
                    or record.state != "processing"
                    or record.lease_owner != lease.owner
                    or record.lease_generation != lease.generation
                ):
                    return False
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
                now = utc_now()
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
    worker = OutboxWorker(sessions, objects)
    report_worker = ReportRevisionWorker(sessions, objects)
    reservation_sweeper = ReservationSweeper(
        sessions, ttl_seconds=settings.reservation_ttl_seconds
    )
    next_reservation_sweep = 0.0
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping:
            worked = worker.run_once()
            worked = report_worker.run_once() or worked
            monotonic_now = time.monotonic()
            if monotonic_now >= next_reservation_sweep:
                worked = reservation_sweeper.run_once() > 0 or worked
                next_reservation_sweep = monotonic_now + settings.reservation_sweep_seconds
            if not worked:
                time.sleep(0.5)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
