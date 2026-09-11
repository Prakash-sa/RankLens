"""Leased, idempotent outbox worker for admitted telemetry."""

from __future__ import annotations

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
    AttemptGeneration,
    NormalizedSegment,
    OutboxRecord,
    SegmentManifest,
    utc_now,
)
from .storage import ImmutableObjectStore, ObjectIntegrityError


@dataclass(frozen=True)
class WorkLease:
    outbox_id: str
    generation: int
    owner: str


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
    worker = OutboxWorker(build_session_factory(engine), LocalObjectStore(settings.object_root))
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping:
            if not worker.run_once():
                time.sleep(0.5)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
