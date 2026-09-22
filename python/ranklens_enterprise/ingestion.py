"""Idempotent object-plus-manifest admission service."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session, sessionmaker

from .contracts import DurableReceipt, ReceiptView, SegmentUpload
from .database import (
    AdmissionReservation,
    AttemptGeneration,
    OutboxRecord,
    SegmentManifest,
    lock_stream,
    utc_now,
)
from .settings import MachinePrincipal
from .storage import ImmutableObjectStore, ObjectIntegrityError


class AdmissionError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 400, retriable: bool = False):
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.retriable = retriable


class AdmissionConflict(AdmissionError):
    def __init__(self, code: str, message: str, *, retriable: bool = False):
        super().__init__(code, message, status_code=409, retriable=retriable)


@dataclass(frozen=True)
class PreparedPayload:
    content: bytes
    object_key: str


class IngestionService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        objects: ImmutableObjectStore,
        *,
        max_expanded_segment_bytes: int = 8 * 1024 * 1024,
    ):
        self._sessions = sessions
        self._objects = objects
        self._max_expanded = max_expanded_segment_bytes

    @staticmethod
    def _stream_identity(tenant_id: str, segment: SegmentUpload) -> str:
        return ":".join(
            (
                tenant_id,
                segment.cluster_id,
                segment.producer_id,
                segment.transport_epoch,
                segment.stream_id,
            )
        )

    @staticmethod
    def _attempt_identity(tenant_id: str, segment: SegmentUpload) -> str:
        return ":".join((tenant_id, segment.cluster_id, segment.attempt_id))

    @staticmethod
    def _range_filter(tenant_id: str, segment: SegmentUpload):
        return and_(
            SegmentManifest.tenant_id == tenant_id,
            SegmentManifest.cluster_id == segment.cluster_id,
            SegmentManifest.producer_id == segment.producer_id,
            SegmentManifest.transport_epoch == segment.transport_epoch,
            SegmentManifest.stream_id == segment.stream_id,
        )

    @staticmethod
    def _exact_filter(tenant_id: str, segment: SegmentUpload):
        return and_(
            IngestionService._range_filter(tenant_id, segment),
            SegmentManifest.first_sequence == segment.first_sequence,
            SegmentManifest.last_sequence == segment.last_sequence,
        )

    def _prepare_payload(self, tenant_id: str, segment: SegmentUpload) -> PreparedPayload:
        if segment.expanded_size_bytes > self._max_expanded:
            raise AdmissionError("segment_too_large", "expanded segment exceeds site policy", status_code=413)
        try:
            payload = base64.b64decode(segment.payload_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise AdmissionError("invalid_payload_encoding", "payload_base64 is invalid") from exc
        if len(payload) != segment.expanded_size_bytes:
            raise AdmissionError("expanded_size_mismatch", "expanded_size_bytes does not match payload")
        digest = hashlib.sha256(payload).hexdigest()
        if digest != segment.payload_sha256:
            raise AdmissionError("payload_digest_mismatch", "payload_sha256 does not match payload")
        records = 0
        for line_number, line in enumerate(payload.splitlines(), start=1):
            if not line.strip():
                continue
            records += 1
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AdmissionError(
                    "invalid_ndjson", f"payload record {line_number} is not valid JSON"
                ) from exc
            if not isinstance(value, dict):
                raise AdmissionError(
                    "invalid_ndjson_record", f"payload record {line_number} must be an object"
                )
        if records != segment.record_count:
            raise AdmissionError(
                "record_count_mismatch", "record_count does not match nonempty NDJSON records"
            )

        tenant_bucket = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()[:16]
        cluster_bucket = hashlib.sha256(segment.cluster_id.encode("utf-8")).hexdigest()[:16]
        object_key = f"v2/{tenant_bucket}/{cluster_bucket}/{digest[:2]}/{digest}.segment"
        return PreparedPayload(payload, object_key)

    def _assert_scope(self, principal: MachinePrincipal, segment: SegmentUpload) -> None:
        if segment.cluster_id not in principal.clusters:
            raise AdmissionError(
                "cluster_forbidden",
                "machine principal is not authorized for this cluster",
                status_code=403,
            )

    @staticmethod
    def _current_generation(session: Session, tenant_id: str, segment: SegmentUpload) -> int:
        state = session.get(
            AttemptGeneration, (tenant_id, segment.cluster_id, segment.attempt_id)
        )
        if state is None:
            state = AttemptGeneration(
                tenant_id=tenant_id,
                cluster_id=segment.cluster_id,
                attempt_id=segment.attempt_id,
                generation=0,
                deleted_at=None,
            )
            session.add(state)
            session.flush()
        if state.deleted_at is not None:
            raise AdmissionConflict("attempt_deleted", "attempt no longer accepts telemetry")
        return state.generation

    @staticmethod
    def _receipt(manifest: SegmentManifest, *, replayed: bool) -> DurableReceipt:
        return DurableReceipt(
            receipt_id=manifest.receipt_id,
            replayed=replayed,
            object_key=manifest.object_key,
            payload_sha256=manifest.payload_sha256,
            committed_at=manifest.committed_at,
        )

    def admit(self, principal: MachinePrincipal, segment: SegmentUpload) -> DurableReceipt:
        self._assert_scope(principal, segment)
        prepared = self._prepare_payload(principal.tenant_id, segment)
        stream_identity = self._stream_identity(principal.tenant_id, segment)
        attempt_identity = self._attempt_identity(principal.tenant_id, segment)
        reservation_id: Optional[str] = None

        with self._sessions.begin() as session:
            lock_stream(session, f"attempt:{attempt_identity}")
            lock_stream(session, f"stream:{stream_identity}")
            lock_stream(session, f"object:{prepared.object_key}")
            generation = self._current_generation(session, principal.tenant_id, segment)
            if generation != segment.deletion_generation:
                raise AdmissionConflict(
                    "stale_deletion_generation",
                    "segment was captured under an obsolete deletion generation",
                )

            exact = session.scalar(select(SegmentManifest).where(self._exact_filter(principal.tenant_id, segment)))
            if exact is not None:
                if exact.payload_sha256 != segment.payload_sha256:
                    raise AdmissionConflict("range_digest_conflict", "sequence range already has different content")
                return self._receipt(exact, replayed=True)

            overlap = session.scalar(
                select(SegmentManifest).where(
                    self._range_filter(principal.tenant_id, segment),
                    SegmentManifest.first_sequence <= segment.last_sequence,
                    SegmentManifest.last_sequence >= segment.first_sequence,
                )
            )
            if overlap is not None:
                raise AdmissionConflict("sequence_overlap", "segment overlaps an admitted sequence range")

            reservation = session.scalar(
                select(AdmissionReservation).where(
                    AdmissionReservation.tenant_id == principal.tenant_id,
                    AdmissionReservation.cluster_id == segment.cluster_id,
                    AdmissionReservation.producer_id == segment.producer_id,
                    AdmissionReservation.transport_epoch == segment.transport_epoch,
                    AdmissionReservation.stream_id == segment.stream_id,
                    AdmissionReservation.first_sequence == segment.first_sequence,
                    AdmissionReservation.last_sequence == segment.last_sequence,
                )
            )
            if reservation is not None:
                if reservation.state == "committed":
                    raise AdmissionConflict(
                        "catalog_inconsistent",
                        "committed reservation is missing its manifest",
                        retriable=True,
                    )
                if reservation.state == "reserved":
                    if reservation.payload_sha256 != segment.payload_sha256:
                        raise AdmissionConflict(
                            "reservation_digest_conflict",
                            "active range has different content",
                        )
                    reservation_id = reservation.reservation_id
                else:
                    # Terminal reservations never own the range. Removing the
                    # old row gives a retry a new identity, fencing any delayed
                    # writer that still holds the former reservation id.
                    session.delete(reservation)
                    session.flush()
                    reservation = None
            if reservation is None:
                reservation_id = str(uuid.uuid4())
                session.add(
                    AdmissionReservation(
                        reservation_id=reservation_id,
                        tenant_id=principal.tenant_id,
                        cluster_id=segment.cluster_id,
                        attempt_id=segment.attempt_id,
                        producer_id=segment.producer_id,
                        transport_epoch=segment.transport_epoch,
                        stream_id=segment.stream_id,
                        first_sequence=segment.first_sequence,
                        last_sequence=segment.last_sequence,
                        payload_sha256=segment.payload_sha256,
                        object_key=prepared.object_key,
                        deletion_generation=generation,
                        state="reserved",
                        created_at=utc_now(),
                    )
                )

        try:
            self._objects.put_verified(prepared.object_key, prepared.content, segment.payload_sha256)
        except ObjectIntegrityError as exc:
            raise AdmissionError(
                "object_integrity_error", str(exc), status_code=503, retriable=True
            ) from exc

        with self._sessions.begin() as session:
            lock_stream(session, f"attempt:{attempt_identity}")
            lock_stream(session, f"stream:{stream_identity}")
            lock_stream(session, f"object:{prepared.object_key}")
            generation = self._current_generation(session, principal.tenant_id, segment)
            reservation = session.get(AdmissionReservation, reservation_id)
            if reservation is None or reservation.state != "reserved":
                raise AdmissionConflict("reservation_expired", "admission reservation is no longer valid", retriable=True)
            if generation != reservation.deletion_generation or generation != segment.deletion_generation:
                reservation.state = "fenced"
                raise AdmissionConflict(
                    "deletion_fenced_admission",
                    "deletion policy changed while the object was being stored",
                )
            if not self._objects.exists_verified(prepared.object_key, segment.payload_sha256):
                raise AdmissionError("object_missing", "verified object is unavailable", status_code=503, retriable=True)

            existing_overlap = session.scalar(
                select(SegmentManifest).where(
                    self._range_filter(principal.tenant_id, segment),
                    SegmentManifest.first_sequence <= segment.last_sequence,
                    SegmentManifest.last_sequence >= segment.first_sequence,
                )
            )
            if existing_overlap is not None:
                if (
                    existing_overlap.first_sequence == segment.first_sequence
                    and existing_overlap.last_sequence == segment.last_sequence
                    and existing_overlap.payload_sha256 == segment.payload_sha256
                ):
                    reservation.state = "committed"
                    return self._receipt(existing_overlap, replayed=True)
                reservation.state = "conflicted"
                raise AdmissionConflict("sequence_overlap", "segment overlaps an admitted sequence range")

            receipt_id = str(uuid.uuid4())
            committed_at = utc_now()
            manifest = SegmentManifest(
                receipt_id=receipt_id,
                tenant_id=principal.tenant_id,
                cluster_id=segment.cluster_id,
                attempt_id=segment.attempt_id,
                producer_id=segment.producer_id,
                transport_epoch=segment.transport_epoch,
                stream_id=segment.stream_id,
                first_sequence=segment.first_sequence,
                last_sequence=segment.last_sequence,
                record_count=segment.record_count,
                payload_sha256=segment.payload_sha256,
                object_key=prepared.object_key,
                deletion_generation=generation,
                committed_at=committed_at,
            )
            session.add(manifest)
            session.add(
                OutboxRecord(
                    outbox_id=str(uuid.uuid4()),
                    tenant_id=principal.tenant_id,
                    event_type="segment.admitted",
                    aggregate_id=receipt_id,
                    payload_json=json.dumps(
                        {"receipt_id": receipt_id, "attempt_id": segment.attempt_id},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    state="pending",
                    attempts=0,
                    created_at=committed_at,
                )
            )
            reservation.state = "committed"
            session.flush()
            return self._receipt(manifest, replayed=False)

    def get_receipt(self, tenant_id: str, receipt_id: str) -> Optional[ReceiptView]:
        with self._sessions() as session:
            manifest = session.scalar(
                select(SegmentManifest).where(
                    SegmentManifest.receipt_id == receipt_id,
                    SegmentManifest.tenant_id == tenant_id,
                )
            )
            if manifest is None:
                return None
            return ReceiptView(
                **self._receipt(manifest, replayed=False).model_dump(),
                tenant_id=manifest.tenant_id,
                cluster_id=manifest.cluster_id,
                attempt_id=manifest.attempt_id,
                producer_id=manifest.producer_id,
                transport_epoch=manifest.transport_epoch,
                stream_id=manifest.stream_id,
                first_sequence=manifest.first_sequence,
                last_sequence=manifest.last_sequence,
            )

    def delete_attempt(self, tenant_id: str, cluster_id: str, attempt_id: str) -> int:
        """Fence new admission for one attempt and return its new generation.

        Physical object/query-store erasure is a separate worker workflow. The
        API intentionally does not expose this administrative primitive yet.
        """

        identity = ":".join((tenant_id, cluster_id, attempt_id))
        with self._sessions.begin() as session:
            lock_stream(session, f"attempt:{identity}")
            state = session.get(AttemptGeneration, (tenant_id, cluster_id, attempt_id))
            if state is None:
                state = AttemptGeneration(
                    tenant_id=tenant_id,
                    cluster_id=cluster_id,
                    attempt_id=attempt_id,
                    generation=1,
                    deleted_at=utc_now(),
                )
                session.add(state)
            elif state.deleted_at is None:
                state.generation += 1
                state.deleted_at = utc_now()
            return state.generation
