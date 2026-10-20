"""Durable incident lifecycle for protected object-integrity failures."""

from __future__ import annotations

import uuid
import json
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .database import (
    AdmissionReservation,
    IntegrityIncident,
    IntegrityIncidentResolution,
    IntegrityNotification,
    ObjectGcCandidate,
    ReportRevision,
    SegmentManifest,
    lock_stream,
    utc_now,
)
from .storage import ImmutableObjectStore, ObjectIntegrityError


@dataclass(frozen=True)
class ObjectReferenceScope:
    tenant_id: str
    cluster_id: str
    attempt_id: str


def _enqueue_notification(
    session: Session,
    incident: IntegrityIncident,
    *,
    event_type: str,
    created_at: datetime,
) -> None:
    payload = {
        "schema_version": 1,
        "event_type": event_type,
        "incident_id": incident.incident_id,
        "tenant_id": incident.tenant_id,
        "cluster_id": incident.cluster_id,
        "attempt_id": incident.attempt_id,
        "category": incident.category,
        "state": incident.state,
        "occurrence": incident.occurrences,
        "object_key": incident.object_key,
        "payload_sha256": incident.payload_sha256,
        "observed_at": incident.last_detected_at.isoformat(),
    }
    session.add(IntegrityNotification(
        notification_id=str(uuid.uuid4()), incident_id=incident.incident_id,
        tenant_id=incident.tenant_id, cluster_id=incident.cluster_id,
        occurrence=incident.occurrences, event_type=event_type,
        payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        state="pending", attempts=0, next_attempt_at=created_at,
        lease_owner=None, lease_generation=0, lease_expires_at=None,
        last_error=None, created_at=created_at, delivered_at=None,
    ))


def object_reference_scopes(session: Session, object_key: str) -> set[ObjectReferenceScope]:
    scopes = {
        ObjectReferenceScope(row.tenant_id, row.cluster_id, row.attempt_id)
        for row in session.scalars(select(SegmentManifest).where(SegmentManifest.object_key == object_key))
    }
    scopes.update(
        ObjectReferenceScope(row.tenant_id, row.cluster_id, row.attempt_id)
        for row in session.scalars(
            select(AdmissionReservation).where(
                AdmissionReservation.object_key == object_key,
                AdmissionReservation.state.in_(("reserved", "committed")),
            )
        )
    )
    scopes.update(
        ObjectReferenceScope(row.tenant_id, row.cluster_id, row.attempt_id)
        for row in session.scalars(
            select(ReportRevision).where(
                (ReportRevision.report_object_key == object_key)
                | (ReportRevision.parquet_object_key == object_key)
            )
        )
    )
    return scopes


def record_integrity_incidents(
    session: Session,
    candidate: ObjectGcCandidate,
    *,
    category: str,
    details: str,
    detected_at: datetime,
) -> int:
    """Create or reopen one incident per tenant/cluster reference scope."""
    grouped: dict[tuple[str, str], set[str]] = {}
    for scope in object_reference_scopes(session, candidate.object_key):
        grouped.setdefault((scope.tenant_id, scope.cluster_id), set()).add(scope.attempt_id)
    for (tenant_id, cluster_id), attempts in grouped.items():
        incident = session.scalar(
            select(IntegrityIncident).where(
                IntegrityIncident.candidate_id == candidate.candidate_id,
                IntegrityIncident.tenant_id == tenant_id,
                IntegrityIncident.cluster_id == cluster_id,
                IntegrityIncident.category == category,
            )
        )
        attempt_id = next(iter(attempts)) if len(attempts) == 1 else None
        if incident is None:
            incident = IntegrityIncident(
                incident_id=str(uuid.uuid4()), candidate_id=candidate.candidate_id,
                tenant_id=tenant_id, cluster_id=cluster_id, attempt_id=attempt_id,
                object_key=candidate.object_key, payload_sha256=candidate.payload_sha256,
                category=category, state="open", details=details[:512], occurrences=1,
                first_detected_at=detected_at, last_detected_at=detected_at,
                resolved_at=None, resolved_by_credential_id=None, resolution=None,
            )
            session.add(incident)
        else:
            incident.state = "open"
            incident.attempt_id = attempt_id
            incident.details = details[:512]
            incident.occurrences += 1
            incident.last_detected_at = detected_at
            incident.resolved_at = None
            incident.resolved_by_credential_id = None
            incident.resolution = None
        _enqueue_notification(
            session, incident, event_type="integrity.incident.detected",
            created_at=detected_at,
        )
    return len(grouped)


class IntegrityIncidentService:
    """Resolve incidents only after catalog and object-store truth agree."""

    def __init__(self, sessions: sessionmaker[Session], objects: ImmutableObjectStore, *, clock=utc_now):
        self._sessions = sessions
        self._objects = objects
        self._clock = clock

    def resolve(self, tenant_id: str, cluster_id: str, incident_id: str, *, actor: str, reason: str) -> IntegrityIncident:
        reason = reason.strip()
        if not reason or len(reason) > 512:
            raise ValueError("resolution reason must contain 1 to 512 characters")
        now = self._clock()
        with self._sessions.begin() as session:
            incident = session.get(IntegrityIncident, incident_id)
            if incident is None or incident.tenant_id != tenant_id or incident.cluster_id != cluster_id:
                raise LookupError("integrity incident not found")
            if incident.state == "resolved":
                if incident.resolved_by_credential_id == actor and incident.resolution == reason:
                    return incident
                raise ValueError("integrity incident is already resolved")
            lock_stream(session, f"object:{incident.object_key}")
            scopes = object_reference_scopes(session, incident.object_key)
            still_referenced = any(s.tenant_id == tenant_id and s.cluster_id == cluster_id for s in scopes)
            if still_referenced:
                try:
                    restored = self._objects.exists_verified(incident.object_key, incident.payload_sha256)
                except ObjectIntegrityError as exc:
                    raise ValueError("protected artifact integrity is not restored") from exc
                if not restored:
                    raise ValueError("protected artifact integrity is not restored")
            incident.state = "resolved"
            incident.resolved_at = now
            incident.resolved_by_credential_id = actor
            incident.resolution = reason
            session.add(IntegrityIncidentResolution(
                resolution_id=str(uuid.uuid4()), incident_id=incident.incident_id,
                tenant_id=tenant_id, cluster_id=cluster_id,
                resolved_by_credential_id=actor, reason=reason, resolved_at=now,
            ))
            _enqueue_notification(
                session, incident, event_type="integrity.incident.resolved",
                created_at=now,
            )
            return incident
