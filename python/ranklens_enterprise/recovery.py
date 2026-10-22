"""Authenticated deletion truth export and post-restore reconciliation."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from .database import (
    AttemptDeletion,
    AttemptGeneration,
    AttemptHold,
    RestoreTombstoneImport,
    lock_stream,
    utc_now,
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_BUNDLE_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True)
class TombstoneExport:
    bundle: bytes
    next_cursor: tuple[str, str, str] | None


@dataclass(frozen=True)
class TombstoneImportResult:
    bundle_sha256: str
    entry_count: int
    applied_count: int
    stale_count: int
    replayed: bool


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def export_completed_tombstones(
    sessions: sessionmaker[Session],
    secret: str,
    *,
    created_at: datetime,
    after: tuple[str, str, str] | None = None,
    limit: int = 1000,
) -> TombstoneExport:
    if not 1 <= limit <= 10_000:
        raise ValueError("tombstone export limit must be within [1, 10000]")
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("tombstone export time must be timezone-aware")
    if not 32 <= len(secret) <= 1024:
        raise ValueError("tombstone signing secret must contain 32 to 1024 characters")
    with sessions() as session:
        statement = (
            select(AttemptDeletion)
            .where(AttemptDeletion.state == "completed")
            .order_by(
                AttemptDeletion.tenant_id,
                AttemptDeletion.cluster_id,
                AttemptDeletion.attempt_id,
            )
            .limit(limit + 1)
        )
        if after is not None:
            tenant_id, cluster_id, attempt_id = after
            statement = statement.where(or_(
                AttemptDeletion.tenant_id > tenant_id,
                and_(
                    AttemptDeletion.tenant_id == tenant_id,
                    AttemptDeletion.cluster_id > cluster_id,
                ),
                and_(
                    AttemptDeletion.tenant_id == tenant_id,
                    AttemptDeletion.cluster_id == cluster_id,
                    AttemptDeletion.attempt_id > attempt_id,
                ),
            ))
        rows = list(session.scalars(statement))
    page = rows[:limit]
    entries = [{
        "tenant_id": row.tenant_id,
        "cluster_id": row.cluster_id,
        "attempt_id": row.attempt_id,
        "deletion_generation": row.deletion_generation,
        "requested_at": _iso_utc(row.requested_at),
        "completed_at": _iso_utc(row.completed_at) if row.completed_at else None,
    } for row in page]
    payload = {
        "schema_version": 1,
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "entries": entries,
    }
    payload_bytes = _canonical(payload)
    signature = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
    bundle = _canonical({"payload": payload, "signature": f"sha256={signature}"})
    next_cursor = None
    if len(rows) > limit and page:
        last = page[-1]
        next_cursor = (last.tenant_id, last.cluster_id, last.attempt_id)
    return TombstoneExport(bundle=bundle, next_cursor=next_cursor)


def apply_completed_tombstones(
    sessions: sessionmaker[Session],
    bundle: bytes,
    secret: str,
    *,
    actor: str,
    clock=utc_now,
) -> TombstoneImportResult:
    if len(bundle) > _MAX_BUNDLE_BYTES:
        raise ValueError("tombstone bundle exceeds 10 MiB")
    if not 32 <= len(secret) <= 1024:
        raise ValueError("tombstone signing secret must contain 32 to 1024 characters")
    if not _IDENTIFIER.fullmatch(actor):
        raise ValueError("restore actor must be a safe identifier")
    try:
        envelope = json.loads(bundle)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("tombstone bundle is not valid JSON") from exc
    if not isinstance(envelope, dict) or set(envelope) != {"payload", "signature"}:
        raise ValueError("tombstone bundle envelope is invalid")
    payload = envelope["payload"]
    signature = envelope["signature"]
    expected = "sha256=" + hmac.new(
        secret.encode(), _canonical(payload), hashlib.sha256
    ).hexdigest()
    if not isinstance(signature, str) or not hmac.compare_digest(signature, expected):
        raise ValueError("tombstone bundle signature is invalid")
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "created_at", "entries"}
        or payload["schema_version"] != 1
        or not isinstance(payload["entries"], list)
        or len(payload["entries"]) > 10_000
    ):
        raise ValueError("tombstone bundle payload is invalid")
    source_created_at = _parse_time(payload["created_at"], "created_at")
    entries = [_parse_entry(entry) for entry in payload["entries"]]
    if entries != sorted(entries, key=lambda item: item[:3]) or len({item[:3] for item in entries}) != len(entries):
        raise ValueError("tombstone entries must be unique and canonically ordered")
    bundle_sha256 = hashlib.sha256(bundle).hexdigest()
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise RuntimeError("restore clock must be timezone-aware")
    with sessions.begin() as session:
        existing = session.get(RestoreTombstoneImport, bundle_sha256)
        if existing is not None:
            return TombstoneImportResult(
                bundle_sha256, existing.entry_count, existing.applied_count,
                existing.stale_count, True,
            )
        applied = 0
        stale = 0
        for tenant_id, cluster_id, attempt_id, generation_value, requested_at, completed_at in entries:
            identity = (tenant_id, cluster_id, attempt_id)
            lock_stream(session, f"attempt:{tenant_id}:{cluster_id}:{attempt_id}")
            generation = session.get(AttemptGeneration, identity)
            if generation is not None and generation.generation > generation_value:
                if generation.deleted_at is None:
                    raise ValueError(
                        "tombstone conflicts with a newer live attempt generation"
                    )
                stale += 1
                continue
            if generation is None:
                generation = AttemptGeneration(
                    tenant_id=tenant_id, cluster_id=cluster_id,
                    attempt_id=attempt_id, generation=generation_value,
                    deleted_at=completed_at,
                )
                session.add(generation)
            else:
                generation.generation = generation_value
                generation.deleted_at = completed_at
            deletion = session.get(AttemptDeletion, identity)
            if deletion is None:
                deletion = AttemptDeletion(
                    tenant_id=tenant_id, cluster_id=cluster_id,
                    attempt_id=attempt_id, deletion_generation=generation_value,
                    state="pending", attempts=0, lease_owner=None,
                    lease_generation=0, lease_expires_at=None,
                    deleted_segment_objects=0, retained_shared_objects=0,
                    deleted_report_objects=0, deleted_catalog_rows=0,
                    last_error=None, requested_at=requested_at, completed_at=None,
                )
                session.add(deletion)
            else:
                deletion.deletion_generation = generation_value
                deletion.state = "pending"
                deletion.attempts = 0
                deletion.lease_owner = None
                deletion.lease_generation += 1
                deletion.lease_expires_at = None
                deletion.last_error = None
                deletion.requested_at = requested_at
                deletion.completed_at = None
            for hold in session.scalars(
                select(AttemptHold).where(
                    AttemptHold.tenant_id == tenant_id,
                    AttemptHold.cluster_id == cluster_id,
                    AttemptHold.attempt_id == attempt_id,
                    AttemptHold.state == "active",
                )
            ):
                hold.state = "released"
                hold.released_by_credential_id = f"restore:{actor}"[:128]
                hold.released_at = completed_at
            applied += 1
        session.add(RestoreTombstoneImport(
            bundle_sha256=bundle_sha256, source_created_at=source_created_at,
            entry_count=len(entries), applied_count=applied, stale_count=stale,
            imported_by=actor, imported_at=now,
        ))
    return TombstoneImportResult(bundle_sha256, len(entries), applied, stale, False)


def _parse_time(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"tombstone {field} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"tombstone {field} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"tombstone {field} must include an offset")
    return parsed.astimezone(timezone.utc)


def _parse_entry(entry: object) -> tuple[str, str, str, int, datetime, datetime]:
    fields = {"tenant_id", "cluster_id", "attempt_id", "deletion_generation", "requested_at", "completed_at"}
    if not isinstance(entry, dict) or set(entry) != fields:
        raise ValueError("tombstone entry is invalid")
    identifiers = (entry["tenant_id"], entry["cluster_id"], entry["attempt_id"])
    if any(not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None for value in identifiers):
        raise ValueError("tombstone identifiers are invalid")
    generation = entry["deletion_generation"]
    if not isinstance(generation, int) or isinstance(generation, bool) or not 1 <= generation <= 2**63 - 1:
        raise ValueError("tombstone deletion generation is invalid")
    requested_at = _parse_time(entry["requested_at"], "requested_at")
    completed_at = _parse_time(entry["completed_at"], "completed_at")
    if completed_at < requested_at:
        raise ValueError("tombstone completion precedes its request")
    return (*identifiers, generation, requested_at, completed_at)
