"""Fail-closed service configuration."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import FrozenSet, Optional, Tuple


MACHINE_PERMISSIONS = frozenset({"segments:write", "telemetry:read"})
_CREDENTIAL_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class MachinePrincipal:
    tenant_id: str
    clusters: FrozenSet[str]
    credential_id: str = "internal"
    permissions: FrozenSet[str] = MACHINE_PERMISSIONS


@dataclass(frozen=True)
class MachineCredential:
    token_sha256: str
    principal: MachinePrincipal
    not_before: Optional[datetime] = None
    expires_at: Optional[datetime] = None


@dataclass(frozen=True)
class Settings:
    database_url: str
    object_root: Path
    machine_credentials: Tuple[MachineCredential, ...]
    max_expanded_segment_bytes: int = 8 * 1024 * 1024
    reservation_ttl_seconds: int = 900
    reservation_sweep_seconds: int = 30
    object_gc_grace_seconds: int = 3600
    object_gc_sweep_seconds: int = 30
    object_gc_audit_interval_seconds: int = 86400
    object_gc_audit_sweep_seconds: int = 300
    object_gc_audit_batch_size: int = 100
    default_retention_seconds: int = 0
    retention_sweep_seconds: int = 300
    retention_sweep_batch_size: int = 100
    bootstrap_schema: bool = False

    @classmethod
    def from_environment(cls) -> "Settings":
        raw_credentials = os.environ.get("RANKLENS_MACHINE_CREDENTIALS_JSON", "")
        if not raw_credentials:
            raise RuntimeError("RANKLENS_MACHINE_CREDENTIALS_JSON is required")
        try:
            decoded = json.loads(raw_credentials)
        except json.JSONDecodeError as exc:
            raise RuntimeError("RANKLENS_MACHINE_CREDENTIALS_JSON must be valid JSON") from exc
        if not isinstance(decoded, dict) or not decoded:
            raise RuntimeError("at least one machine credential is required")

        credentials = []
        seen_digests = set()
        allowed_fields = {
            "token_sha256",
            "tenant_id",
            "clusters",
            "permissions",
            "not_before",
            "expires_at",
        }
        for credential_id, value in decoded.items():
            if (
                not isinstance(credential_id, str)
                or _CREDENTIAL_ID.fullmatch(credential_id) is None
                or not isinstance(value, dict)
            ):
                raise RuntimeError("machine credential IDs must be safe nonempty identifiers")
            unknown_fields = set(value) - allowed_fields
            if unknown_fields:
                raise RuntimeError("machine credentials contain unknown fields")
            token_sha256 = value.get("token_sha256")
            tenant = value.get("tenant_id")
            clusters = value.get("clusters")
            permissions = value.get("permissions")
            if not isinstance(token_sha256, str) or _SHA256.fullmatch(token_sha256) is None:
                raise RuntimeError("each machine credential requires a lowercase SHA-256 token digest")
            if token_sha256 in seen_digests:
                raise RuntimeError("machine credential token digests must be unique")
            if not isinstance(tenant, str) or not tenant or not isinstance(clusters, list):
                raise RuntimeError("each machine credential requires tenant_id and clusters")
            if not clusters or not all(isinstance(item, str) and item for item in clusters):
                raise RuntimeError("machine-credential clusters must be a nonempty string list")
            if (
                not isinstance(permissions, list)
                or not permissions
                or not all(isinstance(item, str) for item in permissions)
                or not set(permissions) <= MACHINE_PERMISSIONS
            ):
                raise RuntimeError("machine-credential permissions are missing or unsupported")
            not_before = cls._optional_utc_datetime(value.get("not_before"), "not_before")
            expires_at = cls._optional_utc_datetime(value.get("expires_at"), "expires_at")
            if not_before is not None and expires_at is not None and expires_at <= not_before:
                raise RuntimeError("machine credential expires_at must be after not_before")
            seen_digests.add(token_sha256)
            credentials.append(
                MachineCredential(
                    token_sha256=token_sha256,
                    principal=MachinePrincipal(
                        tenant,
                        frozenset(clusters),
                        credential_id,
                        frozenset(permissions),
                    ),
                    not_before=not_before,
                    expires_at=expires_at,
                )
            )

        database_url = os.environ.get("RANKLENS_DATABASE_URL")
        object_root = os.environ.get("RANKLENS_OBJECT_ROOT")
        if not database_url or not object_root:
            raise RuntimeError("RANKLENS_DATABASE_URL and RANKLENS_OBJECT_ROOT are required")
        try:
            reservation_ttl = int(os.environ.get("RANKLENS_RESERVATION_TTL_SECONDS", "900"))
            reservation_sweep = int(os.environ.get("RANKLENS_RESERVATION_SWEEP_SECONDS", "30"))
            object_gc_grace = int(os.environ.get("RANKLENS_OBJECT_GC_GRACE_SECONDS", "3600"))
            object_gc_sweep = int(os.environ.get("RANKLENS_OBJECT_GC_SWEEP_SECONDS", "30"))
            object_gc_audit_interval = int(
                os.environ.get("RANKLENS_OBJECT_GC_AUDIT_INTERVAL_SECONDS", "86400")
            )
            object_gc_audit_sweep = int(
                os.environ.get("RANKLENS_OBJECT_GC_AUDIT_SWEEP_SECONDS", "300")
            )
            object_gc_audit_batch = int(
                os.environ.get("RANKLENS_OBJECT_GC_AUDIT_BATCH_SIZE", "100")
            )
            default_retention = int(
                os.environ.get("RANKLENS_DEFAULT_RETENTION_SECONDS", "0")
            )
            retention_sweep = int(
                os.environ.get("RANKLENS_RETENTION_SWEEP_SECONDS", "300")
            )
            retention_sweep_batch = int(
                os.environ.get("RANKLENS_RETENTION_SWEEP_BATCH_SIZE", "100")
            )
        except ValueError as exc:
            raise RuntimeError("lifecycle timing settings must be integers") from exc
        if reservation_ttl < 60 or reservation_ttl > 86400:
            raise RuntimeError("RANKLENS_RESERVATION_TTL_SECONDS must be within [60, 86400]")
        if reservation_sweep < 1 or reservation_sweep > 3600:
            raise RuntimeError("RANKLENS_RESERVATION_SWEEP_SECONDS must be within [1, 3600]")
        if object_gc_grace < 300 or object_gc_grace > 604800:
            raise RuntimeError("RANKLENS_OBJECT_GC_GRACE_SECONDS must be within [300, 604800]")
        if object_gc_sweep < 1 or object_gc_sweep > 3600:
            raise RuntimeError("RANKLENS_OBJECT_GC_SWEEP_SECONDS must be within [1, 3600]")
        if object_gc_audit_interval < 300 or object_gc_audit_interval > 604800:
            raise RuntimeError(
                "RANKLENS_OBJECT_GC_AUDIT_INTERVAL_SECONDS must be within [300, 604800]"
            )
        if object_gc_audit_sweep < 1 or object_gc_audit_sweep > 3600:
            raise RuntimeError(
                "RANKLENS_OBJECT_GC_AUDIT_SWEEP_SECONDS must be within [1, 3600]"
            )
        if object_gc_audit_batch < 1 or object_gc_audit_batch > 10000:
            raise RuntimeError(
                "RANKLENS_OBJECT_GC_AUDIT_BATCH_SIZE must be within [1, 10000]"
            )
        if default_retention != 0 and not 3600 <= default_retention <= 315360000:
            raise RuntimeError(
                "RANKLENS_DEFAULT_RETENTION_SECONDS must be 0 or within [3600, 315360000]"
            )
        if retention_sweep < 1 or retention_sweep > 3600:
            raise RuntimeError(
                "RANKLENS_RETENTION_SWEEP_SECONDS must be within [1, 3600]"
            )
        if retention_sweep_batch < 1 or retention_sweep_batch > 10000:
            raise RuntimeError(
                "RANKLENS_RETENTION_SWEEP_BATCH_SIZE must be within [1, 10000]"
            )
        return cls(
            database_url=database_url,
            object_root=Path(object_root),
            machine_credentials=tuple(credentials),
            reservation_ttl_seconds=reservation_ttl,
            reservation_sweep_seconds=reservation_sweep,
            object_gc_grace_seconds=object_gc_grace,
            object_gc_sweep_seconds=object_gc_sweep,
            object_gc_audit_interval_seconds=object_gc_audit_interval,
            object_gc_audit_sweep_seconds=object_gc_audit_sweep,
            object_gc_audit_batch_size=object_gc_audit_batch,
            default_retention_seconds=default_retention,
            retention_sweep_seconds=retention_sweep,
            retention_sweep_batch_size=retention_sweep_batch,
            bootstrap_schema=os.environ.get("RANKLENS_BOOTSTRAP_SCHEMA", "0").lower()
            in {"1", "true", "yes"},
        )

    @staticmethod
    def _optional_utc_datetime(value: object, field: str) -> Optional[datetime]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise RuntimeError(f"machine credential {field} must be an ISO-8601 timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError(
                f"machine credential {field} must be an ISO-8601 timestamp"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise RuntimeError(f"machine credential {field} must include a UTC offset")
        return parsed.astimezone(timezone.utc)
