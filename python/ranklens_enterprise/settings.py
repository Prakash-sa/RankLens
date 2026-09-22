"""Fail-closed service configuration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, FrozenSet


@dataclass(frozen=True)
class MachinePrincipal:
    tenant_id: str
    clusters: FrozenSet[str]


@dataclass(frozen=True)
class Settings:
    database_url: str
    object_root: Path
    machine_tokens: Dict[str, MachinePrincipal]
    max_expanded_segment_bytes: int = 8 * 1024 * 1024
    reservation_ttl_seconds: int = 900
    reservation_sweep_seconds: int = 30
    object_gc_grace_seconds: int = 3600
    object_gc_sweep_seconds: int = 30
    bootstrap_schema: bool = False

    @classmethod
    def from_environment(cls) -> "Settings":
        raw_tokens = os.environ.get("RANKLENS_MACHINE_TOKENS_JSON", "")
        if not raw_tokens:
            raise RuntimeError("RANKLENS_MACHINE_TOKENS_JSON is required")
        try:
            decoded = json.loads(raw_tokens)
        except json.JSONDecodeError as exc:
            raise RuntimeError("RANKLENS_MACHINE_TOKENS_JSON must be valid JSON") from exc
        if not isinstance(decoded, dict) or not decoded:
            raise RuntimeError("at least one machine token is required")

        principals: Dict[str, MachinePrincipal] = {}
        for token, value in decoded.items():
            if not isinstance(token, str) or len(token) < 24 or not isinstance(value, dict):
                raise RuntimeError("machine tokens must be at least 24 characters")
            tenant = value.get("tenant_id")
            clusters = value.get("clusters")
            if not isinstance(tenant, str) or not tenant or not isinstance(clusters, list):
                raise RuntimeError("each machine token requires tenant_id and clusters")
            if not clusters or not all(isinstance(item, str) and item for item in clusters):
                raise RuntimeError("machine-token clusters must be a nonempty string list")
            principals[token] = MachinePrincipal(tenant, frozenset(clusters))

        database_url = os.environ.get("RANKLENS_DATABASE_URL")
        object_root = os.environ.get("RANKLENS_OBJECT_ROOT")
        if not database_url or not object_root:
            raise RuntimeError("RANKLENS_DATABASE_URL and RANKLENS_OBJECT_ROOT are required")
        try:
            reservation_ttl = int(os.environ.get("RANKLENS_RESERVATION_TTL_SECONDS", "900"))
            reservation_sweep = int(os.environ.get("RANKLENS_RESERVATION_SWEEP_SECONDS", "30"))
            object_gc_grace = int(os.environ.get("RANKLENS_OBJECT_GC_GRACE_SECONDS", "3600"))
            object_gc_sweep = int(os.environ.get("RANKLENS_OBJECT_GC_SWEEP_SECONDS", "30"))
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
        return cls(
            database_url=database_url,
            object_root=Path(object_root),
            machine_tokens=principals,
            reservation_ttl_seconds=reservation_ttl,
            reservation_sweep_seconds=reservation_sweep,
            object_gc_grace_seconds=object_gc_grace,
            object_gc_sweep_seconds=object_gc_sweep,
            bootstrap_schema=os.environ.get("RANKLENS_BOOTSTRAP_SCHEMA", "0").lower()
            in {"1", "true", "yes"},
        )
