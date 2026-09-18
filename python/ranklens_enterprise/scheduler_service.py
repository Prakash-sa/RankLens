"""Long-running, read-only scheduler reconciliation service."""

from __future__ import annotations

import json
import os
import signal
import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence

from .database import assert_schema_ready, build_engine, build_session_factory, initialize_schema
from .scheduler import SchedulerPoller
from .slurm import SlurmAccountingAdapter


@dataclass(frozen=True)
class SchedulerClusterConfig:
    tenant_id: str
    cluster_id: str
    sacct: str = "sacct"
    timeout_seconds: float = 10.0


@dataclass(frozen=True)
class SchedulerServiceSettings:
    database_url: str
    clusters: Sequence[SchedulerClusterConfig]
    poll_interval_seconds: int = 30
    lease_seconds: int = 60
    accounting_lookback_seconds: int = 300
    max_backoff_seconds: int = 900
    bootstrap_schema: bool = False

    @classmethod
    def from_environment(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "SchedulerServiceSettings":
        values = os.environ if environ is None else environ
        database_url = values.get("RANKLENS_DATABASE_URL", "")
        if not database_url:
            raise RuntimeError("RANKLENS_DATABASE_URL is required")
        raw_clusters = values.get("RANKLENS_SCHEDULER_CLUSTERS_JSON", "")
        if not raw_clusters:
            raise RuntimeError("RANKLENS_SCHEDULER_CLUSTERS_JSON is required")
        try:
            decoded = json.loads(raw_clusters)
        except json.JSONDecodeError as exc:
            raise RuntimeError("RANKLENS_SCHEDULER_CLUSTERS_JSON must be valid JSON") from exc
        if not isinstance(decoded, list) or not decoded:
            raise RuntimeError("at least one scheduler cluster is required")

        allowed = {"tenant_id", "cluster_id", "sacct", "timeout_seconds"}
        clusters = []
        seen_cluster_ids = set()
        for item in decoded:
            if not isinstance(item, dict) or set(item).difference(allowed):
                raise RuntimeError("scheduler clusters contain invalid fields")
            tenant_id = item.get("tenant_id")
            cluster_id = item.get("cluster_id")
            sacct = item.get("sacct", "sacct")
            timeout_seconds = item.get("timeout_seconds", 10.0)
            if not isinstance(tenant_id, str) or not tenant_id:
                raise RuntimeError("scheduler cluster tenant_id is required")
            if not isinstance(cluster_id, str) or not cluster_id:
                raise RuntimeError("scheduler cluster cluster_id is required")
            if cluster_id in seen_cluster_ids:
                raise RuntimeError("scheduler cluster_id values must be unique")
            if not isinstance(sacct, str) or not sacct:
                raise RuntimeError("scheduler cluster sacct must be a command path")
            if (
                isinstance(timeout_seconds, bool)
                or not isinstance(timeout_seconds, (int, float))
                or timeout_seconds <= 0
                or timeout_seconds > 60
            ):
                raise RuntimeError("scheduler cluster timeout_seconds must be within (0, 60]")
            seen_cluster_ids.add(cluster_id)
            clusters.append(
                SchedulerClusterConfig(
                    tenant_id=tenant_id,
                    cluster_id=cluster_id,
                    sacct=sacct,
                    timeout_seconds=float(timeout_seconds),
                )
            )

        poll_interval = _bounded_int(values, "RANKLENS_SCHEDULER_POLL_SECONDS", 30, 1, 3600)
        lease_seconds = _bounded_int(values, "RANKLENS_SCHEDULER_LEASE_SECONDS", 60, 1, 3600)
        lookback = _bounded_int(values, "RANKLENS_SCHEDULER_LOOKBACK_SECONDS", 300, 0, 86400)
        max_backoff = _bounded_int(values, "RANKLENS_SCHEDULER_MAX_BACKOFF_SECONDS", 900, 1, 86400)
        if max_backoff < poll_interval:
            raise RuntimeError("RANKLENS_SCHEDULER_MAX_BACKOFF_SECONDS must cover the poll interval")
        return cls(
            database_url=database_url,
            clusters=tuple(clusters),
            poll_interval_seconds=poll_interval,
            lease_seconds=lease_seconds,
            accounting_lookback_seconds=lookback,
            max_backoff_seconds=max_backoff,
            bootstrap_schema=values.get("RANKLENS_BOOTSTRAP_SCHEMA", "0").lower()
            in {"1", "true", "yes"},
        )


@dataclass(frozen=True)
class SchedulerService:
    poller: SchedulerPoller
    engine: Any


def _bounded_int(
    environ: Mapping[str, str],
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} must be within [{minimum}, {maximum}]")
    return value


def build_service(settings: SchedulerServiceSettings) -> SchedulerService:
    engine = build_engine(settings.database_url)
    if settings.bootstrap_schema:
        initialize_schema(engine)
    else:
        assert_schema_ready(engine)
    configs: Dict[str, SchedulerClusterConfig] = {
        config.cluster_id: config for config in settings.clusters
    }

    def adapter_for(cluster_id: str) -> SlurmAccountingAdapter:
        config = configs[cluster_id]
        return SlurmAccountingAdapter(
            cluster_id,
            sacct=config.sacct,
            timeout_seconds=config.timeout_seconds,
        )

    poller = SchedulerPoller(
        build_session_factory(engine),
        adapter_for,
        poll_interval_seconds=settings.poll_interval_seconds,
        lease_seconds=settings.lease_seconds,
        accounting_lookback_seconds=settings.accounting_lookback_seconds,
        max_backoff_seconds=settings.max_backoff_seconds,
    )
    for config in settings.clusters:
        poller.register(config.tenant_id, config.cluster_id)
    return SchedulerService(poller=poller, engine=engine)


def main() -> int:
    service = build_service(SchedulerServiceSettings.from_environment())
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while not stopping:
            if not service.poller.run_once():
                time.sleep(0.5)
    finally:
        service.engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
