"""Scheduler-neutral read-only lifecycle contract."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional, Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import SchedulerObservation, lock_stream, utc_now


class ExecutionState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETING = "completing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"
    TIMED_OUT = "timed_out"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SchedulerAttempt:
    adapter: str
    adapter_version: str
    cluster_id: str
    source_identity: str
    job_id: str
    array_job_id: Optional[str]
    array_task_id: Optional[str]
    step_id: Optional[str]
    restart_count: int
    state: ExecutionState
    state_reason: Optional[str]
    submitted_at: Optional[datetime]
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    allocated_nodes: Optional[int]
    allocated_cpus: Optional[int]
    account: Optional[str]
    partition: Optional[str]
    user_ref: Optional[str]
    observed_at: datetime
    raw: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["state"] = self.state.value
        for key in ("submitted_at", "started_at", "ended_at", "observed_at"):
            if value[key] is not None:
                value[key] = value[key].isoformat()
        return value


class SchedulerAdapter(Protocol):
    def fetch_attempts(self, job_ids: Sequence[str] = ()) -> Sequence[SchedulerAttempt]: ...


@dataclass(frozen=True)
class SchedulerReconcileResult:
    fetched: int
    inserted: int
    duplicates: int


def _compact_raw(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _observation_from_attempt(
    tenant_id: str,
    attempt: SchedulerAttempt,
    now: datetime,
) -> SchedulerObservation:
    return SchedulerObservation(
        observation_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        cluster_id=attempt.cluster_id,
        adapter=attempt.adapter,
        adapter_version=attempt.adapter_version,
        source_identity=attempt.source_identity,
        job_id=attempt.job_id,
        array_job_id=attempt.array_job_id,
        array_task_id=attempt.array_task_id,
        step_id=attempt.step_id,
        restart_count=attempt.restart_count,
        state=attempt.state.value,
        state_reason=attempt.state_reason,
        submitted_at=attempt.submitted_at,
        started_at=attempt.started_at,
        ended_at=attempt.ended_at,
        allocated_nodes=attempt.allocated_nodes,
        allocated_cpus=attempt.allocated_cpus,
        account=attempt.account,
        partition=attempt.partition,
        user_ref=attempt.user_ref,
        observed_at=attempt.observed_at,
        raw_json=_compact_raw(attempt.raw),
        first_seen_at=now,
    )


class SchedulerReconciler:
    """Persist read-only scheduler observations without inventing finality.

    The reconciler records every distinct adapter observation for a tenant and
    cluster. It deliberately does not collapse terminal state into telemetry
    success, because scheduler outcome, telemetry completeness, and scientific
    validity remain separate evidence streams.
    """

    def __init__(
        self,
        session_factory: Callable[[], Session],
        adapter: SchedulerAdapter,
        *,
        tenant_id: str,
    ):
        if not tenant_id:
            raise ValueError("tenant_id is required")
        self._session_factory = session_factory
        self._adapter = adapter
        self._tenant_id = tenant_id

    def poll_once(self, job_ids: Sequence[str] = ()) -> SchedulerReconcileResult:
        attempts = list(self._adapter.fetch_attempts(job_ids))
        now = utc_now()
        inserted = 0
        duplicates = 0
        with self._session_factory() as session:
            for attempt in attempts:
                lock_stream(
                    session,
                    ":".join((
                        "scheduler",
                        self._tenant_id,
                        attempt.cluster_id,
                        attempt.source_identity,
                    )),
                )
                existing = session.execute(
                    select(SchedulerObservation.observation_id).where(
                        SchedulerObservation.tenant_id == self._tenant_id,
                        SchedulerObservation.cluster_id == attempt.cluster_id,
                        SchedulerObservation.source_identity == attempt.source_identity,
                        SchedulerObservation.observed_at == attempt.observed_at,
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    duplicates += 1
                    continue
                session.add(_observation_from_attempt(self._tenant_id, attempt, now))
                inserted += 1
            session.commit()
        return SchedulerReconcileResult(
            fetched=len(attempts),
            inserted=inserted,
            duplicates=duplicates,
        )
