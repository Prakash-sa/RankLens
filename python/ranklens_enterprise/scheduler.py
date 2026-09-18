"""Scheduler-neutral read-only lifecycle contract."""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, Optional, Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .database import SchedulerObservation, SchedulerPollCursor, lock_stream, utc_now


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
    def fetch_attempts(
        self,
        job_ids: Sequence[str] = (),
        *,
        since: Optional[datetime] = None,
    ) -> Sequence[SchedulerAttempt]: ...


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

    def poll_once(
        self,
        job_ids: Sequence[str] = (),
        *,
        since: Optional[datetime] = None,
    ) -> SchedulerReconcileResult:
        attempts = list(self._adapter.fetch_attempts(job_ids, since=since))
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


@dataclass(frozen=True)
class SchedulerPollLease:
    tenant_id: str
    cluster_id: str
    generation: int
    owner: str


@dataclass(frozen=True)
class SchedulerPollResult:
    lease: SchedulerPollLease
    reconciled: Optional[SchedulerReconcileResult]
    published: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class AllocationInterval:
    started_at: datetime
    ended_at: Optional[datetime]
    allocated_nodes: Optional[int]
    allocated_cpus: Optional[int]
    partition: Optional[str]


@dataclass(frozen=True)
class AllocationTimeline:
    source_identity: str
    job_id: str
    latest_state: str
    latest_observed_at: datetime
    coverage_status: str
    coverage_reasons: Sequence[str]
    intervals: Sequence[AllocationInterval]


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def derive_allocation_timeline(
    observations: Sequence[SchedulerObservation],
) -> AllocationTimeline:
    """Derive poll-sampled allocation intervals without claiming unseen changes."""

    if not observations:
        raise ValueError("at least one scheduler observation is required")
    ordered = sorted(observations, key=lambda row: _aware(row.observed_at))
    identity = ordered[0].source_identity
    if any(row.source_identity != identity for row in ordered):
        raise ValueError("observations must belong to one scheduler source identity")

    terminal_states = {
        ExecutionState.SUCCEEDED.value,
        ExecutionState.FAILED.value,
        ExecutionState.CANCELLED.value,
        ExecutionState.PREEMPTED.value,
        ExecutionState.TIMED_OUT.value,
    }
    active_states = {ExecutionState.RUNNING.value, ExecutionState.COMPLETING.value}
    intervals = []
    current: Optional[AllocationInterval] = None
    reasons = set()

    for row in ordered:
        observed_at = _aware(row.observed_at)
        started_at = _aware(row.started_at) if row.started_at is not None else None
        ended_at = _aware(row.ended_at) if row.ended_at is not None else None
        resources = (row.allocated_nodes, row.allocated_cpus, row.partition)
        has_resources = row.allocated_nodes is not None or row.allocated_cpus is not None
        is_terminal = row.state in terminal_states
        can_open = row.state in active_states or (is_terminal and started_at is not None)

        if current is None and can_open and has_resources:
            current = AllocationInterval(
                started_at=started_at or observed_at,
                ended_at=None,
                allocated_nodes=row.allocated_nodes,
                allocated_cpus=row.allocated_cpus,
                partition=row.partition,
            )
        elif current is not None and row.state in active_states:
            current_resources = (
                current.allocated_nodes,
                current.allocated_cpus,
                current.partition,
            )
            if has_resources and resources != current_resources:
                if observed_at >= current.started_at:
                    intervals.append(
                        AllocationInterval(
                            started_at=current.started_at,
                            ended_at=observed_at,
                            allocated_nodes=current.allocated_nodes,
                            allocated_cpus=current.allocated_cpus,
                            partition=current.partition,
                        )
                    )
                    current = AllocationInterval(
                        started_at=observed_at,
                        ended_at=None,
                        allocated_nodes=row.allocated_nodes,
                        allocated_cpus=row.allocated_cpus,
                        partition=row.partition,
                    )
                else:
                    reasons.add("non_monotonic_observation")

        if can_open and not has_resources:
            reasons.add("missing_allocation_size")
        if is_terminal and current is not None:
            boundary = ended_at or observed_at
            if boundary >= current.started_at:
                intervals.append(
                    AllocationInterval(
                        started_at=current.started_at,
                        ended_at=boundary,
                        allocated_nodes=current.allocated_nodes,
                        allocated_cpus=current.allocated_cpus,
                        partition=current.partition,
                    )
                )
            else:
                reasons.add("invalid_terminal_interval")
            current = None

    latest = ordered[-1]
    if current is not None:
        intervals.append(current)
    if not intervals:
        reasons.add("no_observed_allocation")
    if not any(row.started_at is not None for row in ordered):
        reasons.add("missing_scheduler_start")
    if latest.state in terminal_states and latest.ended_at is None:
        reasons.add("missing_scheduler_end")
    if latest.state not in terminal_states:
        reasons.add("attempt_not_terminal")
    # Scheduler polling can miss allocation changes between observations. Even
    # a closed interval is therefore evidence-bounded, never inferred complete.
    if intervals:
        reasons.add("poll_sampled")

    return AllocationTimeline(
        source_identity=identity,
        job_id=latest.job_id,
        latest_state=latest.state,
        latest_observed_at=_aware(latest.observed_at),
        coverage_status="observed" if intervals else "unknown",
        coverage_reasons=tuple(sorted(reasons)),
        intervals=tuple(intervals),
    )


class SchedulerPoller:
    """Runs one fenced, durable polling stream per tenant and cluster.

    The persisted cursor advances only after a successful scheduler query and
    observation transaction. Each query overlaps the prior cursor so delayed
    accounting records are revisited without creating duplicate identities.
    """

    def __init__(
        self,
        sessions: sessionmaker[Session],
        adapter_factory: Callable[[str], SchedulerAdapter],
        *,
        owner: Optional[str] = None,
        poll_interval_seconds: int = 30,
        lease_seconds: int = 60,
        accounting_lookback_seconds: int = 300,
        max_backoff_seconds: int = 900,
        jitter_ratio: float = 0.20,
        clock: Callable[[], datetime] = utc_now,
        random_source: Callable[[], float] = random.random,
    ):
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if accounting_lookback_seconds < 0:
            raise ValueError("accounting_lookback_seconds cannot be negative")
        if max_backoff_seconds < poll_interval_seconds:
            raise ValueError("max_backoff_seconds cannot be less than poll interval")
        if jitter_ratio < 0 or jitter_ratio > 1:
            raise ValueError("jitter_ratio must be within [0, 1]")
        self._sessions = sessions
        self._adapter_factory = adapter_factory
        self._owner = owner or f"scheduler-{uuid.uuid4()}"
        self._poll_interval = poll_interval_seconds
        self._lease_seconds = lease_seconds
        self._lookback = accounting_lookback_seconds
        self._max_backoff = max_backoff_seconds
        self._jitter_ratio = jitter_ratio
        self._clock = clock
        self._random = random_source

    def register(self, tenant_id: str, cluster_id: str, *, adapter: str = "slurm") -> None:
        if not tenant_id or not cluster_id or not adapter:
            raise ValueError("tenant_id, cluster_id, and adapter are required")
        now = self._clock()
        with self._sessions.begin() as session:
            cursor = session.get(SchedulerPollCursor, (tenant_id, cluster_id))
            if cursor is None:
                session.add(
                    SchedulerPollCursor(
                        tenant_id=tenant_id,
                        cluster_id=cluster_id,
                        adapter=adapter,
                        cursor_at=None,
                        next_poll_at=now,
                        lease_owner=None,
                        lease_generation=0,
                        lease_expires_at=None,
                        consecutive_failures=0,
                        last_success_at=None,
                        last_error=None,
                        created_at=now,
                        updated_at=now,
                    )
                )
            elif cursor.adapter != adapter:
                raise ValueError("registered scheduler adapter cannot be changed in place")

    def claim(self) -> Optional[SchedulerPollLease]:
        now = self._clock()
        with self._sessions.begin() as session:
            query = (
                select(SchedulerPollCursor)
                .where(
                    SchedulerPollCursor.next_poll_at <= now,
                    (SchedulerPollCursor.lease_owner.is_(None))
                    | (SchedulerPollCursor.lease_expires_at < now),
                )
                .order_by(
                    SchedulerPollCursor.next_poll_at,
                    SchedulerPollCursor.tenant_id,
                    SchedulerPollCursor.cluster_id,
                )
                .limit(1)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            cursor = session.scalar(query)
            if cursor is None:
                return None
            cursor.lease_owner = self._owner
            cursor.lease_generation += 1
            cursor.lease_expires_at = now + timedelta(seconds=self._lease_seconds)
            cursor.updated_at = now
            return SchedulerPollLease(
                cursor.tenant_id,
                cursor.cluster_id,
                cursor.lease_generation,
                self._owner,
            )

    def _delay(self, base_seconds: int) -> timedelta:
        jittered = base_seconds * (1 + self._jitter_ratio * self._random())
        return timedelta(seconds=jittered)

    def execute(self, lease: SchedulerPollLease) -> SchedulerPollResult:
        started_at = self._clock()
        with self._sessions() as session:
            cursor = session.get(SchedulerPollCursor, (lease.tenant_id, lease.cluster_id))
            if (
                cursor is None
                or cursor.lease_owner != lease.owner
                or cursor.lease_generation != lease.generation
            ):
                return SchedulerPollResult(lease, None, False)
            since = (
                _aware(cursor.cursor_at) - timedelta(seconds=self._lookback)
                if cursor.cursor_at is not None
                else None
            )

        try:
            adapter = self._adapter_factory(lease.cluster_id)
            reconciled = SchedulerReconciler(
                self._sessions,
                adapter,
                tenant_id=lease.tenant_id,
            ).poll_once(since=since)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"[:512]
            self.fail(lease, message)
            return SchedulerPollResult(lease, None, False, message)

        completed_at = self._clock()
        with self._sessions.begin() as session:
            cursor = session.get(SchedulerPollCursor, (lease.tenant_id, lease.cluster_id))
            if (
                cursor is None
                or cursor.lease_owner != lease.owner
                or cursor.lease_generation != lease.generation
            ):
                return SchedulerPollResult(lease, reconciled, False)
            cursor.cursor_at = started_at
            cursor.next_poll_at = completed_at + self._delay(self._poll_interval)
            cursor.lease_owner = None
            cursor.lease_expires_at = None
            cursor.consecutive_failures = 0
            cursor.last_success_at = completed_at
            cursor.last_error = None
            cursor.updated_at = completed_at
        return SchedulerPollResult(lease, reconciled, True)

    def fail(self, lease: SchedulerPollLease, message: str) -> bool:
        now = self._clock()
        with self._sessions.begin() as session:
            cursor = session.get(SchedulerPollCursor, (lease.tenant_id, lease.cluster_id))
            if (
                cursor is None
                or cursor.lease_owner != lease.owner
                or cursor.lease_generation != lease.generation
            ):
                return False
            cursor.consecutive_failures += 1
            exponent = min(cursor.consecutive_failures - 1, 20)
            delay = min(self._max_backoff, self._poll_interval * (2**exponent))
            cursor.next_poll_at = now + self._delay(delay)
            cursor.lease_owner = None
            cursor.lease_expires_at = None
            cursor.last_error = message[:512]
            cursor.updated_at = now
            return True

    def run_once(self) -> bool:
        lease = self.claim()
        if lease is None:
            return False
        self.execute(lease)
        return True
