"""Scheduler-neutral read-only lifecycle contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Protocol, Sequence


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
