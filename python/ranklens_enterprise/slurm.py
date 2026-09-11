"""Capability-tolerant, read-only Slurm accounting adapter."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .scheduler import ExecutionState, SchedulerAttempt


ADAPTER_VERSION = "slurm-sacct-json-v1"


class SlurmAdapterError(RuntimeError):
    pass


def _scalar(value: Any) -> Any:
    """Unwrap common Slurm JSON data-parser values without guessing missing data."""

    if not isinstance(value, Mapping):
        return value
    if value.get("set") is False:
        return None
    for key in ("number", "string", "value", "id"):
        if key in value and not isinstance(value[key], (dict, list)):
            return value[key]
    current = value.get("current")
    if isinstance(current, list) and current:
        return current[0]
    if isinstance(current, str):
        return current
    return None


def _text(value: Any) -> Optional[str]:
    value = _scalar(value)
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _integer(value: Any) -> Optional[int]:
    value = _scalar(value)
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _instant(value: Any) -> Optional[datetime]:
    scalar = _scalar(value)
    if scalar in (None, 0, "0", "Unknown", "N/A"):
        return None
    if isinstance(scalar, (int, float)) and not isinstance(scalar, bool):
        return datetime.fromtimestamp(float(scalar), timezone.utc)
    if isinstance(scalar, str):
        candidate = scalar.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
    return None


def _state(value: Any) -> tuple[ExecutionState, Optional[str]]:
    reason = None
    if isinstance(value, Mapping):
        reason = _text(value.get("reason"))
    token = (_text(value) or "UNKNOWN").upper().split("+")[0]
    if token in {"PENDING", "CONFIGURING", "RESV_DEL_HOLD", "REQUEUE_HOLD", "REQUEUED"}:
        state = ExecutionState.PENDING
    elif token in {"RUNNING", "SUSPENDED"}:
        state = ExecutionState.RUNNING
    elif token in {"COMPLETING", "STAGE_OUT"}:
        state = ExecutionState.COMPLETING
    elif token == "COMPLETED":
        state = ExecutionState.SUCCEEDED
    elif token in {"CANCELLED", "DEADLINE", "REVOKED"}:
        state = ExecutionState.CANCELLED
    elif token in {"PREEMPTED", "REQUEUE_FED"}:
        state = ExecutionState.PREEMPTED
    elif token == "TIMEOUT":
        state = ExecutionState.TIMED_OUT
    elif token in {
        "BOOT_FAIL", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "SPECIAL_EXIT",
        "STOPPED", "LAUNCH_FAILED",
    }:
        state = ExecutionState.FAILED
    else:
        state = ExecutionState.UNKNOWN
    return state, reason


def _source_identity(cluster_id: str, job: Mapping[str, Any]) -> str:
    sluid = _text(job.get("sluid")) or _text(job.get("SLUID"))
    original_sluid = _text(job.get("original_sluid")) or _text(job.get("OriginalSLUID"))
    if original_sluid or sluid:
        return f"slurm:{cluster_id}:sluid:{original_sluid or sluid}:{sluid or original_sluid}"
    array = job.get("array") if isinstance(job.get("array"), Mapping) else {}
    fields = (
        cluster_id,
        _text(job.get("job_id")) or "unknown",
        _text(array.get("job_id")) or _text(job.get("array_job_id")) or "",
        _text(array.get("task_id")) or _text(job.get("array_task_id")) or "",
        _text(job.get("submit_time")) or "",
        str(_integer(job.get("restart_count")) or 0),
    )
    digest = hashlib.sha256("\x1f".join(fields).encode("utf-8")).hexdigest()
    return f"slurm:{cluster_id}:derived:{digest}"


def parse_sacct_json(
    payload: Mapping[str, Any], *, cluster_id: str, observed_at: Optional[datetime] = None
) -> List[SchedulerAttempt]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise SlurmAdapterError("sacct JSON must contain a jobs list")
    observed = observed_at or datetime.now(timezone.utc)
    attempts: List[SchedulerAttempt] = []
    for job in jobs:
        if not isinstance(job, Mapping):
            raise SlurmAdapterError("sacct jobs must be objects")
        job_id = _text(job.get("job_id"))
        if job_id is None:
            raise SlurmAdapterError("sacct job is missing job_id")
        array = job.get("array") if isinstance(job.get("array"), Mapping) else {}
        state, reason = _state(job.get("state"))
        attempts.append(
            SchedulerAttempt(
                adapter="slurm",
                adapter_version=ADAPTER_VERSION,
                cluster_id=cluster_id,
                source_identity=_source_identity(cluster_id, job),
                job_id=job_id,
                array_job_id=_text(array.get("job_id")) or _text(job.get("array_job_id")),
                array_task_id=_text(array.get("task_id")) or _text(job.get("array_task_id")),
                step_id=None,
                restart_count=_integer(job.get("restart_count")) or 0,
                state=state,
                state_reason=reason,
                submitted_at=_instant(job.get("submit_time")),
                started_at=_instant(job.get("start_time")),
                ended_at=_instant(job.get("end_time")),
                allocated_nodes=_integer(job.get("allocation_nodes")) or _integer(job.get("nodes")),
                allocated_cpus=_integer(job.get("allocation_cpus")) or _integer(job.get("cpus")),
                account=_text(job.get("account")),
                partition=_text(job.get("partition")),
                user_ref=_text(job.get("user")),
                observed_at=observed,
                raw=dict(job),
            )
        )
    return attempts


class SlurmAccountingAdapter:
    """Calls only the read-only ``sacct`` JSON interface with a hard timeout."""

    def __init__(self, cluster_id: str, *, sacct: str = "sacct", timeout_seconds: float = 10.0):
        if timeout_seconds <= 0 or timeout_seconds > 60:
            raise ValueError("timeout_seconds must be within (0, 60]")
        self.cluster_id = cluster_id
        self.sacct = sacct
        self.timeout_seconds = timeout_seconds

    def fetch_attempts(self, job_ids: Sequence[str] = ()) -> Sequence[SchedulerAttempt]:
        command = [self.sacct, "--json", "--duplicates"]
        if job_ids:
            if len(job_ids) > 1000 or not all(item and "," not in item for item in job_ids):
                raise ValueError("job_ids must contain at most 1000 nonempty identifiers")
            command.extend(("--jobs", ",".join(job_ids)))
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SlurmAdapterError(f"sacct query failed: {exc}") from exc
        if completed.returncode != 0:
            safe_error = completed.stderr.strip().replace("\n", " ")[:512]
            raise SlurmAdapterError(f"sacct exited with {completed.returncode}: {safe_error}")
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise SlurmAdapterError("sacct returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise SlurmAdapterError("sacct JSON root must be an object")
        return parse_sacct_json(payload, cluster_id=self.cluster_id)
