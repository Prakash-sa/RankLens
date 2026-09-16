from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Sequence

from sqlalchemy import select

from ranklens_enterprise.database import SchedulerObservation, build_engine, build_session_factory, initialize_schema
from ranklens_enterprise.scheduler import ExecutionState, SchedulerAttempt, SchedulerReconciler


class StaticSchedulerAdapter:
    def __init__(self, attempts: Sequence[SchedulerAttempt]):
        self.attempts = list(attempts)
        self.requested_job_ids: Sequence[str] = ()

    def fetch_attempts(self, job_ids: Sequence[str] = ()) -> Sequence[SchedulerAttempt]:
        self.requested_job_ids = tuple(job_ids)
        return list(self.attempts)


def attempt_at(observed_at: datetime, state: ExecutionState = ExecutionState.RUNNING) -> SchedulerAttempt:
    return SchedulerAttempt(
        adapter="slurm",
        adapter_version="slurm-sacct-json-v1",
        cluster_id="cluster-a",
        source_identity="slurm:cluster-a:sluid:cluster=alpha,id=41:cluster=alpha,id=44",
        job_id="44",
        array_job_id=None,
        array_task_id=None,
        step_id=None,
        restart_count=1,
        state=state,
        state_reason=None,
        submitted_at=datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc),
        started_at=datetime(2026, 9, 16, 12, 1, tzinfo=timezone.utc),
        ended_at=None if state == ExecutionState.RUNNING else datetime(2026, 9, 16, 12, 5, tzinfo=timezone.utc),
        allocated_nodes=2,
        allocated_cpus=64,
        account="science",
        partition="compute",
        user_ref="opaque-user-ref",
        observed_at=observed_at,
        raw={"job_id": "44", "state": state.value, "volatile_order": ["b", "a"]},
    )


class SchedulerReconcilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite:///:memory:")
        initialize_schema(self.engine)
        self.sessions = build_session_factory(self.engine)

    def observations(self) -> list[SchedulerObservation]:
        with self.sessions() as session:
            return list(
                session.execute(
                    select(SchedulerObservation).order_by(SchedulerObservation.observed_at)
                ).scalars()
            )

    def test_reconciler_persists_and_deduplicates_observations(self) -> None:
        observed = datetime(2026, 9, 16, 13, 0, tzinfo=timezone.utc)
        adapter = StaticSchedulerAdapter([attempt_at(observed)])
        reconciler = SchedulerReconciler(self.sessions, adapter, tenant_id="tenant-a")

        first = reconciler.poll_once(job_ids=("44",))
        second = reconciler.poll_once(job_ids=("44",))

        self.assertEqual(adapter.requested_job_ids, ("44",))
        self.assertEqual((first.fetched, first.inserted, first.duplicates), (1, 1, 0))
        self.assertEqual((second.fetched, second.inserted, second.duplicates), (1, 0, 1))
        rows = self.observations()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].tenant_id, "tenant-a")
        self.assertEqual(rows[0].state, "running")
        self.assertEqual(rows[0].allocated_cpus, 64)
        self.assertEqual(json.loads(rows[0].raw_json)["job_id"], "44")

    def test_reconciler_keeps_later_state_observation_for_same_identity(self) -> None:
        first_seen = datetime(2026, 9, 16, 13, 0, tzinfo=timezone.utc)
        later = first_seen + timedelta(minutes=5)
        adapter = StaticSchedulerAdapter([
            attempt_at(first_seen, ExecutionState.RUNNING),
            attempt_at(later, ExecutionState.SUCCEEDED),
        ])
        reconciler = SchedulerReconciler(self.sessions, adapter, tenant_id="tenant-a")

        result = reconciler.poll_once()

        self.assertEqual((result.fetched, result.inserted, result.duplicates), (2, 2, 0))
        rows = self.observations()
        self.assertEqual([row.state for row in rows], ["running", "succeeded"])
        self.assertIsNotNone(rows[1].ended_at)


if __name__ == "__main__":
    unittest.main()
