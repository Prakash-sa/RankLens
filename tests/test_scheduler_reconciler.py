from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from sqlalchemy import select

from ranklens_enterprise.database import (
    SchedulerObservation,
    SchedulerPollCursor,
    build_engine,
    build_session_factory,
    initialize_schema,
)
from ranklens_enterprise.scheduler import (
    ExecutionState,
    SchedulerAttempt,
    SchedulerPoller,
    SchedulerReconciler,
)


class StaticSchedulerAdapter:
    def __init__(self, attempts: Sequence[SchedulerAttempt]):
        self.attempts = list(attempts)
        self.requested_job_ids: Sequence[str] = ()

    def fetch_attempts(
        self,
        job_ids: Sequence[str] = (),
        *,
        since: Optional[datetime] = None,
    ) -> Sequence[SchedulerAttempt]:
        self.requested_job_ids = tuple(job_ids)
        self.since = since
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

    def tearDown(self) -> None:
        self.engine.dispose()

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


class FailingSchedulerAdapter(StaticSchedulerAdapter):
    def fetch_attempts(
        self,
        job_ids: Sequence[str] = (),
        *,
        since: Optional[datetime] = None,
    ) -> Sequence[SchedulerAttempt]:
        raise RuntimeError("accounting unavailable")


class MutableClock:
    def __init__(self, value: datetime):
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class SchedulerPollerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite:///:memory:")
        initialize_schema(self.engine)
        self.sessions = build_session_factory(self.engine)
        self.clock = MutableClock(datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc))

    def tearDown(self) -> None:
        self.engine.dispose()

    def poller(self, adapter: StaticSchedulerAdapter, *, owner: str = "poller-a") -> SchedulerPoller:
        return SchedulerPoller(
            self.sessions,
            lambda _cluster_id: adapter,
            owner=owner,
            poll_interval_seconds=30,
            lease_seconds=60,
            accounting_lookback_seconds=300,
            max_backoff_seconds=300,
            jitter_ratio=0,
            clock=self.clock,
        )

    def cursor(self) -> SchedulerPollCursor:
        with self.sessions() as session:
            value = session.get(SchedulerPollCursor, ("tenant-a", "cluster-a"))
            assert value is not None
            session.expunge(value)
            return value

    def test_success_advances_cursor_and_overlaps_next_query(self) -> None:
        adapter = StaticSchedulerAdapter([attempt_at(self.clock())])
        poller = self.poller(adapter)
        poller.register("tenant-a", "cluster-a")

        first = poller.claim()
        assert first is not None
        first_result = poller.execute(first)

        self.assertTrue(first_result.published)
        self.assertIsNone(adapter.since)
        saved = self.cursor()
        self.assertEqual(saved.cursor_at.replace(tzinfo=timezone.utc), self.clock())
        self.assertEqual(saved.consecutive_failures, 0)
        self.assertIsNone(poller.claim())

        self.clock.advance(30)
        adapter.attempts = []
        second = poller.claim()
        assert second is not None
        second_result = poller.execute(second)

        self.assertTrue(second_result.published)
        self.assertEqual(adapter.since, datetime(2026, 9, 18, 11, 55, tzinfo=timezone.utc))

    def test_failure_releases_lease_and_applies_exponential_backoff(self) -> None:
        adapter = FailingSchedulerAdapter([])
        poller = self.poller(adapter)
        poller.register("tenant-a", "cluster-a")

        first = poller.claim()
        assert first is not None
        result = poller.execute(first)

        self.assertFalse(result.published)
        self.assertIn("accounting unavailable", result.error or "")
        saved = self.cursor()
        self.assertEqual(saved.consecutive_failures, 1)
        self.assertIsNone(saved.lease_owner)
        self.assertEqual(
            saved.next_poll_at.replace(tzinfo=timezone.utc),
            self.clock() + timedelta(seconds=30),
        )

        self.clock.advance(30)
        second = poller.claim()
        assert second is not None
        poller.execute(second)
        saved = self.cursor()
        self.assertEqual(saved.consecutive_failures, 2)
        self.assertEqual(
            saved.next_poll_at.replace(tzinfo=timezone.utc),
            self.clock() + timedelta(seconds=60),
        )

    def test_expired_lease_is_reclaimed_and_stale_owner_is_fenced(self) -> None:
        adapter = StaticSchedulerAdapter([])
        first_poller = self.poller(adapter, owner="poller-a")
        second_poller = self.poller(adapter, owner="poller-b")
        first_poller.register("tenant-a", "cluster-a")

        stale = first_poller.claim()
        assert stale is not None
        self.clock.advance(61)
        replacement = second_poller.claim()
        assert replacement is not None

        self.assertGreater(replacement.generation, stale.generation)
        self.assertFalse(first_poller.execute(stale).published)
        self.assertTrue(second_poller.execute(replacement).published)


if __name__ == "__main__":
    unittest.main()
