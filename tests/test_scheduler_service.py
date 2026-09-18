from __future__ import annotations

import json
import unittest

from sqlalchemy import select
from sqlalchemy.orm import Session

from ranklens_enterprise.database import SchedulerPollCursor
from ranklens_enterprise.scheduler_service import SchedulerServiceSettings, build_service


class SchedulerServiceSettingsTests(unittest.TestCase):
    def test_parses_scoped_cluster_and_bounded_poll_policy(self) -> None:
        settings = SchedulerServiceSettings.from_environment(
            {
                "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
                "RANKLENS_SCHEDULER_CLUSTERS_JSON": json.dumps(
                    [
                        {
                            "tenant_id": "tenant-a",
                            "cluster_id": "cluster-a",
                            "sacct": "/opt/slurm/bin/sacct",
                            "timeout_seconds": 4.5,
                        }
                    ]
                ),
                "RANKLENS_SCHEDULER_POLL_SECONDS": "20",
                "RANKLENS_SCHEDULER_LEASE_SECONDS": "45",
                "RANKLENS_SCHEDULER_LOOKBACK_SECONDS": "600",
                "RANKLENS_SCHEDULER_MAX_BACKOFF_SECONDS": "1200",
            }
        )

        self.assertEqual(settings.poll_interval_seconds, 20)
        self.assertEqual(settings.lease_seconds, 45)
        self.assertEqual(settings.accounting_lookback_seconds, 600)
        self.assertEqual(settings.clusters[0].cluster_id, "cluster-a")
        self.assertEqual(settings.clusters[0].timeout_seconds, 4.5)

    def test_rejects_duplicate_cluster_ownership(self) -> None:
        clusters = [
            {"tenant_id": "tenant-a", "cluster_id": "shared"},
            {"tenant_id": "tenant-b", "cluster_id": "shared"},
        ]

        with self.assertRaisesRegex(RuntimeError, "cluster_id values must be unique"):
            SchedulerServiceSettings.from_environment(
                {
                    "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
                    "RANKLENS_SCHEDULER_CLUSTERS_JSON": json.dumps(clusters),
                }
            )

    def test_rejects_unknown_fields_and_invalid_backoff(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "invalid fields"):
            SchedulerServiceSettings.from_environment(
                {
                    "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
                    "RANKLENS_SCHEDULER_CLUSTERS_JSON": json.dumps(
                        [{"tenant_id": "tenant-a", "cluster_id": "a", "token": "secret"}]
                    ),
                }
            )

    def test_build_service_registers_durable_cluster_cursor(self) -> None:
        settings = SchedulerServiceSettings.from_environment(
            {
                "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
                "RANKLENS_SCHEDULER_CLUSTERS_JSON": json.dumps(
                    [{"tenant_id": "tenant-a", "cluster_id": "cluster-a"}]
                ),
                "RANKLENS_BOOTSTRAP_SCHEMA": "true",
            }
        )

        service = build_service(settings)
        try:
            with Session(service.engine) as session:
                cursors = list(session.scalars(select(SchedulerPollCursor)))
            self.assertEqual(len(cursors), 1)
            self.assertEqual(cursors[0].tenant_id, "tenant-a")
            self.assertEqual(cursors[0].cluster_id, "cluster-a")
        finally:
            service.engine.dispose()

        with self.assertRaisesRegex(RuntimeError, "must cover"):
            SchedulerServiceSettings.from_environment(
                {
                    "RANKLENS_DATABASE_URL": "sqlite:///:memory:",
                    "RANKLENS_SCHEDULER_CLUSTERS_JSON": json.dumps(
                        [{"tenant_id": "tenant-a", "cluster_id": "a"}]
                    ),
                    "RANKLENS_SCHEDULER_POLL_SECONDS": "60",
                    "RANKLENS_SCHEDULER_MAX_BACKOFF_SECONDS": "30",
                }
            )


if __name__ == "__main__":
    unittest.main()
