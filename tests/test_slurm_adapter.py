from __future__ import annotations

import unittest
from datetime import datetime, timezone

from ranklens_enterprise.scheduler import ExecutionState
from ranklens_enterprise.slurm import SlurmAdapterError, parse_sacct_json


class SlurmAdapterTests(unittest.TestCase):
    def test_normalizes_state_array_and_capability_wrapped_values(self) -> None:
        observed = datetime(2026, 9, 10, tzinfo=timezone.utc)
        payload = {
            "jobs": [
                {
                    "job_id": 101,
                    "array": {"job_id": 100, "task_id": 7},
                    "state": {"current": ["COMPLETED"], "reason": "None"},
                    "submit_time": {"set": True, "number": 1_700_000_000},
                    "start_time": {"set": True, "number": 1_700_000_010},
                    "end_time": {"set": True, "number": 1_700_000_020},
                    "allocation_nodes": {"set": True, "number": 2},
                    "allocation_cpus": 64,
                    "restart_count": 1,
                    "account": "science",
                    "partition": "compute",
                    "user": "opaque-user-ref",
                }
            ]
        }

        attempt = parse_sacct_json(payload, cluster_id="cluster-a", observed_at=observed)[0]

        self.assertEqual(attempt.state, ExecutionState.SUCCEEDED)
        self.assertEqual(attempt.array_job_id, "100")
        self.assertEqual(attempt.array_task_id, "7")
        self.assertEqual(attempt.allocated_nodes, 2)
        self.assertEqual(attempt.allocated_cpus, 64)
        self.assertEqual(attempt.restart_count, 1)
        self.assertEqual(attempt.observed_at, observed)
        self.assertIn(":derived:", attempt.source_identity)

    def test_prefers_sluid_and_keeps_unknown_state_unknown(self) -> None:
        payload = {
            "jobs": [
                {
                    "job_id": "44",
                    "sluid": "cluster=alpha,id=44",
                    "original_sluid": "cluster=alpha,id=41",
                    "state": {"current": ["FUTURE_STATE"], "reason": "new reason"},
                }
            ]
        }

        attempt = parse_sacct_json(payload, cluster_id="cluster-a")[0]

        self.assertEqual(attempt.state, ExecutionState.UNKNOWN)
        self.assertIn("cluster=alpha,id=41", attempt.source_identity)
        self.assertEqual(attempt.state_reason, "new reason")

    def test_requeue_generation_changes_derived_identity(self) -> None:
        first = {"job_id": 55, "submit_time": 100, "restart_count": 0, "state": "RUNNING"}
        second = {"job_id": 55, "submit_time": 100, "restart_count": 1, "state": "RUNNING"}

        identities = [
            item.source_identity
            for item in parse_sacct_json({"jobs": [first, second]}, cluster_id="cluster-a")
        ]

        self.assertNotEqual(identities[0], identities[1])

    def test_rejects_unstructured_sacct_output(self) -> None:
        with self.assertRaisesRegex(SlurmAdapterError, "jobs list"):
            parse_sacct_json({}, cluster_id="cluster-a")


if __name__ == "__main__":
    unittest.main()
