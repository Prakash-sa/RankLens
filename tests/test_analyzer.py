from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ranklens.analyzer import TelemetryError, analyze
from ranklens.demo import write_demo


class AnalyzerTests(unittest.TestCase):
    def test_demo_detects_straggler_collective_and_hotspot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)

            result = analyze(directory)

            self.assertEqual(result.world_size, 4)
            self.assertEqual(result.ranks_observed, 4)
            self.assertEqual(result.straggler_ranks, [3])
            self.assertGreater(result.imbalance_ratio, 1.3)
            self.assertEqual(result.operations["MPI_Allreduce"].calls, 200)
            self.assertEqual(result.communication_edges[0].source, 0)
            self.assertGreater(result.communication_edges[0].traffic_share, 0.7)
            categories = {finding.category for finding in result.findings}
            self.assertIn("imbalance", categories)
            self.assertIn("collectives", categories)
            self.assertIn("communication", categories)

    def test_rejects_missing_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(TelemetryError, "no rank"):
                analyze(Path(temporary))

    def test_rejects_invalid_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)
            with self.assertRaisesRegex(ValueError, "greater than 1.0"):
                analyze(directory, straggler_threshold=1.0)

    def test_accepts_version_two_with_separate_completion_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            summary = {
                "schema_version": 2,
                "complete": True,
                "capture_state": "finalized",
                "finalize_return_code": 0,
                "context": {"events_dropped": "0"},
                "rank": 0,
                "world_size": 1,
                "hostname": "node-a",
                "pid": 42,
                "runtime_ns": 100,
                "mpi_time_ns": 20,
                "mpi_calls": 1,
                "request_completions": 1,
                "failed_calls": 0,
                "bytes_sent": 8,
                "bytes_received": 0,
                "operations": {
                    "MPI_Isend": {
                        "calls": 1,
                        "duration_ns": 20,
                        "payload_bytes": 8,
                        "record_kind": "api_call",
                    },
                    "MPI_Isend_complete": {
                        "calls": 1,
                        "duration_ns": 0,
                        "payload_bytes": 0,
                        "record_kind": "request_completion",
                    },
                },
            }
            (directory / "rank-00000-summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            result = analyze(directory)

            self.assertTrue(result.complete)
            self.assertEqual(result.operations["MPI_Isend"].calls, 1)
            self.assertEqual(result.operations["MPI_Isend_complete"].calls, 1)
            self.assertEqual(result.coverage["request_lifecycle"], "unknown")
            self.assertEqual(result.coverage["partitioned_requests"], "unknown")

    def test_reports_independent_partial_request_lifecycle_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            summary = {
                "schema_version": 2,
                "complete": True,
                "capture_state": "finalized",
                "finalize_return_code": 0,
                "context": {
                    "events_dropped": "0",
                    "request_tracking_overflows": "3",
                    "writer_failed": "false",
                    "partitioned_requests": "unavailable",
                },
                "rank": 0,
                "world_size": 1,
                "hostname": "node-a",
                "pid": 42,
                "runtime_ns": 100,
                "mpi_time_ns": 20,
                "mpi_calls": 1,
                "request_completions": 0,
                "failed_calls": 0,
                "bytes_sent": 0,
                "bytes_received": 0,
                "operations": {
                    "MPI_Irecv": {
                        "calls": 1,
                        "duration_ns": 20,
                        "payload_bytes": 0,
                        "record_kind": "api_call",
                    }
                },
            }
            (directory / "rank-00000-summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            result = analyze(directory)

            self.assertTrue(result.complete)
            self.assertEqual(result.coverage["summary"], "complete")
            self.assertEqual(result.coverage["event_detail"], "unavailable")
            self.assertEqual(result.coverage["request_lifecycle"], "partial")
            self.assertEqual(result.coverage["partitioned_requests"], "unavailable")
            self.assertIn("request_tracking_limit_reached", result.coverage["reasons"])

    def test_rejects_version_two_with_mixed_call_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            summary = {
                "schema_version": 2,
                "complete": True,
                "capture_state": "finalized",
                "finalize_return_code": 0,
                "context": {},
                "rank": 0,
                "world_size": 1,
                "hostname": "node-a",
                "pid": 42,
                "runtime_ns": 100,
                "mpi_time_ns": 20,
                "mpi_calls": 2,
                "request_completions": 1,
                "failed_calls": 0,
                "bytes_sent": 8,
                "bytes_received": 0,
                "operations": {
                    "MPI_Isend": {
                        "calls": 1,
                        "duration_ns": 20,
                        "payload_bytes": 8,
                        "record_kind": "api_call",
                    },
                    "MPI_Isend_complete": {
                        "calls": 1,
                        "duration_ns": 0,
                        "payload_bytes": 0,
                        "record_kind": "request_completion",
                    },
                },
            }
            (directory / "rank-00000-summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            with self.assertRaisesRegex(TelemetryError, "mpi_calls does not match"):
                analyze(directory)


if __name__ == "__main__":
    unittest.main()
