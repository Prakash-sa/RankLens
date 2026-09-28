from __future__ import annotations

import unittest

from ranklens_enterprise.analytics import RankAggregateError, aggregate_rank_summaries
from ranklens_enterprise.parquet import ParquetSegment, build_normalized_parquet


class EnterpriseAnalyticsTests(unittest.TestCase):
    @staticmethod
    def projection():
        payload = (
            b'{"schema_version":2,"rank":0,"hostname":"node-a","world_size":2,'
            b'"runtime_ns":100,"mpi_time_ns":30,"mpi_calls":4,'
            b'"request_completions":1,"failed_calls":0,"complete":true,'
            b'"capture_state":"finalized"}\n'
            b'{"schema_version":2,"rank":1,"hostname":"node-b","world_size":2,'
            b'"runtime_ns":140,"mpi_time_ns":50,"mpi_calls":6,'
            b'"request_completions":2,"failed_calls":1,"complete":true,'
            b'"capture_state":"finalized"}\n'
            b'{"schema_version":2,"sequence":0,"rank":0,"operation":"MPI_Send",'
            b'"duration_ns":10}\n'
        )
        return build_normalized_parquet(
            tenant_id="tenant-a",
            cluster_id="cluster-a",
            attempt_id="attempt-1",
            segments=[
                ParquetSegment(
                    receipt_id="receipt-1",
                    payload_sha256="a" * 64,
                    expected_records=3,
                    payload=payload,
                )
            ],
        )

    def test_aggregates_complete_finalized_rank_evidence(self) -> None:
        artifact = self.projection()
        result = aggregate_rank_summaries(artifact.payload)

        self.assertEqual(result.source_row_count, 3)
        self.assertEqual(result.projected_summary_records, 2)
        self.assertEqual(result.rank_count, 2)
        self.assertEqual(result.expected_world_size, 2)
        self.assertEqual(result.finalized_rank_count, 2)
        self.assertEqual(result.runtime_min_ns, 100)
        self.assertEqual(result.runtime_median_ns, 120)
        self.assertEqual(result.runtime_max_ns, 140)
        self.assertEqual(result.mpi_time_total_ns, 80)
        self.assertEqual(result.mpi_calls_total, 10)
        self.assertEqual(result.request_completions_total, 3)
        self.assertEqual(result.failed_calls_total, 1)
        self.assertEqual(result.coverage_status, "complete")
        self.assertEqual(result.coverage_reasons, [])

    def test_rejects_a_projection_above_the_query_scan_limit(self) -> None:
        artifact = self.projection()
        with self.assertRaisesRegex(RankAggregateError, "query limit is 2"):
            aggregate_rank_summaries(artifact.payload, max_source_rows=2)

    def test_marks_duplicate_and_nonfinal_rank_evidence_partial(self) -> None:
        payload = (
            b'{"schema_version":2,"rank":0,"hostname":"node-a","world_size":2,'
            b'"runtime_ns":100,"mpi_time_ns":20,"complete":true,'
            b'"capture_state":"finalized"}\n'
            b'{"schema_version":2,"rank":0,"hostname":"node-a","world_size":2,'
            b'"runtime_ns":110,"mpi_time_ns":25,"complete":false,'
            b'"capture_state":"collecting"}\n'
        )
        artifact = build_normalized_parquet(
            tenant_id="tenant-a",
            cluster_id="cluster-a",
            attempt_id="attempt-partial",
            segments=[
                ParquetSegment(
                    receipt_id="receipt-partial",
                    payload_sha256="b" * 64,
                    expected_records=2,
                    payload=payload,
                )
            ],
        )

        result = aggregate_rank_summaries(artifact.payload)

        self.assertEqual(result.coverage_status, "partial")
        self.assertEqual(result.finalized_rank_count, 0)
        self.assertIn("duplicate_rank_summaries", result.coverage_reasons)
        self.assertIn("no_finalized_rank_summaries", result.coverage_reasons)
        self.assertIn(
            "finalized_rank_count_world_size_mismatch", result.coverage_reasons
        )


if __name__ == "__main__":
    unittest.main()
