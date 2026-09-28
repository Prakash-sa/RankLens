from __future__ import annotations

import io
import unittest

import pyarrow.parquet as pq

from ranklens_enterprise.parquet import ParquetSegment, build_normalized_parquet


class EnterpriseParquetTests(unittest.TestCase):
    def test_builds_deterministic_versioned_parquet_with_typed_records(self) -> None:
        segments = [
            ParquetSegment(
                receipt_id="receipt-1",
                payload_sha256="a" * 64,
                expected_records=4,
                payload=(
                    b'{"schema_version":2,"rank":0,"hostname":"node-a","world_size":2,'
                    b'"runtime_ns":1000,"mpi_time_ns":300,"mpi_calls":4,'
                    b'"request_completions":1,"failed_calls":0,"complete":true,'
                    b'"capture_state":"finalized"}\n'
                    b'{"schema_version":2,"sequence":7,"rank":0,"operation":"MPI_Send",'
                    b'"record_kind":"api_call","timestamp_ns":100,"duration_ns":20,'
                    b'"payload_bytes":64,"error_code":0}\n'
                    b'{"z":1,"a":"unknown"}\n'
                    b'{"schema_version":999999999999999999999,"rank":true,'
                    b'"duration_ns":999999999999999999999}\n'
                ),
            )
        ]

        first = build_normalized_parquet(
            tenant_id="tenant-a",
            cluster_id="cluster-a",
            attempt_id="attempt-1",
            segments=segments,
        )
        second = build_normalized_parquet(
            tenant_id="tenant-a",
            cluster_id="cluster-a",
            attempt_id="attempt-1",
            segments=segments,
        )

        self.assertEqual(first.payload, second.payload)
        self.assertEqual(first.row_count, 4)
        table = pq.read_table(io.BytesIO(first.payload))
        self.assertEqual(table.schema.metadata[b"ranklens.schema"], b"normalized-record-v2")
        self.assertEqual(table.column("record_index").to_pylist(), [0, 1, 2, 3])
        self.assertEqual(
            table.column("record_type").to_pylist(),
            ["rank_summary", "mpi_event", "unrecognized", "unrecognized"],
        )
        self.assertEqual(table.column("rank").to_pylist(), [0, 0, None, None])
        self.assertEqual(table.column("operation").to_pylist(), [None, "MPI_Send", None, None])
        self.assertEqual(table.column("runtime_ns").to_pylist(), [1000, None, None, None])
        self.assertEqual(table.column("duration_ns").to_pylist(), [None, 20, None, None])
        self.assertEqual(
            table.column("record_json").to_pylist(),
            [
                '{"capture_state":"finalized","complete":true,"failed_calls":0,'
                '"hostname":"node-a","mpi_calls":4,"mpi_time_ns":300,"rank":0,'
                '"request_completions":1,"runtime_ns":1000,"schema_version":2,'
                '"world_size":2}',
                '{"duration_ns":20,"error_code":0,"operation":"MPI_Send",'
                '"payload_bytes":64,"rank":0,"record_kind":"api_call",'
                '"schema_version":2,"sequence":7,"timestamp_ns":100}',
                '{"a":"unknown","z":1}',
                '{"duration_ns":999999999999999999999,"rank":true,'
                '"schema_version":999999999999999999999}',
            ],
        )

    def test_enforces_input_and_record_count_bounds(self) -> None:
        segment = ParquetSegment(
            receipt_id="receipt-1",
            payload_sha256="a" * 64,
            expected_records=2,
            payload=b'{"record":1}\n',
        )

        with self.assertRaisesRegex(ValueError, "record count mismatch"):
            build_normalized_parquet(
                tenant_id="tenant-a",
                cluster_id="cluster-a",
                attempt_id="attempt-1",
                segments=[segment],
            )
        with self.assertRaisesRegex(ValueError, "input byte limit"):
            build_normalized_parquet(
                tenant_id="tenant-a",
                cluster_id="cluster-a",
                attempt_id="attempt-1",
                segments=[segment],
                max_input_bytes=1,
            )


if __name__ == "__main__":
    unittest.main()
