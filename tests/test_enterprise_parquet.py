from __future__ import annotations

import io
import unittest

import pyarrow.parquet as pq

from ranklens_enterprise.parquet import ParquetSegment, build_normalized_parquet


class EnterpriseParquetTests(unittest.TestCase):
    def test_builds_deterministic_versioned_parquet_with_canonical_records(self) -> None:
        segments = [
            ParquetSegment(
                receipt_id="receipt-1",
                payload_sha256="a" * 64,
                expected_records=2,
                payload=b'{"z":1,"a":"first"}\n{"a":"second","z":2}\n',
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
        self.assertEqual(first.row_count, 2)
        table = pq.read_table(io.BytesIO(first.payload))
        self.assertEqual(table.schema.metadata[b"ranklens.schema"], b"normalized-record-v1")
        self.assertEqual(table.column("record_index").to_pylist(), [0, 1])
        self.assertEqual(
            table.column("record_json").to_pylist(),
            ['{"a":"first","z":1}', '{"a":"second","z":2}'],
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
