"""Deterministic, bounded conversion of admitted NDJSON into versioned Parquet."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_SCHEMA_VERSION = "normalized-record-v1"


@dataclass(frozen=True)
class ParquetSegment:
    receipt_id: str
    payload_sha256: str
    expected_records: int
    payload: bytes


@dataclass(frozen=True)
class ParquetArtifact:
    payload: bytes
    row_count: int
    schema_version: str = PARQUET_SCHEMA_VERSION


def build_normalized_parquet(
    *,
    tenant_id: str,
    cluster_id: str,
    attempt_id: str,
    segments: Sequence[ParquetSegment],
    max_segments: int = 10_000,
    max_records: int = 10_000_000,
    max_input_bytes: int = 64 * 1024 * 1024,
) -> ParquetArtifact:
    if not segments:
        raise ValueError("at least one segment is required")
    if len(segments) > max_segments:
        raise ValueError("Parquet segment limit exceeded")
    total_bytes = sum(len(segment.payload) for segment in segments)
    if total_bytes > max_input_bytes:
        raise ValueError("Parquet input byte limit exceeded")

    receipt_ids = []
    payload_digests = []
    record_indexes = []
    record_json = []
    for segment in segments:
        count = 0
        for line in segment.payload.splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("normalized record must be an object")
            canonical = json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            receipt_ids.append(segment.receipt_id)
            payload_digests.append(segment.payload_sha256)
            record_indexes.append(count)
            record_json.append(canonical)
            count += 1
            if len(record_json) > max_records:
                raise ValueError("Parquet record limit exceeded")
        if count != segment.expected_records:
            raise ValueError("Parquet segment record count mismatch")

    schema = pa.schema(
        [
            pa.field("tenant_id", pa.string(), nullable=False),
            pa.field("cluster_id", pa.string(), nullable=False),
            pa.field("attempt_id", pa.string(), nullable=False),
            pa.field("receipt_id", pa.string(), nullable=False),
            pa.field("payload_sha256", pa.string(), nullable=False),
            pa.field("record_index", pa.int64(), nullable=False),
            pa.field("record_json", pa.large_string(), nullable=False),
        ],
        metadata={
            b"ranklens.schema": PARQUET_SCHEMA_VERSION.encode("ascii"),
            b"ranklens.content": b"canonical-json-records",
        },
    )
    rows = len(record_json)
    table = pa.Table.from_arrays(
        [
            pa.array([tenant_id] * rows, type=pa.string()),
            pa.array([cluster_id] * rows, type=pa.string()),
            pa.array([attempt_id] * rows, type=pa.string()),
            pa.array(receipt_ids, type=pa.string()),
            pa.array(payload_digests, type=pa.string()),
            pa.array(record_indexes, type=pa.int64()),
            pa.array(record_json, type=pa.large_string()),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        use_dictionary=("tenant_id", "cluster_id", "attempt_id", "receipt_id"),
        write_statistics=True,
    )
    return ParquetArtifact(payload=sink.getvalue().to_pybytes(), row_count=rows)
