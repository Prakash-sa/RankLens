"""Deterministic, bounded conversion of admitted NDJSON into versioned Parquet."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_SCHEMA_VERSION = "normalized-record-v2"


def _integer(
    value: object, *, minimum: int = 0, maximum: int = 2**63 - 1
) -> int | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        return None
    return value


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _record_type(value: dict) -> str:
    if (
        _integer(value.get("schema_version"), minimum=1) is not None
        and _integer(value.get("rank")) is not None
        and _integer(value.get("runtime_ns")) is not None
        and _integer(value.get("mpi_time_ns")) is not None
        and _text(value.get("hostname")) is not None
    ):
        return "rank_summary"
    if (
        _integer(value.get("schema_version"), minimum=1) is not None
        and _integer(value.get("rank")) is not None
        and _integer(value.get("sequence")) is not None
        and _text(value.get("operation")) is not None
        and _integer(value.get("duration_ns")) is not None
    ):
        return "mpi_event"
    return "unrecognized"


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
    record_types = []
    schema_versions = []
    ranks = []
    sequences = []
    operations = []
    record_kinds = []
    timestamps_ns = []
    durations_ns = []
    payload_bytes = []
    error_codes = []
    hostnames = []
    world_sizes = []
    runtimes_ns = []
    mpi_times_ns = []
    mpi_calls = []
    request_completions = []
    failed_calls = []
    complete_values = []
    capture_states = []
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
            record_types.append(_record_type(value))
            schema_versions.append(
                _integer(value.get("schema_version"), minimum=1, maximum=2**31 - 1)
            )
            ranks.append(_integer(value.get("rank")))
            sequences.append(_integer(value.get("sequence")))
            operations.append(_text(value.get("operation")))
            record_kinds.append(_text(value.get("record_kind")))
            timestamps_ns.append(_integer(value.get("timestamp_ns")))
            durations_ns.append(_integer(value.get("duration_ns")))
            payload_bytes.append(_integer(value.get("payload_bytes")))
            error_codes.append(
                _integer(
                    value.get("error_code"), minimum=-(2**31), maximum=2**31 - 1
                )
            )
            hostnames.append(_text(value.get("hostname")))
            world_sizes.append(_integer(value.get("world_size"), minimum=1))
            runtimes_ns.append(_integer(value.get("runtime_ns")))
            mpi_times_ns.append(_integer(value.get("mpi_time_ns")))
            mpi_calls.append(_integer(value.get("mpi_calls")))
            request_completions.append(_integer(value.get("request_completions")))
            failed_calls.append(_integer(value.get("failed_calls")))
            complete_values.append(
                value.get("complete") if isinstance(value.get("complete"), bool) else None
            )
            capture_states.append(_text(value.get("capture_state")))
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
            pa.field("record_type", pa.string(), nullable=False),
            pa.field("schema_version", pa.int32()),
            pa.field("rank", pa.int64()),
            pa.field("sequence", pa.int64()),
            pa.field("operation", pa.string()),
            pa.field("record_kind", pa.string()),
            pa.field("timestamp_ns", pa.int64()),
            pa.field("duration_ns", pa.int64()),
            pa.field("payload_bytes", pa.int64()),
            pa.field("error_code", pa.int32()),
            pa.field("hostname", pa.string()),
            pa.field("world_size", pa.int64()),
            pa.field("runtime_ns", pa.int64()),
            pa.field("mpi_time_ns", pa.int64()),
            pa.field("mpi_calls", pa.int64()),
            pa.field("request_completions", pa.int64()),
            pa.field("failed_calls", pa.int64()),
            pa.field("complete", pa.bool_()),
            pa.field("capture_state", pa.string()),
            pa.field("record_json", pa.large_string(), nullable=False),
        ],
        metadata={
            b"ranklens.schema": PARQUET_SCHEMA_VERSION.encode("ascii"),
            b"ranklens.content": b"typed-telemetry-projection-with-canonical-json",
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
            pa.array(record_types, type=pa.string()),
            pa.array(schema_versions, type=pa.int32()),
            pa.array(ranks, type=pa.int64()),
            pa.array(sequences, type=pa.int64()),
            pa.array(operations, type=pa.string()),
            pa.array(record_kinds, type=pa.string()),
            pa.array(timestamps_ns, type=pa.int64()),
            pa.array(durations_ns, type=pa.int64()),
            pa.array(payload_bytes, type=pa.int64()),
            pa.array(error_codes, type=pa.int32()),
            pa.array(hostnames, type=pa.string()),
            pa.array(world_sizes, type=pa.int64()),
            pa.array(runtimes_ns, type=pa.int64()),
            pa.array(mpi_times_ns, type=pa.int64()),
            pa.array(mpi_calls, type=pa.int64()),
            pa.array(request_completions, type=pa.int64()),
            pa.array(failed_calls, type=pa.int64()),
            pa.array(complete_values, type=pa.bool_()),
            pa.array(capture_states, type=pa.string()),
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
        use_dictionary=(
            "tenant_id", "cluster_id", "attempt_id", "receipt_id", "record_type",
            "operation", "record_kind", "hostname", "capture_state",
        ),
        write_statistics=True,
    )
    return ParquetArtifact(payload=sink.getvalue().to_pybytes(), row_count=rows)
