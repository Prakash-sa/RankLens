"""Bounded analytical views over immutable report projections."""

from __future__ import annotations

import io
import statistics
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq


class RankAggregateError(ValueError):
    pass


@dataclass(frozen=True)
class RankAggregate:
    source_row_count: int
    projected_summary_records: int
    rank_count: int
    expected_world_size: Optional[int]
    finalized_rank_count: int
    runtime_min_ns: Optional[int]
    runtime_median_ns: Optional[float]
    runtime_max_ns: Optional[int]
    mpi_time_total_ns: int
    mpi_calls_total: int
    request_completions_total: int
    failed_calls_total: int
    coverage_status: str
    coverage_reasons: list[str]


def aggregate_rank_summaries(
    payload: bytes, *, max_source_rows: int = 100_000
) -> RankAggregate:
    """Build a bounded latest-row-per-rank view from a v2 report projection."""

    if max_source_rows < 1:
        raise ValueError("max_source_rows must be positive")
    try:
        parquet = pq.ParquetFile(io.BytesIO(payload))
    except (pa.ArrowException, OSError) as exc:
        raise RankAggregateError("report projection is not readable Parquet") from exc
    source_rows = parquet.metadata.num_rows
    if source_rows > max_source_rows:
        raise RankAggregateError(
            f"report projection has {source_rows} rows; query limit is {max_source_rows}"
        )
    metadata = parquet.schema_arrow.metadata or {}
    if metadata.get(b"ranklens.schema") != b"normalized-record-v2":
        raise RankAggregateError("report projection schema is not supported")
    columns = (
        "record_type", "rank", "hostname", "world_size", "runtime_ns",
        "mpi_time_ns", "mpi_calls", "request_completions", "failed_calls",
        "complete", "capture_state",
    )
    missing = set(columns).difference(parquet.schema_arrow.names)
    if missing:
        raise RankAggregateError("report projection is missing required typed columns")
    try:
        values = parquet.read(columns=columns).to_pydict()
    except (pa.ArrowException, OSError) as exc:
        raise RankAggregateError("report projection cannot be decoded") from exc
    latest_by_rank = {}
    summary_records = 0
    for index, record_type in enumerate(values["record_type"]):
        if record_type != "rank_summary":
            continue
        rank = values["rank"][index]
        if rank is None:
            continue
        summary_records += 1
        latest_by_rank[rank] = {name: values[name][index] for name in columns}

    reasons = []
    if not latest_by_rank:
        reasons.append("no_rank_summaries")
    if summary_records > len(latest_by_rank):
        reasons.append("duplicate_rank_summaries")
    world_sizes = {
        item["world_size"] for item in latest_by_rank.values()
        if item["world_size"] is not None
    }
    expected_world_size = next(iter(world_sizes)) if len(world_sizes) == 1 else None
    if len(world_sizes) > 1:
        reasons.append("inconsistent_world_size")

    finalized = [
        item for item in latest_by_rank.values()
        if item["complete"] is True and item["capture_state"] == "finalized"
    ]
    if latest_by_rank and len(finalized) != len(latest_by_rank):
        reasons.append("incomplete_or_nonfinal_rank_summaries")
    if latest_by_rank and not finalized:
        reasons.append("no_finalized_rank_summaries")
    if expected_world_size is not None and len(finalized) != expected_world_size:
        reasons.append("finalized_rank_count_world_size_mismatch")

    runtimes = [item["runtime_ns"] for item in finalized if item["runtime_ns"] is not None]
    return RankAggregate(
        source_row_count=source_rows,
        projected_summary_records=summary_records,
        rank_count=len(latest_by_rank),
        expected_world_size=expected_world_size,
        finalized_rank_count=len(finalized),
        runtime_min_ns=min(runtimes) if runtimes else None,
        runtime_median_ns=statistics.median(runtimes) if runtimes else None,
        runtime_max_ns=max(runtimes) if runtimes else None,
        mpi_time_total_ns=sum(item["mpi_time_ns"] or 0 for item in finalized),
        mpi_calls_total=sum(item["mpi_calls"] or 0 for item in finalized),
        request_completions_total=sum(
            item["request_completions"] or 0 for item in finalized
        ),
        failed_calls_total=sum(item["failed_calls"] or 0 for item in finalized),
        coverage_status="complete" if not reasons else "partial",
        coverage_reasons=sorted(set(reasons)),
    )
