"""Cross-rank telemetry loading and rule-based performance diagnosis."""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple

from .models import (
    AnalysisResult,
    CommunicationEdge,
    Finding,
    OperationStats,
    RankSummary,
)

COLLECTIVES = {"MPI_Allreduce", "MPI_Bcast", "MPI_Barrier"}


class TelemetryError(ValueError):
    """Raised when telemetry is absent or violates the RankLens contract."""


def _required_int(data: Mapping[str, object], key: str, path: Path) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TelemetryError(f"{path}: {key!r} must be a non-negative integer")
    return value


def load_summary(path: Path) -> RankSummary:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TelemetryError(f"cannot read {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise TelemetryError(f"{path}: summary root must be an object")
    if data.get("schema_version") != 1:
        raise TelemetryError(f"{path}: unsupported schema_version {data.get('schema_version')!r}")

    raw_operations = data.get("operations")
    if not isinstance(raw_operations, dict):
        raise TelemetryError(f"{path}: operations must be an object")

    operations: Dict[str, OperationStats] = {}
    for name, raw_stats in raw_operations.items():
        if not isinstance(name, str) or not isinstance(raw_stats, dict):
            raise TelemetryError(f"{path}: invalid operation entry")
        operations[name] = OperationStats(
            calls=_required_int(raw_stats, "calls", path),
            duration_ns=_required_int(raw_stats, "duration_ns", path),
            payload_bytes=_required_int(raw_stats, "payload_bytes", path),
        )

    hostname = data.get("hostname")
    if not isinstance(hostname, str):
        raise TelemetryError(f"{path}: hostname must be a string")

    return RankSummary(
        rank=_required_int(data, "rank", path),
        world_size=_required_int(data, "world_size", path),
        hostname=hostname,
        pid=_required_int(data, "pid", path),
        runtime_ns=_required_int(data, "runtime_ns", path),
        mpi_time_ns=_required_int(data, "mpi_time_ns", path),
        mpi_calls=_required_int(data, "mpi_calls", path),
        bytes_sent=_required_int(data, "bytes_sent", path),
        bytes_received=_required_int(data, "bytes_received", path),
        operations=operations,
    )


def load_summaries(directory: Path) -> List[RankSummary]:
    paths = sorted(directory.glob("rank-*-summary.json"))
    if not paths:
        raise TelemetryError(f"no rank-*-summary.json files found in {directory}")

    summaries = [load_summary(path) for path in paths]
    ranks = [summary.rank for summary in summaries]
    if len(ranks) != len(set(ranks)):
        raise TelemetryError(f"duplicate rank summaries found in {directory}")
    world_sizes = {summary.world_size for summary in summaries}
    if len(world_sizes) != 1:
        raise TelemetryError(f"inconsistent world_size values in {directory}")
    return summaries


def _load_communication_edges(
    directory: Path, total_bytes_sent: int, warnings: List[str]
) -> List[CommunicationEdge]:
    edges: Dict[Tuple[int, int], List[int]] = defaultdict(lambda: [0, 0])
    for path in sorted(directory.glob("rank-*-events.jsonl")):
        try:
            stream = path.open("r", encoding="utf-8")
        except OSError as exc:
            warnings.append(f"could not read {path.name}: {exc}")
            continue
        with stream:
            for line_number, line in enumerate(stream, start=1):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    warnings.append(f"ignored malformed {path.name}:{line_number}: {exc.msg}")
                    continue
                if event.get("operation") != "MPI_Send":
                    continue
                source = event.get("rank")
                destination = event.get("peer")
                payload_bytes = event.get("payload_bytes")
                if not all(
                    isinstance(value, int) and not isinstance(value, bool)
                    for value in (source, destination, payload_bytes)
                ):
                    warnings.append(f"ignored invalid send event in {path.name}:{line_number}")
                    continue
                edge = edges[(source, destination)]
                edge[0] += payload_bytes
                edge[1] += 1

    denominator = total_bytes_sent or 1
    return sorted(
        (
            CommunicationEdge(
                source=source,
                destination=destination,
                bytes=values[0],
                messages=values[1],
                traffic_share=values[0] / denominator,
            )
            for (source, destination), values in edges.items()
        ),
        key=lambda edge: edge.bytes,
        reverse=True,
    )


def _aggregate_operations(summaries: Iterable[RankSummary]) -> Dict[str, OperationStats]:
    totals: Dict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
    for summary in summaries:
        for name, stats in summary.operations.items():
            totals[name][0] += stats.calls
            totals[name][1] += stats.duration_ns
            totals[name][2] += stats.payload_bytes
    return {
        name: OperationStats(calls=values[0], duration_ns=values[1], payload_bytes=values[2])
        for name, values in sorted(totals.items())
    }


def _make_findings(result: AnalysisResult) -> List[Finding]:
    findings: List[Finding] = []

    if result.straggler_ranks:
        ranks = ", ".join(str(rank) for rank in result.straggler_ranks)
        findings.append(
            Finding(
                severity="warning",
                category="imbalance",
                title="Runtime stragglers detected",
                evidence=(
                    f"Ranks {ranks} ran at least {result.straggler_threshold:.2f}x the median; "
                    f"maximum/median is {result.imbalance_ratio:.2f}x."
                ),
                recommendation=(
                    "Correlate these ranks with CPU affinity, NUMA locality, per-rank work, and "
                    "node placement before changing resources."
                ),
            )
        )

    aggregate_runtime = result.aggregate_runtime_ns or 1
    allreduce = result.operations.get("MPI_Allreduce")
    if allreduce and allreduce.duration_ns / aggregate_runtime >= 0.15:
        findings.append(
            Finding(
                severity="warning",
                category="collectives",
                title="Allreduce is a major runtime component",
                evidence=(
                    f"MPI_Allreduce accounts for {allreduce.duration_ns / aggregate_runtime:.1%} "
                    "of aggregate rank runtime."
                ),
                recommendation=(
                    "Measure whether reductions can be fused or called less often, then benchmark "
                    "topology-aware collective tuning with scientific output held constant."
                ),
            )
        )

    barrier = result.operations.get("MPI_Barrier")
    if barrier and barrier.duration_ns / aggregate_runtime >= 0.10:
        findings.append(
            Finding(
                severity="warning",
                category="synchronization",
                title="Barrier wait is substantial",
                evidence=(
                    f"MPI_Barrier accounts for {barrier.duration_ns / aggregate_runtime:.1%} "
                    "of aggregate rank runtime."
                ),
                recommendation=(
                    "Inspect work completed immediately before each barrier; optimize the slowest "
                    "path or remove redundant synchronization only after validating correctness."
                ),
            )
        )

    if result.mpi_fraction >= 0.50:
        findings.append(
            Finding(
                severity="info",
                category="scaling",
                title="Communication dominates aggregate runtime",
                evidence=f"Instrumented MPI calls account for {result.mpi_fraction:.1%} of rank time.",
                recommendation=(
                    "Run a controlled rank-count sweep. Fewer ranks or larger work units may improve "
                    "efficiency if this workload has crossed its strong-scaling limit."
                ),
            )
        )

    if result.communication_edges and result.communication_edges[0].traffic_share >= 0.35:
        edge = result.communication_edges[0]
        findings.append(
            Finding(
                severity="warning",
                category="communication",
                title="Point-to-point traffic hotspot detected",
                evidence=(
                    f"Rank {edge.source} to rank {edge.destination} carries "
                    f"{edge.traffic_share:.1%} of observed MPI_Send payload bytes."
                ),
                recommendation=(
                    "Review decomposition and neighbor mapping, then compare placement-aware and "
                    "baseline runs using identical workload metadata."
                ),
            )
        )

    if not findings:
        findings.append(
            Finding(
                severity="ok",
                category="summary",
                title="No configured rule crossed its threshold",
                evidence="The captured operations do not show a dominant bottleneck under v0.1 rules.",
                recommendation=(
                    "Treat this as an absence of detected evidence, not proof of optimal performance; "
                    "compare against a controlled scaling baseline."
                ),
            )
        )
    return findings


def analyze(directory: Path, straggler_threshold: float = 1.20) -> AnalysisResult:
    if straggler_threshold <= 1.0:
        raise ValueError("straggler_threshold must be greater than 1.0")
    directory = directory.expanduser().resolve()
    summaries = load_summaries(directory)
    world_size = summaries[0].world_size
    warnings: List[str] = []
    if len(summaries) != world_size:
        warnings.append(f"expected {world_size} rank summaries but found {len(summaries)}")

    runtimes = [summary.runtime_ns for summary in summaries]
    median_runtime = int(statistics.median(runtimes))
    maximum_runtime = max(runtimes)
    stragglers = sorted(
        summary.rank
        for summary in summaries
        if median_runtime > 0 and summary.runtime_ns / median_runtime >= straggler_threshold
    )
    aggregate_runtime = sum(runtimes)
    mpi_time = sum(summary.mpi_time_ns for summary in summaries)
    total_bytes_sent = sum(summary.bytes_sent for summary in summaries)

    result = AnalysisResult(
        source=str(directory),
        world_size=world_size,
        ranks_observed=len(summaries),
        runtime_median_ns=median_runtime,
        runtime_max_ns=maximum_runtime,
        mpi_time_ns=mpi_time,
        aggregate_runtime_ns=aggregate_runtime,
        mpi_fraction=mpi_time / aggregate_runtime if aggregate_runtime else 0.0,
        imbalance_ratio=maximum_runtime / median_runtime if median_runtime else 0.0,
        straggler_threshold=straggler_threshold,
        straggler_ranks=stragglers,
        operations=_aggregate_operations(summaries),
        communication_edges=_load_communication_edges(directory, total_bytes_sent, warnings),
        rank_runtimes=sorted((summary.rank, summary.runtime_ns) for summary in summaries),
        warnings=warnings,
    )
    result.findings = _make_findings(result)
    return result
