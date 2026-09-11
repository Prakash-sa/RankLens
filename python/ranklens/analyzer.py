"""Cross-rank telemetry loading and rule-based performance diagnosis."""

from __future__ import annotations

import json
import math
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
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TelemetryError(f"cannot read {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise TelemetryError(f"{path}: summary root must be an object")
    schema_version = data.get("schema_version")
    if type(schema_version) is not int or schema_version not in {1, 2}:
        raise TelemetryError(f"{path}: unsupported schema_version {data.get('schema_version')!r}")

    if schema_version == 2:
        if not isinstance(data.get("complete"), bool):
            raise TelemetryError(f"{path}: version 2 complete must be boolean")
        capture_state = data.get("capture_state")
        if capture_state not in {"collecting", "pre_finalize", "finalized", "finalize_failed"}:
            raise TelemetryError(f"{path}: invalid version 2 capture_state {capture_state!r}")
        _required_int(data, "finalize_return_code", path)
        _required_int(data, "request_completions", path)
        _required_int(data, "failed_calls", path)

    raw_operations = data.get("operations")
    if not isinstance(raw_operations, dict):
        raise TelemetryError(f"{path}: operations must be an object")

    operations: Dict[str, OperationStats] = {}
    api_operation_calls = 0
    completion_calls = 0
    for name, raw_stats in raw_operations.items():
        if not isinstance(name, str) or not isinstance(raw_stats, dict):
            raise TelemetryError(f"{path}: invalid operation entry")
        if schema_version == 2:
            record_kind = raw_stats.get("record_kind")
            if record_kind not in {"api_call", "request_completion"}:
                raise TelemetryError(f"{path}: operation {name!r} has invalid record_kind")
            if record_kind == "request_completion":
                completion_calls += _required_int(raw_stats, "calls", path)
            else:
                api_operation_calls += _required_int(raw_stats, "calls", path)
        operations[name] = OperationStats(
            calls=_required_int(raw_stats, "calls", path),
            duration_ns=_required_int(raw_stats, "duration_ns", path),
            payload_bytes=_required_int(raw_stats, "payload_bytes", path),
        )

    if schema_version == 2:
        if _required_int(data, "mpi_calls", path) != api_operation_calls:
            raise TelemetryError(f"{path}: mpi_calls does not match api_call operation records")
        if _required_int(data, "request_completions", path) != completion_calls:
            raise TelemetryError(
                f"{path}: request_completions does not match request_completion records"
            )

    hostname = data.get("hostname")
    if not isinstance(hostname, str):
        raise TelemetryError(f"{path}: hostname must be a string")

    rank = _required_int(data, "rank", path)
    world_size = _required_int(data, "world_size", path)
    if world_size == 0 or rank >= world_size:
        raise TelemetryError(f"{path}: rank must be within a positive world_size")
    for key in ("synthetic", "complete"):
        if key in data and not isinstance(data[key], bool):
            raise TelemetryError(f"{path}: {key} must be boolean")
    context = data.get("context", {})
    if not isinstance(context, dict) or not all(isinstance(v, str) for v in context.values()):
        raise TelemetryError(f"{path}: context must contain string values")

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
        synthetic=data.get("synthetic", False),
        complete=data.get("complete", True),
        context=context,
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
    directory: Path, total_bytes_sent: int, warnings: List[str], world_size: int,
    timeline: List[dict], runtime_max_ns: int,
) -> List[CommunicationEdge]:
    edges: Dict[Tuple[int, int], List[int]] = defaultdict(lambda: [0, 0])
    for path in sorted(directory.glob("rank-*-events.jsonl")):
        try:
            expected_rank = int(path.name.split("-", 2)[1])
        except (IndexError, ValueError):
            warnings.append(f"ignored event file with invalid rank name: {path.name}")
            continue
        last_sequence = -1
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
                    if len(warnings) < 100:
                        warnings.append(f"ignored malformed {path.name}:{line_number}: {exc.msg}")
                    continue
                if not isinstance(event, dict):
                    if len(warnings) < 100:
                        warnings.append(f"ignored non-object event in {path.name}:{line_number}")
                    continue
                schema_version = event.get("schema_version")
                if type(schema_version) is not int or schema_version not in {1, 2}:
                    if len(warnings) < 100:
                        warnings.append(
                            f"ignored unsupported event schema in {path.name}:{line_number}"
                        )
                    continue
                if event.get("rank") != expected_rank:
                    if len(warnings) < 100:
                        warnings.append(f"ignored rank-mismatched event in {path.name}:{line_number}")
                    continue
                if schema_version == 2:
                    sequence = event.get("sequence")
                    if type(sequence) is not int or sequence < 0 or sequence <= last_sequence:
                        if len(warnings) < 100:
                            warnings.append(
                                f"ignored invalid event sequence in {path.name}:{line_number}"
                            )
                        continue
                    last_sequence = sequence
                    record_kind = event.get("record_kind")
                    completion = str(event.get("operation", "")).endswith("_complete")
                    expected_kind = "request_completion" if completion else "api_call"
                    if record_kind != expected_kind:
                        if len(warnings) < 100:
                            warnings.append(
                                f"ignored event with inconsistent record_kind in "
                                f"{path.name}:{line_number}"
                            )
                        continue
                if event.get("error_code", 0) != 0:
                    continue
                timestamp, duration = event.get("timestamp_ns"), event.get("duration_ns")
                if all(type(v) is int and v >= 0 for v in (timestamp, duration)):
                    bucket = min(49, timestamp * 50 // max(runtime_max_ns, 1))
                    timeline[bucket]["calls"] += 1
                    timeline[bucket]["duration_ns"] += duration
                if event.get("operation") not in {"MPI_Send", "MPI_Isend"}:
                    continue
                source = event.get("rank")
                destination = event.get("peer")
                payload_bytes = event.get("payload_bytes")
                if not all(
                    isinstance(value, int) and not isinstance(value, bool)
                    for value in (source, destination, payload_bytes)
                ) or not (0 <= source < world_size and 0 <= destination < world_size and payload_bytes >= 0):
                    if len(warnings) < 100:
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
                evidence="The captured operations do not show a dominant bottleneck under the current rules.",
                recommendation=(
                    "Treat this as an absence of detected evidence, not proof of optimal performance; "
                    "compare against a controlled scaling baseline."
                ),
            )
        )
    return findings


def analyze(directory: Path, straggler_threshold: float = 1.20) -> AnalysisResult:
    if not math.isfinite(straggler_threshold) or straggler_threshold <= 1.0:
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
    timeline = [{"timestamp_ns": maximum_runtime * i // 50, "calls": 0, "duration_ns": 0} for i in range(50)]
    metadata = {}
    manifest = directory / "run.json"
    if manifest.exists():
        try:
            metadata = json.loads(manifest.read_text(encoding="utf-8"))
            if not isinstance(metadata, dict):
                raise ValueError("root must be an object")
        except (OSError, UnicodeError, ValueError) as exc:
            raise TelemetryError(f"cannot read run metadata: {exc}") from exc
    complete = len(summaries) == world_size and all(s.complete for s in summaries)
    if not complete:
        warnings.append("capture is incomplete; runtimes and findings describe partial evidence")
    if any(s.context.get("events_dropped", "0") != "0" for s in summaries):
        warnings.append("event limit reached; communication edges and timeline are partial, operation totals remain complete")
    if not list(directory.glob("rank-*-events.jsonl")):
        warnings.append("no event streams; communication and timeline coverage unavailable")
    if metadata.get("return_code", 0) != 0:
        warnings.append("launcher did not complete successfully; review run metadata")
        complete = False
    run_ids = {s.context.get("run_id") for s in summaries if s.context.get("run_id")}
    if len(run_ids) > 1 or (run_ids and metadata.get("run_id") and metadata["run_id"] not in run_ids):
        raise TelemetryError("mixed run identities in capture")
    if mpi_time > aggregate_runtime:
        warnings.append("summed MPI call time exceeds wall time; concurrent threads may overlap")

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
        communication_edges=_load_communication_edges(directory, total_bytes_sent, warnings, world_size, timeline, maximum_runtime),
        rank_runtimes=sorted((summary.rank, summary.runtime_ns) for summary in summaries),
        warnings=warnings,
        synthetic=any(s.synthetic for s in summaries),
        complete=complete,
        metadata=metadata,
        ranks=[{"rank": s.rank, "hostname": s.hostname, "runtime_ns": s.runtime_ns,
                "mpi_time_ns": s.mpi_time_ns, "context": s.context, "complete": s.complete}
               for s in sorted(summaries, key=lambda s: s.rank)],
        timeline=timeline,
    )
    result.findings = _make_findings(result)
    return result
