"""Typed telemetry and analysis models used by RankLens."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class OperationStats:
    calls: int
    duration_ns: int
    payload_bytes: int


@dataclass(frozen=True)
class RankSummary:
    rank: int
    world_size: int
    hostname: str
    pid: int
    runtime_ns: int
    mpi_time_ns: int
    mpi_calls: int
    bytes_sent: int
    bytes_received: int
    operations: Dict[str, OperationStats]
    synthetic: bool = False
    complete: bool = True
    context: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CommunicationEdge:
    source: int
    destination: int
    bytes: int
    messages: int
    traffic_share: float


@dataclass(frozen=True)
class Finding:
    severity: str
    category: str
    title: str
    evidence: str
    recommendation: str


@dataclass
class AnalysisResult:
    source: str
    world_size: int
    ranks_observed: int
    runtime_median_ns: int
    runtime_max_ns: int
    mpi_time_ns: int
    aggregate_runtime_ns: int
    mpi_fraction: float
    imbalance_ratio: float
    straggler_threshold: float
    straggler_ranks: List[int] = field(default_factory=list)
    operations: Dict[str, OperationStats] = field(default_factory=dict)
    communication_edges: List[CommunicationEdge] = field(default_factory=list)
    rank_runtimes: List[Tuple[int, int]] = field(default_factory=list)
    findings: List[Finding] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    coverage: dict = field(default_factory=dict)
    schema_version: int = 1
    synthetic: bool = False
    complete: bool = True
    metadata: dict = field(default_factory=dict)
    ranks: List[dict] = field(default_factory=list)
    timeline: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
