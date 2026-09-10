type OperationStats = { calls: number; duration_ns: number; payload_bytes: number };
type Finding = { severity: 'warning' | 'info' | 'ok'; category: string; title: string; evidence: string; recommendation: string };
type CommunicationEdge = { source: number; destination: number; bytes: number; messages: number; traffic_share: number };
export type AnalysisResult = {
  source: string; world_size: number; ranks_observed: number; runtime_median_ns: number;
  runtime_max_ns: number; mpi_time_ns: number; aggregate_runtime_ns: number;
  mpi_fraction: number; imbalance_ratio: number; straggler_threshold: number;
  straggler_ranks: number[]; operations: Record<string, OperationStats>;
  communication_edges: CommunicationEdge[]; rank_runtimes: [number, number][];
  findings: Finding[]; warnings: string[];
  synthetic?: boolean; complete?: boolean;
  metadata?: { workload?: string; run_id?: string; scheduler?: Record<string, string>; tags?: Record<string, string> };
  ranks?: { rank: number; hostname: string; runtime_ns: number; mpi_time_ns: number; context: Record<string, string>; complete: boolean }[];
  timeline?: { timestamp_ns: number; calls: number; duration_ns: number }[];
};

export const demoAnalysis: AnalysisResult = {
  synthetic: true, complete: true, source: 'Synthetic four-rank capture', world_size: 4, ranks_observed: 4,
  runtime_median_ns: 2_025_000_000, runtime_max_ns: 2_900_000_000,
  mpi_time_ns: 3_395_000_000, aggregate_runtime_ns: 8_930_000_000,
  mpi_fraction: 0.3801791713, imbalance_ratio: 1.4320987654,
  straggler_threshold: 1.2, straggler_ranks: [3],
  operations: {
    MPI_Allreduce: { calls: 200, duration_ns: 1_650_000_000, payload_bytes: 1_638_400 },
    MPI_Barrier: { calls: 200, duration_ns: 1_585_000_000, payload_bytes: 0 },
    MPI_Recv: { calls: 32, duration_ns: 88_000_000, payload_bytes: 90_112 },
    MPI_Send: { calls: 32, duration_ns: 72_000_000, payload_bytes: 90_112 },
  },
  communication_edges: [
    { source: 0, destination: 1, bytes: 65_536, messages: 8, traffic_share: 0.7272727 },
    { source: 1, destination: 2, bytes: 8_192, messages: 8, traffic_share: 0.0909091 },
    { source: 2, destination: 3, bytes: 8_192, messages: 8, traffic_share: 0.0909091 },
    { source: 3, destination: 0, bytes: 8_192, messages: 8, traffic_share: 0.0909091 },
  ],
  rank_runtimes: [[0, 2_000_000_000], [1, 2_050_000_000], [2, 1_980_000_000], [3, 2_900_000_000]],
  findings: [
    { severity: 'warning', category: 'imbalance', title: 'Runtime straggler detected', evidence: 'Rank 3 ran at least 1.20× the median; maximum/median is 1.43×.', recommendation: 'Correlate rank 3 with CPU affinity, NUMA locality, per-rank work, and node placement before changing resources.' },
    { severity: 'warning', category: 'collectives', title: 'Allreduce is a major runtime component', evidence: 'MPI_Allreduce accounts for 18.5% of aggregate rank runtime.', recommendation: 'Measure whether reductions can be fused or called less often, then benchmark topology-aware collective tuning.' },
    { severity: 'warning', category: 'synchronization', title: 'Barrier wait is substantial', evidence: 'MPI_Barrier accounts for 17.7% of aggregate rank runtime.', recommendation: 'Inspect work immediately before each barrier; optimize the slowest path or remove redundant synchronization only after validating correctness.' },
    { severity: 'warning', category: 'communication', title: 'Point-to-point traffic hotspot detected', evidence: 'Rank 0 → rank 1 carries 72.7% of observed MPI_Send payload bytes.', recommendation: 'Review decomposition and neighbor mapping, then compare placement-aware and baseline runs using identical workload metadata.' },
  ],
  warnings: [],
};

const isCount = (value: unknown) => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const isMetric = (value: unknown) => typeof value === 'number' && Number.isFinite(value) && value >= 0;

function validShape(value: unknown): value is AnalysisResult {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const result = value as Record<string, unknown>;
  const operations = result.operations;
  const ranks = result.rank_runtimes;
  const findings = result.findings;
  const edges = result.communication_edges;

  return typeof result.source === 'string' && isCount(result.world_size) && isCount(result.ranks_observed) &&
    isCount(result.runtime_median_ns) && isCount(result.runtime_max_ns) && isCount(result.mpi_time_ns) &&
    isCount(result.aggregate_runtime_ns) && isMetric(result.mpi_fraction) && isMetric(result.imbalance_ratio) &&
    isMetric(result.straggler_threshold) && Array.isArray(result.straggler_ranks) && result.straggler_ranks.every(isCount) &&
    !!operations && typeof operations === 'object' && !Array.isArray(operations) &&
    Object.entries(operations).every(([name, raw]) => {
      if (!name || !raw || typeof raw !== 'object' || Array.isArray(raw)) return false;
      const stats = raw as Record<string, unknown>;
      return isCount(stats.calls) && isCount(stats.duration_ns) && isCount(stats.payload_bytes);
    }) &&
    Array.isArray(ranks) && ranks.every((row) => Array.isArray(row) && row.length === 2 && isCount(row[0]) && isCount(row[1])) &&
    Array.isArray(findings) && findings.every((raw) => {
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return false;
      const finding = raw as Record<string, unknown>;
      return ['warning', 'info', 'ok'].includes(String(finding.severity)) &&
        ['category', 'title', 'evidence', 'recommendation'].every((key) => typeof finding[key] === 'string');
    }) &&
    Array.isArray(edges) && edges.every((raw) => {
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return false;
      const edge = raw as Record<string, unknown>;
      return isCount(edge.source) && isCount(edge.destination) && isCount(edge.bytes) &&
        isCount(edge.messages) && isMetric(edge.traffic_share);
    }) &&
    Array.isArray(result.warnings) && result.warnings.every((warning) => typeof warning === 'string');
}


export function isAnalysisResult(value: unknown): value is AnalysisResult {
  if (!validShape(value)) return false;
  const r = value;
  if (r.world_size < 1 || r.ranks_observed < 1 || r.ranks_observed > r.world_size ||
      r.straggler_threshold <= 1 || r.rank_runtimes.length !== r.ranks_observed) return false;
  const ids = new Set(r.rank_runtimes.map(([rank]) => rank));
  if (ids.size !== r.ranks_observed || r.rank_runtimes.some(([rank]) => rank >= r.world_size) ||
      r.straggler_ranks.some(rank => !ids.has(rank))) return false;
  if (r.communication_edges.some(e => e.source >= r.world_size || e.destination >= r.world_size)) return false;
  for (const key of ['synthetic', 'complete'] as const) {
    if (r[key] !== undefined && typeof r[key] !== 'boolean') return false;
  }
  const record = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object' && !Array.isArray(v);
  const strings = (v: unknown) => record(v) && Object.values(v).every(s => typeof s === 'string');
  if (r.metadata !== undefined) {
    if (!record(r.metadata)) return false;
    if (r.metadata.workload !== undefined && typeof r.metadata.workload !== 'string') return false;
    if (r.metadata.run_id !== undefined && typeof r.metadata.run_id !== 'string') return false;
    if (r.metadata.scheduler !== undefined && !strings(r.metadata.scheduler)) return false;
    if (r.metadata.tags !== undefined && !strings(r.metadata.tags)) return false;
  }
  if (r.ranks !== undefined && (!Array.isArray(r.ranks) || r.ranks.length !== r.ranks_observed ||
      !r.ranks.every(v => record(v) && isCount(v.rank) && ids.has(v.rank) && typeof v.hostname === 'string' &&
      isCount(v.runtime_ns) && isCount(v.mpi_time_ns) && typeof v.complete === 'boolean' && strings(v.context)))) return false;
  if (r.timeline !== undefined && (!Array.isArray(r.timeline) || r.timeline.length > 1000 ||
      !r.timeline.every(v => record(v) && isCount(v.timestamp_ns) && isCount(v.calls) && isCount(v.duration_ns)))) return false;
  return true;
}

export function comparison(a: AnalysisResult, b: AnalysisResult) {
  const workload = a.metadata?.workload;
  const comparable = !!workload && workload === b.metadata?.workload &&
    a.synthetic === false && b.synthetic === false && a.complete === true && b.complete === true;
  return {
    comparable,
    change: a.runtime_max_ns > 0 ? (b.runtime_max_ns / a.runtime_max_ns - 1) * 100 : null,
    speedup: comparable && b.runtime_max_ns > 0 ? a.runtime_max_ns / b.runtime_max_ns : null,
    reason: comparable ? 'Matching declared workload; scientific output equivalence still requires verification.'
      : 'A speedup requires matching workload identities, complete captures, and explicit nonsynthetic provenance.',
  };
}
