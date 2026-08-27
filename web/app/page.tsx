'use client';

import { ChangeEvent, DragEvent, useMemo, useRef, useState } from 'react';

type OperationStats = { calls: number; duration_ns: number; payload_bytes: number };
type Finding = { severity: 'warning' | 'info' | 'ok'; category: string; title: string; evidence: string; recommendation: string };
type CommunicationEdge = { source: number; destination: number; bytes: number; messages: number; traffic_share: number };
type AnalysisResult = {
  source: string; world_size: number; ranks_observed: number; runtime_median_ns: number;
  runtime_max_ns: number; mpi_time_ns: number; aggregate_runtime_ns: number;
  mpi_fraction: number; imbalance_ratio: number; straggler_threshold: number;
  straggler_ranks: number[]; operations: Record<string, OperationStats>;
  communication_edges: CommunicationEdge[]; rank_runtimes: [number, number][];
  findings: Finding[]; warnings: string[];
};

const demoAnalysis: AnalysisResult = {
  source: 'Synthetic 4-rank stencil · demo capture', world_size: 4, ranks_observed: 4,
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

function isAnalysisResult(value: unknown): value is AnalysisResult {
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

function formatDuration(ns: number) {
  if (ns >= 1_000_000_000) return `${(ns / 1_000_000_000).toFixed(2)} s`;
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)} ms`;
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)} μs`;
  return `${ns} ns`;
}

function formatBytes(bytes: number) {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes; let index = 0;
  while (value >= 1024 && index < units.length - 1) { value /= 1024; index += 1; }
  return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

const percent = (value: number) => `${(value * 100).toFixed(1)}%`;

export default function Home() {
  const [analysis, setAnalysis] = useState<AnalysisResult>(demoAnalysis);
  const [fileName, setFileName] = useState('demo-analysis.json');
  const [error, setError] = useState('');
  const [dragging, setDragging] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const operations = useMemo(() => Object.entries(analysis.operations).sort((a, b) => b[1].duration_ns - a[1].duration_ns), [analysis]);
  const maxRuntime = analysis.rank_runtimes.reduce((maximum, [, runtime]) => Math.max(maximum, runtime), 1);
  const criticalCount = analysis.findings.filter((finding) => finding.severity === 'warning').length;

  async function loadFile(file?: File) {
    if (!file) return;
    if (file.size > 5_000_000) { setError('That file is larger than 5 MB. Choose a RankLens analysis JSON file.'); return; }
    try {
      const parsed: unknown = JSON.parse(await file.text());
      if (!isAnalysisResult(parsed)) throw new Error('invalid schema');
      setAnalysis(parsed); setFileName(file.name); setError('');
    } catch {
      setError('This does not look like RankLens analysis JSON. Run `ranklens analyze … --json analysis.json` and try again.');
    }
  }

  function handleInput(event: ChangeEvent<HTMLInputElement>) { void loadFile(event.target.files?.[0]); }
  function handleDrop(event: DragEvent<HTMLDivElement>) { event.preventDefault(); setDragging(false); void loadFile(event.dataTransfer.files?.[0]); }
  function downloadAnalysis() {
    const blob = new Blob([`${JSON.stringify(analysis, null, 2)}\n`], { type: 'application/json' });
    const url = URL.createObjectURL(blob); const anchor = document.createElement('a');
    anchor.href = url; anchor.download = fileName; anchor.click(); URL.revokeObjectURL(url);
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="#top" aria-label="RankLens dashboard home"><span className="brand-mark" aria-hidden="true"><i /><i /><i /></span><span>RANKLENS</span></a>
        <nav aria-label="Primary navigation"><a href="#diagnosis">Diagnosis</a><a href="#ranks">Ranks</a><a href="#operations">Operations</a></nav>
        <span className="privacy-pill"><span aria-hidden="true">●</span> Local-only analysis</span>
      </header>

      <main id="top">
        <section className="hero">
          <div className="hero-copy">
            <p className="eyebrow"><span>MPI PERFORMANCE INTELLIGENCE</span><b>SCHEMA V1</b></p>
            <h1>Find the rank<br />holding everyone back.</h1>
            <p className="hero-summary">Turn raw MPI telemetry into evidence you can act on. RankLens surfaces imbalance, collective pressure, and communication hotspots—without sending your workload data anywhere.</p>
            <div className="hero-actions">
              <button className="primary-button" onClick={() => fileInput.current?.click()}>Open analysis JSON <span aria-hidden="true">↗</span></button>
              <button className="text-button" onClick={() => { setAnalysis(demoAnalysis); setFileName('demo-analysis.json'); setError(''); }}>Reset demo <span aria-hidden="true">↺</span></button>
              <input ref={fileInput} className="sr-only" type="file" accept="application/json,.json" onClick={(event) => { event.currentTarget.value = ''; }} onChange={handleInput} />
            </div>
          </div>

          <div className={`drop-card ${dragging ? 'is-dragging' : ''}`} onDragEnter={(event) => { event.preventDefault(); setDragging(true); }} onDragOver={(event) => event.preventDefault()} onDragLeave={() => setDragging(false)} onDrop={handleDrop}>
            <div className="drop-card-head"><span className="window-dots" aria-hidden="true"><i /><i /><i /></span><span>capture / analysis.json</span><span className="status-live">READY</span></div>
            <div className="terminal-preview" aria-label="Current capture summary">
              <span>$ ranklens analyze ./capture</span>
              <strong><b>✓</b> {analysis.ranks_observed} of {analysis.world_size} ranks observed</strong>
              <strong><b>!</b> {criticalCount} actionable findings</strong>
              <span className="terminal-muted">max / median&nbsp;&nbsp;&nbsp;{analysis.imbalance_ratio.toFixed(2)}×</span>
              <span className="terminal-muted">MPI time&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{percent(analysis.mpi_fraction)}</span>
            </div>
            <button className="drop-target" onClick={() => fileInput.current?.click()}><span className="upload-glyph" aria-hidden="true">↑</span><span><strong>Drop analysis JSON here</strong><small>or click to choose · max 5 MB</small></span></button>
            {error && <p className="file-error" role="alert">{error}</p>}
          </div>
        </section>

        <section className="dataset-strip" aria-label="Loaded dataset">
          <div><span className="pulse-dot" aria-hidden="true" /><p><small>ACTIVE DATASET</small><strong>{fileName}</strong></p></div>
          <p className="dataset-source">{analysis.source}</p><button onClick={downloadAnalysis}>Export JSON <span aria-hidden="true">↓</span></button>
        </section>

        <section className="metric-grid" aria-label="Performance summary">
          <article><p>Ranks observed <span>01</span></p><strong>{analysis.ranks_observed}<small> / {analysis.world_size}</small></strong><footer><i className={analysis.ranks_observed === analysis.world_size ? 'good' : 'warn'} /> {analysis.ranks_observed === analysis.world_size ? 'Capture complete' : 'Capture incomplete'}</footer></article>
          <article><p>Median runtime <span>02</span></p><strong>{formatDuration(analysis.runtime_median_ns)}</strong><footer>Across observed ranks</footer></article>
          <article className={analysis.imbalance_ratio >= analysis.straggler_threshold ? 'alert-metric' : ''}><p>Max / median <span>03</span></p><strong>{analysis.imbalance_ratio.toFixed(2)}×</strong><footer><i className={analysis.imbalance_ratio >= analysis.straggler_threshold ? 'warn' : 'good'} /> Threshold {analysis.straggler_threshold.toFixed(2)}×</footer></article>
          <article><p>MPI time fraction <span>04</span></p><strong>{percent(analysis.mpi_fraction)}</strong><footer>Aggregate rank time</footer></article>
        </section>

        <section className="coverage-strip" aria-labelledby="coverage-title">
          <div><p className="eyebrow">SIGNAL COVERAGE</p><h2 id="coverage-title">Know what this capture can prove.</h2></div>
          <article className="available"><span>01</span><div><strong>Application / PMPI</strong><small>Rank timing, calls, collectives, payloads</small></div><b>AVAILABLE</b></article>
          <article><span>02</span><div><strong>Scheduler / Slurm</strong><small>Queue, allocation, job-step context</small></div><b>NOT IN CAPTURE</b></article>
          <article><span>03</span><div><strong>Infrastructure</strong><small>CPU, memory, GPU, storage, network</small></div><b>NOT IN CAPTURE</b></article>
        </section>

        <section className="content-grid" id="diagnosis">
          <div className="main-column">
            <div className="section-heading"><div><p className="eyebrow">EVIDENCE-BASED DIAGNOSIS</p><h2>{criticalCount} signals worth investigating</h2></div><span>Ordered by impact</span></div>
            <div className="findings-list">
              {analysis.findings.map((finding, index) => (
                <article className={`finding-card ${finding.severity}`} key={`${finding.category}-${index}`}>
                  <div className="finding-index">{String(index + 1).padStart(2, '0')}</div>
                  <div><p className="finding-category"><span>{finding.severity}</span> / {finding.category}</p><h3>{finding.title}</h3><p>{finding.evidence}</p>
                    <details><summary>Recommended experiment <span aria-hidden="true">＋</span></summary><p>{finding.recommendation}</p></details>
                  </div>
                </article>
              ))}
            </div>
          </div>

          <aside className="rank-panel" id="ranks">
            <div className="panel-heading"><div><p className="eyebrow">RUNTIME DISTRIBUTION</p><h2>Rank profile</h2></div><span>seconds</span></div>
            <div className="rank-chart">
              {analysis.rank_runtimes.map(([rank, runtime]) => {
                const isStraggler = analysis.straggler_ranks.includes(rank);
                return <div className="rank-row" key={rank}><div className="rank-meta"><strong>R{String(rank).padStart(2, '0')}</strong><span>{formatDuration(runtime)}</span></div><div className="rank-track"><i className={isStraggler ? 'straggler' : ''} style={{ width: `${Math.max(3, runtime / maxRuntime * 100)}%` }} /></div>{isStraggler && <span className="straggler-label">STRAGGLER</span>}</div>;
              })}
            </div>
            <div className="chart-legend"><span><i className="legend-normal" /> Nominal</span><span><i className="legend-alert" /> Above threshold</span></div>
          </aside>
        </section>

        <section className="operations-panel" id="operations">
          <div className="panel-heading"><div><p className="eyebrow">INSTRUMENTED OPERATIONS</p><h2>Where rank time goes</h2></div><span>{operations.length} operation types</span></div>
          <div className="table-wrap"><table><thead><tr><th>Operation</th><th>Calls</th><th>Duration</th><th>Rank-time share</th><th>Payload</th></tr></thead><tbody>
            {operations.map(([name, stats]) => { const share = stats.duration_ns / (analysis.aggregate_runtime_ns || 1); return <tr key={name}><td><span className="op-mark" aria-hidden="true" />{name}</td><td>{stats.calls.toLocaleString()}</td><td>{formatDuration(stats.duration_ns)}</td><td><div className="share-cell"><span><i style={{ width: `${Math.min(100, share * 100)}%` }} /></span><b>{percent(share)}</b></div></td><td>{formatBytes(stats.payload_bytes)}</td></tr>; })}
          </tbody></table></div>
        </section>

        <section className="edges-panel">
          <div className="panel-heading"><div><p className="eyebrow">COMMUNICATION MAP</p><h2>Top point-to-point edges</h2></div><span>Observed MPI_Send traffic</span></div>
          <div className="edge-list">
            {analysis.communication_edges.slice(0, 6).map((edge) => <article key={`${edge.source}-${edge.destination}`}><div className="edge-route"><strong>R{String(edge.source).padStart(2, '0')}</strong><span aria-hidden="true">⟶</span><strong>R{String(edge.destination).padStart(2, '0')}</strong></div><div><span>{edge.messages.toLocaleString()} messages</span><b>{formatBytes(edge.bytes)}</b></div><div className="edge-share"><i style={{ width: `${Math.max(2, edge.traffic_share * 100)}%` }} /></div><small>{percent(edge.traffic_share)} of sent bytes</small></article>)}
            {analysis.communication_edges.length === 0 && <p className="empty-state">No point-to-point sends were observed. Event tracing may be disabled.</p>}
          </div>
        </section>

        {analysis.warnings.length > 0 && <section className="warnings" aria-labelledby="warnings-title"><h2 id="warnings-title">Capture warnings</h2><ul>{analysis.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul></section>}
      </main>

      <footer className="site-footer"><div className="brand"><span className="brand-mark" aria-hidden="true"><i /><i /><i /></span><span>RANKLENS</span></div><p>Recommendations are testable hypotheses, not promised speedups.</p><span>v0.1 · Apache-2.0</span></footer>
    </div>
  );
}
