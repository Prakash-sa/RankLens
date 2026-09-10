'use client';

import { useMemo, useRef, useState } from 'react';
import { demoAnalysis, isAnalysisResult, comparison, type AnalysisResult } from './analysis';

function duration(ns: number) {
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(3)} s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
  return `${(ns / 1e3).toFixed(1)} μs`;
}
function bytes(n: number) {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}
const percent = (n: number) => `${(n * 100).toFixed(1)}%`;
function download(content: string, name: string, type: string) {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const anchor = document.createElement('a');
  anchor.href = url; anchor.download = name; anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
const csvCell = (s: unknown) => {
  let value = String(s);
  if (/^[=+@\-\t\r]/.test(value)) value = "'" + value;
  return '"' + value.replaceAll('"', '""') + '"';
};

export default function Home() {
  const [analysis, setAnalysis] = useState<AnalysisResult>(demoAnalysis);
  const [baseline, setBaseline] = useState<AnalysisResult | null>(null);
  const [fileName, setFileName] = useState('Synthetic demo');
  const [baselineName, setBaselineName] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [dragging, setDragging] = useState(false);
  const [category, setCategory] = useState('all');
  const [query, setQuery] = useState('');
  const [stragglersOnly, setStragglersOnly] = useState(false);
  const [rankPage, setRankPage] = useState(0);
  const [rankSort, setRankSort] = useState('rank');
  const [opSort, setOpSort] = useState('duration_ns');
  const [edgeRank, setEdgeRank] = useState('');
  const fileInput = useRef<HTMLInputElement>(null);
  const baselineInput = useRef<HTMLInputElement>(null);
  const importGeneration = useRef(0);
  const baselineGeneration = useRef(0);
  const operations = useMemo(() => Object.entries(analysis.operations).sort((a, b) =>
    b[1][opSort as 'calls' | 'duration_ns' | 'payload_bytes'] - a[1][opSort as 'calls' | 'duration_ns' | 'payload_bytes']), [analysis, opSort]);
  const filteredRanks = useMemo(() => analysis.rank_runtimes
    .filter(([rank]) => String(rank).includes(query) && (!stragglersOnly || analysis.straggler_ranks.includes(rank)))
    .sort((a, b) => rankSort === 'runtime' ? b[1] - a[1] : a[0] - b[0]), [analysis, query, stragglersOnly, rankSort]);
  const pageCount = Math.max(1, Math.ceil(filteredRanks.length / 24));
  const currentPage = Math.min(rankPage, pageCount - 1);
  const shownRanks = filteredRanks.slice(currentPage * 24, (currentPage + 1) * 24);
  const ranksById = useMemo(() => new Map(analysis.ranks?.map(r => [r.rank, r]) ?? []), [analysis]);
  const maxRuntime = Math.max(1, analysis.runtime_max_ns);
  const warningCount = analysis.findings.filter(f => f.severity === 'warning').length;
  const categories = [...new Set(analysis.findings.map(f => f.category))];
  const findings = analysis.findings.filter(f => category === 'all' || f.category === category);
  const edges = analysis.communication_edges.filter(e => edgeRank === '' || e.source === Number(edgeRank) || e.destination === Number(edgeRank));
  const timeline = analysis.timeline ?? [];
  const maxCalls = timeline.reduce((max, row) => Math.max(max, row.calls), 1);
  const complete = analysis.complete === true;
  const synthetic = analysis.synthetic === true;
  const scheduler = analysis.metadata?.scheduler ?? {};
  const delta = baseline ? comparison(baseline, analysis) : null;
  const rss = (analysis.ranks ?? []).map(r => Number(r.context.max_rss_bytes)).filter(Number.isFinite);
  const hostCount = new Set(analysis.ranks?.map(r => r.hostname)).size;

  async function loadFile(file?: File, asBaseline = false) {
    if (!file) return;
    const generation = asBaseline ? baselineGeneration : importGeneration;
    const ticket = ++generation.current;
    if (file.size > 5_000_000) { setError('Choose an analysis JSON file smaller than 5 MB.'); return; }
    setLoading(true); setError('');
    try {
      const parsed: unknown = JSON.parse(await file.text());
      if (!isAnalysisResult(parsed)) throw new Error('Choose the analysis.json exported by RankLens. This file has missing, invalid, or inconsistent fields.');
      if (ticket !== generation.current) return;
      if (asBaseline) { setBaseline(parsed); setBaselineName(file.name); }
      else { setAnalysis(parsed); setFileName(file.name); setRankPage(0); setCategory('all'); setQuery(''); setEdgeRank(''); }
    } catch (e) {
      if (ticket === generation.current) setError(e instanceof Error ? e.message : 'Could not read this file.');
    } finally {
      if (ticket === generation.current) setLoading(false);
    }
  }
  function resetDemo() {
    importGeneration.current++; baselineGeneration.current++;
    setAnalysis(demoAnalysis); setFileName('Synthetic demo'); setBaseline(null);
    setError(''); setLoading(false); setCategory('all'); setQuery(''); setRankPage(0); setEdgeRank('');
  }
  function exportRanks() {
    const rows = [['rank', 'hostname', 'runtime_ns', 'straggler'], ...analysis.rank_runtimes.map(([rank, runtime]) =>
      [rank, ranksById.get(rank)?.hostname ?? '', runtime, analysis.straggler_ranks.includes(rank)])];
    download(rows.map(row => row.map(csvCell).join(',')).join('\n') + '\n', 'ranklens-ranks.csv', 'text/csv');
  }
  return <div className="app-shell">
    <a className="skip-link" href="#workspace">Skip to analysis</a>
    <header className="topbar">
      <a className="brand" href="#workspace"><span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>RANKLENS</a>
      <nav aria-label="Analysis sections"><a href="#diagnosis">Findings</a><a href="#ranks">Ranks</a><a href="#operations">Operations</a><a href="#comparison">Compare</a></nav>
      <span className="privacy-pill">Files stay in your browser</span>
    </header>
    <main id="workspace">
      <section className={`workspace-header ${dragging ? 'dragging' : ''}`}
        onDragOver={e => { e.preventDefault(); setDragging(true); }} onDragLeave={() => setDragging(false)}
        onDrop={e => { e.preventDefault(); setDragging(false); void loadFile(e.dataTransfer.files[0]); }}>
        <div><p className="eyebrow">MPI PERFORMANCE WORKSPACE</p><h1>Run analysis</h1><p>Open a capture, investigate the slowest ranks, and compare your next experiment.</p></div>
        <div className="toolbar">
          <button className="primary-button" onClick={() => fileInput.current?.click()} disabled={loading}>Open analysis <span aria-hidden="true">↑</span></button>
          <button className="secondary-button" onClick={() => baselineInput.current?.click()} disabled={loading}>Compare a baseline</button>
          <button className="text-button" onClick={resetDemo}>Load demo</button>
        </div>
        <input aria-label="Open RankLens analysis JSON" hidden ref={fileInput} type="file" accept=".json,application/json"
          onClick={e => { e.currentTarget.value = ''; }} onChange={e => void loadFile(e.target.files?.[0])} />
        <input aria-label="Open baseline analysis JSON" hidden ref={baselineInput} type="file" accept=".json,application/json"
          onClick={e => { e.currentTarget.value = ''; }} onChange={e => void loadFile(e.target.files?.[0], true)} />
      </section>
      <div aria-live="polite">{loading && <p className="notice">Reading analysis…</p>}</div>
      {error && <div role="alert" className="file-error">{error} <button onClick={() => setError('')}>Dismiss</button></div>}
      <section className="dataset-strip" aria-label="Current capture">
        <div><span className="pulse-dot" aria-hidden="true" /><p><small>ACTIVE CAPTURE</small><strong title={fileName}>{fileName}</strong></p></div>
        <p className="dataset-source" title={analysis.source}>{synthetic ? 'Synthetic example · not benchmark evidence' : complete ? 'Complete capture' : 'Partial capture or legacy status unknown'}</p>
        <div className="export-actions"><button onClick={() => download(JSON.stringify(analysis, null, 2), 'analysis.json', 'application/json')}>Export JSON ↓</button><button onClick={exportRanks}>Export CSV ↓</button></div>
      </section>
      {(synthetic || !complete) && <p className="notice">{synthetic ? 'Demo data illustrates the workflow. Import a real capture to evaluate your workload.' : 'This capture is incomplete or has no completion marker. Treat its findings as partial evidence.'}</p>}
      <section className="metric-grid" aria-label="Performance summary">
        <article><p>Ranks observed</p><strong>{analysis.ranks_observed}<small> / {analysis.world_size}</small></strong><footer>{hostCount ? `${hostCount} observed hosts` : 'Host metadata unavailable'}</footer></article>
        <article><p>Slowest rank</p><strong>{duration(analysis.runtime_max_ns)}</strong><footer>Median {duration(analysis.runtime_median_ns)}</footer></article>
        <article className={analysis.straggler_ranks.length ? 'alert-metric' : ''}><p>Runtime imbalance</p><strong>{analysis.imbalance_ratio.toFixed(2)}×</strong><footer>{analysis.straggler_ranks.length} stragglers · {analysis.straggler_threshold.toFixed(2)}× threshold</footer></article>
        <article><p>MPI call time / wall time</p><strong>{percent(analysis.mpi_fraction)}</strong><footer>Summed across ranks; threads may overlap</footer></article>
      </section>
      <section className="content-grid" id="diagnosis">
        <div className="main-column">
          <div className="section-heading"><div><p className="eyebrow">DIAGNOSIS</p><h2>{warningCount} warning{warningCount === 1 ? '' : 's'} to investigate</h2></div>
            <label>Category<select value={category} onChange={e => setCategory(e.target.value)}><option value="all">All findings</option>{categories.map(c => <option key={c}>{c}</option>)}</select></label></div>
          <div className="findings-list">{findings.map((f, i) => <article className={`finding-card ${f.severity}`} key={i}>
            <div className="finding-index">{String(i + 1).padStart(2, '0')}</div><div><p className="finding-category"><span>{f.severity}</span> / {f.category}</p><h3>{f.title}</h3><p>{f.evidence}</p>
              <details><summary>Recommended experiment <span aria-hidden="true">＋</span></summary><p>{f.recommendation}</p></details></div>
          </article>)}</div>
          {findings.length === 0 && <p className="empty-state">No findings in this category.</p>}
          {analysis.warnings.length > 0 && <section className="warnings"><h3>Capture quality</h3><ul>{analysis.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul></section>}
        </div>
        <aside className="rank-panel" id="ranks">
          <div className="panel-heading"><div><p className="eyebrow">RUNTIME DISTRIBUTION</p><h2>Rank profile</h2></div></div>
          <div className="rank-controls"><label>Find rank<input value={query} placeholder="Rank number" onChange={e => { setQuery(e.target.value); setRankPage(0); }} /></label>
            <label>Sort<select value={rankSort} onChange={e => setRankSort(e.target.value)}><option value="rank">Rank number</option><option value="runtime">Slowest first</option></select></label></div>
          <label className="checkbox-label"><input type="checkbox" checked={stragglersOnly} onChange={e => { setStragglersOnly(e.target.checked); setRankPage(0); }} /> Stragglers only</label>
          <div className="rank-chart">{shownRanks.map(([rank, runtime]) => <div className="rank-row" key={rank}>
            <div className="rank-meta"><strong>Rank {rank}{analysis.straggler_ranks.includes(rank) ? ' · straggler' : ''}</strong><span>{duration(runtime)}</span></div>
            <div className="rank-track"><i className={analysis.straggler_ranks.includes(rank) ? 'straggler' : ''} style={{ width: `${Math.min(100, Math.max(0, runtime / maxRuntime * 100))}%` }} /></div>
            {ranksById.get(rank) && <small className="rank-host">{ranksById.get(rank)?.hostname} · affinity {ranksById.get(rank)?.context.cpu_affinity || 'unavailable'}</small>}
          </div>)}</div>
          {!shownRanks.length && <p className="empty-state">No matching ranks.</p>}
          <div className="pagination"><button disabled={currentPage === 0} onClick={() => setRankPage(currentPage - 1)}>Previous</button><span>{currentPage + 1} / {pageCount}</span><button disabled={currentPage + 1 >= pageCount} onClick={() => setRankPage(currentPage + 1)}>Next</button></div>
        </aside>
      </section>
      <section className="operations-panel" id="operations">
        <div className="panel-heading"><div><p className="eyebrow">INSTRUMENTED OPERATIONS</p><h2>Where rank time goes</h2></div><label>Sort by<select value={opSort} onChange={e => setOpSort(e.target.value)}><option value="duration_ns">Duration</option><option value="calls">Call count</option><option value="payload_bytes">Payload</option></select></label></div>
        <div className="table-wrap" tabIndex={0} aria-label="Scrollable operation totals"><table><caption className="sr-only">MPI operation totals across observed ranks</caption><thead><tr><th scope="col">Operation</th><th scope="col">Calls</th><th scope="col">Duration</th><th scope="col">Rank-time share</th><th scope="col">Payload</th></tr></thead><tbody>
          {operations.map(([name, stats]) => { const share = stats.duration_ns / (analysis.aggregate_runtime_ns || 1); return <tr key={name}><td>{name}</td><td>{stats.calls.toLocaleString('en-US')}</td><td>{duration(stats.duration_ns)}</td><td><div className="share-cell"><span><i style={{ width: `${Math.min(100, share * 100)}%` }} /></span><b>{percent(share)}</b></div></td><td>{bytes(stats.payload_bytes)}</td></tr>; })}
        </tbody></table></div>{!operations.length && <p className="empty-state">No instrumented operations in this capture.</p>}
      </section>
      <section className="edges-panel">
        <div className="panel-heading"><div><p className="eyebrow">EVENT ACTIVITY</p><h2>Calls over rank-local time</h2></div></div>
        {timeline.some(t => t.calls) ? <><div className="timeline" role="img" aria-label="Event call count by rank-local time bucket; clocks are not synchronized across nodes">
          {timeline.map((t, i) => <div key={i} style={{ height: `${Math.max(2, t.calls / maxCalls * 100)}%` }} title={`${duration(t.timestamp_ns)}: ${t.calls} events, ${duration(t.duration_ns)} summed call time`} />)}
        </div><p className="panel-note">50 buckets of local time since each rank initialized. This is an event histogram, not a synchronized distributed trace.</p></> : <p className="empty-state">No event timeline is available. Import a capture analyzed with the current CLI and event tracing enabled.</p>}
        <div className="panel-heading"><h2>Communication edges</h2><label>Filter rank<input type="number" min="0" value={edgeRank} onChange={e => setEdgeRank(e.target.value)} placeholder="All ranks" /></label></div>
        <div className="edge-list">{edges.slice(0, 24).map((e, i) => <article key={i}><div className="edge-route"><strong>Rank {e.source}</strong><span aria-hidden="true">→</span><strong>Rank {e.destination}</strong></div><div><span>{e.messages} messages</span><b>{bytes(e.bytes)}</b></div><div className="edge-share"><i style={{ width: `${Math.min(100, e.traffic_share * 100)}%` }} /></div><small>{percent(e.traffic_share)} of summary sent bytes</small></article>)}</div>
        {!edges.length && <p className="empty-state">No matching sends. Event tracing may be disabled or no sends were observed.</p>}
        {edges.length > 24 && <p className="panel-note">Showing the 24 largest matching edges. Export JSON for the full graph.</p>}
      </section>
      <section className="operations-panel" id="comparison">
        <div className="panel-heading"><div><p className="eyebrow">CONTROLLED EXPERIMENTS</p><h2>Compare with a baseline</h2></div><button className="secondary-button" onClick={() => baselineInput.current?.click()}>Choose baseline</button></div>
        {baseline && delta ? <><p>{baselineName} → {fileName}</p><p className="notice">{delta.reason}</p>
          <div className="comparison-metrics"><article><span>Runtime change</span><strong>{delta.change === null ? 'Unavailable' : `${delta.change > 0 ? '+' : ''}${delta.change.toFixed(1)}%`}</strong><small>Candidate vs baseline, slowest rank</small></article><article><span>Speedup</span><strong>{delta.speedup === null ? 'Unverified' : `${delta.speedup.toFixed(2)}×`}</strong><small>Declared workload: {analysis.metadata?.workload || 'not recorded'}</small></article><article><span>Ranks</span><strong>{baseline.world_size} → {analysis.world_size}</strong><small>Different allocations affect cost and efficiency</small></article></div>
          <button className="text-button" onClick={() => { baselineGeneration.current++; setBaseline(null); }}>Remove baseline</button></> :
          <p className="empty-state">Open a second analysis file to compare runtime and MPI time. Use the same workload identity when capturing runs to establish a meaningful baseline.</p>}
      </section>
      <section className="coverage-strip">
        <div><p className="eyebrow">CAPTURE CONTEXT</p><h2>Evidence and provenance</h2></div>
        <article className="available"><div><strong>MPI application</strong><small>{operations.length} recorded operation types</small><small>Run: {analysis.metadata?.run_id || 'legacy / demo'}</small></div></article>
        <article><div><strong>Scheduler</strong><small>{Object.entries(scheduler).filter(([,v]) => v).map(([k,v]) => `${k}: ${v}`).join(' · ') || 'No scheduler allocation metadata'}</small></div></article>
        <article><div><strong>Process resources</strong><small>{rss.length ? `Maximum per-process RSS: ${bytes(rss.reduce((maximum, value) => Math.max(maximum, value), 0))}` : 'Resource measurements unavailable'}</small><small>GPU, storage, and fabric counters are not collected.</small></div></article>
      </section>
      <details className="getting-started"><summary>Capture a workload and open it here</summary>
        <p>Build the collector, then launch your application with an empty output directory. RankLens writes analysis.json alongside an offline HTML report.</p>
        <pre><code>ranklens run --workload input-checksum --output capture-001 -- mpirun -n 4 ./solver</code></pre>
        <p>For Slurm or Flux, use srun or flux run in place of mpirun. The output directory must be visible to every rank.</p>
      </details>
    </main>
    <footer className="site-footer"><span className="brand">RANKLENS</span><p>Test recommendations with controlled experiments.</p><span>Apache-2.0</span></footer>
  </div>;
}
