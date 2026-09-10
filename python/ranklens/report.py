"""Terminal, JSON, and standalone HTML report rendering."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import List

from .models import AnalysisResult


def _seconds(nanoseconds: int) -> float:
    return nanoseconds / 1_000_000_000


def _bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


def render_text(result: AnalysisResult) -> str:
    lines = [
        "RANKLENS PERFORMANCE REPORT",
        "=" * 56,
        f"Capture status    {'complete' if result.complete else 'INCOMPLETE'}{' / SYNTHETIC DATA' if result.synthetic else ''}",
        f"Ranks observed     {result.ranks_observed}/{result.world_size}",
        f"Median runtime     {_seconds(result.runtime_median_ns):.6f} s",
        f"Maximum runtime    {_seconds(result.runtime_max_ns):.6f} s",
        f"Runtime imbalance  {result.imbalance_ratio:.2f}x max/median",
        f"MPI time fraction  {result.mpi_fraction:.1%} (aggregate rank time)",
        "",
        "Operation totals",
        "-" * 56,
    ]
    for name, stats in sorted(
        result.operations.items(), key=lambda item: item[1].duration_ns, reverse=True
    ):
        fraction = stats.duration_ns / (result.aggregate_runtime_ns or 1)
        lines.append(
            f"{name:<20} {stats.calls:>8} calls  {_seconds(stats.duration_ns):>10.6f} s  "
            f"{fraction:>6.1%}  {_bytes(stats.payload_bytes):>10}"
        )

    lines.extend(["", "Findings", "-" * 56])
    for finding in result.findings:
        lines.extend(
            [
                f"[{finding.severity.upper()}] {finding.title}",
                f"  Evidence: {finding.evidence}",
                f"  Action:   {finding.recommendation}",
                "",
            ]
        )

    if result.communication_edges:
        lines.extend(["Top communication edges", "-" * 56])
        for edge in result.communication_edges[:5]:
            lines.append(
                f"rank {edge.source} -> {edge.destination}: {_bytes(edge.bytes)}, "
                f"{edge.messages} messages, {edge.traffic_share:.1%} of sent bytes"
            )
        lines.append("")

    if result.warnings:
        lines.extend(["Capture warnings", "-" * 56])
        lines.extend(f"- {warning}" for warning in result.warnings)
    return "\n".join(lines).rstrip() + "\n"


def write_json(result: AnalysisResult, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8")


def _bar_rows(result: AnalysisResult) -> str:
    maximum = result.runtime_max_ns or 1
    rows: List[str] = []
    for rank, runtime in result.rank_runtimes:
        width = max(1.0, runtime / maximum * 100)
        marker = " straggler" if rank in result.straggler_ranks else ""
        rows.append(
            '<div class="rank-row">'
            f'<span class="rank-label">Rank {rank}</span>'
            f'<div class="bar-track"><div class="bar{marker}" style="width:{width:.2f}%"></div></div>'
            f'<span class="rank-value">{_seconds(runtime):.6f}s</span>'
            "</div>"
        )
    return "\n".join(rows)


def render_html(result: AnalysisResult) -> str:
    operation_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(name)}</td><td>{stats.calls:,}</td>"
        f"<td>{_seconds(stats.duration_ns):.6f}s</td>"
        f"<td>{stats.duration_ns / (result.aggregate_runtime_ns or 1):.1%}</td>"
        f"<td>{_bytes(stats.payload_bytes)}</td>"
        "</tr>"
        for name, stats in sorted(
            result.operations.items(), key=lambda item: item[1].duration_ns, reverse=True
        )
    )
    finding_cards = "\n".join(
        f'<article class="finding {html.escape(finding.severity)}">'
        f'<span class="severity">{html.escape(finding.severity.upper())}</span>'
        f"<h3>{html.escape(finding.title)}</h3>"
        f"<p><strong>Evidence.</strong> {html.escape(finding.evidence)}</p>"
        f"<p><strong>Recommended next experiment.</strong> {html.escape(finding.recommendation)}</p>"
        "</article>"
        for finding in result.findings
    )
    edge_rows = "\n".join(
        "<tr>"
        f"<td>{edge.source}</td><td>{edge.destination}</td><td>{edge.messages:,}</td>"
        f"<td>{_bytes(edge.bytes)}</td><td>{edge.traffic_share:.1%}</td>"
        "</tr>"
        for edge in result.communication_edges[:10]
    ) or '<tr><td colspan="5">Event tracing was disabled or no point-to-point sends were observed.</td></tr>'
    warning_html = "".join(f"<li>{html.escape(warning)}</li>" for warning in result.warnings)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>RankLens performance report</title>
  <style>
    :root {{ --ink:#18212f; --muted:#64748b; --line:#dbe3ed; --brand:#2563eb;
      --brand2:#14b8a6; --warning:#f59e0b; --danger:#dc2626; --paper:#f8fafc; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font:15px/1.55 system-ui,sans-serif; }}
    main {{ width:min(1120px,92vw); margin:42px auto 80px; }}
    header {{ padding:32px; color:white; border-radius:18px; background:linear-gradient(130deg,#0f172a,#1d4ed8); }}
    header p {{ color:#dbeafe; margin:6px 0 0; }}
    h1,h2,h3 {{ line-height:1.2; }} h1 {{ margin:0; font-size:34px; }} h2 {{ margin-top:34px; }}
    .metrics {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; margin:20px 0; }}
    .metric,.panel,.finding {{ background:white; border:1px solid var(--line); border-radius:14px; box-shadow:0 5px 18px #0f172a0a; }}
    .metric {{ padding:18px; }} .metric span {{ display:block; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .metric strong {{ display:block; margin-top:6px; font-size:23px; }} .panel {{ padding:22px; overflow:auto; }}
    table {{ width:100%; border-collapse:collapse; }} th,td {{ padding:11px; border-bottom:1px solid var(--line); text-align:left; }}
    th {{ color:var(--muted); font-size:12px; text-transform:uppercase; }}
    .rank-row {{ display:grid; grid-template-columns:70px 1fr 92px; gap:12px; align-items:center; margin:9px 0; }}
    .bar-track {{ height:13px; border-radius:10px; background:#e2e8f0; overflow:hidden; }}
    .bar {{ height:100%; border-radius:10px; background:linear-gradient(90deg,var(--brand),var(--brand2)); }}
    .bar.straggler {{ background:linear-gradient(90deg,var(--warning),var(--danger)); }}
    .rank-value {{ text-align:right; color:var(--muted); font-variant-numeric:tabular-nums; }}
    .findings {{ display:grid; gap:13px; }} .finding {{ padding:20px; border-left:5px solid var(--brand); }}
    .finding.warning {{ border-left-color:var(--warning); }} .finding.ok {{ border-left-color:var(--brand2); }}
    .finding h3 {{ margin:6px 0; }} .finding p {{ margin:7px 0; }}
    .severity {{ color:var(--muted); font-size:11px; letter-spacing:.08em; }}
    footer {{ color:var(--muted); margin-top:30px; }} code {{ background:#e2e8f0; padding:2px 5px; border-radius:5px; }}
  </style>
</head>
<body><main>
  <header><h1>RankLens</h1><p>Evidence-driven MPI performance report · schema v1</p><p>{'SYNTHETIC DATA — not benchmark evidence' if result.synthetic else 'Captured telemetry'} · {'Complete capture' if result.complete else 'INCOMPLETE capture'}</p></header>
  <section class="metrics">
    <div class="metric"><span>Ranks observed</span><strong>{result.ranks_observed}/{result.world_size}</strong></div>
    <div class="metric"><span>Median runtime</span><strong>{_seconds(result.runtime_median_ns):.4f}s</strong></div>
    <div class="metric"><span>Max / median</span><strong>{result.imbalance_ratio:.2f}×</strong></div>
    <div class="metric"><span>MPI time fraction</span><strong>{result.mpi_fraction:.1%}</strong></div>
  </section>
  <h2>Rank runtime distribution</h2><section class="panel">{_bar_rows(result)}</section>
  <h2>Diagnosis</h2><section class="findings">{finding_cards}</section>
  <h2>Instrumented operations</h2><section class="panel"><table><thead><tr><th>Operation</th><th>Calls</th><th>Time</th><th>Rank-time share</th><th>Payload</th></tr></thead><tbody>{operation_rows}</tbody></table></section>
  <h2>Communication edges</h2><section class="panel"><table><thead><tr><th>Source</th><th>Destination</th><th>Messages</th><th>Bytes</th><th>Share</th></tr></thead><tbody>{edge_rows}</tbody></table></section>
  {f'<h2>Capture warnings</h2><section class="panel"><ul>{warning_html}</ul></section>' if warning_html else ''}
  <footer>Generated by RankLens v1.0. Recommendations are hypotheses to validate with controlled benchmarks, not promised speedups.</footer>
</main></body></html>
"""


def write_html(result: AnalysisResult, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_html(result), encoding="utf-8")
