"""Portable report bundles, controlled comparisons, and a local capture catalog."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import random
import sqlite3
import statistics
import subprocess
import time
import zipfile
from pathlib import Path
from typing import Optional

from .analyzer import analyze
from .report import render_html
from .runner import instrumented_environment, _forward_macos_openmpi_environment


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def _bootstrap_interval(
    values: list[float], statistic, *, confidence: float = 0.95, samples: int = 5000
) -> list[float]:
    """Return a deterministic percentile-bootstrap interval for observed trials.

    This quantifies sampling uncertainty in the recorded run; it does not model
    unobserved workload, placement, or system-noise regimes.
    """

    if not values:
        return [0.0, 0.0]
    generator = random.Random(0)
    estimates = [
        statistic([values[generator.randrange(len(values))] for _ in values])
        for _ in range(samples)
    ]
    tail = (1.0 - confidence) / 2.0
    return [_percentile(estimates, tail), _percentile(estimates, 1.0 - tail)]


def export_csv(result, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["rank", "hostname", "runtime_ns", "mpi_time_ns", "straggler"])
        for rank in result.ranks:
            host = rank["hostname"]
            if host.startswith(("=", "+", "-", "@", "\t", "\r")):
                host = "'" + host
            writer.writerow([rank["rank"], host, rank["runtime_ns"], rank["mpi_time_ns"],
                             rank["rank"] in result.straggler_ranks])


def bundle(result, destination: Path) -> None:
    """A report archive is readable offline and contains no executable collector."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("analysis.json", json.dumps(result.to_dict(), indent=2, allow_nan=False))
        archive.writestr("report.html", render_html(result))
        archive.writestr("README.txt", "Open report.html in a browser, or import analysis.json into RankLens.\n")


def compare(baseline, candidate, hourly_rate=None) -> dict:
    workload = baseline.metadata.get("workload")
    same_workload = bool(workload and workload == candidate.metadata.get("workload"))
    comparable = same_workload and baseline.complete and candidate.complete and not (
        baseline.synthetic or candidate.synthetic)
    base, current = baseline.runtime_max_ns, candidate.runtime_max_ns
    result = {
        "baseline": baseline.source, "candidate": candidate.source,
        "workload": workload, "comparable": comparable,
        "reason": "matching declared workload and complete captures" if comparable else
                  "requires matching nonempty workload identity, complete captures, and nonsynthetic data",
        "baseline_max_ns": base, "candidate_max_ns": current,
        "runtime_change_percent": (current / base - 1) * 100 if base else None,
        "speedup": base / current if comparable and current else None,
        "rank_count": [baseline.world_size, candidate.world_size],
        "mpi_fraction": [baseline.mpi_fraction, candidate.mpi_fraction],
        "scientific_equivalence": "not verified; workload identity is user-declared",
    }
    if hourly_rate is not None:
        if not math.isfinite(hourly_rate) or hourly_rate < 0:
            raise ValueError("hourly rate must be finite and non-negative")
        result["estimated_cost"] = {
            "basis": "user-supplied total allocation hourly rate; captured max runtime excludes queue and launch time",
            "hourly_rate": hourly_rate,
            "baseline": base / 3_600_000_000_000 * hourly_rate,
            "candidate": current / 3_600_000_000_000 * hourly_rate,
        }
    return result


def catalog(database: Path, directory=None) -> list:
    """Transactional, content-addressed report ingestion; safe to repeat after interruption."""
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database, timeout=10) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS captures (id TEXT PRIMARY KEY, source TEXT, report TEXT NOT NULL)")
        if directory is not None:
            result = analyze(directory)
            report = json.dumps(result.to_dict(), sort_keys=True, allow_nan=False)
            identity = hashlib.sha256(report.encode()).hexdigest()
            connection.execute("INSERT OR IGNORE INTO captures VALUES (?, ?, ?)", (identity, result.source, report))
        return [{"id": row[0], "source": row[1], "analysis": json.loads(row[2])}
                for row in connection.execute("SELECT id, source, report FROM captures ORDER BY rowid DESC")]


def benchmark(
    command,
    library: Path,
    output: Path,
    repeats=3,
    timeout=300,
    *,
    mode: str = "summary",
    max_median_overhead_percent: Optional[float] = None,
    max_p95_overhead_percent: Optional[float] = None,
) -> dict:
    """Alternate baseline/instrumented trials and optionally enforce overhead budgets."""
    if (
        not command
        or repeats < 2
        or repeats > 100
        or not math.isfinite(timeout)
        or timeout <= 0
        or mode not in {"summary", "detail"}
    ):
        raise ValueError("provide a command, 2–100 repeats, positive finite timeout, and valid mode")
    for budget in (max_median_overhead_percent, max_p95_overhead_percent):
        if budget is not None and (not math.isfinite(budget) or budget < 0):
            raise ValueError("overhead budgets must be finite non-negative percentages")
    output.mkdir(parents=True, exist_ok=False)
    trials = []
    for trial in range(repeats):
        for instrumented in ([False, True] if trial % 2 == 0 else [True, False]):
            directory = output / f"trial-{trial}-{'instrumented' if instrumented else 'baseline'}"
            directory.mkdir()
            env = instrumented_environment(library, directory) if instrumented else None
            if env is not None:
                env["RANKLENS_TRACE_EVENTS"] = "1" if mode == "detail" else "0"
            launch = _forward_macos_openmpi_environment(command, env) if env else command
            started = time.perf_counter_ns()
            completed = subprocess.run(launch, env=env, capture_output=True, timeout=timeout, check=False)
            elapsed = time.perf_counter_ns() - started
            (directory / "stdout.txt").write_bytes(completed.stdout)
            (directory / "stderr.txt").write_bytes(completed.stderr)
            if completed.returncode:
                raise ValueError(f"benchmark trial failed ({completed.returncode}); inspect {directory}")
            if instrumented:
                captured = analyze(directory)
                if not captured.complete:
                    raise ValueError(f"benchmark capture incomplete: {directory}")
            trials.append({
                "trial": trial,
                "instrumented": instrumented,
                "elapsed_ns": elapsed,
                "stdout_sha256": hashlib.sha256(completed.stdout).hexdigest(),
                "stderr_sha256": hashlib.sha256(completed.stderr).hexdigest(),
            })
    baseline = statistics.median(t["elapsed_ns"] for t in trials if not t["instrumented"])
    measured = statistics.median(t["elapsed_ns"] for t in trials if t["instrumented"])
    paired_overheads = []
    for trial in range(repeats):
        baseline_elapsed = next(t["elapsed_ns"] for t in trials if t["trial"] == trial and not t["instrumented"])
        measured_elapsed = next(t["elapsed_ns"] for t in trials if t["trial"] == trial and t["instrumented"])
        paired_overheads.append((measured_elapsed / baseline_elapsed - 1) * 100)
    median_overhead = statistics.median(paired_overheads)
    p95_overhead = _percentile(paired_overheads, 0.95)
    median_interval = _bootstrap_interval(paired_overheads, statistics.median)
    p95_interval = _bootstrap_interval(
        paired_overheads, lambda sample: _percentile(sample, 0.95)
    )
    budget_requested = (
        max_median_overhead_percent is not None
        or max_p95_overhead_percent is not None
    )
    minimum_budget_repeats = 5
    conclusive = repeats >= minimum_budget_repeats
    budget = {
        "max_median_overhead_percent": max_median_overhead_percent,
        "max_p95_overhead_percent": max_p95_overhead_percent,
        "minimum_repeats": minimum_budget_repeats,
        "conclusive": conclusive,
        "passed": (
            (not budget_requested or conclusive)
            and (
                max_median_overhead_percent is None
                or median_interval[1] <= max_median_overhead_percent
            )
            and (
                max_p95_overhead_percent is None
                or p95_interval[1] <= max_p95_overhead_percent
            )
        ),
    }
    result = {
        "command": list(command),
        "mode": mode,
        "repeats": repeats,
        "trials": trials,
        "baseline_median_ns": baseline,
        "instrumented_median_ns": measured,
        "overhead_percent": (measured / baseline - 1) * 100,
        "paired_overhead_percent": paired_overheads,
        "paired_median_overhead_percent": median_overhead,
        "paired_p95_overhead_percent": p95_overhead,
        "paired_median_overhead_ci95_percent": median_interval,
        "paired_p95_overhead_ci95_percent": p95_interval,
        "statistics": {
            "confidence_level": 0.95,
            "method": "deterministic percentile bootstrap over paired trials",
            "bootstrap_samples": 5000,
            "scope": "sampling uncertainty for this recorded run only",
        },
        "budget": budget,
        "stdout_equal": len({t["stdout_sha256"] for t in trials}) == 1,
        "stderr_equal": len({t["stderr_sha256"] for t in trials}) == 1,
        "scope": (
            "wall-clock launcher time; stdout/stderr equality does not prove scientific "
            "equivalence; summary mode disables event tracing"
        ),
        "environment": {
            "ranklens_trace_events": "1" if mode == "detail" else "0",
            "python_hash_seed": os.environ.get("PYTHONHASHSEED", ""),
        },
    }
    (output / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if not budget["passed"]:
        raise ValueError(
            "benchmark overhead budget failed or evidence was inconclusive; "
            f"inspect {output / 'benchmark.json'}"
        )
    return result
