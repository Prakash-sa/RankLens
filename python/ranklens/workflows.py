"""Portable report bundles, controlled comparisons, and a local capture catalog."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
import statistics
import subprocess
import time
import zipfile
from pathlib import Path

from .analyzer import analyze
from .report import render_html
from .runner import instrumented_environment, _forward_macos_openmpi_environment


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


def benchmark(command, library: Path, output: Path, repeats=3, timeout=300) -> dict:
    """Alternate baseline/instrumented trials; record stdout equality without claiming numerical equivalence."""
    if not command or repeats < 2 or repeats > 100 or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("provide a command, 2–100 repeats, and a positive finite timeout")
    output.mkdir(parents=True, exist_ok=False)
    trials = []
    for trial in range(repeats):
        for instrumented in ([False, True] if trial % 2 == 0 else [True, False]):
            directory = output / f"trial-{trial}-{'instrumented' if instrumented else 'baseline'}"
            directory.mkdir()
            env = instrumented_environment(library, directory) if instrumented else None
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
            trials.append({"trial": trial, "instrumented": instrumented, "elapsed_ns": elapsed,
                           "stdout_sha256": hashlib.sha256(completed.stdout).hexdigest()})
    baseline = statistics.median(t["elapsed_ns"] for t in trials if not t["instrumented"])
    measured = statistics.median(t["elapsed_ns"] for t in trials if t["instrumented"])
    result = {"command": list(command), "trials": trials, "baseline_median_ns": baseline,
              "instrumented_median_ns": measured, "overhead_percent": (measured / baseline - 1) * 100,
              "stdout_equal": len({t["stdout_sha256"] for t in trials}) == 1,
              "scope": "wall-clock launcher time; stdout equality does not prove scientific equivalence"}
    (output / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result
