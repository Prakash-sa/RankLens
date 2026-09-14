"""RankLens command-line interface."""

from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence

from . import __version__
from .analyzer import TelemetryError, analyze
from .demo import write_demo
from .report import render_text, write_html, write_json
from .runner import RunnerError, discover_library, run
from .workflows import bundle, catalog, compare, export_csv, benchmark


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ranklens",
        description="Observe and diagnose MPI workload behavior with PMPI telemetry.",
    )
    parser.add_argument("--version", action="version", version=f"RankLens {__version__}")
    subcommands = parser.add_subparsers(dest="subcommand", required=True)

    analyze_parser = subcommands.add_parser("analyze", help="analyze a telemetry directory")
    analyze_parser.add_argument("directory", type=Path)
    analyze_parser.add_argument("--html", type=Path, help="write a standalone HTML report")
    analyze_parser.add_argument("--json", type=Path, help="write the analysis as JSON")
    analyze_parser.add_argument("--csv", type=Path, help="export per-rank rows for DataFrames")
    analyze_parser.add_argument("--bundle", type=Path, help="create an offline ZIP report bundle")
    analyze_parser.add_argument(
        "--straggler-threshold",
        type=float,
        default=1.20,
        help="flag ranks at or above this multiple of median runtime (default: 1.20)",
    )

    run_parser = subcommands.add_parser("run", help="launch a command with MPI interception")
    run_parser.add_argument("--library", type=Path, help="path to libranklens_mpi")
    run_parser.add_argument("--output", type=Path, default=Path("ranklens-results"))
    run_parser.add_argument("--workload", default="", help="identity of scientific inputs/configuration for comparison")
    run_parser.add_argument("--tag", action="append", default=[], metavar="KEY=VALUE")
    run_parser.add_argument("--timeout", type=float, help="stop a run after this many seconds")
    run_parser.add_argument(
        "--no-analyze", action="store_true", help="do not analyze telemetry after the command exits"
    )
    run_parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="command after --, e.g. mpirun -n 4 ./solver"
    )

    demo_parser = subcommands.add_parser("demo", help="generate and analyze synthetic demo data")
    demo_parser.add_argument("--output", type=Path, default=Path("demo-results"))
    compare_parser = subcommands.add_parser("compare", help="compare two captures with provenance checks")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--hourly-rate", type=float, help="total allocation hourly cost, in your currency")
    compare_parser.add_argument("--json", type=Path)
    catalog_parser = subcommands.add_parser("catalog", help="ingest/list captures in a local SQLite catalog")
    catalog_parser.add_argument("--database", type=Path, default=Path("ranklens-catalog.sqlite3"))
    catalog_parser.add_argument("--ingest", type=Path)
    bench_parser = subcommands.add_parser("benchmark", help="measure observer overhead with alternating trials")
    bench_parser.add_argument("--library", type=Path)
    bench_parser.add_argument("--output", type=Path, required=True)
    bench_parser.add_argument("--repeats", type=int, default=3)
    bench_parser.add_argument("--timeout", type=float, default=300)
    bench_parser.add_argument(
        "--mode",
        choices=("summary", "detail"),
        default="summary",
        help="capture mode for instrumented trials; summary disables event tracing",
    )
    bench_parser.add_argument(
        "--max-median-overhead-percent",
        type=float,
        help="fail if paired median overhead is above this percentage",
    )
    bench_parser.add_argument(
        "--max-p95-overhead-percent",
        type=float,
        help="fail if paired p95 overhead is above this percentage",
    )
    bench_parser.add_argument("command", nargs=argparse.REMAINDER)
    subcommands.add_parser("doctor", help="check local launcher and interceptor availability")
    return parser


def _analyze_command(
    directory: Path,
    threshold: float,
    html_destination: Optional[Path] = None,
    json_destination: Optional[Path] = None,
    csv_destination: Optional[Path] = None,
    bundle_destination: Optional[Path] = None,
) -> int:
    result = analyze(directory, threshold)
    print(render_text(result), end="")
    if html_destination:
        write_html(result, html_destination)
        print(f"HTML report written to {html_destination.resolve()}")
    if json_destination:
        write_json(result, json_destination)
        print(f"JSON analysis written to {json_destination.resolve()}")
    if csv_destination:
        export_csv(result, csv_destination)
    if bundle_destination:
        bundle(result, bundle_destination)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.subcommand == "analyze":
            return _analyze_command(
                arguments.directory,
                arguments.straggler_threshold,
                arguments.html,
                arguments.json,
                arguments.csv,
                arguments.bundle,
            )

        if arguments.subcommand == "demo":
            output = arguments.output.expanduser().resolve()
            if output.exists() and any(output.iterdir()):
                raise ValueError("demo output directory must be empty")
            write_demo(output)
            print(f"Generated synthetic demo telemetry in {output}\n")
            return _analyze_command(output, 1.20, output / "report.html", output / "analysis.json")

        if arguments.subcommand == "run":
            command = list(arguments.command)
            if command and command[0] == "--":
                command = command[1:]
            library = discover_library(arguments.library)
            tags = {}
            for tag in arguments.tag:
                key, separator, value = tag.partition("=")
                if not separator or not key:
                    raise ValueError("tags must use KEY=VALUE")
                tags[key] = value
            if arguments.timeout is not None and (not math.isfinite(arguments.timeout) or arguments.timeout <= 0):
                raise ValueError("timeout must be positive and finite")
            return_code = run(command, library, arguments.output, workload=arguments.workload,
                              tags=tags, timeout=arguments.timeout)
            if not arguments.no_analyze:
                try:
                    print()
                    _analyze_command(arguments.output, 1.20, arguments.output / "report.html", arguments.output / "analysis.json")
                except TelemetryError as exc:
                    print(f"ranklens: run completed but telemetry analysis was unavailable: {exc}", file=sys.stderr)
            return return_code
        if arguments.subcommand == "compare":
            data = compare(analyze(arguments.baseline), analyze(arguments.candidate), arguments.hourly_rate)
            rendered = json.dumps(data, indent=2, allow_nan=False) + "\n"
            if arguments.json:
                arguments.json.parent.mkdir(parents=True, exist_ok=True)
                arguments.json.write_text(rendered, encoding="utf-8")
            print(rendered, end="")
            return 0
        if arguments.subcommand == "catalog":
            print(json.dumps(catalog(arguments.database, arguments.ingest), indent=2))
            return 0
        if arguments.subcommand == "benchmark":
            command = arguments.command[1:] if arguments.command[:1] == ["--"] else arguments.command
            print(json.dumps(
                benchmark(
                    command,
                    discover_library(arguments.library),
                    arguments.output,
                    arguments.repeats,
                    arguments.timeout,
                    mode=arguments.mode,
                    max_median_overhead_percent=arguments.max_median_overhead_percent,
                    max_p95_overhead_percent=arguments.max_p95_overhead_percent,
                ),
                indent=2,
            ))
            return 0
        if arguments.subcommand == "doctor":
            checks = {name: shutil.which(name) for name in ("mpirun", "srun", "flux", "cmake")}
            try:
                checks["library"] = str(discover_library())
            except RunnerError:
                checks["library"] = None
            print(json.dumps(checks, indent=2))
            return 0 if checks["library"] else 1
    except (RunnerError, TelemetryError, ValueError, OSError, UnicodeError, subprocess.SubprocessError) as exc:
        print(f"ranklens: error: {exc}", file=sys.stderr)
        return 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
