"""RankLens command-line interface."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from .analyzer import TelemetryError, analyze
from .demo import write_demo
from .report import render_text, write_html, write_json
from .runner import RunnerError, discover_library, run


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ranklens",
        description="Observe and diagnose MPI workload behavior with PMPI telemetry.",
    )
    parser.add_argument("--version", action="version", version="RankLens 0.1.0")
    subcommands = parser.add_subparsers(dest="subcommand", required=True)

    analyze_parser = subcommands.add_parser("analyze", help="analyze a telemetry directory")
    analyze_parser.add_argument("directory", type=Path)
    analyze_parser.add_argument("--html", type=Path, help="write a standalone HTML report")
    analyze_parser.add_argument("--json", type=Path, help="write the analysis as JSON")
    analyze_parser.add_argument(
        "--straggler-threshold",
        type=float,
        default=1.20,
        help="flag ranks at or above this multiple of median runtime (default: 1.20)",
    )

    run_parser = subcommands.add_parser("run", help="launch a command with MPI interception")
    run_parser.add_argument("--library", type=Path, help="path to libranklens_mpi")
    run_parser.add_argument("--output", type=Path, default=Path("ranklens-results"))
    run_parser.add_argument(
        "--no-analyze", action="store_true", help="do not print a report after a successful run"
    )
    run_parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="command after --, e.g. mpirun -n 4 ./solver"
    )

    demo_parser = subcommands.add_parser("demo", help="generate and analyze synthetic demo data")
    demo_parser.add_argument("--output", type=Path, default=Path("demo-results"))
    return parser


def _analyze_command(
    directory: Path,
    threshold: float,
    html_destination: Optional[Path] = None,
    json_destination: Optional[Path] = None,
) -> int:
    result = analyze(directory, threshold)
    print(render_text(result), end="")
    if html_destination:
        write_html(result, html_destination)
        print(f"HTML report written to {html_destination.resolve()}")
    if json_destination:
        write_json(result, json_destination)
        print(f"JSON analysis written to {json_destination.resolve()}")
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
            )

        if arguments.subcommand == "demo":
            output = arguments.output.expanduser().resolve()
            write_demo(output)
            print(f"Generated synthetic demo telemetry in {output}\n")
            return _analyze_command(output, 1.20, output / "report.html", output / "analysis.json")

        if arguments.subcommand == "run":
            command = list(arguments.command)
            if command and command[0] == "--":
                command = command[1:]
            library = discover_library(arguments.library)
            return_code = run(command, library, arguments.output)
            if return_code == 0 and not arguments.no_analyze:
                try:
                    print()
                    _analyze_command(arguments.output, 1.20, arguments.output / "report.html")
                except TelemetryError as exc:
                    print(f"ranklens: run completed but telemetry analysis was unavailable: {exc}", file=sys.stderr)
            return return_code
    except (RunnerError, TelemetryError, ValueError) as exc:
        print(f"ranklens: error: {exc}", file=sys.stderr)
        return 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())

