#!/usr/bin/env python3
"""Run a real MPI process under RankLens and validate the emitted contract."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.runner import run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mpiexec", required=True)
    parser.add_argument("--np-flag", required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    if arguments.output.exists():
        shutil.rmtree(arguments.output)
    arguments.output.mkdir(parents=True)
    command = [arguments.mpiexec, arguments.np_flag, "2", str(arguments.executable)]
    return_code = run(command, arguments.library, arguments.output)
    if return_code != 0:
        raise RuntimeError(f"MPI integration command exited with {return_code}")

    result = analyze(arguments.output)
    if result.ranks_observed != 2:
        raise RuntimeError(f"expected two rank summaries, found {result.ranks_observed}")
    required_operations = {"MPI_Send", "MPI_Recv", "MPI_Allreduce", "MPI_Bcast", "MPI_Barrier"}
    missing = required_operations.difference(result.operations)
    if missing:
        raise RuntimeError(f"missing intercepted operations: {sorted(missing)}")
    if result.operations["MPI_Send"].calls != 40:
        raise RuntimeError("unexpected MPI_Send call count")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
