"""Launch MPI applications with the RankLens PMPI library preloaded."""

from __future__ import annotations

import os
import json
import uuid
from datetime import datetime, timezone
import platform
import subprocess
import sys
from pathlib import Path
from typing import Mapping, Optional, Sequence


class RunnerError(ValueError):
    """Raised for an invalid RankLens launch configuration."""


def discover_library(explicit: Optional[Path] = None) -> Path:
    candidates = []
    if explicit is not None:
        resolved = explicit.expanduser().resolve()
        if not resolved.is_file():
            raise RunnerError(f"explicit interceptor library does not exist: {resolved}")
        return resolved
    configured = os.environ.get("RANKLENS_LIBRARY")
    if configured:
        candidates.append(Path(configured))

    extension = ".dylib" if platform.system() == "Darwin" else ".so"
    package_root = Path(__file__).resolve().parents[2]
    candidates.extend(
        [
            Path.cwd() / "build" / "src" / "interceptor" / f"libranklens_mpi{extension}",
            package_root / "build" / "src" / "interceptor" / f"libranklens_mpi{extension}",
            Path(sys.prefix) / "lib" / f"libranklens_mpi{extension}",
            Path("/usr/local/lib") / f"libranklens_mpi{extension}",
        ]
    )
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.is_file():
            return resolved
    searched = ", ".join(str(candidate) for candidate in candidates)
    raise RunnerError(
        "RankLens interceptor library was not found. Build it with CMake and pass "
        f"--library, or set RANKLENS_LIBRARY. Searched: {searched}"
    )


def instrumented_environment(
    library: Path, output: Path, base: Optional[Mapping[str, str]] = None
) -> dict:
    environment = dict(os.environ if base is None else base)
    system = platform.system()
    if system == "Darwin":
        key = "DYLD_INSERT_LIBRARIES"
        environment["DYLD_FORCE_FLAT_NAMESPACE"] = "1"
    elif system == "Linux":
        key = "LD_PRELOAD"
    else:
        raise RunnerError(f"preload-based instrumentation is not supported on {system}")

    previous = environment.get(key)
    environment[key] = str(library) if not previous else f"{library}{os.pathsep}{previous}"
    environment["RANKLENS_OUTPUT_DIR"] = str(output.expanduser().resolve())
    return environment


def _forward_macos_openmpi_environment(command: Sequence[str], environment: Mapping[str, str]) -> list:
    """Open MPI on macOS requires explicit export of DYLD variables to child ranks."""
    adjusted = list(command)
    if platform.system() != "Darwin" or not adjusted:
        return adjusted
    if Path(adjusted[0]).name not in {"mpirun", "mpiexec", "prterun"}:
        return adjusted
    try:
        version = subprocess.run(
            [adjusted[0], "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return adjusted
    version_text = f"{version.stdout}\n{version.stderr}"
    if not any(marker in version_text for marker in ("Open MPI", "OpenRTE", "PRRTE")):
        return adjusted
    exports = []
    for name in sorted(environment):
        if not (name.startswith("RANKLENS_") or name in {"DYLD_INSERT_LIBRARIES", "DYLD_FORCE_FLAT_NAMESPACE"}):
            continue
        if name in environment:
            exports.extend(["-x", name])
    return [adjusted[0], *exports, *adjusted[1:]]


def run(command: Sequence[str], library: Path, output: Path, *, workload: str = "",
        tags: Optional[Mapping[str, str]] = None, timeout: Optional[float] = None) -> int:
    if not command:
        raise RunnerError("no MPI launcher or application command was provided after --")
    output = output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise RunnerError(f"capture directory must be empty to avoid mixing runs: {output}")
    environment = instrumented_environment(library, output)
    run_id = uuid.uuid4().hex
    capture_epoch = uuid.uuid4().hex
    environment["RANKLENS_RUN_ID"] = run_id
    environment["RANKLENS_CAPTURE_EPOCH"] = capture_epoch
    environment.setdefault("RANKLENS_ATTEMPT_ID", run_id)
    metadata = {"schema_version": 1, "run_id": run_id, "workload": workload,
                "capture_epoch": capture_epoch,
                "attempt_id": environment["RANKLENS_ATTEMPT_ID"],
                "tags": dict(tags or {}), "command": list(command),
                "started_at": datetime.now(timezone.utc).isoformat(), "state": "running",
                "scheduler": {k: v for k, v in os.environ.items() if k in {
                    "SLURM_JOB_ID", "SLURM_JOB_NAME", "SLURM_NTASKS", "SLURM_JOB_NODELIST",
                    "SLURM_CPUS_PER_TASK", "FLUX_JOB_ID", "PCLUSTER_CLUSTER_NAME"}}}
    def save_metadata():
        temporary = output / "run.json.tmp"
        temporary.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        temporary.replace(output / "run.json")
    save_metadata()
    launch_command = _forward_macos_openmpi_environment(command, environment)
    try:
        completed = subprocess.run(launch_command, env=environment, check=False, timeout=timeout)
        code = completed.returncode
    except subprocess.TimeoutExpired:
        code = 124
    except KeyboardInterrupt:
        code = 130
    except OSError as exc:
        metadata["error"] = str(exc)
        code = 127
    metadata.update(return_code=code, state="completed" if code == 0 else "failed",
                    finished_at=datetime.now(timezone.utc).isoformat())
    save_metadata()
    return code
