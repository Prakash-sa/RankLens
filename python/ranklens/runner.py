"""Launch MPI applications with the RankLens PMPI library preloaded."""

from __future__ import annotations

import os
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
        candidates.append(explicit)
    configured = os.environ.get("RANKLENS_LIBRARY")
    if configured:
        candidates.append(Path(configured))

    extension = ".dylib" if platform.system() == "Darwin" else ".so"
    package_root = Path(__file__).resolve().parents[2]
    candidates.extend(
        [
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
    for name in ("DYLD_INSERT_LIBRARIES", "DYLD_FORCE_FLAT_NAMESPACE", "RANKLENS_OUTPUT_DIR"):
        if name in environment:
            exports.extend(["-x", name])
    return [adjusted[0], *exports, *adjusted[1:]]


def run(command: Sequence[str], library: Path, output: Path) -> int:
    if not command:
        raise RunnerError("no MPI launcher or application command was provided after --")
    output = output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    environment = instrumented_environment(library, output)
    launch_command = _forward_macos_openmpi_environment(command, environment)
    completed = subprocess.run(launch_command, env=environment, check=False)
    return completed.returncode
