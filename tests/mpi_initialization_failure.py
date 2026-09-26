"""Verify recorder allocation failure cannot fail the MPI application."""

import subprocess
import sys
import tempfile
from pathlib import Path

from ranklens.runner import (
    _forward_macos_openmpi_environment,
    instrumented_environment,
)


launcher, library, executable = sys.argv[1:]
with tempfile.TemporaryDirectory() as temporary:
    output = Path(temporary)
    environment = instrumented_environment(Path(library), output)
    environment["RANKLENS_TEST_FAIL_RECORDER_INITIALIZATION"] = "1"
    command = _forward_macos_openmpi_environment(
        [launcher, "-n", "2", executable], environment
    )

    completed = subprocess.run(
        command,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    assert list(output.glob("rank-*-summary.json")) == []
    assert list(output.glob("rank-*-events.jsonl")) == []
