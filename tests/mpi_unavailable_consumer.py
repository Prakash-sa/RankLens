"""Verify an unavailable capture destination cannot fail the MPI application."""

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
    root = Path(temporary)
    blocked_output = root / "capture-blocked-by-file"
    blocked_output.write_text("not a directory", encoding="utf-8")
    environment = instrumented_environment(Path(library), blocked_output)
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
    assert blocked_output.is_file()
    assert blocked_output.read_text(encoding="utf-8") == "not a directory"
    assert list(root.glob("rank-*-summary.json")) == []
