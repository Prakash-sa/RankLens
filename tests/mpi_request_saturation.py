"""Verify request tracking fails open with explicit bounded-loss evidence."""

import json
import os
import sys
import tempfile
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.runner import run


launcher, library, executable = sys.argv[1:]
previous = os.environ.get("RANKLENS_MAX_TRACKED_REQUESTS")
os.environ["RANKLENS_MAX_TRACKED_REQUESTS"] = "2"
try:
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        assert run([launcher, "-n", "2", executable], Path(library), output) == 0
        result = analyze(output)
        assert result.complete
        assert result.operations["MPI_Irecv"].calls == 8
        assert result.operations["MPI_Isend"].calls == 8
        assert result.operations["MPI_Irecv_complete"].calls == 4
        assert "MPI_Isend_complete" not in result.operations
        assert result.coverage["request_lifecycle"] == "partial"
        assert "request_tracking_limit_reached" in result.coverage["reasons"]
        assert any("request tracking limit" in warning for warning in result.warnings)
        for summary in output.glob("rank-*-summary.json"):
            data = json.loads(summary.read_text())
            assert data["context"]["request_tracking_limit"] == "2"
            assert data["context"]["request_tracking_overflows"] == "6"
finally:
    if previous is None:
        os.environ.pop("RANKLENS_MAX_TRACKED_REQUESTS", None)
    else:
        os.environ["RANKLENS_MAX_TRACKED_REQUESTS"] = previous
