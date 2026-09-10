"""Real MPI verification of request completion and communicator translation."""
import json
import os
import sys
import tempfile
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.runner import run

launcher, library, executable = sys.argv[1:]
with tempfile.TemporaryDirectory() as temporary:
    path = Path(temporary)
    assert run([launcher, "-n", "2", executable], Path(library), path) == 0
    result = analyze(path)
    assert result.complete
    assert result.operations["MPI_Isend"].calls == 4
    assert result.operations["MPI_Irecv_complete"].payload_bytes == 16
    assert sum(edge.bytes for edge in result.communication_edges) == 16
    assert {(edge.source, edge.destination) for edge in result.communication_edges} == {(0, 1), (1, 0)}
    for stream in path.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text().splitlines()]
        opened = {e["request_id"] for e in events if e["operation"] in {"MPI_Isend", "MPI_Irecv"}}
        closed = {e["request_id"] for e in events if e["operation"].endswith("_complete")}
        assert opened == closed
with tempfile.TemporaryDirectory() as temporary:
    os.environ["RANKLENS_MAX_EVENTS"] = "2"
    path = Path(temporary)
    assert run([launcher, "-n", "2", executable], Path(library), path) == 0
    result = analyze(path)
    assert result.operations["MPI_Isend"].calls == 4
    assert any("event limit" in warning for warning in result.warnings)
    assert all(len(p.read_text().splitlines()) == 2 for p in path.glob("*-events.jsonl"))
