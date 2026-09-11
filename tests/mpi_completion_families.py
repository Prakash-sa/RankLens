"""Real MPI verification for the nonblocking completion families."""

import json
import sys
import tempfile
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.runner import run


launcher, library, executable = sys.argv[1:]
with tempfile.TemporaryDirectory() as temporary:
    output = Path(temporary)
    assert run([launcher, "-n", "2", executable], Path(library), output) == 0
    result = analyze(output)
    assert result.complete
    required = {"MPI_Testall", "MPI_Waitany", "MPI_Waitsome", "MPI_Testany", "MPI_Testsome"}
    assert required.issubset(result.operations)
    assert result.operations["MPI_Isend"].calls == 10
    assert result.operations["MPI_Irecv_complete"].payload_bytes == 40
    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text().splitlines()]
        opened = {event["request_id"] for event in events if event["operation"] in {"MPI_Isend", "MPI_Irecv"}}
        closed = {event["request_id"] for event in events if event["operation"].endswith("_complete")}
        assert opened == closed
