"""Real MPI verification for persistent request lifecycle telemetry."""

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
    required = {"MPI_Send_init", "MPI_Recv_init", "MPI_Start", "MPI_Startall"}
    assert required.issubset(result.operations)
    assert result.operations["MPI_Send_init"].calls == 2
    assert result.operations["MPI_Recv_init"].calls == 4
    assert result.operations["MPI_Startall"].calls == 6
    assert result.operations["MPI_Start"].calls == 2
    assert result.operations["MPI_Irecv_complete"].calls == 8

    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text().splitlines()]
        templates = {
            event["request_id"]
            for event in events
            if event["operation"] in {"MPI_Send_init", "MPI_Recv_init"}
        }
        completed = {
            event["request_id"]
            for event in events
            if event["operation"].endswith("_complete")
        }
        freed = {
            event["request_id"]
            for event in events
            if event["operation"] == "MPI_Request_free"
        }
        assert templates == completed == freed
