"""Real MPI verification for MPI-4 partitioned request telemetry."""

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
    expected_calls = {
        "MPI_Psend_init": 2,
        "MPI_Precv_init": 2,
        "MPI_Pready": 2,
        "MPI_Pready_range": 2,
        "MPI_Pready_list": 2,
        "MPI_Parrived": 2,
    }
    for operation, calls in expected_calls.items():
        assert result.operations[operation].calls == calls
    assert result.operations["MPI_Isend_complete"].calls == 2
    assert result.operations["MPI_Irecv_complete"].calls == 2

    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text().splitlines()]
        templates = {
            event["request_id"]
            for event in events
            if event["operation"] in {"MPI_Psend_init", "MPI_Precv_init"}
        }
        lifecycle = {
            event["request_id"]
            for event in events
            if event["operation"]
            in {"MPI_Pready", "MPI_Pready_range", "MPI_Pready_list", "MPI_Parrived"}
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
        assert templates == lifecycle == completed == freed
