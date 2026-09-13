"""Real MPI verification for concurrent MPI_THREAD_MULTIPLE capture."""

import json
import os
import sys
import tempfile
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.runner import run


launcher, library, executable = sys.argv[1:]
with tempfile.TemporaryDirectory() as temporary:
    output = Path(temporary)
    previous_buffer = os.environ.get("RANKLENS_EVENT_BUFFER_RECORDS")
    previous_events = os.environ.get("RANKLENS_MAX_EVENTS")
    os.environ["RANKLENS_EVENT_BUFFER_RECORDS"] = "8192"
    os.environ["RANKLENS_MAX_EVENTS"] = "10000"
    try:
        code = run([launcher, "-n", "2", executable], Path(library), output, timeout=30)
    finally:
        if previous_buffer is None:
            os.environ.pop("RANKLENS_EVENT_BUFFER_RECORDS", None)
        else:
            os.environ["RANKLENS_EVENT_BUFFER_RECORDS"] = previous_buffer
        if previous_events is None:
            os.environ.pop("RANKLENS_MAX_EVENTS", None)
        else:
            os.environ["RANKLENS_MAX_EVENTS"] = previous_events
    if code == 77:
        raise SystemExit(0)
    assert code == 0
    result = analyze(output)
    assert result.complete
    assert result.operations["MPI_Isend"].calls == 512
    assert result.operations["MPI_Irecv"].calls == 512
    assert result.operations["MPI_Waitall"].calls == 512
    assert result.operations["MPI_Isend_complete"].calls == 512
    assert result.operations["MPI_Irecv_complete"].calls == 512
    assert result.operations["MPI_Irecv_complete"].payload_bytes == 2048
    assert result.operations["MPI_Allreduce"].calls == 2

    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text().splitlines()]
        opened = {
            event["request_id"]
            for event in events
            if event["operation"] in {"MPI_Isend", "MPI_Irecv"}
        }
        completed = {
            event["request_id"]
            for event in events
            if event["operation"].endswith("_complete")
        }
        assert opened == completed
