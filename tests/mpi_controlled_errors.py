"""Real MPI verification for controlled failed-call telemetry."""

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
    assert result.operations["MPI_Send"].calls == 2
    assert result.operations["MPI_Send"].payload_bytes == 0
    assert result.operations["MPI_Isend"].calls == 2
    assert result.operations["MPI_Irecv_complete"].payload_bytes == 8

    for summary_path in output.glob("rank-*-summary.json"):
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["failed_calls"] == 1
        send_stats = summary["operations"]["MPI_Send"]
        assert send_stats["calls"] == 1
        assert send_stats["payload_bytes"] == 0

    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text(encoding="utf-8").splitlines()]
        failed = [event for event in events if event["operation"] == "MPI_Send"]
        assert len(failed) == 1
        assert failed[0]["error_code"] != 0
        assert failed[0]["payload_bytes"] == 0
        assert failed[0]["communicator"] == -1
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
