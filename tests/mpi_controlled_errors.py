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
    assert result.operations["MPI_Send"].calls == 4
    assert result.operations["MPI_Send"].payload_bytes == 0
    assert result.operations["MPI_Isend"].calls == 4
    assert result.operations["MPI_Isend"].payload_bytes == 8
    assert result.operations["MPI_Irecv"].calls == 4
    assert result.operations["MPI_Allreduce"].calls == 2
    assert result.operations["MPI_Allreduce"].payload_bytes == 0
    assert result.operations["MPI_Irecv_complete"].payload_bytes == 8

    for summary_path in output.glob("rank-*-summary.json"):
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["failed_calls"] == 5
        send_stats = summary["operations"]["MPI_Send"]
        assert send_stats["calls"] == 2
        assert send_stats["payload_bytes"] == 0

    for stream in output.glob("*-events.jsonl"):
        events = [json.loads(line) for line in stream.read_text(encoding="utf-8").splitlines()]
        failed = [event for event in events if event["error_code"] != 0]
        assert len(failed) == 5
        assert {event["operation"] for event in failed} == {
            "MPI_Send",
            "MPI_Isend",
            "MPI_Irecv",
            "MPI_Allreduce",
        }
        assert all(event["payload_bytes"] == 0 for event in failed)
        assert all(event["communicator"] == -1 for event in failed)
        assert all(event["request_id"] == -1 for event in failed)
        opened = {
            event["request_id"]
            for event in events
            if event["operation"] in {"MPI_Isend", "MPI_Irecv"}
            and event["request_id"] >= 0
        }
        completed = {
            event["request_id"]
            for event in events
            if event["operation"].endswith("_complete")
        }
        assert opened == completed
