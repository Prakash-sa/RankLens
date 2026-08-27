"""Generate deterministic synthetic telemetry for a zero-infrastructure demo."""

from __future__ import annotations

import json
from pathlib import Path


def write_demo(directory: Path) -> None:
    """Write a clearly labeled synthetic four-rank capture."""
    directory.mkdir(parents=True, exist_ok=True)
    runtimes = [2_000_000_000, 2_050_000_000, 1_980_000_000, 2_900_000_000]
    barrier_times = [510_000_000, 480_000_000, 535_000_000, 60_000_000]
    allreduce_times = [410_000_000, 405_000_000, 415_000_000, 420_000_000]
    send_bytes = [65_536, 8_192, 8_192, 8_192]

    for rank in range(4):
        operations = {
            "MPI_Allreduce": {
                "calls": 50,
                "duration_ns": allreduce_times[rank],
                "payload_bytes": 409_600,
            },
            "MPI_Barrier": {
                "calls": 50,
                "duration_ns": barrier_times[rank],
                "payload_bytes": 0,
            },
            "MPI_Send": {
                "calls": 8,
                "duration_ns": 18_000_000,
                "payload_bytes": send_bytes[rank],
            },
            "MPI_Recv": {
                "calls": 8,
                "duration_ns": 22_000_000,
                "payload_bytes": send_bytes[(rank - 1) % 4],
            },
        }
        mpi_time = sum(item["duration_ns"] for item in operations.values())
        summary = {
            "schema_version": 1,
            "synthetic": True,
            "rank": rank,
            "world_size": 4,
            "hostname": f"demo-node-{rank // 2}",
            "pid": 4000 + rank,
            "runtime_ns": runtimes[rank],
            "mpi_time_ns": mpi_time,
            "mpi_calls": sum(item["calls"] for item in operations.values()),
            "bytes_sent": send_bytes[rank],
            "bytes_received": send_bytes[(rank - 1) % 4],
            "operations": operations,
        }
        summary_path = directory / f"rank-{rank:05d}-summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

        event_path = directory / f"rank-{rank:05d}-events.jsonl"
        destination = (rank + 1) % 4
        per_message = send_bytes[rank] // 8
        events = [
            {
                "schema_version": 1,
                "timestamp_ns": index * 10_000_000,
                "rank": rank,
                "operation": "MPI_Send",
                "duration_ns": 2_250_000,
                "payload_bytes": per_message,
                "peer": destination,
                "tag": index,
                "error_code": 0,
            }
            for index in range(8)
        ]
        event_path.write_text(
            "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
            encoding="utf-8",
        )

    (directory / "DEMO_DATA.txt").write_text(
        "This directory contains deterministic synthetic RankLens telemetry.\n"
        "It demonstrates report rendering and is not benchmark evidence.\n",
        encoding="utf-8",
    )

