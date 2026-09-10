# Telemetry schema v1

Durations are non-negative integer nanoseconds and payload sizes are non-negative integer bytes.
Readers reject unknown major schema versions. New optional fields can be added within v1 without
changing existing meanings.

## Run manifest

Filename: `run.json`. The launcher writes it atomically before and after the command.

```json
{
  "schema_version": 1,
  "run_id": "62f2d4c477944c2a84ae9dc610fbd13f",
  "workload": "cavity-re100-grid256-inputsha256",
  "tags": {"commit": "abc123"},
  "command": ["mpirun", "-n", "2", "./solver"],
  "started_at": "2026-09-09T18:00:00+00:00",
  "finished_at": "2026-09-09T18:00:02+00:00",
  "state": "completed",
  "return_code": 0,
  "scheduler": {"SLURM_JOB_ID": "12345"}
}
```

`workload` and tags are user-declared provenance. They are not independently verified.

## Rank summary

Filename: `rank-<zero-padded-rank>-summary.json`.

```json
{
  "schema_version": 1,
  "complete": true,
  "context": {
    "run_id": "62f2d4c477944c2a84ae9dc610fbd13f",
    "slurm_job_id": "12345",
    "slurm_step_id": "0",
    "flux_job_id": "",
    "traceparent": "",
    "events_dropped": "0",
    "tracing_enabled": "true",
    "max_rss_bytes": "4194304",
    "user_cpu_us": "1200000",
    "system_cpu_us": "50000",
    "cpu_affinity": "0,1"
  },
  "rank": 0,
  "world_size": 2,
  "hostname": "node-a",
  "pid": 1234,
  "runtime_ns": 1800000000,
  "mpi_time_ns": 600000000,
  "mpi_calls": 50,
  "bytes_sent": 4096,
  "bytes_received": 4096,
  "operations": {
    "MPI_Send": {"calls": 10, "duration_ns": 50000000, "payload_bytes": 4096}
  }
}
```

A periodic checkpoint has `complete: false`; the final atomic summary has `complete: true`.
`runtime_ns` begins after successful MPI initialization and ends at the snapshot. `mpi_time_ns`
is summed inclusive wall time in instrumented calls. It can exceed process wall time when multiple
threads overlap. RSS units are normalized to bytes. Context values are strings for forward
compatibility.

## Event stream

Filename: `rank-<zero-padded-rank>-events.jsonl`. Each line is independent.

```json
{"schema_version":1,"timestamp_ns":1000,"rank":0,"operation":"MPI_Isend","duration_ns":800,"payload_bytes":4096,"peer":1,"tag":7,"error_code":0,"communicator":3,"request_id":42}
```

`timestamp_ns` is relative to that rank's recorder initialization; clocks are not synchronized.
`peer` is mapped to an `MPI_COMM_WORLD` rank when possible and is negative when inapplicable or
undefined. Wildcard receives use the resolved status at completion. `communicator` is the local
MPI Fortran handle value and is diagnostic only—not a globally stable communicator identity.

Nonblocking request initiation and its `MPI_Isend_complete` or `MPI_Irecv_complete` event share
a process-local `request_id`. Receive payload at completion is the byte count reported by MPI.
A successful `MPI_Isend` event contributes to directed communication edges; its zero-duration
completion event does not count the payload again.

`payload_bytes` is logical application payload, not protocol overhead or collective algorithm
wire traffic. Failed MPI calls remain visible in events with their return code, but aggregate
payload and communication edges count only successful calls.

## Analysis JSON

`analysis.json` includes schema version 1, aggregate metrics, findings, capture warnings, run
metadata, per-rank context, a rank-local 50-bucket event histogram, and communication edges. A
strict browser validator rejects missing, nonfinite, negative, out-of-range, or internally
inconsistent fields.

