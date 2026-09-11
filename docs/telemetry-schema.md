# Telemetry schemas v1 and v2

Durations are non-negative integer nanoseconds and payload sizes are non-negative integer bytes.
Readers reject unknown major schema versions. The launcher manifest and analysis output remain
version 1. Native summaries and events emitted by the current collector are version 2; the analyzer
continues to read version-1 captures without silently changing their stored values.

## Run manifest

Filename: `run.json`. The launcher writes it atomically before and after the command.

```json
{
  "schema_version": 1,
  "run_id": "62f2d4c477944c2a84ae9dc610fbd13f",
  "capture_epoch": "e2d6bc6a16d74df994f8bc798d8bdf35",
  "attempt_id": "62f2d4c477944c2a84ae9dc610fbd13f",
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
  "schema_version": 2,
  "complete": true,
  "capture_state": "finalized",
  "finalize_return_code": 0,
  "context": {
    "run_id": "62f2d4c477944c2a84ae9dc610fbd13f",
    "capture_epoch": "e2d6bc6a16d74df994f8bc798d8bdf35",
    "attempt_id": "62f2d4c477944c2a84ae9dc610fbd13f",
    "slurm_job_id": "12345",
    "slurm_step_id": "0",
    "flux_job_id": "",
    "traceparent": "",
    "events_dropped": "0",
    "events_written": "50",
    "event_buffer_records": "1024",
    "request_tracking_overflows": "0",
    "writer_failed": "false",
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
  "request_completions": 4,
  "failed_calls": 0,
  "bytes_sent": 4096,
  "bytes_received": 4096,
  "operations": {
    "MPI_Send": {
      "calls": 10,
      "duration_ns": 50000000,
      "payload_bytes": 4096,
      "record_kind": "api_call"
    },
    "MPI_Isend_complete": {
      "calls": 4,
      "duration_ns": 0,
      "payload_bytes": 0,
      "record_kind": "request_completion"
    }
  }
}
```

A periodic checkpoint has `capture_state: "collecting"` and `complete: false`. A writer thread that
never calls MPI performs event and summary filesystem I/O, so ordinary recorded calls do not write
files. Before `PMPI_Finalize`, the writer is stopped and publishes `pre_finalize`; only after
`PMPI_Finalize` returns successfully is `finalized` published with `complete: true`. This can delay
the finalize wrapper while its last file is written, but it does not claim that a pre-finalize file
proves process or scheduler success. Atomic rename avoids half-written JSON; it is not an fsync
durability guarantee.

`runtime_ns` begins after successful MPI initialization and ends at the snapshot. `mpi_time_ns`
is summed inclusive wall time in instrumented calls. It can exceed process wall time when multiple
threads overlap. RSS units are normalized to bytes. Context values are strings for forward
compatibility.

In version 2, `mpi_calls` counts wrapped API invocations and excludes synthetic request-completion
records. `request_completions` counts those lifecycle records separately. Failed calls contribute
to invocation count and duration, have zero payload, and increment `failed_calls`. Each operation's
`record_kind` makes the distinction machine-readable. Version-1 operation totals retain their
legacy meaning, where completion records contributed to the overall call total and failure counts
were not separately available.

## Event stream

Filename: `rank-<zero-padded-rank>-events.jsonl`. Each line is independent.

```json
{"schema_version":2,"sequence":7,"timestamp_ns":1000,"rank":0,"operation":"MPI_Isend","record_kind":"api_call","duration_ns":800,"payload_bytes":4096,"peer":1,"tag":7,"error_code":0,"communicator":3,"request_id":42}
```

`timestamp_ns` is relative to that rank's recorder initialization; clocks are not synchronized.
`peer` is mapped to an `MPI_COMM_WORLD` rank when possible and is negative when inapplicable or
undefined. Wildcard receives use the resolved status at completion. `communicator` is the local
MPI Fortran handle value and is diagnostic only—not a globally stable communicator identity.

Nonblocking request initiation and its `MPI_Isend_complete` or `MPI_Irecv_complete` event share
a process-local `request_id`. Receive payload at completion is the byte count reported by MPI.
A successful `MPI_Isend` event contributes to directed communication edges; its zero-duration
completion event does not count the payload again.

Events are staged in a preallocated, bounded per-process buffer and written asynchronously. Once
the configured lifetime event limit or live buffer capacity is exhausted, optional detail is
dropped and `events_dropped` increases; summary accounting continues. `request_tracking_overflows`
independently reports when the bounded nonblocking-request table cannot accept another request.

`payload_bytes` is logical application payload, not protocol overhead or collective algorithm
wire traffic. Failed MPI calls remain visible in events with their return code, but aggregate
payload and communication edges count only successful calls.

## Analysis JSON

`analysis.json` includes schema version 1, aggregate metrics, findings, capture warnings, run
metadata, per-rank context, a rank-local 50-bucket event histogram, and communication edges. A
strict browser validator rejects missing, nonfinite, negative, out-of-range, or internally
inconsistent fields.
