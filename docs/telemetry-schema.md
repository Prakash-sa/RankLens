# Telemetry schema v1

Every integer duration is in nanoseconds and every byte count is a non-negative integer.

## Rank summary

Filename: `rank-<zero-padded-rank>-summary.json`

```json
{
  "schema_version": 1,
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
    "MPI_Send": {
      "calls": 10,
      "duration_ns": 50000000,
      "payload_bytes": 4096
    }
  }
}
```

`runtime_ns` begins after successful `PMPI_Init`/`PMPI_Init_thread` and ends immediately before
`PMPI_Finalize`. `mpi_time_ns` is the sum of inclusive wall time inside instrumented blocking calls.
It is not CPU time and operations can overlap across ranks.

## Event stream

Filename: `rank-<zero-padded-rank>-events.jsonl`

Each line is an independent JSON object:

```json
{"schema_version":1,"timestamp_ns":1000,"rank":0,"operation":"MPI_Send","duration_ns":800,"payload_bytes":4096,"peer":1,"tag":7,"error_code":0}
```

`timestamp_ns` is relative to recorder initialization. `peer` is a source/destination/root where
that concept applies and `-1` otherwise. For a receive using `MPI_ANY_SOURCE` or `MPI_ANY_TAG`, the
recorded values are the resolved status values. `payload_bytes` is application payload; for a
collective it does not represent implementation-specific network traffic.

## Compatibility policy

Readers must reject unknown major schema versions. New optional fields may be added within schema
v1. Existing fields will retain their meanings for the lifetime of v1.

