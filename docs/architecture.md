# Architecture

RankLens 1.0 separates capture, contract, analysis, and presentation so each layer can fail or
evolve without changing application semantics.

## Capture layer

`libranklens_mpi` exports normal MPI symbols and forwards them to PMPI. Each rank writes its own
files and RankLens never introduces a collective. Timing uses `std::chrono::steady_clock`; a mutex
protects local recorder and pending-request state for MPI thread support.

Blocking calls are recorded at completion. Nonblocking initiation records the request and logical
payload, while `Wait`, `Test`, and `Waitall` close tracked requests and resolve receive status.
Communicator-local ranks are translated to `MPI_COMM_WORLD` ranks when possible. Application MPI
return codes are preserved.

The collector snapshots `summary.json.tmp` and atomically publishes a partial summary about once
per second. `MPI_Finalize` publishes the complete summary. Event output is optional and capped per
rank. Payload bytes are logical application payload, not wire traffic.

## Contract and run provenance

The rank summary and event formats use schema major version 1. Readers reject unknown major
versions, inconsistent rank/world-size data, and invalid metrics. Optional fields can grow within
the version.

The launcher creates an atomic `run.json` containing a random run ID, timestamps, command, declared
workload identity, tags, scheduler environment, and final return state. The analyzer rejects mixed
run identities and marks missing ranks, failed launches, event caps, and unfinished summaries as
partial evidence.

## Analysis layer

The dependency-free Python analyzer deterministically aggregates operation time, computes rank
imbalance, reconstructs successful send edges, builds a bounded rank-local event histogram, and
evaluates documented diagnostic rules. Every finding includes evidence and a recommended
experiment—not a promised optimization.

The same result model drives terminal, JSON, HTML, CSV, ZIP, comparison, and SQLite workflows.
Comparisons expose an observed runtime delta for context, but report speedup only for complete,
nonsynthetic captures with the same declared workload identity.

## Presentation layer

The standalone HTML report embeds no remote assets. The web workspace strictly validates imported
analysis JSON and keeps files in the browser. Its timeline intentionally labels rank-local time;
cross-node clocks are not treated as synchronized.

## Failure behavior

- A telemetry directory or event-file error disables affected recording and never changes an MPI
  call's return value.
- MPI errors may appear in the event stream but are excluded from successful-operation aggregates.
- Missing or unfinished summaries remain analyzable only as explicitly partial captures.
- Malformed event lines are skipped with bounded warnings; malformed summary files fail analysis.
- A launcher timeout or failure is persisted to `run.json`; telemetry already flushed is retained.
- Output directories must be empty, preventing accidental cross-run aggregation.

## Performance discipline

Instrumentation overhead must be measured with controlled workloads. Record input identity, code
commit, MPI implementation, node type/count, placement, scheduler ID, and thread environment.
Disable event tracing to isolate summary-only cost. The bundled benchmark harness alternates trial
order and reports observed wall-clock overhead without making a universal overhead claim.

