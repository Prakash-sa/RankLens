# Architecture

This page describes the current local toolkit. The proposed shared platform is specified in the
[enterprise system design](enterprise-system-design.md), with explicit baseline gaps and delivery gates.

RankLens 1.0 separates capture, contract, analysis, and presentation so each layer can fail or
evolve without changing application semantics.

## Capture layer

`libranklens_mpi` exports normal MPI symbols and forwards them to PMPI. Each rank owns its files
and RankLens never introduces a collective. Timing uses `std::chrono::steady_clock`; bounded,
preallocated event buffers and fixed operation slots keep serialization and filesystem writes off
ordinary MPI call paths. A non-MPI writer thread drains detail and publishes snapshots. Mutexes
protect recorder and pending-request state for MPI thread support; their measured contention remains
part of the observer-overhead gate.

Blocking calls are recorded at completion. Nonblocking initiation records the request and logical
payload, while `Wait`, `Test`, and `Waitall` close tracked requests and resolve receive status.
Communicator-local ranks are translated to `MPI_COMM_WORLD` ranks when possible. Application MPI
return codes are preserved.

The writer publishes `summary.json.tmp` about once per second and atomically renames it to a partial
summary. It publishes `pre_finalize` before the underlying finalization and `finalized` with
`complete: true` only after `PMPI_Finalize` returns successfully. Rename is not an fsync durability
guarantee. Event output is optional and bounded by a live buffer and lifetime cap; drops and request
tracking overflow are explicit. Payload bytes are logical application payload, not wire traffic.

## Contract and run provenance

Current native rank summaries and events use schema major version 2. The analyzer also accepts
legacy native version 1 while preserving its accounting meaning. Readers reject unknown major
versions, inconsistent rank/world-size data, invalid lifecycle states, and mixed API/completion
totals.

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
- Failed calls contribute API count and duration, zero payload, and a separate failure counter.
  Request-completion lifecycle records remain separate from API-invocation totals in version 2;
  version-1 imports preserve the older semantics.
- Missing or unfinished summaries remain analyzable only as explicitly partial captures.
- Malformed event lines are skipped with bounded warnings; malformed summary files fail analysis.
- A launcher timeout or failure is persisted to `run.json`; telemetry already flushed is retained.
- Output directories must be empty, preventing accidental cross-run aggregation.

## Performance discipline

Instrumentation overhead must be measured with controlled workloads. Record input identity, code
commit, MPI implementation, node type/count, placement, scheduler ID, and thread environment.
Disable event tracing to isolate summary-only cost. The bundled benchmark harness alternates trial
order and reports observed wall-clock overhead without making a universal overhead claim.
