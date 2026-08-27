# Architecture

RankLens v0.1 has three deliberately separate layers.

## Capture layer

`libranklens_mpi` exports normal MPI symbols and forwards each call to the corresponding PMPI
symbol. Timing uses `std::chrono::steady_clock`, so clock adjustments cannot produce negative
durations. Each rank writes its own files and never performs a RankLens-added collective.

The interceptor does bounded work around a call: datatype-size lookup, timestamping, aggregate
counter updates, and (when enabled) one JSON Lines append. A mutex protects local state when MPI is
initialized with thread support. Payload bytes are logical application payload, not a claim about
wire bytes or collective algorithm traffic.

## Contract layer

The capture/analyzer boundary is a versioned JSON contract. Summary files contain the minimum data
needed for cross-rank diagnosis; event files enable directed communication edges. The analyzer
rejects unsupported schema versions and inconsistent world sizes, and warns about partial captures.

This boundary makes future collectors possible without coupling them to the interceptor. A Slurm
epilog, node agent, or streaming collector can move the same files without changing analysis rules.

## Analysis layer

The Python package is dependency-free and deterministic. It aggregates operation time across ranks,
compares each runtime with the median, reconstructs `MPI_Send` edges, evaluates transparent rules,
and renders the same result to text, JSON, or HTML.

Rules produce:

1. the evidence that crossed a documented threshold;
2. a category and severity; and
3. a next experiment, not a guaranteed optimization.

## Failure behavior

- Failure to create the telemetry directory disables recording for that rank and never changes the
  wrapped MPI call's return value.
- MPI errors are recorded in events and returned unchanged to the application.
- Missing summaries produce an explicit partial-capture warning.
- Malformed event lines are skipped with a warning; malformed summary files fail analysis because
  aggregate conclusions would be unreliable.
- RankLens flushes on `MPI_Finalize`. Abrupt process termination can leave event data without a
  summary, which v0.2 will address with crash-tolerant checkpoints.

## Performance discipline

Instrumentation overhead must be measured on controlled workloads. Useful comparisons record the
application and input checksum, code commit, MPI implementation, node type/count, rank placement,
thread environment, scheduler job ID, and per-rank timing. Event tracing can be disabled to measure
the summary-only path. No overhead target is claimed before reproducible benchmark data exists.

