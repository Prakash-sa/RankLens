# Changelog

## Unreleased — enterprise foundation

- Moved event serialization and periodic summary I/O to a non-MPI writer thread with bounded,
  preallocated buffers and explicit drop/request-tracking coverage.
- Added native telemetry schema v2, separate API-call/request-completion accounting, source event
  sequences, capture epochs, and a complete marker published only after successful MPI finalization.
- Added an opt-in FastAPI/SQLAlchemy admission service with scoped machine authentication,
  verified NDJSON and digests, immutable local objects, idempotent receipts, overlap rejection,
  deletion-generation fencing, transactional outbox, leased normalization worker, and Alembic schema.
- Added a read-only Slurm accounting normalizer for arrays, requeues, SLUID capability discovery,
  lifecycle state and bounded `sacct --json` execution.
- Added a Go site agent that incrementally seals summaries/events to a crash-aware private spool,
  requires HTTPS off loopback, verifies durable receipts, and safely replays after outages.
- Expanded Python, API, migration, Go, and MPI integration gates. This is an implementation
  foundation; it is not yet the fleet-scale enterprise certification described in the plan.

## 1.0.0 — 2026-09-09

- Added production PMPI capture for blocking and nonblocking point-to-point operations, major
  collectives, and request completion.
- Added bounded event streams, periodic atomic summaries, explicit completeness, run identity,
  scheduler metadata, CPU affinity, process CPU time, and peak RSS.
- Added strict partial/malformed/mixed-capture handling and rank-local activity histograms.
- Added CSV and ZIP export, provenance-gated comparisons, optional allocation-cost calculation,
  local SQLite cataloging, launcher diagnostics, timeouts, and an overhead benchmark harness.
- Rebuilt the browser experience as an accessible local analysis workspace with rank, operation,
  communication, timeline, export, and baseline workflows.
- Added real MPI nonblocking integration coverage and expanded Python validation tests.

RankLens reports measured evidence and proposed experiments; it does not guarantee performance
improvements or scientific equivalence.
