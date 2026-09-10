# Changelog

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

