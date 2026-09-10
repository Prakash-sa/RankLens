# RankLens

RankLens is a local-first observability and performance-diagnosis toolkit for MPI workloads. Its
PMPI interceptor records per-rank timing and communication evidence without source changes; its
dependency-free Python analyzer turns a capture into explainable findings, portable reports, and
controlled comparisons; and its responsive browser workspace explores an `analysis.json` without
uploading it.

RankLens 1.0 instruments blocking and nonblocking point-to-point operations, major collectives,
request completion, process placement context, and bounded event streams. Recommendations remain
testable hypotheses: RankLens does not invent speedups or claim scientific equivalence.

## What ships

- PMPI wrappers for `MPI_Send`, `MPI_Recv`, `MPI_Isend`, `MPI_Irecv`, `MPI_Wait`, `MPI_Test`,
  `MPI_Waitall`, `MPI_Allreduce`, `MPI_Bcast`, and `MPI_Barrier`;
- crash-tolerant periodic summaries, atomic final summaries, event caps, and explicit partial-run
  status;
- resolved wildcard receives and communicator-local peers mapped to world ranks;
- CPU affinity, hostname, peak RSS, scheduler identifiers, run identity, workload identity, and
  user-defined tags;
- terminal, strict JSON, standalone HTML, CSV, and offline ZIP outputs;
- provenance-gated run comparison, optional allocation-cost calculation, a local SQLite catalog,
  and an alternating baseline/instrumented overhead harness;
- an accessible web workspace with validation, rank filtering, event activity, communication
  edges, provenance, exports, and baseline comparison;
- unit tests plus real two-rank blocking and nonblocking MPI integration tests.

## Architecture

```text
MPI application
    │ PMPI interception (no added collectives)
    ▼
Per-rank summary JSON + optional bounded JSONL events
    │
    ▼
Deterministic Python analyzer
    ├── stragglers and runtime imbalance
    ├── MPI operation and synchronization cost
    ├── communication edges and traffic hotspots
    ├── terminal / JSON / HTML / CSV / ZIP
    └── controlled comparison / SQLite catalog
                 │
                 ▼
          local browser workspace
```

Telemetry is rank-local and RankLens introduces no MPI collective. See
[architecture](docs/architecture.md) and the [telemetry contract](docs/telemetry-schema.md).

## Install

Prerequisites are CMake 3.20+, a C++17 compiler, an MPI implementation with PMPI support, and
Python 3.9+.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

On Apple Silicon, if Apple Clang cannot use the headers expected by Homebrew Open MPI, select
Homebrew LLVM explicitly:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++ \
  -DMPI_CXX_COMPILER=/opt/homebrew/bin/mpicxx
```

## Capture and analyze

Use a new output directory for every run:

```bash
ranklens run \
  --library build/src/interceptor/libranklens_mpi.dylib \
  --output captures/solver-a \
  --workload cavity-re100-grid256-inputsha256 \
  --tag commit=abc123 \
  -- mpirun -n 4 ./solver input.json
```

The library suffix is `.so` on Linux. `ranklens run` configures `LD_PRELOAD` on Linux and
`DYLD_INSERT_LIBRARIES` on macOS, records launcher/scheduler provenance, and writes
`analysis.json` plus `report.html` after the command exits. Replace `mpirun` with `srun` or
`flux run` when appropriate. A multi-node output path must be visible to every rank, or rank-local
files must be collected into one directory before analysis.

Analyze previously collected telemetry and create every portable output:

```bash
ranklens analyze captures/solver-a \
  --json captures/solver-a/analysis.json \
  --html captures/solver-a/report.html \
  --csv captures/solver-a/ranks.csv \
  --bundle captures/solver-a.zip
```

## Compare, catalog, and measure overhead

The same nonempty `--workload` value and complete nonsynthetic captures are required before
RankLens reports a speedup. Workload identity is user-declared; validate scientific outputs in the
application itself.

```bash
ranklens compare captures/baseline captures/candidate --hourly-rate 12.50
ranklens catalog --database ranklens.sqlite3 --ingest captures/candidate
ranklens catalog --database ranklens.sqlite3
ranklens benchmark --library build/src/interceptor/libranklens_mpi.dylib \
  --output overhead-study --repeats 5 -- mpirun -n 4 ./solver input.json
ranklens doctor
```

`benchmark` alternates baseline and instrumented trials, reports launcher wall-clock overhead, and
records stdout hashes. Matching hashes are useful evidence but do not prove numerical equivalence.

## Web workspace

```bash
cd web
npm ci
npm run dev
```

Open the printed URL and choose an `analysis.json`. Parsing and exploration occur entirely in the
browser. Node.js 22.13+ is required; `npm run lint` and `npm run build` validate a production build.

## Runtime controls

| Variable | Default | Purpose |
|---|---:|---|
| `RANKLENS_OUTPUT_DIR` | `ranklens-results` | Capture directory visible to each rank |
| `RANKLENS_TRACE_EVENTS` | `1` | Set to `0` for summary-only, lower-I/O capture |
| `RANKLENS_MAX_EVENTS` | `100000` | Maximum event records per rank; `0` disables events |
| `RANKLENS_LIBRARY` | unset | Interceptor used by `ranklens run`, `doctor`, and `benchmark` |

RankLens preserves a pre-existing preload value. The launcher wrapper forwards RankLens and DYLD
variables through Open MPI on macOS.

## Test

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure

cd web
npm ci
npm run lint
npm run build
```

## Boundaries

RankLens currently observes MPI application behavior and lightweight process context. It does not
collect GPU counters, fabric counters, storage throughput, power, or cluster-wide utilization, and
it does not replace a scheduler or infrastructure monitoring platform. `mpi_time_ns` is summed
inclusive wall time across instrumented calls and can overlap across threads. Event timestamps are
rank-local, so the UI histogram is not a synchronized distributed trace. See the
[roadmap](docs/roadmap.md) for planned adapters and validation work.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before adding wrappers or analysis rules. Every new
recommendation needs a precise evidence condition and a controlled test. Report security issues as
described in [SECURITY.md](SECURITY.md).

Licensed under Apache-2.0.
