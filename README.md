# RankLens

RankLens is an open-source observability and performance-diagnosis system for MPI workloads. It
intercepts selected MPI calls through the standard PMPI interface, writes per-rank telemetry, and
turns that evidence into terminal, JSON, and standalone HTML reports.

The v0.1 release is deliberately narrow and usable: it instruments `MPI_Send`, `MPI_Recv`,
`MPI_Allreduce`, `MPI_Bcast`, and `MPI_Barrier`; calculates per-rank runtime and MPI time; builds a
point-to-point communication matrix; and flags stragglers, collective pressure, synchronization
wait, communication hotspots, and possible strong-scaling saturation.

RankLens recommendations are testable hypotheses. It does not invent expected speedups or claim
that an application is optimal when no rule fires.

## Architecture

```text
MPI application
    │
    ▼
PMPI interceptor (C++ shared library)
    │  one summary + optional event stream per rank
    ▼
Versioned JSON telemetry
    │
    ▼
Cross-rank analyzer (dependency-free Python)
    ├── imbalance and stragglers
    ├── operation and collective cost
    ├── communication edges and hotspots
    └── evidence-linked recommendations
         ├── terminal
         ├── JSON
         └── standalone HTML
```

The telemetry files are rank-local: the instrumented process performs no extra MPI collectives,
so observation cannot introduce a new collective deadlock. See [docs/architecture.md](docs/architecture.md)
and [docs/telemetry-schema.md](docs/telemetry-schema.md).

## Quick start

Prerequisites: CMake 3.20+, a C++17 compiler, an MPI implementation with PMPI support, and Python
3.9+.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

If CMake on an Apple Silicon Mac finds Homebrew Open MPI but the Apple command-line compiler cannot
find C++ standard headers, select Homebrew LLVM explicitly:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++ \
  -DMPI_CXX_COMPILER=/opt/homebrew/bin/mpicxx
```

Run the included workload under the interceptor:

```bash
ranklens run \
  --library build/src/interceptor/libranklens_mpi.dylib \
  --output ranklens-results \
  -- mpirun -n 2 build/examples/ranklens_ping_pong
```

On Linux, the library suffix is `.so`. `ranklens run` configures `LD_PRELOAD` on Linux or
`DYLD_INSERT_LIBRARIES` on macOS. For an existing application, replace the example command with
your normal `mpirun` or `srun` command; no source-code changes are required.

Analyze an existing capture and create machine-readable output:

```bash
ranklens analyze ranklens-results \
  --html ranklens-results/report.html \
  --json ranklens-results/analysis.json
```

Try the report pipeline without MPI. This data is explicitly marked synthetic and is not benchmark
evidence:

```bash
ranklens demo --output demo-results
```

## Runtime controls

| Variable | Default | Purpose |
|---|---:|---|
| `RANKLENS_OUTPUT_DIR` | `ranklens-results` | Destination shared or rank-local directory |
| `RANKLENS_TRACE_EVENTS` | `1` | Set to `0` for summaries only and lower I/O volume |
| `RANKLENS_LIBRARY` | unset | Default interceptor path used by `ranklens run` |

For a multi-node job, the configured output path must be visible to every rank (for example, a
shared filesystem) or rank-local files must be collected after the job. Filenames include the rank,
but two simultaneous jobs must use separate output directories.

## Test

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The suite includes pure-Python unit tests and a real two-rank MPI interposition test.

## Current boundaries

v0.1 observes blocking calls only. It does not yet instrument nonblocking completion, derive CPU or
NUMA placement, collect hardware counters, integrate directly with Slurm accounting, or estimate a
speedup. Those boundaries are intentional and tracked in [docs/roadmap.md](docs/roadmap.md).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing an interceptor or analysis rule. Every new
recommendation needs a precise evidence condition and a controlled test. Security concerns should
follow [SECURITY.md](SECURITY.md).

Licensed under Apache-2.0.
