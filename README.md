# RankLens

### Find out why your MPI job is slow—without changing your application.

[![CI](https://github.com/Prakash-sa/RankLens/actions/workflows/ci.yml/badge.svg)](https://github.com/Prakash-sa/RankLens/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-2ea44f)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-3776ab)](pyproject.toml)

![RankLens reveals performance bottlenecks across an HPC cluster](docs/assets/ranklens-hero.png)

RankLens turns an MPI run into a clear performance report. It highlights slow ranks, communication
hotspots, time spent inside MPI, and incomplete captures so you can choose the next experiment with
confidence.

Your workload does not need source-code changes. The standard workflow stays local: telemetry is
written to your chosen directory, analysis runs on your machine, and the browser dashboard reads
the report without uploading it.

## What RankLens helps you answer

- **Which ranks are holding the job back?** See runtime imbalance and likely stragglers.
- **Where is communication time going?** Compare MPI operations and rank-to-rank traffic.
- **Did my change actually help?** Compare complete runs with workload-provenance checks.
- **Can I trust this report?** Capture gaps, dropped detail, and partial runs remain visible.
- **How can I share the result?** Export a standalone HTML report, JSON, CSV, or offline ZIP bundle.

RankLens presents evidence and recommended experiments—not invented speedup claims.

## Try the demo in two minutes

You need Python 3.9 or newer. The demo does not require MPI.

```bash
git clone https://github.com/Prakash-sa/RankLens.git
cd RankLens
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
ranklens demo --output demo-results
```

Open `demo-results/report.html` in your browser. You will also get `analysis.json`, which can be
opened in the interactive workspace:

```bash
cd web
npm ci
npm run dev
```

Choose `demo-results/analysis.json` from the workspace. The file stays in your browser.

## Analyze a real MPI job

Prerequisites: CMake 3.20+, a C++17 compiler, and an MPI implementation with PMPI support.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel

ranklens run \
  --library build/src/interceptor/libranklens_mpi.so \
  --output captures/solver-a \
  --workload cavity-re100-grid256 \
  -- mpirun -n 4 ./solver input.json
```

On macOS, the library ends in `.dylib`. On Slurm, replace `mpirun` with your normal `srun` command.
RankLens produces a terminal summary plus `report.html` and `analysis.json` in the capture directory.

Use a new output directory for every run. For multi-node jobs, choose a path visible to all ranks or
gather the rank files into one directory before analysis.

## Compare two experiments

Give baseline and candidate runs the same meaningful `--workload` value. RankLens will refuse to
report a speedup when provenance is missing, a capture is incomplete, or the workloads do not
match.

```bash
ranklens compare captures/baseline captures/candidate
```

You can optionally estimate allocation cost with `--hourly-rate`, export rank data to CSV, or keep
runs in a local SQLite catalog.

## Designed for trustworthy HPC analysis

RankLens keeps collection rank-local and does not add MPI collectives. Event detail is bounded, and
the writer works asynchronously so a slow reporting path does not become part of every MPI call.
The analyzer distinguishes MPI calls from nonblocking-request completions and retains warnings when
evidence is missing.

Current coverage includes common blocking and nonblocking point-to-point calls, Wait/Test
completion families, `MPI_Allreduce`, `MPI_Bcast`, and `MPI_Barrier` on Linux and macOS. GPU,
fabric, storage, and cluster-wide infrastructure counters are not yet collected by the local tool.

For teams building a shared service, the repository also includes an authenticated ingestion API,
a durable Go node agent, PostgreSQL migrations, a leased worker, a read-only Slurm adapter, and a
container-based development profile. Start with [the deployment guide](deploy/README.md) and review
the [security policy](SECURITY.md) before enabling network ingestion.

## Useful commands

```bash
ranklens doctor
ranklens analyze captures/solver-a --html report.html --json analysis.json --csv ranks.csv
ranklens benchmark --library build/src/interceptor/libranklens_mpi.so \
  --output overhead-study --repeats 5 -- mpirun -n 4 ./solver input.json
```

`ranklens benchmark` alternates baseline and instrumented trials and records output hashes. Matching
hashes are useful evidence, but scientific validity must still be checked with the application’s
own tolerances and validators.

## Contributing and support

Read [CONTRIBUTING.md](CONTRIBUTING.md) before adding MPI wrappers or diagnosis rules. The public
[architecture overview](docs/architecture.md) and [telemetry contract](docs/telemetry-schema.md)
describe the supported implementation without publishing internal delivery planning.

Please report security issues privately as described in [SECURITY.md](SECURITY.md). For usage
questions or reproducible bugs that do not contain sensitive workload data, open a GitHub issue.

Licensed under the [Apache License 2.0](LICENSE).
