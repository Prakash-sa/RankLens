# Contributing to RankLens

Thank you for improving RankLens. The project values correct MPI behavior, reproducible performance
evidence, and recommendations whose limits are visible.

## Development setup

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
PYTHONPATH=python python3 -m unittest discover -s tests -v
ctest --test-dir build --output-on-failure
```

## Interceptor changes

- Always call the matching `PMPI_*` symbol and preserve its return code.
- Do not add a collective, communicator mutation, or rank-dependent control path around an
  application call.
- Handle wildcard receive status, ignored statuses, zero counts, and MPI errors.
- Keep state rank-local and thread-safe.
- Document precisely what each byte and timing field measures.
- Add an integration assertion that proves the new wrapper is reached under a real MPI runtime.

## Analysis rules

Each rule must include a deterministic threshold, evidence text, a bounded recommendation, and a
unit test. Phrase recommendations as experiments. A metric correlation alone does not justify an
expected speedup claim.

## Performance results

Record enough metadata to establish equivalence: workload/input checksum, commit, dependencies,
MPI version, node type and count, ranks and placement, thread environment, scheduler job ID, and
per-rank timings. Vary one factor at a time and validate application output within an appropriate
tolerance.

