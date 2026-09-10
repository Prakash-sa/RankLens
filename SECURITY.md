# Security policy

RankLens preloads native code into MPI processes and writes telemetry to a user-selected path. Treat
its shared library, Python package, reports, and web build with the same trust level as the profiled
application.

RankLens 1.0 does not transmit telemetry over the network, invoke a shell, or inspect application
buffers. The launcher executes the argument vector supplied after `--` without shell expansion.
The web workspace parses selected JSON locally and has no upload endpoint.

Use a new telemetry directory for every run. Place it on storage that is not writable by untrusted
users, apply cluster-appropriate permissions, and review it before sharing: command arguments,
hostnames, scheduler IDs, user tags, CPU affinity, process resource usage, and trace correlation
identifiers can be sensitive. Standalone reports and ZIP bundles contain the same evidence.

Native preloading is supported only on Linux and macOS. Do not run RankLens with elevated
privileges or preload it into setuid/setgid programs. Install dependencies from the checked-in lock
file and verify release provenance for production use.

Please report suspected vulnerabilities privately to the repository owner before opening a public
issue. Include the RankLens version, MPI implementation, operating system, reproduction steps, and
impact, but do not attach confidential workload data.

