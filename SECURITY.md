# Security policy

RankLens preloads native code into MPI processes and writes telemetry to a user-selected path. Treat
the shared library and Python package with the same trust level as the profiled application.

Do not use a telemetry directory writable by untrusted users. RankLens v0.1 does not transmit data
over a network, invoke a shell, or read application buffers; it records operation metadata only.

Please report suspected vulnerabilities privately to the repository owner before opening a public
issue. Include the RankLens version, MPI implementation, operating system, reproduction steps, and
impact. Avoid attaching sensitive workload data.

