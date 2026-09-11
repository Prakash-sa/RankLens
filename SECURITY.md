# Security policy

RankLens preloads native code into MPI processes and writes telemetry to a user-selected path. Treat
its shared library, Python package, reports, and web build with the same trust level as the profiled
application.

The RankLens 1.0 offline workflow does not transmit telemetry over the network, invoke a shell, or
inspect application buffers. The launcher executes the argument vector supplied after `--` without
shell expansion. The browser workspace parses selected JSON locally and has no upload endpoint.

The opt-in enterprise package and Go agent add authenticated network ingestion. Tenant scope comes
from the configured machine principal, not an upload field. Non-loopback agent endpoints require
HTTPS; tokens must be stored in files inaccessible to group/other users. Deploy the API behind a
site-approved TLS/mTLS proxy, run reviewed Alembic migrations, keep object/catalog storage private,
and set explicit segment/spool quotas. The current local object adapter is for development and
disconnected pilots; production storage adapters require their own conditional-write, encryption,
backup, and recovery validation.

Use a new telemetry directory for every run. Place it on storage that is not writable by untrusted
users, apply cluster-appropriate permissions, and review it before sharing: command arguments,
hostnames, scheduler IDs, user tags, CPU affinity, process resource usage, and trace correlation
identifiers can be sensitive. Standalone reports and ZIP bundles contain the same evidence.

Native preloading is supported only on Linux and macOS. Do not run RankLens with elevated
privileges or preload it into setuid/setgid programs. Resolve dependencies under a reviewed,
profile-specific constraints/lock file and verify release provenance for production use.

Please report suspected vulnerabilities privately to the repository owner before opening a public
issue. Include the RankLens version, MPI implementation, operating system, reproduction steps, and
impact, but do not attach confidential workload data.
