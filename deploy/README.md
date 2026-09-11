# Enterprise development deployment

This Compose profile exercises the implemented admission foundation: PostgreSQL migrations,
authenticated API, immutable local object adapter, and leased worker. It is a development/pilot
profile, not an HA, mTLS, managed-object-storage, backup, RLS, or fleet-scale certification.

Set two secrets in the invoking environment:

```bash
export RANKLENS_POSTGRES_PASSWORD='replace-with-a-random-secret'
export RANKLENS_MACHINE_TOKENS_JSON='{"replace-with-a-24-character-token":{"tenant_id":"tenant-a","clusters":["cluster-a"]}}'
docker compose -f deploy/compose.yaml up --build
```

The API binds only to loopback on port 8080. Put a site-approved TLS/mTLS reverse proxy in front of
it before an agent connects from another host. Do not put the password/token JSON in this repository
or the Compose file. Production deployments must pin image digests, use a KMS/secrets manager,
replace the local object adapter with a verified storage adapter, configure PostgreSQL backup/HA and
RLS roles, enforce resource limits, and pass the release gates.

Run one local agent pass after creating a mode-0600 token file:

```bash
go run ./agent \
  --capture-dir ./captures/solver-a \
  --spool-dir ./agent-spool \
  --api-url http://127.0.0.1:8080 \
  --token-file ./agent.token \
  --cluster-id cluster-a \
  --attempt-id attempt-1 \
  --producer-id node-agent-1 \
  --max-spool-bytes 1073741824 \
  --once
```

The agent retains sealed segments until the API returns a `DURABLE` receipt and writes receipts
before reclaiming pending files. It rejects token symlinks, bounds summaries and individual
segments, and refuses new segments once the configured pending-spool byte budget is exhausted. Its
current transport is identity-compressed NDJSON over HTTPS; Protobuf/Zstandard, node-local
IPC/shared memory, service-manager packaging, mTLS enrollment, multi-segment large-file draining,
reserved emergency summary capacity, and adaptive sampling remain open implementation work.
