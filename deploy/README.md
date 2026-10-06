# Enterprise development deployment

This Compose profile exercises the implemented admission foundation: PostgreSQL migrations,
authenticated API, immutable local object adapter, and leased worker. It is a development/pilot
profile, not an HA, mTLS, managed-object-storage, backup, RLS, or fleet-scale certification.

Set the database secret and a machine credential in the invoking environment. The API configuration
contains only the token digest, while the agent receives the original token:

```bash
export RANKLENS_POSTGRES_PASSWORD='replace-with-a-random-secret'
export RANKLENS_AGENT_TOKEN="$(openssl rand -hex 32)"
export RANKLENS_AGENT_TOKEN_SHA256="$(printf %s "$RANKLENS_AGENT_TOKEN" | shasum -a 256 | cut -d ' ' -f 1)"
export RANKLENS_MACHINE_CREDENTIALS_JSON="{\"agent-primary\":{\"token_sha256\":\"$RANKLENS_AGENT_TOKEN_SHA256\",\"tenant_id\":\"tenant-a\",\"clusters\":[\"cluster-a\"],\"permissions\":[\"segments:write\"],\"expires_at\":\"2027-01-01T00:00:00Z\"}}"
docker compose -f deploy/compose.yaml up --build
```

Use a separate credential with `telemetry:read` for read-only API clients, and grant
`retention:admin` only to identities allowed to view, place, and release retention holds. Validity windows use
offset-aware ISO-8601 timestamps; overlapping old and new credentials support a controlled rotation.
Removing a credential from the configuration revokes it after the API is restarted.

Retention administrators can also create append-only cluster policy revisions. A policy applies to
new attempts only, so later edits never silently rewrite an existing attempt's fixed expiry. Use a
retention value of zero to disable automatic expiry for new attempts in that cluster.

The API binds only to loopback on port 8080. Put a site-approved TLS/mTLS reverse proxy in front of
it before an agent connects from another host. Do not put the password, token, or credential JSON in this repository
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
