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
`retention:admin` only to identities allowed to manage retention. Grant `integrity:admin` only to
operators responsible for reviewing and closing protected-artifact incidents. Reserve `credentials:revoke` for
a separate security-administrator identity. That administrator can permanently revoke another
credential through the API without a restart; the tenant-scoped reason, actor, and time are retained
for audit, and a revoked credential ID must not be reused. Validity windows use offset-aware ISO-8601
timestamps, and overlapping old and new credentials support a controlled rotation. Removing a
credential from configuration still requires an API restart to take effect.

Retention administrators can also create append-only cluster policy revisions. A new revision
applies to new attempts only, so later edits never silently rewrite an existing attempt's fixed
expiry. An administrator can migrate attempts from one named revision to another only through the
bounded preview/apply workflow, which confirms the exact candidate digest and records each
attempt's before/after assignment with an audit reason. Use a retention value of zero to disable
automatic expiry for new attempts in that cluster.

To route protected-artifact incidents to an operator system, configure
`RANKLENS_INTEGRITY_WEBHOOK_URL` with an HTTPS endpoint and
`RANKLENS_INTEGRITY_WEBHOOK_SECRET` with a secret of at least 32 characters on the worker. Incident
detection and resolution are first committed to a durable outbox, so a paging outage never blocks
the object audit. The worker retries with bounded exponential backoff and signs the exact JSON body
as `HMAC-SHA256(secret, timestamp + "." + body)` in `X-RankLens-Signature`; receivers should also
validate `X-RankLens-Timestamp` and deduplicate `X-RankLens-Event-ID`. Store the secret in the
deployment secret manager, not in Compose or source control. Integrity administrators can list
delivery state through the API and redrive a quarantined notification after correcting the route;
each redrive requires a reason and retains an immutable actor/time audit record.
The scoped notification summary reports backlog age, state counts, maximum attempts, and the last
successful delivery time so operators can attach their own paging objective without scanning rows.

For signing-key rotation, configure a bounded JSON key set in
`RANKLENS_INTEGRITY_WEBHOOK_KEYS_JSON` and select one key with
`RANKLENS_INTEGRITY_WEBHOOK_ACTIVE_KEY_ID`. The sender includes that safe identifier in
`X-RankLens-Key-ID` without exposing key material. Add the new key to the receiver first, deploy the
overlapping key set, switch the active ID, and remove the old key only after the receiver's replay
window closes. The single-secret variable remains supported as a legacy `legacy` key ID; do not
configure both forms together.

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
