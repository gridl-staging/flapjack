# Operations & Release Discipline

This document is the canonical operator-facing guide for release proof,
upgrade/rollback discipline, and day-2 runbooks in the open-source Flapjack
repo.

- For deployment shapes and example topologies, see [DEPLOYMENT.md](./DEPLOYMENT.md).
- For environment variables and defaults, see [OPS_CONFIGURATION.md](./OPS_CONFIGURATION.md).
- For security contracts (production auth floor, admin-only surfaces, request correlation), see [SECURITY_BASELINE.md](./SECURITY_BASELINE.md).
- For shipped/readiness status, see [../FEATURES.md](../FEATURES.md).

## What is already verified

The repo already has executable proof surfaces for the core operational paths:

| Concern | Current proof surface |
|---|---|
| Single-node Linux/systemd deployment | `engine/examples/systemd/README.md` plus the 2026-03-26 live VPS verification recorded in `FEATURES.md` |
| Crash/restart durability for acknowledged writes | `cargo test -p flapjack-server --test crash_durability_test -- acknowledged_batch_write_remains_searchable_after_crash_restart` |
| Restart catch-up before a restarted replica serves traffic | `cargo test -p flapjack --test test_replication -- test_restart_catches_up_before_serving` |
| Snapshot upload/list/restore/scheduled backup/auto-restore | `engine/examples/s3-snapshot/test_snapshots.sh` |
| Startup failure (env, auth, lock, blank key) | `cargo test -p flapjack-server --test env_mode_test` |
| Admin-key rotation and recovery | `cargo test -p flapjack-server --test admin_key_test` |
| Metrics auth contract | `cargo test -p flapjack-http router::tests::metrics_returns_200_with_admin_key_only -- --exact` |
| Release-to-release data-dir handoff | `engine/tests/upgrade_smoke.sh --old-bin <old> --new-bin <new>` |

The main remaining gap is not whether these flows exist. It is how much
long-duration and release-to-release evidence has been captured around them.

Canonical ownership reminder: deployment topology/catalog ownership lives in
[DEPLOYMENT.md](./DEPLOYMENT.md#deployment-surfaces-in-this-repo), and verified
example-path catalog ownership lives in
[OPS_CONFIGURATION.md](./OPS_CONFIGURATION.md#verified-example-paths).

## Release proof pack

Each release proof pack should record the same four things:

1. exact dev commit and exact staging/public commit
2. exact focused validations run
3. staged/public CI result for that commit
4. known acceptable caveats that still remain true

Keep the proof pack concise. Prefer links to the canonical docs and executable
proof surfaces over restating their contents.

## Upgrade smoke

Flapjack does not yet maintain a versioned historical compatibility matrix across
arbitrary old releases. The current bar is a repeatable upgrade smoke against a
known-good prior build on the same data directory.

### Minimum upgrade smoke

1. Stop the current service cleanly.
2. Capture the currently running binary version/commit and back up the data dir.
3. Stage the new binary without changing the data directory.
4. Start the new binary or restart the systemd service.
5. Verify `GET /health` and `GET /health/ready`.
6. Run one known-good search against pre-existing data.
7. Run one write path and wait for its task to publish.
8. Load `/dashboard` and confirm it renders.
9. Record the exact commands, commits, and any caveats.

For the documented Linux/systemd path, use the same deployment contract in
`engine/examples/systemd/README.md`: binary on disk, `EnvironmentFile`,
`systemctl enable --now`, then health/readiness verification.

The repo also ships a reusable focused harness:

```bash
engine/tests/upgrade_smoke.sh --old-bin <old-binary> --new-bin <new-binary>
```

That script exercises:

- pre-existing data dir still loads
- searches still work after upgrade
- writes still work after upgrade
- `/health` and `/health/ready` still succeed
- `/dashboard` still serves HTML on the upgraded binary

## Rollback guidance

Rollback currently means restoring the previous known-good binary and, when
needed, restoring operator-controlled data taken before the upgrade.

### What is safe to assume today

- Forward startup on an existing data directory is exercised by normal restart
  and upgrade-smoke workflows.
- Backward compatibility of an upgraded data directory with an older binary is
  **best-effort**, not a guaranteed contract across arbitrary historical builds.
- Operators should treat a pre-upgrade snapshot or filesystem backup as the
  rollback boundary.

### Recommended rollback procedure

1. Stop the upgraded process.
2. Restore the previous binary or package.
3. If no post-upgrade writes were accepted, restart on the same data dir and
   verify health/readiness.
4. If post-upgrade writes were accepted and rollback is required, restore the
   pre-upgrade snapshot or backup before restarting the older binary.
5. Re-run the same search + write + readiness checks used in upgrade smoke.

## Runbooks

### Startup failure

#### Permission denied on data dir or `.admin_key`

What you will see:
- Startup exits with `ERROR: Failed to create data directory <path>: Permission denied`, or
- `❌ Error: Failed to save admin key: Permission denied`.

What to do:
1. Verify the service user can read/write `FLAPJACK_DATA_DIR` and its parent directory.
2. Fix ownership/mode on the data dir and `.admin_key`, then restart.
3. If this happened after manual key-file edits, restore correct permissions before retry.

Proof surface: `cargo test -p flapjack-server --test env_mode_test`

#### Bind address or port conflict

What you will see:
- Startup exits before the banner; stderr contains an OS bind error (for example `Address already in use` from `TcpListener::bind`).

What to do:
1. Free the conflicting listener or change `FLAPJACK_BIND_ADDR` / `--bind-addr`.
2. Restart and confirm the startup banner prints the resolved bind URL.

Proof surface: `cargo test -p flapjack-server --test env_mode_test`

#### Missing or short production admin key

What you will see:
- `ERROR: FLAPJACK_ADMIN_KEY is required in production mode.`, or
- `ERROR: FLAPJACK_ADMIN_KEY must be at least 16 characters in production.`

What to do:
1. Set `FLAPJACK_ADMIN_KEY` only once (secret store or systemd env file).
2. Ensure length is at least 16 characters.
3. Restart and verify production starts without printing the key value.

Policy contract owner: [SECURITY_BASELINE.md](./SECURITY_BASELINE.md#production-auth-floor).

Proof surface: `cargo test -p flapjack-server --test env_mode_test`

#### `.process.lock` contention

What you will see:
- `ERROR: Data directory already in use: <data_dir>/.process.lock. Use unique --data-dir per instance.`

What to do:
1. Ensure only one process uses a given `--data-dir`.
2. For multiple local instances, give each process a unique data dir.
3. Restart after the conflicting process exits.

Proof surface: `cargo test -p flapjack-server --test env_mode_test`

#### Blank or unreadable `.admin_key`

What you will see:
- `❌ Error: .admin_key file <path> is empty`, or
- `❌ Error: Failed to read .admin_key file <path>: <io error>`, then
- `Run: flapjack --data-dir <path> reset-admin-key`.

What to do:
1. Stop the server.
2. Run `flapjack --data-dir <path> reset-admin-key`.
3. Restart and verify admin-authenticated endpoints with the new key.

Proof surface: `cargo test -p flapjack-server --test env_mode_test` (detection: `blank_admin_key_file_prints_explicit_reset_hint`)

### Readiness failing

#### Readiness returns canonical 503

What you will see:
- `/health` can still return 200 while `/health/ready` returns:
  `{"message":"Service unavailable","status":503}`.
- Logs include either:
  `readiness probe failed to inspect tenant dirs at <path>: <error>`, or
  `readiness probe failed to search tenant <id>: <error>`.

What to do:
1. Check data-dir readability and tenant-directory integrity.
2. If a visible tenant exists, run a direct query against that tenant to confirm searchability.
3. Keep the canonical 503 body in automation checks; do not key off ad-hoc log text.

Proof surface: `cargo test -p flapjack-http handlers::readiness::tests::ready_returns_canonical_503_when_visible_tenant_discovery_fails -- --exact`

#### Readiness returns ready with no visible tenants

What you will see:
- `/health/ready` returns `{"ready":true}` when no visible tenant directories exist.

What to do:
1. Treat this as expected for an empty/new data dir.
2. Only escalate if tenant directories exist but readiness remains 503.

Proof surface: `cargo test -p flapjack-http handlers::readiness::tests::ready_returns_ready_when_no_visible_tenants_exist -- --exact`

### Disk-full / memory-pressure triage

What to check first:

- free disk space for the data dir and snapshot target
- `/metrics` for request latency and oplog progress
- write-path `429` rates under current load
- configured memory bounds (`FLAPJACK_MEMORY_*` when used by the loadtest
  pressure harness)

Current product behavior:

- bounded write-path `429` under overload is acceptable backpressure
- unexpected `4xx` and `5xx` rates should remain low
- request latency histograms are exported in `/metrics`

Useful references:

- `engine/loadtest/BENCHMARKS.md`

### Snapshot restore

#### Restore latest snapshot

What you will see:
- `POST /1/indexes/<index>/restore` (empty body) returns
  `{"status":"restored","key":"snapshots/<index>/...tar.gz","size_bytes":<n>}`.

What to do:
1. Use latest restore for standard rollback to most recent snapshot.
2. Re-run a query immediately after restore to verify expected hit count.

Proof surface: `engine/examples/s3-snapshot/test_snapshots.sh`

#### Restore by explicit snapshot key

What you will see:
- `POST /1/indexes/<index>/restore` with body `{"key":"snapshots/<index>/<file>.tar.gz"}`
  returns `{"status":"restored","key":"snapshots/<index>/<file>.tar.gz","size_bytes":<n>}`.

What to do:
1. Use explicit key when rolling back to a specific backup point.
2. Keep key format under the same index prefix; cross-index keys are rejected.

Proof surface: `engine/examples/s3-snapshot/test_snapshots.sh`

#### Cross-index key rejection

What you will see:
- `POST /1/indexes/<index>/restore` with a key outside `snapshots/<index>/`
  returns `400 {"message":"key must reference a snapshot for the requested index","status":400}`.

What to do:
1. Correct the key to the target index prefix.
2. Retry restore with a key from `GET /1/indexes/<index>/snapshots`.

Proof surface: `engine/examples/s3-snapshot/test_snapshots.sh`

#### Invalid import payload is sanitized

What you will see:
- `POST /1/indexes/<index>/import` with corrupt bytes returns
  `500 {"message":"Internal server error","status":500}`.
- The response does not leak internal tar/gzip parser details.

What to do:
1. Re-export or re-download the snapshot artifact and retry.
2. Confirm existing indexed data remains queryable after the failed import.

Proof surface: `cargo test -p flapjack --test test_snapshot_import_failure_contract`

#### Empty data dir S3 auto-restore at startup

What you will see:
- Startup logs include `Empty data dir detected, attempting S3 auto-restore...`.
- Successful restore logs include `S3 auto-restore: restored <tenant> from <key> (<bytes> bytes)`.

What to do:
1. Treat this as expected bootstrap behavior for empty data dirs with S3 configured.
2. Verify by querying a known document after startup.

Proof surface: `engine/examples/s3-snapshot/test_snapshots.sh`

### Replication lag / peer failure

#### Diagnose node-level replication status

What you will see:
- `curl -s http://<node>/internal/status` returns JSON including:
  `node_id`, `replication_enabled`, `peer_count`, `storage_total_bytes`, `tenant_count`.
- `replication_enabled=false` with `peer_count=0` means standalone mode, not peer failure.

What to do:
1. Confirm the affected node is actually in replication mode (`replication_enabled=true`).
2. If true but stale data persists, proceed to peer reachability and catch-up checks below.

Proof surface: `cargo test -p flapjack-http handlers::internal::tests::status_includes_storage_total_and_tenant_count -- --exact`

#### Peer-status meanings used by catch-up

What you will see:
- Peer health statuses are one of:
  `never_contacted`, `healthy` (<60s since success), `stale` (<300s), `unhealthy` (>=300s), `circuit_open`.

What to do:
1. Treat `never_contacted` and `circuit_open` as immediate bootstrap risk.
2. Investigate network/auth/timeouts before restarting replicas that must catch up.

Proof surface: `cargo test -p flapjack-http handlers::internal::tests::cluster_status_ha_returns_peer_list_with_correct_shape -- --exact`; `cargo test -p flapjack-replication manager::tests::test_peer_statuses_initially_never_contacted -- --exact`

#### Bootstrap refusal when peer is unreachable

What you will see:
- Restarted replica fails to become healthy instead of serving stale data.
- Error output includes peer fetch failures such as
  `Failed to fetch tenants`, `Failed to fetch ops`, or `tripped circuit breakers`.
- Strict timeout path emits:
  `pre-serve catch-up timed out after <n>s; refusing to serve stale data`.

What to do:
1. Restore peer reachability first (address, auth header, network path).
2. Restart the replica only after peer ops/snapshot routes are reachable.

Proof surface: `cargo test -p flapjack --test test_replication -- test_restart_refuses_to_serve_when_peer_is_unreachable`

Known boundary: under sustained rolling restarts with continuous writes, per-node
document counts may diverge in the nginx-routed example topology. Availability
survives restarts (nodes rejoin and serve traffic), but exact document-count
convergence across all nodes is not guaranteed while restart rotation is active.
See [`engine/loadtest/BENCHMARKS.md`](../../loadtest/BENCHMARKS.md) for canonical
evidence from the retained 2h soak proof.

### Admin-key rotation and recovery

Security contract owner (admin-only operational surfaces and immediate key
invalidation): [SECURITY_BASELINE.md](./SECURITY_BASELINE.md#admin-only-operational-surfaces).

#### Rotate admin key online

What you will see:
- `POST /internal/rotate-admin-key` returns:
  `{"key":"fj_admin_<32hex>","message":"Admin key rotated"}`.
- Old key immediately fails admin routes (for example `/metrics`), new key succeeds.
- `.admin_key` is rewritten to the new plaintext key.

What to do:
1. Store the returned key immediately in your secrets system.
2. Update all automation to the new key before removing old credentials.
3. Verify old-key rejection and new-key success on `/metrics`.

Proof surface: `cargo test -p flapjack-server --test admin_key_test`

#### Recover when key is lost

What you will see:
- `flapjack --data-dir <path> reset-admin-key` prints a new `fj_admin_...` key on stdout.

What to do:
1. Stop the service, run reset, and capture the emitted key securely.
2. Restart and re-verify admin-only endpoints with the new key.

Proof surface: `cargo test -p flapjack-server --test admin_key_test`

#### Recovery failure when key store is missing

What you will see:
- `flapjack --data-dir <path> reset-admin-key` exits non-zero with:
  `ERROR: No keys.json found. Start the server first to initialize.`

What to do:
1. Start the server once on that data dir to initialize `keys.json`.
2. Re-run `reset-admin-key` only after `keys.json` exists.

Proof surface: `cargo test -p flapjack-server --test admin_key_test`

## Observability contract

Operators can rely on the following today:

| Surface | Current contract |
|---|---|
| `/health` | Liveness probe semantics are owned by [DEPLOYMENT.md](./DEPLOYMENT.md#health-probes) |
| `/health/ready` | Readiness probe semantics are owned by [DEPLOYMENT.md](./DEPLOYMENT.md#health-probes) |
| `/metrics` | Admin-only surface contract is owned by [SECURITY_BASELINE.md](./SECURITY_BASELINE.md#admin-only-operational-surfaces) |
| `x-request-id` | Correlation/error-consistency contract is owned by [SECURITY_BASELINE.md](./SECURITY_BASELINE.md#request-correlation-and-error-consistency) |
| Structured JSON logs | Enabled by `FLAPJACK_LOG_FORMAT=json` |
| Request latency histograms | Exposed under `/metrics`; admin-surface policy remains in [SECURITY_BASELINE.md](./SECURITY_BASELINE.md#admin-only-operational-surfaces) |
| OpenTelemetry export | Available when the server is built with `--features otel` and `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

Not yet shipped:

- richer release-proof packaging beyond checklist/proof-pack docs

## Documentation rule

When operational behavior changes, update the executable proof surface or this
document first, then update higher-level status docs. Avoid creating second
copies of runbooks in roadmap or status files.
