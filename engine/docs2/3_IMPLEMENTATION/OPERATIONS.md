# Operations & Release Discipline

This document is the canonical operator-facing guide for release proof,
upgrade/rollback discipline, and day-2 runbooks in the open-source Flapjack
repo.

- For deployment shapes and example topologies, see [DEPLOYMENT.md](./DEPLOYMENT.md).
- For environment variables and defaults, see [OPS_CONFIGURATION.md](./OPS_CONFIGURATION.md).
- For shipped/readiness status, see [../FEATURES.md](../FEATURES.md).

## What is already verified

The repo already has executable proof surfaces for the core operational paths:

| Concern | Current proof surface |
|---|---|
| Single-node Linux/systemd deployment | `engine/examples/systemd/README.md` plus the 2026-03-26 live VPS verification recorded in `FEATURES.md` |
| Crash/restart durability for acknowledged writes | `cargo test -p flapjack-server --test crash_durability_test -- acknowledged_batch_write_remains_searchable_after_crash_restart` |
| Restart catch-up before a restarted replica serves traffic | `cargo test -p flapjack --test test_replication -- test_restart_catches_up_before_serving` |
| Snapshot upload/list/restore/scheduled backup/auto-restore | `engine/examples/s3-snapshot/test_snapshots.sh` |
| Admin-key rotation and recovery | `cargo test -p flapjack-server --test admin_key_test` |
| Metrics auth contract | `cargo test -p flapjack-http router::tests::metrics_returns_200_with_admin_key_only -- --exact` |
| Release-to-release data-dir handoff | `engine/tests/upgrade_smoke.sh --old-bin <old> --new-bin <new>` |

The main remaining gap is not whether these flows exist. It is how much
long-duration and release-to-release evidence has been captured around them.

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

What to check first:

- process logs or `journalctl -u flapjack`
- bad `FLAPJACK_ADMIN_KEY` / `FLAPJACK_ENV` combination
- data-dir lock contention (`.process.lock`)
- filesystem permissions on the data dir and `.admin_key`
- bind-address/port conflicts

Useful proofs and references:

- `engine/examples/systemd/README.md`
- `cargo test -p flapjack-server --test env_mode_test`
- `cargo test -p flapjack-server --test admin_key_test`

### Readiness failing

What to check first:

- whether `/health` is green while `/health/ready` is `503`
- whether a visible tenant directory exists but is not searchable
- whether the data dir contains only hidden or infrastructure directories
- warning logs emitted by the readiness handler

Current readiness contract:

- `/health/ready` returns `200 {"ready":true}` when no visible tenant
  directories exist
- otherwise it probes the first visible tenant in sorted order
- tenant-dir discovery failure or probe failure returns the canonical
  `503 {"message":"Service unavailable","status":503}` envelope

Useful proofs:

- `cargo test -p flapjack-http handlers::readiness::tests::ready_returns_canonical_503_when_visible_tenant_discovery_fails -- --exact`
- `cargo test -p flapjack --test test_replication -- test_restart_catches_up_before_serving`

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

For the full exercised path, use the snapshot example:

1. `cd engine/examples/s3-snapshot`
2. `docker compose up -d --build`
3. `./test_snapshots.sh`
4. `docker compose down -v`

Public API surface covered by that harness:

- `POST /1/indexes/:indexName/snapshot`
- `GET /1/indexes/:indexName/snapshots`
- `POST /1/indexes/:indexName/restore`

### Replication lag / peer failure

What to check first:

- whether the affected node is healthy locally
- whether peer addresses still match the real topology
- whether writes are visible on peer nodes through public `/1/...` routes
- whether analytics fan-out reports `cluster.partial`

Useful references:

- `engine/examples/replication/README.md`
- `engine/examples/ha-cluster/README.md`
- `cargo test -p flapjack --test test_replication -- test_restart_catches_up_before_serving`

### Admin-key rotation and recovery

Rotation:

- Use `POST /internal/rotate-admin-key` with the current admin key.
- Confirm the old key no longer accesses `/metrics`.
- Confirm the new key is persisted to `.admin_key`.

Recovery:

- Stop the service if needed.
- Run `flapjack reset-admin-key --data-dir <path>` against the target data dir.
- Restart and re-verify `/metrics` with the new key.

Useful proof:

- `cargo test -p flapjack-server --test admin_key_test`

## Observability contract

Operators can rely on the following today:

| Surface | Current contract |
|---|---|
| `/health` | Basic liveness/capability visibility |
| `/health/ready` | Readiness gate backed by visible-tenant discovery and a real search probe |
| `/metrics` | Prometheus text exposition, admin-key protected |
| `x-request-id` | Always present on responses; echoed when supplied by the client |
| Structured JSON logs | Enabled by `FLAPJACK_LOG_FORMAT=json` |
| Request latency histograms | Exported in `/metrics` as request-duration metrics |
| OpenTelemetry export | Available when the server is built with `--features otel` and `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

Not yet shipped:

- richer release-proof packaging beyond checklist/proof-pack docs

## Documentation rule

When operational behavior changes, update the executable proof surface or this
document first, then update higher-level status docs. Avoid creating second
copies of runbooks in roadmap or status files.
