# Deployment & Infrastructure

This document is the maintained deployment entry point for the open-source
Flapjack repo.

For authoritative env-var names/defaults, see [OPS_CONFIGURATION.md](./OPS_CONFIGURATION.md).
For upgrade/rollback and operator runbooks, see [OPERATIONS.md](./OPERATIONS.md).
For the public hardening baseline, see [SECURITY_BASELINE.md](./SECURITY_BASELINE.md).
For launch-blocker status, see [../1_STRATEGY/HIGHEST_PRIORITY.md](../1_STRATEGY/HIGHEST_PRIORITY.md).
For shipped/readiness status, see [../FEATURES.md](../FEATURES.md).

## Deployment surfaces in this repo

The repo currently maintains four deployment-oriented proof surfaces:

1. `engine/examples/systemd/`
2. `engine/examples/ha-cluster/`
3. `engine/examples/replication/`
4. `engine/examples/s3-snapshot/`

Each one should be treated as a source-backed deployment guide, not as optional
reference prose.

## Recommended launch path

For open-source launch, the clearest operator path is:

1. install the binary
2. run a single-node instance
3. verify `/health` and `/health/ready`
4. move to Linux/systemd for long-running service management

The reusable Linux/systemd templates live in:

- `engine/examples/systemd/flapjack.service`
- `engine/examples/systemd/env.example`
- `engine/examples/systemd/README.md`

Important status note: this documented Linux/systemd path was live-verified on
2026-03-26. The templates and README remain the canonical single-node
deployment surface for production-style hosts.

## Verified example topologies

### Linux/systemd templates

Purpose:

- production-style single-node service management
- dedicated service account
- env-file based configuration
- health and readiness probe guidance

Entry point:

- `engine/examples/systemd/README.md`

What is and is not proven:

- repo-local templates and docs exist
- service layout and env-file pattern are documented
- live Linux/VPS end-to-end validation completed on 2026-03-26
- this does not by itself create a long-term historical upgrade matrix

### 3-node HA cluster

Purpose:

- nginx-routed availability for a single-node outage
- replication visibility across peers
- startup catch-up before serving
- analytics fan-out/merge

Entry points:

- `engine/examples/ha-cluster/README.md`
- `engine/examples/ha-cluster/test_ha.sh`

### 2-node replication + analytics fan-out

Purpose:

- replication across public `/1/...` routes
- analytics fan-out across public `/2/...` routes

Entry points:

- `engine/examples/replication/README.md`
- `engine/examples/replication/test_replication.sh`

### S3 snapshots

Purpose:

- snapshot upload/list/restore
- scheduled backups
- empty-dir auto-restore on startup

Entry points:

- `engine/examples/s3-snapshot/README.md`
- `engine/examples/s3-snapshot/test_snapshots.sh`

## Health probes

Operators should use:

- `/health` for basic process liveness/capability visibility
- `/health/ready` for readiness checks before routing traffic

Launch-facing docs should not advertise a deployment path as complete unless
these probes are verified in that topology.

## Secrets and host-specific configuration

- Keep shared defaults in tracked templates where safe.
- Keep host-specific secrets out of git and out of prose examples.
- For systemd hosts, prefer an env file such as `/etc/flapjack/env` derived from
  `engine/examples/systemd/env.example`.

## Documentation rule

When deployment behavior changes, update the corresponding example README or
proof script first, then update [OPERATIONS.md](./OPERATIONS.md) if the operator
workflow changed, and only then update higher-level status docs. Avoid
duplicating detailed deployment instructions across multiple docs.
