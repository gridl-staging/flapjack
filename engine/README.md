# Flapjack

Self-hosted, Algolia-compatible search engine. Drop-in replacement for Algolia's REST API — same wire format, same SDK compatibility, same search behavior. Runs as a single binary.

**Status:** Shipped capability, production-readiness state, and post-launch work are maintained in [`docs2/FEATURES.md`](docs2/FEATURES.md).

---

## What it does

- Runs the self-hosted Flapjack engine and Algolia-compatible HTTP layer from a single binary.
- Maintains Algolia-compatible route semantics and REST wire format.
- Works with existing `algoliasearch` SDKs and InstantSearch.js clients without API shape changes.

---

## Documentation

| What you need | Where to look |
|---|---|
| Quickstart and API smoke flow | [`../README.md`](../README.md) |
| Shipped features and readiness state | [`docs2/FEATURES.md`](docs2/FEATURES.md) |
| Runtime configuration (env vars + defaults) | [`docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md`](docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md) |
| Deployment paths (single-node, Docker, systemd) | [`docs2/3_IMPLEMENTATION/DEPLOYMENT.md`](docs2/3_IMPLEMENTATION/DEPLOYMENT.md) |
| Runbooks, upgrade/rollback, observability contract | [`docs2/3_IMPLEMENTATION/OPERATIONS.md`](docs2/3_IMPLEMENTATION/OPERATIONS.md) |
| Public hardening baseline | [`docs2/3_IMPLEMENTATION/SECURITY_BASELINE.md`](docs2/3_IMPLEMENTATION/SECURITY_BASELINE.md) |
| HA cluster proof and test harness | [`examples/ha-cluster/README.md`](examples/ha-cluster/README.md) |
| Architecture overview and crate map | [`docs2/3_IMPLEMENTATION/ARCHITECTURE.md`](docs2/3_IMPLEMENTATION/ARCHITECTURE.md) |
| Test strategy and command reference | [`docs2/1_STRATEGY/TESTING.md`](docs2/1_STRATEGY/TESTING.md) |
| Embedding Flapjack as a Rust library | [`LIB.md`](LIB.md) |

---

## Principles

- **Algolia wire compatibility is the API contract.** Flapjack keeps the REST request/response wire format aligned with Algolia so existing `algoliasearch` SDKs and InstantSearch.js integrations can run unmodified.
- **Self-hosted control is the default.** Data and operational ownership stay with the operator, with deployment paths documented for local, Docker, and systemd environments.
- **Operational safety is a product feature.** Memory limits and guarded write paths are enforced to keep nodes stable under load.
- **Canonical docs over duplicated guidance.** Feature status is owned in [`docs2/FEATURES.md`](docs2/FEATURES.md), and runtime configuration is owned in [`docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md`](docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md).

---

## Quick reference

```bash
# Build
cargo build -p flapjack-server

# Canonical test runner
./s/test                 # unit + integ + server
./s/test --unit          # Rust lib tests
./s/test --integ         # Rust integration tests
./s/test --server        # flapjack-server tests
./s/test --dashboard     # dashboard unit + smoke browser tests
./s/test --dashboard-full # dashboard unit + smoke + full browser suite
./s/test --all           # full non-Algolia suite

# Run server
cargo run -p flapjack-server

# Dashboard dev
cd dashboard && npm run dev

# Direct dashboard tests
cd dashboard && npm run test:unit:run
cd dashboard && npm run test:e2e-ui:smoke
cd dashboard && npm run test:e2e-ui:full
```

---

## Multi-Instance Development

For parallel local development, run each process with an isolated data directory.
Flapjack enforces this with a startup lock (`{data_dir}/.process.lock`).

```bash
# Derived isolated data dir + deterministic port:
flapjack --instance branch_a --no-auth

# Derived isolated data dir + OS-assigned port:
flapjack --instance branch_b --auto-port --no-auth

# Fully explicit:
flapjack --data-dir /tmp/fj/agent_a --bind-addr 127.0.0.1:18110 --no-auth
flapjack --data-dir /tmp/fj/agent_b --bind-addr 127.0.0.1:18111 --no-auth

# Agent helper scripts (tracked PID/log + explicit instance identity):
_dev/s/start-multi-instance.sh agent_a --auto-port --no-auth
_dev/s/start-multi-instance.sh agent_b --auto-port --no-auth
_dev/s/stop-multi-instance.sh agent_a
_dev/s/stop-multi-instance.sh agent_b
```

Never share the same `--data-dir` across concurrent processes.
`--auto-port` overrides env bind settings (`FLAPJACK_BIND_ADDR` / `FLAPJACK_PORT`) and only conflicts with explicit `--bind-addr` or `--port`.
All `--no-auth` examples above are for loopback-only local development on a trusted machine. Do not reuse them on shared networks, in port-forwarded environments, or on any host reachable by other users.

For parallel local branch development, set per-clone test ports in repo root:

```bash
cp flapjack.local.conf.example flapjack.local.conf
# then edit FJ_BACKEND_PORT / FJ_DASHBOARD_PORT per clone
```

Dashboard Playwright/Vite config and `_dev` test runners read this file, so each clone can run its own isolated backend + dashboard test stack.
