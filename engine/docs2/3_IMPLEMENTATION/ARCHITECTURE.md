# Architecture

This document is the maintained high-level map of the shipped Flapjack workspace.
It focuses on current structure and ownership boundaries, not historical design
notes.

For product/readiness status, see [../FEATURES.md](../FEATURES.md).
For frontend-specific design, see the bundled dashboard sources in `engine/dashboard/`.
For deployment/runbook entry points, see [./DEPLOYMENT.md](./DEPLOYMENT.md).

## Workspace layout

Flapjack is organized as a Rust workspace with a bundled dashboard:

- `engine/src/` — core search engine library
- `engine/flapjack-http/` — Axum HTTP layer and request handlers
- `engine/flapjack-server/` — binary entrypoint and CLI/env bootstrap
- `engine/flapjack-replication/` — replication peer coordination
- `engine/flapjack-ssl/` — TLS / ACME support
- `engine/dashboard/` — bundled dashboard frontend
- `engine/examples/` — runnable proof surfaces for specific topologies

## Core library

The core library in `engine/src/` owns the data model and search behavior:

- `index/` — index lifecycle, settings, write queue, persistence, snapshots
- `query/` — parsing, typo tolerance, filters, geo, highlighting
- `analytics/` — events, rollups, query analytics
- `vector/` — vector search and embedder support
- `query_suggestions/` — query-suggestion generation
- `tokenizer/` — custom tokenization pipeline

This crate should remain usable outside the bundled HTTP server where practical.

## HTTP layer

`engine/flapjack-http/` owns:

- router construction
- middleware
- auth and API key enforcement
- health/readiness endpoints
- Algolia-compatible request/response handling

Representative files:

- `flapjack-http/src/server.rs`
- `flapjack-http/src/router.rs`
- `flapjack-http/src/handlers/`
- `flapjack-http/src/auth/`

## Binary entrypoint

`engine/flapjack-server/src/main.rs` is the runtime bootstrap layer:

- CLI parsing
- env-var resolution for server startup
- local-dev instance isolation helpers
- `reset-admin-key` and uninstall commands

The binary should stay thin. Business logic belongs in the core library or HTTP
crate, not in `main.rs`.

## Dashboard

`engine/dashboard/` is the bundled TypeScript/React dashboard frontend served by
the Flapjack binary under `/dashboard`.

Current route inventory is canonical in:

- `engine/dashboard/src/App.tsx`
- `engine/dashboard/tests/E2E_UI_COVERAGE_CHECKLIST.md`

The dashboard is part of the launch surface and is validated by Playwright
coverage in `engine/dashboard/tests/`.

## Operational proof surfaces

Deployment claims should be anchored to runnable examples or validation scripts,
not prose alone:

- `engine/examples/systemd/` — reusable Linux/systemd deployment templates
- `engine/examples/ha-cluster/` — nginx-routed 3-node HA compose proof
- `engine/examples/replication/` — 2-node replication + analytics fan-out proof
- `engine/examples/s3-snapshot/` — MinIO-backed snapshot proof
- `./s/test` — canonical unified test runner

## Design constraints

- Keep Algolia compatibility stable at the wire level.
- Prefer a single source of truth per operational concern.
- Avoid duplicating status/readiness claims outside `engine/docs2/FEATURES.md`.
- Keep deployment guidance grounded in verified examples.
