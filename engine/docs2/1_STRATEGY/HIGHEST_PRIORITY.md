# Highest Priority: Open-Source Launch Readiness

**Updated: 2026-03-31 (v1.0.0 released; OSS launch gate closed; latest dev-main hardening merged; post-launch hardening phase)**

## Mission

Ship Flapjack as a polished, delightful open-source search engine. First impressions matter — a developer who cannot get from download to working search in 5 minutes closes the tab.

## Core Principles

- **Algolia parity is the floor, not the ceiling.** Wire format matches exactly. Differentiation comes on top.
- **Attract everyone.** Not just Algolia refugees — Meilisearch and Typesense users too.
- **Single maintainer.** Keep the codebase clean and simple. No unnecessary complexity.
- **Move fast.** The API is stable and Algolia-compatible, but correctness wins over backwards compatibility when they conflict.

## Launch Status

Full checklist with per-item evidence lives in [`engine/docs2/FEATURES.md`](../FEATURES.md) (the canonical status ledger). This document tracks the final launch gate and the few remaining sign-off tasks.

## Current Gate

**✅ Gate closed.** Gate-closing CI run `23671792399` on commit `745a059` completed `success` — all Rust tests, dashboard full e2e, Clippy, integration smoke, and cross-language SDK matrix passed. v1.0.0 released (run `23721789375`) with 5 binary targets + Docker image.

The gate-closing process resolved several regressions across 6 staging CI iterations: dashboard e2e chat-UI readiness contracts, Algolia API status code parity (`/2/abtests` → `200 OK`, `POST /1/indexes/{indexName}` → `201 Created`), OpenAPI spec sync with debbie’s scrai-strip hook, crash-durability test transport error handling, and experiment schema tightening. Since then, current dev `main` also merged the public-doc sync-surface hardening pass plus a focused post-merge regression-gate follow-through (FastEmbed test nondeterminism fix and committed OpenAPI export re-sync).

## Next Up After Launch Sign-Off

1. **Debbie sync wave 3** — the latest dev-main hardening is not in staging/prod yet. The next sync should carry the tour closure truth-sync, HA proof retention/truth-sync, explicit public-doc sync contract + validators, regression-gate follow-through, and refreshed committed OpenAPI export together.
2. **HA convergence/topology follow-up** — the Mar 30 soak proof retained the right evidence and wording, but the nginx-routed example topology still ends in `diverged` final document counts after restart rotation. Decide whether to harden the topology, add write-retry guidance, or document that boundary permanently.
3. **Stage 4/5/6 follow-through** — the docs/proof surfaces now exist (`engine/tests/upgrade_smoke.sh`, `engine/docs2/3_IMPLEMENTATION/OPERATIONS.md`, `engine/docs2/3_IMPLEMENTATION/SECURITY_BASELINE.md`), but they should keep being refined from real incidents and future release cycles.
4. **Post-launch hardening** — incident-response maturity and the deeper OWASP-style pass remain the main longer-range gaps. OpenTelemetry shipped (PR-11).

## Deterministic Parity Progress

The Stage 1 deterministic parity foundation is now green locally:

- canonical high-risk mutation inventory lives in `engine/flapjack-http/src/mutation_parity.rs`
- behavior-level parity checks live in `engine/tests/test_mutation_parity.rs`
- spec-level parity checks now live in `flapjack-http::openapi::tests::high_risk_mutation_openapi_contracts_match_shared_matrix`
- committed OpenAPI sync is re-verified by `openapi_export_tests::committed_docs2_openapi_matches_export_output`

That work already paid off by catching and fixing additional local drift that staging had not yet validated:

- `POST /1/indexes/{indexName}` was returning `200` instead of Algolia’s documented `201`
- `/1/indexes/{indexName}` auto-ID save was not exported in OpenAPI
- `/1/indexes/{indexName}/{objectID}/partial` was not exported in OpenAPI
- `/2/abtests/{id}/conclude` was documented with a weaker response schema than the runtime guarantees

Debbie identity-rewrite verification is now also complete for the public sync targets:

- README badges/releases URLs and install commands now resolve to the target public repo/host
- `.github/workflows/ci.yml` and `nightly.yml` are excluded from rewrite transforms (their check-repo guards use literal repo names for staging and prod only — the private dev repo is intentionally excluded to avoid burning paid Actions minutes)

## Stage 4-6 Progress

The operator-facing Stage 4-6 surfaces are now materially stronger locally:

- `engine/tests/upgrade_smoke.sh` now proves a data directory written by the gate-closing staging commit `745a059` can be opened by the current binary, with health/readiness/search/write/dashboard all re-verified after upgrade.
- `engine/docs2/3_IMPLEMENTATION/OPERATIONS.md` is now the canonical operator doc for upgrade smoke, rollback semantics, runbooks, and observability guarantees.
- `engine/docs2/3_IMPLEMENTATION/SECURITY_BASELINE.md` now captures the scoped public hardening baseline, verified auth/admin/restrictSources proofs, and the explicit boundary to the still-deferred deeper OWASP pass.

## Stage 3 Progress

The sustained-behavior proof gap is no longer theoretical:

- `engine/loadtest/soak_proof.sh` now owns repeatable 2h soak capture with gzipped k6 JSON artifacts, periodic RSS/heap sampling, restart-on-same-data-dir checks, and stable post-soak/post-restart count comparisons.
- `engine/flapjack-server/tests/restart_during_writes_test.rs` now proves acknowledged writes survive a restart while traffic is still active.
- `engine/flapjack-server/tests/crash_durability_test.rs` now includes a nontrivial acknowledged-dataset crash/restart proof in addition to the earlier focused case.
- the 2026-03-28 2h mixed/write soak artifacts are now recorded in `engine/loadtest/BENCHMARKS.md`.

## Recently Resolved Launch Blockers

1. ~~**Exact-HEAD wrapper proof**~~ — ✅ Resolved (2026-03-26). Green proof completed at commit `aa7dd7db61d7e274cdf946ac6dd7d7435c4dcdf4`, with all 14 wrapper sections passed and the prior red proof at commit `23ac8a9e` superseded. Port contention between Playwright smoke and full e2e runs was fixed by pm_12 (port-release hardening in wrapper).
2. ~~**Systemd VPS end-to-end test**~~ — ✅ Resolved (2026-03-26). Live VPS verification confirmed the deployment contract end-to-end: Linux ELF installed at `/opt/flapjack/bin/flapjack`, tracked unit with `EnvironmentFile=/etc/flapjack/env` enabled via `systemctl enable --now flapjack`, successful public `/health` + `/health/ready` probes, clean manual restart, and `Restart=always` recovery after SIGKILL.
3. ~~**HA cluster dashboard in OSS dashboard**~~ — ✅ Resolved (2026-03-26). New Cluster page at `/cluster` shows live peer health with 5s auto-refresh. Peer status badges (healthy/stale/unhealthy/circuit_open/never_contacted), overview cards, and peer table. Standalone mode shows config guidance. Full TDD test coverage. Branch: `mattman/mar26_am_1_ha_cluster_dashboard`.
4. ~~**README & Show HN polish**~~ — ✅ Resolved (2026-03-26). Show HN draft stale claims fixed (was incorrectly listing "English-only, no vector search, no HA" as limitations — all shipped). Root README feature comparison table verified accurate, architecture tree duplicate removed, Docker Compose quickstart added. engine/README cleaned for public audience ("no customers" line replaced with API stability statement). Branch: `mattman/mar26_pm_3_readme_launch_polish`.
5. ~~**Debbie sync config hardening**~~ — ✅ Resolved (2026-03-26). Replaced dangerous blacklist `.debbie.toml` with proper whitelist config. Was syncing entire repo root with only 14 exclusions — would have leaked 60+ internal files (AI sessions, strategy docs, competitive research). New config uses explicit `sync.files` + targeted `[[sync.dirs]]`. Post-sync hook added for Cargo.toml path dep fixup. Dry-run validated. Branch: `mattman/mar26_pm_2_debbie_config_hardening`.
6. ~~**Post-merge regression validation**~~ — ✅ Resolved (2026-03-26). Full test suite green at HEAD after merging am_1 (HA dashboard) and am_2 (VPS systemd). Cargo check/clippy/fmt clean, 2839+ Rust lib tests, 25 server tests, 542+ vitest tests, nextest 0 leaky, Playwright smoke+full, SDK/CLI all passing. Green wrapper proof at commit `aa7dd7db`. Branch: `mattman/mar26_pm_1_post_merge_regression_validation`.

### Recent Quality Improvements (pm_14)

- **Nextest leak eliminated:** Integration test helpers now properly shut down server processes and clean up file descriptors. `cargo nextest run` reports 0 leaky, 0 failed.
- **Clippy clean:** `cargo clippy --workspace` produces zero warnings.
- **Fmt clean:** `cargo fmt --check` passes with no diffs.

## Post-Launch

- OpenTelemetry distributed tracing (PR-11) — ✅ Done (2026-03-28). OTLP gRPC export shipped behind `otel` feature flag.
- Runbooks & incident response (PR-12) — build from real production incidents
- Mobile/responsive dashboard (PR-13) — low priority, desktop-first acceptable
- OWASP full deep pass — needed before multi-tenant SaaS, not for open-source launch
