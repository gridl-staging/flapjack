# Highest Priority: Open-Source Launch Readiness

**Updated: 2026-03-27 (launch gate still open; post-parity staging rerun narrowed the remaining blocker to one stale CRUD setup assertion)**

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

Staging run `23660898127` was not a false alarm. It exposed two real launch regressions:

1. **Dashboard full e2e drift** — the positive chat/navigation Playwright specs still assumed the chat UI became ready after only configuring a stub provider, but the dashboard now correctly requires a persisted embedder before rendering the composer.
2. **API key create-status drift** — Rust tests and OpenAPI assertions still expected `201 Created` after the API contract had settled on `200 OK`.

Those regressions were fixed locally, validated, committed in dev as `6160696e`, synced to staging, and pushed there as `1992167`.

Replacement run `23662728127` then cleared the dashboard full e2e failure but exposed one more stale test expectation around `POST /2/abtests`. The first pass aligned the smoke test to the runtime’s `201 Created` response and unblocked the run, but launch review then confirmed that this alignment was backwards: Algolia’s current A/B testing create endpoint returns `200 OK`, so the handler, smoke test, and OpenAPI had all drifted together away from the external contract.

Replacement run `23663387346` then cleared the dashboard full e2e and Rust fast-test failures but exposed one more release-surface gap: the staged/public mirror did not include the committed `engine/docs2/openapi.json` artifact, so `flapjack-http::openapi_export_tests::committed_docs2_openapi_matches_export_output` failed in CI. That sync fix was committed in dev as `db928f89`, synced to staging, and pushed there as `1accd59`.

The pre-review staging run [`23664621314`](https://github.com/gridl-staging/flapjack/actions/runs/23664621314) completed `success` on staging commit `1accd59`, so it remains useful evidence for the dashboard and OpenAPI-sync fixes. It is not the final launch gate because it predates the corrected `/2/abtests` `200 OK` compatibility fix and the later deterministic-parity hardening that also corrected `POST /1/indexes/{indexName}` to `201 Created` and restored missing OpenAPI mutation-path coverage.

That parity bundle did reach staging in rerun [`23670478503`](https://github.com/gridl-staging/flapjack/actions/runs/23670478503) on commit `ed0b64f`. That run cleared `Dashboard full e2e tests`, `Clippy`, `Integration smoke`, the dashboard matrix, and the language/SDK jobs. The only remaining failures were `Rust tests (fast)` and `Rust tests (all)`, both caused by the same stale assertion in `engine/tests/test_sdk_contract_crud.rs::multi_index_get_objects_returns_results_array`: its setup still expected `201 Created` while seeding with `PUT /1/indexes/{index}/{objectID}`, even though the runtime and neighboring CRUD tests correctly return `200 OK`.

## Remaining Before Launch

1. **Push the stale CRUD setup-expectation fix and rerun staging** — sync the `test_sdk_contract_crud.rs` correction for `PUT /1/indexes/{index}/{objectID}` and use that replacement GitHub Actions run as the new launch gate.
2. **Run deferred validation scripts** — execute `engine/tests/validate_doc_links.sh` and `engine/tests/readme_api_smoke.sh` against the final pre-launch tree once staging is green.
3. **Publish the proof pack** — update the active verification notes with the exact green staging run, commit IDs, and validation evidence.
4. **Complete source-of-truth cleanup** — keep the repo-level priority and roadmap docs aligned with the canonical launch record so the public state is no longer split across stale narratives.

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

Debbie identity-rewrite verification is now also complete for this staging tree:

- staging README badges/releases URLs point at `gridl-staging/flapjack`
- staging install commands point at `https://staging.flapjack.foo`
- `.github/workflows/ci.yml` intentionally keeps the three-repo guard unchanged and is correctly excluded from rewrite transforms

## Recently Resolved Launch Blockers

1. ~~**Exact-HEAD wrapper proof**~~ — ✅ Resolved (2026-03-26). Green proof at commit `aa7dd7db61d7e274cdf946ac6dd7d7435c4dcdf4` in `engine/state/test_summary_all_20260326_191752.md`: all 14 sections passed with exit 0. Supersedes the prior red proof at commit `23ac8a9e` (`test_summary_all_20260325_170929.md`). Port contention between Playwright smoke and full e2e runs fixed by pm_12 (port-release hardening in wrapper).
2. ~~**Systemd VPS end-to-end test**~~ — ✅ Resolved (2026-03-26). Live VPS verification confirmed the deployment contract end-to-end: Linux ELF installed at `/opt/flapjack/bin/flapjack`, tracked unit with `EnvironmentFile=/etc/flapjack/env` enabled via `systemctl enable --now flapjack`, successful public `/health` + `/health/ready` probes, clean manual restart, and `Restart=always` recovery after SIGKILL. Canonical artifact: `engine/state/systemd_vps_proof_20260326.md`.
3. ~~**HA cluster dashboard in OSS dashboard**~~ — ✅ Resolved (2026-03-26). New Cluster page at `/cluster` shows live peer health with 5s auto-refresh. Peer status badges (healthy/stale/unhealthy/circuit_open/never_contacted), overview cards, and peer table. Standalone mode shows config guidance. Full TDD test coverage. Branch: `mattman/mar26_am_1_ha_cluster_dashboard`.
4. ~~**README & Show HN polish**~~ — ✅ Resolved (2026-03-26). Show HN draft stale claims fixed (was incorrectly listing "English-only, no vector search, no HA" as limitations — all shipped). Root README feature comparison table verified accurate, architecture tree duplicate removed, Docker Compose quickstart added. engine/README cleaned for public audience ("no customers" line replaced with API stability statement). Branch: `mattman/mar26_pm_3_readme_launch_polish`.
5. ~~**Debbie sync config hardening**~~ — ✅ Resolved (2026-03-26). Replaced dangerous blacklist `.debbie.toml` with proper whitelist config. Was syncing entire repo root with only 14 exclusions — would have leaked 60+ internal files (AI sessions, strategy docs, competitive research). New config uses explicit `sync.files` + targeted `[[sync.dirs]]`. Post-sync hook added for Cargo.toml path dep fixup. Dry-run validated. Branch: `mattman/mar26_pm_2_debbie_config_hardening`.
6. ~~**Post-merge regression validation**~~ — ✅ Resolved (2026-03-26). Full test suite green at HEAD after merging am_1 (HA dashboard) and am_2 (VPS systemd). Cargo check/clippy/fmt clean, 2839+ Rust lib tests, 25 server tests, 542+ vitest tests, nextest 0 leaky, Playwright smoke+full, SDK/CLI all passing. Green wrapper proof at commit `aa7dd7db`. Branch: `mattman/mar26_pm_1_post_merge_regression_validation`.

### Recent Quality Improvements (pm_14)

- **Nextest leak eliminated:** Integration test helpers now properly shut down server processes and clean up file descriptors. `cargo nextest run` reports 0 leaky, 0 failed.
- **Clippy clean:** `cargo clippy --workspace` produces zero warnings.
- **Fmt clean:** `cargo fmt --check` passes with no diffs.

## Post-Launch

- OpenTelemetry distributed tracing (PR-11) — groundwork shipped, OTLP export remaining
- Runbooks & incident response (PR-12) — build from real production incidents
- Mobile/responsive dashboard (PR-13) — low priority, desktop-first acceptable
- OWASP full deep pass — needed before multi-tenant SaaS, not for open-source launch
