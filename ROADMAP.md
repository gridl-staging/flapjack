# Flapjack — Roadmap

**Last updated:** 2026-04-16
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

The substantive Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. OSS launch sign-off is now also complete, closed by staging run `23671792399` on commit `745a059`. All workspace crates are at v1.0.0 with OSS policy docs (SECURITY.md, CHANGELOG.md, CONTRIBUTING.md) shipped. OpenTelemetry distributed tracing is available behind the `otel` feature flag. Codebase quality work completed through 2026-04-15: ~601 TODO stubs replaced with real docs, 15 error-leaking HTTP 500 sites fixed via HandlerError migration, 5 high-complexity hotspots decomposed (CC 21-35 → private helpers), debbie sync pipeline wave 2 and wave 3 to staging+prod, dashboard tour coverage closed to 24/24 per-feature MP4 artifacts, a retained 2h HA multi-node soak proof pack recorded with a truthful `warning-findings` / `diverged` outcome, a completed HA boundary/runbook truth-sync pass, an explicit public-doc sync contract plus validator coverage, Apr 8 dashboard/server cleanup, and Apr 15 analytics/test-hygiene follow-through that is now also publicly published. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Launch sign-off is complete. The remaining open items are the standing post-launch tracks:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| ~~CONF-1~~ | ~~Sustained overload follow-up~~ | ✅ Done | Resolved 2026-03-28. Soak scenarios now use `SOAK_WRITE_THRESHOLDS` (relaxed for sustained overload); short baselines keep `WRITE_THRESHOLDS` unchanged. The threshold breach was a classification problem, not an engine defect. See `engine/loadtest/BENCHMARKS.md`. |
| PR-11 | Distributed tracing (OpenTelemetry) | ✅ Done (2026-03-28) | OTLP gRPC export shipped behind the `otel` feature flag with runtime endpoint configuration via `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| — | TODO stub cleanup + HA soak hardening | ✅ Done (2026-03-29) | ~601 stubs → real docs. HA soak test script delivered. Doc-regression tests. This row covers harness delivery, not completed HA soak evidence. Branch: `mar28_pm_3`. |
| — | Codebase quality round 2 | ✅ Done (2026-03-29) | 15 error-leaking sites fixed, HandlerError migration, complexity decomposition, search_query refactor. Branch: `mar29`. |
| — | File size violations | ✅ Done (2026-03-29) | 13 inline test modules extracted to `*_tests.rs`, 2 production code splits (`search_helpers.rs`, `promote.rs`). 0 files over 800-line guardrail. Branch: `mar29_pm_2`. |
| — | Debbie sync to staging (post-cleanup) | ✅ Done (2026-03-29) | Synced, CI green on staging run `23721442173`. Root cause of prior CI failure: 18 TODO stubs on utoipa handlers caused OpenAPI mismatch after scrai strip. |
| — | GitHub release v1.0.0 | ✅ Done (2026-03-29) | All 5 binary targets + Docker image published. Release run `23721789375`. |
| — | Debbie sync pipeline (wave 2) | ✅ Done (2026-03-30) | Full sync to staging+prod. OpenAPI test dedup, experiment handler extraction, soak proof improvements. Branch: `mar30_pm_1`. |
| — | Cognitive complexity reduction | ✅ Done (2026-03-30) | 5 hotspots decomposed: `merge_settings_payload` (CC=35), `validate` (CC=29), `compute_exact_vs_prefix_bucket` (CC=26), `build_results_response` (CC=22), `browse_index` (CC=21). Branch: `mar30_pm_2`. |
| — | Full regression gate + targeted fixes | ✅ Done (2026-03-31) | Full post-merge regression gate run. Real bug fixed: FastEmbed local-embedder test nondeterminism caused by concurrent ONNX/model cache initialization. Proof artifacts captured in `engine/state/`. Current main also includes the follow-through OpenAPI export re-sync after restoring real browse/experiment endpoint summaries. Branch: `mar30_pm_5`. |
| — | Public doc sync surface hardening | ✅ Done (2026-03-31) | `.debbie.toml` now explicitly whitelists the canonical public doc graph. Added `engine/tests/doc_sync_helpers.sh`, `engine/tests/validate_sync_surface.sh`, widened `engine/tests/validate_doc_links.sh`, and scrubbed non-public path references from the synced docs. Branch: `mar30_pm_6`. |
| — | Debbie sync wave 3 | ✅ Done (2026-03-31) | Published the latest post-launch hardening to staging commit `6166055` (CI run `23818440499`) and prod commit `b7841a0` (CI run `23819698304`): HA boundary truth surfaces, public doc sync contract, regression-gate follow-through, and refreshed committed OpenAPI export. |
| — | HA convergence/topology follow-up | ✅ Done (2026-03-31) | Boundary path executed: document-count divergence under sustained rolling restarts is documented as an inherent property of the nginx-routed async-replication example topology. See [`engine/examples/ha-cluster/README.md`](engine/examples/ha-cluster/README.md) and [`engine/loadtest/BENCHMARKS.md`](engine/loadtest/BENCHMARKS.md). |
| — | Nightly CI + sync hygiene | ✅ Done (2026-03-31) | Restored nightly Rust CI parity with the dashboard dist stub, added `CHANGELOG.md` / `CONTRIBUTING.md` / `SECURITY.md` to `.debbie.toml`, and clarified README vector/hybrid platform caveats. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Operator runbook hardening | ✅ Done (2026-03-31) | `engine/docs2/3_IMPLEMENTATION/OPERATIONS.md` now carries proof-backed startup/readiness/replication/admin-key/snapshot failure runbooks plus corrected `reset-admin-key` syntax and tighter proof citations. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Security baseline docs + test coverage | ✅ Done (2026-03-31) | `SECURITY_BASELINE.md` and `4_EVIDENCE/SECURITY_BASELINE_AUDIT.md` now document the shipped HTTP hardening surface (CORS, trusted proxies, per-key rate limiting, body limits) with focused proof references, and `startup_catchup.rs` now warns-and-continues on write-queue timeout outside strict bootstrap. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Security baseline follow-through | ✅ Done (2026-03-31) | Closed the two scoped HTTP-hardening proof gaps by adding invalid-key non-consumption and `FLAPJACK_MAX_BODY_MB` `413` tests, extracting `max_body_mb_from_value`, aligning the security doc/audit, refreshing the committed OpenAPI export, and blocking symlinked sync destinations in helper scripts. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Runbook parity + admin-key truth sync | ✅ Done (2026-03-31) | Standardized `flapjack --data-dir <path> reset-admin-key` across startup output, dashboard auth help, `AUTH_DESIGN.md`, and `OPERATIONS.md`, including quoting for spaced data-dir paths. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Experiment handler merge guardrails | ✅ Done (2026-03-31) | `/2/abtests/{id}/results` now uses the shared resolver seam, targeted results-endpoint proofs cover store-unavailable and numeric/UUID resolution, and experiment OpenAPI docs now declare resolver-driven `500` responses consistently. Published in the completed public lineage; see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| — | Apr 8 targeted cleanup follow-through | ✅ Done (2026-04-08) | Dashboard experiment normalization/results typing now live in `engine/dashboard/src/lib/experiment-normalization.ts`, and a stale server cognitive-complexity suppression was removed. Commits: `44e7fa9c`, `7250f00b`. |
| — | Analytics retention hardening + rollup foundation | ✅ Retention done / 🔶 rollup foundation only (2026-04-15) | Retention cleanup remains partition-based, deterministic, and 90-day-by-default. Durable rollup design, known-answer query contracts, schema/config helpers, and `RollupManifest` are merged. Rollup writer, query-planner rollup reads, HLL serialization choice, and certified-retention gating remain open. Branch: `batman/apr07_pm_2_analytics_phase5_retention`. |
| — | Test hygiene, SDK contract CI, and query safety audit | ✅ Done with deferrals (2026-04-15) | Added SDK contract coverage to public CI, tightened dashboard/browser false-positive patterns, removed stale `_dev/s` test shims in favor of `engine/s/test`, added targeted test coverage, and hardened confirmed filter/experiment error paths. OpenAPI snapshot follow-up was deferred in the session handoff. Branch: `batman/apr07_pm_3_test_hygiene_and_safety_audit`. |
| — | Next public sync wave | ✅ Done (2026-04-15) | Published from canonical dev source `1a0f34c2dfe1b6b973c9359bef49fe7a098d0128` to staging `61f62b0cfa15d5d3926e4c39004f9764265ec40d` (CI `24483181835`) and prod `cdbfc2fa229c18633fb15c58e3c89d6c1bc201d7` (CI `24484835679`). For full lineage, stale-file cleanup, validator boundaries, and residual installer note, see [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). |
| PR-12 | Runbooks & incident response | In progress | Canonical operator docs now exist in `engine/docs2/3_IMPLEMENTATION/OPERATIONS.md`; the HA boundary/runbook truth-sync and Mar 31 proof-backed failure-mode pass are merged, and the remaining work is real-incident refinement plus any future proof-summary cleanup. |
| PR-14 | Search HA ownership/freshness design | Not started / deferred | `apr07_pm_1_ha_search_failover.md` was reviewed and narrowed because automatic write promotion is unsafe without a tested source of truth for index owner, generation/term, replica freshness, restart recovery, and split-brain behavior. Safe forwarding or 503 behavior should follow that design gate. |
| PR-15 | Durable analytics rollup writer/query planner | Not started / design-gated | Foundation types are merged, but no durable rollup writer or rollup-backed analytics query planner exists yet. This must proceed with known-answer query tests and certified coverage checks to avoid double-counting or raw-data loss. |
| PR-13 | Mobile / responsive dashboard | Not started | Desktop-first acceptable for admin tooling. |
| — | OWASP full deep pass | Not started | Required before multi-tenant SaaS, not for OSS launch. |

See [`engine/docs2/FEATURES.md#post-launch-work`](engine/docs2/FEATURES.md#post-launch-work) for details.

## Open-Source Launch Planning

For launch sequencing and current status, use:

- [`engine/docs2/FEATURES.md#recommended-execution-order`](engine/docs2/FEATURES.md#recommended-execution-order)
- [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md)

## Confidence-Completeness Planning

For the next engineering block after the closed launch gate, use:

- [`engine/docs2/FEATURES.md#recommended-execution-order`](engine/docs2/FEATURES.md#recommended-execution-order)
- [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md)
- [`PRIORITIES.md`](PRIORITIES.md)

Detailed working checklists and proof-pack session notes may exist in the private dev repo, but public routing docs should resolve entirely within the synced public tree.

## Archive

When this list grows stale or too large, move completed items to `roadmap-history/YYYY-QN.md`.
