# Flapjack — Roadmap

**Last updated:** 2026-03-31
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

The substantive Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. OSS launch sign-off is now also complete, closed by staging run `23671792399` on commit `745a059`. All workspace crates are at v1.0.0 with OSS policy docs (SECURITY.md, CHANGELOG.md, CONTRIBUTING.md) shipped. OpenTelemetry distributed tracing is available behind the `otel` feature flag. Codebase quality work completed through 2026-03-31: ~601 TODO stubs replaced with real docs, 15 error-leaking HTTP 500 sites fixed via HandlerError migration, 5 high-complexity hotspots decomposed (CC 21-35 → private helpers), debbie sync pipeline wave 2 to staging+prod, dashboard tour coverage closed to 24/24 per-feature MP4 artifacts, a retained 2h HA multi-node soak proof pack recorded with a truthful `warning-findings` / `diverged` outcome, an explicit public-doc sync contract plus validator coverage, and a post-merge regression-gate follow-through that left the committed OpenAPI export green at current `main`. See canonical details:

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
| — | Debbie sync wave 3 | In progress | The next staging/prod sync needs to carry the latest post-launch hardening work that merged after wave 2: tour closure truth-sync, HA proof retention/truth-sync, public doc sync surface hardening, regression-gate follow-through, and the refreshed committed OpenAPI export. |
| — | HA convergence/topology follow-up | In progress | The Mar 30 proof pack now exists at `engine/loadtest/results/20260330T211227Z-ha-soak/`. Restart survivability was proven across 39 restart rotations, but final document counts diverged in the nginx-routed example topology. Decide whether to harden the example topology, add write-retry guidance, or keep the limitation as a documented boundary. |
| PR-12 | Runbooks & incident response | In progress | Canonical operator docs now exist in `engine/docs2/3_IMPLEMENTATION/OPERATIONS.md`; continue refining from real incidents. |
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
