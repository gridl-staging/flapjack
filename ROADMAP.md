# Flapjack — Roadmap

**Last updated:** 2026-03-29
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

The substantive Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. OSS launch sign-off is now also complete, closed by staging run `23671792399` on commit `745a059`. All workspace crates are at v1.0.0 with OSS policy docs (SECURITY.md, CHANGELOG.md, CONTRIBUTING.md) shipped. OpenTelemetry distributed tracing is available behind the `otel` feature flag. Codebase quality work completed on 2026-03-29: ~601 TODO stubs replaced with real docs, 15 error-leaking HTTP 500 sites fixed via HandlerError migration, cognitive_complexity suppressions removed, execute_search_query decomposed. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Launch sign-off is complete. The remaining open items are the standing post-launch tracks:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| ~~CONF-1~~ | ~~Sustained overload follow-up~~ | ✅ Done | Resolved 2026-03-28. Soak scenarios now use `SOAK_WRITE_THRESHOLDS` (relaxed for sustained overload); short baselines keep `WRITE_THRESHOLDS` unchanged. The threshold breach was a classification problem, not an engine defect. See `engine/loadtest/BENCHMARKS.md`. |
| PR-11 | Distributed tracing (OpenTelemetry) | ✅ Done (2026-03-28) | OTLP gRPC export shipped behind the `otel` feature flag with runtime endpoint configuration via `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| — | TODO stub cleanup + HA soak | ✅ Done (2026-03-29) | ~601 stubs → real docs. HA soak test script. Doc-regression tests. Branch: `mar28_pm_3`. |
| — | Codebase quality round 2 | ✅ Done (2026-03-29) | 15 error-leaking sites fixed, HandlerError migration, complexity decomposition, search_query refactor. Branch: `mar29`. |
| — | File size violations | ✅ Done (2026-03-29) | 13 inline test modules extracted to `*_tests.rs`, 2 production code splits (`search_helpers.rs`, `promote.rs`). 0 files over 800-line guardrail. Branch: `mar29_pm_2`. |
| — | Debbie sync to staging (post-cleanup) | ✅ Done (2026-03-29) | Synced, CI green on staging run `23721442173`. Root cause of prior CI failure: 18 TODO stubs on utoipa handlers caused OpenAPI mismatch after scrai strip. |
| — | GitHub release v1.0.0 | ✅ Done (2026-03-29) | All 5 binary targets + Docker image published. Release run `23721789375`. |
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
