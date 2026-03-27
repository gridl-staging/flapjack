# Flapjack — Roadmap

**Last updated:** 2026-03-27
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

The substantive Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. Final launch sign-off and confidence-completeness hardening are tracked separately below. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Launch sign-off is still active, followed by the standing post-launch items:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| LAUNCH-1 | Replacement staging verification | In progress | Staging rerun `23671047087` cleared the stale CRUD setup assertion from `23670478503` and again passed dashboard, OpenAPI-sync, Clippy, integration smoke, and the cross-language/sdk matrix. The only remaining blockers are one crash-durability helper retry issue and one stale OpenAPI typed-schema expectation, both now fixed locally and pending the next rerun. |
| LAUNCH-2 | Launch proof pack and top-level truth-sync | In progress | Keep the proof notes and routing docs aligned with the real gate: latest rerun `23671047087` failed only on the crash-durability helper issue and stale OpenAPI typed-schema expectation above, and the next rerun needs to be captured as the final launch proof. |
| CONF-1 | Confidence-completeness hardening | In progress | Stage 1 deterministic parity hardening and Stage 2 public-source-of-truth checks are now green locally. Remaining confidence-completeness work is the heavier operational block: soak/load interpretation, failure-mode proof, upgrade/rollback guidance, runbooks, and security confidence. |
| PR-11 | Distributed tracing (OpenTelemetry) | Not started | OTLP export behind `otel` feature flag. Structured logging groundwork shipped. |
| PR-12 | Runbooks & incident response | Not started | Build from real production incidents. |
| PR-13 | Mobile / responsive dashboard | Not started | Desktop-first acceptable for admin tooling. |
| — | OWASP full deep pass | Not started | Required before multi-tenant SaaS, not for OSS launch. |

See [`engine/docs2/FEATURES.md#post-launch-work`](engine/docs2/FEATURES.md#post-launch-work) for details.

## Open-Source Launch Planning

For launch sequencing and current status, use:

- [`engine/docs2/FEATURES.md#recommended-execution-order`](engine/docs2/FEATURES.md#recommended-execution-order)
- [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md)

## Confidence-Completeness Planning

For the next engineering block after the active launch gate, use:

- [`engine/docs2/FEATURES.md#recommended-execution-order`](engine/docs2/FEATURES.md#recommended-execution-order)
- [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md)
- [`PRIORITIES.md`](PRIORITIES.md)

Detailed working checklists and proof-pack session notes may exist in the private dev repo, but public routing docs should resolve entirely within the synced public tree.

## Archive

When this list grows stale or too large, move completed items to `roadmap-history/YYYY-QN.md`.
