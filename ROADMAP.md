# Flapjack — Roadmap

**Last updated:** 2026-03-27
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

The substantive Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. OSS launch sign-off is now also complete, closed by staging run `23671792399` on commit `745a059`. Remaining work is the confidence-completeness block below. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Launch sign-off is still active, followed by the standing post-launch items:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| CONF-1 | Confidence-completeness hardening | In progress | Stage 1 deterministic parity hardening plus Stage 2/7/8 source-of-truth and launch-proof work are now staging-validated, and the OSS launch gate is closed. Remaining confidence-completeness work is the heavier operational block: soak/load interpretation, failure-mode proof, upgrade/rollback guidance, runbooks, and security confidence. |
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
