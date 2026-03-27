# Flapjack — Roadmap

**Last updated:** 2026-03-27
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

Tier 1 launch blockers and Tier 2 production-confidence engineering work are complete. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Launch sign-off is still active, followed by the standing post-launch items:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| LAUNCH-1 | Replacement staging verification | In progress | Run `23662728127` at commit `1992167` is the current gate after failed run `23660898127` exposed the last real regressions. |
| LAUNCH-2 | Launch proof pack and top-level truth-sync | In progress | Finalize verification notes, run deferred validation scripts, and align public docs with the exact staging state that ships. |
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

## Archive

When this list grows stale or too large, move completed items to `roadmap-history/YYYY-QN.md`.
