# Flapjack — Roadmap

**Last updated:** 2026-03-27
**Status ledger policy:** This file is a routing page only. Canonical product status is maintained in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md).

## Implemented

All Tier 1 launch blockers and Tier 2 production-confidence items are complete. See canonical details:

- Shipped feature status: [`engine/docs2/FEATURES.md#shipped-feature-status`](engine/docs2/FEATURES.md#shipped-feature-status)
- Production-readiness state: [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)
- Completed-work archive: [`engine/docs2/FEATURES.md#completed-work-archive`](engine/docs2/FEATURES.md#completed-work-archive)

## Open / Not Yet Implemented

Only post-launch (Tier 3) items remain:

| # | Work Item | Status | Details |
|---|-----------|--------|---------|
| PR-11 | Distributed tracing (OpenTelemetry) | Not started | OTLP export behind `otel` feature flag. Structured logging groundwork shipped. |
| PR-12 | Runbooks & incident response | Not started | Build from real production incidents. |
| PR-13 | Mobile / responsive dashboard | Not started | Desktop-first acceptable for admin tooling. |
| — | OWASP full deep pass | Not started | Required before multi-tenant SaaS, not for OSS launch. |

See [`engine/docs2/FEATURES.md#post-launch-work`](engine/docs2/FEATURES.md#post-launch-work) for details.

## Open-Source Launch Planning

For open-source launch sequencing, use:

- [`engine/docs2/FEATURES.md#recommended-execution-order`](engine/docs2/FEATURES.md#recommended-execution-order)
- [`engine/docs2/FEATURES.md#current-production-readiness-state`](engine/docs2/FEATURES.md#current-production-readiness-state)

## Archive

When this list grows stale or too large, move completed items to `roadmap-history/YYYY-QN.md`.
