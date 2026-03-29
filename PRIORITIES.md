# Flapjack — Priorities

**Last updated:** 2026-03-29

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

All launch blockers closed. v1.0.0 cut. OSS policy docs shipped. OpenTelemetry shipped. TODO stub cleanup and codebase quality round 2 completed. Focus is now post-launch hardening and release pipeline.

### Completed (most recent first)

1. ~~**Codebase quality cleanup**~~ — ✅ Resolved (2026-03-29). Fixed 15 error-leaking 500 sites, migrated 5 handler files to `HandlerError`, decomposed `execute_search_query`, removed cognitive_complexity suppressions.
2. ~~**TODO stub cleanup**~~ — ✅ Resolved (2026-03-29). Replaced ~601 auto-generated `TODO: Document` stubs with real doc comments. HA soak test script added.
3. ~~**Distributed tracing**~~ — ✅ Resolved (2026-03-28). OTLP gRPC export shipped behind `otel` feature flag.
4. ~~**OSS packaging**~~ — ✅ Resolved (2026-03-28). SECURITY.md, CHANGELOG.md, CONTRIBUTING.md shipped. All crates at 1.0.0.
5. ~~**Overload interpretation / tuning**~~ — ✅ Resolved (2026-03-28). Soak scenarios use `SOAK_WRITE_THRESHOLDS`.

### Active

1. **Debbie sync to staging** — push v1.0.0 + new policy docs + TODO cleanup + codebase quality fixes to staging, verify CI green, then sync to prod.
2. **Post-merge regression validation** — full test suite run on merged HEAD to confirm no behavioral regressions from the pm_3 + mar29 merges.
3. **HA multi-node soak testing** — single-node soak is proven; multi-node cluster soak is the remaining confidence gap. HA soak script now exists at `engine/_dev/s/manual-tests/ha-soak-test.sh`.
4. **Cut GitHub release v1.0.0** — tagged release with CHANGELOG content, 5-target binary matrix + Docker image.
5. **Runbooks iteration** — refine `OPERATIONS.md` from real failure scenarios.
6. **Security depth** — OWASP deep pass deferred until multi-tenant SaaS, but keep `SECURITY_BASELINE.md` scoped.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (runbooks, mobile dashboard, OWASP deep pass).
