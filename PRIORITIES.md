# Flapjack — Priorities

**Last updated:** 2026-03-29

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

All launch blockers closed. v1.0.0 release pipeline triggered. OSS policy docs shipped. OpenTelemetry shipped. TODO stub cleanup and codebase quality round 2 completed. Staging CI fully green. Focus is now post-launch hardening.

### Completed (most recent first)

1. ~~**File size violations**~~ — ✅ Resolved (2026-03-29). All 20 files brought under 800-line guardrail via 13 test module extractions + 2 production code splits. 0 violations remaining.
2. ~~**HA multi-node soak testing**~~ — ✅ Resolved (2026-03-29). HA harness (`engine/_dev/s/manual-tests/ha-soak-test.sh`), structural tests (`engine/tests/test_ha_soak_harness.rs`), shell acceptance tests, and CI integration delivered; actual multi-node soak execution deferred pending Docker availability.
3. ~~**GitHub release v1.0.0**~~ — ✅ Done (2026-03-29). 5 binary targets + Docker image published. Release run `23721789375`, CI run `23721442173`.
4. ~~**Debbie sync to staging**~~ — ✅ Resolved (2026-03-29). Pushed v1.0.0 + OTEL + TODO cleanup + codebase quality fixes + OpenAPI regen. CI green.
5. ~~**Post-merge regression validation**~~ — ✅ Resolved (2026-03-29). Full test suite green: 1546 (flapjack) + 1429 (flapjack-http) + server/ssl/replication crates.
6. ~~**OpenAPI spec CI fix**~~ — ✅ Resolved (2026-03-29). Root cause: 18 TODO stubs on utoipa-annotated handlers caused spec mismatch after scrai strip. Replaced with real doc comments.
7. ~~**Codebase quality cleanup**~~ — ✅ Resolved (2026-03-29). Fixed 15 error-leaking 500 sites, migrated 5 handler files to `HandlerError`, decomposed `execute_search_query`, removed cognitive_complexity suppressions.
8. ~~**TODO stub cleanup**~~ — ✅ Resolved (2026-03-29). Replaced ~601 auto-generated `TODO: Document` stubs with real doc comments. HA soak test script added.
9. ~~**Distributed tracing**~~ — ✅ Resolved (2026-03-28). OTLP gRPC export shipped behind `otel` feature flag.
10. ~~**OSS packaging**~~ — ✅ Resolved (2026-03-28). SECURITY.md, CHANGELOG.md, CONTRIBUTING.md shipped. All crates at 1.0.0.
11. ~~**Overload interpretation / tuning**~~ — ✅ Resolved (2026-03-28). Soak scenarios use `SOAK_WRITE_THRESHOLDS`.

### Active

1. **Runbooks iteration** — refine `OPERATIONS.md` from real failure scenarios.
2. **Security depth** — OWASP deep pass deferred until multi-tenant SaaS, but keep `SECURITY_BASELINE.md` scoped.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (runbooks, mobile dashboard, OWASP deep pass).
