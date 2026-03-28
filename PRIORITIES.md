# Flapjack — Priorities

**Last updated:** 2026-03-28

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

Gate-closing evidence: staging run `23671792399` on commit `745a059` completed `success`. Stage 3 proof pack, Stage 4-6 docs/proof, and soak threshold resolution are all landed.

1. ~~**Overload interpretation / tuning**~~ — ✅ Resolved (2026-03-28). Soak scenarios now use `SOAK_WRITE_THRESHOLDS`; short baselines keep `WRITE_THRESHOLDS` unchanged. See `engine/loadtest/BENCHMARKS.md`.
2. **Runbooks iteration** — refine the new `OPERATIONS.md` runbooks from real incidents instead of only green-path proof surfaces.
3. **Distributed tracing** — OTEL export is still the largest observability gap after the current logs/metrics/readiness baseline.
4. **Security depth** — keep the new `SECURITY_BASELINE.md` honest and scoped while the deeper OWASP-style pass remains deferred.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (OpenTelemetry, runbooks, mobile dashboard, OWASP deep pass).
