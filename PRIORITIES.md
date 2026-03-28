# Flapjack — Priorities

**Last updated:** 2026-03-28

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

**Complete the remaining Stage 3 confidence evidence after the now-green OSS launch gate.** Gate-closing evidence: staging run `23671792399` on commit `745a059` completed `success`. Stages 4-6 now have proof-backed docs and a real upgrade smoke locally.

1. **Stage 3: soak/load/failure handling** — capture the multi-hour soak artifact and close the remaining recovery-depth gaps.
2. **Failure-mode depth** — add proof for restart during active writes and crash/restart recovery with a more nontrivial dataset.
3. **Runbooks iteration** — refine the new `OPERATIONS.md` runbooks from real incidents instead of only green-path proof surfaces.
4. **Security depth** — keep the new `SECURITY_BASELINE.md` honest and scoped while the deeper OWASP-style pass remains deferred.

Identity rewrite verification is complete: the staged tree still rewrites README/release/install links to `gridl-staging/flapjack` and `staging.flapjack.foo`, while `.github/workflows/ci.yml` intentionally keeps the three-repo guard unchanged.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (OpenTelemetry, runbooks, mobile dashboard, OWASP deep pass).
