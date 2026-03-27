# Flapjack — Priorities

**Last updated:** 2026-03-27

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

**Complete confidence completeness after the now-green OSS launch gate.** Gate-closing evidence: staging run `23671792399` on commit `745a059` completed `success`.

1. **Stage 3: soak/load/failure handling** — convert current burst evidence into stronger sustained-behavior and recovery proof.
2. **Stage 4: upgrade/rollback discipline** — define upgrade smoke, rollback semantics, and release proof structure.
3. **Stage 5: runbooks/supportability** — make the most important operator workflows explicit and easier to follow safely.
4. **Stage 6: security confidence** — document the current hardening baseline and the next scoped security pass.

Identity rewrite verification is complete: the staged tree still rewrites README/release/install links to `gridl-staging/flapjack` and `staging.flapjack.foo`, while `.github/workflows/ci.yml` intentionally keeps the three-repo guard unchanged.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (OpenTelemetry, runbooks, mobile dashboard, OWASP deep pass).
