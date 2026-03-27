# Flapjack — Priorities

**Last updated:** 2026-03-27

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

**Ship the open-source launch.** Core launch work is code-complete, but final sign-off is still in progress. Remaining work:

1. **Push and verify the corrected mutation-parity bundle** — the launch blocker is now the full locally-green parity bundle, not just the original `/2/abtests` fix. In addition to restoring `POST /2/abtests` to Algolia’s `200 OK` contract, Stage 1 hardening also corrected `POST /1/indexes/{indexName}` to `201 Created`, tightened the conclude-A/B-test response schema, and restored missing OpenAPI registrations for auto-ID save and partial update. The next staging rerun needs to validate that whole set together.
   Update: staging rerun `23670478503` validated that bundle almost completely. The only remaining blocker is one stale CRUD setup assertion in `engine/tests/test_sdk_contract_crud.rs::multi_index_get_objects_returns_results_array`, now fixed locally and pending the next staging rerun.
2. **Complete deferred validation scripts on the final staged tree** — `engine/tests/validate_doc_links.sh` and `engine/tests/readme_api_smoke.sh` are green in dev, and staged-tree doc-link validation is green on the current staging checkout. They still need one final pass on the exact parity-fixed tree that will ship.
3. **Truth-sync public launch docs and capture proof** — align the top-level status docs, verification notes, and launch evidence with the exact staging state that is about to ship.

Identity rewrite verification is complete: the staged tree still rewrites README/release/install links to `gridl-staging/flapjack` and `staging.flapjack.foo`, while `.github/workflows/ci.yml` intentionally keeps the three-repo guard unchanged.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (OpenTelemetry, runbooks, mobile dashboard, OWASP deep pass).
