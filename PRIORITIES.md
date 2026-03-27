# Flapjack — Priorities

**Last updated:** 2026-03-27

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

**Ship the open-source launch.** Core launch work is code-complete, but final sign-off is still in progress. Remaining work:

1. **Push and verify the A/B create-status compatibility correction** — review of the live launch fixes confirmed `POST /2/abtests` had drifted away from Algolia compatibility: the handler, smoke test, and OpenAPI all said `201 Created`, but Algolia’s current A/B testing docs specify `200 OK`. The runtime and contract files are now corrected locally; the next staging rerun becomes the real launch gate once this fix is synced and pushed.
2. **Complete deferred validation scripts** — run `engine/tests/validate_doc_links.sh` and `engine/tests/readme_api_smoke.sh` once the replacement staging run is green.
3. **Truth-sync public launch docs and capture proof** — align the top-level status docs, verification notes, and launch evidence with the exact staging state that is about to ship.
4. **Identity rewrite verification** — confirm the debbie staging sync still applies the expected staging/public rewrites after the latest sync and push.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (OpenTelemetry, runbooks, mobile dashboard, OWASP deep pass).
