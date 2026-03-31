# Flapjack — Priorities

**Last updated:** 2026-03-31

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Current Priority

All launch blockers closed. v1.0.0 released. Debbie sync pipeline wave 2 complete (staging+prod). Cognitive complexity reduction is done across 5 hotspots. Tour walkthrough coverage is now 24/24 with the former vector/chat blockers closed. The HA multi-node soak proof pack is now retained in-repo with a truthful `warning-findings` / `diverged` classification for the nginx-routed 3-node topology. Since the latest merge wave, the dev repo also has an explicit public-doc sync contract plus validator coverage, and the post-merge regression gate follow-through fixed FastEmbed test nondeterminism while leaving the committed OpenAPI export green. Focus is now the next public sync wave, HA convergence follow-up, and operator/runbook refinement.

### Completed (most recent first)

1. ~~**Tour walkthrough closure + tour typecheck unblock**~~ — ✅ Resolved (2026-03-30). Closed the former vector/chat blockers (specs 05/06), archived MP4s for all 24/24 per-feature tours, default-enabled vector/local embedding support in the build, wired local AI-provider env bridging, and fixed `npx tsc --noEmit -p tsconfig.tour.json` by adding local Node types plus aligning tour typings with the real `VideoTour` API.
2. ~~**HA multi-node soak execution + truth sync**~~ — ✅ Executed (2026-03-30). The 2h 3-node soak proof pack is now retained at `engine/loadtest/results/20260330T211227Z-ha-soak/`. Final classification was `warning-findings` with `diverged` convergence after 39 restart rotations in the nginx-routed example topology. See [`engine/loadtest/BENCHMARKS.md`](engine/loadtest/BENCHMARKS.md).
3. ~~**Cognitive complexity reduction**~~ — ✅ Resolved (2026-03-30). Decomposed 5 high-CC hotspots: `merge_settings_payload` (CC=35), `validate` (CC=29), `compute_exact_vs_prefix_bucket` (CC=26), `build_results_response` (CC=22), `browse_index` (CC=21). Added settings characterization tests.
4. ~~**Debbie sync pipeline wave 2**~~ — ✅ Resolved (2026-03-30). Full sync to staging+prod. OpenAPI test dedup, experiment handler extraction, soak proof improvements.
5. ~~**Full regression gate + targeted fixes**~~ — ✅ Resolved (2026-03-31). Ran the full post-merge regression gate, fixed FastEmbed local-embedder test nondeterminism by serializing the affected ONNX/model-cache tests, captured proof artifacts in `engine/state/`, and re-synced the committed OpenAPI export after restoring real browse/experiment endpoint summaries.
6. ~~**Public doc sync surface hardening**~~ — ✅ Resolved (2026-03-31). `.debbie.toml` now explicitly syncs the canonical public doc graph, `engine/tests/validate_sync_surface.sh` and shared doc-sync helpers are in place, `validate_doc_links.sh` covers the widened public graph, and public docs no longer refer to dev-only `_dev/s/` multi-instance helper scripts.
7. ~~**File size violations**~~ — ✅ Resolved (2026-03-29). All 20 files brought under 800-line guardrail via 13 test module extractions + 2 production code splits. 0 violations remaining.
8. ~~**GitHub release v1.0.0**~~ — ✅ Done (2026-03-29). 5 binary targets + Docker image published. Release run `23721789375`, CI run `23721442173`.
9. ~~**Debbie sync to staging**~~ — ✅ Resolved (2026-03-29). Pushed v1.0.0 + OTEL + TODO cleanup + codebase quality fixes + OpenAPI regen. CI green.
10. ~~**Post-merge regression validation**~~ — ✅ Resolved (2026-03-29). Full test suite green: 1546 (flapjack) + 1429 (flapjack-http) + server/ssl/replication crates.
11. ~~**OpenAPI spec CI fix**~~ — ✅ Resolved (2026-03-29). Root cause: 18 TODO stubs on utoipa-annotated handlers caused spec mismatch after scrai strip. Replaced with real doc comments.
12. ~~**Codebase quality cleanup**~~ — ✅ Resolved (2026-03-29). Fixed 15 error-leaking 500 sites, migrated 5 handler files to `HandlerError`, decomposed `execute_search_query`, removed cognitive_complexity suppressions.
13. ~~**TODO stub cleanup**~~ — ✅ Resolved (2026-03-29). Replaced ~601 auto-generated `TODO: Document` stubs with real doc comments. HA soak test script added.
14. ~~**Distributed tracing**~~ — ✅ Resolved (2026-03-28). OTLP gRPC export shipped behind `otel` feature flag.
15. ~~**OSS packaging**~~ — ✅ Resolved (2026-03-28). SECURITY.md, CHANGELOG.md, CONTRIBUTING.md shipped. All crates at 1.0.0.
16. ~~**Overload interpretation / tuning**~~ — ✅ Resolved (2026-03-28). Soak scenarios use `SOAK_WRITE_THRESHOLDS`.

### Active

1. **Debbie sync wave 3** — latest dev-main post-launch commits are not in staging/prod yet. The outstanding sync wave now needs to carry the tour closure + HA truth-sync surfaces, the explicit public-doc sync contract and validators, the regression-gate follow-through, and the refreshed committed OpenAPI export.
2. **HA convergence follow-up** — the Mar 30 proof shows restart survivability, but not final document-count convergence, for the nginx-routed example topology. Decide whether to harden the example topology, add client/write-retry guidance, or leave the limitation as a documented boundary.
3. **Runbooks iteration** — refine `OPERATIONS.md` from real failure scenarios.
4. **Security depth** — OWASP deep pass deferred until multi-tenant SaaS, but keep `SECURITY_BASELINE.md` scoped.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (runbooks, mobile dashboard, OWASP deep pass).
