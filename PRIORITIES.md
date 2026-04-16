# Flapjack — Priorities

**Last updated:** 2026-04-16

Canonical priority details are maintained in [`engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md`](engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md).

## Summary

All launch blockers closed. v1.0.0 released. The latest public sync lineage now includes the Mar 31 pm1-pm6 hardening stack plus Apr 8 and Apr 15 follow-through. Keep exact SHAs, CI runs, and sync audit details in the canonical ledger at [`engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical`](engine/docs2/FEATURES.md#public-sync-lineage-ledger-canonical). Current priority is closing the design-gated follow-through gaps for search HA ownership/freshness and durable analytics rollup writer/query-planner integration.

### Completed (most recent first)

1. ~~**Test hygiene, SDK contract CI, and query safety audit**~~ — ✅ Resolved with deferrals (2026-04-15). Public CI now runs the engine SDK contract tests, dashboard/browser false-positive patterns were tightened, stale `_dev/s` test shims were removed in favor of `engine/s/test`, targeted test coverage was added, and confirmed filter/experiment error paths now return typed/sanitized errors. OpenAPI snapshot follow-up remained deferred in the session handoff.
2. ~~**Analytics retention hardening + rollup foundation**~~ — ✅ Retention resolved; rollup foundation merged (2026-04-15). Partition retention is deterministic and still defaults to 90 days. Durable rollup design, known-answer query contracts, schema/config helpers, and `RollupManifest` foundation are merged. Rollup writer/query-planner/gated deletion remain open.
3. ~~**Targeted cleanup follow-through**~~ — ✅ Resolved (2026-04-08). Dashboard experiment normalization/results typing moved into `engine/dashboard/src/lib/experiment-normalization.ts`, and a stale server cognitive-complexity suppression was removed.
4. ~~**Experiment handler merge guardrails**~~ — ✅ Resolved (2026-03-31). `/2/abtests/{id}/results` now routes through the shared store-and-ID resolver seam, direct results-endpoint proofs cover store-unavailable plus numeric/UUID resolution, and experiment OpenAPI docs now declare resolver-driven `500` paths consistently. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
5. ~~**Runbook parity + admin-key truth sync**~~ — ✅ Resolved (2026-03-31). Standardized `flapjack --data-dir <path> reset-admin-key` across startup output, dashboard auth help, internal auth docs, and operator runbooks, including shell-safe quoting for spaced data-dir paths. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
6. ~~**Security baseline follow-through**~~ — ✅ Resolved (2026-03-31). Closed the scoped HTTP-hardening proof gaps by adding invalid-key non-consumption and `FLAPJACK_MAX_BODY_MB` body-limit `413` tests, aligning the security docs/audit, refreshing the committed OpenAPI export, and tightening helper-script safety around sync destinations. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
7. ~~**Security baseline docs + test coverage**~~ — ✅ Resolved (2026-03-31). `SECURITY_BASELINE.md` and the audit matrix now capture the shipped CORS/body-limit/trusted-proxy/rate-limit surface with focused proof references, and `startup_catchup.rs` now treats write-queue timeout as warn-and-continue outside strict bootstrap. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
8. ~~**Nightly CI + sync hygiene**~~ — ✅ Resolved (2026-03-31). Restored nightly Rust CI parity by creating the dashboard dist stub, added `CHANGELOG.md` / `CONTRIBUTING.md` / `SECURITY.md` to the public sync whitelist, and clarified README vector/hybrid support caveats by binary target. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
9. ~~**Operations runbook hardening**~~ — ✅ Resolved (2026-03-31). `OPERATIONS.md` now carries proof-backed startup/readiness/replication/admin-key/snapshot failure runbooks, stronger ownership links to deployment/security/config docs, verified proof citations, and the corrected `flapjack --data-dir <path> reset-admin-key` syntax. Published in the completed public lineage (see the canonical ledger in `engine/docs2/FEATURES.md`).
10. ~~**Debbie sync wave 3**~~ — ✅ Resolved (2026-03-31). Published the last wave of post-launch hardening to staging (`6166055`, CI run `23818440499`) and prod (`b7841a0`, CI run `23819698304`): public-doc sync contract, HA boundary truth surfaces, regression-gate follow-through, and the refreshed committed OpenAPI export.
11. ~~**HA convergence contract + runbook truth sync**~~ — ✅ Resolved (2026-03-31). Boundary path selected. Added `engine/docs2/4_EVIDENCE/HA_CONVERGENCE_ANALYSIS.md`, aligned `OPERATIONS.md` and the HA example README to the proven async-replication boundary, and tightened `engine/loadtest/tests/ha_soak_acceptance.sh`.
12. ~~**Tour walkthrough closure + tour typecheck unblock**~~ — ✅ Resolved (2026-03-30). Closed the former vector/chat blockers (specs 05/06), archived MP4s for all 24/24 per-feature tours, default-enabled vector/local embedding support in the build, wired local AI-provider env bridging, and fixed `npx tsc --noEmit -p tsconfig.tour.json` by adding local Node types plus aligning tour typings with the real `VideoTour` API.
13. ~~**HA multi-node soak execution + truth sync**~~ — ✅ Executed (2026-03-30). The 2h 3-node soak proof pack is now retained at `engine/loadtest/results/20260330T211227Z-ha-soak/`. Final classification was `warning-findings` with `diverged` convergence after 39 restart rotations in the nginx-routed example topology. See [`engine/loadtest/BENCHMARKS.md`](engine/loadtest/BENCHMARKS.md).
14. ~~**Cognitive complexity reduction**~~ — ✅ Resolved (2026-03-30). Decomposed 5 high-CC hotspots: `merge_settings_payload` (CC=35), `validate` (CC=29), `compute_exact_vs_prefix_bucket` (CC=26), `build_results_response` (CC=22), `browse_index` (CC=21). Added settings characterization tests.
15. ~~**Debbie sync pipeline wave 2**~~ — ✅ Resolved (2026-03-30). Full sync to staging+prod. OpenAPI test dedup, experiment handler extraction, soak proof improvements.
16. ~~**Full regression gate + targeted fixes**~~ — ✅ Resolved (2026-03-31). Ran the full post-merge regression gate, fixed FastEmbed local-embedder test nondeterminism by serializing the affected ONNX/model-cache tests, captured proof artifacts in `engine/state/`, and re-synced the committed OpenAPI export after restoring real browse/experiment endpoint summaries.
17. ~~**Public doc sync surface hardening**~~ — ✅ Resolved (2026-03-31). `.debbie.toml` now explicitly syncs the canonical public doc graph, `engine/tests/validate_sync_surface.sh` and shared doc-sync helpers are in place, `validate_doc_links.sh` covers the widened public graph, and public docs no longer refer to dev-only `_dev/s/` multi-instance helper scripts.
18. ~~**File size violations**~~ — ✅ Resolved (2026-03-29). All 20 files brought under 800-line guardrail via 13 test module extractions + 2 production code splits. 0 violations remaining.
19. ~~**GitHub release v1.0.0**~~ — ✅ Done (2026-03-29). 5 binary targets + Docker image published. Release run `23721789375`, CI run `23721442173`.
20. ~~**Debbie sync to staging**~~ — ✅ Resolved (2026-03-29). Pushed v1.0.0 + OTEL + TODO cleanup + codebase quality fixes + OpenAPI regen. CI green.
21. ~~**Post-merge regression validation**~~ — ✅ Resolved (2026-03-29). Full test suite green: 1546 (flapjack) + 1429 (flapjack-http) + server/ssl/replication crates.
22. ~~**OpenAPI spec CI fix**~~ — ✅ Resolved (2026-03-29). Root cause: 18 TODO stubs on utoipa-annotated handlers caused spec mismatch after scrai strip. Replaced with real doc comments.
23. ~~**Codebase quality cleanup**~~ — ✅ Resolved (2026-03-29). Fixed 15 error-leaking 500 sites, migrated 5 handler files to `HandlerError`, decomposed `execute_search_query`, removed cognitive_complexity suppressions.
24. ~~**TODO stub cleanup**~~ — ✅ Resolved (2026-03-29). Replaced ~601 auto-generated `TODO: Document` stubs with real doc comments. HA soak test script added.
25. ~~**Distributed tracing**~~ — ✅ Resolved (2026-03-28). OTLP gRPC export shipped behind `otel` feature flag.
26. ~~**OSS packaging**~~ — ✅ Resolved (2026-03-28). SECURITY.md, CHANGELOG.md, CONTRIBUTING.md shipped. All crates at 1.0.0.
27. ~~**Overload interpretation / tuning**~~ — ✅ Resolved (2026-03-28). Soak scenarios use `SOAK_WRITE_THRESHOLDS`.

### Active

1. **Search HA ownership/freshness design gate** — do not implement automatic write promotion until index owner, generation/term, replica freshness, restart recovery, and split-brain behavior have one tested source of truth. Safe forwarding/503 behavior is the likely first implementation step after the design gate.
2. **Durable analytics rollup writer/query planner** — build on the merged design/schema/config/manifest foundation, but require known-answer query tests and certified-coverage retention gates before any rollup reads or raw deletion.
3. **OpenAPI snapshot follow-through** — the test-hygiene session handoff called out an OpenAPI export mismatch as deferred; rerun the focused test at current `main` and regenerate `engine/docs2/openapi.json` if still red.
4. **Runbooks iteration** — keep refining `OPERATIONS.md` from real incidents; the docs and admin-key recovery contract now exist, so the remaining work is operational polish rather than missing baseline coverage.

## Post-Launch

See [`ROADMAP.md#open--not-yet-implemented`](ROADMAP.md#open--not-yet-implemented) for post-launch work (search HA design, durable analytics rollups, runbooks, mobile dashboard, OWASP deep pass).
