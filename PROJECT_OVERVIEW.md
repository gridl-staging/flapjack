# Flapjack - Project Overview

**Last updated:** 2026-08-06 evening

## Mission

Flapjack is a drop-in replacement for Algolia: a typo-tolerant full-text search engine with faceting, geo search, custom ranking, vector search, and click analytics.
It keeps the Algolia-compatible client and InstantSearch.js surface working while running as a single static binary with data stored on disk.

## Highest Priority

**Adoption status, stated once so no other document has to guess: Flapjack has no users and no customers.** `v1.0.11` published 2026-08-06 and the source is public,
but nobody runs it in production, Flapjack Cloud is not live, and no revenue exists. Corrected 2026-08-02: an earlier commercial-adoption claim was false and had
been propagating into risk arguments that assumed real workloads. **(1) Breaking changes are cheap** — no installed base to migrate, so prefer the correct design
over the compatible one wherever they conflict, except on the Algolia-compatible wire surface, which is the product. **(2) Nothing is urgent because it is "in
production" — the urgency is that we cannot yet honestly ask anyone to adopt it.** The bar is not "no known incidents"; it is "we can prove the claims we make."

**What changed on 2026-08-06, because it reorders everything below.** The release path shipped — `v1.0.11` is tagged, released, on GHCR, and resolved by the unpinned
installer, and the Flapjack Cloud engine AMI bakes from that same archive. For the first time since 2026-06-09 engine work reaches a user by either route. No roadmap
row is gated on the release path, so the binding constraint is no longer *delivery* but *proof*. That evening the console also gained all three migration providers —
so the top of this list is now one unprovable shipped capability followed by the signal work that would prove the rest.

The current strategic order is:

1. **`MIG-22` — the console migrates from all three providers and we cannot prove it.**
   First as of 2026-08-06 evening: the only row where a *shipped* capability is
   unprovable. `MIG-21`'s provider half merged, but the Meilisearch/Typesense loopback
   admission seams are `#[cfg(debug_assertions)]`-gated, so the `--release` backend the
   e2e harness builds compiles them out and both provider specs fail. **The console is
   not the defect — do not "fix" it there.** The shipped binary also ignores an opt-in
   the console tells self-hosting users to set, which is a correctness problem in its own
   right. In flight in lane `aug06_10am_1a`. Owner: [`ROADMAP.md`](ROADMAP.md) `MIG-22`.

2. **`NIGHT-1` — 30 consecutive red nights**, 2026-07-08 through 2026-08-06, last green
   2026-07-07. The only recurring real-Algolia end-to-end guard the project has, and it
   gave no signal across the whole period the migration feature set was rewritten.
   **Restated 2026-08-06 evening: this is now a wait, not a repair.** Three of four
   attributed buckets carry landed repairs — migration-oracle selector, dashboard
   capability provisioning, Algolia fixture credential readiness. The **Rust Clippy
   bucket has no identified repair commit**, though its exact invocation and
   `cargo fmt --all --check` exit `0` at HEAD on darwin-arm64 (evidence, not proof — the
   nightly runs on Linux). Exit is two consecutive green scheduled runs; 2026-08-07 is
   the first exercisable one, so 2026-08-08 is the earliest closure. Owner:
   [`ROADMAP.md`](ROADMAP.md) `NIGHT-1`.

3. **`CI-E2E-1` — the last release-path integrity residual; its repair has landed.**
   What remains is an observation, not an implementation: prod CI's `Dashboard full e2e
   tests` job must be seen green on the mirror at a SHA where it previously failed. **Do
   not re-implement the fix, and do not close it by weakening `engine/src/security.rs`**
   — the refusal it trips over is a real SSRF control with a documented opt-in the CI job
   did not set. Owner: [`ROADMAP.md`](ROADMAP.md) `CI-E2E-1`.

4. **Test-signal hygiene — `TEST-FLAKE-2` first, then `TEST-FLAKE-1`.**
   `merge_owner_survives_consecutive_commits` narrowed on 2026-08-06: its signal-based
   wait landed and it passed its detached specimen, but the broad gate exited `124` and
   surfaced an **uncensused** red, `bounded_aggregate_concurrency_across_simultaneous_requests`.
   That is `TEST-FLAKE-2`, sequenced first because no disposition covers it and it is
   what stops the broad gate from exiting `0`. Standing warnings: **rule out the stale
   incremental cache** with a quarantined `engine/target` at `CARGO_INCREMENTAL=0` before
   blaming a product defect, and distinguish a 0%-CPU hang from slowness with `%cpu` plus
   `sample` before touching any timeout.

5. **Security follow-through — the `SEC-W4` hygiene residual.** The serving path is no longer a capability question: HTTPS from operator PEM or hot-rotating ACME material,
   mandatory replication-peer credentials over non-cleartext transport, no key material in browser storage. `SEC-G5` closed 2026-08-06. One item remains: a disposition
   for the production moderate advisories below the audit gate's high-and-above threshold.

6. **Backend↔frontend joined proof (JOIN-1).** Last clean measurement **`59 / 59` at
   `05c546ca5`** — 0 failed, 0 skipped, 0 not-run, 0 unresolved. **The vendor-credential
   story is falsified and must not be re-inherited:** all four hops pass with the
   canonical pair and `P29` is green; a `403` `Invalid Application-ID or API key` body is
   byte-identical for an empty key, a bogus key, and a genuine refusal, so it can never
   on its own attribute a failure to the vendor. **The denominator then moved to 61**
   when the console half registered `P30`/`P31`, both red today on `MIG-22` — expected,
   and the reason the exit is a predicate. **The number is a command:** re-run
   `node scripts/join_proof_report.mjs`; never copy `59` or `61` forward. **Policy
   reversal, 2026-08-02: the React dashboard is not frozen and is not scheduled for
   replacement**; every prior "deferred to the Svelte console" disposition is void.

7. **RF-4 — runbooks iteration.** Keep operational routing in [`engine/docs2/3_IMPLEMENTATION/OPERATIONS.md`](engine/docs2/3_IMPLEMENTATION/OPERATIONS.md) and keep folding incident learnings into the runbooks.

8. **PL-10 — write-path saturation under sustained load.** The single-writer Tantivy ceiling remains the architectural constraint for v1.1. Three standing warnings: further
   scale work needs a *new falsifiable question* beyond the verified 1M floor rather than another run; the superseded July 25 failures must not be profiled as current
   defects; and the residual is **remote harness reliability**, not host contention — "needs a quiet host" is void.

9. **Migration deferred-scope follow-through.** Create-only import, fenced
   existing-target overwrite, and interrupted-job resume all ship on the synchronous and
   authenticated async Algolia paths. `flapjack migrate preview` reaches the CLI for all
   three providers — the one migration capability measurably ahead of both competitors'
   own migration guides, neither of which ships a dry-run — but **no console path reaches
   it**, which is `MIG-21`'s unstarted half. HA-converging import stays refused under
   [`ROADMAP.md`](ROADMAP.md) `MIG-7`; resume remains Algolia-only.

10. **ADR-0005 OQ4 — cross-node failover idempotency dedup.** Node-local restart-durable idempotency ships; cross-node dedup remains a v1.1 planned item.

Release history and shipped-feature lineage stay in [`CHANGELOG.md`](CHANGELOG.md) and
[`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md). `PROJECT_OVERVIEW.md` owns
mission and priority order; it does not duplicate that status ledger.

## Scope

- Maintain Algolia API compatibility for existing client integrations.
- Provide one-time migration into Flapjack from Algolia, Meilisearch, and
  Typesense through shared lifecycle/publication owners and source-specific
  adapters.
- Keep search latency low and memory usage bounded under realistic workloads.
- Extend analytics, vector search, HA, and operational tooling without increasing
  operator complexity.
- Keep public documentation routed through canonical owners: `PROJECT_OVERVIEW.md` for
  mission and priority order, [`ROADMAP.md`](ROADMAP.md) for open work,
  [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md) for shipped capability status,
  and [`CHANGELOG.md`](CHANGELOG.md) for release history.

## Non-Goals

- Recreating Algolia's hosted control plane or proprietary infrastructure.
- Moving public roadmap state into private chats, evidence bundles, or ad hoc
  release notes.
- Treating beta release history as the active work ledger.
- Weakening durability, API compatibility, or validation gates to improve raw
  throughput numbers.
