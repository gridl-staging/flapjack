# Flapjack - Project Overview

**Last updated:** 2026-08-07 morning

## Mission

Flapjack is a drop-in replacement for Algolia: a typo-tolerant full-text search engine with faceting, geo search, custom ranking, vector search, and click analytics.
It keeps the Algolia-compatible client and InstantSearch.js surface working while running as a single static binary with data stored on disk.

## Highest Priority

**Adoption status, stated once so no other document has to guess: Flapjack has no users and no customers.** `v1.0.11` published 2026-08-06 and the source is public,
but nobody runs it in production, Flapjack Cloud is not live, and no revenue exists. Corrected 2026-08-02: an earlier commercial-adoption claim was false and had
been propagating into risk arguments that assumed real workloads. **(1) Breaking changes are cheap** — no installed base to migrate, so prefer the correct design
over the compatible one wherever they conflict, except on the Algolia-compatible wire surface, which is the product. **(2) Nothing is urgent because it is "in
production" — the urgency is that we cannot yet honestly ask anyone to adopt it.** The bar is not "no known incidents"; it is "we can prove the claims we make."

**What changed on 2026-08-07, because it reorders everything below.** The console migration shipped whole — the browser now drives discovery, a
translation-report dry-run, and a submit against a real source for **all three** providers, with migrated documents proven through the served search
API, and the shipped-profile backend defect that blocked two of them (`MIG-22`) is repaired and proven. That row has closed; its shipped behaviour is
owned by [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md), and neither competitor's console ships a dry-run. Meanwhile the signal half proved
healthier than any ledger recorded: staging `31176417863`, the first nightly on either mirror carrying every landed repair, returned **410 passed /
1 failed**. The binding constraint is no longer *delivery* and no longer *repair* — it is **distribution of proof**: our own public mirror has not
received the repairs, so it keeps producing red that reads exactly like an unfixed defect.

The current strategic order is:

1. **`SYNC-1` — the prod mirror is a day behind and it is manufacturing false red.**
   First because it is the only item here whose absence corrupts the evidence other
   rows are read from. Prod head `35da0206f` (2026-08-06 11:16Z) carries none of the
   four backend-start env exports the dashboard-harness repair adds, so every prod
   nightly since has re-run the pre-repair harness. **Do not attribute a prod CI failure
   without reading the mirror's own copy of the workflow at the run's `headSha`.** The
   sync is an outward-facing publish and needs operator authorization; the durable half
   — a staleness gate — is ordinary lane work. Owner: [`ROADMAP.md`](ROADMAP.md) `SYNC-1`.

2. **`NIGHT-1` — 31 consecutive red nights, now down to a single bucket.**
   2026-07-08 through 2026-08-07, last green 2026-07-07. Three of the four original
   buckets are repaired, and buckets 2 and the `CI-E2E-1` symptom are now **positively
   proven green** on staging rather than merely believed repaired. What remains is one
   Linux-only fixture-teardown defect — `source_provider_fixture_ctl.sh down typesense`
   cannot `rm -rf` root-owned container files, a boundary macOS never exercises — in
   flight in lane `aug06_10am_9`. Exit is still two consecutive green scheduled runs,
   and it is gated on `SYNC-1`. Owner: [`ROADMAP.md`](ROADMAP.md) `NIGHT-1`.

3. **Proof that runs by itself — `MIG-22`'s recurring-gate clause and `JOIN-1`'s re-run.**
   These are two halves of one problem: a capability is proven once and then nothing
   re-proves it. `MIG-22`'s shipped-profile loopback contract passes `26/26` and reds
   correctly under mutation, but it is in neither `--all` nor `--ci` nor any workflow, so
   it never runs unless a human names it — this repo's own "a guard that cannot fail is
   not a guard" rule, in its quietest form. `JOIN-1`'s two red proof keys are the console
   migration specs, which now pass; the report simply has not been re-run, and **the
   number is a command** — run `node scripts/join_proof_report.mjs`, never copy `59` or
   `61` forward. Owners: [`ROADMAP.md`](ROADMAP.md) `MIG-22`, `JOIN-1`.

4. **Test-signal integrity — `TEST-FLAKE-3` first, then `TEST-FLAKE-2`, then `TEST-FLAKE-1`.**
   `TEST-FLAKE-3` is new and is not cosmetic: three fail-closed outbound controls
   (pin adherence, DNS-rebind refusal, blocked-vendor refusal before connect) fail only
   under the full union, from shared `cfg(test)` resolver state, and they are uncensused.
   In flight in `aug06_10am_10`, whose Stage 3 quarantined union is also the missing
   evidence for `TEST-FLAKE-2` and `TEST-FLAKE-1` — so one lane discharges all three.
   Standing warnings: **rule out the stale incremental cache** with a quarantined
   `engine/target` at `CARGO_INCREMENTAL=0` before blaming a product defect, and
   distinguish a 0%-CPU hang from slowness with `%cpu` plus `sample` before touching any
   timeout.

5. **`CI-E2E-1` — the last release-path integrity residual, now proven on staging.**
   Its repair passed `vector-settings.spec.ts` on Linux at staging SHA `1db1f8dcb`. What
   remains is `SYNC-1` plus observing the job green on prod at a SHA where it previously
   failed. **Do not re-implement the fix, and do not close it by weakening
   `engine/src/security.rs`** — the refusal it trips over is a real SSRF control with a
   documented opt-in the CI job did not set. Owner: [`ROADMAP.md`](ROADMAP.md) `CI-E2E-1`.

6. **`DOC-SSOT-1` — the instruction file every agent reads contradicts the ledger.**
   Higher than its size suggests, because it is upstream of planning itself:
   `.scrai/overview.md:11` assembles into `CLAUDE.md`/`AGENTS.md` and still says the
   React dashboard is **frozen, do not plan new work here**, five days after this
   document reversed that — and the work that just closed at priority-1 was new feature
   work in exactly that directory. Owner: [`ROADMAP.md`](ROADMAP.md) `DOC-SSOT-1`.

7. **Security follow-through — the `SEC-W4` hygiene residual.** The serving path is no longer a capability question: HTTPS from operator PEM or hot-rotating ACME material,
mandatory replication-peer credentials over non-cleartext transport, no key material in browser storage. `SEC-G5` closed 2026-08-06. One item remains: a disposition
for the production moderate advisories below the audit gate's high-and-above threshold. Separately, `SDK-1` was **re-diagnosed 2026-08-07**: the source fix has been in `sdks/` since 2026-07-16, so the remaining work is publication, and
three of its four channels need no registry credential. See [`ROADMAP.md`](ROADMAP.md) `SDK-1`; do not restate its decision here.

8. **RF-4 — runbooks iteration.** Keep operational routing in [`engine/docs2/3_IMPLEMENTATION/OPERATIONS.md`](engine/docs2/3_IMPLEMENTATION/OPERATIONS.md) and keep folding incident learnings into the runbooks.

9. **PL-10 — write-path saturation under sustained load.** The single-writer Tantivy ceiling remains the architectural constraint for v1.1. Three standing warnings: further
scale work needs a *new falsifiable question* beyond the verified 1M floor rather than another run; the superseded July 25 failures must not be profiled as current
defects; and the residual is **remote harness reliability**, not host contention — "needs a quiet host" is void.

10. **Migration deferred-scope follow-through.** Create-only import, fenced
existing-target overwrite, and interrupted-job resume all ship on the synchronous and
authenticated async Algolia paths. `flapjack migrate preview` reaches the CLI **and the
console** for all three providers. HA-converging import stays refused under
[`ROADMAP.md`](ROADMAP.md) `MIG-7`; resume remains Algolia-only, and adding a console
job-status/resume surface is a recorded follow-up candidate rather than open work.

11. **ADR-0005 OQ4 — cross-node failover idempotency dedup.** Node-local restart-durable idempotency ships; cross-node dedup remains a v1.1 planned item.

Release history and shipped-feature lineage stay in [`CHANGELOG.md`](CHANGELOG.md) and
[`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md). `PROJECT_OVERVIEW.md` owns
mission and priority order; it does not duplicate that status ledger.

## Product surfaces

`engine/dashboard/` (React) is a **maintained, first-class product surface**. It is not frozen and is not scheduled for replacement; every earlier
"deferred to the Svelte console" disposition is void. This is stated here because `.scrai/overview.md` — which assembles into `CLAUDE.md` and
`AGENTS.md` — points every agent at this file for it. Shipped dashboard capability is described in
[`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md), not here.

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
