# Flapjack - Project Overview

**Last updated:** 2026-08-08 early morning

## Mission

Flapjack is a drop-in replacement for Algolia: a typo-tolerant full-text search engine with faceting, geo search, custom ranking, vector search, and click analytics.
It keeps the Algolia-compatible client and InstantSearch.js surface working while running as a single static binary with data stored on disk.

## Highest Priority

**Adoption status, stated once so no other document has to guess: Flapjack has no users and no customers.** `v1.0.11` published 2026-08-06 and the source is public,
but nobody runs it in production, Flapjack Cloud is not live, and no revenue exists. Corrected 2026-08-02: an earlier commercial-adoption claim was false and had
been propagating into risk arguments that assumed real workloads. **(1) Breaking changes are cheap** — no installed base to migrate, so prefer the correct design
over the compatible one wherever they conflict, except on the Algolia-compatible wire surface, which is the product. **(2) Nothing is urgent because it is "in
production" — the urgency is that we cannot yet honestly ask anyone to adopt it.** The bar is not "no known incidents"; it is "we can prove the claims we make."

**Current priority order follows `ROADMAP.md ## Up Next`; row state and evidence live there.** Staging push CI is green end to end for the first time on record, and a dispatched staging nightly passed all 36 jobs. The remaining work is proof distribution, uncensused test-signal integrity, and exact survivor follow-through.

1. **`STRAND-1`** — reconcile ahead branches that the repo-local roster cannot see, especially the cross-repo `jul28_9pm_8` lane.
2. **`SYNC-1`** — publish the already-green staging state to prod only after the row's scheduled-nightly gate allows it; do not infer mirror contents.
3. **`NIGHT-1`** — observe scheduled runs, not manual dispatches.
4. **`SDK-1`** — publish the already-landed SDK host fix through the channels that do not need registry credentials.
5. **`MIG-22`** — put the release-profile loopback contract into a recurring gate.
6. **`JOIN-1`** — rerun the full joined-proof suite and report command at one clean SHA.
7. **`TEST-FLAKE-3`, `TEST-FLAKE-2`, `TEST-FLAKE-1`** — keep the shared resolver and union-run evidence in one lane; do not add symptoms to the census in place of a fix.
8. **`CI-E2E-1`** — after `SYNC-1`, observe the prod job green at a SHA where it previously failed.
9. **`SURV-1`** — work only the exact `aug07_8pm_6` residual shapes not already owned elsewhere; source-less gap specs remain closeout limitations until recovered.
10. **`TEST-ORPHAN-1`** — fix the bounded-test orphan leak and in-test deadline gap without broad process kills.

Other standing priorities remain routed to their rows: `SEC-W4` for the security hygiene residual, `RF-4` for runbooks, `PL-10` for write-path saturation, `MIG-7`/`MIG-13`/`MIG-14` for migration limitations, and `ADR-0005 OQ4` for cross-node dedup.

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
