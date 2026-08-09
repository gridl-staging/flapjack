# Flapjack - Project Overview

**Last updated:** 2026-08-08

## Mission

Flapjack is a drop-in replacement for Algolia: a typo-tolerant full-text search engine with faceting, geo search, custom ranking, vector search, and click analytics.
It keeps the Algolia-compatible client and InstantSearch.js surface working while running as a single static binary with data stored on disk.

## Highest Priority

**Adoption status, stated once so no other document has to guess: Flapjack has no users and no customers.** `v1.0.11` published 2026-08-06 and the source is public,
but nobody runs it in production, Flapjack Cloud is not live, and no revenue exists. Corrected 2026-08-02: an earlier commercial-adoption claim was false and had
been propagating into risk arguments that assumed real workloads. **(1) Breaking changes are cheap** — no installed base to migrate, so prefer the correct design
over the compatible one wherever they conflict, except on the Algolia-compatible wire surface, which is the product. **(2) Nothing is urgent because it is "in
production" — the urgency is that we cannot yet honestly ask anyone to adopt it.** The bar is not "no known incidents"; it is "we can prove the claims we make."

**Current priority order follows `ROADMAP.md ## Up Next`; row state and evidence live there.** Reordered 2026-08-08: staging push CI is green end to end and a **scheduled** staging nightly concluded `success` for the first time since 2026-07-07, so the last question about whether the repairs survive a cron trigger is answered. The remaining work is almost entirely **distribution and reading** rather than building — publishing proven code to prod, and reading a measurement already in flight.

1. **`SYNC-1`** — publish the proven staging state to prod. Promoted to first because 2026-08-08 turned it from a wait into a lane: its gate failed on one clause, and the repair for that clause is now on `main`. Do not infer mirror contents; probe them.
2. **`JOIN-1` — do not dispatch; RETARGETED 2026-08-08.** The joined-proof rerun is withdrawn as a priority, not deferred. ADR 0006 deletes `engine/dashboard` at the unification cutover, so a numerator proving that tree works has a scheduled expiry; six lanes in nine days produced none, while the tree still took 164 commits in two weeks. What survives is manifest accuracy — the port map for the console migration and the OSS-console column of fjcloud's capability matrix. **This unblocks nothing on its own: `MIG-22` lost its declared closing partner and needs a new one.**
3. **`TEST-FLAKE-1`, `TEST-FLAKE-2`, `TEST-HARNESS-1`** — the post-merge union completed 2026-08-08, closing one flake row outright and leaving these three. The two remaining flake rows now wait on **one named failure**, `non_json_failure_redacts_api_key_from_stderr`, and close together when it is fixed. `TEST-HARNESS-1`'s diagnosis is confirmed by controlled comparison and its repair is known. Do not add symptoms to the census in place of a fix.
4. **`STRAND-1`** — one branch left, `jul28_9pm_8`, and it is still taking commits, so its recorded quiescence resume condition is falsified. Reach its cross-repo owner rather than re-observing it.
5. **`SDK-1`** — publish the already-landed SDK host fix through the channels that do not need registry credentials. The only open row that reduces user-facing harm rather than internal proof debt.
6. **`NIGHT-1`** — observe scheduled **prod** runs; there is no code left in the row, and a staging result is not a prod result.
7. **`CI-E2E-1`** — after `SYNC-1`, observe the prod job green at a SHA where it previously failed.
8. **`MIG-22`** — clause (1) is met by the scheduled `nightly.yml` gate. Its clause (2) was to close alongside `JOIN-1`; that partner was retargeted 2026-08-08, so **clause (2) is now unowned and must be re-homed before this row can close.** Do not read the retarget as discharging it.
9. **`SURV-1`** — work only the exact `aug07_8pm_6` residual shapes not already owned elsewhere; source-less gap specs remain closeout limitations until recovered.
10. **`TEST-ORPHAN-1`** — fix the bounded-test orphan leak and in-test deadline gap without broad process kills.

Other standing priorities remain routed to their rows: `SEC-W4` for the security hygiene residual, `RF-4` for runbooks, `PL-10` for write-path saturation, `MIG-7`/`MIG-13`/`MIG-14` for migration limitations, and `ADR-0005 OQ4` for cross-node dedup.

Release history and shipped-feature lineage stay in [`CHANGELOG.md`](CHANGELOG.md) and
[`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md). `PROJECT_OVERVIEW.md` owns
mission and priority order; it does not duplicate that status ledger.

## Product surfaces

`engine/dashboard/` (React) is **the shipping UI, and it is frozen against new screens as of 2026-08-08.** This section is the single owner of that policy:
`.scrai/overview.md` — which assembles into `CLAUDE.md` and `AGENTS.md` — deliberately states no scheduling policy and points every agent here, so this is
the only place the answer exists. **Corrected 2026-08-08: this section previously read "not frozen and not scheduled for replacement," which was false in both
halves and contradicted this file's own priority list two sections above it.** That is `DOC-SSOT-1`'s defect recurring one day after it closed — not as a stale
restatement this time, but as an owner that disagreed with itself, which is the harder version to notice.

The policy, sourced from the decision rather than restated from memory: [ADR 0006](engine/docs2/3_IMPLEMENTATION/decisions/active/0006_console_source_home.md)
has been **Accepted since 2026-07-18** — one Svelte console replaces both this dashboard and the managed cloud console, and this tree is deleted at the
program's **parity-gated** cutover (the program's phases live in `fjcloud_dev docs/design/console_unification_revised_plan.md`, not here). Two consequences
that are easy to get backwards:

- **Deletion is gated on parity, not scheduled on a date.** ADR 0006 states `engine/dashboard` remains the shipping UI until that cutover and that nothing in
  this repo changed at decision time. Do not read the freeze as "already replaced," and do not stop fixing it.
- **The freeze forbids ADDING a route, not editing one.** Enforced by `engine/dashboard/src/App.routeFreeze.test.tsx`, which pins the 24 route paths in
  `App.tsx`. Removing a route is allowed because that is migration progress; bugfix, CI, and security work inside existing screens stays legitimate until
  cutover, because a freeze that blocks repairs gets routed around within a week. A feature added inside an existing screen is not caught — a stated bound.

The enforcement exists because the decision alone did not hold: in the two weeks after 2026-07-25 this tree took 164 commits and +17,157 / −10,814 lines into a
codebase already decided for deletion. Shipped dashboard capability is described in [`engine/docs2/FEATURES.md`](engine/docs2/FEATURES.md), not here.

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
