# Display Preferences Stage 1 Findings (Behavior Spec Baseline)

**Date:** 2026-03-17  
**Scope:** Stage 1 checklist completion evidence for behavior-contract baseline (Tier 1 + Tier 2 + research audit trail)

## Sources Reviewed
- `engine/dashboard/docs/BDD_SPECIFICATIONS.md` (Search & Browse story codes/format and final `B-SRH-003` acceptance criteria)
- `engine/dashboard/tests/specs/search-browse.md` (Tier 2 header/fixtures/TEST block template)
- `engine/dashboard/tests/specs/behaviors/search-facets.md` (extension-file pattern for existing search stories)
- `engine/dashboard/tests/specs/display-preferences.md` (Stage 1 Tier 2 contract output)
- `engine/dashboard/src/components/search/DocumentCard.tsx` (Browse-card baseline chrome and fail-soft rendering behavior)
- `engine/dashboard/src/components/search/DocumentCard.test.tsx` (shipped regression coverage for configured-card controls and safe image fallback)
- `<home>/.matt/projects/<project>/MAR17_12_02_display_preferences.md-9e13c491/refined_input.md` (goal/non-goal source of truth)

## Research Audit Findings

### R1. Story code and formatting convention
- Evidence: `B-SRH-001` and `B-SRH-002` already exist in Search & Browse, and entries use `### <code>: <title>` plus As/I want/So that blocks and Acceptance Criteria bullets (`engine/dashboard/docs/BDD_SPECIFICATIONS.md:136-157`).
- Conclusion: Next available code is `B-SRH-003`; matching heading/body format is mandatory for consistency.

### R2. Tier 2 template conventions
- Evidence: Existing Tier 2 spec uses metadata header (`Feature`, `BDD Spec`, `Priority`, `Last Updated`), fixture section, then repeated `## TEST` blocks with Setup/Execute/Verify/Expected/Cleanup (`engine/dashboard/tests/specs/search-browse.md:1-77`).
- Conclusion: Display Preferences Tier 2 spec should reuse this exact structure.

### R3. Top-level spec vs behaviors extension
- Evidence: `search-facets.md` is explicitly an extension file for existing search behavior entries (`engine/dashboard/tests/specs/behaviors/search-facets.md:1-27`).
- Evidence: Display Preferences is a new Browse capability with its own story and acceptance contract, not an edge-case addendum to existing search stories (`refined_input.md:13-25`).
- Conclusion: Top-level spec at `tests/specs/display-preferences.md` is the correct location.

### R4. Full behavior set and scope guardrails
- Evidence (required behaviors): Browse entry trigger, save/clear per-index preferences, auto-detect common names, persistence across navigation/refresh, fallback when no preferences, and consumed-field exclusion after configured header rendering (`refined_input.md:21-25`, `refined_input.md:42-49`).
- Evidence (non-goals): no backend persistence/API, no merchandising/rules integration, no metrics overlay/CTR annotations, no detected-preferences banner, no modal image preview (`refined_input.md:27-33`).
- Conclusion: Stage 1 contract must include both positive behavior requirements and explicit out-of-scope exclusions.

### R5. Browse-card baseline preservation and degraded-data behavior
- Evidence (existing chrome): current Browse cards retain the objectID badge plus JSON, Copy, and Delete controls in `engine/dashboard/src/components/search/DocumentCard.tsx`.
- Evidence (fail-soft behavior): configured title/subtitle/tag resolution tolerates missing/null values, and configured image rendering only accepts safe relative or `http/https` URLs while unusable values fall back without rendering an image element (`engine/dashboard/src/components/search/DocumentCard.tsx`).
- Conclusion: Final Stage 1 output must explicitly preserve the existing Browse card chrome and codify fail-soft behavior for missing/null configured values and unusable image values.

## Checklist Deliverable Coverage Matrix

- `@work:build` Tier 1 story drafted under Search & Browse: complete in `engine/dashboard/docs/BDD_SPECIFICATIONS.md:159-163`.
- Tier 1 acceptance criteria for save/clear/auto-detect/persist/fallback/isolation/consumed-field exclusion: complete in `engine/dashboard/docs/BDD_SPECIFICATIONS.md:165-175`.
- Tier 1 explicit non-goal exclusions: complete in `engine/dashboard/docs/BDD_SPECIFICATIONS.md:176`.
- Tier 1 explicit preservation of existing Browse card chrome and fail-soft configured-field behavior: complete in `engine/dashboard/docs/BDD_SPECIFICATIONS.md:172-173`.
- `@work:build` Tier 2 header + fixture contract: complete in `engine/dashboard/tests/specs/display-preferences.md:1-56`.
- Modal interaction contract (open, select title/subtitle/image/tags, save, clear): complete in `engine/dashboard/tests/specs/display-preferences.md:60-187`.
- Card rendering contract (configured header render + consumed-field exclusion + preserved card chrome + fail-soft behavior + default fallback): complete in `engine/dashboard/tests/specs/display-preferences.md`.
- Persistence and per-index isolation contract: complete in `engine/dashboard/tests/specs/display-preferences.md`.
- Unit-spec section (hook, modal, card): complete in `engine/dashboard/tests/specs/display-preferences.md`.
- Tier 3 coverage mapping ownership: complete in `engine/dashboard/tests/specs/display-preferences.md`.
- Tier 2 out-of-scope boundaries codified (including metrics-overlay exclusion): complete in `engine/dashboard/tests/specs/display-preferences.md`.

## Resolved Decisions
- Configured image rendering is no longer an open question: only safe relative or `http/https` values render as images, and unusable values fail soft without rendering an image element.

## Open Questions
- Should subtitle auto-detect ever select `description` by default, or remain conservative (`null`) unless explicitly chosen by the user?
