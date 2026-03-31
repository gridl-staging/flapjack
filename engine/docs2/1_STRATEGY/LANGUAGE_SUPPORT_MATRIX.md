# Stage 2 Language Support Matrix

Date: 2026-02-24

This document closes Stage 2 checklist items:
- `F.1` CJK behavior assessment vs Algolia
- `F.3` mixed CJK+Latin quick-win assessment / TODO logging
- `H.7` language support matrix

## Source of truth

Matrix and notes are derived from current implementation:
- `engine/src/language.rs`
- `engine/src/index/mod.rs`
- `engine/src/tokenizer/cjk_tokenizer.rs`
- `engine/src/query/stopwords/mod.rs`
- `engine/src/query/plurals.rs`
- `engine/src/query/decompound.rs`

## Summary

- Canonical language codes: `66`
- Stopword coverage: `30` languages with embedded lists (`pt-br` aliases to `pt` list)
- Plural coverage: `4` languages (`en`, `fr`, `de`, `es`)
- Decompound coverage:
  - Declared languages: `6` (`de`, `nl`, `fi`, `da`, `sv`, `no`)
  - Dictionary-backed implementation: `1` (`de`)
- CJK tokenizer trigger for `indexLanguages`: `ja`, `zh`, `ko`
- Default (empty `indexLanguages`) remains CJK-aware for backwards compatibility

## Quality legend

- `none`: not implemented for that language
- `embedded`: compile-time bundled static data (stopwords)
- `alias->pt`: `pt-br` maps to Portuguese stopword list
- `rules`: rule-based morphology (no dictionary yet)
- `dictionary+rules`: dictionary-backed with rule fallback
- `declared`: language is accepted for feature routing but currently falls through
- `dictionary`: language has active dictionary-backed implementation
- `latin`: Latin-style token grouping (including non-CJK scripts)
- `cjk-char`: character-level CJK splitting at index/search analyzer layer

## CJK behavior assessment (F.1)

Current supported behavior is intentionally simple and deterministic:
- `CjkAwareTokenizer` splits CJK characters individually when CJK mode is enabled.
- Latin/digit runs are kept as full word tokens.
- `indexLanguages` controls analyzer mode:
  - contains any of `ja`, `zh`, `ko` -> CJK character splitting enabled
  - otherwise -> `latin_only` mode (CJK grouped as regular word runs)

Known limitations:
- Algolia uses dictionary-based segmentation (ICU/MeCab style behavior), not character-only segmentation.
- Character-level CJK mode has higher recall for single-character probes but lower linguistic precision for real word boundaries.

Observed mixed-script behavior today:
- In CJK-aware mode: `hello中国world` tokenizes as `hello`, `中`, `国`, `world`
- In latin-only mode: `hello中国world` tokenizes as one run (`hello中国world`)

## Mixed CJK+Latin quick-win assessment (F.3)

No safe low-risk tokenizer change was identified for this stage that improves parity without introducing broader ranking side effects.

Reasons:
- Adding extra mixed-script boundary tokens can materially change ranking and typo behavior globally.
- Word-boundary improvements for CJK require dictionary segmentation, not small separator tweaks.
- Existing Stage 2 tests already lock expected current behavior for CJK-aware vs latin-only modes.

Future-work upgrade path:
- Introduce dictionary-backed CJK segmentation providers per language (`ja`, `zh`, `ko`) behind a feature flag.
- Add mixed-script token strategy evaluation with ranking A/B checks before changing default token emission.
- Add normalization pass for script/width variants (full-width/half-width) before segmentation.
- Add parity fixtures against real Algolia CJK/mixed-script query cases.

## Per-language support matrix (H.7)

| Language code | Index tokenization mode when selected | Stopwords | Plurals | Decompound |
| --- | --- | --- | --- | --- |
| af | latin | none | none | none |
| ar | latin | embedded | none | none |
| az | latin | none | none | none |
| bn | latin | none | none | none |
| bg | latin | embedded | none | none |
| ca | latin | embedded | none | none |
| cs | latin | embedded | none | none |
| cy | latin | none | none | none |
| da | latin | embedded | none | declared |
| de | latin | embedded | rules | dictionary |
| el | latin | embedded | none | none |
| en | latin | embedded | dictionary+rules | none |
| eo | latin | none | none | none |
| es | latin | embedded | rules | none |
| et | latin | none | none | none |
| eu | latin | none | none | none |
| fa | latin | none | none | none |
| fi | latin | embedded | none | declared |
| fo | latin | none | none | none |
| fr | latin | embedded | rules | none |
| ga | latin | embedded | none | none |
| gl | latin | none | none | none |
| he | latin | none | none | none |
| hi | latin | embedded | none | none |
| hu | latin | embedded | none | none |
| hy | latin | none | none | none |
| id | latin | embedded | none | none |
| it | latin | embedded | none | none |
| ja | cjk-char | embedded | none | none |
| ka | latin | none | none | none |
| kk | latin | none | none | none |
| ko | cjk-char | embedded | none | none |
| ku | latin | none | none | none |
| ky | latin | none | none | none |
| lt | latin | embedded | none | none |
| lv | latin | none | none | none |
| mi | latin | none | none | none |
| mn | latin | none | none | none |
| mr | latin | none | none | none |
| ms | latin | none | none | none |
| mt | latin | none | none | none |
| nl | latin | embedded | none | declared |
| no | latin | embedded | none | declared |
| ns | latin | none | none | none |
| pl | latin | embedded | none | none |
| ps | latin | none | none | none |
| pt | latin | embedded | none | none |
| pt-br | latin | alias->pt | none | none |
| qu | latin | none | none | none |
| ro | latin | embedded | none | none |
| ru | latin | embedded | none | none |
| sk | latin | none | none | none |
| sq | latin | none | none | none |
| sv | latin | embedded | none | declared |
| sw | latin | none | none | none |
| ta | latin | none | none | none |
| te | latin | none | none | none |
| tl | latin | none | none | none |
| tn | latin | none | none | none |
| tr | latin | embedded | none | none |
| tt | latin | none | none | none |
| th | latin | embedded | none | none |
| uk | latin | embedded | none | none |
| ur | latin | none | none | none |
| uz | latin | none | none | none |
| zh | cjk-char | embedded | none | none |

## Stage 3 dictionary precedence matrix (I.2)

Scope: query preprocessing behavior when built-in language data and custom dictionary entries coexist.

Implementation references:
- `engine/src/dictionaries/manager.rs`
- `engine/src/index/manager.rs`

| Dictionary type | Built-in data source | Custom data source | If `disableStandardEntries` is `false` | If `disableStandardEntries` is `true` | Additional precedence behavior |
| --- | --- | --- | --- | --- | --- |
| `stopwords` | `query::stopwords::stopwords_for_lang` | Dictionary `stopwords` entries | Effective set = built-in union custom enabled entries, then remove custom `state: "disabled"` words | Built-in excluded; effective set = custom enabled entries, then remove custom disabled words | A custom disabled stopword removes that word from the effective set even if it exists in built-ins |
| `plurals` | `query::plurals::expand_plurals_for_lang` (rules/dictionary by language) | Dictionary `plurals` entries (`words` equivalence sets) | Query expansion uses built-in expansions plus all matching custom sets | Built-in expansion skipped; custom plural sets still apply | Custom sets are additive and remain active regardless of built-in toggle |
| `compounds` | `query::decompound::decompound_for_lang` | Dictionary `compounds` entries (`word` -> `decomposition`) | Query decompound uses built-in parts and custom decompositions | Built-in decompound skipped; custom decompositions still apply | Custom decomposition parts are merged into term expansion map and can override/add coverage |

Language scoping and fallback:
- Settings are scoped per dictionary type and language code (`disableStandardEntries.{dict}.{lang}`).
- A toggle for one language does not affect other languages.
- On dictionary load failure, query path logs a warning and falls back to built-in behavior for robustness.
