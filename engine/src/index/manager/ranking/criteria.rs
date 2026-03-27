use super::super::*;

#[derive(Clone, Copy)]
pub(super) enum RankingCriterion {
    Typo,
    Geo,
    Words,
    Filters,
    Proximity,
    Attribute,
    Exact,
}

const DEFAULT_RANKING_CRITERIA: [RankingCriterion; 7] = [
    RankingCriterion::Typo,
    RankingCriterion::Geo,
    RankingCriterion::Words,
    RankingCriterion::Filters,
    RankingCriterion::Proximity,
    RankingCriterion::Attribute,
    RankingCriterion::Exact,
];

/// TODO: Document parse_ranking_criteria.
pub(super) fn parse_ranking_criteria(settings: Option<&IndexSettings>) -> Vec<RankingCriterion> {
    let criteria: Vec<_> = settings
        .and_then(|index_settings| index_settings.ranking.as_ref())
        .into_iter()
        .flatten()
        .filter_map(|criterion| match criterion.as_str() {
            "typo" => Some(RankingCriterion::Typo),
            "geo" => Some(RankingCriterion::Geo),
            "words" => Some(RankingCriterion::Words),
            "filters" => Some(RankingCriterion::Filters),
            "proximity" => Some(RankingCriterion::Proximity),
            "attribute" => Some(RankingCriterion::Attribute),
            "exact" => Some(RankingCriterion::Exact),
            _ => None,
        })
        .collect();

    if criteria.is_empty() {
        DEFAULT_RANKING_CRITERIA.to_vec()
    } else {
        criteria
    }
}

pub(in crate::index::manager) fn str_prefix_by_chars(input: &str, char_count: usize) -> String {
    input.chars().take(char_count).collect()
}

/// Computes typo distance between a query term and a candidate token.
/// When `allow_prefix = true`, prefix matches (candidate starts with query) count as distance 0.
/// When `allow_prefix = false`, only exact equality is distance 0; prefix matches are scored
/// by full Damerau-Levenshtein distance.
pub(in crate::index::manager) fn typo_distance_strict(
    query_term: &str,
    candidate_token: &str,
    allow_prefix: bool,
) -> usize {
    if query_term == candidate_token {
        return 0;
    }

    let query_len_chars = query_term.chars().count();
    let candidate_len_chars = candidate_token.chars().count();
    if query_len_chars == 0 || candidate_len_chars == 0 {
        return usize::MAX;
    }

    let full_distance = strsim::damerau_levenshtein(query_term, candidate_token);
    if candidate_len_chars >= query_len_chars {
        let prefix = str_prefix_by_chars(candidate_token, query_len_chars);
        if allow_prefix && prefix == query_term {
            return 0;
        }
        if allow_prefix {
            let prefix_distance = strsim::damerau_levenshtein(query_term, &prefix);
            return full_distance.min(prefix_distance);
        }
    } else {
        return full_distance;
    }
    full_distance
}

/// TODO: Document compute_typo_bucket_from_tokens.
pub(in crate::index::manager) fn compute_typo_bucket_from_tokens(
    query_terms: &[String],
    doc_tokens: &[String],
    prefix_eligible: &[bool],
    min_word_size_for_1_typo: usize,
    min_word_size_for_2_typos: usize,
) -> u8 {
    if query_terms.is_empty() {
        return 3;
    }

    if doc_tokens.is_empty() {
        return 3;
    }

    let mut worst_term_distance = 0usize;
    for (i, query_term) in query_terms.iter().enumerate() {
        let allow_prefix = prefix_eligible.get(i).copied().unwrap_or(false);
        let term_len = query_term.chars().count();
        let max_allowed_typos = max_allowed_typos_for_term_len(
            term_len,
            min_word_size_for_1_typo,
            min_word_size_for_2_typos,
        );
        let best = doc_tokens
            .iter()
            .map(|token| typo_distance_strict(query_term, token, allow_prefix))
            .min()
            .unwrap_or(usize::MAX);

        if best == usize::MAX || best > max_allowed_typos {
            return 3;
        }

        worst_term_distance = worst_term_distance.max(best);
        if worst_term_distance > 2 {
            return 3;
        }
    }

    match worst_term_distance {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 3,
    }
}

/// Returns `(typo_distance, is_prefix_only)` for a query term vs candidate token.
/// `is_prefix_only` is true when the best match is a prefix match (candidate starts
/// with query term but they are not equal) and there is no exact or fuzzy-exact match.
pub(in crate::index::manager) fn classify_match(
    query_term: &str,
    candidate_token: &str,
) -> (usize, bool) {
    if query_term == candidate_token {
        return (0, false); // exact match
    }

    let query_len_chars = query_term.chars().count();
    let candidate_len_chars = candidate_token.chars().count();
    if query_len_chars == 0 || candidate_len_chars == 0 {
        return (usize::MAX, false);
    }

    // Check prefix match: candidate is longer and starts with query
    if candidate_len_chars >= query_len_chars {
        let prefix = str_prefix_by_chars(candidate_token, query_len_chars);
        if prefix == query_term {
            return (0, true); // prefix match — distance 0 but prefix-only
        }
    }

    // Fall back to full edit distance (not a prefix match)
    let full_distance = strsim::damerau_levenshtein(query_term, candidate_token);
    if candidate_len_chars >= query_len_chars {
        let prefix = str_prefix_by_chars(candidate_token, query_len_chars);
        let prefix_distance = strsim::damerau_levenshtein(query_term, &prefix);
        (full_distance.min(prefix_distance), false)
    } else {
        (full_distance, false)
    }
}

/// Determine which query terms are prefix-eligible based on `query_type`.
/// - `prefixAll` → all true
/// - `prefixNone` → all false
/// - `prefixLast` → only the last term, and only if the query doesn't end with a space
pub(in crate::index::manager) fn compute_prefix_eligible(
    query_type: &str,
    num_terms: usize,
    query_text: &str,
) -> Vec<bool> {
    match query_type {
        "prefixAll" => vec![true; num_terms],
        "prefixNone" => vec![false; num_terms],
        _ => {
            // prefixLast (default)
            let mut flags = vec![false; num_terms];
            if num_terms > 0 && !query_text.ends_with(' ') {
                flags[num_terms - 1] = true;
            }
            flags
        }
    }
}

pub(in crate::index::manager) type TermAlternatives = Vec<Vec<String>>;

/// TODO: Document build_term_alternatives.
pub(in crate::index::manager) fn build_term_alternatives(
    query_terms: &[String],
    alternatives_as_exact: &[String],
    synonym_store: Option<&SynonymStore>,
    plural_map: Option<&HashMap<String, Vec<String>>>,
) -> TermAlternatives {
    let include_ignore_plurals = alternatives_as_exact.iter().any(|v| v == "ignorePlurals");
    let include_single_word_synonym = alternatives_as_exact
        .iter()
        .any(|v| v == "singleWordSynonym");
    let include_multi_word_synonym = alternatives_as_exact
        .iter()
        .any(|v| v == "multiWordsSynonym");

    let collect_synonym_candidates = |alternatives: &mut Vec<String>, synonyms: &[String]| {
        for candidate in synonyms {
            let normalized = candidate.trim().to_lowercase();
            if normalized.is_empty() {
                continue;
            }
            let token_count = normalized.split_whitespace().count();
            let should_include = (token_count == 1 && include_single_word_synonym)
                || (token_count > 1 && include_multi_word_synonym);
            if should_include {
                push_unique_terms(alternatives, std::iter::once(normalized));
            }
        }
    };

    query_terms
        .iter()
        .map(|term| {
            let mut alternatives = vec![term.clone()];

            if include_ignore_plurals {
                if let Some(forms) = plural_map.and_then(|m| m.get(term)) {
                    push_unique_terms(&mut alternatives, forms.iter().cloned());
                }
            }

            if include_single_word_synonym || include_multi_word_synonym {
                if let Some(store) = synonym_store {
                    for synonym in store.values() {
                        match synonym {
                            Synonym::Regular { synonyms, .. } => {
                                if synonyms
                                    .iter()
                                    .any(|value| value.eq_ignore_ascii_case(term))
                                {
                                    collect_synonym_candidates(&mut alternatives, synonyms);
                                }
                            }
                            Synonym::OneWay {
                                input, synonyms, ..
                            } => {
                                if input.eq_ignore_ascii_case(term) {
                                    collect_synonym_candidates(&mut alternatives, synonyms);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            alternatives
        })
        .collect()
}

/// TODO: Document find_term_positions.
pub(in crate::index::manager) fn find_term_positions(
    tokens: &[String],
    term: &str,
    is_prefix: bool,
) -> Vec<usize> {
    tokens
        .iter()
        .enumerate()
        .filter(|(_, tok)| {
            if is_prefix {
                tok.starts_with(term)
            } else {
                tok.as_str() == term
            }
        })
        .map(|(pos, _)| pos)
        .collect()
}

/// TODO: Document min_distance_sorted.
pub(in crate::index::manager) fn min_distance_sorted(
    positions_a: &[usize],
    positions_b: &[usize],
) -> u32 {
    if positions_a.is_empty() || positions_b.is_empty() {
        return u32::MAX;
    }

    let mut best = u32::MAX;
    let mut i = 0;
    let mut j = 0;
    while i < positions_a.len() && j < positions_b.len() {
        let dist = (positions_a[i] as i64 - positions_b[j] as i64).unsigned_abs() as u32;
        best = best.min(dist);
        if best == 1 {
            break;
        }
        if positions_a[i] < positions_b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }

    best
}

pub(in crate::index::manager) fn contains_contiguous_subsequence(
    tokens: &[String],
    sequence: &[String],
) -> bool {
    if sequence.is_empty() || sequence.len() > tokens.len() {
        return false;
    }
    tokens
        .windows(sequence.len())
        .any(|window| window.iter().zip(sequence.iter()).all(|(a, b)| a == b))
}

/// Returns 0 if all prefix-eligible terms have exact matches, 1 if any
/// prefix-eligible term's best match is prefix-only. Non-prefix-eligible terms
/// are always treated as "exact" for this tier.
///
/// Exact-match semantics are controlled by `exact_on_single_word_query` (for single-term queries):
///   - `"attribute"` (Algolia default): exact = query matches the ENTIRE attribute value (sole token).
///   - `"word"`: exact = query matches any individual token in any attribute.
///   - `"none"`: exact tier disabled for single-word queries → always returns 0.
///
/// Attributes listed in `disable_exact_on_attributes` are excluded from exact-tier consideration.
/// Only non-disabled attribute tokens participate in both exact and prefix checks here.
pub(in crate::index::manager) fn compute_exact_vs_prefix_bucket(
    query_terms: &[String],
    tokens_by_path: &[(usize, Vec<String>)],
    searchable_paths: &[String],
    prefix_eligible: &[bool],
    term_alternatives: &[Vec<String>],
    exact_on_single_word_query: &str,
    disable_exact_on_attributes: &[String],
) -> u8 {
    let is_single_word = query_terms.len() == 1;

    // "none" mode: exact tier disabled for single-word queries.
    if is_single_word && exact_on_single_word_query == "none" {
        return 0;
    }

    let is_disabled_path = |path: &str| {
        disable_exact_on_attributes
            .iter()
            .any(|d| d == path || path.starts_with(&format!("{}.", d)))
    };

    // Partition tokens once so we can detect "match only on disabled attribute".
    let mut eligible: Vec<&[String]> = Vec::new();
    let mut disabled: Vec<&[String]> = Vec::new();
    for (path_idx, tokens) in tokens_by_path {
        let path = searchable_paths
            .get(*path_idx)
            .map(|s| s.as_str())
            .unwrap_or("");
        if is_disabled_path(path) {
            disabled.push(tokens.as_slice());
        } else {
            eligible.push(tokens.as_slice());
        }
    }

    for (i, query_term) in query_terms.iter().enumerate() {
        let is_prefix_eligible = prefix_eligible.get(i).copied().unwrap_or(false);
        if !is_prefix_eligible {
            continue; // non-prefix terms can't produce prefix-tier distinctions
        }

        let raw_alternatives = term_alternatives
            .get(i)
            .filter(|alternatives| !alternatives.is_empty())
            .map(|alternatives| alternatives.as_slice())
            .unwrap_or_else(|| std::slice::from_ref(query_term));
        let mut single_word_alternatives: Vec<String> = Vec::new();
        let mut multi_word_alternatives: Vec<Vec<String>> = Vec::new();
        for alternative in raw_alternatives {
            let tokens: Vec<String> = alternative
                .split_whitespace()
                .map(|part| part.to_string())
                .collect();
            if tokens.len() <= 1 {
                if let Some(token) = tokens.first() {
                    if !single_word_alternatives.contains(token) {
                        single_word_alternatives.push(token.clone());
                    }
                }
            } else if !multi_word_alternatives.contains(&tokens) {
                multi_word_alternatives.push(tokens);
            }
        }
        if single_word_alternatives.is_empty() {
            single_word_alternatives.push(query_term.clone());
        }

        // Exact-match check — semantics depend on mode and query length.
        let has_exact = if is_single_word && exact_on_single_word_query == "attribute" {
            // Algolia "attribute" mode: the attribute must contain ONLY this single term.
            // For multi-word synonyms, the synonym must cover the entire attribute value.
            eligible.iter().any(|tokens| {
                (tokens.len() == 1
                    && single_word_alternatives
                        .iter()
                        .any(|alternative| tokens[0] == *alternative))
                    || multi_word_alternatives.iter().any(|alternative| {
                        tokens.len() == alternative.len()
                            && contains_contiguous_subsequence(tokens, alternative)
                    })
            })
        } else {
            // "word" mode or multi-word query: any matching token counts.
            eligible.iter().any(|tokens| {
                tokens.iter().any(|token| {
                    single_word_alternatives
                        .iter()
                        .any(|alternative| token == alternative)
                }) || multi_word_alternatives
                    .iter()
                    .any(|alternative| contains_contiguous_subsequence(tokens, alternative))
            })
        };

        if has_exact {
            continue; // exact match found for this term
        }

        // No exact match by the current mode's definition.
        // Check if there's ANY match (word-level exact or prefix) in eligible attributes.
        // In "attribute" mode, a word-level exact match (e.g. "red" in "Red Shoes")
        // is NOT an attribute-level exact, so it still produces tier 1 (non-exact).
        let any_token_set_matches = |token_sets: &[&[String]]| {
            token_sets.iter().any(|tokens| {
                tokens.iter().any(|token| {
                    single_word_alternatives.iter().any(|alternative| {
                        token == alternative || classify_match(alternative, token).1
                    })
                }) || multi_word_alternatives
                    .iter()
                    .any(|alternative| contains_contiguous_subsequence(tokens, alternative))
            })
        };
        if any_token_set_matches(&eligible) {
            return 1; // matched but not "exact" by current mode's standard
        }
        if any_token_set_matches(&disabled) {
            return 1; // only disabled attrs matched: this must not receive exact-tier credit
        }
    }
    0 // all prefix-eligible terms have exact matches (or no prefix-eligible terms)
}

/// Returns the lowest `searchable_paths` index where any query term matches any
/// token, respecting per-term prefix eligibility and typo-length thresholds.
/// Returns `usize::MAX` if no match found in any listed attribute.
pub(in crate::index::manager) struct AttributeRankingConfig<'a> {
    pub prefix_eligible: &'a [bool],
    pub min_word_size_for_1_typo: usize,
    pub min_word_size_for_2_typos: usize,
    pub attribute_criteria_computed_by_min_proximity: bool,
    pub min_proximity: u32,
    pub unordered_path_indexes: &'a HashSet<usize>,
}

/// TODO: Document compute_best_attribute_index.
pub(in crate::index::manager) fn compute_best_attribute_index(
    query_terms: &[String],
    tokens_by_path: &[(usize, Vec<String>)],
    config: &AttributeRankingConfig<'_>,
) -> usize {
    if config.attribute_criteria_computed_by_min_proximity {
        return compute_best_attribute_by_proximity(
            query_terms,
            tokens_by_path,
            config.prefix_eligible,
            config.min_proximity,
            config.unordered_path_indexes,
        );
    }

    let mut best = usize::MAX;
    for &(path_idx, ref tokens) in tokens_by_path {
        if path_idx >= best {
            continue; // can't improve
        }
        for (i, query_term) in query_terms.iter().enumerate() {
            let allow_prefix = config.prefix_eligible.get(i).copied().unwrap_or(false);
            let term_len = query_term.chars().count();
            let max_allowed_typos = max_allowed_typos_for_term_len(
                term_len,
                config.min_word_size_for_1_typo,
                config.min_word_size_for_2_typos,
            );
            let matched = tokens.iter().any(|token| {
                typo_distance_strict(query_term, token, allow_prefix) <= max_allowed_typos
            });
            if matched {
                best = path_idx;
                break; // found match in this path, move to next path
            }
        }
    }
    best
}

/// TODO: Document compute_best_attribute_by_proximity.
pub(in crate::index::manager) fn compute_best_attribute_by_proximity(
    query_terms: &[String],
    tokens_by_path: &[(usize, Vec<String>)],
    prefix_eligible: &[bool],
    min_proximity: u32,
    unordered_path_indexes: &HashSet<usize>,
) -> usize {
    if query_terms.is_empty() {
        return usize::MAX;
    }

    if query_terms.len() == 1 {
        let term = &query_terms[0];
        let is_prefix = prefix_eligible.first().copied().unwrap_or(false);
        let mut best_path = usize::MAX;
        for (path_idx, tokens) in tokens_by_path {
            if !find_term_positions(tokens, term, is_prefix).is_empty() {
                best_path = best_path.min(*path_idx);
            }
        }
        return best_path;
    }

    let mut best_path = usize::MAX;
    let mut best_score = u32::MAX;

    for (path_idx, tokens) in tokens_by_path {
        let mut attr_total = 0u32;
        let mut has_any_pair = false;

        for pair_idx in 0..query_terms.len() - 1 {
            let term_a = &query_terms[pair_idx];
            let term_b = &query_terms[pair_idx + 1];
            let a_is_prefix = prefix_eligible.get(pair_idx).copied().unwrap_or(false);
            let b_is_prefix = prefix_eligible.get(pair_idx + 1).copied().unwrap_or(false);
            let positions_a = find_term_positions(tokens, term_a, a_is_prefix);
            let positions_b = find_term_positions(tokens, term_b, b_is_prefix);
            let pair_distance = min_distance_sorted(&positions_a, &positions_b);
            if pair_distance == u32::MAX {
                continue;
            }
            has_any_pair = true;
            let effective_distance = if unordered_path_indexes.contains(path_idx) {
                min_proximity
            } else {
                pair_distance.max(min_proximity)
            };
            attr_total = attr_total.saturating_add(effective_distance);
        }

        if !has_any_pair {
            continue;
        }

        if attr_total < best_score || (attr_total == best_score && *path_idx < best_path) {
            best_score = attr_total;
            best_path = *path_idx;
        }
    }

    best_path
}

// Tantivy BM25 constants in our fork are fixed at k1=1.2, b=0.75.
// Stage 2 tunes only length normalization behavior for short product fields.
const BM25_K1: f32 = 1.2;
const BM25_SOURCE_B: f32 = 0.75;
const BM25_TARGET_B_SHORT_FIELDS: f32 = 0.4;

pub(in crate::index::manager) fn bm25_length_normalization_denominator(
    doc_len_tokens: usize,
    avg_doc_len_tokens: f32,
    b: f32,
) -> f32 {
    if avg_doc_len_tokens <= 0.0 {
        return 1.0;
    }
    let dl = doc_len_tokens.max(1) as f32;
    1.0 + BM25_K1 * (1.0 - b + b * (dl / avg_doc_len_tokens))
}

/// TODO: Document bm25_short_field_correction_factor.
pub(in crate::index::manager) fn bm25_short_field_correction_factor(
    doc_len_tokens: usize,
    avg_doc_len_tokens: f32,
) -> f32 {
    let source =
        bm25_length_normalization_denominator(doc_len_tokens, avg_doc_len_tokens, BM25_SOURCE_B);
    let target = bm25_length_normalization_denominator(
        doc_len_tokens,
        avg_doc_len_tokens,
        BM25_TARGET_B_SHORT_FIELDS,
    );
    if target <= 0.0 {
        1.0
    } else {
        source / target
    }
}

pub(in crate::index::manager) fn tune_bm25_score_for_short_fields(
    raw_score: f32,
    doc_len_tokens: usize,
    avg_doc_len_tokens: f32,
) -> f32 {
    raw_score * bm25_short_field_correction_factor(doc_len_tokens, avg_doc_len_tokens)
}

pub(in crate::index::manager) fn max_allowed_typos_for_term_len(
    term_len: usize,
    min_word_size_for_1_typo: usize,
    min_word_size_for_2_typos: usize,
) -> usize {
    if term_len >= min_word_size_for_2_typos {
        2
    } else if term_len >= min_word_size_for_1_typo {
        1
    } else {
        0
    }
}
