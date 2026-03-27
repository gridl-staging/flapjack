use std::collections::{HashMap, HashSet};

use flapjack::query::highlighter::{HighlightValue, MatchLevel};

/// Map synonym-matched words back to their original query terms.
/// This implements Algolia's replaceSynonymsInHighlight=false behavior:
/// - matchedWords shows original query terms (e.g., "notebook")
/// - Highlighted text shows document words (e.g., "laptop")
/// - Only replaces words that are actually synonyms, preserving partial matches
pub(super) fn map_synonym_matches(
    value: HighlightValue,
    original_query_words: &[String],
    synonym_map: &HashMap<String, HashSet<String>>,
) -> HighlightValue {
    match value {
        HighlightValue::Single(mut result) => {
            if !result.matched_words.is_empty() {
                let mut mapped_words = HashSet::new();

                // For each matched word, check if it's a synonym and map back to original
                for matched in &result.matched_words {
                    let matched_lower = matched.to_lowercase();
                    let mut found_original = false;

                    // Check if this matched word is a synonym of any original query word
                    for original in original_query_words {
                        let original_lower = original.to_lowercase();
                        if let Some(synonyms) = synonym_map.get(&original_lower) {
                            if synonyms.contains(&matched_lower) || matched_lower == original_lower
                            {
                                mapped_words.insert(original_lower);
                                found_original = true;
                                break;
                            }
                        }
                    }

                    // If not a synonym, keep the original matched word
                    if !found_original {
                        mapped_words.insert(matched_lower);
                    }
                }

                result.matched_words = mapped_words.into_iter().collect();

                // Update matchLevel based on original query coverage
                if result.matched_words.len() == original_query_words.len() {
                    result.match_level = MatchLevel::Full;
                } else if !result.matched_words.is_empty() {
                    result.match_level = MatchLevel::Partial;
                }
            }
            HighlightValue::Single(result)
        }
        HighlightValue::Array(results) => {
            let updated = results
                .into_iter()
                .map(|r| {
                    if let HighlightValue::Single(s) = map_synonym_matches(
                        HighlightValue::Single(r),
                        original_query_words,
                        synonym_map,
                    ) {
                        s
                    } else {
                        unreachable!()
                    }
                })
                .collect();
            HighlightValue::Array(updated)
        }
        HighlightValue::Object(map) => {
            let updated = map
                .into_iter()
                .map(|(k, v)| (k, map_synonym_matches(v, original_query_words, synonym_map)))
                .collect();
            HighlightValue::Object(updated)
        }
    }
}
