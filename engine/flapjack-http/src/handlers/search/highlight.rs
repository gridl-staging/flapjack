use std::collections::HashMap;

use flapjack::query::highlighter::{
    HighlightResult, HighlightValue, MatchLevel, SnippetResult, SnippetValue,
};

pub(super) fn highlight_value_map_to_json(
    map: &HashMap<String, HighlightValue>,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in map {
        obj.insert(k.clone(), highlight_value_to_json(v));
    }
    serde_json::Value::Object(obj)
}

pub(super) fn highlight_value_to_json(value: &HighlightValue) -> serde_json::Value {
    match value {
        HighlightValue::Single(result) => serde_json::to_value(result).unwrap(),
        HighlightValue::Array(results) => serde_json::Value::Array(
            results
                .iter()
                .map(|r| serde_json::to_value(r).unwrap())
                .collect(),
        ),
        HighlightValue::Object(map) => highlight_value_map_to_json(map),
    }
}

pub(super) fn snippet_value_map_to_json(map: &HashMap<String, SnippetValue>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in map {
        obj.insert(k.clone(), snippet_value_to_json(v));
    }
    serde_json::Value::Object(obj)
}

pub(super) fn snippet_value_to_json(value: &SnippetValue) -> serde_json::Value {
    match value {
        SnippetValue::Single(result) => serde_json::to_value(result).unwrap(),
        SnippetValue::Array(results) => serde_json::Value::Array(
            results
                .iter()
                .map(|r| serde_json::to_value(r).unwrap())
                .collect(),
        ),
        SnippetValue::Object(map) => snippet_value_map_to_json(map),
    }
}

/// Filter highlight array values to only include elements with a match (matchLevel != "none").
/// Used when restrictHighlightAndSnippetArrays is true.
pub(super) fn restrict_highlight_array(value: HighlightValue) -> HighlightValue {
    match value {
        HighlightValue::Array(results) => {
            let filtered: Vec<HighlightResult> = results
                .into_iter()
                .filter(|r| !matches!(r.match_level, MatchLevel::None))
                .collect();
            // If no matches, return empty array (Algolia behavior)
            HighlightValue::Array(filtered)
        }
        HighlightValue::Object(map) => {
            let updated = map
                .into_iter()
                .map(|(k, v)| (k, restrict_highlight_array(v)))
                .collect();
            HighlightValue::Object(updated)
        }
        other => other,
    }
}

/// Filter snippet array values to only include elements with a match (matchLevel != "none").
pub(super) fn restrict_snippet_array(value: SnippetValue) -> SnippetValue {
    match value {
        SnippetValue::Array(results) => {
            let filtered: Vec<SnippetResult> = results
                .into_iter()
                .filter(|r| !matches!(r.match_level, MatchLevel::None))
                .collect();
            SnippetValue::Array(filtered)
        }
        SnippetValue::Object(map) => {
            let updated = map
                .into_iter()
                .map(|(k, v)| (k, restrict_snippet_array(v)))
                .collect();
            SnippetValue::Object(updated)
        }
        other => other,
    }
}

/// Collect facet values from a FieldValue into a counts map.
/// Handles Text, Integer, Float, and Array variants for facetingAfterDistinct recomputation.
pub(super) fn collect_facet_values(
    fv: &flapjack::types::FieldValue,
    counts: &mut HashMap<String, u64>,
) {
    match fv {
        flapjack::types::FieldValue::Text(s) => {
            *counts.entry(s.clone()).or_insert(0) += 1;
        }
        flapjack::types::FieldValue::Integer(i) => {
            *counts.entry(i.to_string()).or_insert(0) += 1;
        }
        flapjack::types::FieldValue::Float(f) => {
            *counts.entry(f.to_string()).or_insert(0) += 1;
        }
        flapjack::types::FieldValue::Array(arr) => {
            for item in arr {
                collect_facet_values(item, counts);
            }
        }
        _ => {}
    }
}
