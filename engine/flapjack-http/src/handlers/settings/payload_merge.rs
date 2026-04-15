use axum::http::StatusCode;
use std::collections::HashMap;

use super::SetSettingsRequest;
use flapjack::index::replica::{validate_replicas, ReplicaEntry};
use flapjack::index::settings::{
    detect_embedder_changes, DistinctValue, EmbedderChange, IndexMode, IndexSettings,
};

/// Merge a partial settings payload into the current settings state, returning
/// any validated replica entries that need follow-up link maintenance.
pub(super) fn merge_settings_payload(
    settings: &mut IndexSettings,
    mut payload: SetSettingsRequest,
    index_name: &str,
) -> Result<Option<Vec<ReplicaEntry>>, (StatusCode, String)> {
    if let Some(distinct_value) = parse_distinct_value(payload.distinct.take()) {
        settings.distinct = Some(distinct_value);
    }

    apply_search_config_fields(settings, &mut payload);
    apply_response_and_display_fields(settings, &mut payload);
    apply_embedders_update(settings, payload.embedders.take());

    let validated_replicas =
        validate_and_apply_replicas(settings, payload.replicas.take(), index_name)?;
    warn_neural_without_embedders(settings);
    Ok(validated_replicas)
}

/// Log embedder additions, removals, and mutations after a settings merge.
pub(super) fn log_embedder_changes(
    old: &Option<HashMap<String, serde_json::Value>>,
    settings: &IndexSettings,
) {
    for change in detect_embedder_changes(old, &settings.embedders) {
        match change {
            EmbedderChange::Modified(name) => tracing::warn!(
                "embedder '{}' configuration changed; existing vectors may be stale",
                name
            ),
            EmbedderChange::Removed(name) => tracing::warn!(
                "embedder '{}' removed; associated vectors will be orphaned",
                name
            ),
            EmbedderChange::Added(name) => tracing::info!("embedder '{}' configured", name),
        }
    }
}

/// Applies search-behavior fields from a settings payload to the index settings.
///
/// Covers fields that affect indexing, ranking, query interpretation, and search mode (15 fields).
/// Each field is consumed via `.take()` so the caller retains ownership of remaining fields.
fn apply_search_config_fields(settings: &mut IndexSettings, payload: &mut SetSettingsRequest) {
    if let Some(v) = payload.attributes_for_faceting.take() {
        settings.attributes_for_faceting = v;
    }
    if let Some(v) = payload.searchable_attributes.take() {
        settings.searchable_attributes = Some(v);
    }
    if let Some(v) = payload.ranking.take() {
        settings.ranking = Some(v);
    }
    if let Some(v) = payload.custom_ranking.take() {
        settings.custom_ranking = Some(v);
    }
    if let Some(v) = payload.pagination_limited_to.take() {
        settings.pagination_limited_to = v;
    }
    if let Some(v) = payload.remove_stop_words.take() {
        settings.remove_stop_words = v;
    }
    if let Some(v) = payload.ignore_plurals.take() {
        settings.ignore_plurals = v;
    }
    if let Some(v) = payload.query_languages.take() {
        settings.query_languages = v;
    }
    if let Some(v) = payload.query_type.take() {
        settings.query_type = v;
    }
    if let Some(v) = payload.numeric_attributes_for_filtering.take() {
        settings.numeric_attributes_for_filtering = Some(v);
    }
    if let Some(v) = payload.allow_compression_of_integer_array.take() {
        settings.allow_compression_of_integer_array = Some(v);
    }
    if let Some(v) = payload.relevancy_strictness.take() {
        settings.relevancy_strictness = Some(v);
    }
    if let Some(v) = payload.mode.take() {
        settings.mode = Some(v);
    }
    if let Some(v) = payload.semantic_search.take() {
        settings.semantic_search = Some(v);
    }
    if let Some(v) = payload.enable_personalization.take() {
        settings.enable_personalization = Some(v);
    }
}

/// Applies response-shape and display-behavior fields from a settings payload.
///
/// Covers fields that control attribute retrieval, highlighting, snippeting,
/// proximity tuning, typo tolerance overrides, and miscellaneous display options (17 fields).
/// Each field is consumed via `.take()` so the caller retains ownership of remaining fields.
fn apply_response_and_display_fields(
    settings: &mut IndexSettings,
    payload: &mut SetSettingsRequest,
) {
    if let Some(v) = payload.attributes_to_retrieve.take() {
        settings.attributes_to_retrieve = Some(v);
    }
    if let Some(v) = payload.unretrievable_attributes.take() {
        settings.unretrievable_attributes = Some(v);
    }
    if let Some(v) = payload.attribute_for_distinct.take() {
        settings.attribute_for_distinct = Some(v);
    }
    if let Some(v) = payload.rendering_content.take() {
        settings.rendering_content = Some(v);
    }
    if let Some(v) = payload.user_data.take() {
        settings.user_data = Some(v);
    }
    if let Some(v) = payload.enable_rules.take() {
        settings.enable_rules = Some(v);
    }
    if let Some(v) = payload.advanced_syntax_features.take() {
        settings.advanced_syntax_features = Some(v);
    }
    if let Some(v) = payload.sort_facet_values_by.take() {
        settings.sort_facet_values_by = Some(v);
    }
    if let Some(v) = payload.snippet_ellipsis_text.take() {
        settings.snippet_ellipsis_text = Some(v);
    }
    if let Some(v) = payload.restrict_highlight_and_snippet_arrays.take() {
        settings.restrict_highlight_and_snippet_arrays = Some(v);
    }
    if let Some(v) = payload.min_proximity.take() {
        settings.min_proximity = Some(v);
    }
    if let Some(v) = payload.disable_exact_on_attributes.take() {
        settings.disable_exact_on_attributes = Some(v);
    }
    if let Some(v) = payload.replace_synonyms_in_highlight.take() {
        settings.replace_synonyms_in_highlight = Some(v);
    }
    if let Some(v) = payload.attribute_criteria_computed_by_min_proximity.take() {
        settings.attribute_criteria_computed_by_min_proximity = Some(v);
    }
    if let Some(v) = payload.enable_re_ranking.take() {
        settings.enable_re_ranking = Some(v);
    }
    if let Some(v) = payload.disable_typo_tolerance_on_words.take() {
        settings.disable_typo_tolerance_on_words = Some(v);
    }
    if let Some(v) = payload.disable_typo_tolerance_on_attributes.take() {
        settings.disable_typo_tolerance_on_attributes = Some(v);
    }
}

fn parse_distinct_value(raw: Option<serde_json::Value>) -> Option<DistinctValue> {
    raw.and_then(|value| match value {
        serde_json::Value::Bool(is_distinct) => Some(DistinctValue::Bool(is_distinct)),
        serde_json::Value::Number(number) => number
            .as_u64()
            .map(|distinct_count| DistinctValue::Integer(distinct_count as u32)),
        _ => None,
    })
}

fn apply_embedders_update(
    settings: &mut IndexSettings,
    embedders: Option<HashMap<String, serde_json::Value>>,
) {
    if let Some(map) = embedders {
        let filtered: HashMap<String, serde_json::Value> = map
            .into_iter()
            .filter(|(_, value)| !value.is_null())
            .collect();
        settings.embedders = (!filtered.is_empty()).then_some(filtered);
    }
}

/// Validates replica index names (must differ from the primary and use valid `virtual()` syntax) and returns parsed replica entries for creation.
fn validate_and_apply_replicas(
    settings: &mut IndexSettings,
    replicas: Option<Vec<String>>,
    index_name: &str,
) -> Result<Option<Vec<ReplicaEntry>>, (StatusCode, String)> {
    let replicas = match replicas {
        Some(replicas) => replicas,
        None => return Ok(None),
    };
    if replicas.is_empty() {
        settings.replicas = None;
        return Ok(Some(Vec::new()));
    }
    let parsed = validate_replicas(index_name, &replicas)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    settings.replicas = Some(replicas);
    Ok(Some(parsed))
}

fn warn_neural_without_embedders(settings: &IndexSettings) {
    if settings.mode == Some(IndexMode::NeuralSearch) && settings.embedders.is_none() {
        tracing::warn!(
            "mode set to neuralSearch but no embedders configured; hybrid search will fall back to keyword-only until embedders are added"
        );
    }
}
