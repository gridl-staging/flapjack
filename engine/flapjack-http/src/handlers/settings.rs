//! Stub summary for settings.rs.
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::replicas::{
    clear_removed_replica_primary_links, is_virtual_settings_only_index,
    persist_replica_primary_links,
};
use super::AppState;
use crate::error_response::HandlerError;
use crate::extractors::ValidatedIndexName;
use flapjack::index::replica::validate_replicas;
use flapjack::index::settings::{
    detect_embedder_changes, DistinctValue, EmbedderChange, IndexMode, IndexSettings,
    SemanticSearchSettings,
};

/// Deserializable payload for the `POST /1/indexes/{indexName}/settings` endpoint.
///
/// All fields are optional; only supplied fields are merged into the existing `IndexSettings`.
/// Unrecognized keys are captured in `other` via `#[serde(flatten)]` and reported as unsupported.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SetSettingsRequest {
    #[serde(rename = "attributesForFaceting")]
    pub attributes_for_faceting: Option<Vec<String>>,

    #[serde(rename = "searchableAttributes")]
    pub searchable_attributes: Option<Vec<String>>,

    #[serde(rename = "ranking")]
    pub ranking: Option<Vec<String>>,

    #[serde(rename = "customRanking")]
    pub custom_ranking: Option<Vec<String>>,

    #[serde(rename = "attributesToRetrieve")]
    pub attributes_to_retrieve: Option<Vec<String>>,

    #[serde(rename = "unretrievableAttributes")]
    pub unretrievable_attributes: Option<Vec<String>>,

    #[serde(rename = "paginationLimitedTo")]
    pub pagination_limited_to: Option<u32>,

    #[serde(rename = "attributeForDistinct")]
    pub attribute_for_distinct: Option<String>,

    pub distinct: Option<serde_json::Value>,

    #[serde(rename = "removeStopWords")]
    pub remove_stop_words: Option<flapjack::query::stopwords::RemoveStopWordsValue>,

    #[serde(rename = "ignorePlurals")]
    pub ignore_plurals: Option<flapjack::query::plurals::IgnorePluralsValue>,

    #[serde(rename = "queryLanguages")]
    pub query_languages: Option<Vec<String>>,

    #[serde(rename = "queryType")]
    pub query_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedders: Option<HashMap<String, serde_json::Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<IndexMode>,

    #[serde(rename = "semanticSearch", skip_serializing_if = "Option::is_none")]
    pub semantic_search: Option<SemanticSearchSettings>,

    #[serde(
        rename = "enablePersonalization",
        skip_serializing_if = "Option::is_none"
    )]
    pub enable_personalization: Option<bool>,

    #[serde(rename = "renderingContent", skip_serializing_if = "Option::is_none")]
    pub rendering_content: Option<serde_json::Value>,

    #[serde(rename = "userData", skip_serializing_if = "Option::is_none")]
    pub user_data: Option<serde_json::Value>,

    #[serde(rename = "enableRules", skip_serializing_if = "Option::is_none")]
    pub enable_rules: Option<bool>,

    #[serde(
        rename = "advancedSyntaxFeatures",
        skip_serializing_if = "Option::is_none"
    )]
    pub advanced_syntax_features: Option<Vec<String>>,

    #[serde(rename = "sortFacetValuesBy", skip_serializing_if = "Option::is_none")]
    pub sort_facet_values_by: Option<String>,

    #[serde(
        rename = "snippetEllipsisText",
        skip_serializing_if = "Option::is_none"
    )]
    pub snippet_ellipsis_text: Option<String>,

    #[serde(
        rename = "restrictHighlightAndSnippetArrays",
        skip_serializing_if = "Option::is_none"
    )]
    pub restrict_highlight_and_snippet_arrays: Option<bool>,

    #[serde(rename = "minProximity", skip_serializing_if = "Option::is_none")]
    pub min_proximity: Option<u32>,

    #[serde(
        rename = "disableExactOnAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_exact_on_attributes: Option<Vec<String>>,

    #[serde(
        rename = "replaceSynonymsInHighlight",
        skip_serializing_if = "Option::is_none"
    )]
    pub replace_synonyms_in_highlight: Option<bool>,

    #[serde(
        rename = "attributeCriteriaComputedByMinProximity",
        skip_serializing_if = "Option::is_none"
    )]
    pub attribute_criteria_computed_by_min_proximity: Option<bool>,

    #[serde(rename = "enableReRanking", skip_serializing_if = "Option::is_none")]
    pub enable_re_ranking: Option<bool>,

    #[serde(
        rename = "disableTypoToleranceOnWords",
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_typo_tolerance_on_words: Option<Vec<String>>,

    #[serde(
        rename = "disableTypoToleranceOnAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub disable_typo_tolerance_on_attributes: Option<Vec<String>>,

    #[serde(rename = "replicas", skip_serializing_if = "Option::is_none")]
    pub replicas: Option<Vec<String>>,

    #[serde(
        rename = "numericAttributesForFiltering",
        alias = "numericAttributesToIndex",
        skip_serializing_if = "Option::is_none"
    )]
    pub numeric_attributes_for_filtering: Option<Vec<String>>,

    #[serde(
        rename = "allowCompressionOfIntegerArray",
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_compression_of_integer_array: Option<bool>,

    #[serde(
        rename = "relevancyStrictness",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub relevancy_strictness: Option<u32>,

    #[serde(flatten)]
    pub other: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SetSettingsResponse {
    #[serde(rename = "updatedAt")]
    pub updated_at: String,

    #[serde(rename = "taskID")]
    pub task_id: i64,

    #[serde(rename = "unsupportedParams", skip_serializing_if = "Option::is_none")]
    pub unsupported_params: Option<Vec<String>>,
}

fn settings_file_path(base_path: &Path, index_name: &str) -> PathBuf {
    base_path.join(index_name).join("settings.json")
}

fn load_settings_or_default(settings_path: &Path) -> Result<IndexSettings, HandlerError> {
    if settings_path.exists() {
        // Persisted settings parse failures are internal corruption and must be 500s.
        IndexSettings::load(settings_path).map_err(|error| match error {
            flapjack::error::FlapjackError::Json(parse_error) => HandlerError::from(format!(
                "Failed to parse persisted settings file '{}': {}",
                settings_path.display(),
                parse_error
            )),
            other => HandlerError::from(other.to_string()),
        })
    } else {
        Ok(IndexSettings::default())
    }
}

fn unsupported_settings_params(payload: &SetSettingsRequest) -> Vec<String> {
    let mut unsupported = Vec::new();
    if let Some(other) = &payload.other {
        unsupported.extend(other.keys().cloned());
    }
    unsupported
}

/// Parse an optional boolean query parameter, defaulting to `false` when absent.
pub(super) fn parse_bool_query_param(
    query_params: &HashMap<String, String>,
    key: &str,
) -> Result<bool, HandlerError> {
    query_params
        .get(key)
        .map(|value| {
            value
                .parse::<bool>()
                .map_err(|_| HandlerError::bad_request(format!("{key} must be 'true' or 'false'")))
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

/// Update the settings for the specified index.
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/settings",
    tag = "settings",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = SetSettingsRequest, description = "Settings to update"),
    responses(
        (status = 200, description = "Settings updated successfully", body = SetSettingsResponse),
        (status = 400, description = "Invalid settings")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn set_settings(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
    Query(query_params): Query<HashMap<String, String>>,
    Json(payload): Json<SetSettingsRequest>,
) -> Result<impl IntoResponse, HandlerError> {
    let is_virtual_settings_only = is_virtual_settings_only_index(&state, &index_name);
    if payload.relevancy_strictness.is_some() && !is_virtual_settings_only {
        return Err(HandlerError::bad_request(
            "relevancyStrictness can only be set on virtual replica indices".to_string(),
        ));
    }

    let forward_to_replicas = parse_bool_query_param(&query_params, "forwardToReplicas")?;
    if !is_virtual_settings_only {
        state
            .manager
            .create_tenant(&index_name)
            .map_err(|error| HandlerError::from(error.to_string()))?;
    }

    let settings_path = settings_file_path(&state.manager.base_path, &index_name);
    let unsupported = unsupported_settings_params(&payload);
    let attributes_for_faceting_provided = payload.attributes_for_faceting.is_some();
    let query_languages_provided = payload.query_languages.is_some();
    #[cfg(feature = "vector-search")]
    let embedders_updated = payload.embedders.is_some();

    let mut settings = load_settings_or_default(&settings_path)?;
    let previous_settings = settings.clone();
    let previous_replicas = settings.replicas.clone();
    let old_embedders = settings.embedders.clone();

    let validated_replicas = merge_settings_payload(&mut settings, payload, &index_name)
        .map_err(|(status, message)| HandlerError::Custom { status, message })?;
    settings.restore_redacted_response_secrets(&previous_settings);
    settings
        .validate_embedders()
        .map_err(HandlerError::bad_request)?;
    log_embedder_changes(&old_embedders, &settings);

    save_settings(&settings, &settings_path)?;
    state.manager.invalidate_settings_cache(&index_name);
    state.manager.invalidate_facet_cache(&index_name);

    if let Some(ref replicas) = validated_replicas {
        clear_removed_replica_primary_links(
            &state,
            &index_name,
            previous_replicas.as_deref(),
            replicas,
        )
        .map_err(|error| HandlerError::from(error.to_string()))?;
        persist_replica_primary_links(&state, &index_name, replicas)
            .map_err(|error| HandlerError::from(error.to_string()))?;
    }

    if forward_to_replicas {
        forward_settings_to_replicas(
            &state,
            &settings,
            attributes_for_faceting_provided,
            query_languages_provided,
        )
        .map_err(|error| HandlerError::from(error.to_string()))?;
    }

    #[cfg(feature = "vector-search")]
    if embedders_updated {
        state.embedder_store.invalidate(&index_name);
    }

    state.manager.append_oplog(
        &index_name,
        "settings",
        serde_json::to_value(&settings).unwrap_or_default(),
    );

    let noop_task = state
        .manager
        .make_noop_task(&index_name)
        .map_err(|error| HandlerError::from(error.to_string()))?;
    let response = SetSettingsResponse {
        updated_at: chrono::Utc::now().to_rfc3339(),
        task_id: noop_task.numeric_id,
        unsupported_params: if unsupported.is_empty() {
            None
        } else {
            Some(unsupported)
        },
    };

    let status = if response.unsupported_params.is_some() {
        StatusCode::MULTI_STATUS
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
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

/// TODO: Document merge_settings_payload.
fn merge_settings_payload(
    settings: &mut IndexSettings,
    mut payload: SetSettingsRequest,
    index_name: &str,
) -> Result<Option<Vec<flapjack::index::replica::ReplicaEntry>>, (StatusCode, String)> {
    let distinct_value = parse_distinct_value(payload.distinct.take());
    if let Some(dv) = distinct_value {
        settings.distinct = Some(dv);
    }

    apply_search_config_fields(settings, &mut payload);
    apply_response_and_display_fields(settings, &mut payload);
    apply_embedders_update(settings, payload.embedders.take());

    let validated_replicas =
        validate_and_apply_replicas(settings, payload.replicas.take(), index_name)?;
    warn_neural_without_embedders(settings);
    Ok(validated_replicas)
}

#[utoipa::path(
    put,
    path = "/1/indexes/{indexName}/settings",
    tag = "settings",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = SetSettingsRequest, description = "Settings to update"),
    responses(
        (status = 200, description = "Settings updated successfully", body = SetSettingsResponse),
        (status = 400, description = "Invalid settings")
    ),
    security(
        ("api_key" = [])
    )
)]
#[allow(dead_code)]
pub(crate) async fn set_settings_put_doc() {}

fn parse_distinct_value(raw: Option<serde_json::Value>) -> Option<DistinctValue> {
    raw.and_then(|v| match v {
        serde_json::Value::Bool(b) => Some(DistinctValue::Bool(b)),
        serde_json::Value::Number(n) => n.as_u64().map(|u| DistinctValue::Integer(u as u32)),
        _ => None,
    })
}

fn apply_embedders_update(
    settings: &mut IndexSettings,
    embedders: Option<HashMap<String, serde_json::Value>>,
) {
    if let Some(map) = embedders {
        let filtered: HashMap<String, serde_json::Value> =
            map.into_iter().filter(|(_, v)| !v.is_null()).collect();
        settings.embedders = if filtered.is_empty() {
            None
        } else {
            Some(filtered)
        };
    }
}

/// Validates replica index names (must differ from the primary and use valid `virtual()` syntax) and returns parsed replica entries for creation.
fn validate_and_apply_replicas(
    settings: &mut IndexSettings,
    replicas: Option<Vec<String>>,
    index_name: &str,
) -> Result<Option<Vec<flapjack::index::replica::ReplicaEntry>>, (StatusCode, String)> {
    let replicas = match replicas {
        Some(r) => r,
        None => return Ok(None),
    };
    if replicas.is_empty() {
        settings.replicas = None;
        return Ok(Some(Vec::new()));
    }
    let parsed = validate_replicas(index_name, &replicas)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
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

/// TODO: Document log_embedder_changes.
fn log_embedder_changes(
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

fn save_settings(settings: &IndexSettings, path: &Path) -> Result<(), HandlerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| HandlerError::from(error.to_string()))?;
    }
    settings.save(path).map_err(|error| match error {
        flapjack::error::FlapjackError::Json(serialize_error) => HandlerError::from(format!(
            "Failed to serialize settings file '{}': {}",
            path.display(),
            serialize_error
        )),
        other => HandlerError::from(other.to_string()),
    })
}

/// Forward the current settings from a primary index to all its configured replicas.
/// Preserves each replica's `primary` field (system-managed) while merging forwarded settings.
fn forward_settings_to_replicas(
    state: &Arc<AppState>,
    primary_settings: &IndexSettings,
    attributes_for_faceting_provided: bool,
    query_languages_provided: bool,
) -> Result<(), flapjack::error::FlapjackError> {
    use flapjack::index::replica::parse_replica_entry;

    let replicas = match &primary_settings.replicas {
        Some(r) => r,
        None => return Ok(()),
    };

    for replica_str in replicas {
        let parsed = parse_replica_entry(replica_str)?;
        let replica_name = parsed.name();

        let settings_path = settings_file_path(&state.manager.base_path, replica_name);
        if !settings_path.exists() {
            continue;
        }

        let mut replica_settings = IndexSettings::load(&settings_path)?;
        let preserved_primary = replica_settings.primary.clone();
        let preserved_replicas = replica_settings.replicas.clone();

        // Merge forwarded fields from primary (skip replicas and primary — those are index-specific)
        if let Some(ref sa) = primary_settings.searchable_attributes {
            replica_settings.searchable_attributes = Some(sa.clone());
        }
        if let Some(ref cr) = primary_settings.custom_ranking {
            replica_settings.custom_ranking = Some(cr.clone());
        }
        if attributes_for_faceting_provided {
            replica_settings.attributes_for_faceting =
                primary_settings.attributes_for_faceting.clone();
        }
        if let Some(ref atr) = primary_settings.attributes_to_retrieve {
            replica_settings.attributes_to_retrieve = Some(atr.clone());
        }
        if let Some(ref ua) = primary_settings.unretrievable_attributes {
            replica_settings.unretrievable_attributes = Some(ua.clone());
        }
        if let Some(ref afd) = primary_settings.attribute_for_distinct {
            replica_settings.attribute_for_distinct = Some(afd.clone());
        }
        if let Some(ref d) = primary_settings.distinct {
            replica_settings.distinct = Some(d.clone());
        }
        if let Some(ref rc) = primary_settings.rendering_content {
            replica_settings.rendering_content = Some(rc.clone());
        }
        if let Some(ref emb) = primary_settings.embedders {
            replica_settings.embedders = Some(emb.clone());
        }
        if let Some(ref mode) = primary_settings.mode {
            replica_settings.mode = Some(mode.clone());
        }
        if let Some(ref ss) = primary_settings.semantic_search {
            replica_settings.semantic_search = Some(ss.clone());
        }
        if query_languages_provided {
            replica_settings.query_languages = primary_settings.query_languages.clone();
        }
        if let Some(ref naf) = primary_settings.numeric_attributes_for_filtering {
            replica_settings.numeric_attributes_for_filtering = Some(naf.clone());
        }

        // Restore system-managed fields
        replica_settings.primary = preserved_primary;
        replica_settings.replicas = preserved_replicas;

        replica_settings.save(&settings_path)?;
        state.manager.invalidate_settings_cache(replica_name);
        state.manager.invalidate_facet_cache(replica_name);
    }
    Ok(())
}

/// Get index settings
#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}/settings",
    tag = "settings",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "Index settings", body = serde_json::Value),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
) -> Result<impl IntoResponse, HandlerError> {
    let settings_path = settings_file_path(&state.manager.base_path, &index_name);
    let settings = load_settings_or_default(&settings_path)?;

    Ok(Json(settings.redacted_for_response()))
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
