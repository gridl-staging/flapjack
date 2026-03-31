//! Stub summary for migration.rs.
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::AppState;
use crate::error_response::json_error_parts;
use flapjack::index::rules::{Rule, RuleStore};
use flapjack::index::settings::IndexSettings;
use flapjack::index::synonyms::{Synonym, SynonymStore};
use flapjack::types::Document;

/// Request payload for migrating an index from Algolia to Flapjack.
///
/// Contains Algolia credentials and the source index name. When `target_index` is
/// omitted the source index name is reused. Set `overwrite` to replace an existing
/// target index; otherwise a 409 Conflict is returned.
#[derive(Debug, Deserialize, ToSchema)]
pub struct MigrateFromAlgoliaRequest {
    #[serde(rename = "appId")]
    pub app_id: String,

    #[serde(rename = "apiKey")]
    pub api_key: String,

    #[serde(rename = "sourceIndex")]
    pub source_index: String,

    #[serde(rename = "targetIndex")]
    pub target_index: Option<String>,

    /// If true, delete any existing target index before migrating.
    /// Without this, migrating to an existing index returns 409.
    #[serde(default)]
    pub overwrite: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MigrateFromAlgoliaResponse {
    pub status: String,
    pub settings: bool,
    pub synonyms: MigrateCount,
    pub rules: MigrateCount,
    pub objects: MigrateCount,
    #[serde(rename = "taskID")]
    pub task_id: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MigrateCount {
    pub imported: usize,
}

fn algolia_host(app_id: &str) -> String {
    format!("{}-dsn.algolia.net", app_id)
}

fn algolia_url(app_id: &str, path: &str) -> String {
    format!("https://{}{}", algolia_host(app_id), path)
}

fn algolia_headers(app_id: &str, api_key: &str) -> Vec<(&'static str, String)> {
    vec![
        ("x-algolia-application-id", app_id.to_string()),
        ("x-algolia-api-key", api_key.to_string()),
        ("content-type", "application/json".to_string()),
    ]
}

/// Send an authenticated GET request to the Algolia REST API.
///
/// # Arguments
///
/// * `client` - Shared HTTP client.
/// * `app_id` - Algolia application ID, used for host resolution and auth headers.
/// * `api_key` - Algolia admin API key.
/// * `path` - API path including the leading slash (e.g. `/1/indexes`).
///
/// # Returns
///
/// The parsed JSON response body, or a human-readable error string on network
/// failure, non-2xx status, or deserialization failure.
async fn algolia_get(
    client: &reqwest::Client,
    app_id: &str,
    api_key: &str,
    path: &str,
) -> Result<serde_json::Value, String> {
    let url = algolia_url(app_id, path);
    let mut req = client.get(&url);
    for (k, v) in algolia_headers(app_id, api_key) {
        req = req.header(k, v);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Algolia request failed: {}", e))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Algolia returned {}: {}", status, body));
    }
    resp.json()
        .await
        .map_err(|e| format!("Failed to parse Algolia response: {}", e))
}

/// Send an authenticated POST request with a JSON body to the Algolia REST API.
///
/// # Arguments
///
/// * `client` - Shared HTTP client.
/// * `app_id` - Algolia application ID, used for host resolution and auth headers.
/// * `api_key` - Algolia admin API key.
/// * `path` - API path including the leading slash.
/// * `body` - JSON value sent as the request body.
///
/// # Returns
///
/// The parsed JSON response body, or a human-readable error string on network
/// failure, non-2xx status, or deserialization failure.
async fn algolia_post(
    client: &reqwest::Client,
    app_id: &str,
    api_key: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = algolia_url(app_id, path);
    let mut req = client.post(&url).json(body);
    for (k, v) in algolia_headers(app_id, api_key) {
        req = req.header(k, v);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Algolia request failed: {}", e))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Algolia returned {}: {}", status, body));
    }
    resp.json()
        .await
        .map_err(|e| format!("Failed to parse Algolia response: {}", e))
}

// ── List Algolia indexes ────────────────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListAlgoliaIndexesRequest {
    #[serde(rename = "appId")]
    pub app_id: String,

    #[serde(rename = "apiKey")]
    pub api_key: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AlgoliaIndexInfo {
    pub name: String,
    pub entries: u64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListAlgoliaIndexesResponse {
    pub indexes: Vec<AlgoliaIndexInfo>,
}

/// List all indexes available in an Algolia application.
///
/// Validates that `appId` and `apiKey` are non-empty, then calls the Algolia
/// `/1/indexes` endpoint and returns index name, entry count, and last-updated
/// timestamp for each index. Returns 400 if credentials are missing or 502 if
/// the upstream Algolia call fails.
#[utoipa::path(
    post,
    path = "/1/algolia-list-indexes",
    tag = "migration",
    request_body = ListAlgoliaIndexesRequest,
    responses(
        (status = 200, description = "Available Algolia indexes", body = ListAlgoliaIndexesResponse),
        (status = 400, description = "Missing Algolia credentials"),
        (status = 502, description = "Upstream Algolia request failed")
    ),
    security(("api_key" = []))
)]
pub async fn list_algolia_indexes(
    Json(payload): Json<ListAlgoliaIndexesRequest>,
) -> Result<Json<ListAlgoliaIndexesResponse>, (StatusCode, Json<serde_json::Value>)> {
    if payload.app_id.is_empty() || payload.api_key.is_empty() {
        return Err(json_error_parts(
            StatusCode::BAD_REQUEST,
            "appId and apiKey are required",
        ));
    }

    let client = reqwest::Client::new();
    let resp = algolia_get(&client, &payload.app_id, &payload.api_key, "/1/indexes")
        .await
        .map_err(|e| algolia_error(&e))?;

    let items = resp
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let indexes: Vec<AlgoliaIndexInfo> = items
        .iter()
        .filter_map(|item| {
            let name = item.get("name")?.as_str()?.to_string();
            let entries = item.get("entries").and_then(|v| v.as_u64()).unwrap_or(0);
            let updated_at = item
                .get("updatedAt")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(AlgoliaIndexInfo {
                name,
                entries,
                updated_at,
            })
        })
        .collect();

    Ok(Json(ListAlgoliaIndexesResponse { indexes }))
}

/// Shared context for Algolia API requests during migration.
struct AlgoliaClient<'a> {
    client: &'a reqwest::Client,
    app_id: &'a str,
    api_key: &'a str,
    source_index: &'a str,
}

type MigrateError = (StatusCode, Json<serde_json::Value>);

/// One-click migration from Algolia to Flapjack.
///
/// Fetches settings, synonyms, rules, and all objects from the source Algolia
/// index and imports them into the target Flapjack index in a single call.
#[utoipa::path(
    post,
    path = "/1/migrate-from-algolia",
    tag = "migration",
    request_body = MigrateFromAlgoliaRequest,
    responses(
        (status = 200, description = "Migration completed", body = MigrateFromAlgoliaResponse),
        (status = 400, description = "Invalid migration request"),
        (status = 409, description = "Target index already exists"),
        (status = 502, description = "Upstream Algolia request failed")
    ),
    security(("api_key" = []))
)]
pub async fn migrate_from_algolia(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<MigrateFromAlgoliaRequest>,
) -> Result<Json<MigrateFromAlgoliaResponse>, MigrateError> {
    let target_index = payload
        .target_index
        .as_deref()
        .unwrap_or(&payload.source_index);

    if payload.app_id.is_empty() || payload.api_key.is_empty() || payload.source_index.is_empty() {
        return Err(json_error_parts(
            StatusCode::BAD_REQUEST,
            "appId, apiKey, and sourceIndex are required",
        ));
    }

    flapjack::index::manager::validate_index_name(target_index)
        .map_err(|e| json_error_parts(StatusCode::BAD_REQUEST, e.to_string()))?;
    prepare_target_index(&state, target_index, payload.overwrite).await?;

    let http_client = reqwest::Client::new();
    let ac = AlgoliaClient {
        client: &http_client,
        app_id: &payload.app_id,
        api_key: &payload.api_key,
        source_index: &payload.source_index,
    };

    import_algolia_settings(&ac, &state, target_index).await?;
    let synonyms_count = import_algolia_synonyms(&ac, &state, target_index).await?;
    let rules_count = import_algolia_rules(&ac, &state, target_index).await?;
    let (total_objects, last_task_id) = import_algolia_objects(&ac, &state, target_index).await?;
    await_indexing_completion(&state, target_index).await;

    tracing::info!(
        "[migrate] Migration complete: settings=true, synonyms={}, rules={}, objects={}",
        synonyms_count,
        rules_count,
        total_objects
    );

    Ok(Json(MigrateFromAlgoliaResponse {
        status: "complete".to_string(),
        settings: true,
        synonyms: MigrateCount {
            imported: synonyms_count,
        },
        rules: MigrateCount {
            imported: rules_count,
        },
        objects: MigrateCount {
            imported: total_objects,
        },
        task_id: last_task_id,
    }))
}

/// Ensures the target index directory exists and is ready for migration — deletes it first if `overwrite` is true, or returns a conflict error if it already exists.
async fn prepare_target_index(
    state: &Arc<AppState>,
    target_index: &str,
    overwrite: bool,
) -> Result<(), MigrateError> {
    let target_path = state.manager.base_path.join(target_index);
    if target_path.exists() {
        if !overwrite {
            return Err(json_error_parts(
                StatusCode::CONFLICT,
                format!(
                    "Target index '{}' already exists. Use \"overwrite\": true to replace it.",
                    target_index
                ),
            ));
        }
        tracing::info!("[migrate] Overwriting existing index '{}'", target_index);
        state
            .manager
            .delete_tenant(&target_index.to_string())
            .await
            .map_err(|e| {
                json_error_parts(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to delete existing index: {}", e),
                )
            })?;
    }
    state.manager.create_tenant(target_index).map_err(|e| {
        json_error_parts(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create index: {}", e),
        )
    })?;
    Ok(())
}

/// Extract a string-array setting from Algolia JSON into a `Vec<String>`.
fn extract_string_array(json: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    json.get(key).and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    })
}

/// Fetches index settings from the Algolia API and applies them to the local target index.
async fn import_algolia_settings(
    ac: &AlgoliaClient<'_>,
    state: &Arc<AppState>,
    target_index: &str,
) -> Result<(), MigrateError> {
    tracing::info!(
        "[migrate] Fetching settings from Algolia {}/{}",
        ac.app_id,
        ac.source_index
    );
    let settings_json = algolia_get(
        ac.client,
        ac.app_id,
        ac.api_key,
        &format!(
            "/1/indexes/{}/settings",
            urlencoding::encode(ac.source_index)
        ),
    )
    .await
    .map_err(|e| algolia_error(&e))?;

    let settings_path = state
        .manager
        .base_path
        .join(target_index)
        .join("settings.json");
    let mut settings = if settings_path.exists() {
        IndexSettings::load(&settings_path).unwrap_or_default()
    } else {
        IndexSettings::default()
    };

    settings.searchable_attributes = extract_string_array(&settings_json, "searchableAttributes")
        .or(settings.searchable_attributes);
    if let Some(facets) = extract_string_array(&settings_json, "attributesForFaceting") {
        settings.attributes_for_faceting = facets;
    }
    settings.custom_ranking =
        extract_string_array(&settings_json, "customRanking").or(settings.custom_ranking);
    settings.attributes_to_retrieve = extract_string_array(&settings_json, "attributesToRetrieve")
        .or(settings.attributes_to_retrieve);
    settings.unretrievable_attributes =
        extract_string_array(&settings_json, "unretrievableAttributes")
            .or(settings.unretrievable_attributes);
    if let Some(s) = settings_json
        .get("attributeForDistinct")
        .and_then(|v| v.as_str())
    {
        settings.attribute_for_distinct = Some(s.to_string());
    }

    if let Some(parent) = settings_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            json_error_parts(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create dir: {}", e),
            )
        })?;
    }
    settings.save(&settings_path).map_err(|e| {
        json_error_parts(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to save settings: {}", e),
        )
    })?;
    state.manager.invalidate_settings_cache(target_index);
    state.manager.invalidate_facet_cache(target_index);
    tracing::info!("[migrate] Settings imported");
    Ok(())
}

/// Fetch paginated results from an Algolia search endpoint, stripping highlight metadata.
async fn fetch_algolia_paginated<T: serde::de::DeserializeOwned>(
    ac: &AlgoliaClient<'_>,
    path_suffix: &str,
) -> Result<Vec<T>, MigrateError> {
    let mut all_items = Vec::new();
    let mut page = 0usize;
    loop {
        let resp = algolia_post(
            ac.client,
            ac.app_id,
            ac.api_key,
            &format!(
                "/1/indexes/{}/{}",
                urlencoding::encode(ac.source_index),
                path_suffix
            ),
            &serde_json::json!({"query": "", "hitsPerPage": 1000, "page": page}),
        )
        .await
        .map_err(|e| algolia_error(&e))?;

        let hits = resp.get("hits").and_then(|v| v.as_array());
        let nb_hits = resp.get("nbHits").and_then(|v| v.as_u64()).unwrap_or(0);

        if let Some(hits) = hits {
            for hit in hits {
                let mut clean = hit.clone();
                if let Some(obj) = clean.as_object_mut() {
                    obj.remove("_highlightResult");
                }
                if let Ok(item) = serde_json::from_value::<T>(clean) {
                    all_items.push(item);
                }
            }
        }

        let fetched = (page + 1) * 1000;
        if fetched >= nb_hits as usize || hits.map(|h| h.len()).unwrap_or(0) < 1000 {
            break;
        }
        page += 1;
    }
    Ok(all_items)
}

/// Imports synonyms from a remote Algolia index into the local tenant.
async fn import_algolia_synonyms(
    ac: &AlgoliaClient<'_>,
    state: &Arc<AppState>,
    target_index: &str,
) -> Result<usize, MigrateError> {
    tracing::info!("[migrate] Fetching synonyms from Algolia");
    let all_synonyms: Vec<Synonym> = fetch_algolia_paginated(ac, "synonyms/search").await?;

    let synonyms_path = state
        .manager
        .base_path
        .join(target_index)
        .join("synonyms.json");
    let mut syn_store = SynonymStore::new();
    for syn in &all_synonyms {
        syn_store.insert(syn.clone());
    }
    syn_store.save(&synonyms_path).map_err(|e| {
        json_error_parts(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to save synonyms: {}", e),
        )
    })?;
    state.manager.invalidate_synonyms_cache(target_index);
    let count = all_synonyms.len();
    tracing::info!("[migrate] Imported {} synonyms", count);
    Ok(count)
}

/// Imports rules from a remote Algolia index into the local tenant.
async fn import_algolia_rules(
    ac: &AlgoliaClient<'_>,
    state: &Arc<AppState>,
    target_index: &str,
) -> Result<usize, MigrateError> {
    tracing::info!("[migrate] Fetching rules from Algolia");
    let all_rules: Vec<Rule> = fetch_algolia_paginated(ac, "rules/search").await?;

    let count = all_rules.len();
    if !all_rules.is_empty() {
        let rules_path = state
            .manager
            .base_path
            .join(target_index)
            .join("rules.json");
        let mut rule_store = RuleStore::new();
        for rule in &all_rules {
            rule_store.insert(rule.clone());
        }
        rule_store.save(&rules_path).map_err(|e| {
            json_error_parts(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to save rules: {}", e),
            )
        })?;
        state.manager.invalidate_rules_cache(target_index);
    }
    tracing::info!("[migrate] Imported {} rules", count);
    Ok(count)
}

/// Browses all objects from the Algolia source index via cursor pagination and batch-upserts them into the local target index.
async fn import_algolia_objects(
    ac: &AlgoliaClient<'_>,
    state: &Arc<AppState>,
    target_index: &str,
) -> Result<(usize, i64), MigrateError> {
    tracing::info!("[migrate] Browsing objects from Algolia");
    let mut total_objects = 0usize;
    let mut cursor: Option<String> = None;
    let mut last_task_id: i64 = 0;

    loop {
        let body = match cursor.as_deref() {
            Some(c) => serde_json::json!({"cursor": c}),
            None => serde_json::json!({"hitsPerPage": 1000}),
        };

        let resp = algolia_post(
            ac.client,
            ac.app_id,
            ac.api_key,
            &format!("/1/indexes/{}/browse", urlencoding::encode(ac.source_index)),
            &body,
        )
        .await
        .map_err(|e| algolia_error(&e))?;

        let hits = resp
            .get("hits")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if hits.is_empty() {
            break;
        }

        let documents = parse_algolia_documents(&hits);
        let batch_size = documents.len();
        if !documents.is_empty() {
            let task = state
                .manager
                .add_documents(target_index, documents)
                .map_err(|e| {
                    json_error_parts(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to import objects: {}", e),
                    )
                })?;
            last_task_id = task.numeric_id;
        }
        total_objects += batch_size;

        cursor = resp
            .get("cursor")
            .and_then(|v| v.as_str())
            .map(String::from);
        if cursor.is_none() {
            break;
        }
    }

    tracing::info!("[migrate] Imported {} objects total", total_objects);
    Ok((total_objects, last_task_id))
}

/// Parses Algolia browse hits into flapjack `Document`s, stripping internal fields.
fn parse_algolia_documents(hits: &[serde_json::Value]) -> Vec<Document> {
    hits.iter()
        .filter_map(|hit| {
            let mut clean = hit.clone();
            if let Some(obj) = clean.as_object_mut() {
                obj.remove("_highlightResult");
                obj.remove("_snippetResult");
                obj.remove("_rankingInfo");
            }
            match Document::from_json(&clean) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    tracing::warn!("[migrate] Skipping doc: {}", e);
                    None
                }
            }
        })
        .collect()
}

/// Waits up to 60 seconds for all pending write-queue tasks to drain.
async fn await_indexing_completion(state: &Arc<AppState>, target_index: &str) {
    let max_wait = std::time::Duration::from_secs(60);
    let start = std::time::Instant::now();
    loop {
        let pending = state.manager.pending_task_count(target_index);
        if pending == 0 {
            break;
        }
        if start.elapsed() > max_wait {
            tracing::warn!(
                "[migrate] Timed out waiting for indexing ({} tasks pending)",
                pending
            );
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}

fn algolia_error(msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    json_error_parts(StatusCode::BAD_GATEWAY, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flapjack::types::field_value_to_json_value;
    use serde_json::json;

    #[test]
    fn algolia_host_format() {
        assert_eq!(algolia_host("ABC123"), "ABC123-dsn.algolia.net");
    }

    #[test]
    fn algolia_url_format() {
        assert_eq!(
            algolia_url("ABC123", "/1/indexes"),
            "https://ABC123-dsn.algolia.net/1/indexes"
        );
    }

    #[test]
    fn algolia_url_with_encoded_path() {
        assert_eq!(
            algolia_url("X", "/1/indexes/my%20index/settings"),
            "https://X-dsn.algolia.net/1/indexes/my%20index/settings"
        );
    }

    #[test]
    fn algolia_headers_contain_required() {
        let headers = algolia_headers("APP", "KEY");
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0], ("x-algolia-application-id", "APP".to_string()));
        assert_eq!(headers[1], ("x-algolia-api-key", "KEY".to_string()));
        assert_eq!(headers[2], ("content-type", "application/json".to_string()));
    }

    #[test]
    fn algolia_error_returns_bad_gateway() {
        let (status, body) = algolia_error("connection refused");
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body.0["message"], "connection refused");
        assert_eq!(body.0["status"], 502);
    }

    #[test]
    fn extract_string_array_ignores_non_string_members() {
        let settings_json = json!({
            "searchableAttributes": ["title", 7, null, "brand", true]
        });

        assert_eq!(
            extract_string_array(&settings_json, "searchableAttributes"),
            Some(vec!["title".to_string(), "brand".to_string()])
        );
        assert_eq!(extract_string_array(&settings_json, "missingField"), None);
    }
    /// TODO: Document parse_algolia_documents_strips_metadata_and_skips_non_objects.
    #[test]
    fn parse_algolia_documents_strips_metadata_and_skips_non_objects() {
        let documents = parse_algolia_documents(&[
            json!({
                "objectID": "doc-1",
                "title": "Keyboard",
                "_highlightResult": {"title": {"value": "<em>Keyboard</em>"}},
                "_snippetResult": {"title": {"value": "Keyboard"}},
                "_rankingInfo": {"nbTypos": 0}
            }),
            json!("not-an-object"),
        ]);

        assert_eq!(documents.len(), 1, "non-object hits should be skipped");
        assert_eq!(documents[0].id, "doc-1");
        assert_eq!(
            documents[0]
                .fields
                .get("title")
                .map(field_value_to_json_value),
            Some(json!("Keyboard"))
        );
        assert!(
            !documents[0].fields.contains_key("_highlightResult"),
            "Algolia metadata should be stripped before document parsing"
        );
        assert!(
            !documents[0].fields.contains_key("_snippetResult"),
            "Algolia metadata should be stripped before document parsing"
        );
        assert!(
            !documents[0].fields.contains_key("_rankingInfo"),
            "Algolia metadata should be stripped before document parsing"
        );
    }
}
