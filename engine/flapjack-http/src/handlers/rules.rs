use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use super::{safe_nb_pages, settings::parse_bool_query_param, AppState};
use crate::error_response::HandlerError;
use crate::extractors::{validate_index_http, ValidatedIndexName};
use flapjack::index::rules::{Rule, RuleStore};

#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}/rules/{objectID}",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Rule ID")
    ),
    responses(
        (status = 200, description = "Rule retrieved", body = serde_json::Value),
        (status = 404, description = "Rule not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
) -> Result<Json<Rule>, HandlerError> {
    validate_index_http(&index_name)?;
    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    if !rules_path.exists() {
        return Err(HandlerError::not_found(format!(
            "Rule {} not found",
            object_id
        )));
    }

    let store = RuleStore::load(&rules_path).map_err(HandlerError::internal)?;

    store
        .get(&object_id)
        .cloned()
        .ok_or_else(|| HandlerError::not_found(format!("Rule {} not found", object_id)))
        .map(Json)
}

#[utoipa::path(
    put,
    path = "/1/indexes/{indexName}/rules/{objectID}",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Rule ID")
    ),
    request_body(content = serde_json::Value, description = "Rule data"),
    responses(
        (status = 200, description = "Rule saved", body = serde_json::Value)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn save_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, _object_id)): Path<(String, String)>,
    Json(rule): Json<Rule>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index_http(&index_name)?;
    state
        .manager
        .create_tenant(&index_name)
        .map_err(HandlerError::internal)?;

    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    let mut store = if rules_path.exists() {
        RuleStore::load(&rules_path).map_err(HandlerError::internal)?
    } else {
        RuleStore::new()
    };

    store.insert(rule.clone());

    store.save(&rules_path).map_err(HandlerError::internal)?;

    state.manager.invalidate_rules_cache(&index_name);

    state.manager.append_oplog(
        &index_name,
        "save_rule",
        serde_json::to_value(&rule).unwrap_or_default(),
    );

    let task = state
        .manager
        .make_noop_task(&index_name)
        .map_err(HandlerError::internal)?;
    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "updatedAt": chrono::Utc::now().to_rfc3339(),
        "id": rule.object_id
    })))
}

#[utoipa::path(
    delete,
    path = "/1/indexes/{indexName}/rules/{objectID}",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Rule ID")
    ),
    responses(
        (status = 200, description = "Rule deleted", body = serde_json::Value),
        (status = 404, description = "Rule not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index_http(&index_name)?;
    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    if !rules_path.exists() {
        return Err(HandlerError::not_found(format!(
            "Rule {} not found",
            object_id
        )));
    }

    let mut store = RuleStore::load(&rules_path).map_err(HandlerError::internal)?;

    store
        .remove(&object_id)
        .ok_or_else(|| HandlerError::not_found(format!("Rule {} not found", object_id)))?;

    store.save(&rules_path).map_err(HandlerError::internal)?;

    state.manager.invalidate_rules_cache(&index_name);

    state.manager.append_oplog(
        &index_name,
        "delete_rule",
        serde_json::json!({"objectID": object_id}),
    );

    let task = state
        .manager
        .make_noop_task(&index_name)
        .map_err(HandlerError::internal)?;
    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "deletedAt": chrono::Utc::now().to_rfc3339()
    })))
}

#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/rules/batch",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Array of rules"),
    responses(
        (status = 200, description = "Rules saved", body = serde_json::Value),
        (status = 400, description = "Invalid query parameter value")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn save_rules(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
    Query(params): Query<HashMap<String, String>>,
    Json(rules): Json<Vec<Rule>>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    state
        .manager
        .create_tenant(&index_name)
        .map_err(HandlerError::internal)?;

    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    let clear_existing = parse_bool_query_param(&params, "clearExistingRules")?;

    let mut store = if clear_existing || !rules_path.exists() {
        RuleStore::new()
    } else {
        RuleStore::load(&rules_path).map_err(HandlerError::internal)?
    };

    let rules_json: Vec<serde_json::Value> = rules
        .iter()
        .map(|r| serde_json::to_value(r).unwrap_or_default())
        .collect();

    for rule in rules {
        store.insert(rule);
    }

    store.save(&rules_path).map_err(HandlerError::internal)?;

    state.manager.invalidate_rules_cache(&index_name);

    // Forward rules to replicas if requested
    let forward_to_replicas = parse_bool_query_param(&params, "forwardToReplicas")?;
    if forward_to_replicas {
        forward_rules_to_replicas(&state, &index_name, &store).map_err(HandlerError::internal)?;
    }

    state.manager.append_oplog(
        &index_name,
        "save_rules",
        serde_json::json!({"rules": rules_json, "clearExisting": clear_existing}),
    );

    let task = state
        .manager
        .make_noop_task(&index_name)
        .map_err(HandlerError::internal)?;
    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "updatedAt": chrono::Utc::now().to_rfc3339()
    })))
}

/// Forward the current rule store from a primary index to all its configured replicas.
fn forward_rules_to_replicas(
    state: &Arc<AppState>,
    primary_index_name: &str,
    store: &RuleStore,
) -> Result<(), flapjack::error::FlapjackError> {
    use flapjack::index::replica::parse_replica_entry;
    use flapjack::index::settings::IndexSettings;

    let settings_path = state
        .manager
        .base_path
        .join(primary_index_name)
        .join("settings.json");
    if !settings_path.exists() {
        return Ok(());
    }
    let primary_settings = IndexSettings::load(&settings_path)?;
    let replicas = match &primary_settings.replicas {
        Some(r) => r,
        None => return Ok(()),
    };

    for replica_str in replicas {
        let parsed = parse_replica_entry(replica_str)?;
        let replica_name = parsed.name();
        let replica_dir = state.manager.base_path.join(replica_name);
        if !replica_dir.exists() {
            continue;
        }
        let replica_rules_path = replica_dir.join("rules.json");
        store.save(&replica_rules_path)?;
        state.manager.invalidate_rules_cache(replica_name);
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/rules/clear",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "Rules cleared", body = serde_json::Value)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn clear_rules(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
) -> Result<Json<serde_json::Value>, HandlerError> {
    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    if rules_path.exists() {
        std::fs::remove_file(&rules_path).map_err(HandlerError::internal)?;
    }

    state.manager.invalidate_rules_cache(&index_name);
    state
        .manager
        .append_oplog(&index_name, "clear_rules", serde_json::json!({}));

    let task = state
        .manager
        .make_noop_task(&index_name)
        .map_err(HandlerError::internal)?;
    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "updatedAt": chrono::Utc::now().to_rfc3339()
    })))
}

#[derive(Deserialize)]
pub struct SearchRulesRequest {
    #[serde(default)]
    pub query: String,

    #[serde(default)]
    pub page: usize,

    #[serde(rename = "hitsPerPage", default = "default_hits_per_page")]
    pub hits_per_page: usize,
}

fn default_hits_per_page() -> usize {
    20
}

#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/rules/search",
    tag = "rules",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Search parameters"),
    responses(
        (status = 200, description = "Matching rules", body = serde_json::Value)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn search_rules(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
    Json(req): Json<SearchRulesRequest>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    let rules_path = state.manager.base_path.join(&index_name).join("rules.json");

    let store = if rules_path.exists() {
        RuleStore::load(&rules_path).map_err(HandlerError::internal)?
    } else {
        RuleStore::new()
    };

    let (hits, total) = store.search(&req.query, req.page, req.hits_per_page);
    let nb_pages = safe_nb_pages(total, req.hits_per_page);

    Ok(Json(serde_json::json!({
        "hits": hits,
        "nbHits": total,
        "page": req.page,
        "nbPages": nb_pages
    })))
}
