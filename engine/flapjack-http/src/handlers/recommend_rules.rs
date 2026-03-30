//! CRUD handlers for recommend rules scoped by index and recommendation model, supporting get, put, delete, batch, and search operations.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use flapjack::recommend::rules::{self, RecommendRule};

use super::AppState;
use crate::error_response::HandlerError;
use crate::extractors::validate_index_http;

fn validate_index(name: &str) -> Result<(), HandlerError> {
    validate_index_http(name).map_err(|(status, msg)| HandlerError::Custom {
        status,
        message: msg,
    })
}

fn validate_model_http(model: &str) -> Result<(), HandlerError> {
    rules::validate_model(model).map_err(HandlerError::bad_request)
}

// ── GET ─────────────────────────────────────────────────────────────────────

/// Retrieve a single recommend rule by objectID for a given index and model.
///
/// # Returns
///
/// The full `RecommendRule` JSON on success, or 404 if not found.
pub async fn get_recommend_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, model, object_id)): Path<(String, String, String)>,
) -> Result<Json<RecommendRule>, HandlerError> {
    validate_index(&index_name)?;
    validate_model_http(&model)?;

    let rule = rules::get_rule(&state.manager.base_path, &index_name, &model, &object_id)?;

    rule.ok_or_else(|| HandlerError::not_found(format!("ObjectID {} does not exist", object_id)))
        .map(Json)
}

// ── DELETE ───────────────────────────────────────────────────────────────────

/// Delete a single recommend rule by objectID for a given index and model.
///
/// Returns a `taskID` and `deletedAt` timestamp on success, or 404 if the rule does not exist.
pub async fn delete_recommend_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, model, object_id)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index(&index_name)?;
    validate_model_http(&model)?;

    let removed = rules::delete_rule(&state.manager.base_path, &index_name, &model, &object_id)?;

    if !removed {
        return Err(HandlerError::not_found(format!(
            "ObjectID {} does not exist",
            object_id
        )));
    }

    let task = state.manager.make_noop_task(&index_name)?;

    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "deletedAt": chrono::Utc::now().to_rfc3339()
    })))
}

// ── PUT ──────────────────────────────────────────────────────────────────────

/// Create or update a single recommend rule at the objectID specified in the URL path.
///
/// The URL's objectID always overrides any `objectID` present in the request body. Returns `taskID`, `updatedAt`, and the canonical `id`.
pub async fn put_recommend_rule(
    State(state): State<Arc<AppState>>,
    Path((index_name, model, object_id)): Path<(String, String, String)>,
    Json(mut rule): Json<RecommendRule>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index(&index_name)?;
    validate_model_http(&model)?;

    // Ensure the stored rule's objectID matches the URL param.
    rule.object_id = object_id.clone();

    rules::save_rules_batch(
        &state.manager.base_path,
        &index_name,
        &model,
        vec![rule],
        false,
    )?;

    let task = state.manager.make_noop_task(&index_name)?;

    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "updatedAt": chrono::Utc::now().to_rfc3339(),
        "id": object_id
    })))
}

// ── BATCH ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BatchBody {
    /// Array of rules (simple form)
    Rules(Vec<RecommendRule>),
    /// Object form with optional `clearExistingRules`
    WithOptions {
        rules: Vec<RecommendRule>,
        #[serde(default, rename = "clearExistingRules")]
        clear_existing_rules: bool,
    },
}

/// Save or delete recommend rules in bulk for a given index and model.
///
/// Accepts either a plain JSON array of rules or an object with `rules` and an optional `clearExistingRules` flag. When `clearExistingRules` is true, all existing rules for the model are removed before the new batch is written.
///
/// # Returns
///
/// A JSON response containing `taskID` and `updatedAt`.
pub async fn batch_recommend_rules(
    State(state): State<Arc<AppState>>,
    Path((index_name, model)): Path<(String, String)>,
    Json(body): Json<BatchBody>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index(&index_name)?;
    validate_model_http(&model)?;

    let (incoming, clear_existing) = match body {
        BatchBody::Rules(rules) => (rules, false),
        BatchBody::WithOptions {
            rules,
            clear_existing_rules,
        } => (rules, clear_existing_rules),
    };

    rules::save_rules_batch(
        &state.manager.base_path,
        &index_name,
        &model,
        incoming,
        clear_existing,
    )?;

    let task = state.manager.make_noop_task(&index_name)?;

    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "updatedAt": chrono::Utc::now().to_rfc3339()
    })))
}

// ── SEARCH ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchRecommendRulesRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub hits_per_page: Option<usize>,
}

/// Search recommend rules for a given index and model with optional query filtering and pagination.
///
/// Accepts a JSON body with `query`, `page`, and `hitsPerPage` fields. Returns matching rules with pagination metadata (`hits`, `nbHits`, `page`, `nbPages`).
///
/// # Returns
///
/// A JSON response with paginated hits, or 400 if `hitsPerPage` is zero.
pub async fn search_recommend_rules(
    State(state): State<Arc<AppState>>,
    Path((index_name, model)): Path<(String, String)>,
    Json(body): Json<SearchRecommendRulesRequest>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    validate_index(&index_name)?;
    validate_model_http(&model)?;

    let page = body.page.unwrap_or(0);
    let hits_per_page = body.hits_per_page.unwrap_or(20);
    if hits_per_page == 0 {
        return Err(HandlerError::bad_request(
            "hitsPerPage must be greater than 0",
        ));
    }

    let (hits, total) = rules::search_rules(
        &state.manager.base_path,
        &index_name,
        &model,
        &body.query,
        page,
        hits_per_page,
    )?;

    let nb_pages = if total == 0 {
        0
    } else {
        total.div_ceil(hits_per_page)
    };

    Ok(Json(serde_json::json!({
        "hits": hits,
        "nbHits": total,
        "page": page,
        "nbPages": nb_pages
    })))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "recommend_rules_tests.rs"]
mod tests;
