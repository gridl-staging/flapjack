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
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, post};
    use axum::Router;
    use tempfile::TempDir;
    use tower::ServiceExt;

    /// Build an Axum router with all recommend-rules CRUD and search routes wired to the given shared state.
    fn recommend_rules_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/1/indexes/:indexName/:model/recommend/rules/batch",
                post(batch_recommend_rules),
            )
            .route(
                "/1/indexes/:indexName/:model/recommend/rules/search",
                post(search_recommend_rules),
            )
            .route(
                "/1/indexes/:indexName/:model/recommend/rules/:objectID",
                get(get_recommend_rule)
                    .put(put_recommend_rule)
                    .delete(delete_recommend_rule),
            )
            .with_state(state)
    }

    /// Send a one-shot HTTP request to the test router and return the status code with the parsed JSON body.
    async fn http_request(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let body = match body {
            Some(v) => Body::from(serde_json::to_vec(&v).unwrap()),
            None => Body::empty(),
        };
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        (status, json)
    }

    // ── C6.1: GET and DELETE ────────────────────────────────────────────

    /// Verify the basic batch-then-GET round-trip: save a rule via batch and retrieve it by objectID.
    #[tokio::test]
    async fn batch_then_get_rule() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Batch save a rule
        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([{
                "objectID": "rule-1",
                "description": "Test rule",
                "consequence": {
                    "promote": [{"objectID": "prod-1", "position": 0}]
                }
            }])),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["taskID"].is_number());

        // GET the rule
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["objectID"], "rule-1");
        assert_eq!(body["description"], "Test rule");
    }

    #[tokio::test]
    async fn get_nonexistent_rule_returns_404() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/missing",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["status"], 404);
    }

    /// Verify that GET returns 404 after a previously existing rule is deleted.
    #[tokio::test]
    async fn delete_rule_then_get_returns_404() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Batch save
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([{
                "objectID": "rule-1",
                "description": "Doomed rule"
            }])),
        )
        .await;

        // Delete
        let (status, body) = http_request(
            &app,
            "DELETE",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["taskID"].is_number());

        // GET should 404
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_nonexistent_rule_returns_404() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "DELETE",
            "/1/indexes/products/related-products/recommend/rules/never-existed",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["status"], 404);
    }

    #[tokio::test]
    async fn invalid_model_returns_400() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/invalid-model/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
    }

    // ── C6.2: Batch ────────────────────────────────────────────────────

    /// Verify that rules saved via batch POST are individually retrievable via GET.
    #[tokio::test]
    async fn batch_saves_rules_retrievable_via_get() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "r1", "description": "Rule 1"},
                {"objectID": "r2", "description": "Rule 2"}
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["taskID"].is_number());
        assert!(body["updatedAt"].is_string());

        // Verify both retrievable
        let (s1, b1) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/r1",
            None,
        )
        .await;
        assert_eq!(s1, StatusCode::OK);
        assert_eq!(b1["objectID"], "r1");

        let (s2, b2) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/r2",
            None,
        )
        .await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(b2["objectID"], "r2");
    }

    /// Verify that `clearExistingRules: true` removes all prior rules before saving the new batch.
    #[tokio::test]
    async fn batch_clear_existing_rules() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Save initial rules
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "old-1"},
                {"objectID": "old-2"}
            ])),
        )
        .await;

        // Batch with clearExistingRules
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!({
                "rules": [{"objectID": "new-1"}],
                "clearExistingRules": true
            })),
        )
        .await;

        // Old rules gone
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/old-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // New rule present
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/new-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    /// Verify that a rule with `_operation: "delete"` in a batch request removes the targeted rule while leaving others intact.
    #[tokio::test]
    async fn batch_delete_operation() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Save initial
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "r1"},
                {"objectID": "r2"},
                {"objectID": "r3"}
            ])),
        )
        .await;

        // Batch with delete operation on r2
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "r2", "_operation": "delete"}
            ])),
        )
        .await;

        let (s1, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/r1",
            None,
        )
        .await;
        assert_eq!(s1, StatusCode::OK);

        let (s2, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/r2",
            None,
        )
        .await;
        assert_eq!(s2, StatusCode::NOT_FOUND);

        let (s3, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/r3",
            None,
        )
        .await;
        assert_eq!(s3, StatusCode::OK);
    }

    // ── C6.3: Search ───────────────────────────────────────────────────

    /// Verify that searching with an empty query string returns every stored rule.
    #[tokio::test]
    async fn search_rules_empty_query_returns_all() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "r1", "description": "First rule"},
                {"objectID": "r2", "description": "Second rule"},
                {"objectID": "r3", "description": "Third rule"}
            ])),
        )
        .await;

        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/search",
            Some(serde_json::json!({"query": ""})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["nbHits"], 3);
        assert_eq!(body["hits"].as_array().unwrap().len(), 3);
        assert_eq!(body["page"], 0);
    }

    /// Verify that searching with a non-empty query filters rules by matching description text.
    #[tokio::test]
    async fn search_rules_by_term() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([
                {"objectID": "nike-promo", "description": "Nike product promotion"},
                {"objectID": "adidas-promo", "description": "Adidas product promotion"},
                {"objectID": "general", "description": "General boost"}
            ])),
        )
        .await;

        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/search",
            Some(serde_json::json!({"query": "Nike"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["nbHits"], 1);
        assert_eq!(body["hits"][0]["objectID"], "nike-promo");
    }

    /// Verify that `page` and `hitsPerPage` parameters correctly paginate search results and compute `nbPages`.
    #[tokio::test]
    async fn search_rules_pagination() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let rules: Vec<serde_json::Value> = (0..5)
            .map(|i| serde_json::json!({"objectID": format!("rule-{}", i)}))
            .collect();

        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!(rules)),
        )
        .await;

        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/search",
            Some(serde_json::json!({"query": "", "page": 0, "hitsPerPage": 2})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["nbHits"], 5);
        assert_eq!(body["hits"].as_array().unwrap().len(), 2);
        assert_eq!(body["page"], 0);
        assert_eq!(body["nbPages"], 3);

        // Page 2 (last page, 1 item)
        let (_, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/search",
            Some(serde_json::json!({"query": "", "page": 2, "hitsPerPage": 2})),
        )
        .await;
        assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    }

    /// Verify that requesting `hitsPerPage: 0` is rejected with HTTP 400.
    #[tokio::test]
    async fn search_rules_hits_per_page_zero_returns_400() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([{"objectID": "r1"}])),
        )
        .await;

        let (status, body) = http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/search",
            Some(serde_json::json!({"query": "", "hitsPerPage": 0})),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
    }

    // ── C6.5: Multi-model ──────────────────────────────────────────────

    /// Verify that rules saved under one model are not visible under a different model for the same index.
    #[tokio::test]
    async fn rules_are_model_scoped_via_api() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Save rule for bought-together
        http_request(
            &app,
            "POST",
            "/1/indexes/products/bought-together/recommend/rules/batch",
            Some(serde_json::json!([{"objectID": "bt-rule"}])),
        )
        .await;

        // Should not be visible under related-products
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/bt-rule",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Should be visible under bought-together
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/bought-together/recommend/rules/bt-rule",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // ── Security: error sanitization ──────────────────────────────────

    /// Verify that HandlerError sanitizes storage error messages via FlapjackError,
    /// ensuring filesystem paths and OS details are not exposed to clients.
    #[tokio::test]
    async fn handler_error_does_not_leak_filesystem_paths() {
        use axum::response::IntoResponse;

        let he = HandlerError::from(
            "Failed to read rules file: /data/flapjack/products/recommend_rules/related-products.json: Permission denied".to_string()
        );
        let resp = he.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["message"], "Internal server error");
        assert_eq!(json["status"], 500);

        let body_str = json.to_string();
        assert!(
            !body_str.contains("/data"),
            "response must not contain filesystem paths"
        );
        assert!(
            !body_str.contains("Permission denied"),
            "response must not contain OS error details"
        );
        assert!(
            !body_str.contains("flapjack"),
            "response must not contain internal component names"
        );
    }

    /// Verify that the recommend-rules GET route preserves sanitized 500 responses
    /// when storage returns an internal parse failure.
    #[tokio::test]
    async fn get_recommend_rule_sanitizes_storage_parse_failures() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        state.manager.create_tenant("products").unwrap();

        let rules_dir = tmp
            .path()
            .join("products")
            .join("recommend_rules")
            .join("related-products");
        std::fs::create_dir_all(&rules_dir).unwrap();
        std::fs::write(rules_dir.join("rules.json"), "{ definitely not valid json").unwrap();

        let app = recommend_rules_router(state);
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["status"], 500);
        assert_eq!(body["message"], "Internal server error");
        assert!(
            !body.to_string().contains("Failed to parse rules file"),
            "storage parse details must stay server-side"
        );
    }

    /// Verify that all five recommendation models (`related-products`, `bought-together`, `trending-items`, `trending-facets`, `looking-similar`) accept and return rules.
    #[tokio::test]
    async fn all_five_models_accept_rules() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let models = [
            "related-products",
            "bought-together",
            "trending-items",
            "trending-facets",
            "looking-similar",
        ];

        for model in &models {
            let (status, body) = http_request(
                &app,
                "POST",
                &format!("/1/indexes/products/{}/recommend/rules/batch", model),
                Some(serde_json::json!([{"objectID": format!("{}-rule", model)}])),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "batch failed for model: {}", model);
            assert!(body["taskID"].is_number());

            let (status, body) = http_request(
                &app,
                "GET",
                &format!(
                    "/1/indexes/products/{}/recommend/rules/{}-rule",
                    model, model
                ),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "GET failed for model: {}", model);
            assert_eq!(body["objectID"], format!("{}-rule", model));
        }
    }

    // ── PUT ────────────────────────────────────────────────────────────────

    /// Verify that PUT to a non-existent objectID creates the rule and returns a valid taskID.
    #[tokio::test]
    async fn put_rule_creates_new_rule() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "PUT",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            Some(serde_json::json!({"objectID": "rule-1", "description": "Created via PUT"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["taskID"].is_number());
        assert!(body["updatedAt"].is_string());
        assert_eq!(body["id"], "rule-1");

        // GET it back and verify
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["objectID"], "rule-1");
        assert_eq!(body["description"], "Created via PUT");
    }

    /// Verify that PUT overwrites an existing rule's fields rather than creating a duplicate.
    #[tokio::test]
    async fn put_rule_upserts_existing_rule() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // Save v1 via batch
        http_request(
            &app,
            "POST",
            "/1/indexes/products/related-products/recommend/rules/batch",
            Some(serde_json::json!([{"objectID": "rule-1", "description": "v1"}])),
        )
        .await;

        // PUT v2
        let (status, _) = http_request(
            &app,
            "PUT",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            Some(serde_json::json!({"objectID": "rule-1", "description": "v2"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // GET and verify v2
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["description"], "v2");
    }

    #[tokio::test]
    async fn put_rule_invalid_model_returns_400() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "PUT",
            "/1/indexes/products/bad-model/recommend/rules/rule-1",
            Some(serde_json::json!({"objectID": "rule-1"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
    }

    /// Verify that PUT succeeds even when the target index has never been created, lazily creating storage.
    #[tokio::test]
    async fn put_rule_nonexistent_index_still_saves() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        // PUT to an index that has never been created
        let (status, body) = http_request(
            &app,
            "PUT",
            "/1/indexes/brand-new-index/related-products/recommend/rules/rule-1",
            Some(serde_json::json!({"objectID": "rule-1", "description": "new index rule"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["taskID"].is_number());

        // GET confirms persistence
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/brand-new-index/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["objectID"], "rule-1");
        assert_eq!(body["description"], "new index rule");
    }

    /// Verify that `promote`, `hide`, and `params` consequence fields round-trip correctly through PUT then GET.
    #[tokio::test]
    async fn put_rule_with_consequence_fields() {
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, _) = http_request(
            &app,
            "PUT",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            Some(serde_json::json!({
                "objectID": "rule-1",
                "consequence": {
                    "promote": [{"objectID": "prod-a", "position": 0}],
                    "hide": [{"objectID": "prod-b"}],
                    "params": {"filters": "brand:Nike"}
                }
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // GET and verify consequence fields round-trip
        let (status, body) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/rule-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["consequence"]["promote"][0]["objectID"], "prod-a");
        assert_eq!(body["consequence"]["promote"][0]["position"], 0);
        assert_eq!(body["consequence"]["hide"][0]["objectID"], "prod-b");
        assert_eq!(body["consequence"]["params"]["filters"], "brand:Nike");
    }

    /// Verify that the objectID in the URL path takes precedence over a conflicting objectID in the request body.
    #[tokio::test]
    async fn put_rule_url_object_id_overrides_body() {
        // The URL objectID wins even if body provides a different value.
        let tmp = TempDir::new().unwrap();
        let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
        let app = recommend_rules_router(state);

        let (status, body) = http_request(
            &app,
            "PUT",
            "/1/indexes/products/related-products/recommend/rules/url-id",
            Some(serde_json::json!({"objectID": "body-id", "description": "mismatch test"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["id"], "url-id");

        // Only url-id should exist
        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/url-id",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = http_request(
            &app,
            "GET",
            "/1/indexes/products/related-products/recommend/rules/body-id",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
