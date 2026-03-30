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

#[path = "recommend_rules_tests_put.rs"]
mod put;
