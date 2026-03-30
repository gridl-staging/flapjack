use super::*;

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

    http_request(
        &app,
        "POST",
        "/1/indexes/products/related-products/recommend/rules/batch",
        Some(serde_json::json!([{"objectID": "rule-1", "description": "v1"}])),
    )
    .await;

    let (status, _) = http_request(
        &app,
        "PUT",
        "/1/indexes/products/related-products/recommend/rules/rule-1",
        Some(serde_json::json!({"objectID": "rule-1", "description": "v2"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

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

    let (status, body) = http_request(
        &app,
        "PUT",
        "/1/indexes/brand-new-index/related-products/recommend/rules/rule-1",
        Some(serde_json::json!({"objectID": "rule-1", "description": "new index rule"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["taskID"].is_number());

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
