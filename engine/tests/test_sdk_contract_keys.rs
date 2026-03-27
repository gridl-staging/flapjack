//! Stage 4: API Keys and Header Contracts
//!
//! Tests Algolia-exact wire-format for key lifecycle operations and header contracts:
//! - Key lifecycle: create, list, get, update, delete, restore
//! - Response envelope field naming (camelCase, no snake_case leakage)
//! - X-Algolia-UserToken passthrough
//! - X-Algolia-Query-ID response header when clickAnalytics: true
//!
//! NOTE: DTO shape without internal fields (hash, salt, hmac_key) is covered
//! in test_key_parity.rs. Auth enforcement is covered in test_api_keys.rs.

use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-sdk-contract";

// ═══════════════════════════════════════════════════════════════════════════════
// 4A: Key Lifecycle Wire-Format Contracts
// ═══════════════════════════════════════════════════════════════════════════════

/// T4.1: Create key returns camelCase envelope with key and createdAt
#[tokio::test]
async fn create_key_returns_camelcase_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "SDK contract test key"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "create key should return 201: {body}"
    );

    // Must have camelCase fields
    assert!(
        body.get("key").is_some(),
        "create response must have 'key' field"
    );
    assert!(
        body.get("createdAt").is_some(),
        "create response must have 'createdAt' field"
    );

    // Must NOT have snake_case fields
    assert!(
        body.get("created_at").is_none(),
        "create response must NOT have snake_case 'created_at'"
    );

    // createdAt must be RFC3339
    common::assert_iso8601_value(&body["createdAt"], "createdAt");

    // key must be non-empty string
    let key_value = body["key"].as_str().expect("key must be string");
    assert!(!key_value.is_empty(), "key must not be empty");
}

/// T4.2: List keys returns wrapped array with camelCase fields
#[tokio::test]
async fn list_keys_returns_wrapped_camelcase_array() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key first
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "List keys test",
            "maxHitsPerQuery": 100,
            "maxQueriesPerIPPerHour": 50
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // List keys
    let (status, body) = common::send_json(&app, Method::GET, "/1/keys", ADMIN_KEY, None).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "list keys should return 200: {body}"
    );

    // Must wrap in { "keys": [...] }
    let keys = body["keys"]
        .as_array()
        .expect("list must have 'keys' array");
    assert!(!keys.is_empty(), "keys array must not be empty");

    // Find our created key
    let found_key = keys
        .iter()
        .find(|k| k["value"] == key_value)
        .expect("created key must be in list");

    // Verify all camelCase fields present
    assert!(
        found_key.get("value").is_some(),
        "key DTO must have 'value'"
    );
    assert!(
        found_key.get("createdAt").is_some(),
        "key DTO must have 'createdAt'"
    );
    assert!(found_key.get("acl").is_some(), "key DTO must have 'acl'");
    assert!(
        found_key.get("description").is_some(),
        "key DTO must have 'description'"
    );
    assert!(
        found_key.get("indexes").is_some(),
        "key DTO must have 'indexes'"
    );
    assert!(
        found_key.get("maxHitsPerQuery").is_some(),
        "key DTO must have 'maxHitsPerQuery'"
    );
    assert!(
        found_key.get("maxQueriesPerIPPerHour").is_some(),
        "key DTO must have 'maxQueriesPerIPPerHour'"
    );
    assert!(
        found_key.get("queryParameters").is_some(),
        "key DTO must have 'queryParameters'"
    );
    assert!(
        found_key.get("referers").is_some(),
        "key DTO must have 'referers'"
    );
    assert!(
        found_key.get("validity").is_some(),
        "key DTO must have 'validity'"
    );

    // Verify no snake_case leakage
    assert!(
        found_key.get("created_at").is_none(),
        "key DTO must NOT have snake_case 'created_at'"
    );
    assert!(
        found_key.get("max_hits_per_query").is_none(),
        "key DTO must NOT have snake_case 'max_hits_per_query'"
    );

    // Verify values match what was created
    assert_eq!(found_key["description"], "List keys test");
    assert_eq!(found_key["maxHitsPerQuery"], 100);
    assert_eq!(found_key["maxQueriesPerIPPerHour"], 50);
}

/// T4.3: Get key returns full camelCase DTO
#[tokio::test]
async fn get_key_returns_full_camelcase_dto() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key with all fields
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search", "browse"],
            "description": "Get key test",
            "indexes": ["idx1", "idx2"],
            "maxHitsPerQuery": 200,
            "maxQueriesPerIPPerHour": 100,
            "queryParameters": "filters=category:book",
            "referers": ["https://example.com/*"],
            "validity": 3600
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // Get the key
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "get key should return 200: {body}");

    // Verify all fields match
    assert_eq!(body["value"], key_value);
    assert_eq!(body["acl"], json!(["search", "browse"]));
    assert_eq!(body["description"], "Get key test");
    assert_eq!(body["indexes"], json!(["idx1", "idx2"]));
    assert_eq!(body["maxHitsPerQuery"], 200);
    assert_eq!(body["maxQueriesPerIPPerHour"], 100);
    assert_eq!(body["queryParameters"], "filters=category:book");
    assert_eq!(body["referers"], json!(["https://example.com/*"]));
    assert_eq!(body["validity"], 3600);

    // GET key createdAt must be integer epoch timestamp.
    common::assert_integer_value(&body["createdAt"], "createdAt");
}

/// T4.4: Get missing key returns 404 with Algolia error shape
#[tokio::test]
async fn get_missing_key_returns_404_algolia_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/keys/nonexistent-key-12345",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "get missing key should return 404: {body}"
    );
    common::assert_error_envelope(&body, 404);
}

/// T4.5: Update key returns camelCase envelope with updatedAt
#[tokio::test]
async fn update_key_returns_camelcase_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "Before update"
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // Update the key
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        Some(json!({
            "acl": ["search", "addObject"],
            "description": "After update"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "update key should return 200: {body}"
    );

    // Must have camelCase fields
    assert_eq!(
        body["key"], key_value,
        "update response must return the key value"
    );
    assert!(
        body.get("updatedAt").is_some(),
        "update response must have 'updatedAt' field"
    );

    // Must NOT have snake_case
    assert!(
        body.get("updated_at").is_none(),
        "update response must NOT have snake_case 'updated_at'"
    );

    // Verify updatedAt is RFC3339
    common::assert_iso8601_value(&body["updatedAt"], "updatedAt");
}

/// T4.6: Update missing key returns 404
#[tokio::test]
async fn update_missing_key_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/keys/nonexistent-key-12345",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "Update attempt"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "update missing key should return 404: {body}"
    );
    common::assert_error_envelope(&body, 404);
}

/// T4.7: Delete key returns camelCase envelope with deletedAt
#[tokio::test]
async fn delete_key_returns_camelcase_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "To be deleted"
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // Delete the key
    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "delete key should return 200: {body}"
    );

    // Must have deletedAt in camelCase
    assert!(
        body.get("deletedAt").is_some(),
        "delete response must have 'deletedAt' field"
    );
    assert!(
        body.get("deleted_at").is_none(),
        "delete response must NOT have snake_case 'deleted_at'"
    );

    // Verify deletedAt is RFC3339
    common::assert_iso8601_value(&body["deletedAt"], "deletedAt");
}

/// T4.8: Delete admin key returns 403 forbidden
#[tokio::test]
async fn delete_admin_key_returns_403() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        &format!("/1/keys/{}", ADMIN_KEY),
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "delete admin key should return 403: {body}"
    );
    common::assert_error_envelope(&body, 403);
}

/// T4.9: Restore key returns camelCase envelope
#[tokio::test]
async fn restore_key_returns_camelcase_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "To be restored"
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // Delete the key first
    let (del_status, _) = common::send_json(
        &app,
        Method::DELETE,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(del_status, StatusCode::OK);

    // Restore the key
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/keys/{}/restore", key_value),
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "restore key should return 200: {body}"
    );

    // Must have camelCase fields
    assert_eq!(
        body["key"], key_value,
        "restore response must return the key value"
    );
    assert!(
        body.get("createdAt").is_some(),
        "restore response must have 'createdAt' field"
    );

    // Verify createdAt is RFC3339
    common::assert_iso8601_value(&body["createdAt"], "createdAt");
}

/// T4.10: Restore missing key returns 404
#[tokio::test]
async fn restore_missing_key_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys/nonexistent-key-12345/restore",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "restore missing key should return 404: {body}"
    );
    common::assert_error_envelope(&body, 404);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4B: Header Contracts
// ═══════════════════════════════════════════════════════════════════════════════

/// T4.11: X-Algolia-UserToken header is accepted without error
#[tokio::test]
async fn x_algolia_user_token_header_accepted() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed a document into the index
    common::seed_docs(
        &app,
        "usertoken-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "doc1", "title": "test" })],
    )
    .await;

    // Search with X-Algolia-UserToken header
    let (status, body) = common::send_json_with_headers(
        &app,
        Method::POST,
        "/1/indexes/usertoken-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "test" })),
        &[("x-algolia-usertoken", "user-123")],
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "search with UserToken should succeed: {body}"
    );
    assert_eq!(body["nbHits"], 1, "should find the document");
}

/// T4.12: X-Algolia-Query-ID response header present when clickAnalytics: true
#[tokio::test]
async fn search_with_click_analytics_includes_query_id_header() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed a document into the index
    common::seed_docs(
        &app,
        "queryid-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "doc1", "title": "hello world" })],
    )
    .await;

    // Search with clickAnalytics: true using oneshot to access headers
    use tower::ServiceExt;

    let req = common::authed_request(
        Method::POST,
        "/1/indexes/queryid-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "hello", "clickAnalytics": true })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // Check X-Algolia-Query-ID header is present
    let query_id_header = resp
        .headers()
        .get("x-algolia-query-id")
        .expect("X-Algolia-Query-ID header must be present when clickAnalytics is true")
        .to_str()
        .unwrap()
        .to_string();

    // Verify it matches body.queryID
    let body = common::body_json(resp).await;
    let body_query_id = body["queryID"]
        .as_str()
        .expect("body must have queryID when clickAnalytics is true");
    assert_eq!(
        query_id_header, body_query_id,
        "header queryID must match body queryID"
    );
}

/// T4.13: X-Algolia-Query-ID response header omitted when clickAnalytics: false
#[tokio::test]
async fn search_without_click_analytics_omits_query_id_header() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed a document into the index
    common::seed_docs(
        &app,
        "no-queryid-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "doc1", "title": "hello world" })],
    )
    .await;

    // Search without clickAnalytics using oneshot to access headers
    use tower::ServiceExt;

    let req = common::authed_request(
        Method::POST,
        "/1/indexes/no-queryid-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "hello" })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);

    // X-Algolia-Query-ID header should NOT be present
    assert!(
        resp.headers().get("x-algolia-query-id").is_none(),
        "X-Algolia-Query-ID header must NOT be present when clickAnalytics is not set"
    );

    // Body should also not have queryID
    let body = common::body_json(resp).await;
    assert!(
        body.get("queryID").is_none(),
        "body must NOT have queryID when clickAnalytics is not set"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4C: ACL Validation Error Format
// ═══════════════════════════════════════════════════════════════════════════════

/// T4.14: Create key with invalid ACL returns 400 with Algolia error shape
#[tokio::test]
async fn create_key_with_invalid_acl_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["invalidPermission"],
            "description": "Invalid ACL test"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "invalid ACL should return 400: {body}"
    );
    common::assert_error_envelope(&body, 400);

    // Message should mention the invalid ACL
    let message = body["message"].as_str().unwrap();
    assert!(
        message.to_lowercase().contains("invalid") || message.contains("acl"),
        "error message should mention invalid ACL: {message}"
    );
}

/// T4.15: Update key with invalid ACL returns 400
#[tokio::test]
async fn update_key_with_invalid_acl_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key first
    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "Update ACL test"
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::CREATED);
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // Try to update with invalid ACL
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        Some(json!({
            "acl": ["anotherInvalidAcl"],
            "description": "Update attempt"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "invalid ACL update should return 400: {body}"
    );
    common::assert_error_envelope(&body, 400);
}
