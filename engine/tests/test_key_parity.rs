/// Stage 2: API Keys Security & Parity tests
/// Tests key response DTO shape, ACL validation, and restriction enforcement.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

use common::authed_request;
use common::body_json;

// ─── 2A: Key Response DTO Shape ───

#[tokio::test]
async fn get_key_returns_dto_shape_without_internal_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key first
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "DTO test key"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let create_body = body_json(resp).await;
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // GET /1/keys/{key}
    let get_req = authed_request(
        Method::GET,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    // Must have these fields
    assert!(
        body.get("value").is_some(),
        "GET key must have 'value' field, got: {}",
        body
    );
    assert!(
        body.get("createdAt").is_some(),
        "GET key must have 'createdAt'"
    );
    assert!(body.get("acl").is_some(), "GET key must have 'acl'");
    assert!(
        body.get("description").is_some(),
        "GET key must have 'description'"
    );
    assert!(body.get("indexes").is_some(), "GET key must have 'indexes'");
    assert!(
        body.get("maxHitsPerQuery").is_some(),
        "GET key must have 'maxHitsPerQuery'"
    );
    assert!(
        body.get("maxQueriesPerIPPerHour").is_some(),
        "GET key must have 'maxQueriesPerIPPerHour'"
    );
    assert!(
        body.get("queryParameters").is_some(),
        "GET key must have 'queryParameters'"
    );
    assert!(
        body.get("referers").is_some(),
        "GET key must have 'referers'"
    );
    assert!(
        body.get("validity").is_some(),
        "GET key must have 'validity'"
    );

    // Must NOT have internal fields
    assert!(
        body.get("hash").is_none(),
        "GET key must NOT leak 'hash', got: {}",
        body
    );
    assert!(body.get("salt").is_none(), "GET key must NOT leak 'salt'");
    assert!(
        body.get("hmac_key").is_none(),
        "GET key must NOT leak 'hmac_key'"
    );

    // value should be the actual key value
    assert_eq!(
        body["value"].as_str().unwrap(),
        key_value,
        "value must match the key"
    );
}

#[tokio::test]
async fn get_key_created_at_is_epoch_integer() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "createdAt type test"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let create_body = body_json(resp).await;
    let key_value = create_body["key"].as_str().unwrap().to_string();

    // GET /1/keys/{key}
    let get_req = authed_request(
        Method::GET,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(get_req).await.unwrap();
    let body = body_json(resp).await;

    // GET key createdAt must be integer epoch timestamp.
    common::assert_integer_value(&body["createdAt"], "createdAt");
}

#[tokio::test]
async fn list_keys_returns_wrapped_dto_array() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // GET /1/keys
    let list_req = authed_request(Method::GET, "/1/keys", ADMIN_KEY, None);
    let resp = app.clone().oneshot(list_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    // Must wrap in { "keys": [...] }
    let keys = body["keys"]
        .as_array()
        .expect("list must have 'keys' array");
    assert!(
        keys.len() >= 2,
        "should have at least admin + default search key"
    );

    // Every key in the list must have DTO shape
    for key in keys {
        assert!(
            key.get("value").is_some(),
            "list key must have 'value', got: {}",
            key
        );
        assert!(
            key.get("createdAt").is_some(),
            "list key must have 'createdAt'"
        );
        assert!(key.get("acl").is_some(), "list key must have 'acl'");
        assert!(
            key.get("hash").is_none(),
            "list key must NOT have 'hash', got: {}",
            key
        );
        assert!(key.get("salt").is_none(), "list key must NOT have 'salt'");
        assert!(
            key.get("hmac_key").is_none(),
            "list key must NOT have 'hmac_key'"
        );

        common::assert_integer_value(&key["createdAt"], "createdAt");
    }

    // Admin key's value should be recoverable
    let admin = keys
        .iter()
        .find(|k| k["description"] == "Admin API Key")
        .unwrap();
    assert!(
        !admin["value"].as_str().unwrap().is_empty(),
        "admin key value must not be empty"
    );
}

#[tokio::test]
async fn create_key_returns_key_and_rfc3339_created_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "POST shape test"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = body_json(resp).await;

    // POST /1/keys returns { "key": "...", "createdAt": "<RFC3339>" }
    assert!(body.get("key").is_some(), "POST must return 'key'");
    assert!(
        body.get("value").is_none(),
        "POST must NOT return 'value' (use 'key')"
    );
    assert!(
        body["key"].as_str().unwrap().starts_with("fj_"),
        "key should have fj_ prefix"
    );

    // createdAt should be RFC3339 string for mutation responses
    common::assert_iso8601_value(&body["createdAt"], "createdAt");
}

#[tokio::test]
async fn restore_key_returns_key_and_created_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create, then delete, then restore
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "Restore test"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    let delete_req = authed_request(
        Method::DELETE,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(delete_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let restore_req = authed_request(
        Method::POST,
        &format!("/1/keys/{}/restore", key_value),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(restore_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    // Restore must return { "key": "...", "createdAt": "<RFC3339>" }
    assert!(
        body.get("key").is_some(),
        "restore must return 'key', got: {}",
        body
    );
    assert_eq!(
        body["key"].as_str().unwrap(),
        key_value,
        "restore 'key' must match"
    );
    common::assert_iso8601_value(&body["createdAt"], "createdAt");
}

// ─── 2B: ACL Validation ───

#[tokio::test]
async fn create_key_rejects_unknown_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search", "fakePermission"], "description": "Bad ACL"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unknown ACL should be rejected"
    );
    let body = body_json(resp).await;
    assert!(body.get("message").is_some(), "error should have 'message'");
    assert_eq!(body["status"].as_i64().unwrap(), 400);
}

#[tokio::test]
async fn update_key_rejects_unknown_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a valid key first
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "ACL update test"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Try to update with invalid ACL
    let update_req = authed_request(
        Method::PUT,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        Some(json!({"acl": ["search", "admin"], "description": "ACL update test"})),
    );
    let resp = app.clone().oneshot(update_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "unknown ACL 'admin' should be rejected on update"
    );
    let body = body_json(resp).await;
    assert!(body.get("message").is_some());
    assert_eq!(body["status"].as_i64().unwrap(), 400);
}

#[tokio::test]
async fn create_key_accepts_all_valid_acls() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let all_acls = vec![
        "search",
        "browse",
        "addObject",
        "deleteObject",
        "deleteIndex",
        "settings",
        "editSettings",
        "analytics",
        "recommendation",
        "usage",
        "logs",
        "listIndexes",
        "seeUnretrievableAttributes",
        "inference",
        "personalization",
    ];

    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": all_acls, "description": "All 15 ACLs"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "all 15 valid ACLs should be accepted"
    );
}

// ─── 2C: Key Restriction Enforcement ───

#[tokio::test]
async fn max_hits_per_query_caps_search_results() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed an index with some data
    let batch_req = authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({"requests": [
            {"action": "addObject", "body": {"objectID": "1", "name": "Product 1"}},
            {"action": "addObject", "body": {"objectID": "2", "name": "Product 2"}},
            {"action": "addObject", "body": {"objectID": "3", "name": "Product 3"}},
            {"action": "addObject", "body": {"objectID": "4", "name": "Product 4"}},
            {"action": "addObject", "body": {"objectID": "5", "name": "Product 5"}},
            {"action": "addObject", "body": {"objectID": "6", "name": "Product 6"}},
            {"action": "addObject", "body": {"objectID": "7", "name": "Product 7"}},
            {"action": "addObject", "body": {"objectID": "8", "name": "Product 8"}},
            {"action": "addObject", "body": {"objectID": "9", "name": "Product 9"}},
            {"action": "addObject", "body": {"objectID": "10", "name": "Product 10"}}
        ]})),
    );
    let resp = app.clone().oneshot(batch_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let batch_body = body_json(resp).await;
    let task_id = batch_body["taskID"].as_i64().unwrap();

    // Wait for task
    common::wait_for_task_local(&app, task_id).await;

    // Create key with maxHitsPerQuery: 3
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "Max hits test",
            "maxHitsPerQuery": 3
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Search with the capped key (requesting hitsPerPage=20, but should be capped to 3)
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/products/query",
        &key_value,
        Some(json!({"query": "", "hitsPerPage": 20})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let hits = body["hits"].as_array().unwrap();
    assert!(
        hits.len() <= 3,
        "maxHitsPerQuery:3 should cap hits to 3, got {}",
        hits.len()
    );
}

#[tokio::test]
async fn index_restriction_rejects_disallowed_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create key restricted to "products" only
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "indexes": ["products"],
            "description": "Products only"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Try to search "orders" index → 403
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/orders/query",
        &key_value,
        Some(json!({"query": "test"})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "key restricted to 'products' should be rejected for 'orders'"
    );
    let body = body_json(resp).await;
    assert!(
        body.get("message").is_some(),
        "403 should be JSON error with message"
    );
}

#[tokio::test]
async fn referers_restriction_rejects_disallowed_referer() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create key with referers restriction
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "referers": ["*.example.com"],
            "description": "Referer restricted"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Request with disallowed referer → 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/query")
        .header("x-algolia-api-key", &key_value)
        .header("x-algolia-application-id", "test")
        .header("content-type", "application/json")
        .header("referer", "https://evil.com/page")
        .body(Body::from(json!({"query": "test"}).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "disallowed referer should be rejected"
    );

    // Request with allowed referer → should pass auth (may 404 on index, that's OK)
    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/query")
        .header("x-algolia-api-key", &key_value)
        .header("x-algolia-application-id", "test")
        .header("content-type", "application/json")
        .header("referer", "https://shop.example.com/page")
        .body(Body::from(json!({"query": "test"}).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    // Should NOT be 403 (may be 200 or 404 depending on index existence)
    assert_ne!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "allowed referer should pass auth"
    );
}

#[tokio::test]
async fn rate_limit_returns_429_when_exceeded() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create key with maxQueriesPerIPPerHour: 2
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "maxQueriesPerIPPerHour": 2,
            "description": "Rate limited"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Seed an index so search works
    let batch_req = authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(
            json!({"requests": [{"action": "addObject", "body": {"objectID": "1", "name": "Test"}}]}),
        ),
    );
    let resp = app.clone().oneshot(batch_req).await.unwrap();
    let task_id = body_json(resp).await["taskID"].as_i64().unwrap();
    common::wait_for_task_local(&app, task_id).await;

    // First 2 requests should succeed
    for i in 0..2 {
        let search_req = authed_request(
            Method::POST,
            "/1/indexes/products/query",
            &key_value,
            Some(json!({"query": "test"})),
        );
        let resp = app.clone().oneshot(search_req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "request {} should succeed",
            i + 1
        );
    }

    // 3rd request should be rate limited
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/products/query",
        &key_value,
        Some(json!({"query": "test"})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        429,
        "3rd request should be rate limited with 429"
    );
    let body = body_json(resp).await;
    assert!(body.get("message").is_some(), "429 should have message");
    assert_eq!(body["status"].as_i64().unwrap(), 429);
}

#[tokio::test]
async fn query_parameters_override_user_params() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed an index with data
    let batch_req = authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({"requests": [
            {"action": "addObject", "body": {"objectID": "1", "name": "Nike Shoe", "brand": "Nike"}},
            {"action": "addObject", "body": {"objectID": "2", "name": "Adidas Shoe", "brand": "Adidas"}}
        ]})),
    );
    let resp = app.clone().oneshot(batch_req).await.unwrap();
    let task_id = body_json(resp).await["taskID"].as_i64().unwrap();
    common::wait_for_task_local(&app, task_id).await;

    // Create key with forced queryParameters (e.g., hitsPerPage=1)
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "queryParameters": "hitsPerPage=1",
            "description": "Forced params"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Search with user requesting hitsPerPage=20, but key forces hitsPerPage=1
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/products/query",
        &key_value,
        Some(json!({"query": "", "hitsPerPage": 20})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let hits = body["hits"].as_array().unwrap();
    assert!(
        hits.len() <= 1,
        "key queryParameters hitsPerPage=1 should override user's hitsPerPage=20, got {} hits",
        hits.len()
    );
}

#[tokio::test]
async fn secured_key_restrictions_compose_with_parent_key_restrictions() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    for index_name in ["products_us", "products_eu"] {
        let batch_req = authed_request(
            Method::POST,
            &format!("/1/indexes/{index_name}/batch"),
            ADMIN_KEY,
            Some(json!({"requests": [
                {"action": "addObject", "body": {"objectID": format!("{index_name}-1"), "name": "Doc 1"}},
                {"action": "addObject", "body": {"objectID": format!("{index_name}-2"), "name": "Doc 2"}},
                {"action": "addObject", "body": {"objectID": format!("{index_name}-3"), "name": "Doc 3"}},
                {"action": "addObject", "body": {"objectID": format!("{index_name}-4"), "name": "Doc 4"}}
            ]})),
        );
        let resp = app.clone().oneshot(batch_req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let task_id = body_json(resp).await["taskID"].as_i64().unwrap();
        common::wait_for_task_local(&app, task_id).await;
    }

    // Parent key allows products_* and caps max hits at 3.
    let parent_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "indexes": ["products_*"],
            "maxHitsPerQuery": 3,
            "description": "parent key for composition"
        })),
    );
    let resp = app.clone().oneshot(parent_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let parent_key = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Secured key narrows index access and adds a stricter hitsPerPage cap of 2.
    let secured_req = authed_request(
        Method::POST,
        "/1/keys/generateSecuredApiKey",
        ADMIN_KEY,
        Some(json!({
            "parentApiKey": parent_key,
            "restrictions": {
                "restrictIndices": ["products_us"],
                "hitsPerPage": 2
            }
        })),
    );
    let resp = app.clone().oneshot(secured_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let secured_key = body_json(resp).await["securedApiKey"]
        .as_str()
        .unwrap()
        .to_string();

    // Allowed index should pass, and effective cap should be min(parent=3, secured=2) => 2.
    let allowed_req = authed_request(
        Method::POST,
        "/1/indexes/products_us/query",
        &secured_key,
        Some(json!({"query": "", "hitsPerPage": 100})),
    );
    let allowed_resp = app.clone().oneshot(allowed_req).await.unwrap();
    assert_eq!(allowed_resp.status(), StatusCode::OK);
    let allowed_body = body_json(allowed_resp).await;
    let hits = allowed_body["hits"].as_array().unwrap();
    assert!(
        hits.len() <= 2,
        "effective cap should compose to 2 hits, got {}",
        hits.len()
    );

    // Parent allows products_* but secured key narrows to products_us, so products_eu must fail.
    let denied_req = authed_request(
        Method::POST,
        "/1/indexes/products_eu/query",
        &secured_key,
        Some(json!({"query": ""})),
    );
    let denied_resp = app.clone().oneshot(denied_req).await.unwrap();
    assert_eq!(denied_resp.status(), StatusCode::FORBIDDEN);
}

// ─── Bug regression tests ───

/// Bug: update_key didn't preserve hmac_key, so after update the key's value
/// would be empty in list responses and secured key generation would break.
#[tokio::test]
async fn updated_key_preserves_value_in_list_response() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "Before update"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Update the key (change description)
    let update_req = authed_request(
        Method::PUT,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        Some(json!({"acl": ["search", "browse"], "description": "After update"})),
    );
    let resp = app.clone().oneshot(update_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET the key - value should still be populated
    let get_req = authed_request(
        Method::GET,
        &format!("/1/keys/{}", key_value),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(
        body["value"].as_str().unwrap(),
        key_value,
        "value must survive key update"
    );
    assert_eq!(body["description"].as_str().unwrap(), "After update");

    // Also check in list response
    let list_req = authed_request(Method::GET, "/1/keys", ADMIN_KEY, None);
    let resp = app.clone().oneshot(list_req).await.unwrap();
    let list_body = body_json(resp).await;
    let keys = list_body["keys"].as_array().unwrap();
    let updated_key = keys
        .iter()
        .find(|k| k["description"] == "After update")
        .unwrap();
    assert_eq!(
        updated_key["value"].as_str().unwrap(),
        key_value,
        "value in list response must survive key update"
    );
}

/// Bug: batch search (POST /1/indexes/*/queries) with index-restricted key
/// was rejected at the middleware level because extract_index_name returned "*"
/// which didn't match the key's restricted indexes.
#[tokio::test]
async fn batch_search_with_index_restricted_key_works_for_allowed_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed "products" index
    let batch_req = authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({"requests": [
            {"action": "addObject", "body": {"objectID": "1", "name": "Widget"}}
        ]})),
    );
    let resp = app.clone().oneshot(batch_req).await.unwrap();
    let task_id = body_json(resp).await["taskID"].as_i64().unwrap();
    common::wait_for_task_local(&app, task_id).await;

    // Create key restricted to "products"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "indexes": ["products"],
            "description": "Batch search restricted key"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Batch search for allowed index should work
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/*/queries",
        &key_value,
        Some(json!({"requests": [{"indexName": "products", "query": ""}]})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "batch search for allowed index 'products' should succeed with index-restricted key"
    );
}

/// Bug: batch search handler didn't check base key indexes restriction per-query,
/// so after fixing the middleware bypass, disallowed indexes in batch queries
/// would slip through.
#[tokio::test]
async fn batch_search_with_index_restricted_key_rejects_disallowed_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create key restricted to "products"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "indexes": ["products"],
            "description": "Batch restricted key"
        })),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // Batch search for disallowed index should fail with 403
    let search_req = authed_request(
        Method::POST,
        "/1/indexes/*/queries",
        &key_value,
        Some(json!({"requests": [{"indexName": "orders", "query": "test"}]})),
    );
    let resp = app.clone().oneshot(search_req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "batch search for disallowed index 'orders' should be rejected with 403"
    );
}

// ─── 2D: ACL Matrix Enforcement — Parity-Critical Routes ───

#[tokio::test]
async fn recommendations_route_requires_recommendation_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key with only "search" ACL — NOT "recommendation"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "search-only key"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // POST to recommendations route must be rejected with 403
    let req = authed_request(
        Method::POST,
        "/1/indexes/products/recommendations",
        &key_value,
        Some(
            json!({"requests": [{"indexName": "products", "model": "related-products", "objectID": "obj1"}]}),
        ),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied recommendations route (requires 'recommendation' ACL)"
    );
    let body = body_json(resp).await;
    assert!(
        body.get("message").is_some(),
        "403 must return Algolia-shaped JSON with 'message', got: {}",
        body
    );
    assert_eq!(
        body["status"].as_u64(),
        Some(403),
        "403 body must include status:403, got: {}",
        body
    );

    // A key with "recommendation" ACL must be allowed through (may get 404 on index, that's ok)
    let create_req2 = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["recommendation"], "description": "recommendation key"})),
    );
    let resp2 = app.clone().oneshot(create_req2).await.unwrap();
    let rec_key = body_json(resp2).await["key"].as_str().unwrap().to_string();

    let req2 = authed_request(
        Method::POST,
        "/1/indexes/products/recommendations",
        &rec_key,
        Some(json!({"requests": []})),
    );
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_ne!(
        resp2.status(),
        StatusCode::FORBIDDEN,
        "recommendation key must NOT be denied recommendations route"
    );
}

#[tokio::test]
async fn analytics_route_requires_analytics_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key with only "search" ACL — NOT "analytics"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "search-only key"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // GET /2/searches must be rejected with 403
    let req = authed_request(Method::GET, "/2/searches", &key_value, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied /2/searches (requires 'analytics' ACL)"
    );
    let body = body_json(resp).await;
    assert!(
        body.get("message").is_some(),
        "403 must return Algolia-shaped JSON with 'message', got: {}",
        body
    );
    assert_eq!(
        body["status"].as_u64(),
        Some(403),
        "403 body must include status:403, got: {}",
        body
    );

    // Also verify /2/countries is gated
    let req2 = authed_request(Method::GET, "/2/countries", &key_value, None);
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(
        resp2.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied /2/countries (requires 'analytics' ACL)"
    );

    // A key with "analytics" ACL must be allowed through (may get empty results, that's ok)
    let create_req2 = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["analytics"], "description": "analytics key"})),
    );
    let resp3 = app.clone().oneshot(create_req2).await.unwrap();
    let analytics_key = body_json(resp3).await["key"].as_str().unwrap().to_string();

    let req3 = authed_request(Method::GET, "/2/searches", &analytics_key, None);
    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_ne!(
        resp3.status(),
        StatusCode::FORBIDDEN,
        "analytics key must NOT be denied /2/searches"
    );
}

#[tokio::test]
async fn query_suggestions_logs_requires_logs_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key with only "search" ACL — NOT "logs"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "search-only key"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // GET /1/logs/:indexName must be rejected with 403
    let req = authed_request(Method::GET, "/1/logs/products", &key_value, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied /1/logs/products (requires 'logs' ACL)"
    );
    let body = body_json(resp).await;
    assert!(
        body.get("message").is_some(),
        "403 must return Algolia-shaped JSON with 'message', got: {}",
        body
    );
    assert_eq!(
        body["status"].as_u64(),
        Some(403),
        "403 body must include status:403, got: {}",
        body
    );

    // A key with "logs" ACL must be allowed through
    let create_req2 = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["logs"], "description": "logs key"})),
    );
    let resp2 = app.clone().oneshot(create_req2).await.unwrap();
    let logs_key = body_json(resp2).await["key"].as_str().unwrap().to_string();

    let req2 = authed_request(Method::GET, "/1/logs/products", &logs_key, None);
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_ne!(
        resp2.status(),
        StatusCode::FORBIDDEN,
        "logs key must NOT be denied /1/logs/products"
    );
}

#[tokio::test]
async fn query_suggestions_config_requires_settings_or_edit_settings_acl() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key with only "search" ACL — NOT "settings" or "editSettings"
    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["search"], "description": "search-only key"})),
    );
    let resp = app.clone().oneshot(create_req).await.unwrap();
    let key_value = body_json(resp).await["key"].as_str().unwrap().to_string();

    // GET /1/configs must be rejected with 403 (requires "settings")
    let req = authed_request(Method::GET, "/1/configs", &key_value, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied GET /1/configs (requires 'settings' ACL)"
    );
    let body = body_json(resp).await;
    assert!(
        body.get("message").is_some(),
        "403 must return Algolia-shaped JSON with 'message', got: {}",
        body
    );
    assert_eq!(
        body["status"].as_u64(),
        Some(403),
        "403 body must include status:403, got: {}",
        body
    );

    // POST /1/configs must be rejected with 403 (requires "editSettings")
    let req2 = authed_request(Method::POST, "/1/configs", &key_value, Some(json!({})));
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(
        resp2.status(),
        StatusCode::FORBIDDEN,
        "search-only key must be denied POST /1/configs (requires 'editSettings' ACL)"
    );

    // A key with "settings" ACL must be allowed through GET /1/configs
    let create_req2 = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["settings"], "description": "settings key"})),
    );
    let resp3 = app.clone().oneshot(create_req2).await.unwrap();
    let settings_key = body_json(resp3).await["key"].as_str().unwrap().to_string();

    let req3 = authed_request(Method::GET, "/1/configs", &settings_key, None);
    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_ne!(
        resp3.status(),
        StatusCode::FORBIDDEN,
        "settings key must NOT be denied GET /1/configs"
    );

    // A key with "editSettings" ACL must be allowed through POST /1/configs
    let create_req3 = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({"acl": ["editSettings"], "description": "editSettings key"})),
    );
    let resp4 = app.clone().oneshot(create_req3).await.unwrap();
    let edit_key = body_json(resp4).await["key"].as_str().unwrap().to_string();

    let req4 = authed_request(Method::POST, "/1/configs", &edit_key, Some(json!({})));
    let resp4 = app.clone().oneshot(req4).await.unwrap();
    assert_ne!(
        resp4.status(),
        StatusCode::FORBIDDEN,
        "editSettings key must NOT be denied POST /1/configs"
    );
}
