//! RED/GREEN summary (`cargo test -p flapjack --test test_error_parity`):
//!
//! GREEN (19): all tests pass — error contract is correct for all paths
//!   - delete_nonexistent_index_returns_404: FIXED in manager.rs (added TenantNotFound check)

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Method, Request, Response, StatusCode},
    response::IntoResponse,
};
use flapjack::FlapjackError;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

async fn assert_algolia_error(resp: Response<Body>, expected_status: u16) -> Value {
    common::assert_error_contract_from_oneshot(resp, expected_status).await
}

async fn seed_index(app: &axum::Router, index_name: &str) {
    let batch_req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "addObject",
                    "body": {"objectID": "seeded-object-1", "title": "seeded"}
                },
                {
                    "action": "addObject",
                    "body": {"objectID": "seeded-object-2", "title": "seeded-2"}
                },
            ]
        })),
    );
    let resp = app.clone().oneshot(batch_req).await.unwrap();
    let body = common::body_json(resp).await;
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local(app, task_id).await;
}

async fn create_api_key(app: &axum::Router, key_body: Value) -> String {
    let req = common::authed_request(Method::POST, "/1/keys", ADMIN_KEY, Some(key_body));
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = common::body_json(resp).await;
    body["key"]
        .as_str()
        .unwrap_or_else(|| panic!("expected 'key' in create key response: {body}"))
        .to_string()
}

fn request_with_connect_info(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let mut request = builder.body(body).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
    request
}

#[tokio::test]
async fn invalid_index_name_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = common::authed_request(
        Method::POST,
        "/1/indexes",
        ADMIN_KEY,
        Some(json!({"uid": "../traversal"})),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn malformed_json_search_body_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "malformed-body-index";
    seed_index(&app, index_name).await;

    let req = request_with_connect_info(
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        &[
            ("x-algolia-application-id", "test"),
            ("x-algolia-api-key", ADMIN_KEY),
            ("content-type", "application/json"),
        ],
        Body::from("{\"query\":"),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn invalid_batch_action_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "invalid-batch-action-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "noSuchAction",
                    "body": {"objectID": "x"}
                }
            ]
        })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn missing_body_in_batch_add_object_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "missing-batch-body-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({"requests": [{"action": "addObject"}]})),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn missing_object_id_in_batch_update_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "missing-object-id-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({
            "requests": [{
                "action": "updateObject",
                "body": {"name": "x"}
            }]
        })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn search_hits_per_page_exceeds_max_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "hits-per-page-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        ADMIN_KEY,
        Some(json!({"hitsPerPage": 9999})),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn delete_by_query_missing_filters_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "delete-by-query-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/deleteByQuery"),
        ADMIN_KEY,
        Some(json!({})),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 400).await;
}

#[tokio::test]
async fn search_nonexistent_index_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = common::authed_request(
        Method::POST,
        "/1/indexes/no-such-index/query",
        ADMIN_KEY,
        Some(json!({"query": "x"})),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn get_object_nonexistent_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "object-not-found-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::GET,
        &format!("/1/indexes/{index_name}/nonexistent-oid"),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn get_rule_nonexistent_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "rule-nonexistent-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::GET,
        &format!("/1/indexes/{index_name}/rules/no-such-rule"),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn get_synonym_nonexistent_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "synonym-nonexistent-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::GET,
        &format!("/1/indexes/{index_name}/synonyms/no-such-syn"),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn get_task_nonexistent_global_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = common::authed_request(Method::GET, "/1/task/999999999", ADMIN_KEY, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn get_task_nonexistent_index_scoped_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "task-scoped-index";
    seed_index(&app, index_name).await;

    let req = common::authed_request(
        Method::GET,
        &format!("/1/indexes/{index_name}/task/999999999"),
        ADMIN_KEY,
        None,
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn delete_nonexistent_index_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = common::authed_request(Method::DELETE, "/1/indexes/no-such-index", ADMIN_KEY, None);
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_algolia_error(resp, 404).await;
}

#[tokio::test]
async fn missing_api_key_returns_403() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = request_with_connect_info(
        Method::GET,
        "/1/indexes",
        &[("x-algolia-application-id", "test")],
        Body::empty(),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = assert_algolia_error(resp, 403).await;
    assert_eq!(
        body,
        json!({"message": "Invalid Application-ID or API key", "status": 403}),
    );
}

#[tokio::test]
async fn invalid_api_key_returns_403() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let req = request_with_connect_info(
        Method::GET,
        "/1/indexes",
        &[
            ("x-algolia-application-id", "test"),
            ("x-algolia-api-key", "bogus"),
        ],
        Body::empty(),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = assert_algolia_error(resp, 403).await;
    assert_eq!(
        body,
        json!({"message": "Invalid Application-ID or API key", "status": 403}),
    );
}

#[tokio::test]
async fn insufficient_acl_returns_403() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "acl-check-index";
    seed_index(&app, index_name).await;

    let key_value = create_api_key(
        &app,
        json!({"acl": ["search"], "description": "search-only"}),
    )
    .await;

    let req = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        &key_value,
        Some(json!({
            "requests": [{"action": "addObject", "body": {"objectID": "blocked", "name": "blocked"}}]
        })),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = assert_algolia_error(resp, 403).await;
    assert_eq!(
        body["message"],
        json!("Method not allowed with this API key"),
    );
}

#[tokio::test]
async fn index_already_exists_error_has_correct_shape() {
    // NOTE: accountCopyIndex not implemented.
    let resp = FlapjackError::IndexAlreadyExists("test".into()).into_response();
    assert_algolia_error(resp, 409).await;
}

#[tokio::test]
async fn rate_limited_returns_429() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_name = "rate-limit-index";
    seed_index(&app, index_name).await;

    let key_value = create_api_key(
        &app,
        json!({
            "acl": ["search"],
            "maxQueriesPerIPPerHour": 1,
            "description": "rate limited key",
        }),
    )
    .await;

    let first_request = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        &key_value,
        Some(json!({"query": "x"})),
    );
    let first_response = app.clone().oneshot(first_request).await.unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);

    let second_request = common::authed_request(
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        &key_value,
        Some(json!({"query": "x"})),
    );
    let second_response = app.clone().oneshot(second_request).await.unwrap();
    assert_algolia_error(second_response, 429).await;
}
