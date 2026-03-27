mod common;

use axum::http::{Method, StatusCode};
use serde_json::json;
use tower::ServiceExt;

const ADMIN_KEY: &str = "test-admin-key-parity";

#[tokio::test]
async fn internal_ops_missing_tenant_returns_404_with_canonical_error_shape() {
    let (addr, _tmp) = common::spawn_server_with_internal("s02-node-a").await;

    let response = reqwest::get(format!(
        "http://{addr}/internal/ops?tenant_id=no-such&since_seq=0"
    ))
    .await
    .unwrap();

    common::assert_error_contract_from_reqwest(response, StatusCode::NOT_FOUND.as_u16()).await;
}

#[tokio::test]
async fn snapshot_without_s3_env_configured_returns_503_with_canonical_error_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let resp = app
        .clone()
        .oneshot(common::authed_request(
            Method::POST,
            "/1/indexes/no-such/snapshot",
            ADMIN_KEY,
            None,
        ))
        .await
        .unwrap();

    common::assert_error_contract_from_oneshot(resp, StatusCode::SERVICE_UNAVAILABLE.as_u16())
        .await;
}

#[tokio::test]
async fn dictionaries_batch_invalid_dictionary_returns_400_with_canonical_error_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let resp = app
        .clone()
        .oneshot(common::authed_request(
            Method::POST,
            "/1/dictionaries/INVALID/batch",
            ADMIN_KEY,
            Some(json!({"requests":[]})),
        ))
        .await
        .unwrap();

    common::assert_error_contract_from_oneshot(resp, StatusCode::BAD_REQUEST.as_u16()).await;
}

#[tokio::test]
async fn query_suggestions_create_with_invalid_index_name_returns_400_with_canonical_error_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let resp = app
        .clone()
        .oneshot(common::authed_request(
            Method::POST,
            "/1/configs",
            ADMIN_KEY,
            Some(json!({"indexName": "../bad", "sourceIndices": []})),
        ))
        .await
        .unwrap();

    common::assert_error_contract_from_oneshot(resp, StatusCode::BAD_REQUEST.as_u16()).await;
}

#[tokio::test]
async fn migration_list_indexes_missing_credentials_returns_400_with_canonical_error_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let resp = app
        .clone()
        .oneshot(common::authed_request(
            Method::POST,
            "/1/algolia-list-indexes",
            ADMIN_KEY,
            Some(json!({"appId": "", "apiKey": ""})),
        ))
        .await
        .unwrap();

    common::assert_error_contract_from_oneshot(resp, StatusCode::BAD_REQUEST.as_u16()).await;
}

#[tokio::test]
async fn migration_missing_required_fields_returns_400_with_canonical_error_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let resp = app
        .clone()
        .oneshot(common::authed_request(
            Method::POST,
            "/1/migrate-from-algolia",
            ADMIN_KEY,
            Some(json!({"appId": "", "apiKey": "", "sourceIndex": ""})),
        ))
        .await
        .unwrap();

    common::assert_error_contract_from_oneshot(resp, StatusCode::BAD_REQUEST.as_u16()).await;
}
