use axum::http::{Method, StatusCode};
use serde_json::json;
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-tenant-isolation";

use common::{authed_request, body_json};

#[tokio::test]
async fn batch_search_restricted_key_rejects_mixed_allowed_and_forbidden_indexes() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "tenant_allowed",
        ADMIN_KEY,
        vec![json!({"objectID": "allowed-1", "name": "Allowed Document"})],
    )
    .await;

    let create_req = authed_request(
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "indexes": ["tenant_allowed"],
            "description": "tenant isolation restricted key"
        })),
    );
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let key_value = body_json(create_resp).await["key"]
        .as_str()
        .expect("create key response must include key")
        .to_string();

    let mixed_batch_req = authed_request(
        Method::POST,
        "/1/indexes/*/queries",
        &key_value,
        Some(json!({
            "requests": [
                {"indexName": "tenant_allowed", "query": "Allowed"},
                {"indexName": "tenant_forbidden", "query": "Forbidden"}
            ]
        })),
    );
    let mixed_batch_resp = app.clone().oneshot(mixed_batch_req).await.unwrap();
    let status = mixed_batch_resp.status();
    let body = common::assert_error_contract_from_oneshot(mixed_batch_resp, 403).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "mixed-index batch search must be denied when any query targets a forbidden index",
    );
    assert_eq!(
        body,
        json!({
            "message": "Invalid Application-ID or API key",
            "status": 403
        }),
        "index-restricted batch rejection must use canonical invalid-credentials envelope",
    );
}
