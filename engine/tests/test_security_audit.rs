use axum::http::{Method, StatusCode};
use serde_json::json;
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-security-audit";

use common::authed_request;

#[tokio::test]
async fn malformed_secured_keys_return_canonical_403_without_decode_leaks() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Both a non-Base64 token and a short decoded payload must follow the same
    // canonical rejection shape from invalid_api_credentials_error().
    for malformed_key in ["not_base64!!!", "c2hvcnQ="] {
        let req = authed_request(
            Method::POST,
            "/1/indexes/products/query",
            malformed_key,
            Some(json!({"query": "test"})),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = common::assert_error_contract_from_oneshot(resp, 403).await;

        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "malformed credential '{malformed_key}' must be rejected with 403",
        );
        assert_eq!(
            body,
            json!({
                "message": "Invalid Application-ID or API key",
                "status": 403
            }),
            "malformed secured key must not leak decode/parse internals in error payload",
        );
    }
}
