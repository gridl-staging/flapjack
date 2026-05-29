use axum::{
    body::Body,
    http::{Method, StatusCode},
};
use serde_json::json;

mod common;

// The test app is built in `CorsMode::LoopbackOnly` (see
// `tests/common/state.rs::build_test_app_for_data_dir`), which is the hardened
// default: only loopback origins receive CORS headers. These parity tests
// therefore use a loopback origin so the allow path is exercised, and assert
// the reflected origin exactly. `cors_non_loopback_origin_is_rejected` locks
// the reject path so a regression back to permissive CORS is caught.
const ALLOWED_LOOPBACK_ORIGIN: &str = "http://localhost";
const REJECTED_ORIGIN: &str = "https://example.com";

fn header_value<'a>(resp: &'a axum::http::Response<Body>, name: &str) -> &'a str {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_else(|| panic!("missing header {name}"))
}

#[tokio::test]
async fn cors_preflight_includes_required_headers_and_max_age() {
    let (app, _tmp) = common::build_test_app_for_local_requests(None);

    let resp = common::send_oneshot(
        &app,
        Method::OPTIONS,
        "/1/indexes/cors-stage5d/query",
        &[
            ("origin", ALLOWED_LOOPBACK_ORIGIN),
            ("access-control-request-method", "POST"),
            (
                "access-control-request-headers",
                "x-algolia-api-key, x-algolia-application-id",
            ),
        ],
        Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        header_value(&resp, "access-control-allow-origin"),
        ALLOWED_LOOPBACK_ORIGIN
    );
    // The CORS layer uses `allow_methods(Any)` / `allow_headers(Any)`, so an
    // allowed-origin preflight grants the `*` wildcard for both — POST and the
    // Algolia client headers are therefore permitted.
    assert_eq!(header_value(&resp, "access-control-allow-methods"), "*");
    assert_eq!(header_value(&resp, "access-control-allow-headers"), "*");
    assert_eq!(header_value(&resp, "access-control-max-age"), "86400");
}

#[tokio::test]
async fn cors_private_network_preflight_includes_allow_private_network_true() {
    let (app, _tmp) = common::build_test_app_for_local_requests(None);

    let resp = common::send_oneshot(
        &app,
        Method::OPTIONS,
        "/1/indexes/cors-stage5d/query",
        &[
            ("origin", ALLOWED_LOOPBACK_ORIGIN),
            ("access-control-request-method", "POST"),
            ("access-control-request-private-network", "true"),
        ],
        Body::empty(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        header_value(&resp, "access-control-allow-private-network"),
        "true"
    );
}

#[tokio::test]
async fn cors_non_preflight_post_includes_allow_origin_header() {
    let (app, _tmp) = common::build_test_app_for_local_requests(None);
    common::seed_doc_local(&app, "cors-stage5d").await;

    let resp = common::send_oneshot(
        &app,
        Method::POST,
        "/1/indexes/cors-stage5d/query",
        &[
            ("origin", ALLOWED_LOOPBACK_ORIGIN),
            ("content-type", "application/json"),
        ],
        Body::from(json!({ "query": "alpha" }).to_string()),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        header_value(&resp, "access-control-allow-origin"),
        ALLOWED_LOOPBACK_ORIGIN
    );
    let body = common::parse_response_json(resp).await;
    assert_eq!(body["nbHits"], json!(1));
}

#[tokio::test]
async fn cors_preflight_then_followup_post_flow_succeeds_with_cors_headers() {
    let (app, _tmp) = common::build_test_app_for_local_requests(None);
    common::seed_doc_local(&app, "cors-stage5d-flow").await;

    let options = common::send_oneshot(
        &app,
        Method::OPTIONS,
        "/1/indexes/cors-stage5d-flow/query",
        &[
            ("origin", ALLOWED_LOOPBACK_ORIGIN),
            ("access-control-request-method", "POST"),
            (
                "access-control-request-headers",
                "content-type, x-algolia-api-key, x-algolia-application-id",
            ),
        ],
        Body::empty(),
    )
    .await;
    assert_eq!(options.status(), StatusCode::OK);
    assert_eq!(
        header_value(&options, "access-control-allow-origin"),
        ALLOWED_LOOPBACK_ORIGIN
    );
    assert_eq!(header_value(&options, "access-control-max-age"), "86400");

    let post = common::send_oneshot(
        &app,
        Method::POST,
        "/1/indexes/cors-stage5d-flow/query",
        &[
            ("origin", ALLOWED_LOOPBACK_ORIGIN),
            ("content-type", "application/json"),
            ("x-algolia-api-key", "test-key"),
            ("x-algolia-application-id", "test-app"),
        ],
        Body::from(json!({ "query": "alpha" }).to_string()),
    )
    .await;

    assert_eq!(post.status(), StatusCode::OK);
    assert_eq!(
        header_value(&post, "access-control-allow-origin"),
        ALLOWED_LOOPBACK_ORIGIN
    );
    let post_body = common::parse_response_json(post).await;
    assert_eq!(post_body["nbHits"], json!(1));
}

#[tokio::test]
async fn cors_non_loopback_origin_is_rejected() {
    // Locks the hardened loopback-only contract: a non-loopback origin must not
    // receive an `access-control-allow-origin` header on preflight, so browsers
    // outside the allowlist cannot read cross-origin responses.
    let (app, _tmp) = common::build_test_app_for_local_requests(None);

    let resp = common::send_oneshot(
        &app,
        Method::OPTIONS,
        "/1/indexes/cors-stage5d/query",
        &[
            ("origin", REJECTED_ORIGIN),
            ("access-control-request-method", "POST"),
        ],
        Body::empty(),
    )
    .await;

    assert!(
        resp.headers().get("access-control-allow-origin").is_none(),
        "non-loopback origin must not receive access-control-allow-origin, got: {:?}",
        resp.headers().get("access-control-allow-origin")
    );
}
