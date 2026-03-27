use axum::{
    body::Body,
    http::{Method, StatusCode},
};
use serde_json::json;

mod common;

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
            ("origin", "https://example.com"),
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
    assert!(!header_value(&resp, "access-control-allow-origin").is_empty());
    assert!(
        header_value(&resp, "access-control-allow-methods").contains("POST"),
        "allow methods should include POST"
    );
    let allow_headers = header_value(&resp, "access-control-allow-headers").to_ascii_lowercase();
    assert!(allow_headers.contains("x-algolia-api-key"));
    assert!(allow_headers.contains("x-algolia-application-id"));
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
            ("origin", "https://example.com"),
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
            ("origin", "https://example.com"),
            ("content-type", "application/json"),
        ],
        Body::from(json!({ "query": "alpha" }).to_string()),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!header_value(&resp, "access-control-allow-origin").is_empty());
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
            ("origin", "https://example.com"),
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
    assert!(!header_value(&options, "access-control-allow-origin").is_empty());
    assert_eq!(header_value(&options, "access-control-max-age"), "86400");

    let post = common::send_oneshot(
        &app,
        Method::POST,
        "/1/indexes/cors-stage5d-flow/query",
        &[
            ("origin", "https://example.com"),
            ("content-type", "application/json"),
            ("x-algolia-api-key", "test-key"),
            ("x-algolia-application-id", "test-app"),
        ],
        Body::from(json!({ "query": "alpha" }).to_string()),
    )
    .await;

    assert_eq!(post.status(), StatusCode::OK);
    assert!(!header_value(&post, "access-control-allow-origin").is_empty());
    let post_body = common::parse_response_json(post).await;
    assert_eq!(post_body["nbHits"], json!(1));
}
