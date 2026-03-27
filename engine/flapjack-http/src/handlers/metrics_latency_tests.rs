//! Test that request latency metrics are correctly collected and exposed via the metrics handler.
use super::metrics_handler;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use tempfile::TempDir;
use tower::ServiceExt;

/// Verify that request latency histograms are exposed in the metrics endpoint with method, route, and status class labels after processing requests through latency observation middleware.
#[tokio::test]
async fn metrics_handler_includes_latency_histograms_after_seeded_requests() {
    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();

    let app = Router::new()
        .route(
            "/1/indexes/:indexName/query",
            post(|| async { StatusCode::OK }),
        )
        .route("/metrics", get(metrics_handler))
        .with_state(state)
        .layer(axum::middleware::from_fn(
            crate::latency_middleware::observe_request_latency,
        ));

    let seeded_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/1/indexes/metrics_test/query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(seeded_response.status(), StatusCode::OK);

    let metrics_response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();

    assert!(text.contains("flapjack_active_writers"));
    assert!(text.contains("request_duration_seconds"));
    assert!(
        text.lines().any(|line| {
            line.starts_with("request_duration_seconds_count")
                && line.contains("method=\"POST\"")
                && line.contains("route=\"/1/indexes/:indexName/query\"")
                && line.contains("status_class=\"2xx\"")
        }),
        "expected method+route-normalized request_duration_seconds_count in:\n{text}"
    );
}
