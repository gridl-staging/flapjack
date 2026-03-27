//! Observes HTTP request duration and publishes Prometheus metrics with route template, method, and status class labels.

use axum::{
    extract::{MatchedPath, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use once_cell::sync::Lazy;
use prometheus::{core::Collector, proto::MetricFamily, HistogramOpts, HistogramVec};

const REQUEST_DURATION_METRIC_NAME: &str = "request_duration_seconds";
const REQUEST_DURATION_METRIC_HELP: &str = "HTTP request duration in seconds";
const UNMATCHED_ROUTE_LABEL: &str = "unmatched";

static REQUEST_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(REQUEST_DURATION_METRIC_NAME, REQUEST_DURATION_METRIC_HELP),
        &["method", "route", "status_class"],
    )
    .expect("request duration histogram should be constructible")
});

pub fn gather_latency_metric_families() -> Vec<MetricFamily> {
    REQUEST_DURATION_SECONDS
        .collect()
        .into_iter()
        .filter(|family| !family.get_metric().is_empty())
        .collect()
}

fn normalize_route_label(request: &Request) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched_path| matched_path.as_str().to_string())
        .unwrap_or_else(|| UNMATCHED_ROUTE_LABEL.to_string())
}

fn normalize_method_label(request: &Request) -> &str {
    request.method().as_str()
}

fn status_class_label(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "unknown",
    }
}

pub async fn observe_request_latency(request: Request, next: Next) -> Response {
    let method = normalize_method_label(&request).to_string();
    let route = normalize_route_label(&request);
    let start = std::time::Instant::now();
    let response = next.run(request).await;
    let status_class = status_class_label(response.status());
    REQUEST_DURATION_SECONDS
        .with_label_values(&[method.as_str(), route.as_str(), status_class])
        .observe(start.elapsed().as_secs_f64());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, post};
    use axum::Router;
    use prometheus::{Encoder, TextEncoder};
    use tower::ServiceExt;

    fn latency_metrics_text() -> String {
        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&gather_latency_metric_families(), &mut encoded)
            .unwrap();
        String::from_utf8(encoded).unwrap()
    }

    fn assert_observed_route_status(method: &str, route: &str, status_class: &str) {
        let metrics = latency_metrics_text();
        assert!(
            metrics.lines().any(|line| {
                line.starts_with("request_duration_seconds_count")
                    && line.contains(&format!("method=\"{method}\""))
                    && line.contains(&format!("route=\"{route}\""))
                    && line.contains(&format!("status_class=\"{status_class}\""))
            }),
            "missing request_duration_seconds_count for method={method}, route={route}, status_class={status_class}\n{metrics}"
        );
    }

    /// Verify that the route label in latency metrics uses the matched index route template (`/1/indexes/:indexName/query`) instead of the concrete request path.
    #[tokio::test]
    async fn normalize_route_label_uses_matched_index_route_template() {
        let app = Router::new()
            .route(
                "/1/indexes/:indexName/query",
                post(|| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn(observe_request_latency));

        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/1/indexes/products/query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_observed_route_status("POST", "/1/indexes/:indexName/query", "2xx");
    }

    /// Verify that the route label in latency metrics uses the matched key route template (`/1/keys/:key`) instead of the concrete request path.
    #[tokio::test]
    async fn normalize_route_label_uses_matched_key_route_template() {
        let app = Router::new()
            .route("/1/keys/:key", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(observe_request_latency));

        app.oneshot(
            Request::builder()
                .method("GET")
                .uri("/1/keys/abcd1234")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_observed_route_status("GET", "/1/keys/:key", "2xx");
    }

    #[test]
    fn normalize_route_label_uses_fixed_unmatched_fallback() {
        let request = Request::builder()
            .uri("/1/keys/secret-value")
            .body(Body::empty())
            .unwrap();

        let route_label = normalize_route_label(&request);
        assert_eq!(route_label, UNMATCHED_ROUTE_LABEL);
        assert_ne!(route_label, "/1/keys/secret-value");
    }

    #[test]
    fn status_class_label_maps_2xx_4xx_and_5xx() {
        assert_eq!(status_class_label(StatusCode::OK), "2xx");
        assert_eq!(status_class_label(StatusCode::FORBIDDEN), "4xx");
        assert_eq!(status_class_label(StatusCode::INTERNAL_SERVER_ERROR), "5xx");
    }

    #[test]
    fn normalize_method_label_uses_http_method() {
        let request = Request::builder()
            .method("DELETE")
            .uri("/1/indexes/products")
            .body(Body::empty())
            .unwrap();

        assert_eq!(normalize_method_label(&request), "DELETE");
    }
}
