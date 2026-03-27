use super::{metrics_handler, register_billing_usage_gauges};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use prometheus::Registry;
use tempfile::TempDir;
use tower::ServiceExt;

/// The 7 billing usage metric names that form the fjcloud metering contract.
const BILLING_METRIC_NAMES: [&str; 7] = [
    "flapjack_search_requests_total",
    "flapjack_write_operations_total",
    "flapjack_read_requests_total",
    "flapjack_bytes_in_total",
    "flapjack_search_results_total",
    "flapjack_documents_indexed_total",
    "flapjack_documents_deleted_total",
];

/// Extract the numeric value for a metric+index from Prometheus text output.
fn find_metric_value(text: &str, metric_name: &str, index: &str) -> f64 {
    text.lines()
        .find(|line| line.contains(metric_name) && line.contains(index) && !line.starts_with('#'))
        .unwrap_or_else(|| panic!("{}{{index={}}} not found in:\n{}", metric_name, index, text))
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap()
}

/// Poll /metrics on a test app and return the body as a string.
async fn poll_metrics(app: &Router<()>) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

/// Verify all 7 billing usage series are annotated as `gauge` (not `counter`) in
/// Prometheus text format.
#[tokio::test]
async fn billing_series_use_gauge_type_not_counter() {
    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();

    let counters = crate::usage_middleware::TenantUsageCounters::new();
    counters
        .search_count
        .store(1, std::sync::atomic::Ordering::Relaxed);
    state
        .usage_counters
        .insert("type_check".to_string(), counters);

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let text = poll_metrics(&app).await;

    for name in BILLING_METRIC_NAMES {
        let type_line = text
            .lines()
            .find(|line| line.starts_with("# TYPE") && line.contains(name))
            .unwrap_or_else(|| panic!("missing # TYPE line for {}", name));
        assert!(
            type_line.ends_with(" gauge"),
            "{} must be typed as gauge per metering contract, got: {}",
            name,
            type_line
        );
    }
}

/// After daily rollup, all 7 billing usage series must report 0 in /metrics output.
#[tokio::test]
async fn billing_usage_gauges_reset_to_zero_after_rollup() {
    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();

    let counters = crate::usage_middleware::TenantUsageCounters::new();
    counters
        .search_count
        .store(10, std::sync::atomic::Ordering::Relaxed);
    counters
        .write_count
        .store(5, std::sync::atomic::Ordering::Relaxed);
    counters
        .read_count
        .store(3, std::sync::atomic::Ordering::Relaxed);
    counters
        .bytes_in
        .store(1024, std::sync::atomic::Ordering::Relaxed);
    counters
        .search_results_total
        .store(42, std::sync::atomic::Ordering::Relaxed);
    counters
        .documents_indexed_total
        .store(8, std::sync::atomic::Ordering::Relaxed);
    counters
        .documents_deleted_total
        .store(2, std::sync::atomic::Ordering::Relaxed);
    state
        .usage_counters
        .insert("rollup_idx".to_string(), counters);

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    let text_before = poll_metrics(&app).await;
    assert_eq!(
        find_metric_value(&text_before, "flapjack_search_requests_total", "rollup_idx"),
        10.0
    );

    let persistence = crate::usage_persistence::UsagePersistence::new(tmp.path()).unwrap();
    persistence
        .rollup("2026-03-15", &state.usage_counters)
        .unwrap();

    let text_after = poll_metrics(&app).await;
    for name in BILLING_METRIC_NAMES {
        let value = find_metric_value(&text_after, name, "rollup_idx");
        assert_eq!(value, 0.0, "{} must be 0 after rollup, got {}", name, value);
    }
}

/// Two consecutive `/metrics` polls without activity must return identical billing values.
#[tokio::test]
async fn billing_usage_gauges_are_stable_across_consecutive_polls() {
    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();

    let counters = crate::usage_middleware::TenantUsageCounters::new();
    counters
        .search_count
        .store(7, std::sync::atomic::Ordering::Relaxed);
    counters
        .write_count
        .store(3, std::sync::atomic::Ordering::Relaxed);
    state
        .usage_counters
        .insert("stable_idx".to_string(), counters);

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let text1 = poll_metrics(&app).await;
    let text2 = poll_metrics(&app).await;

    for name in BILLING_METRIC_NAMES {
        let value_first = find_metric_value(&text1, name, "stable_idx");
        let value_second = find_metric_value(&text2, name, "stable_idx");
        assert_eq!(
            value_first, value_second,
            "{} changed between polls: {} vs {}",
            name, value_first, value_second
        );
    }
}

/// Verify register_billing_usage_gauges registers exactly 7 metric families.
#[test]
fn register_billing_usage_gauges_populates_seven_series() {
    let registry = Registry::new();
    let usage_counters = dashmap::DashMap::new();
    let counters = crate::usage_middleware::TenantUsageCounters::new();
    counters
        .search_count
        .store(1, std::sync::atomic::Ordering::Relaxed);
    usage_counters.insert("extract_idx".to_string(), counters);

    register_billing_usage_gauges(&registry, &usage_counters);

    let families = registry.gather();
    let family_names: Vec<&str> = families.iter().map(|family| family.get_name()).collect();
    assert_eq!(
        family_names.len(),
        7,
        "billing usage should register exactly 7 metric families, got: {:?}",
        family_names
    );
    for name in BILLING_METRIC_NAMES {
        assert!(
            family_names.contains(&name),
            "missing billing metric family: {}",
            name
        );
    }
}
