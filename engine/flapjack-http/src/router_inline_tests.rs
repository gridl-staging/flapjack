use super::*;
use crate::startup::CorsMode;
use crate::test_helpers::{body_json, send_empty_request, send_json_request, TestStateBuilder};
use axum::body::Body;
use axum::http::{Method, Request};
use axum::routing::post;
use flapjack::analytics::{AnalyticsCollector, AnalyticsConfig};
#[cfg(unix)]
use std::ffi::OsStr;
use std::io;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct SharedLogBuffer {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl SharedLogBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.buffer.lock().unwrap().clone()).unwrap()
    }
}

struct SharedLogWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedLogBuffer {
    type Writer = SharedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedLogWriter {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

fn build_test_router_for_data_dir(
    tmp: &TempDir,
    key_store: Option<Arc<KeyStore>>,
    data_dir: &Path,
) -> Router {
    build_test_router_with_state_for_data_dir(tmp, key_store, data_dir).0
}

/// TODO: Document build_test_router_with_state_for_data_dir.
fn build_test_router_with_state_for_data_dir(
    tmp: &TempDir,
    key_store: Option<Arc<KeyStore>>,
    data_dir: &Path,
) -> (Router, Arc<AppState>) {
    let state = TestStateBuilder::new(tmp).with_analytics().build_shared();
    let analytics_config = AnalyticsConfig {
        enabled: false,
        data_dir: tmp.path().join("analytics"),
        flush_interval_secs: 60,
        flush_size: 1000,
        retention_days: 30,
    };
    let analytics_collector = AnalyticsCollector::new(analytics_config);
    let trusted_proxy_matcher = Arc::new(TrustedProxyMatcher::from_optional_csv(None).unwrap());

    let app = build_router(
        Arc::clone(&state),
        key_store,
        analytics_collector,
        trusted_proxy_matcher,
        CorsMode::Permissive,
        data_dir,
    );

    (app, state)
}

fn build_test_router(tmp: &TempDir, key_store: Option<Arc<KeyStore>>) -> Router {
    build_test_router_for_data_dir(tmp, key_store, tmp.path())
}

fn build_test_router_with_state(
    tmp: &TempDir,
    key_store: Option<Arc<KeyStore>>,
) -> (Router, Arc<AppState>) {
    build_test_router_with_state_for_data_dir(tmp, key_store, tmp.path())
}

async fn body_text(resp: axum::http::Response<axum::body::Body>) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}
/// TODO: Document build_router_open_mode_allows_protected_routes_without_auth_layer.
#[tokio::test]
async fn build_router_open_mode_allows_protected_routes_without_auth_layer() {
    let tmp = TempDir::new().unwrap();
    let app = build_test_router(&tmp, None);

    let create_resp = send_json_request(
        &app,
        Method::POST,
        "/1/indexes",
        serde_json::json!({ "uid": "test" }),
    )
    .await;
    assert_eq!(create_resp.status(), axum::http::StatusCode::OK);

    let search_resp = send_json_request(
        &app,
        Method::POST,
        "/1/indexes/test/query",
        serde_json::json!({ "query": "test" }),
    )
    .await;
    assert_eq!(search_resp.status(), axum::http::StatusCode::OK);

    let body = body_json(search_resp).await;
    assert!(
        body.get("hits").is_some(),
        "search response should include hits"
    );
}
/// TODO: Document build_router_open_mode_allows_dictionary_routes_without_auth_layer.
#[tokio::test]
async fn build_router_open_mode_allows_dictionary_routes_without_auth_layer() {
    let tmp = TempDir::new().unwrap();
    let app = build_test_router(&tmp, None);

    let batch_resp = send_json_request(
        &app,
        Method::POST,
        "/1/dictionaries/stopwords/batch",
        serde_json::json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                {
                    "action": "addEntry",
                    "body": {
                        "objectID": "open-mode-1",
                        "word": "alpha",
                        "language": "en"
                    }
                }
            ]
        }),
    )
    .await;
    assert_eq!(batch_resp.status(), axum::http::StatusCode::OK);

    let search_resp = send_json_request(
        &app,
        Method::POST,
        "/1/dictionaries/stopwords/search",
        serde_json::json!({ "query": "alpha" }),
    )
    .await;
    assert_eq!(search_resp.status(), axum::http::StatusCode::OK);

    let body = body_json(search_resp).await;
    assert_eq!(body["nbHits"].as_u64(), Some(1));
}

#[tokio::test]
async fn build_router_open_mode_does_not_expose_internal_routes() {
    let tmp = TempDir::new().unwrap();
    let app = build_test_router(&tmp, None);

    let response = send_empty_request(&app, Method::GET, "/internal/storage").await;
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
}
/// TODO: Document build_router_does_not_log_trusted_proxy_initialization.
#[tokio::test]
async fn build_router_does_not_log_trusted_proxy_initialization() {
    let tmp = TempDir::new().unwrap();
    let logs = SharedLogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let _ = build_test_router(&tmp, None);
    });

    assert!(
        !logs.contents().contains("Trusted proxy header forwarding"),
        "router construction should not re-log trusted proxy initialization"
    );
}
/// TODO: Document cors_preflight_returns_expected_allow_origin_for_restricted_and_permissive_modes.
#[tokio::test]
async fn cors_preflight_returns_expected_allow_origin_for_restricted_and_permissive_modes() {
    let restricted_router = Router::new()
        .route("/cors", post(|| async { axum::http::StatusCode::OK }))
        .layer(build_cors_layer(&CorsMode::Restricted(vec![
            "https://allowed.example".parse().unwrap(),
        ])));

    let restricted_response = restricted_router
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/cors")
                .header("origin", "https://allowed.example")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let restricted_origin = restricted_response
        .headers()
        .get("access-control-allow-origin")
        .and_then(|value| value.to_str().ok());
    assert_eq!(restricted_origin, Some("https://allowed.example"));

    let permissive_router = Router::new()
        .route("/cors", post(|| async { axum::http::StatusCode::OK }))
        .layer(build_cors_layer(&CorsMode::Permissive));
    let permissive_response = permissive_router
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/cors")
                .header("origin", "https://allowed.example")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let permissive_origin = permissive_response
        .headers()
        .get("access-control-allow-origin")
        .and_then(|value| value.to_str().ok());
    assert_eq!(permissive_origin, Some("https://allowed.example"));
}
/// TODO: Document cors_preflight_rejects_blocked_origins_in_restricted_mode.
#[tokio::test]
async fn cors_preflight_rejects_blocked_origins_in_restricted_mode() {
    let app = Router::new()
        .route("/cors", post(|| async { axum::http::StatusCode::OK }))
        .layer(build_cors_layer(&CorsMode::Restricted(vec![
            "https://allowed.example".parse().unwrap(),
        ])));

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/cors")
                .header("origin", "https://blocked.example")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response
            .headers()
            .get("access-control-allow-origin")
            .is_none(),
        "blocked origin should not receive access-control-allow-origin"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn build_router_accepts_non_utf8_data_dir_paths() {
    let tmp = TempDir::new().unwrap();
    let non_utf8_path = Path::new(OsStr::from_bytes(b"test-\xFF-data"));
    let app = build_test_router_for_data_dir(&tmp, None, non_utf8_path);

    let health_resp = send_empty_request(&app, Method::GET, "/health").await;
    assert_eq!(health_resp.status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn metrics_returns_403_without_auth_headers() {
    let tmp = TempDir::new().unwrap();
    let key_store = Arc::new(KeyStore::load_or_create(tmp.path(), "admin-key"));
    let app = build_test_router(&tmp, Some(key_store));

    let resp = send_empty_request(&app, Method::GET, "/metrics").await;
    assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({
            "message": "Invalid Application-ID or API key",
            "status": 403
        })
    );
}
/// TODO: Document metrics_returns_200_with_admin_key_only.
#[tokio::test]
async fn metrics_returns_200_with_admin_key_only() {
    use axum::body::Body;
    use axum::http::Request;
    use flapjack::types::{Document, FieldValue};
    use std::collections::HashMap;
    use tower::ServiceExt;

    let tmp = TempDir::new().unwrap();
    let key_store = Arc::new(KeyStore::load_or_create(tmp.path(), "admin-key"));
    let (app, state) = build_test_router_with_state(&tmp, Some(key_store));

    state.manager.create_tenant("metrics_auth_tenant").unwrap();
    state
        .manager
        .add_documents_sync(
            "metrics_auth_tenant",
            vec![Document {
                id: "d1".to_string(),
                fields: HashMap::from([(
                    "name".to_string(),
                    FieldValue::Text("router-metrics-seed".to_string()),
                )]),
            }],
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("x-algolia-api-key", "admin-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/plain"),
        "expected Prometheus text/plain, got: {content_type}"
    );

    let body = body_text(resp).await;
    let oplog_line = body
        .lines()
        .find(|line| {
            line.contains("flapjack_oplog_current_seq")
                && line.contains("metrics_auth_tenant")
                && !line.starts_with('#')
        })
        .unwrap_or_else(|| {
            panic!(
                "expected flapjack_oplog_current_seq for metrics_auth_tenant in:\n{}",
                body
            )
        });
    let value: f64 = oplog_line
        .split_whitespace()
        .last()
        .expect("missing metric value")
        .parse()
        .expect("metric value should parse as f64");
    assert!(value > 0.0, "expected positive oplog seq, got: {value}");
}
