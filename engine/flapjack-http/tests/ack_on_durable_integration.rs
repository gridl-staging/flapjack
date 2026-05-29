use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use dashmap::DashMap;
use flapjack::dictionaries::manager::DictionaryManager;
use flapjack::error::FlapjackError;
use flapjack::recommend::RecommendConfig;
use flapjack::types::{Document, FieldValue, TaskStatus};
use flapjack::IndexManager;
use flapjack_http::handlers::metrics::MetricsState;
use flapjack_http::handlers::{add_documents, AppState};
use flapjack_http::idempotency::IdempotencyCache;
use flapjack_http::pause_registry::PausedIndexes;
use flapjack_http::usage_middleware::TenantUsageCounters;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::{sleep, Instant};

// Stage-1 contract anchor: engine/src/index/write_queue/mod.rs defines
// WRITE_QUEUE_CHANNEL_CAPACITY as 2_000 at HEAD, but that constant is private
// to the crate and inaccessible from integration tests.
const WRITE_QUEUE_CHANNEL_CAPACITY: usize = 2_000;

fn make_state(tmp: &TempDir) -> Arc<AppState> {
    let manager = IndexManager::new(tmp.path());
    let dictionary_manager = Arc::new(DictionaryManager::new(tmp.path()));
    manager.set_dictionary_manager(Arc::clone(&dictionary_manager));

    Arc::new(AppState {
        manager,
        key_store: None,
        replication_manager: None,
        ssl_manager: None,
        analytics_engine: None,
        recommend_config: RecommendConfig::default(),
        experiment_store: None,
        dictionary_manager,
        metrics_state: Some(MetricsState::new()),
        usage_counters: Arc::new(DashMap::<String, TenantUsageCounters>::new()),
        usage_persistence: None,
        paused_indexes: PausedIndexes::new(),
        geoip_reader: None,
        notification_service: None,
        start_time: std::time::Instant::now(),
        conversation_store: flapjack_http::conversation_store::ConversationStore::default_shared(),
        embedder_store: Arc::new(flapjack_http::embedder_store::EmbedderStore::new()),
        idempotency_cache: Arc::new(IdempotencyCache::new(Duration::from_secs(300))),
    })
}

async fn post_single_doc(
    state: &Arc<AppState>,
    index: &str,
    object_id: &str,
) -> axum::http::Response<Body> {
    let body = json!({
        "objectID": object_id,
        "title": format!("title-{object_id}"),
    });

    add_documents(
        State(Arc::clone(state)),
        Path(index.to_string()),
        HeaderMap::new(),
        Json(body),
    )
    .await
    .expect("handler should produce response")
}

async fn response_json(resp: axum::http::Response<Body>) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    serde_json::from_slice(&bytes).expect("response body should be valid json")
}

fn extract_numeric_task_id(payload: &Value, context: &str) -> i64 {
    payload["taskID"]
        .as_i64()
        .unwrap_or_else(|| panic!("{context} response should include numeric taskID: {payload}"))
}

async fn wait_for_task_terminal(
    manager: &Arc<IndexManager>,
    numeric_task_id: i64,
    timeout: Duration,
) -> TaskStatus {
    let deadline = Instant::now() + timeout;
    let key = numeric_task_id.to_string();

    loop {
        let status = manager
            .get_task(&key)
            .expect("task should be present")
            .status;
        match status {
            TaskStatus::Enqueued | TaskStatus::Processing => {
                if Instant::now() >= deadline {
                    panic!("task {numeric_task_id} did not reach terminal status in {timeout:?}");
                }
                sleep(Duration::from_millis(10)).await;
            }
            done => return done,
        }
    }
}

fn make_doc(id: usize) -> Document {
    Document {
        id: format!("doc-{id}"),
        fields: std::collections::HashMap::from([(
            "title".to_string(),
            FieldValue::Text(format!("queued-{id}")),
        )]),
    }
}

#[cfg(unix)]
fn set_tenant_permissions_read_only(base: &std::path::Path, tenant: &str) {
    use std::os::unix::fs::PermissionsExt;
    let tenant_path = base.join(tenant);
    let mut perms = std::fs::metadata(&tenant_path)
        .expect("tenant dir metadata should exist")
        .permissions();
    perms.set_mode(0o555);
    std::fs::set_permissions(&tenant_path, perms)
        .expect("tenant dir should become read-only to force commit failure");
}

#[cfg(unix)]
fn set_tenant_permissions_writable(base: &std::path::Path, tenant: &str) {
    use std::os::unix::fs::PermissionsExt;
    let tenant_path = base.join(tenant);
    let mut perms = std::fs::metadata(&tenant_path)
        .expect("tenant dir metadata should exist")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&tenant_path, perms)
        .expect("tenant dir permissions should be restorable");
}

#[tokio::test(flavor = "current_thread")]
async fn test_http_200_implies_tantivy_durable() {
    let tmp = TempDir::new().expect("tempdir should create");
    let state = make_state(&tmp);
    let manager = Arc::clone(&state.manager);
    let tenant = "durability_red";
    manager
        .create_tenant(tenant)
        .expect("tenant should exist before single-doc add path");

    let warmup_resp = post_single_doc(&state, tenant, "warmup-doc").await;
    assert_eq!(
        warmup_resp.status(),
        StatusCode::OK,
        "warm-up write must ACK 200"
    );
    let warmup_payload = response_json(warmup_resp).await;
    let warmup_task = extract_numeric_task_id(&warmup_payload, "warm-up");
    let warmup_status = wait_for_task_terminal(&manager, warmup_task, Duration::from_secs(5)).await;
    assert!(
        matches!(warmup_status, TaskStatus::Succeeded),
        "warm-up write should commit before inducing the defect, got {warmup_status:?}"
    );
    let write_resp = post_single_doc(&state, tenant, "lost-after-200").await;
    assert_eq!(
        write_resp.status(),
        StatusCode::OK,
        "defect-triggering HTTP write must ACK 200"
    );

    drop(state);

    let reopened = IndexManager::new(tmp.path());
    reopened
        .create_tenant(tenant)
        .expect("tenant should reopen from disk");

    let persisted = reopened
        .get_document(tenant, "lost-after-200")
        .expect("document lookup should succeed");

    assert!(
        persisted.is_some(),
        "expected ACKed doc to be durable after restart; HEAD silently drops it"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_queue_full_returns_429_retry_after() {
    let tmp = TempDir::new().expect("tempdir should create");
    let state = make_state(&tmp);
    let tenant = "queue_full_guardrail";

    state
        .manager
        .create_tenant(tenant)
        .expect("tenant should create");

    let mut overflow_error: Option<FlapjackError> = None;

    for i in 0..=WRITE_QUEUE_CHANNEL_CAPACITY {
        let result = state.manager.add_documents(tenant, vec![make_doc(i)]);
        if i < WRITE_QUEUE_CHANNEL_CAPACITY {
            assert!(
                result.is_ok(),
                "enqueue {} should succeed before capacity is exceeded; got {result:?}",
                i + 1
            );
        } else {
            overflow_error = result.err();
        }
    }

    let error = overflow_error.expect("capacity+1 enqueue must fail with QueueFull");
    assert!(
        matches!(error, FlapjackError::QueueFull),
        "overflow must return QueueFull (429 path), got {error:?}"
    );

    let response = error.into_response();
    assert_eq!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "QueueFull must map to HTTP 429"
    );
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok());
    assert_eq!(
        retry_after,
        Some("1"),
        "QueueFull response must include Retry-After: 1"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_commit_failure_returns_5xx() {
    let tmp = TempDir::new().expect("tempdir should create");
    let state = make_state(&tmp);
    let tenant = "commit_fail_red";
    state
        .manager
        .create_tenant(tenant)
        .expect("tenant should exist before single-doc add path");

    let warmup_resp = post_single_doc(&state, tenant, "warmup").await;
    assert_eq!(
        warmup_resp.status(),
        StatusCode::OK,
        "warm-up write must ACK 200"
    );
    let warmup_payload = response_json(warmup_resp).await;
    let warmup_task = extract_numeric_task_id(&warmup_payload, "warm-up");
    let warmup_status =
        wait_for_task_terminal(&state.manager, warmup_task, Duration::from_secs(5)).await;
    assert!(
        matches!(warmup_status, TaskStatus::Succeeded),
        "warm-up write should commit before inducing commit failure, got {warmup_status:?}"
    );

    #[cfg(unix)]
    set_tenant_permissions_read_only(tmp.path(), tenant);

    let write_resp = post_single_doc(&state, tenant, "commit-should-fail").await;
    let write_status = write_resp.status();
    let write_payload = response_json(write_resp).await;
    let write_task = extract_numeric_task_id(&write_payload, "commit-failure write");
    let queued_status =
        wait_for_task_terminal(&state.manager, write_task, Duration::from_secs(5)).await;
    assert!(
        matches!(queued_status, TaskStatus::Failed(_)),
        "filesystem seam must produce an actual failed queued task before asserting HTTP contract, got {queued_status:?}"
    );

    #[cfg(unix)]
    set_tenant_permissions_writable(tmp.path(), tenant);

    assert!(
        write_status.is_server_error(),
        "commit failure must surface as 5xx; got {} (false ACK indicates defect)",
        write_status
    );
}
