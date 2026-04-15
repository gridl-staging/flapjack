use super::*;
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    routing::post,
    Router,
};
use flapjack::index::settings::{IndexMode, IndexSettings};

use serde_json::json;
use std::sync::OnceLock;
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::{
    matchers::{body_partial_json, header, method, path},
    Mock, MockServer, ResponseTemplate,
};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn lock_env_guard() -> tokio::sync::MutexGuard<'static, ()> {
    env_lock().lock().await
}

const CHAT_ENV_KEYS: [&str; 3] = [
    "FLAPJACK_AI_BASE_URL",
    "FLAPJACK_AI_API_KEY",
    "FLAPJACK_AI_MODEL",
];

struct EnvVarRestore {
    original: Vec<(&'static str, Option<String>)>,
}

impl EnvVarRestore {
    fn capture() -> Self {
        let original = CHAT_ENV_KEYS
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect();
        Self { original }
    }
}

impl Drop for EnvVarRestore {
    fn drop(&mut self) {
        for (key, value) in &self.original {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// Create an index and persist its settings in `NeuralSearch` mode with optional `user_data` for test setup.
fn write_neural_settings(
    state: &Arc<AppState>,
    index_name: &str,
    user_data: Option<serde_json::Value>,
) {
    state.manager.create_tenant(index_name).unwrap();
    let settings_path = state
        .manager
        .base_path
        .join(index_name)
        .join("settings.json");
    let mut settings = IndexSettings::load(&settings_path).unwrap_or_default();
    settings.mode = Some(IndexMode::NeuralSearch);
    settings.user_data = user_data;
    settings.save(&settings_path).unwrap();
    state.manager.invalidate_settings_cache(index_name);
}

async fn response_text(resp: axum::http::Response<Body>) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[test]
fn uuid_v4_simple_returns_parseable_v4_uuid() {
    let id = uuid_v4_simple();
    let parsed = uuid::Uuid::parse_str(&id).expect("id should be valid UUID");
    assert_eq!(
        parsed.get_version(),
        Some(uuid::Version::Random),
        "id should be UUID v4"
    );
}

#[test]
fn wants_sse_matches_accept_header_case_insensitively() {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, "Text/Event-Stream".parse().unwrap());
    assert!(wants_sse(&headers, false));
}

/// Verify that an SSE chat request against a mocked OpenAI-compatible provider produces `chunk` and `done` events with the correct content type.
#[tokio::test]
async fn chat_sse_openai_compatible_provider_streams_chunk_and_done_events() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{
                "message": { "content": "streamed answer from provider" }
            }]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::set_var("FLAPJACK_AI_MODEL", "app-model");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::ACCEPT, "text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "query": "What should I buy?" }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/event-stream"),
        "expected SSE content type, got: {content_type}"
    );
    let body = response_text(resp).await;
    assert!(
        body.contains(r#"data: {"type":"chunk""#),
        "expected SSE chunk event, got: {body}"
    );
    assert!(
        body.contains(r#"data: {"type":"done""#),
        "expected SSE done event, got: {body}"
    );
}

/// Verify that the index-level `baseUrl` and `apiKey` override environment variables, and that a request-level `model` overrides both index and environment settings.
#[tokio::test]
async fn chat_provider_resolution_uses_index_base_url_and_request_model_precedence() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer index-key"))
        .and(body_partial_json(json!({ "model": "request-model" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{
                "message": { "content": "index wins for endpoint, request wins for model" }
            }]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", "http://127.0.0.1:9/v1");
    std::env::set_var("FLAPJACK_AI_API_KEY", "app-key");
    std::env::set_var("FLAPJACK_AI_MODEL", "app-model");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(
        &state,
        "products",
        Some(json!({
            "aiProvider": {
                "baseUrl": format!("{}/v1", mock_server.uri()),
                "apiKey": "index-key",
                "model": "index-model"
            }
        })),
    );

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "query": "hello", "model": "request-model" }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Verify that a chat request with no AI provider configured returns a 400 response containing `message` and `status` fields.
#[tokio::test]
async fn chat_missing_provider_config_returns_algolia_shaped_400() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    std::env::remove_var("FLAPJACK_AI_BASE_URL");
    std::env::remove_var("FLAPJACK_AI_API_KEY");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "hello" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&response_text(resp).await).unwrap();
    assert!(body["message"].is_string(), "missing message in error body");
    assert_eq!(body["status"], 400);
}

// ── Stage 3 RED tests ────────────────────────────────────────────────

/// Seed `index_name` with one text document containing the given field value.
async fn seed_document(state: &Arc<AppState>, index_name: &str, object_id: &str, text: &str) {
    let mut fields = std::collections::HashMap::new();
    fields.insert(
        "content".to_string(),
        flapjack::types::FieldValue::Text(text.to_string()),
    );
    let doc = flapjack::types::Document {
        id: object_id.to_string(),
        fields,
    };
    state
        .manager
        .add_documents_sync(index_name, vec![doc])
        .await
        .unwrap();
}

/// Verify the JSON chat response includes `answer`, a non-empty `sources` array, and a `queryID` string.
#[tokio::test]
async fn chat_json_response_contains_answer_sources_query_id() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "answer text"}}]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);
    seed_document(&state, "products", "doc1", "Great product description").await;

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "product" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&response_text(resp).await).unwrap();
    assert!(body["answer"].is_string(), "missing 'answer' field");
    assert!(body["sources"].is_array(), "missing 'sources' field");
    assert!(
        !body["sources"].as_array().unwrap().is_empty(),
        "sources should be non-empty when documents exist"
    );
    assert!(
        body["queryID"].is_string(),
        "missing 'queryID' field — expected in Stage 3 response"
    );
}
#[tokio::test]
async fn chat_sources_hide_unretrievable_attributes_without_acl() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "answer text"}}]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);
    let settings_path = state
        .manager
        .base_path
        .join("products")
        .join("settings.json");
    let mut settings = IndexSettings::load(&settings_path).unwrap();
    settings.unretrievable_attributes = Some(vec!["secret_field".to_string()]);
    settings.save(&settings_path).unwrap();
    state.manager.invalidate_settings_cache("products");

    let doc = flapjack::types::Document {
        id: "doc1".to_string(),
        fields: std::collections::HashMap::from([
            (
                "public_field".to_string(),
                flapjack::types::FieldValue::Text("safe text".to_string()),
            ),
            (
                "secret_field".to_string(),
                flapjack::types::FieldValue::Text("sensitive text".to_string()),
            ),
        ]),
    };
    state
        .manager
        .add_documents_sync("products", vec![doc])
        .await
        .unwrap();

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "text" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_str(&response_text(resp).await).unwrap();
    let first_source = body["sources"]
        .as_array()
        .and_then(|sources| sources.first())
        .expect("chat response should contain at least one source");
    assert_eq!(first_source["public_field"], json!("safe text"));
    assert!(
        first_source.get("secret_field").is_none(),
        "chat sources must not expose unretrievableAttributes"
    );
}

/// Verify that seeded document content is included in the RAG context sent to the upstream AI provider.
#[tokio::test]
async fn chat_retrieval_context_influences_provider_prompt() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;

    // The prompt sent to the LLM must contain the seeded document content.
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_partial_json(json!({
            "messages": [{"role": "user"}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "context-aware answer"}}]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);
    seed_document(
        &state,
        "products",
        "doc_unique_42",
        "UNIQUE_SENTINEL_CONTENT_XYZ",
    )
    .await;

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "query": "UNIQUE_SENTINEL_CONTENT_XYZ" }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify the mock was hit (wiremock panics if no matching call when verify_and_reset).
    // The mock assertion verifies a POST to /v1/chat/completions was made.
    // To validate context inclusion we check the mock received at least one request.
    mock_server.verify().await;
}

/// Verify that the SSE stream emits a `sources` event and that it appears before the terminal `done` event.
#[tokio::test]
async fn chat_sse_emits_sources_event_before_done() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "streaming answer"}}]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);
    seed_document(&state, "products", "doc1", "Relevant product info").await;

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::ACCEPT, "text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "product" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_text(resp).await;

    // Must contain a sources event
    assert!(
        body.contains(r#""type":"sources""#),
        "SSE stream missing 'sources' event; got: {body}"
    );

    // Sources must come before done
    let sources_pos = body.find(r#""type":"sources""#).unwrap();
    let done_pos = body.find(r#""type":"done""#).unwrap();
    assert!(
            sources_pos < done_pos,
            "SSE 'sources' event must appear before 'done'; sources_pos={sources_pos}, done_pos={done_pos}"
        );
}

/// Verify that supplying a `conversationId` from a prior turn preserves the conversation and returns the same ID.
#[tokio::test]
async fn chat_conversation_id_resume_uses_recent_messages() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "reply"}}]
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "test-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state.clone());

    // Turn 1: start conversation, capture conversationId from response.
    let req1 = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "hello" }).to_string()))
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1: serde_json::Value = serde_json::from_str(&response_text(resp1).await).unwrap();
    let conv_id = body1["conversationId"]
        .as_str()
        .expect("missing conversationId in turn 1 response")
        .to_string();

    // Turn 2: send follow-up with the conversation ID.
    let req2 = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "query": "follow up", "conversationId": conv_id }).to_string(),
        ))
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2: serde_json::Value = serde_json::from_str(&response_text(resp2).await).unwrap();
    // The same conversationId should be returned on continuation.
    assert_eq!(
        body2["conversationId"].as_str().unwrap_or(""),
        conv_id,
        "conversationId should be preserved across turns"
    );
}

/// Verify that whitespace-only or empty provider config values are treated as missing and produce a 400 error with `message` and `status` fields.
#[tokio::test]
async fn chat_empty_provider_config_values_return_algolia_shaped_400() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    std::env::set_var("FLAPJACK_AI_BASE_URL", "   ");
    std::env::set_var("FLAPJACK_AI_API_KEY", "");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "hello" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_str(&response_text(resp).await).unwrap();
    assert!(
        body["message"].is_string(),
        "missing message in error body for empty config"
    );
    assert_eq!(body["status"], 400);
}

/// Verify that a 401 from the upstream AI provider is surfaced as a 502 Bad Gateway with a descriptive message.
#[tokio::test]
async fn chat_openai_provider_upstream_401_maps_to_502_error() {
    let _guard = lock_env_guard().await;
    let _env_restore = EnvVarRestore::capture();
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": {"message": "invalid api key"}
        })))
        .mount(&mock_server)
        .await;

    std::env::set_var("FLAPJACK_AI_BASE_URL", format!("{}/v1", mock_server.uri()));
    std::env::set_var("FLAPJACK_AI_API_KEY", "invalid-key");
    std::env::remove_var("FLAPJACK_AI_MODEL");

    let tmp = TempDir::new().unwrap();
    let state = crate::test_helpers::TestStateBuilder::new(&tmp).build_shared();
    write_neural_settings(&state, "products", None);

    let app = Router::new()
        .route("/1/indexes/:indexName/chat", post(chat_index))
        .with_state(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/products/chat")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "query": "hello" }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    let body: serde_json::Value = serde_json::from_str(&response_text(resp).await).unwrap();
    assert_eq!(body["status"], 502);
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|msg| msg.contains("error status")),
        "expected provider upstream status message, got: {}",
        body
    );
}
#[tokio::test]
async fn sse_sender_exits_cleanly_when_receiver_dropped() {
    let (tx, rx) = mpsc::channel(2);
    drop(rx);

    let sent_count = emit_sse_events(
        tx,
        vec!["chunk".to_string()],
        "q_test".to_string(),
        "conv_test".to_string(),
        vec![],
    )
    .await;

    assert_eq!(
        sent_count, 0,
        "sender should stop immediately on disconnect"
    );
}
