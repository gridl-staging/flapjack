use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use utoipa::ToSchema;

use crate::auth::{epoch_millis_to_rfc3339, ApiKey, KeyStore};
use crate::error_response::json_error;

/// Deserialize an incoming API key creation request with Algolia-compatible camelCase field names.
///
/// All fields except `acl` are optional and fall back to sensible defaults (empty strings, zero, or empty vecs) when absent.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateKeyRequest {
    pub acl: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub indexes: Option<Vec<String>>,
    #[serde(default, rename = "maxHitsPerQuery")]
    pub max_hits_per_query: Option<i64>,
    #[serde(default, rename = "maxQueriesPerIPPerHour")]
    pub max_queries_per_ip_per_hour: Option<i64>,
    #[serde(default, rename = "queryParameters")]
    pub query_parameters: Option<String>,
    #[serde(default)]
    pub referers: Option<Vec<String>>,
    #[serde(default, rename = "restrictSources")]
    pub restrict_sources: Option<Vec<String>>,
    #[serde(default)]
    pub validity: Option<i64>,
}

impl CreateKeyRequest {
    /// TODO: Document CreateKeyRequest.into_api_key.
    fn into_api_key(self) -> ApiKey {
        ApiKey {
            hash: String::new(),
            salt: String::new(),
            hmac_key: None,
            created_at: 0,
            acl: self.acl,
            description: self.description.unwrap_or_default(),
            indexes: self.indexes.unwrap_or_default(),
            max_hits_per_query: self.max_hits_per_query.unwrap_or(0),
            max_queries_per_ip_per_hour: self.max_queries_per_ip_per_hour.unwrap_or(0),
            query_parameters: self.query_parameters.unwrap_or_default(),
            referers: self.referers.unwrap_or_default(),
            restrict_sources: self.restrict_sources,
            validity: self.validity.unwrap_or(0),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateKeyResponse {
    pub key: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListKeysResponse {
    pub keys: Vec<crate::auth::KeyApiResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UpdateKeyResponse {
    pub key: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteKeyResponse {
    #[serde(rename = "deletedAt")]
    pub deleted_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RestoreKeyResponse {
    pub key: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GenerateSecuredKeyResponse {
    #[serde(rename = "securedApiKey")]
    pub secured_api_key: String,
}

/// Create a new API key
#[utoipa::path(
    post,
    path = "/1/keys",
    tag = "keys",
    request_body(content = CreateKeyRequest, description = "Key configuration"),
    responses(
        (status = 200, description = "Key created", body = CreateKeyResponse)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn create_key(
    State(key_store): State<Arc<KeyStore>>,
    Json(body): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    if let Some(response) = validate_create_or_update_key_request(&body) {
        return response;
    }

    let key = body.into_api_key();
    let description = key.description.clone();
    let (created, plaintext_value) = key_store.create_key(key);

    notify_key_lifecycle(&description, "created");

    let created_at = epoch_millis_to_rfc3339(created.created_at);

    let response = CreateKeyResponse {
        key: plaintext_value,
        created_at,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// List all API keys
#[utoipa::path(
    get,
    path = "/1/keys",
    tag = "keys",
    responses(
        (status = 200, description = "List of keys", body = ListKeysResponse)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn list_keys(State(key_store): State<Arc<KeyStore>>) -> impl IntoResponse {
    let keys = key_store.list_all_as_dto();
    Json(ListKeysResponse { keys })
}

/// Get an API key by value
#[utoipa::path(
    get,
    path = "/1/keys/{key}",
    tag = "keys",
    params(
        ("key" = String, Path, description = "API key value")
    ),
    responses(
        (status = 200, description = "Key details", body = crate::auth::KeyApiResponse),
        (status = 404, description = "Key not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_key(
    State(key_store): State<Arc<KeyStore>>,
    Path(key_value): Path<String>,
) -> impl IntoResponse {
    match key_store.lookup_as_dto(&key_value) {
        Some(dto) => Json(dto).into_response(),
        None => json_error(StatusCode::NOT_FOUND, "Key not found"),
    }
}

/// Update an API key
#[utoipa::path(
    put,
    path = "/1/keys/{key}",
    tag = "keys",
    params(
        ("key" = String, Path, description = "API key value")
    ),
    request_body(content = CreateKeyRequest, description = "Key updates"),
    responses(
        (status = 200, description = "Key updated", body = UpdateKeyResponse),
        (status = 404, description = "Key not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn update_key(
    State(key_store): State<Arc<KeyStore>>,
    Path(key_value): Path<String>,
    Json(body): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    if let Some(response) = validate_create_or_update_key_request(&body) {
        return response;
    }

    let updated = body.into_api_key();

    match key_store.update_key(&key_value, updated) {
        Some(_) => Json(UpdateKeyResponse {
            key: key_value,
            updated_at: current_timestamp(),
        })
        .into_response(),
        None => json_error(StatusCode::NOT_FOUND, "Key not found"),
    }
}

/// Delete an API key
#[utoipa::path(
    delete,
    path = "/1/keys/{key}",
    tag = "keys",
    params(
        ("key" = String, Path, description = "API key value")
    ),
    responses(
        (status = 200, description = "Key deleted", body = DeleteKeyResponse),
        (status = 404, description = "Key not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_key(
    State(key_store): State<Arc<KeyStore>>,
    Path(key_value): Path<String>,
) -> impl IntoResponse {
    if key_store.is_admin(&key_value) {
        return json_error(StatusCode::FORBIDDEN, "Cannot delete admin key");
    }

    // Look up description before deleting (for notification)
    let description = key_store
        .lookup(&key_value)
        .map(|k| k.description.clone())
        .unwrap_or_default();

    if key_store.delete_key(&key_value) {
        notify_key_lifecycle(&description, "deleted");

        Json(DeleteKeyResponse {
            deleted_at: current_timestamp(),
        })
        .into_response()
    } else {
        json_error(StatusCode::NOT_FOUND, "Key not found")
    }
}

/// Restore a deleted API key
#[utoipa::path(
    post,
    path = "/1/keys/{key}/restore",
    tag = "keys",
    params(
        ("key" = String, Path, description = "API key value")
    ),
    responses(
        (status = 200, description = "Key restored", body = RestoreKeyResponse),
        (status = 404, description = "Key not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn restore_key(
    State(key_store): State<Arc<KeyStore>>,
    Path(key_value): Path<String>,
) -> impl IntoResponse {
    match key_store.restore_key(&key_value) {
        Some(_) => Json(RestoreKeyResponse {
            key: key_value,
            created_at: current_timestamp(),
        })
        .into_response(),
        None => json_error(StatusCode::NOT_FOUND, "Key not found"),
    }
}
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GenerateSecuredKeyRequest {
    pub parent_api_key: String,
    #[serde(default)]
    pub restrictions: SecuredKeyRestrictions,
}

#[derive(Debug, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecuredKeyRestrictions {
    #[serde(default)]
    pub filters: Option<String>,
    #[serde(default)]
    pub valid_until: Option<i64>,
    #[serde(default)]
    pub restrict_indices: Option<Vec<String>>,
    #[serde(default)]
    pub user_token: Option<String>,
    #[serde(default)]
    pub hits_per_page: Option<usize>,
    #[serde(default)]
    pub restrict_sources: Option<String>,
}

impl SecuredKeyRestrictions {
    /// TODO: Document SecuredKeyRestrictions.to_query_params.
    fn to_query_params(&self) -> String {
        let mut params = Vec::new();
        push_encoded_query_param(&mut params, "filters", self.filters.as_deref());
        push_plain_query_param(&mut params, "validUntil", self.valid_until);
        push_json_query_param(
            &mut params,
            "restrictIndices",
            self.restrict_indices.as_ref(),
        );
        push_encoded_query_param(&mut params, "userToken", self.user_token.as_deref());
        push_plain_query_param(&mut params, "hitsPerPage", self.hits_per_page);
        push_encoded_query_param(
            &mut params,
            "restrictSources",
            self.restrict_sources.as_deref(),
        );
        params.join("&")
    }
}

fn validate_create_or_update_key_request(
    body: &CreateKeyRequest,
) -> Option<axum::response::Response> {
    validate_key_request_acls(&body.acl)
        .or_else(|| validate_request_restrict_sources_entries(body.restrict_sources.as_deref()))
}

fn validate_key_request_acls(acls: &[String]) -> Option<axum::response::Response> {
    crate::auth::validate_acls(acls)
        .err()
        .map(|invalid| json_error(StatusCode::BAD_REQUEST, format!("Invalid ACL: {invalid}")))
}

fn validate_request_restrict_sources_entries(
    restrict_sources: Option<&[String]>,
) -> Option<axum::response::Response> {
    restrict_sources.and_then(|entries| {
        crate::auth::validate_restrict_sources_entries(entries)
            .err()
            .map(invalid_restrict_sources_response)
    })
}

fn validate_request_restrict_sources_csv(
    restrict_sources: Option<&str>,
) -> Option<axum::response::Response> {
    restrict_sources.and_then(|entries| {
        crate::auth::validate_restrict_sources_csv(entries)
            .err()
            .map(invalid_restrict_sources_response)
    })
}

fn invalid_restrict_sources_response(invalid: String) -> axum::response::Response {
    json_error(
        StatusCode::BAD_REQUEST,
        format!("Invalid restrictSources entry: {invalid}"),
    )
}

fn notify_key_lifecycle(description: &str, event: &str) {
    if let Some(notifier) = crate::notifications::global_notifier() {
        notifier.send_key_lifecycle(description, event);
    }
}

fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

fn push_encoded_query_param(parts: &mut Vec<String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        parts.push(format!("{key}={}", urlencoding::encode(value)));
    }
}

fn push_plain_query_param<T: Display>(parts: &mut Vec<String>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        parts.push(format!("{key}={value}"));
    }
}

fn push_json_query_param<T: Serialize>(parts: &mut Vec<String>, key: &str, value: Option<&T>) {
    if let Some(value) = value {
        let json = serde_json::to_string(value).unwrap_or_default();
        parts.push(format!("{key}={}", urlencoding::encode(&json)));
    }
}

/// Generate a secured API key with restrictions
#[utoipa::path(
    post,
    path = "/1/keys/generateSecuredApiKey",
    tag = "keys",
    request_body(content = GenerateSecuredKeyRequest, description = "Secured key restrictions"),
    responses(
        (status = 200, description = "Secured key generated", body = GenerateSecuredKeyResponse),
        (status = 400, description = "Invalid request")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn generate_secured_key(
    State(key_store): State<Arc<KeyStore>>,
    Json(body): Json<GenerateSecuredKeyRequest>,
) -> impl IntoResponse {
    if let Some(response) =
        validate_request_restrict_sources_csv(body.restrictions.restrict_sources.as_deref())
    {
        return response;
    }

    // Look up the parent key
    let parent_key = match key_store.lookup(&body.parent_api_key) {
        Some(k) => k,
        None => return json_error(StatusCode::NOT_FOUND, "Parent key not found"),
    };

    // Admin keys cannot be used as parents for secured keys
    if parent_key.hmac_key.is_none() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "Cannot generate secured key from admin key",
        );
    }

    let params_str = body.restrictions.to_query_params();
    // Use the hmac_key for secured key generation
    let secured_key = crate::auth::generate_secured_api_key(&body.parent_api_key, &params_str);

    Json(GenerateSecuredKeyResponse {
        secured_api_key: secured_key,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::body_json;
    use axum::{
        body::Body,
        http::{Method, Request},
        routing::{delete, get, post, put},
        Router,
    };
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    const TEST_ADMIN_KEY: &str = "test-admin-key-12345";

    fn keys_router(key_store: Arc<KeyStore>) -> Router {
        Router::new()
            .route("/1/keys", post(create_key))
            .route("/1/keys", get(list_keys))
            .route("/1/keys/:key", get(get_key))
            .route("/1/keys/:key", put(update_key))
            .route("/1/keys/:key", delete(delete_key))
            .with_state(key_store)
    }

    fn make_test_key_store(tmp: &TempDir) -> Arc<KeyStore> {
        Arc::new(KeyStore::load_or_create(tmp.path(), TEST_ADMIN_KEY))
    }

    /// Assert that a JSON value is a valid RFC 3339 timestamp string.
    fn assert_rfc3339(val: &serde_json::Value, label: &str) {
        assert!(
            val.is_string(),
            "{} should be an RFC 3339 string, got: {}",
            label,
            val
        );
        let s = val.as_str().unwrap();
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap_or_else(|e| panic!("{} '{}' is not valid RFC 3339: {}", label, s, e));
    }

    /// Assert that a JSON value is an integer epoch timestamp.
    fn assert_epoch_millis_integer(val: &serde_json::Value, label: &str) {
        assert!(
            val.is_i64() || val.is_u64(),
            "{} should be an integer epoch timestamp, got: {}",
            label,
            val
        );
    }

    /// Send a POST /1/keys request with a representative key payload and return the parsed JSON response.
    ///
    /// Asserts that the response status is 200 OK before returning.
    async fn create_test_key(app: &Router) -> Value {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "acl": ["search", "browse"],
                            "description": "test key",
                            "indexes": ["idx1"],
                            "maxHitsPerQuery": 100,
                            "maxQueriesPerIPPerHour": 1000,
                            "queryParameters": "tags=public",
                            "referers": ["*.example.com"],
                            "validity": 3600
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        body_json(resp).await
    }

    // T1.1: GET /1/keys/{key} — createdAt is integer epoch timestamp
    /// Verify GET /1/keys/{key} returns `createdAt` as an integer timestamp for SDK compatibility.
    #[tokio::test]
    async fn get_key_created_at_is_epoch_integer() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let created = create_test_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/1/keys/{key_value}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_epoch_millis_integer(&json["createdAt"], "GET key createdAt");
    }

    // T1.2: GET /1/keys — every key's createdAt is integer epoch timestamp
    /// Verify GET /1/keys returns every key's `createdAt` as an integer timestamp.
    #[tokio::test]
    async fn list_keys_created_at_is_epoch_integer() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        // Create an extra key besides the admin key
        create_test_key(&app).await;

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/1/keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        let keys = json["keys"].as_array().expect("keys should be an array");
        assert!(!keys.is_empty(), "should have at least one key");
        for key in keys {
            assert_epoch_millis_integer(&key["createdAt"], "list keys createdAt");
        }
    }

    // T1.3: POST /1/keys — createdAt in response is RFC 3339 string
    #[tokio::test]
    async fn create_key_created_at_is_iso_string() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let created = create_test_key(&app).await;
        assert_rfc3339(&created["createdAt"], "POST key createdAt");
    }

    // T1.4: POST /1/keys — response field is `key`, not `value`
    #[tokio::test]
    async fn create_key_response_field_is_key_not_value() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let created = create_test_key(&app).await;
        assert!(
            created.get("key").is_some(),
            "POST response should have 'key' field"
        );
        assert!(
            created.get("value").is_none(),
            "POST response should NOT have 'value' field"
        );
    }

    // T1.5: GET key — hash, salt, hmac_key never exposed
    /// Verify GET /1/keys/{key} never leaks internal credential fields (`hash`, `salt`, `hmac_key`, `hmacKey`).
    #[tokio::test]
    async fn key_response_never_exposes_hash_salt_hmac() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let created = create_test_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/1/keys/{key_value}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(resp).await;

        assert!(json.get("hash").is_none(), "hash should not be in response");
        assert!(json.get("salt").is_none(), "salt should not be in response");
        assert!(
            json.get("hmac_key").is_none(),
            "hmac_key should not be in response"
        );
        assert!(
            json.get("hmacKey").is_none(),
            "hmacKey should not be in response"
        );
    }

    // T1.6: GET key — all Algolia fields present
    /// Verify GET /1/keys/{key} includes all Algolia-compatible fields: `value`, `createdAt`, `acl`, `description`, `indexes`, `maxHitsPerQuery`, `maxQueriesPerIPPerHour`, `queryParameters`, `referers`, and `validity`.
    #[tokio::test]
    async fn key_response_includes_all_algolia_fields() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let created = create_test_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/1/keys/{key_value}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(resp).await;

        let required_fields = [
            "value",
            "createdAt",
            "acl",
            "description",
            "indexes",
            "maxHitsPerQuery",
            "maxQueriesPerIPPerHour",
            "queryParameters",
            "referers",
            "validity",
        ];
        for field in &required_fields {
            assert!(
                json.get(field).is_some(),
                "Missing required field '{}' in GET key response. Got: {}",
                field,
                json
            );
        }
    }

    // ── Content-Type middleware verification tests ──
    // These prove normalize_content_type middleware is essential for Json<> extractor endpoints.

    fn keys_router_with_middleware(key_store: Arc<KeyStore>) -> Router {
        Router::new()
            .route("/1/keys", post(create_key))
            .route("/1/keys", get(list_keys))
            .route("/1/keys/:key", get(get_key))
            .route("/1/keys/:key", put(update_key))
            .route("/1/keys/:key", delete(delete_key))
            .layer(axum::middleware::from_fn(
                crate::middleware::normalize_content_type,
            ))
            .with_state(key_store)
    }

    fn create_key_json_body() -> String {
        serde_json::json!({
            "acl": ["search"],
            "description": "content-type test key"
        })
        .to_string()
    }

    // ── Notification wiring tests ──

    /// Verify POST /1/keys increments the `key_lifecycle_call_count` on the global notifier.
    #[tokio::test]
    async fn create_key_sends_lifecycle_notification() {
        let service = std::sync::Arc::new(crate::notifications::NotificationService::disabled());
        crate::notifications::init_global_notifier(std::sync::Arc::clone(&service));

        let notifier = crate::notifications::global_notifier().expect("notifier should be set");
        let before = notifier
            .key_lifecycle_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        create_test_key(&app).await;

        let after = notifier
            .key_lifecycle_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after > before,
            "send_key_lifecycle should have been called on create: before={before}, after={after}"
        );
    }

    /// Verify DELETE /1/keys/{key} increments the `key_lifecycle_call_count` on the global notifier.
    #[tokio::test]
    async fn delete_key_sends_lifecycle_notification() {
        let service = std::sync::Arc::new(crate::notifications::NotificationService::disabled());
        crate::notifications::init_global_notifier(std::sync::Arc::clone(&service));

        let notifier = crate::notifications::global_notifier().expect("notifier should be set");

        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        // Create a key first
        let created = create_test_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        let before = notifier
            .key_lifecycle_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        // Delete it
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/1/keys/{key_value}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let after = notifier
            .key_lifecycle_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after > before,
            "send_key_lifecycle should have been called on delete: before={before}, after={after}"
        );
    }

    // T3.1: Without middleware, text/plain content-type is rejected by Json<> extractor
    /// Verify that without `normalize_content_type` middleware, a `text/plain` request body is rejected by Axum's `Json<>` extractor.
    #[tokio::test]
    async fn create_key_text_plain_rejected_without_middleware() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store); // NO middleware

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "text/plain")
                    .body(Body::from(create_key_json_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Axum Json<> extractor rejects non-application/json content types
        assert_ne!(
            resp.status(),
            StatusCode::OK,
            "Without middleware, text/plain should be rejected by Json<> extractor"
        );
    }

    // T3.2: With middleware, text/plain is normalized to application/json and accepted
    /// Verify that with `normalize_content_type` middleware, a `text/plain` content-type is rewritten to `application/json` and the request succeeds.
    #[tokio::test]
    async fn create_key_text_plain_accepted_with_middleware() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router_with_middleware(store); // WITH middleware

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "text/plain")
                    .body(Body::from(create_key_json_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "With middleware, text/plain should be normalized to application/json"
        );
    }

    // T3.3: With middleware, charset variant is normalized and accepted
    /// Verify that with `normalize_content_type` middleware, `application/json; charset=utf-8` is accepted without rejection.
    #[tokio::test]
    async fn create_key_charset_content_type_accepted_with_middleware() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router_with_middleware(store);

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "application/json; charset=utf-8")
                    .body(Body::from(create_key_json_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "application/json; charset=utf-8 must be accepted with middleware"
        );
    }

    // T3.4: With middleware, missing content-type is normalized and accepted
    /// Verify that with `normalize_content_type` middleware, a request with no Content-Type header is accepted and treated as JSON.
    #[tokio::test]
    async fn create_key_no_content_type_accepted_with_middleware() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router_with_middleware(store);

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .body(Body::from(create_key_json_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Missing Content-Type must be accepted with middleware"
        );
    }

    // ── restrictSources CRUD round-trip tests ──

    /// POST /1/keys with restrictSources → GET /1/keys/{key} returns the same array.
    #[tokio::test]
    async fn post_key_with_restrict_sources_round_trips_through_get() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "acl": ["search"],
                            "restrictSources": ["192.168.1.0/24", "10.0.0.1"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let created = body_json(resp).await;
        let key_value = created["key"].as_str().unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/1/keys/{key_value}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(
            json["restrictSources"],
            serde_json::json!(["192.168.1.0/24", "10.0.0.1"])
        );
    }

    /// PUT /1/keys/{key} updates restrictSources; GET /1/keys list includes the updated value.
    #[tokio::test]
    async fn put_key_updates_restrict_sources_visible_in_list() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        // Create a key without restrictSources
        let created = create_test_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        // PUT update with restrictSources
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/1/keys/{key_value}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "acl": ["search"],
                            "restrictSources": ["10.0.0.0/8"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // GET /1/keys list — find the key and verify restrictSources
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/1/keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        let keys = json["keys"].as_array().unwrap();
        let matching = keys
            .iter()
            .find(|k| k["value"].as_str() == Some(key_value))
            .expect("key should appear in list");
        assert_eq!(
            matching["restrictSources"],
            serde_json::json!(["10.0.0.0/8"])
        );
    }

    /// TODO: Document create_key_rejects_malformed_restrict_sources.
    #[tokio::test]
    async fn create_key_rejects_malformed_restrict_sources() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "acl": ["search"],
                            "restrictSources": ["not-a-network"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let json = body_json(resp).await;
        assert!(
            json["message"]
                .as_str()
                .is_some_and(|msg| msg.contains("Invalid restrictSources entry")),
            "error message should call out restrictSources validation: {json}"
        );
    }

    /// TODO: Document update_key_rejects_malformed_restrict_sources.
    #[tokio::test]
    async fn update_key_rejects_malformed_restrict_sources() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = keys_router(store);
        let created = create_test_key(&app).await;
        let key_value = created["key"]
            .as_str()
            .expect("created key should be present");

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/1/keys/{key_value}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "acl": ["search"],
                            "restrictSources": ["bad-source"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let json = body_json(resp).await;
        assert!(
            json["message"]
                .as_str()
                .is_some_and(|msg| msg.contains("Invalid restrictSources entry")),
            "error message should call out restrictSources validation: {json}"
        );
    }

    /// TODO: Document generate_secured_key_rejects_malformed_restrict_sources.
    #[tokio::test]
    async fn generate_secured_key_rejects_malformed_restrict_sources() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let app = Router::new()
            .route("/1/keys/generateSecuredApiKey", post(generate_secured_key))
            .with_state(store.clone());

        let (_, parent_api_key) = store.create_key(ApiKey {
            hash: String::new(),
            salt: String::new(),
            hmac_key: None,
            created_at: 0,
            acl: vec!["search".to_string()],
            description: "parent".to_string(),
            indexes: vec![],
            max_hits_per_query: 0,
            max_queries_per_ip_per_hour: 0,
            query_parameters: String::new(),
            referers: vec![],
            validity: 0,
            restrict_sources: None,
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/1/keys/generateSecuredApiKey")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "parentApiKey": parent_api_key,
                            "restrictions": {
                                "restrictSources": "not-a-network"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let json = body_json(resp).await;
        assert!(
            json["message"]
                .as_str()
                .is_some_and(|msg| msg.contains("Invalid restrictSources entry")),
            "error message should call out restrictSources validation: {json}"
        );
    }

    /// `GenerateSecuredKeyRequest` must preserve `restrictSources` in signed params so auth middleware can enforce source restrictions.
    #[test]
    fn generate_secured_key_request_carries_restrict_sources_into_signed_payload() {
        let tmp = TempDir::new().unwrap();
        let store = make_test_key_store(&tmp);
        let (_, parent_api_key) = store.create_key(ApiKey {
            hash: String::new(),
            salt: String::new(),
            hmac_key: None,
            created_at: 0,
            acl: vec!["search".to_string()],
            description: "parent".to_string(),
            indexes: vec![],
            max_hits_per_query: 0,
            max_queries_per_ip_per_hour: 0,
            query_parameters: String::new(),
            referers: vec![],
            validity: 0,
            restrict_sources: None,
        });

        let request: GenerateSecuredKeyRequest = serde_json::from_value(serde_json::json!({
            "parentApiKey": parent_api_key,
            "restrictions": {
                "restrictSources": "127.0.0.0/8,10.0.0.0/8"
            }
        }))
        .expect("request JSON should deserialize");

        let params_str = request.restrictions.to_query_params();
        let secured = crate::auth::generate_secured_api_key(&request.parent_api_key, &params_str);
        let (_, parsed_restrictions) = crate::auth::validate_secured_key(&secured, &store)
            .expect("generated secured key should validate");

        assert_eq!(
            parsed_restrictions.restrict_sources,
            Some("127.0.0.0/8,10.0.0.0/8".to_string()),
            "signed restrictions must include restrictSources for middleware enforcement"
        );
    }
}
