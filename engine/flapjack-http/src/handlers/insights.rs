//! Algolia Insights API-compatible event ingestion, debug event inspection, and GDPR user token deletion handlers.
use axum::{
    extract::{Path, Query, State},
    Json,
};
use std::sync::Arc;

use flapjack::analytics::schema::{validate_user_token, InsightEvent};
use flapjack::analytics::{AnalyticsCollector, DebugEvent};
use flapjack::error::FlapjackError;

const DEBUG_EVENTS_DEFAULT_LIMIT: usize = 100;
const DEBUG_EVENTS_MAX_LIMIT: usize = 1000;
const DEBUG_EVENTS_LIMIT_ERROR: &str = "limit must be a positive integer between 1 and 1000";
const DEBUG_EVENTS_TIME_ERROR: &str =
    "from and until must be non-negative unix timestamps in milliseconds";
const DEBUG_EVENTS_TIME_RANGE_ERROR: &str = "from must be less than or equal to until";

/// POST /1/events - Algolia Insights API compatible event ingestion
#[utoipa::path(post, path = "/1/events", tag = "insights", security(("api_key" = [])))]
pub async fn post_events(
    State(collector): State<Arc<AnalyticsCollector>>,
    Json(body): Json<InsightsRequest>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    if body.events.len() > 1000 {
        return Err(FlapjackError::InvalidQuery(
            "Maximum 1000 events per request".to_string(),
        ));
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut accepted = 0;
    let mut errors: Vec<String> = Vec::new();

    for event in body.events {
        let debug_entry = |http_code: u16, validation_errors: Vec<String>| DebugEvent {
            timestamp_ms: event.timestamp.unwrap_or(now_ms),
            index: event.index.clone(),
            event_type: event.event_type.clone(),
            event_subtype: event.event_subtype.clone(),
            event_name: event.event_name.clone(),
            user_token: event.user_token.clone(),
            object_ids: event.effective_object_ids().to_vec(),
            http_code,
            validation_errors,
        };

        match event.validate() {
            Ok(()) => {
                collector.record_debug_event(debug_entry(200, vec![]));
                collector.record_insight(event);
                accepted += 1;
            }
            Err(e) => {
                collector.record_debug_event(debug_entry(422, vec![e.clone()]));
                errors.push(e);
            }
        }
    }

    if !errors.is_empty() && accepted == 0 {
        return Err(FlapjackError::InvalidQuery(format!(
            "All events rejected: {}",
            errors.join("; ")
        )));
    }

    Ok(Json(serde_json::json!({
        "status": 200,
        "message": "OK"
    })))
}

/// GET /1/events/debug - Return recent events from the debug ring buffer
#[utoipa::path(get, path = "/1/events/debug", tag = "insights", security(("api_key" = [])))]
pub async fn get_debug_events(
    State(collector): State<Arc<AnalyticsCollector>>,
    Query(params): Query<DebugEventsQuery>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    if let Some(status) = params.status.as_deref() {
        if !matches!(status, "ok" | "error") {
            return Err(FlapjackError::InvalidQuery(
                "status must be one of: ok, error".to_string(),
            ));
        }
    }

    let limit = parse_debug_limit(params.limit.as_deref())?;
    let from_timestamp_ms = parse_debug_timestamp(params.from.as_deref())?;
    let until_timestamp_ms = parse_debug_timestamp(params.until.as_deref())?;
    if let (Some(from_ms), Some(until_ms)) = (from_timestamp_ms, until_timestamp_ms) {
        if from_ms > until_ms {
            return Err(FlapjackError::InvalidQuery(
                DEBUG_EVENTS_TIME_RANGE_ERROR.to_string(),
            ));
        }
    }
    let events = collector.get_debug_events(
        limit,
        params.index.as_deref(),
        params.event_type.as_deref(),
        params.status.as_deref(),
        from_timestamp_ms,
        until_timestamp_ms,
    );

    Ok(Json(serde_json::json!({
        "events": events,
        "count": events.len(),
    })))
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugEventsQuery {
    pub limit: Option<String>,
    pub index: Option<String>,
    pub event_type: Option<String>,
    pub status: Option<String>,
    pub from: Option<String>,
    pub until: Option<String>,
}

/// Parse and validate the `limit` query parameter for the debug events endpoint.
///
/// Returns `DEBUG_EVENTS_DEFAULT_LIMIT` when `limit` is `None`. Clamps valid values
/// to `DEBUG_EVENTS_MAX_LIMIT`.
///
/// # Returns
///
/// The parsed limit clamped to `[1, 1000]`, or a validation error for zero,
/// negative, or non-numeric input.
fn parse_debug_limit(limit: Option<&str>) -> Result<usize, FlapjackError> {
    let Some(raw_limit) = limit else {
        return Ok(DEBUG_EVENTS_DEFAULT_LIMIT);
    };

    let parsed_limit = raw_limit
        .parse::<usize>()
        .map_err(|_| FlapjackError::InvalidQuery(DEBUG_EVENTS_LIMIT_ERROR.to_string()))?;

    if parsed_limit == 0 {
        return Err(FlapjackError::InvalidQuery(
            DEBUG_EVENTS_LIMIT_ERROR.to_string(),
        ));
    }

    Ok(parsed_limit.min(DEBUG_EVENTS_MAX_LIMIT))
}

fn parse_debug_timestamp(value: Option<&str>) -> Result<Option<i64>, FlapjackError> {
    let Some(raw_value) = value else {
        return Ok(None);
    };

    let parsed = raw_value
        .parse::<i64>()
        .map_err(|_| FlapjackError::InvalidQuery(DEBUG_EVENTS_TIME_ERROR.to_string()))?;
    if parsed < 0 {
        return Err(FlapjackError::InvalidQuery(
            DEBUG_EVENTS_TIME_ERROR.to_string(),
        ));
    }
    Ok(Some(parsed))
}

/// DELETE /1/usertokens/{userToken} - GDPR deletion for all insight events tied to a user token
///
/// Multi-store cleanup: purges insight events from analytics collector AND
/// deletes the personalization profile cache for the user token. Ordering is
/// deterministic (analytics first, then profile cache) with best-effort
/// semantics — each store is cleaned independently so a failure in one does
/// not block cleanup of the other.
#[utoipa::path(delete, path = "/1/usertokens/{userToken}", tag = "insights",
    params(("userToken" = String, Path, description = "User token to delete")),
    security(("api_key" = [])))]
pub async fn delete_usertoken(
    State(state): State<GdprDeleteState>,
    Path(user_token): Path<String>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_user_token(&user_token).map_err(FlapjackError::InvalidQuery)?;

    // 1. Purge analytics events (in-memory buffer + on-disk Parquet)
    if let Err(e) = state.analytics_collector.purge_user_token(&user_token) {
        tracing::warn!(
            user_token_len = user_token.len(),
            "GDPR delete: failed to purge analytics events: {e}"
        );
    }

    // 2. Delete personalization profile cache
    let profile_store =
        flapjack::personalization::PersonalizationProfileStore::new(&state.profile_store_base_path);
    if let Err(e) = profile_store.delete_profile(&user_token) {
        tracing::warn!(
            user_token_len = user_token.len(),
            "GDPR delete: failed to remove personalization profile: {e}"
        );
    }

    let deleted_at = chrono::Utc::now().to_rfc3339();

    if let Some(notifier) = crate::notifications::global_notifier() {
        notifier.send_gdpr_confirmation(&user_token);
    }

    Ok(Json(serde_json::json!({
        "status": 200,
        "message": "OK",
        "deletedAt": deleted_at
    })))
}

/// State for the GDPR delete endpoint, bundling the analytics collector and
/// the base path needed to construct a PersonalizationProfileStore.
#[derive(Clone)]
pub struct GdprDeleteState {
    pub analytics_collector: Arc<AnalyticsCollector>,
    pub profile_store_base_path: std::path::PathBuf,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct InsightsRequest {
    pub events: Vec<InsightEvent>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{body_json, send_empty_request, send_json_request};
    use axum::{
        http::{Method, StatusCode},
        routing::{delete, get, post},
        Router,
    };
    use flapjack::analytics::{AnalyticsConfig, AnalyticsQueryEngine};
    use serde_json::json;
    use tempfile::TempDir;

    fn test_analytics_config(tmp: &TempDir) -> AnalyticsConfig {
        AnalyticsConfig {
            enabled: true,
            data_dir: tmp.path().join("analytics"),
            flush_interval_secs: 3600,
            flush_size: 10_000,
            retention_days: 90,
        }
    }

    fn app_router(collector: Arc<AnalyticsCollector>) -> Router {
        app_router_with_base(collector, std::path::PathBuf::from("/dev/null"))
    }

    /// Build the test router with a custom profile store base path for GDPR delete integration tests.
    fn app_router_with_base(
        collector: Arc<AnalyticsCollector>,
        profile_store_base_path: std::path::PathBuf,
    ) -> Router {
        let gdpr_state = GdprDeleteState {
            analytics_collector: Arc::clone(&collector),
            profile_store_base_path,
        };
        Router::new()
            .route("/1/events", post(post_events))
            .route("/1/events/debug", get(get_debug_events))
            .with_state(collector)
            .merge(
                Router::new()
                    .route("/1/usertokens/:userToken", delete(delete_usertoken))
                    .with_state(gdpr_state),
            )
    }

    /// Send a one-shot HTTP request with a JSON body to the given router and return the raw response.
    fn assert_rejected_event_status(status: StatusCode) {
        assert!(
            status == StatusCode::BAD_REQUEST || status == StatusCode::UNPROCESSABLE_ENTITY,
            "expected 400 or 422, got {status}"
        );
    }

    /// Verify that DELETE /1/usertokens/:token returns 200 with an RFC 3339 `deletedAt` timestamp.
    #[tokio::test]
    async fn delete_usertoken_returns_ok_with_rfc3339_deleted_at() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/user_123").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["status"], json!(200));
        assert_eq!(body["message"], json!("OK"));
        let deleted_at = body["deletedAt"]
            .as_str()
            .expect("deletedAt should be an RFC3339 timestamp string");
        chrono::DateTime::parse_from_rfc3339(deleted_at)
            .expect("deletedAt should be parseable RFC3339");
    }

    /// Verify that GDPR delete removes the target user's events from Parquet query results while leaving other users' data intact.
    #[tokio::test]
    async fn delete_usertoken_purges_events_from_analytics_queries() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let app = app_router(collector.clone());

        let ingest_body = json!({
            "events": [
                {
                    "eventType": "view",
                    "eventName": "Viewed",
                    "index": "products",
                    "userToken": "delete-me",
                    "objectIDs": ["obj1"]
                },
                {
                    "eventType": "view",
                    "eventName": "Viewed",
                    "index": "products",
                    "userToken": "other_user",
                    "objectIDs": ["obj2"]
                }
            ]
        });
        let ingest_response = send_json_request(&app, Method::POST, "/1/events", ingest_body).await;
        assert_eq!(ingest_response.status(), StatusCode::OK);
        collector.flush_all();

        let delete_response =
            send_empty_request(&app, Method::DELETE, "/1/usertokens/delete-me").await;
        assert_eq!(delete_response.status(), StatusCode::OK);
        let delete_body = body_json(delete_response).await;
        let deleted_at = delete_body["deletedAt"]
            .as_str()
            .expect("deletedAt should be present");
        chrono::DateTime::parse_from_rfc3339(deleted_at)
            .expect("deletedAt should be parseable RFC3339");

        let engine = AnalyticsQueryEngine::new(config);
        let rows = engine
            .query_events(
                "products",
                "SELECT user_token, COUNT(*) as count FROM events GROUP BY user_token ORDER BY user_token",
            )
            .await
            .unwrap();

        assert!(
            !rows
                .iter()
                .any(|row| row.get("user_token") == Some(&json!("delete-me"))),
            "delete-me should be fully removed from events: {rows:?}"
        );
        assert!(
            rows.iter()
                .any(|row| row.get("user_token") == Some(&json!("other_user"))),
            "non-target users should remain present: {rows:?}"
        );
    }

    /// Verify that click events without a `positions` array are rejected with 400 or 422.
    #[tokio::test]
    async fn post_events_rejects_click_without_positions() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "click",
                "eventName": "Product Clicked",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that click events are rejected when the length of `positions` does not match `objectIDs`.
    #[tokio::test]
    async fn post_events_rejects_click_when_positions_count_mismatches_object_ids() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "click",
                "eventName": "Product Clicked",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1", "obj2"],
                "positions": [1]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that a click event with a valid `queryID` and matching `positions`/`objectIDs` counts is accepted.
    #[tokio::test]
    async fn post_events_accepts_click_with_query_id_and_matching_positions() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let query_id = "a".repeat(32);
        let body = json!({
            "events": [{
                "eventType": "click",
                "eventName": "Product Clicked",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"],
                "positions": [1],
                "queryID": query_id
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Verify that user tokens containing characters outside the allowed set (e.g. `@`) are rejected.
    #[tokio::test]
    async fn post_events_rejects_user_token_with_invalid_characters() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "view",
                "eventName": "Viewed Product",
                "index": "products",
                "userToken": "user@email.com",
                "objectIDs": ["obj1"]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that user tokens composed of alphanumerics, hyphens, and underscores are accepted.
    #[tokio::test]
    async fn post_events_accepts_user_token_with_allowed_characters() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "view",
                "eventName": "Viewed Product",
                "index": "products",
                "userToken": "valid-user_123",
                "objectIDs": ["obj1"]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Verify that user tokens exceeding 129 characters are rejected.
    #[tokio::test]
    async fn post_events_rejects_user_token_longer_than_129_chars() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "view",
                "eventName": "Viewed Product",
                "index": "products",
                "userToken": "x".repeat(130),
                "objectIDs": ["obj1"]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that `eventSubtype` is rejected on non-conversion event types such as click.
    #[tokio::test]
    async fn post_events_rejects_event_subtype_on_non_conversion_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "click",
                "eventName": "Product Clicked",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"],
                "positions": [1],
                "eventSubtype": "addToCart"
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that unrecognized `eventSubtype` values on conversion events are rejected.
    #[tokio::test]
    async fn post_events_rejects_invalid_event_subtype_on_conversion_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "conversion",
                "eventName": "Product Purchased",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"],
                "eventSubtype": "invalid"
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());
    }

    /// Verify that `eventSubtype: "purchase"` is accepted on conversion events.
    #[tokio::test]
    async fn post_events_accepts_purchase_event_subtype_on_conversion_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "conversion",
                "eventName": "Product Purchased",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"],
                "eventSubtype": "purchase"
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Verify that `eventSubtype: "addToCart"` is accepted on conversion events.
    #[tokio::test]
    async fn post_events_accepts_add_to_cart_event_subtype_on_conversion_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "conversion",
                "eventName": "Product Added To Cart",
                "index": "products",
                "userToken": "user_123",
                "objectIDs": ["obj1"],
                "eventSubtype": "addToCart"
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn debug_endpoint_returns_empty_when_no_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["count"], 0);
        assert_eq!(body["events"].as_array().unwrap().len(), 0);
    }

    /// Verify that the debug ring buffer records both accepted (200) and rejected (422) events with correct metadata.
    #[tokio::test]
    async fn debug_endpoint_records_valid_and_invalid_events() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        // Send a mix of valid and invalid events
        let ingest_body = json!({
            "events": [
                {
                    "eventType": "view",
                    "eventName": "Viewed Product",
                    "index": "products",
                    "userToken": "user_abc",
                    "objectIDs": ["obj1"]
                },
                {
                    "eventType": "bogus",
                    "eventName": "Bad Event",
                    "index": "products",
                    "userToken": "user_xyz",
                    "objectIDs": ["obj2"]
                }
            ]
        });
        let resp = send_json_request(&app, Method::POST, "/1/events", ingest_body).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Query debug endpoint
        let debug_resp = send_empty_request(&app, Method::GET, "/1/events/debug").await;
        assert_eq!(debug_resp.status(), StatusCode::OK);
        let body = body_json(debug_resp).await;
        assert_eq!(body["count"], 2);

        let events = body["events"].as_array().unwrap();
        // Newest first (reverse chronological)
        let invalid = &events[0];
        assert_eq!(invalid["eventType"], "bogus");
        assert_eq!(invalid["httpCode"], 422);
        assert!(!invalid["validationErrors"].as_array().unwrap().is_empty());

        let valid = &events[1];
        assert_eq!(valid["eventType"], "view");
        assert_eq!(valid["httpCode"], 200);
        assert!(valid["validationErrors"].as_array().unwrap().is_empty());
    }

    /// Verify that the debug endpoint correctly filters events by `index` and `status` query parameters.
    #[tokio::test]
    async fn debug_endpoint_filters_by_index_and_status() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let ingest_body = json!({
            "events": [
                {
                    "eventType": "view",
                    "eventName": "V1",
                    "index": "products",
                    "userToken": "user_a",
                    "objectIDs": ["o1"]
                },
                {
                    "eventType": "view",
                    "eventName": "V2",
                    "index": "orders",
                    "userToken": "user_b",
                    "objectIDs": ["o2"]
                }
            ]
        });
        send_json_request(&app, Method::POST, "/1/events", ingest_body).await;

        // Filter by index
        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?index=products").await;
        let body = body_json(resp).await;
        assert_eq!(body["count"], 1);
        assert_eq!(body["events"][0]["index"], "products");

        // Filter by status=ok (both are valid)
        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?status=ok").await;
        let body = body_json(resp).await;
        assert_eq!(body["count"], 2);

        // Filter by status=error (none are errors)
        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?status=error").await;
        let body = body_json(resp).await;
        assert_eq!(body["count"], 0);
    }

    /// Verify that an unrecognized `status` filter value returns 400 with a descriptive error.
    #[tokio::test]
    async fn debug_endpoint_rejects_invalid_status_filter() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?status=invalid").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = body_json(resp).await;
        let message = body["message"]
            .as_str()
            .expect("error body should include message");
        assert!(
            message.contains("status"),
            "expected status validation message, got: {message}"
        );
    }

    /// Verify that `from` and `until` query parameters filter debug events to the specified millisecond time window.
    #[tokio::test]
    async fn debug_endpoint_filters_by_time_range() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let base = chrono::Utc::now().timestamp_millis() - 10_000;
        let ingest_body = json!({
            "events": [
                {
                    "eventType": "view",
                    "eventName": "older",
                    "index": "products",
                    "userToken": "user_a",
                    "objectIDs": ["o1"],
                    "timestamp": base
                },
                {
                    "eventType": "view",
                    "eventName": "middle",
                    "index": "products",
                    "userToken": "user_a",
                    "objectIDs": ["o2"],
                    "timestamp": base + 1_000
                },
                {
                    "eventType": "view",
                    "eventName": "newer",
                    "index": "products",
                    "userToken": "user_a",
                    "objectIDs": ["o3"],
                    "timestamp": base + 2_000
                }
            ]
        });
        let post_resp = send_json_request(&app, Method::POST, "/1/events", ingest_body).await;
        assert_eq!(post_resp.status(), StatusCode::OK);

        let resp = send_empty_request(
            &app,
            Method::GET,
            &format!(
                "/1/events/debug?from={}&until={}",
                base + 1_000,
                base + 2_000
            ),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;
        assert_eq!(status, StatusCode::OK, "unexpected response body: {body}");
        assert_eq!(body["count"], 2);
        let names: Vec<String> = body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventName"].as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"middle".to_string()));
        assert!(names.contains(&"newer".to_string()));
        assert!(!names.contains(&"older".to_string()));
    }

    /// Verify that `from` greater than `until` returns 400 with a time-range validation error.
    #[tokio::test]
    async fn debug_endpoint_rejects_invalid_time_range() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp =
            send_empty_request(&app, Method::GET, "/1/events/debug?from=2000&until=1000").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = body_json(resp).await;
        let message = body["message"]
            .as_str()
            .expect("error body should include message");
        assert!(
            message.contains("from"),
            "expected time-range validation message, got: {message}"
        );
    }

    /// Verify that `limit=0` returns 400 since the minimum allowed limit is 1.
    #[tokio::test]
    async fn debug_endpoint_rejects_zero_limit() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?limit=0").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = body_json(resp).await;
        let message = body["message"]
            .as_str()
            .expect("error body should include message");
        assert!(
            message.contains("limit"),
            "expected limit validation message, got: {message}"
        );
    }

    /// Verify that a non-numeric `limit` value returns 400 with a limit validation error.
    #[tokio::test]
    async fn debug_endpoint_rejects_non_numeric_limit() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?limit=abc").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = body_json(resp).await;
        let message = body["message"]
            .as_str()
            .expect("error body should include message");
        assert!(
            message.contains("limit"),
            "expected limit validation message, got: {message}"
        );
    }

    /// Verify that a negative `limit` value returns 400 with a limit validation error.
    #[tokio::test]
    async fn debug_endpoint_rejects_negative_limit() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?limit=-1").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = body_json(resp).await;
        let message = body["message"]
            .as_str()
            .expect("error body should include message");
        assert!(
            message.contains("limit"),
            "expected limit validation message, got: {message}"
        );
    }

    /// Verify that the GDPR delete endpoint invokes `send_gdpr_confirmation` on the global notification service.
    #[tokio::test]
    async fn delete_usertoken_sends_gdpr_notification() {
        // Initialize global notifier (OnceLock — only first call wins, which is fine)
        let service = Arc::new(crate::notifications::NotificationService::disabled());
        crate::notifications::init_global_notifier(Arc::clone(&service));

        // Get reference to the global service for counter checks
        let notifier = crate::notifications::global_notifier().expect("notifier should be set");
        let before = notifier
            .gdpr_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let response =
            send_empty_request(&app, Method::DELETE, "/1/usertokens/user_test_gdpr").await;
        assert_eq!(response.status(), StatusCode::OK);

        let after = notifier
            .gdpr_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            after > before,
            "send_gdpr_confirmation should have been called: before={before}, after={after}"
        );
    }

    /// Verify that the `limit` parameter caps the number of returned debug events.
    #[tokio::test]
    async fn debug_endpoint_respects_limit() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let events: Vec<_> = (0..5)
            .map(|i| {
                json!({
                    "eventType": "view",
                    "eventName": format!("V{i}"),
                    "index": "products",
                    "userToken": "user_a",
                    "objectIDs": [format!("o{i}")]
                })
            })
            .collect();
        send_json_request(&app, Method::POST, "/1/events", json!({ "events": events })).await;

        let resp = send_empty_request(&app, Method::GET, "/1/events/debug?limit=2").await;
        let body = body_json(resp).await;
        assert_eq!(body["count"], 2);
    }

    // ── Stage D: GDPR Multi-Store Deletion Tests ──

    /// Verify that GDPR delete removes the target user's personalization profile from disk.
    #[tokio::test]
    async fn gdpr_delete_usertoken_removes_personalization_profile_cache() {
        use flapjack::personalization::{PersonalizationProfile, PersonalizationProfileStore};
        use std::collections::BTreeMap;

        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));

        // Seed a personalization profile for the target user
        let store = PersonalizationProfileStore::new(tmp.path());
        let mut brand_scores = BTreeMap::new();
        brand_scores.insert("Nike".to_string(), 15);
        let mut scores = BTreeMap::new();
        scores.insert("brand".to_string(), brand_scores);
        store
            .save_profile(&PersonalizationProfile {
                user_token: "gdpr-target".to_string(),
                last_event_at: Some(chrono::Utc::now().to_rfc3339()),
                scores,
            })
            .unwrap();
        assert!(
            store.load_profile("gdpr-target").unwrap().is_some(),
            "profile should exist before GDPR delete"
        );

        let app = app_router_with_base(collector, tmp.path().to_path_buf());

        let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/gdpr-target").await;
        assert_eq!(response.status(), StatusCode::OK);

        // Profile cache must be removed after GDPR delete
        assert!(
            store.load_profile("gdpr-target").unwrap().is_none(),
            "personalization profile should be deleted after GDPR usertoken delete"
        );
    }

    /// Verify that GDPR delete for one user leaves another user's analytics events and personalization profile intact.
    #[tokio::test]
    async fn gdpr_delete_usertoken_keeps_other_users_data() {
        use flapjack::personalization::{PersonalizationProfile, PersonalizationProfileStore};
        use std::collections::BTreeMap;

        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let app = app_router_with_base(Arc::clone(&collector), tmp.path().to_path_buf());

        // Ingest events for two users
        let ingest_body = json!({
            "events": [
                {
                    "eventType": "view",
                    "eventName": "Viewed",
                    "index": "products",
                    "userToken": "target-user",
                    "objectIDs": ["obj1"]
                },
                {
                    "eventType": "view",
                    "eventName": "Viewed",
                    "index": "products",
                    "userToken": "safe-user",
                    "objectIDs": ["obj2"]
                }
            ]
        });
        let resp = send_json_request(&app, Method::POST, "/1/events", ingest_body).await;
        assert_eq!(resp.status(), StatusCode::OK);
        collector.flush_all();

        // Seed profiles for both users
        let store = PersonalizationProfileStore::new(tmp.path());
        for token in &["target-user", "safe-user"] {
            let mut scores = BTreeMap::new();
            let mut brand = BTreeMap::new();
            brand.insert("Nike".to_string(), 10);
            scores.insert("brand".to_string(), brand);
            store
                .save_profile(&PersonalizationProfile {
                    user_token: token.to_string(),
                    last_event_at: Some(chrono::Utc::now().to_rfc3339()),
                    scores,
                })
                .unwrap();
        }

        // Delete only target-user
        let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/target-user").await;
        assert_eq!(response.status(), StatusCode::OK);

        // safe-user data must remain intact
        let engine = AnalyticsQueryEngine::new(config);
        let rows = engine
            .query_events(
                "products",
                "SELECT user_token, COUNT(*) as count FROM events GROUP BY user_token ORDER BY user_token",
            )
            .await
            .unwrap();
        assert!(
            !rows
                .iter()
                .any(|row| row.get("user_token") == Some(&json!("target-user"))),
            "target-user events should be purged: {rows:?}"
        );
        assert!(
            rows.iter()
                .any(|row| row.get("user_token") == Some(&json!("safe-user"))),
            "safe-user events should remain: {rows:?}"
        );

        // safe-user profile must remain
        assert!(
            store.load_profile("safe-user").unwrap().is_some(),
            "safe-user profile should remain after target-user GDPR delete"
        );
        // target-user profile must be deleted
        assert!(
            store.load_profile("target-user").unwrap().is_none(),
            "target-user profile should be removed"
        );
    }

    /// Verify that deleting the same user token twice succeeds both times and returns `deletedAt` on each call.
    #[tokio::test]
    async fn gdpr_delete_usertoken_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router_with_base(collector, tmp.path().to_path_buf());

        // First delete
        let r1 = send_empty_request(&app, Method::DELETE, "/1/usertokens/nonexistent-user").await;
        assert_eq!(r1.status(), StatusCode::OK);
        let body1 = body_json(r1).await;
        assert_eq!(body1["status"], json!(200));

        // Second delete of same token — must still succeed
        let r2 = send_empty_request(&app, Method::DELETE, "/1/usertokens/nonexistent-user").await;
        assert_eq!(r2.status(), StatusCode::OK);
        let body2 = body_json(r2).await;
        assert_eq!(body2["status"], json!(200));
        assert!(
            body2["deletedAt"].as_str().is_some(),
            "second delete should still return deletedAt"
        );
    }

    /// Stage 3 §4: When all events in a batch are invalid, the error response must use the
    /// standard `{ "message": "...", "status": N }` shape with no extra fields leaked.
    #[tokio::test]
    async fn all_invalid_events_rejected_with_standard_error_body_shape() {
        let tmp = TempDir::new().unwrap();
        let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
        let app = app_router(collector);

        let body = json!({
            "events": [{
                "eventType": "click",
                "eventName": "Bad Click",
                "index": "products",
                "userToken": "user@invalid",
                "objectIDs": ["obj1"],
                "positions": [1]
            }]
        });

        let response = send_json_request(&app, Method::POST, "/1/events", body).await;
        assert_rejected_event_status(response.status());

        let error_body = body_json(response).await;
        assert!(
            error_body["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "error body must contain a non-empty message field"
        );
        let status_code = error_body["status"]
            .as_u64()
            .expect("error body must contain a numeric status field");
        assert!(
            status_code == 400 || status_code == 422,
            "status field should match HTTP status: {status_code}"
        );
        let keys: Vec<&str> = error_body
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert!(
            keys.iter().all(|k| *k == "message" || *k == "status"),
            "error body must not contain extra fields: {keys:?}"
        );
    }
}
