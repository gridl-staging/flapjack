//! Algolia-compatible usage statistics endpoints (`GET /1/usage/:statistic` and `GET /1/usage/:statistic/:indexName`) that merge persisted historical snapshots with live in-memory counters and return time-series data points.
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{NaiveDate, Utc};
use dashmap::DashMap;
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::AppState;
use crate::error_response::json_error_parts;
use crate::usage_middleware::TenantUsageCounters;
use crate::usage_persistence::{DailyUsageSnapshot, IndexUsageSnapshot};

/// All supported Algolia usage statistic names.
const KNOWN_STATS: &[&str] = &[
    "search_operations",
    "total_write_operations",
    "total_read_operations",
    "records",
    "bytes_received",
    "search_results_total",
    "documents_deleted",
    "queries_operations",
    "multi_queries_operations",
];

/// Query parameters accepted by all usage endpoints.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageParams {
    #[serde(default = "default_start_date")]
    pub start_date: String,
    #[serde(default = "default_end_date")]
    pub end_date: String,
    /// Granularity of returned data points (currently only "daily" is supported).
    #[serde(default = "default_granularity")]
    pub granularity: String,
}

fn default_start_date() -> String {
    (Utc::now() - chrono::Duration::days(8))
        .format("%Y-%m-%d")
        .to_string()
}

fn default_end_date() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

fn default_granularity() -> String {
    "daily".to_string()
}

type HandlerError = (StatusCode, Json<serde_json::Value>);

/// Parse a comma-separated statistic string into validated canonical names.
/// Returns 400 if any name is unknown.
fn parse_statistics(raw: &str) -> Result<Vec<&'static str>, HandlerError> {
    raw.split(',')
        .map(str::trim)
        .map(|name| {
            KNOWN_STATS
                .iter()
                .copied()
                .find(|s| *s == name)
                .ok_or_else(|| {
                    json_error_parts(
                        StatusCode::BAD_REQUEST,
                        format!("Unknown statistic: {}", name),
                    )
                })
        })
        .collect()
}

/// Read one statistic value from a live counter entry.
fn stat_value(counters: &TenantUsageCounters, stat: &str) -> u64 {
    match stat {
        "search_operations" | "queries_operations" | "multi_queries_operations" => {
            counters.search_count.load(Ordering::Relaxed)
        }
        "total_write_operations" => counters.write_count.load(Ordering::Relaxed),
        "total_read_operations" => counters.read_count.load(Ordering::Relaxed),
        "records" => counters.documents_indexed_total.load(Ordering::Relaxed),
        "bytes_received" => counters.bytes_in.load(Ordering::Relaxed),
        "search_results_total" => counters.search_results_total.load(Ordering::Relaxed),
        "documents_deleted" => counters.documents_deleted_total.load(Ordering::Relaxed),
        _ => 0,
    }
}

/// Read one statistic value from a persisted index snapshot.
fn snapshot_stat(index_stat: &IndexUsageSnapshot, stat: &str) -> u64 {
    match stat {
        "search_operations" | "queries_operations" | "multi_queries_operations" => {
            index_stat.search_operations
        }
        "total_write_operations" => index_stat.total_write_operations,
        "total_read_operations" => index_stat.total_read_operations,
        "records" => index_stat.records,
        "bytes_received" => index_stat.bytes_received,
        "search_results_total" => index_stat.search_results_total,
        "documents_deleted" => index_stat.documents_deleted,
        _ => 0,
    }
}

/// Aggregate a stat from a daily snapshot, optionally filtered to one index.
/// Returns `None` when the index filter names an index not present in the snapshot.
fn snapshot_aggregate(
    snapshot: &DailyUsageSnapshot,
    stat: &str,
    index_filter: Option<&str>,
) -> Option<u64> {
    match index_filter {
        Some(idx) => snapshot.indexes.get(idx).map(|s| snapshot_stat(s, stat)),
        None => {
            if snapshot.indexes.is_empty() {
                None
            } else {
                Some(
                    snapshot
                        .indexes
                        .values()
                        .map(|s| snapshot_stat(s, stat))
                        .sum(),
                )
            }
        }
    }
}

/// Aggregate a stat value across all live counters, or for a single index.
/// Returns `None` when there are no counter entries matching the filter.
fn aggregate_stat(
    counters: &Arc<DashMap<String, TenantUsageCounters>>,
    stat: &str,
    index_filter: Option<&str>,
) -> Option<u64> {
    match index_filter {
        Some(idx) => counters.get(idx).map(|c| stat_value(&c, stat)),
        None => {
            if counters.is_empty() {
                None
            } else {
                Some(counters.iter().map(|c| stat_value(&c, stat)).sum())
            }
        }
    }
}

/// Build the Algolia-shaped usage response, merging historical snapshots with
/// live in-memory counters.
///
/// Historical days become one `{"t": midnight_epoch_ms, "v": value}` entry
/// each.  When `include_live` is true the current live counters are appended
/// as a final data point using the current timestamp.
fn build_response_merged(
    stats: &[&str],
    historical: &[(NaiveDate, DailyUsageSnapshot)],
    live_counters: &Arc<DashMap<String, TenantUsageCounters>>,
    include_live: bool,
    index_filter: Option<&str>,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for stat in stats {
        let mut data_points: Vec<serde_json::Value> = Vec::new();

        for (date, snapshot) in historical {
            if let Some(v) = snapshot_aggregate(snapshot, stat, index_filter) {
                let t = date
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight is always valid")
                    .and_utc()
                    .timestamp_millis();
                data_points.push(serde_json::json!({"t": t, "v": v}));
            }
        }

        if include_live {
            if let Some(v) = aggregate_stat(live_counters, stat, index_filter) {
                let t = Utc::now().timestamp_millis();
                data_points.push(serde_json::json!({"t": t, "v": v}));
            }
        }

        map.insert((*stat).to_string(), serde_json::Value::Array(data_points));
    }

    serde_json::Value::Object(map)
}

/// Resolve query params into historical snapshots + a live flag, then build
/// the response.  Historical days are loaded from persistence (when available);
/// the live flag is set when `endDate` is today or later.
fn build_response_with_range(
    state: &Arc<AppState>,
    stats: &[&str],
    params: &UsageParams,
    index_filter: Option<&str>,
) -> serde_json::Value {
    let today = Utc::now().date_naive();

    let start = NaiveDate::parse_from_str(&params.start_date, "%Y-%m-%d")
        .unwrap_or_else(|_| today - chrono::Duration::days(8));
    let end = NaiveDate::parse_from_str(&params.end_date, "%Y-%m-%d").unwrap_or(today);

    // Load historical snapshots for all completed days in the range.
    let historical: Vec<(NaiveDate, DailyUsageSnapshot)> =
        if let Some(persistence) = &state.usage_persistence {
            // Completed days are strictly before today.
            let hist_end = if end < today {
                end
            } else {
                match today.pred_opt() {
                    Some(d) => d,
                    None => {
                        return build_response_merged(
                            stats,
                            &[],
                            &state.usage_counters,
                            true,
                            index_filter,
                        )
                    }
                }
            };
            if start <= hist_end {
                persistence
                    .load_date_range(start, hist_end)
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

    let include_live = end >= today;

    build_response_merged(
        stats,
        &historical,
        &state.usage_counters,
        include_live,
        index_filter,
    )
}

/// `GET /1/usage/:statistic`
///
/// Returns usage statistics aggregated across all indexes.
/// `:statistic` may be a comma-separated list of statistic names.
#[utoipa::path(get, path = "/1/usage/{statistic}", tag = "usage",
    params(("statistic" = String, Path, description = "Statistic name (comma-separated)")),
    security(("api_key" = [])))]
pub async fn usage_global(
    State(state): State<Arc<AppState>>,
    Path(statistic): Path<String>,
    Query(params): Query<UsageParams>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    let stats = parse_statistics(&statistic)?;
    Ok(Json(build_response_with_range(
        &state, &stats, &params, None,
    )))
}

/// `GET /1/usage/:statistic/:indexName`
///
/// Returns usage statistics filtered to a single index.
/// `:statistic` may be a comma-separated list of statistic names.
#[utoipa::path(get, path = "/1/usage/{statistic}/{indexName}", tag = "usage",
    params(
        ("statistic" = String, Path, description = "Statistic name (comma-separated)"),
        ("indexName" = String, Path, description = "Index name"),
    ),
    security(("api_key" = [])))]
pub async fn usage_per_index(
    State(state): State<Arc<AppState>>,
    Path((statistic, index_name)): Path<(String, String)>,
    Query(params): Query<UsageParams>,
) -> Result<Json<serde_json::Value>, HandlerError> {
    let stats = parse_statistics(&statistic)?;
    Ok(Json(build_response_with_range(
        &state,
        &stats,
        &params,
        Some(&index_name),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::body_json;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware,
        routing::get,
        Router,
    };
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;
    use tower::ServiceExt;

    /// Create a minimal `AppState` for usage tests with no persistence layer and empty counters.
    fn make_state(tmp: &TempDir) -> Arc<AppState> {
        let mut state = crate::test_helpers::TestStateBuilder::new(tmp).build();
        state.metrics_state = None;
        Arc::new(state)
    }

    fn usage_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/1/usage/:statistic", get(usage_global))
            .route("/1/usage/:statistic/:indexName", get(usage_per_index))
            .with_state(state)
    }

    async fn get_usage(app: Router, path: &str) -> axum::response::Response {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        app.oneshot(req).await.unwrap()
    }

    // ── Red tests ──

    /// Verify the `search_operations` statistic returns a single data point with the correct count and a numeric timestamp.
    #[tokio::test]
    async fn usage_search_operations_returns_daily_counts() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        state
            .usage_counters
            .entry("my_index".to_string())
            .or_default()
            .search_count
            .fetch_add(1, Ordering::Relaxed);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/search_operations").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["v"].as_u64().unwrap(), 1);
        assert!(arr[0]["t"].is_number());
    }

    /// Verify the `total_write_operations` statistic returns the accumulated write count from live counters.
    #[tokio::test]
    async fn usage_total_write_operations_returns_counts() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        state
            .usage_counters
            .entry("products".to_string())
            .or_default()
            .write_count
            .fetch_add(3, Ordering::Relaxed);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/total_write_operations").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["total_write_operations"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["v"].as_u64().unwrap(), 3);
    }

    /// Verify the `records` statistic reflects the `documents_indexed_total` counter value.
    #[tokio::test]
    async fn usage_records_returns_document_count() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        state
            .usage_counters
            .entry("catalog".to_string())
            .or_default()
            .documents_indexed_total
            .fetch_add(42, Ordering::Relaxed);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/records").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["records"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["v"].as_u64().unwrap(), 42);
    }

    #[tokio::test]
    async fn usage_returns_empty_for_no_activity() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);
        // usage_counters is empty — no requests have been tracked

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/search_operations").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert!(arr.is_empty(), "expected empty array when no activity");
    }

    /// Verify that `GET /1/usage/:stat/:indexName` returns only the named index's counter, excluding other indexes.
    #[tokio::test]
    async fn usage_per_index_endpoint() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        state
            .usage_counters
            .entry("index_a".to_string())
            .or_default()
            .search_count
            .fetch_add(5, Ordering::Relaxed);

        // index_b's count must not bleed into index_a's result
        state
            .usage_counters
            .entry("index_b".to_string())
            .or_default()
            .search_count
            .fetch_add(99, Ordering::Relaxed);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/search_operations/index_a").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(
            arr[0]["v"].as_u64().unwrap(),
            5,
            "per-index result must only include that index's count"
        );
    }

    /// Verify that a comma-separated statistic path like `search_operations,total_write_operations` returns independent arrays for each requested metric.
    #[tokio::test]
    async fn usage_comma_separated_statistics() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        {
            let entry = state.usage_counters.entry("idx".to_string()).or_default();
            entry.search_count.fetch_add(2, Ordering::Relaxed);
            entry.write_count.fetch_add(7, Ordering::Relaxed);
        }

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/search_operations,total_write_operations").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        assert!(json["search_operations"].is_array());
        assert!(json["total_write_operations"].is_array());
        assert_eq!(json["search_operations"][0]["v"].as_u64().unwrap(), 2);
        assert_eq!(json["total_write_operations"][0]["v"].as_u64().unwrap(), 7);
    }

    #[tokio::test]
    async fn usage_unknown_statistic_returns_error() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/bogus_stat").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let json = body_json(resp).await;
        assert_eq!(json["status"].as_u64().unwrap(), 400);
        assert!(
            json["message"].as_str().unwrap().contains("bogus_stat"),
            "error message should name the unknown stat"
        );
    }

    /// Verify that an API key lacking the `usage` ACL receives a 403 Forbidden response from the usage endpoint.
    #[tokio::test]
    async fn usage_requires_usage_acl() {
        use crate::auth::{authenticate_and_authorize, ApiKey, KeyStore, RateLimiter};

        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        let ks = Arc::new(KeyStore::load_or_create(tmp.path(), "admin-key"));
        let (_, search_key) = ks.create_key(ApiKey {
            hash: String::new(),
            salt: String::new(),
            hmac_key: None,
            created_at: 0,
            acl: vec!["search".to_string()],
            description: "search-only".to_string(),
            indexes: vec![],
            max_hits_per_query: 0,
            max_queries_per_ip_per_hour: 0,
            query_parameters: String::new(),
            referers: vec![],
            restrict_sources: None,
            validity: 0,
        });

        let ks_clone = ks.clone();
        let rl = RateLimiter::new();
        let app = Router::new()
            .route("/1/usage/:statistic", get(usage_global))
            .with_state(state)
            .layer(middleware::from_fn(
                move |mut req: axum::extract::Request, next: middleware::Next| {
                    let ks = ks_clone.clone();
                    let rl = rl.clone();
                    async move {
                        req.extensions_mut().insert(ks);
                        req.extensions_mut().insert(rl);
                        authenticate_and_authorize(req, next).await
                    }
                },
            ));

        let req = Request::builder()
            .uri("/1/usage/search_operations")
            .header("x-algolia-application-id", "test")
            .header("x-algolia-api-key", search_key.as_str())
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "key without usage ACL must get 403"
        );

        let json = body_json(resp).await;
        assert!(
            json["message"].as_str().is_some(),
            "error must have message"
        );
    }

    /// Verify that `startDate` and `endDate` query parameters are accepted without causing a parse error.
    #[tokio::test]
    async fn usage_date_range_params_accepted() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        let app = usage_router(state);
        let resp = get_usage(
            app,
            "/1/usage/search_operations?startDate=2026-02-25&endDate=2026-02-26",
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "date range params must not cause a parse error"
        );
    }

    /// Verify the response shape matches the Algolia usage API: top-level stat name mapping to an array of `{"t": <epoch_ms>, "v": <integer>}` objects.
    #[tokio::test]
    async fn usage_response_format_matches_algolia() {
        let tmp = TempDir::new().unwrap();
        let state = make_state(&tmp);

        state
            .usage_counters
            .entry("idx".to_string())
            .or_default()
            .search_count
            .fetch_add(1, Ordering::Relaxed);

        let app = usage_router(state);
        let resp = get_usage(app, "/1/usage/search_operations").await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;

        // Top-level key is the statistic name mapping to an array
        let arr = json["search_operations"]
            .as_array()
            .expect("search_operations must be an array");
        assert_eq!(arr.len(), 1);

        let point = &arr[0];
        // "t" must be a positive integer (epoch milliseconds)
        let t = point["t"].as_i64().expect("t must be an integer");
        assert!(t > 0, "t must be a positive epoch-millisecond timestamp");

        // "v" must be an integer
        assert!(
            point["v"].is_u64() || point["v"].is_i64(),
            "v must be an integer"
        );
    }

    // ── Date range + persistence tests ──

    /// Create an `AppState` wired to the given `UsagePersistence` instance so tests can exercise historical snapshot loading.
    fn make_state_with_persistence(
        tmp: &TempDir,
        persistence: Arc<crate::usage_persistence::UsagePersistence>,
    ) -> Arc<AppState> {
        let mut state = crate::test_helpers::TestStateBuilder::new(tmp).build();
        state.metrics_state = None;
        state.usage_persistence = Some(persistence);
        Arc::new(state)
    }

    /// Verify that querying a past date range loads only the matching persisted snapshots and returns the correct values.
    #[tokio::test]
    async fn usage_date_range_returns_historical_data() {
        use crate::usage_persistence::UsagePersistence;

        let tmp = TempDir::new().unwrap();
        let persistence = Arc::new(UsagePersistence::new(tmp.path()).unwrap());

        // Persist snapshots for 3 separate days
        for (date, count) in [
            ("2026-02-22", 10u64),
            ("2026-02-23", 20),
            ("2026-02-24", 30),
        ] {
            let c: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
                dashmap::DashMap::new();
            c.entry("idx".to_string())
                .or_default()
                .search_count
                .fetch_add(count, Ordering::Relaxed);
            persistence.save_snapshot(date, &c).unwrap();
        }

        let state = make_state_with_persistence(&tmp, persistence);
        let app = usage_router(state);

        // Query spanning only 2 of the 3 persisted dates (both before today)
        let resp = get_usage(
            app,
            "/1/usage/search_operations?startDate=2026-02-22&endDate=2026-02-23",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(
            arr.len(),
            2,
            "should return exactly 2 historical data points"
        );

        let values: Vec<u64> = arr.iter().map(|p| p["v"].as_u64().unwrap()).collect();
        assert!(values.contains(&10), "day 1 value must be present");
        assert!(values.contains(&20), "day 2 value must be present");
    }

    /// Verify that a date range ending on today merges yesterday's persisted snapshot with today's live in-memory counters into two data points.
    #[tokio::test]
    async fn usage_date_range_includes_current_day_inmemory() {
        use crate::usage_persistence::UsagePersistence;

        let tmp = TempDir::new().unwrap();
        let persistence = Arc::new(UsagePersistence::new(tmp.path()).unwrap());

        let yesterday = (chrono::Utc::now() - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // Persist yesterday's snapshot
        let hist: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();
        hist.entry("idx".to_string())
            .or_default()
            .search_count
            .fetch_add(5, Ordering::Relaxed);
        persistence.save_snapshot(&yesterday, &hist).unwrap();

        let state = make_state_with_persistence(&tmp, persistence);

        // Add live (today) counter
        state
            .usage_counters
            .entry("idx".to_string())
            .or_default()
            .search_count
            .fetch_add(99, Ordering::Relaxed);

        let app = usage_router(state);
        let path = format!(
            "/1/usage/search_operations?startDate={}&endDate={}",
            yesterday, today
        );
        let resp = get_usage(app, &path).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(
            arr.len(),
            2,
            "should return 1 historical + 1 live data point"
        );

        let values: Vec<u64> = arr.iter().map(|p| p["v"].as_u64().unwrap()).collect();
        assert!(values.contains(&5), "historical value must be present");
        assert!(values.contains(&99), "live value must be present");
    }

    /// Verify that the per-index endpoint only returns the filtered index's value from a persisted snapshot containing multiple indexes.
    #[tokio::test]
    async fn usage_date_range_per_index_filters_correctly() {
        use crate::usage_persistence::UsagePersistence;

        let tmp = TempDir::new().unwrap();
        let persistence = Arc::new(UsagePersistence::new(tmp.path()).unwrap());

        // Snapshot has both idx_a and idx_b
        let c: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();
        c.entry("idx_a".to_string())
            .or_default()
            .search_count
            .fetch_add(10, Ordering::Relaxed);
        c.entry("idx_b".to_string())
            .or_default()
            .search_count
            .fetch_add(20, Ordering::Relaxed);
        persistence.save_snapshot("2026-02-24", &c).unwrap();

        let state = make_state_with_persistence(&tmp, persistence);
        let app = usage_router(state);

        let resp = get_usage(
            app,
            "/1/usage/search_operations/idx_a?startDate=2026-02-24&endDate=2026-02-24",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(
            arr[0]["v"].as_u64().unwrap(),
            10,
            "per-index filter must only return idx_a's value"
        );
    }

    /// Verify daily granularity produces exactly one data point per calendar day, each stamped at midnight UTC and ordered chronologically.
    #[tokio::test]
    async fn usage_granularity_daily_returns_one_point_per_day() {
        use crate::usage_persistence::UsagePersistence;

        let tmp = TempDir::new().unwrap();
        let persistence = Arc::new(UsagePersistence::new(tmp.path()).unwrap());

        // Save two days of data (both before today)
        for (date, count) in [("2026-02-23", 1u64), ("2026-02-24", 2u64)] {
            let c: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
                dashmap::DashMap::new();
            c.entry("idx".to_string())
                .or_default()
                .search_count
                .fetch_add(count, Ordering::Relaxed);
            persistence.save_snapshot(date, &c).unwrap();
        }

        let state = make_state_with_persistence(&tmp, persistence);
        let app = usage_router(state);

        let resp = get_usage(
            app,
            "/1/usage/search_operations?startDate=2026-02-23&endDate=2026-02-24&granularity=daily",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let json = body_json(resp).await;
        let arr = json["search_operations"].as_array().unwrap();
        assert_eq!(
            arr.len(),
            2,
            "daily granularity returns one point per calendar day with data"
        );

        // Timestamps must be midnight UTC (divisible by 86_400_000 ms)
        for point in arr {
            let t = point["t"].as_i64().unwrap();
            assert_eq!(t % 86_400_000, 0, "each t must be at midnight UTC");
        }

        // Points must be in chronological order
        let t0 = arr[0]["t"].as_i64().unwrap();
        let t1 = arr[1]["t"].as_i64().unwrap();
        assert!(t1 > t0, "data points must be in chronological order");
    }
}
