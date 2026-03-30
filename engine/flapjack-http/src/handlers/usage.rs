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
#[path = "usage_tests.rs"]
mod tests;
