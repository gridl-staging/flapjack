//! Geo/device/revenue/countries/status analytics endpoint handlers.
use axum::{
    extract::{Query, RawQuery, State},
    http::HeaderMap,
    Json,
};
use std::sync::Arc;

use flapjack::analytics::{AnalyticsQueryEngine, AnalyticsQueryParams};
use flapjack::error::FlapjackError;

use super::super::analytics_dto::*;
use super::{
    clamp_limit, maybe_fan_out, validate_analytics_index, validate_date_range, AnalyticsParams,
};

/// GET /2/devices - Device/platform breakdown from analytics_tags
#[utoipa::path(
    get,
    path = "/2/devices",
    tag = "analytics",
    responses((status = 200, description = "Device/platform breakdown", body = AnalyticsDeviceBreakdownResponse)),
    security(("api_key" = []))
)]
pub async fn get_device_breakdown(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let result = engine
        .device_breakdown(&params.index, &params.start_date, &params.end_date)
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let result = maybe_fan_out(
        &headers,
        "devices",
        "/2/devices",
        &raw_query.unwrap_or_default(),
        result,
        1000,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/geo - Geographic breakdown from country field
#[utoipa::path(
    get,
    path = "/2/geo",
    tag = "analytics",
    responses((status = 200, description = "Geographic breakdown", body = AnalyticsGeoBreakdownResponse)),
    security(("api_key" = []))
)]
pub async fn get_geo_breakdown(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let limit = clamp_limit(params.limit.unwrap_or(50));
    let result = engine
        .geo_breakdown(&params.index, &params.start_date, &params.end_date, limit)
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let result = maybe_fan_out(
        &headers,
        "geo",
        "/2/geo",
        &raw_query.unwrap_or_default(),
        result,
        limit,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/geo/:country - Top searches for a specific country
#[utoipa::path(get, path = "/2/geo/{country}", tag = "analytics",
    params(("country" = String, Path, description = "Country code")),
    responses((status = 200, description = "Top searches for country", body = AnalyticsGeoTopSearchesResponse)),
    security(("api_key" = [])))]
pub async fn get_geo_top_searches(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    axum::extract::Path(country): axum::extract::Path<String>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let limit = clamp_limit(params.limit.unwrap_or(10));
    let result = engine
        .geo_top_searches(
            &params.index,
            &country,
            &params.start_date,
            &params.end_date,
            limit,
        )
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let endpoint = format!("geo/{}", country);
    let path = format!("/2/geo/{}", country);
    let result = maybe_fan_out(
        &headers,
        &endpoint,
        &path,
        &raw_query.unwrap_or_default(),
        result,
        limit,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/geo/:country/regions - Region (state) breakdown for a country
#[utoipa::path(get, path = "/2/geo/{country}/regions", tag = "analytics",
    params(("country" = String, Path, description = "Country code")),
    responses((status = 200, description = "Region breakdown for country", body = AnalyticsGeoRegionsResponse)),
    security(("api_key" = [])))]
pub async fn get_geo_regions(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    axum::extract::Path(country): axum::extract::Path<String>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let limit = clamp_limit(params.limit.unwrap_or(50));
    let result = engine
        .geo_region_breakdown(
            &params.index,
            &country,
            &params.start_date,
            &params.end_date,
            limit,
        )
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let endpoint = format!("geo/{}/regions", country);
    let path = format!("/2/geo/{}/regions", country);
    let result = maybe_fan_out(
        &headers,
        &endpoint,
        &path,
        &raw_query.unwrap_or_default(),
        result,
        limit,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/conversions/revenue - Revenue from purchase conversions, grouped by currency
#[utoipa::path(
    get,
    path = "/2/conversions/revenue",
    tag = "analytics",
    responses((status = 200, description = "Revenue by currency with daily breakdown", body = AnalyticsRevenueResponse)),
    security(("api_key" = []))
)]
pub async fn get_revenue(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let result = engine
        .revenue(&params.index, &params.start_date, &params.end_date)
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let result = maybe_fan_out(
        &headers,
        "conversions/revenue",
        "/2/conversions/revenue",
        &raw_query.unwrap_or_default(),
        result,
        1000,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/countries - Search counts grouped by country code
#[utoipa::path(
    get,
    path = "/2/countries",
    tag = "analytics",
    responses((status = 200, description = "Search counts by country", body = AnalyticsCountriesResponse)),
    security(("api_key" = []))
)]
pub async fn get_countries(
    headers: HeaderMap,
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    RawQuery(raw_query): RawQuery,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    validate_date_range(&params.start_date, &params.end_date)?;
    let limit = clamp_limit(params.limit.unwrap_or(1000));
    let offset = params.offset.unwrap_or(0);
    let result = engine
        .countries(
            &AnalyticsQueryParams {
                index_name: &params.index,
                start_date: &params.start_date,
                end_date: &params.end_date,
                limit,
                tags: params.tags.as_deref(),
            },
            offset,
            params.order_by.as_deref(),
        )
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    let result = maybe_fan_out(
        &headers,
        "countries",
        "/2/countries",
        &raw_query.unwrap_or_default(),
        result,
        limit,
    )
    .await;
    Ok(Json(result))
}

/// GET /2/status - Analytics status (local only, no fan-out)
#[utoipa::path(
    get,
    path = "/2/status",
    tag = "analytics",
    responses((status = 200, description = "Analytics subsystem status", body = AnalyticsStatusResponse)),
    security(("api_key" = []))
)]
pub async fn get_analytics_status(
    State(engine): State<Arc<AnalyticsQueryEngine>>,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    validate_analytics_index(&params.index)?;
    let result = engine
        .status(&params.index)
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Analytics error: {}", e)))?;
    Ok(Json(result))
}
