//! Estimates required sample sizes and experiment duration for A/B tests based on statistical significance targets and available daily traffic.
use super::*;

/// Estimate A/B test sample size and duration for a given minimum detectable effect and traffic split.
///
/// # Arguments
///
/// * `State(state)` - Application state containing the optional analytics engine
/// * `Json(body)` - A/B test request with variants and configuration including minimum detectable effect size
///
/// # Returns
///
/// An HTTP response containing either an error response or an AlgoliaEstimateResponse with calculated sample sizes per variant and estimated duration in days.
///
/// Returns an error if: variants count is not exactly 2, MDE size is outside (0, 1), or traffic percentages are not each in [1, 99] and do not sum to 100.
///
/// # Behavior
///
/// Calculates required sample size using a simplified formula with 95% confidence interval and 80% statistical power. Queries the analytics engine for historical daily search traffic (past 30 days) to estimate experiment duration. If traffic data is unavailable, uses a conservative default duration.
#[utoipa::path(
    post,
    path = "/2/abtests/estimate",
    tag = "experiments",
    request_body(content = AlgoliaEstimateRequest, description = "A/B test estimate payload"),
    responses(
        (status = 200, description = "Estimated sample sizes and duration", body = AlgoliaEstimateResponse),
        (status = 400, description = "Invalid estimate configuration")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn estimate_ab_test(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AlgoliaEstimateRequest>,
) -> Response {
    if body.variants.len() != 2 {
        return experiment_error_to_response(ExperimentError::InvalidConfig(
            "variants must contain exactly 2 entries".to_string(),
        ));
    }

    let mde_size = body.configuration.minimum_detectable_effect.size;
    if mde_size <= 0.0 || mde_size > 1.0 {
        return experiment_error_to_response(ExperimentError::InvalidConfig(
            "minimumDetectableEffect.size must be between 0 and 1 (exclusive)".to_string(),
        ));
    }

    let traffic_a_pct = body.variants[0].traffic_percentage;
    let traffic_b_pct = body.variants[1].traffic_percentage;
    if !(1..=99).contains(&traffic_a_pct) || !(1..=99).contains(&traffic_b_pct) {
        return experiment_error_to_response(ExperimentError::InvalidConfig(
            "variant trafficPercentage values must each be between 1 and 99".to_string(),
        ));
    }
    if traffic_a_pct + traffic_b_pct != 100 {
        return experiment_error_to_response(ExperimentError::InvalidConfig(
            "variant trafficPercentage values must sum to 100".to_string(),
        ));
    }

    let traffic_a = traffic_a_pct as f64 / 100.0;
    let traffic_b = traffic_b_pct as f64 / 100.0;

    // Sample size calculation using simplified formula:
    // n = (Z_alpha/2 + Z_beta)^2 * 2 * p * (1-p) / mde^2
    // Using Z_alpha/2 = 1.96 (95% CI), Z_beta = 0.84 (80% power), p = 0.5 (worst case)
    let z_sum = 1.96 + 0.84; // 2.80
    let base_n = (z_sum * z_sum * 2.0 * 0.5 * 0.5 / (mde_size * mde_size)).ceil() as i64;

    let sample_a = (base_n as f64 / traffic_a).ceil() as i64;
    let sample_b = (base_n as f64 / traffic_b).ceil() as i64;

    // Estimate duration from historical daily traffic.
    let daily_traffic = estimate_daily_traffic(state.as_ref(), &body.variants[0].index).await;
    let max_sample = sample_a.max(sample_b);
    let duration_days = if daily_traffic > 0 {
        ((max_sample as f64) / (daily_traffic as f64)).ceil() as i64
    } else {
        // No traffic data — return a conservative default.
        DEFAULT_ESTIMATE_DURATION_DAYS
    };

    Json(AlgoliaEstimateResponse {
        duration_days,
        sample_sizes: vec![sample_a, sample_b],
    })
    .into_response()
}

/// Estimate daily tracked search traffic for an index using the analytics engine.
async fn estimate_daily_traffic(state: &AppState, index_name: &str) -> i64 {
    let Some(engine) = state.analytics_engine.as_ref() else {
        return 0;
    };

    let end_date = chrono::Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(ESTIMATE_TRAFFIC_LOOKBACK_DAYS - 1);
    let start = start_date.format("%Y-%m-%d").to_string();
    let end = end_date.format("%Y-%m-%d").to_string();

    match engine.search_count(index_name, &start, &end).await {
        Ok(result) => {
            let total_searches = result.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            if total_searches <= 0 {
                return 0;
            }
            ((total_searches as f64) / (ESTIMATE_TRAFFIC_LOOKBACK_DAYS as f64)).ceil() as i64
        }
        Err(err) => {
            tracing::warn!(
                "failed to read analytics search_count for abtest estimate index {}: {}",
                index_name,
                err
            );
            0
        }
    }
}
