use std::collections::HashMap;

use crate::experiments::config::PrimaryMetric;

use super::types::{ArmMetrics, EventRow, ExperimentMetrics, PerUserAgg, PreSearchRow, SearchRow};
use crate::experiments::stats;

/// Aggregate raw search + event rows into experiment metrics.
///
/// This is the pure computation core — separated from I/O for testability.
/// The caller is responsible for reading parquet files and passing in the rows.
pub(super) fn aggregate_experiment_metrics(
    searches: &[SearchRow],
    events: &[EventRow],
    winsorization_cap: Option<f64>,
) -> ExperimentMetrics {
    // 1. Separate stable-id vs query_id-fallback searches
    let mut stable_searches = Vec::new();
    let mut no_stable_id_queries: u64 = 0;

    for s in searches {
        if s.assignment_method == "user_token" || s.assignment_method == "session_id" {
            stable_searches.push(s);
        } else {
            no_stable_id_queries += 1;
        }
    }

    // 2. Build query_id -> event lookup for click/conversion join
    let mut events_by_qid: HashMap<&str, Vec<&EventRow>> = HashMap::new();
    for e in events {
        events_by_qid.entry(&e.query_id).or_default().push(e);
    }

    // 3. Per-user aggregation: (user_token, variant_id) -> PerUserAgg
    // Key: (user_token, variant_id)
    let mut per_user: HashMap<(&str, &str), PerUserAgg> = HashMap::new();

    for s in &stable_searches {
        let key = (s.user_token.as_str(), s.variant_id.as_str());
        let agg = per_user.entry(key).or_default();
        agg.searches += 1;

        if s.nb_hits == 0 {
            agg.zero_result_searches += 1;
        }

        // Join with events via query_id
        let mut search_got_click = false;
        if let Some(ref qid) = s.query_id {
            if let Some(matched_events) = events_by_qid.get(qid.as_str()) {
                for ev in matched_events {
                    match ev.event_type.as_str() {
                        "click" => {
                            agg.clicks += 1;
                            search_got_click = true;
                            // Collect min position for MeanClickRank diagnostic
                            if let Some(ref pos_str) = ev.positions {
                                if let Ok(positions) = serde_json::from_str::<Vec<i64>>(pos_str) {
                                    if let Some(min_pos) = positions
                                        .into_iter()
                                        .filter_map(|p| {
                                            if p > 0 {
                                                u32::try_from(p).ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .min()
                                    {
                                        agg.click_min_positions.push(min_pos);
                                    }
                                }
                            }
                        }
                        "conversion" => {
                            agg.conversions += 1;
                            agg.revenue += ev.value.unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Abandoned = has results but no click
        if s.has_results && !search_got_click {
            agg.abandoned_searches += 1;
        }
    }

    // 4. Outlier detection
    let user_search_counts: HashMap<String, u64> = {
        let mut map = HashMap::new();
        for ((user, _), agg) in &per_user {
            *map.entry(user.to_string()).or_default() += agg.searches;
        }
        map
    };

    let outlier_set = stats::detect_outlier_users(&user_search_counts);
    let outlier_users_excluded = outlier_set.len();

    // 5. Split into control and variant, excluding outliers
    let mut control_users: Vec<(&str, &PerUserAgg)> = Vec::new();
    let mut variant_users: Vec<(&str, &PerUserAgg)> = Vec::new();

    for ((user, variant_id), agg) in &per_user {
        if outlier_set.contains(*user) {
            continue;
        }
        if *variant_id == "control" {
            control_users.push((user, agg));
        } else {
            variant_users.push((user, agg));
        }
    }

    // 6. Build arm metrics
    let control = build_arm_metrics("control", &control_users, winsorization_cap);
    let variant = build_arm_metrics("variant", &variant_users, winsorization_cap);

    ExperimentMetrics {
        control,
        variant,
        outlier_users_excluded,
        no_stable_id_queries,
        winsorization_cap_applied: winsorization_cap,
    }
}

/// Build arm-level metrics from per-user aggregations.
fn build_arm_metrics(
    arm_name: &str,
    users: &[(&str, &PerUserAgg)],
    winsorization_cap: Option<f64>,
) -> ArmMetrics {
    if users.is_empty() {
        return ArmMetrics::empty(arm_name);
    }

    let mut total_searches: u64 = 0;
    let mut total_clicks: u64 = 0;
    let mut total_conversions: u64 = 0;
    let mut total_revenue: f64 = 0.0;
    let mut total_zero_result: u64 = 0;
    let mut total_abandoned: u64 = 0;
    let mut per_user_ids: Vec<String> = Vec::with_capacity(users.len());
    let mut per_user_ctrs: Vec<(f64, f64)> = Vec::with_capacity(users.len());
    let mut per_user_conversion_rates: Vec<(f64, f64)> = Vec::with_capacity(users.len());
    let mut per_user_zero_result_rates: Vec<(f64, f64)> = Vec::with_capacity(users.len());
    let mut per_user_abandonment_rates: Vec<(f64, f64)> = Vec::with_capacity(users.len());
    let mut per_user_revenues: Vec<f64> = Vec::with_capacity(users.len());

    for (user_id, agg) in users {
        per_user_ids.push(user_id.to_string());
        total_searches += agg.searches;
        total_clicks += agg.clicks;
        total_conversions += agg.conversions;
        total_revenue += agg.revenue;
        total_zero_result += agg.zero_result_searches;
        total_abandoned += agg.abandoned_searches;

        per_user_ctrs.push((agg.clicks as f64, agg.searches as f64));
        per_user_conversion_rates.push((agg.conversions as f64, agg.searches as f64));
        per_user_zero_result_rates.push((agg.zero_result_searches as f64, agg.searches as f64));
        let searches_with_results = agg.searches.saturating_sub(agg.zero_result_searches);
        per_user_abandonment_rates
            .push((agg.abandoned_searches as f64, searches_with_results as f64));
        per_user_revenues.push(agg.revenue);
    }

    // Apply winsorization to per-user CTRs if cap is specified
    if let Some(cap) = winsorization_cap {
        let mut raw_ctrs: Vec<f64> = per_user_ctrs
            .iter()
            .filter(|(_, s)| *s > 0.0)
            .map(|(c, s)| c / s)
            .collect();
        stats::winsorize(&mut raw_ctrs, cap);
        // Recompute per_user_ctrs with capped ratios (keep original searches)
        let mut capped_idx = 0;
        for (clicks, searches) in &mut per_user_ctrs {
            if *searches > 0.0 {
                let capped_ctr = raw_ctrs[capped_idx];
                *clicks = capped_ctr * *searches;
                capped_idx += 1;
            }
        }
    }

    // Compute rates (safe against zero division)
    let searches_with_results = total_searches - total_zero_result;
    let ctr = safe_div(
        per_user_ctrs
            .iter()
            .map(|(clicks, searches)| safe_div(*clicks, *searches))
            .sum::<f64>(),
        per_user_ctrs.len() as f64,
    );
    let conversion_rate = safe_div(total_conversions as f64, total_searches as f64);
    let revenue_per_search = safe_div(total_revenue, total_searches as f64);
    let zero_result_rate = safe_div(total_zero_result as f64, total_searches as f64);
    let abandonment_rate = safe_div(total_abandoned as f64, searches_with_results as f64);

    // MeanClickRank: per-user average of min-click-positions, then average across users.
    // Avoids heavy-user bias (Deng et al.).
    let mean_click_rank = {
        let mut user_means: Vec<f64> = Vec::new();
        for (_, agg) in users {
            if !agg.click_min_positions.is_empty() {
                let sum: f64 = agg.click_min_positions.iter().map(|&p| p as f64).sum();
                user_means.push(sum / agg.click_min_positions.len() as f64);
            }
        }
        safe_div(user_means.iter().sum::<f64>(), user_means.len() as f64)
    };

    ArmMetrics {
        arm_name: arm_name.to_string(),
        searches: total_searches,
        users: users.len() as u64,
        clicks: total_clicks,
        conversions: total_conversions,
        revenue: total_revenue,
        zero_result_searches: total_zero_result,
        abandoned_searches: total_abandoned,
        ctr,
        conversion_rate,
        revenue_per_search,
        zero_result_rate,
        abandonment_rate,
        per_user_ctrs,
        per_user_conversion_rates,
        per_user_zero_result_rates,
        per_user_abandonment_rates,
        per_user_revenues,
        per_user_ids,
        mean_click_rank,
    }
}

pub(super) fn safe_div(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

/// Compute per-user metric values from pre-experiment search/event data.
///
/// Returns a map of user_token -> metric value for use as CUPED covariates.
/// Uses the same metric calculation as the experiment aggregation.
pub(super) fn compute_pre_experiment_covariates(
    searches: &[PreSearchRow],
    events: &[EventRow],
    metric: &PrimaryMetric,
) -> HashMap<String, f64> {
    if searches.is_empty() {
        return HashMap::new();
    }

    // Build query_id -> event lookup
    let mut events_by_qid: HashMap<&str, Vec<&EventRow>> = HashMap::new();
    for e in events {
        events_by_qid.entry(&e.query_id).or_default().push(e);
    }

    // Per-user aggregation
    let mut per_user: HashMap<&str, PerUserAgg> = HashMap::new();
    for s in searches {
        let agg = per_user.entry(&s.user_token).or_default();
        agg.searches += 1;

        if s.nb_hits == 0 {
            agg.zero_result_searches += 1;
        }

        let mut search_got_click = false;
        if let Some(ref qid) = s.query_id {
            if let Some(matched_events) = events_by_qid.get(qid.as_str()) {
                for ev in matched_events {
                    match ev.event_type.as_str() {
                        "click" => {
                            agg.clicks += 1;
                            search_got_click = true;
                        }
                        "conversion" => {
                            agg.conversions += 1;
                            agg.revenue += ev.value.unwrap_or(0.0);
                        }
                        _ => {}
                    }
                }
            }
        }

        if s.has_results && !search_got_click {
            agg.abandoned_searches += 1;
        }
    }

    // Convert to metric values
    per_user
        .into_iter()
        .filter(|(_, agg)| agg.searches > 0)
        .map(|(user, agg)| {
            let value = match metric {
                PrimaryMetric::Ctr => safe_div(agg.clicks as f64, agg.searches as f64),
                PrimaryMetric::ConversionRate => {
                    safe_div(agg.conversions as f64, agg.searches as f64)
                }
                PrimaryMetric::RevenuePerSearch => safe_div(agg.revenue, agg.searches as f64),
                PrimaryMetric::ZeroResultRate => {
                    safe_div(agg.zero_result_searches as f64, agg.searches as f64)
                }
                PrimaryMetric::AbandonmentRate => {
                    let with_results = agg.searches.saturating_sub(agg.zero_result_searches);
                    safe_div(agg.abandoned_searches as f64, with_results as f64)
                }
            };
            (user.to_string(), value)
        })
        .collect()
}
