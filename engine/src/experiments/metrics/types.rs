use crate::experiments::stats;

/// Per-user raw aggregation (intermediate, before rate computation).
#[derive(Debug, Clone, Default)]
pub(super) struct PerUserAgg {
    pub(super) searches: u64,
    pub(super) clicks: u64,
    pub(super) conversions: u64,
    pub(super) revenue: f64,
    pub(super) zero_result_searches: u64,
    /// Searches that returned results (nb_hits > 0) but got no click.
    pub(super) abandoned_searches: u64,
    /// Min position from each click event that had positions data.
    /// Used to compute per-user mean click rank.
    pub(super) click_min_positions: Vec<u32>,
}

/// Aggregate metrics for one arm of an experiment.
#[derive(Debug, Clone)]
pub struct ArmMetrics {
    pub arm_name: String,
    pub searches: u64,
    pub users: u64,
    pub clicks: u64,
    pub conversions: u64,
    pub revenue: f64,
    pub zero_result_searches: u64,
    pub abandoned_searches: u64,
    pub ctr: f64,
    pub conversion_rate: f64,
    pub revenue_per_search: f64,
    pub zero_result_rate: f64,
    pub abandonment_rate: f64,
    /// Per-user (clicks_i, searches_i) tuples for `delta_method_z_test`.
    pub per_user_ctrs: Vec<(f64, f64)>,
    /// Per-user (conversions_i, searches_i) tuples for `delta_method_z_test`.
    pub per_user_conversion_rates: Vec<(f64, f64)>,
    /// Per-user (zero_result_i, searches_i) tuples for `delta_method_z_test`.
    pub per_user_zero_result_rates: Vec<(f64, f64)>,
    /// Per-user (abandoned_i, searches_with_results_i) tuples for `delta_method_z_test`.
    pub per_user_abandonment_rates: Vec<(f64, f64)>,
    /// Per-user total revenue for `welch_t_test`.
    pub per_user_revenues: Vec<f64>,
    /// User IDs aligned with per_user_* vectors, for CUPED covariate matching.
    pub per_user_ids: Vec<String>,
    /// Mean click rank diagnostic metric.
    /// Per-user average of min-click-position, then averaged across users.
    /// Lower = better. 0.0 when arm has zero clicks.
    pub mean_click_rank: f64,
}

impl ArmMetrics {
    /// Create an `ArmMetrics` with all counters at zero and empty per-user vectors.
    ///
    /// # Arguments
    ///
    /// * `arm_name` - Label for the arm (e.g. "control" or "variant").
    pub(super) fn empty(arm_name: &str) -> Self {
        Self {
            arm_name: arm_name.to_string(),
            searches: 0,
            users: 0,
            clicks: 0,
            conversions: 0,
            revenue: 0.0,
            zero_result_searches: 0,
            abandoned_searches: 0,
            ctr: 0.0,
            conversion_rate: 0.0,
            revenue_per_search: 0.0,
            zero_result_rate: 0.0,
            abandonment_rate: 0.0,
            per_user_ctrs: Vec::new(),
            per_user_conversion_rates: Vec::new(),
            per_user_zero_result_rates: Vec::new(),
            per_user_abandonment_rates: Vec::new(),
            per_user_revenues: Vec::new(),
            per_user_ids: Vec::new(),
            mean_click_rank: 0.0,
        }
    }
}

/// Combined metrics for both arms of an experiment.
#[derive(Debug)]
pub struct ExperimentMetrics {
    pub control: ArmMetrics,
    pub variant: ArmMetrics,
    pub outlier_users_excluded: usize,
    pub no_stable_id_queries: u64,
    pub winsorization_cap_applied: Option<f64>,
}

/// A single search event row relevant to experiment metrics.
#[derive(Debug, Clone)]
pub(super) struct SearchRow {
    pub(super) user_token: String,
    pub(super) variant_id: String,
    pub(super) query_id: Option<String>,
    pub(super) nb_hits: u32,
    pub(super) has_results: bool,
    pub(super) assignment_method: String,
}

/// A single insight event row relevant to experiment metrics.
#[derive(Debug, Clone)]
pub(super) struct EventRow {
    pub(super) query_id: String,
    pub(super) event_type: String,
    pub(super) value: Option<f64>,
    /// JSON-encoded positions array from click events (e.g. "[1,3,5]").
    /// 1-indexed per Algolia API convention.
    pub(super) positions: Option<String>,
    /// Team attribution for interleaving experiments: "control" or "variant".
    pub(super) interleaving_team: Option<String>,
}

/// A simplified search row for pre-experiment (non-experiment) traffic.
#[derive(Debug, Clone)]
pub(super) struct PreSearchRow {
    pub(super) user_token: String,
    pub(super) query_id: Option<String>,
    pub(super) nb_hits: u32,
    pub(super) has_results: bool,
}

/// Aggregate interleaving preference metrics for an experiment.
pub struct InterleavingMetrics {
    pub preference: stats::PreferenceResult,
    pub total_queries: u32,
    /// Fraction of queries where Team A was first (for data quality check).
    /// Should be roughly 0.5 — values outside 0.45..0.55 indicate a bug.
    pub first_team_a_ratio: f64,
}
