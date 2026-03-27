use flapjack::experiments::config::{
    Experiment, ExperimentArm, ExperimentConclusion, ExperimentStatus, PrimaryMetric,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateExperimentRequest {
    pub name: String,
    pub index_name: String,
    pub traffic_split: f64,
    pub control: ExperimentArm,
    pub variant: ExperimentArm,
    pub primary_metric: PrimaryMetric,
    #[serde(default)]
    pub minimum_days: Option<u32>,
    #[serde(default)]
    pub winsorization_cap: Option<f64>,
    #[serde(default)]
    pub interleaving: Option<bool>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConcludeExperimentRequest {
    pub winner: Option<String>,
    pub reason: String,
    pub control_metric: f64,
    pub variant_metric: f64,
    pub confidence: f64,
    pub significant: bool,
    pub promoted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExperimentsQuery {
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExperimentsResponse {
    pub abtests: Vec<Experiment>,
    pub count: usize,
    pub total: usize,
}

/// Full statistical results payload for an experiment.
///
/// Contains arm-level metrics, sample-size gating status, frequentist significance,
/// Bayesian probability, SRM detection, guard-rail alerts, CUPED adjustment status,
/// interleaving analysis, and an actionable recommendation string.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResultsResponse {
    #[serde(rename = "experimentID")]
    pub experiment_id: String,
    pub name: String,
    pub status: ExperimentStatus,
    pub index_name: String,
    pub start_date: Option<String>,
    pub ended_at: Option<String>,
    pub conclusion: Option<ExperimentConclusion>,
    pub traffic_split: f64,
    pub gate: GateResponse,
    pub control: ArmResponse,
    pub variant: ArmResponse,
    pub primary_metric: PrimaryMetric,
    pub significance: Option<SignificanceResponse>,
    pub bayesian: Option<BayesianResponse>,
    pub sample_ratio_mismatch: bool,
    pub guard_rail_alerts: Vec<GuardRailAlertResponse>,
    pub cuped_applied: bool,
    pub outlier_users_excluded: usize,
    pub no_stable_id_queries: u64,
    pub recommendation: Option<String>,
    pub interleaving: Option<InterleavingResponse>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterleavingResponse {
    pub delta_ab: f64,
    pub wins_control: u32,
    pub wins_variant: u32,
    pub ties: u32,
    pub p_value: f64,
    pub significant: bool,
    pub total_queries: u32,
    pub data_quality_ok: bool,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GuardRailAlertResponse {
    pub metric_name: String,
    pub control_value: f64,
    pub variant_value: f64,
    pub drop_pct: f64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GateResponse {
    pub minimum_n_reached: bool,
    pub minimum_days_reached: bool,
    pub ready_to_read: bool,
    pub required_searches_per_arm: u64,
    pub current_searches_per_arm: u64,
    pub progress_pct: f64,
    pub estimated_days_remaining: Option<f64>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArmResponse {
    pub name: String,
    pub searches: u64,
    pub users: u64,
    pub clicks: u64,
    pub conversions: u64,
    pub revenue: f64,
    pub ctr: f64,
    pub conversion_rate: f64,
    pub revenue_per_search: f64,
    pub zero_result_rate: f64,
    pub abandonment_rate: f64,
    pub mean_click_rank: f64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignificanceResponse {
    pub z_score: f64,
    pub p_value: f64,
    pub confidence: f64,
    pub significant: bool,
    pub relative_improvement: f64,
    pub winner: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BayesianResponse {
    pub prob_variant_better: f64,
}
