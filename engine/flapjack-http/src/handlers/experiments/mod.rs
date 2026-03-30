use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
    Json,
};
use flapjack::experiments::{
    config::{
        Experiment, ExperimentArm, ExperimentConclusion, ExperimentError, ExperimentStatus,
        PrimaryMetric, QueryOverrides,
    },
    metrics,
    store::ExperimentStore,
};
use std::sync::Arc;

use super::dto_algolia::{
    self, AlgoliaAbTestActionResponse, AlgoliaCreateAbTestRequest, AlgoliaCreateAbTestResponse,
    AlgoliaEstimateRequest, AlgoliaEstimateResponse, AlgoliaListAbTestsQuery,
    AlgoliaListAbTestsResponse,
};
use super::AppState;
use crate::error_response::json_error;

mod promote;
mod schemas;
use promote::promote_variant_settings;
pub use schemas::{
    ArmResponse, BayesianResponse, ConcludeExperimentRequest, CreateExperimentRequest,
    GateResponse, GuardRailAlertResponse, InterleavingResponse, ListExperimentsQuery,
    ListExperimentsResponse, ResultsResponse, SignificanceResponse,
};

const DEFAULT_LIST_LIMIT: usize = 10;
const DEFAULT_LIST_OFFSET: usize = 0;
pub(super) const DEFAULT_ESTIMATE_DURATION_DAYS: i64 = 21;
pub(super) const ESTIMATE_TRAFFIC_LOOKBACK_DAYS: i64 = 30;
const EXPERIMENT_WARNING_HEADER_NAME: &str = "x-flapjack-warning";
const VARIANT_PROMOTION_FAILED_WARNING: &str = "variant-promotion-failed";
#[cfg(test)]
const DEFAULT_MINIMUM_DAYS: u32 = dto_algolia::DEFAULT_MINIMUM_DAYS;

fn experiment_store_unavailable_response() -> Response {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "experiment store unavailable",
    )
}

fn get_experiment_store(state: &AppState) -> Option<&ExperimentStore> {
    state.experiment_store.as_deref()
}

/// Map an `ExperimentError` variant to the appropriate HTTP status code and JSON error body.
///
/// Translates validation errors to 400, not-found to 404, status conflicts to 409,
/// and I/O or serialization errors to 500.
fn experiment_error_to_response(err: ExperimentError) -> Response {
    let status = match err {
        ExperimentError::InvalidConfig(_) => StatusCode::BAD_REQUEST,
        ExperimentError::NotFound(_) => StatusCode::NOT_FOUND,
        ExperimentError::InvalidStatus(_) => StatusCode::CONFLICT,
        ExperimentError::AlreadyExists(_) => StatusCode::CONFLICT,
        ExperimentError::Io(_) | ExperimentError::Json(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    json_error(status, err.to_string())
}

/// Resolve a path parameter that could be an integer ID or a UUID string.
/// Returns the (UUID, numeric_id) pair.
fn resolve_experiment_id(
    store: &ExperimentStore,
    id_str: &str,
) -> Result<(String, i64), ExperimentError> {
    // Try parsing as integer first (Algolia-compatible path).
    if let Ok(numeric) = id_str.parse::<i64>() {
        let uuid = store
            .get_uuid_for_numeric(numeric)
            .ok_or_else(|| ExperimentError::NotFound(id_str.to_string()))?;
        return Ok((uuid, numeric));
    }
    // Fall back to UUID string lookup.
    let numeric = store
        .get_numeric_id(id_str)
        .ok_or_else(|| ExperimentError::NotFound(id_str.to_string()))?;
    Ok((id_str.to_string(), numeric))
}

/// Shared implementation for start/stop/delete-style lifecycle endpoints that
/// resolve an experiment, call a store method, and return an action response.
fn lifecycle_action_response(
    state: &AppState,
    id_str: &str,
    action: fn(&ExperimentStore, &str) -> Result<Experiment, ExperimentError>,
) -> Response {
    let store = match get_experiment_store(state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, id_str) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    match action(store, &uuid) {
        Ok(experiment) => Json(AlgoliaAbTestActionResponse {
            ab_test_id: numeric_id,
            index: experiment.index_name,
            task_id: numeric_id,
        })
        .into_response(),
        Err(err) => experiment_error_to_response(err),
    }
}

fn has_any_variant_metrics(metrics: &metrics::ExperimentMetrics) -> bool {
    let control = &metrics.control;
    let variant = &metrics.variant;
    control.searches > 0
        || variant.searches > 0
        || control.clicks > 0
        || variant.clicks > 0
        || control.conversions > 0
        || variant.conversions > 0
        || control.users > 0
        || variant.users > 0
}

fn fill_algolia_variant_metrics(
    target: &mut dto_algolia::AlgoliaVariant,
    arm: &metrics::ArmMetrics,
) {
    target.average_click_position = Some(arm.mean_click_rank);
    target.click_count = Some(arm.clicks as i64);
    target.click_through_rate = Some(arm.ctr);
    target.conversion_count = Some(arm.conversions as i64);
    target.conversion_rate = Some(arm.conversion_rate);
    target.no_result_count = Some(arm.zero_result_searches as i64);
    target.search_count = Some(arm.searches as i64);
    target.tracked_search_count = Some(arm.searches as i64);
    target.user_count = Some(arm.users as i64);
    target.tracked_user_count = Some(arm.users as i64);
}

fn apply_metrics_to_algolia_response(
    payload: &mut dto_algolia::AlgoliaAbTest,
    metrics: &metrics::ExperimentMetrics,
) {
    if payload.variants.len() < 2 || !has_any_variant_metrics(metrics) {
        return;
    }
    fill_algolia_variant_metrics(&mut payload.variants[0], &metrics.control);
    fill_algolia_variant_metrics(&mut payload.variants[1], &metrics.variant);
}

fn validate_conclusion_winner(winner: Option<String>) -> Result<Option<String>, ExperimentError> {
    match winner {
        Some(w) if w == "control" || w == "variant" => Ok(Some(w)),
        Some(w) => Err(ExperimentError::InvalidConfig(format!(
            "winner must be 'control' or 'variant', got '{w}'"
        ))),
        None => Ok(None),
    }
}

/// Concrete conclude-response schema.
///
/// The generic `Experiment` model allows `conclusion` to be absent for draft and
/// running states, but a successful conclude response always includes it. Keeping
/// this as a separate DTO lets the OpenAPI contract enforce that stronger promise.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConcludedExperimentResponse {
    pub id: String,
    pub name: String,
    pub index_name: String,
    pub status: ExperimentStatus,
    pub traffic_split: f64,
    pub control: ExperimentArm,
    pub variant: ExperimentArm,
    pub primary_metric: PrimaryMetric,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<i64>,
    pub minimum_days: u32,
    pub winsorization_cap: Option<f64>,
    pub conclusion: ExperimentConclusion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interleaving: Option<bool>,
}

/// Extracts the conclusion payload from a concluded experiment, returning a
/// structured response or an internal-error response when the store returns an
/// impossible concluded experiment without a conclusion payload.
#[allow(clippy::result_large_err)] // Response is inherently large in axum; boxing adds indirection without benefit at a single call site
fn concluded_experiment_response(
    experiment: Experiment,
) -> Result<ConcludedExperimentResponse, Response> {
    let Experiment {
        id,
        name,
        index_name,
        status,
        traffic_split,
        control,
        variant,
        primary_metric,
        created_at,
        started_at,
        ended_at,
        stopped_at,
        minimum_days,
        winsorization_cap,
        conclusion,
        interleaving,
    } = experiment;

    let conclusion = match conclusion {
        Some(conclusion) => conclusion,
        None => {
            tracing::error!(
                experiment_id = %id,
                experiment_status = ?status,
                "concluded experiment response missing conclusion payload"
            );
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "concluded experiment missing conclusion",
            ));
        }
    };

    Ok(ConcludedExperimentResponse {
        id,
        name,
        index_name,
        status,
        traffic_split,
        control,
        variant,
        primary_metric,
        created_at,
        started_at,
        ended_at,
        stopped_at,
        minimum_days,
        winsorization_cap,
        conclusion,
        interleaving,
    })
}

fn attach_experiment_warning_header(
    mut response: Response,
    warning: Option<&'static str>,
) -> Response {
    if let Some(warning) = warning {
        response.headers_mut().insert(
            axum::http::header::HeaderName::from_static(EXPERIMENT_WARNING_HEADER_NAME),
            axum::http::HeaderValue::from_static(warning),
        );
    }
    response
}

/// Create a new A/B test experiment with traffic splitting between a main index and variant.
#[utoipa::path(
    post,
    path = "/2/abtests",
    tag = "experiments",
    request_body(content = AlgoliaCreateAbTestRequest, description = "A/B test creation payload"),
    responses(
        (status = 200, description = "Experiment created", body = AlgoliaCreateAbTestResponse),
        (status = 400, description = "Invalid experiment configuration"),
        (status = 409, description = "Experiment already exists"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn create_experiment(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AlgoliaCreateAbTestRequest>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let experiment = match dto_algolia::algolia_create_to_experiment(&body) {
        Ok(exp) => exp,
        Err(msg) => {
            return experiment_error_to_response(ExperimentError::InvalidConfig(msg));
        }
    };

    let index_name = experiment.index_name.clone();
    match store.create(experiment) {
        Ok(created) => {
            let numeric_id = store.get_numeric_id(&created.id).unwrap_or(0);
            (
                StatusCode::OK,
                Json(AlgoliaCreateAbTestResponse {
                    ab_test_id: numeric_id,
                    index: index_name,
                    task_id: numeric_id, // use same ID as task placeholder
                }),
            )
                .into_response()
        }
        Err(err) => experiment_error_to_response(err),
    }
}

/// List experiments with optional index prefix/suffix filtering and pagination.
///
/// Supports Algolia-compatible query parameters for filtering by index name pattern.
/// Results are sorted by creation time with deterministic tie-breaking on numeric ID.
///
/// # Returns
///
/// A paginated response containing the matching experiments, page count, and total count.
#[utoipa::path(
    get,
    path = "/2/abtests",
    tag = "experiments",
    params(
        ("offset" = Option<usize>, Query, description = "Pagination offset"),
        ("limit" = Option<usize>, Query, description = "Maximum number of experiments to return"),
        ("indexPrefix" = Option<String>, Query, description = "Filter experiments by index name prefix"),
        ("indexSuffix" = Option<String>, Query, description = "Filter experiments by index name suffix")
    ),
    responses(
        (status = 200, description = "Experiments list", body = AlgoliaListAbTestsResponse),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn list_experiments(
    State(state): State<Arc<AppState>>,
    Query(params): Query<AlgoliaListAbTestsQuery>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let mut experiments = store.list(None);

    // Apply Algolia-style index prefix/suffix filters.
    if let Some(ref prefix) = params.index_prefix {
        experiments.retain(|e| e.index_name.starts_with(prefix.as_str()));
    }
    if let Some(ref suffix) = params.index_suffix {
        experiments.retain(|e| e.index_name.ends_with(suffix.as_str()));
    }

    // Pair each experiment with its numeric ID for deterministic sort and DTO mapping.
    let mut exp_with_ids: Vec<(Experiment, i64)> = experiments
        .into_iter()
        .map(|e| {
            let nid = store.get_numeric_id(&e.id).unwrap_or(0);
            (e, nid)
        })
        .collect();
    exp_with_ids.sort_by(|(a, a_id), (b, b_id)| {
        a.created_at.cmp(&b.created_at).then_with(|| a_id.cmp(b_id))
    });

    let total = exp_with_ids.len();
    let offset = params.offset.unwrap_or(DEFAULT_LIST_OFFSET);
    let limit = params.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    let page: Vec<(Experiment, i64)> = exp_with_ids.into_iter().skip(offset).take(limit).collect();
    let count = page.len();

    let abtests: Vec<_> = page
        .iter()
        .map(|(exp, numeric_id)| {
            dto_algolia::experiment_to_algolia_with_updated_at(
                exp,
                *numeric_id,
                store.get_last_updated_ms(&exp.id),
            )
        })
        .collect();

    let abtests = if total == 0 { None } else { Some(abtests) };

    Json(AlgoliaListAbTestsResponse {
        abtests,
        count,
        total,
    })
    .into_response()
}

/// Fetch a single experiment by numeric ID or UUID, hydrated with live metrics.
///
/// Retrieves the experiment from the store, converts it to the Algolia DTO format,
/// and overlays click/conversion/search metrics from the analytics engine when available.
///
/// # Returns
///
/// The Algolia-format AB test payload with metric fields populated, or an error response.
#[utoipa::path(
    get,
    path = "/2/abtests/{id}",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    responses(
        (status = 200, description = "Experiment details", body = dto_algolia::AlgoliaAbTest),
        (status = 404, description = "Experiment not found"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    match store.get(&uuid) {
        Ok(experiment) => {
            let mut payload = dto_algolia::experiment_to_algolia_with_updated_at(
                &experiment,
                numeric_id,
                store.get_last_updated_ms(&experiment.id),
            );

            if let Some(engine) = state.analytics_engine.as_ref() {
                let mut index_names = vec![experiment.index_name.as_str()];
                if let Some(variant_index) = experiment.variant.index_name.as_deref() {
                    if variant_index != experiment.index_name {
                        index_names.push(variant_index);
                    }
                }

                match metrics::get_experiment_metrics(
                    &experiment.id,
                    &index_names,
                    &engine.config().data_dir,
                    experiment.winsorization_cap,
                )
                .await
                {
                    Ok(aggregates) => apply_metrics_to_algolia_response(&mut payload, &aggregates),
                    Err(err) => {
                        tracing::warn!(
                            "failed to hydrate abtest metrics for {}: {}",
                            experiment.id,
                            err
                        );
                    }
                }
            }

            Json(payload).into_response()
        }
        Err(err) => experiment_error_to_response(err),
    }
}

/// Replace an experiment's mutable configuration fields while preserving its identity and lifecycle state.
///
/// Merges the request body with the existing experiment, keeping `id`, `status`,
/// timestamps, and conclusion unchanged. Falls back to existing values for optional
/// fields not provided in the request.
///
/// # Returns
///
/// The updated experiment in Algolia DTO format, or an error response.
#[utoipa::path(
    put,
    path = "/2/abtests/{id}",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    request_body(content = CreateExperimentRequest, description = "Updated experiment configuration"),
    responses(
        (status = 200, description = "Experiment updated", body = dto_algolia::AlgoliaAbTest),
        (status = 400, description = "Invalid experiment configuration"),
        (status = 404, description = "Experiment not found"),
        (status = 409, description = "Experiment is in an invalid status for update"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn update_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<CreateExperimentRequest>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    let existing = match store.get(&uuid) {
        Ok(experiment) => experiment,
        Err(err) => return experiment_error_to_response(err),
    };

    let updated = Experiment {
        id: existing.id,
        name: body.name,
        index_name: body.index_name,
        status: existing.status,
        traffic_split: body.traffic_split,
        control: body.control,
        variant: body.variant,
        primary_metric: body.primary_metric,
        created_at: existing.created_at,
        started_at: existing.started_at,
        ended_at: existing.ended_at,
        stopped_at: existing.stopped_at,
        minimum_days: body.minimum_days.unwrap_or(existing.minimum_days),
        winsorization_cap: body.winsorization_cap.or(existing.winsorization_cap),
        conclusion: existing.conclusion,
        interleaving: body.interleaving.or(existing.interleaving),
    };

    match store.update(updated) {
        Ok(experiment) => Json(dto_algolia::experiment_to_algolia_with_updated_at(
            &experiment,
            numeric_id,
            store.get_last_updated_ms(&experiment.id),
        ))
        .into_response(),
        Err(err) => experiment_error_to_response(err),
    }
}

/// Delete an experiment by numeric ID or UUID.
///
/// Removes the experiment from the persistent store. This operation is irreversible.
///
/// # Returns
///
/// JSON action response confirming deletion with the experiment's numeric ID and index name,
/// or an error if the experiment is not found.
#[utoipa::path(
    delete,
    path = "/2/abtests/{id}",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    responses(
        (status = 200, description = "Experiment deleted", body = AlgoliaAbTestActionResponse),
        (status = 404, description = "Experiment not found"),
        (status = 409, description = "Cannot delete a running experiment"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    let experiment = match store.get(&uuid) {
        Ok(exp) => exp,
        Err(err) => return experiment_error_to_response(err),
    };

    let index_name = experiment.index_name.clone();
    match store.delete(&uuid) {
        Ok(()) => Json(AlgoliaAbTestActionResponse {
            ab_test_id: numeric_id,
            index: index_name,
            task_id: numeric_id,
        })
        .into_response(),
        Err(err) => experiment_error_to_response(err),
    }
}

/// Start a previously created experiment, beginning traffic assignment.
///
/// Resolves the experiment by numeric or UUID path parameter, then transitions
/// its status to running via the store.
///
/// # Returns
///
/// JSON action response with the experiment's numeric ID and index name,
/// or an error if the experiment is not found or not in a startable state.
#[utoipa::path(
    post,
    path = "/2/abtests/{id}/start",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    responses(
        (status = 200, description = "Experiment started", body = AlgoliaAbTestActionResponse),
        (status = 404, description = "Experiment not found"),
        (status = 409, description = "Experiment is in an invalid status for start"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn start_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    lifecycle_action_response(&state, &id, ExperimentStore::start)
}

/// Stop a running experiment, freezing its traffic assignment.
///
/// Resolves the experiment by numeric or UUID path parameter, then transitions
/// its status to stopped via the store.
///
/// # Returns
///
/// JSON action response with the experiment's numeric ID and index name,
/// or an error if the experiment is not found or not in a stoppable state.
#[utoipa::path(
    post,
    path = "/2/abtests/{id}/stop",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    responses(
        (status = 200, description = "Experiment stopped", body = AlgoliaAbTestActionResponse),
        (status = 404, description = "Experiment not found"),
        (status = 409, description = "Experiment is in an invalid status for stop"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn stop_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    lifecycle_action_response(&state, &id, ExperimentStore::stop)
}

/// Conclude a running experiment by selecting a winner and promoting the winning configuration.
#[utoipa::path(
    post,
    path = "/2/abtests/{id}/conclude",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    request_body(content = ConcludeExperimentRequest, description = "Conclusion summary and winner declaration"),
    responses(
        (status = 200, description = "Experiment concluded", body = ConcludedExperimentResponse),
        (status = 400, description = "Invalid conclusion payload"),
        (status = 404, description = "Experiment not found"),
        (status = 409, description = "Experiment is in an invalid status for conclude"),
        (status = 503, description = "Experiment store unavailable")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn conclude_experiment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<ConcludeExperimentRequest>,
) -> Response {
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, _numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    let winner = match validate_conclusion_winner(body.winner) {
        Ok(winner) => winner,
        Err(err) => return experiment_error_to_response(err),
    };

    let conclusion = ExperimentConclusion {
        winner,
        reason: body.reason,
        control_metric: body.control_metric,
        variant_metric: body.variant_metric,
        confidence: body.confidence,
        significant: body.significant,
        promoted: body.promoted,
    };

    match store.conclude(&uuid, conclusion) {
        Ok(experiment) => {
            let mut promotion_warning = None;
            if experiment.conclusion.as_ref().is_some_and(|c| c.promoted)
                && experiment
                    .conclusion
                    .as_ref()
                    .and_then(|c| c.winner.as_deref())
                    == Some("variant")
            {
                if let Err(e) = promote_variant_settings(&state, &experiment) {
                    tracing::error!(
                        experiment_id = %experiment.id,
                        error = %e,
                        "failed to promote variant settings"
                    );
                    // Conclude succeeded, promotion failed — return the experiment
                    // with a warning header so the caller knows promotion was partial.
                    promotion_warning = Some(VARIANT_PROMOTION_FAILED_WARNING);
                }
            }
            match concluded_experiment_response(experiment) {
                Ok(response) => attach_experiment_warning_header(
                    Json(response).into_response(),
                    promotion_warning,
                ),
                Err(response) => attach_experiment_warning_header(response, promotion_warning),
            }
        }
        Err(err) => experiment_error_to_response(err),
    }
}

// ── Sub-modules ──────────────────────────────────────────────────
pub(crate) mod estimate;
pub(crate) mod results;

pub use estimate::estimate_ab_test;
pub use results::get_experiment_results;

#[cfg(test)]
use flapjack::experiments::stats;
#[cfg(test)]
use results::*;

#[cfg(test)]
mod review_regression_tests {
    use super::*;
    use crate::test_helpers::{body_json, send_empty_request, send_json_request, TestStateBuilder};
    use axum::{
        http::{Method, StatusCode},
        routing::post,
        Router,
    };
    use tempfile::TempDir;

    fn conclude_test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/2/abtests", post(create_experiment))
            .route("/2/abtests/:id/start", post(start_experiment))
            .route("/2/abtests/:id/conclude", post(conclude_experiment))
            .with_state(state)
    }

    #[tokio::test]
    async fn conclude_experiment_sets_warning_header_when_variant_promotion_fails() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp)
            .with_experiments()
            .build_shared();
        let app = conclude_test_router(state.clone());

        state.manager.create_tenant("products").unwrap();

        let create_resp = send_json_request(
            &app,
            Method::POST,
            "/2/abtests",
            serde_json::json!({
                "name": "Promotion warning test",
                "variants": [
                    { "index": "products", "trafficPercentage": 50, "description": "control" },
                    { "index": "products_v2", "trafficPercentage": 50, "description": "variant" }
                ],
                "endAt": "2099-01-01T00:00:00Z",
                "metrics": [{ "name": "clickThroughRate" }]
            }),
        )
        .await;
        assert_eq!(create_resp.status(), StatusCode::OK);
        let id = body_json(create_resp).await["abTestID"].as_i64().unwrap();

        let start_resp =
            send_empty_request(&app, Method::POST, &format!("/2/abtests/{id}/start")).await;
        assert_eq!(start_resp.status(), StatusCode::OK);

        let conclude_resp = send_json_request(
            &app,
            Method::POST,
            &format!("/2/abtests/{id}/conclude"),
            serde_json::json!({
                "winner": "variant",
                "reason": "Promotion should warn when variant settings are missing",
                "controlMetric": 0.12,
                "variantMetric": 0.14,
                "confidence": 0.97,
                "significant": true,
                "promoted": true
            }),
        )
        .await;

        assert_eq!(conclude_resp.status(), StatusCode::OK);
        assert_eq!(
            conclude_resp
                .headers()
                .get(EXPERIMENT_WARNING_HEADER_NAME)
                .and_then(|value| value.to_str().ok()),
            Some(VARIANT_PROMOTION_FAILED_WARNING)
        );
    }
}

#[cfg(test)]
#[path = "../experiments_tests.rs"]
mod tests;
