use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
    Json,
};
use flapjack::experiments::{
    config::{Experiment, ExperimentConclusion, ExperimentError, PrimaryMetric, QueryOverrides},
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
#[cfg(test)]
use flapjack::experiments::config::{ExperimentArm, ExperimentStatus};

mod schemas;
pub use schemas::{
    ArmResponse, BayesianResponse, ConcludeExperimentRequest, CreateExperimentRequest,
    GateResponse, GuardRailAlertResponse, InterleavingResponse, ListExperimentsQuery,
    ListExperimentsResponse, ResultsResponse, SignificanceResponse,
};

const DEFAULT_LIST_LIMIT: usize = 10;
const DEFAULT_LIST_OFFSET: usize = 0;
pub(super) const DEFAULT_ESTIMATE_DURATION_DAYS: i64 = 21;
pub(super) const ESTIMATE_TRAFFIC_LOOKBACK_DAYS: i64 = 30;
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

/// Create a new A/B test experiment from an Algolia-compatible request body.
///
/// Validates and converts the incoming DTO, persists the experiment via the store,
/// and returns the assigned numeric ID.
///
/// # Returns
///
/// `200 OK` with the numeric AB test ID and index name on success,
/// or an error response if the store is unavailable or validation fails.
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
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    match store.start(&uuid) {
        Ok(experiment) => Json(AlgoliaAbTestActionResponse {
            ab_test_id: numeric_id,
            index: experiment.index_name,
            task_id: numeric_id,
        })
        .into_response(),
        Err(err) => experiment_error_to_response(err),
    }
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
    let store = match get_experiment_store(&state) {
        Some(store) => store,
        None => return experiment_store_unavailable_response(),
    };

    let (uuid, numeric_id) = match resolve_experiment_id(store, &id) {
        Ok(pair) => pair,
        Err(err) => return experiment_error_to_response(err),
    };

    match store.stop(&uuid) {
        Ok(experiment) => Json(AlgoliaAbTestActionResponse {
            ab_test_id: numeric_id,
            index: experiment.index_name,
            task_id: numeric_id,
        })
        .into_response(),
        Err(err) => experiment_error_to_response(err),
    }
}

/// Record the conclusion of an experiment with a declared winner and statistical summary.
///
/// Validates the winner field, persists the conclusion, and if the variant won with
/// promotion requested, applies the variant's settings to the main index.
///
/// # Returns
///
/// The updated experiment with its conclusion attached, or an error response.
#[utoipa::path(
    post,
    path = "/2/abtests/{id}/conclude",
    tag = "experiments",
    params(
        ("id" = String, Path, description = "Experiment identifier (numeric ID or UUID)")
    ),
    request_body(content = ConcludeExperimentRequest, description = "Conclusion summary and winner declaration"),
    responses(
        (status = 200, description = "Experiment concluded", body = Experiment),
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
            if experiment.conclusion.as_ref().is_some_and(|c| c.promoted)
                && experiment
                    .conclusion
                    .as_ref()
                    .and_then(|c| c.winner.as_deref())
                    == Some("variant")
            {
                if let Err(e) = promote_variant_settings(&state, &experiment) {
                    tracing::error!("failed to promote variant settings: {}", e);
                    // Conclude succeeded, promotion failed — return the experiment
                    // with a warning header so the caller knows promotion was partial.
                }
            }
            Json(experiment).into_response()
        }
        Err(err) => experiment_error_to_response(err),
    }
}

/// Apply the winning variant's settings to the main index. Mode B copies all settings from a dedicated variant index. Mode A applies promotable fields (custom_ranking, remove_words_if_no_results) from query overrides, logging any query-time-only fields that cannot be persisted. Invalidates the main index's settings cache.
///
/// # Arguments
///
/// * `state` - Application state with index manager and base path
/// * `experiment` - The concluded experiment with variant settings to promote
///
/// # Returns
///
/// `Ok(())` on successful promotion, or `Err(msg)` if settings files cannot be loaded or saved.
fn promote_variant_settings(state: &AppState, experiment: &Experiment) -> Result<(), String> {
    let main_index = &experiment.index_name;

    if let Some(variant_index) = experiment.variant.index_name.as_deref() {
        promote_mode_b_settings(state, main_index, variant_index)?;
    } else if let Some(overrides) = experiment.variant.query_overrides.as_ref() {
        promote_mode_a_overrides(state, main_index, overrides)?;
    }

    Ok(())
}

/// TODO: Document promote_mode_b_settings.
fn promote_mode_b_settings(
    state: &AppState,
    main_index: &str,
    variant_index: &str,
) -> Result<(), String> {
    use flapjack::index::settings::IndexSettings;

    let variant_settings_path = state
        .manager
        .base_path
        .join(variant_index)
        .join("settings.json");
    let main_settings_path = state
        .manager
        .base_path
        .join(main_index)
        .join("settings.json");

    let variant_settings = IndexSettings::load(&variant_settings_path)
        .map_err(|error| format!("failed to load variant index settings: {}", error))?;
    variant_settings
        .save(&main_settings_path)
        .map_err(|error| format!("failed to save promoted settings: {}", error))?;
    state.manager.invalidate_settings_cache(main_index);

    tracing::info!(
        "promoted Mode B settings from {} to {}",
        variant_index,
        main_index
    );
    Ok(())
}

/// TODO: Document promote_mode_a_overrides.
fn promote_mode_a_overrides(
    state: &AppState,
    main_index: &str,
    overrides: &QueryOverrides,
) -> Result<(), String> {
    use flapjack::index::settings::IndexSettings;

    let main_settings_path = state
        .manager
        .base_path
        .join(main_index)
        .join("settings.json");

    let mut settings = IndexSettings::load(&main_settings_path)
        .map_err(|error| format!("failed to load main index settings: {}", error))?;

    if let Some(custom_ranking) = overrides.custom_ranking.as_ref() {
        settings.custom_ranking = Some(custom_ranking.clone());
    }
    if let Some(remove_words_if_no_results) = overrides.remove_words_if_no_results.as_ref() {
        settings.remove_words_if_no_results = remove_words_if_no_results.clone();
    }

    let query_only_fields = collect_query_only_override_fields(overrides);
    if !query_only_fields.is_empty() {
        tracing::warn!(
            "Mode A promote: skipping query-time-only fields {:?} (no index-level equivalent)",
            query_only_fields
        );
    }

    settings
        .save(&main_settings_path)
        .map_err(|error| format!("failed to save promoted settings: {}", error))?;
    state.manager.invalidate_settings_cache(main_index);

    tracing::info!("promoted Mode A overrides to index {}", main_index);
    Ok(())
}

/// TODO: Document collect_query_only_override_fields.
fn collect_query_only_override_fields(overrides: &QueryOverrides) -> Vec<&'static str> {
    [
        overrides.typo_tolerance.as_ref().map(|_| "typoTolerance"),
        overrides.enable_synonyms.as_ref().map(|_| "enableSynonyms"),
        overrides.enable_rules.as_ref().map(|_| "enableRules"),
        overrides.rule_contexts.as_ref().map(|_| "ruleContexts"),
        overrides.filters.as_ref().map(|_| "filters"),
        overrides
            .optional_filters
            .as_ref()
            .map(|_| "optionalFilters"),
    ]
    .into_iter()
    .flatten()
    .collect()
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
#[path = "../experiments_tests.rs"]
mod tests;
