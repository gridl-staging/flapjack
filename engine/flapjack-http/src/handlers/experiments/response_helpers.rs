use super::{
    dto_algolia, metrics, resolve::resolve_store_and_experiment_id, AppState, Experiment,
    ExperimentArm, ExperimentConclusion, ExperimentError, ExperimentStatus, ExperimentStore,
    PrimaryMetric, EXPERIMENT_WARNING_HEADER_NAME,
};
use crate::error_response::json_error;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

/// Map an `ExperimentError` variant to the appropriate HTTP status code and JSON error body.
///
/// Translates validation errors to 400, not-found to 404, status conflicts to 409,
/// and I/O or serialization errors to 500.
pub(super) fn experiment_error_to_response(err: ExperimentError) -> Response {
    match err {
        ExperimentError::InvalidConfig(message) => json_error(StatusCode::BAD_REQUEST, message),
        ExperimentError::NotFound(message) => json_error(StatusCode::NOT_FOUND, message),
        ExperimentError::InvalidStatus(message) => json_error(StatusCode::CONFLICT, message),
        ExperimentError::AlreadyExists(message) => json_error(StatusCode::CONFLICT, message),
        ExperimentError::Io(error) => {
            tracing::error!(error = %error, "experiment storage I/O failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }
        ExperimentError::Json(error) => {
            tracing::error!(error = %error, "experiment serialization failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        }
    }
}

/// Build the Algolia-compatible action payload returned by create/start/stop/delete endpoints.
pub(super) fn algolia_action_response(numeric_id: i64, index_name: String) -> Response {
    Json(super::dto_algolia::AlgoliaAbTestActionResponse {
        ab_test_id: numeric_id,
        index: index_name,
        task_id: numeric_id,
    })
    .into_response()
}

/// Resolve an experiment path parameter and execute a lifecycle transition against the store.
pub(super) fn lifecycle_action_response(
    state: &AppState,
    id_str: &str,
    action: fn(&ExperimentStore, &str) -> Result<Experiment, ExperimentError>,
) -> Response {
    let (store, uuid, numeric_id) = match resolve_store_and_experiment_id(state, id_str) {
        Ok(values) => values,
        Err(response) => return response,
    };

    match action(store, &uuid) {
        Ok(experiment) => algolia_action_response(numeric_id, experiment.index_name),
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

pub(super) fn apply_metrics_to_algolia_response(
    payload: &mut dto_algolia::AlgoliaAbTest,
    metrics: &metrics::ExperimentMetrics,
) {
    if payload.variants.len() < 2 || !has_any_variant_metrics(metrics) {
        return;
    }
    fill_algolia_variant_metrics(&mut payload.variants[0], &metrics.control);
    fill_algolia_variant_metrics(&mut payload.variants[1], &metrics.variant);
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

/// Convert a concluded experiment into the stronger response DTO used by the conclude endpoint.
///
/// The store contract says a concluded experiment should always carry a conclusion payload.
/// If that invariant is broken, we return a sanitized 500 response instead of serializing
/// an impossible shape to clients.
#[allow(clippy::result_large_err)] // Response is inherently large in axum; boxing adds indirection without benefit at a single call site
pub(super) fn concluded_experiment_response(
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

pub(super) fn attach_experiment_warning_header(
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn response_json(resp: Response) -> (StatusCode, serde_json::Value) {
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn experiment_io_errors_are_sanitized() {
        let response = experiment_error_to_response(ExperimentError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "/var/lib/flapjack/experiments.json: permission denied",
        )));
        let (status, json) = response_json(response).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["message"], "Internal server error");
    }

    #[tokio::test]
    async fn experiment_invalid_config_errors_remain_client_visible() {
        let response = experiment_error_to_response(ExperimentError::InvalidConfig(
            "bad traffic split".into(),
        ));
        let (status, json) = response_json(response).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["message"], "bad traffic split");
    }
}
