use super::AppState;
use crate::error_response::json_error;
use crate::extractors::ValidatedIndexName;
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use flapjack::index::s3::S3Config;
use flapjack::index::snapshot::export_to_bytes;
use std::{path::PathBuf, sync::Arc};

fn s3_config_or_error(message: &'static str) -> Result<S3Config, Box<Response>> {
    S3Config::from_env().ok_or_else(|| {
        Box::new(json_error(StatusCode::SERVICE_UNAVAILABLE, message).into_response())
    })
}

fn index_path_or_404(state: &AppState, index_name: &str) -> Result<PathBuf, Box<Response>> {
    let index_path = state.manager.base_path.join(index_name);
    if index_path.exists() {
        Ok(index_path)
    } else {
        Err(Box::new(
            json_error(StatusCode::NOT_FOUND, "Index not found").into_response(),
        ))
    }
}

fn internal_error(prefix: &str, error: impl std::fmt::Display) -> Response {
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("{prefix}: {error}"),
    )
    .into_response()
}

fn snapshot_retention() -> usize {
    std::env::var("FLAPJACK_SNAPSHOT_RETENTION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(24)
}

/// TODO: Document download_restore_payload.
async fn download_restore_payload(
    s3_config: &S3Config,
    index_name: &str,
    key_override: Option<String>,
) -> Result<(String, Vec<u8>), Box<Response>> {
    if let Some(key) = key_override {
        let data = flapjack::index::s3::download_snapshot(s3_config, &key)
            .await
            .map_err(|error| Box::new(internal_error("S3 download failed", error)))?;
        Ok((key, data))
    } else {
        flapjack::index::s3::download_latest_snapshot(s3_config, index_name)
            .await
            .map_err(|error| {
                Box::new(json_error(StatusCode::NOT_FOUND, error.to_string()).into_response())
            })
    }
}

/// TODO: Document export_snapshot.
#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}/export",
    tag = "snapshots",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "Snapshot file", body = Vec<u8>),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn export_snapshot(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
) -> Response {
    let index_path = match index_path_or_404(state.as_ref(), &index_name) {
        Ok(path) => path,
        Err(response) => return *response,
    };

    match export_to_bytes(&index_path) {
        Ok(bytes) => {
            let headers = [
                ("Content-Type", "application/gzip"),
                (
                    "Content-Disposition",
                    &format!("attachment; filename=\"{}.tar.gz\"", index_name),
                ),
            ];
            (headers, bytes).into_response()
        }
        Err(e) => {
            tracing::error!("Export failed: {:?}", e);
            internal_error("Export failed", e)
        }
    }
}

/// Import index from uploaded snapshot
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/import",
    tag = "snapshots",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = Vec<u8>, description = "Snapshot tar.gz file"),
    responses(
        (status = 200, description = "Import successful", body = serde_json::Value),
        (status = 500, description = "Import failed")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn import_snapshot(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
    body: Bytes,
) -> Response {
    match crate::startup_catchup::install_snapshot_bytes(&state.manager, &index_name, &body) {
        Ok(()) => Json(serde_json::json!({ "status": "imported" })).into_response(),
        Err(e) => {
            tracing::error!("Import failed: {:?}", e);
            internal_error("Import failed", e)
        }
    }
}

/// TODO: Document snapshot_to_s3.
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/snapshot",
    tag = "snapshots",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "Snapshot uploaded to S3", body = serde_json::Value),
        (status = 503, description = "S3 not configured"),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn snapshot_to_s3(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
) -> Response {
    let s3_config = match s3_config_or_error(
        "S3 not configured. Set FLAPJACK_S3_BUCKET and FLAPJACK_S3_REGION.",
    ) {
        Ok(config) => config,
        Err(response) => return *response,
    };
    let index_path = match index_path_or_404(state.as_ref(), &index_name) {
        Ok(path) => path,
        Err(response) => return *response,
    };

    let bytes = match export_to_bytes(&index_path) {
        Ok(b) => b,
        Err(e) => return internal_error("Export failed", e),
    };

    match flapjack::index::s3::upload_snapshot(&s3_config, &index_name, &bytes).await {
        Ok(key) => {
            let _ = flapjack::index::s3::enforce_retention(
                &s3_config,
                &index_name,
                snapshot_retention(),
            )
            .await;

            Json(serde_json::json!({
                "status": "uploaded",
                "key": key,
                "size_bytes": bytes.len(),
            }))
            .into_response()
        }
        Err(e) => internal_error("S3 upload failed", e),
    }
}

/// TODO: Document restore_from_s3.
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/restore",
    tag = "snapshots",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Restore options with snapshot ID"),
    responses(
        (status = 200, description = "Restore successful", body = serde_json::Value),
        (status = 503, description = "S3 not configured"),
        (status = 404, description = "Snapshot not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn restore_from_s3(
    State(state): State<Arc<AppState>>,
    ValidatedIndexName(index_name): ValidatedIndexName,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let s3_config = match s3_config_or_error("S3 not configured") {
        Ok(config) => config,
        Err(response) => return *response,
    };

    let key_override = body.and_then(|b| b.get("key").and_then(|v| v.as_str()).map(String::from));

    let (key, data) = match download_restore_payload(&s3_config, &index_name, key_override).await {
        Ok(payload) => payload,
        Err(response) => return *response,
    };

    let data_len = data.len();
    match crate::startup_catchup::install_snapshot_bytes(&state.manager, &index_name, &data) {
        Ok(()) => Json(serde_json::json!({
            "status": "restored",
            "key": key,
            "size_bytes": data_len,
        }))
        .into_response(),
        Err(e) => internal_error("Restore failed", e),
    }
}

/// List available S3 snapshots for an index
#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}/snapshots",
    tag = "snapshots",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "List of snapshots", body = serde_json::Value),
        (status = 503, description = "S3 not configured")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn list_s3_snapshots(ValidatedIndexName(index_name): ValidatedIndexName) -> Response {
    let s3_config = match s3_config_or_error("S3 not configured") {
        Ok(config) => config,
        Err(response) => return *response,
    };

    match flapjack::index::s3::list_snapshots(&s3_config, &index_name).await {
        Ok(keys) => Json(serde_json::json!({ "snapshots": keys })).into_response(),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{export_snapshot, import_snapshot};
    use crate::test_helpers::{body_json, TestStateBuilder};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
        Router,
    };
    use flapjack::index::snapshot::export_to_bytes;
    use flapjack::types::{Document, FieldValue};
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tower::ServiceExt;

    /// TODO: Document export_snapshot_missing_index_returns_json_without_router_error_wrapper.
    #[tokio::test]
    async fn export_snapshot_missing_index_returns_json_without_router_error_wrapper() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();
        let app = Router::new()
            .route("/1/indexes/:indexName/export", get(export_snapshot))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/1/indexes/missing/export")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("application/json"),
            "expected JSON error content-type, got: {content_type}"
        );
        assert_eq!(
            body_json(response).await,
            serde_json::json!({
                "message": "Index not found",
                "status": 404
            })
        );
    }

    /// TODO: Document import_snapshot_success_returns_json_without_router_error_wrapper.
    #[tokio::test]
    async fn import_snapshot_success_returns_json_without_router_error_wrapper() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();
        state.manager.create_tenant("products").unwrap();
        state
            .manager
            .add_documents_sync(
                "products",
                vec![Document {
                    id: "1".to_string(),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text("snapshot source".to_string()),
                    )]),
                }],
            )
            .await
            .unwrap();
        let snapshot_bytes = export_to_bytes(&state.manager.base_path.join("products")).unwrap();

        let app = Router::new()
            .route("/1/indexes/:indexName/import", post(import_snapshot))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/1/indexes/products/import")
                    .body(Body::from(snapshot_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("application/json"),
            "expected JSON success content-type, got: {content_type}"
        );
        assert_eq!(
            body_json(response).await,
            serde_json::json!({
                "status": "imported"
            })
        );
    }
}
