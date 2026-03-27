use super::AppState;
use crate::security_sources::{SecuritySourceEntry, SecuritySourcesStore};
use axum::{
    extract::{Path, State},
    Json,
};
use flapjack::error::FlapjackError;
use std::sync::Arc;

#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct SourceMutationTimestampResponse {
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "deletedAt", skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
}

fn store_from_state(state: &Arc<AppState>) -> SecuritySourcesStore {
    SecuritySourcesStore::new(&state.manager.base_path)
}

/// Get all configured source CIDR allowlist entries.
#[utoipa::path(
    get,
    path = "/1/security/sources",
    tag = "security",
    responses(
        (status = 200, description = "Current source allowlist entries", body = [SecuritySourceEntry])
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_security_sources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<SecuritySourceEntry>>, FlapjackError> {
    let entries = store_from_state(&state).list()?;
    Ok(Json(entries))
}

/// Replace the entire source CIDR allowlist.
#[utoipa::path(
    put,
    path = "/1/security/sources",
    tag = "security",
    request_body = [SecuritySourceEntry],
    responses(
        (status = 200, description = "Allowlist replaced", body = SourceMutationTimestampResponse),
        (status = 400, description = "Malformed CIDR")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn replace_security_sources(
    State(state): State<Arc<AppState>>,
    Json(entries): Json<Vec<SecuritySourceEntry>>,
) -> Result<Json<SourceMutationTimestampResponse>, FlapjackError> {
    store_from_state(&state).replace(entries)?;
    Ok(Json(SourceMutationTimestampResponse {
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
        created_at: None,
        deleted_at: None,
    }))
}

/// Append or update a single source CIDR entry.
#[utoipa::path(
    post,
    path = "/1/security/sources/append",
    tag = "security",
    request_body = SecuritySourceEntry,
    responses(
        (status = 200, description = "Source appended", body = SourceMutationTimestampResponse),
        (status = 400, description = "Malformed CIDR")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn append_security_source(
    State(state): State<Arc<AppState>>,
    Json(entry): Json<SecuritySourceEntry>,
) -> Result<Json<SourceMutationTimestampResponse>, FlapjackError> {
    store_from_state(&state).append(entry)?;
    Ok(Json(SourceMutationTimestampResponse {
        updated_at: None,
        created_at: Some(chrono::Utc::now().to_rfc3339()),
        deleted_at: None,
    }))
}

/// Delete a source CIDR allowlist entry.
#[utoipa::path(
    delete,
    path = "/1/security/sources/{source}",
    tag = "security",
    params(
        ("source" = String, Path, description = "CIDR to remove (URL encoded)")
    ),
    responses(
        (status = 200, description = "Source removed", body = SourceMutationTimestampResponse),
        (status = 400, description = "Malformed CIDR")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_security_source(
    State(state): State<Arc<AppState>>,
    Path(source): Path<String>,
) -> Result<Json<SourceMutationTimestampResponse>, FlapjackError> {
    // Axum decodes path params, but decode defensively in case upstream forwards encoded content.
    let decoded_source = urlencoding::decode(&source)
        .map(|value| value.into_owned())
        .unwrap_or(source);

    store_from_state(&state).delete(&decoded_source)?;
    Ok(Json(SourceMutationTimestampResponse {
        updated_at: None,
        created_at: None,
        deleted_at: Some(chrono::Utc::now().to_rfc3339()),
    }))
}
