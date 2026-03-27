//! Axum handler that serves the embedded single-page dashboard application, with MIME detection and client-side routing fallback.
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../dashboard/dist/"]
struct DashboardAssets;

/// Serve embedded dashboard assets with SPA-style client-side routing fallback.
///
/// Resolves the request URI against the embedded `DashboardAssets` (built from `../dashboard/dist/`). Requests with no path default to `index.html`. Requests with a file extension that don't match an embedded file return 404. Extensionless paths fall back to `index.html` to support client-side routing.
pub async fn dashboard_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    // Default to index.html for empty path
    let path = if path.is_empty() { "index.html" } else { path };

    // Try to serve the exact file
    if let Some(file) = DashboardAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime.as_ref())],
            file.data,
        )
            .into_response()
    } else if path.contains('.') {
        // Has a file extension but wasn't found — genuine 404
        StatusCode::NOT_FOUND.into_response()
    } else {
        // No file extension — SPA route, serve index.html for client-side routing
        match DashboardAssets::get("index.html") {
            Some(file) => Html(file.data).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
}
