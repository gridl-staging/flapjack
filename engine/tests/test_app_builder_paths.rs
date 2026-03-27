#[cfg(target_os = "linux")]
use axum::body::Body;
#[cfg(target_os = "linux")]
use axum::http::{Request, StatusCode};
#[cfg(target_os = "linux")]
use std::ffi::OsStr;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use tower::ServiceExt;

mod common;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_build_test_app_for_existing_data_dir_accepts_non_utf8_path() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let data_dir = temp_dir.path().join(OsStr::from_bytes(b"utf8-ok-\xff"));
    std::fs::create_dir(&data_dir).unwrap();

    let app = common::build_test_app_for_existing_data_dir(&data_dir, Some("admin-key"));
    let request = Request::builder()
        .method("GET")
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
