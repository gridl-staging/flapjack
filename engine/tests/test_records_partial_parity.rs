use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

#[tokio::test]
async fn partial_update_put_create_if_not_exists_false_returns_404_json_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/products/missing/partial?createIfNotExists=false",
        ADMIN_KEY,
        Some(json!({"title": "Ghost"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["status"], json!(404));
    assert_eq!(body["message"], json!("ObjectID does not exist"));
}

#[tokio::test]
async fn partial_update_put_create_if_not_exists_true_creates_record() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, write_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/products/new-item/partial?createIfNotExists=true",
        ADMIN_KEY,
        Some(json!({"title": "Created by partial"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(write_body["objectID"], json!("new-item"));
    common::wait_for_task_local(&app, common::extract_task_id(&write_body)).await;

    let (get_status, obj) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/new-item",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(obj["objectID"], json!("new-item"));
    assert_eq!(obj["title"], json!("Created by partial"));
}

#[tokio::test]
async fn batch_partial_update_no_create_silently_skips_missing_object() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, batch_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "partialUpdateObjectNoCreate",
                    "body": {"objectID": "ghost", "title": "Should not exist"}
                }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(batch_body["taskID"].is_i64());
    assert_eq!(batch_body["objectIDs"], json!(["ghost"]));
    common::wait_for_task_local(&app, common::extract_task_id(&batch_body)).await;

    let (get_status, _) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/ghost",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::NOT_FOUND);
}

/// Batch `partialUpdateObject` should create the record when it doesn't exist,
/// unlike `partialUpdateObjectNoCreate` which silently skips.
#[tokio::test]
async fn batch_partial_update_object_creates_missing_record() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, batch_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "partialUpdateObject",
                    "body": {"objectID": "created-via-partial", "title": "Auto-created"}
                }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(batch_body["taskID"].is_i64());
    assert_eq!(batch_body["objectIDs"], json!(["created-via-partial"]));
    common::wait_for_task_local(&app, common::extract_task_id(&batch_body)).await;

    let (get_status, obj) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/created-via-partial",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(
        get_status,
        StatusCode::OK,
        "partialUpdateObject should create missing record"
    );
    assert_eq!(obj["objectID"], json!("created-via-partial"));
    assert_eq!(obj["title"], json!("Auto-created"));
}
