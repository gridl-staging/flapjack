//! Extended mutation parity tests that go beyond the matrix's status/field checks
//! to verify sequential state transitions, error paths, and duplicate-handling
//! behavior that the core parity test intentionally does not cover.

use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity-extended";

// ── Sequential lifecycle: create → update → verify → delete ──

/// Verify that an API key survives a full create → update → list → delete
/// lifecycle with each step producing the expected status and envelope.
#[tokio::test]
async fn key_lifecycle_create_update_list_delete() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Step 1: Create a key.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "lifecycle test key"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create key failed: {body}");
    let key = body["key"]
        .as_str()
        .expect("create response must include key")
        .to_string();
    assert!(
        body["createdAt"].is_string(),
        "create response must include createdAt"
    );

    // Step 2: Update the key to add browse ACL.
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        &format!("/1/keys/{key}"),
        ADMIN_KEY,
        Some(json!({
            "acl": ["search", "browse"],
            "description": "lifecycle test key updated"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update key failed: {body}");
    assert_eq!(body["key"], key, "update must echo the same key");
    assert!(
        body["updatedAt"].is_string(),
        "update response must include updatedAt"
    );

    // Step 3: List keys and verify the updated key is present.
    let (status, body) = common::send_json(&app, Method::GET, "/1/keys", ADMIN_KEY, None).await;
    assert_eq!(status, StatusCode::OK, "list keys failed: {body}");
    let keys_array = body["keys"]
        .as_array()
        .expect("list response must include keys array");
    let found = keys_array.iter().any(|k| k["value"].as_str() == Some(&key));
    assert!(found, "updated key must appear in keys list");

    // Step 4: Delete the key.
    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        &format!("/1/keys/{key}"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete key failed: {body}");
    assert!(
        body["deletedAt"].is_string(),
        "delete response must include deletedAt"
    );

    // Step 5: Verify the key is gone from the list.
    let (status, body) = common::send_json(&app, Method::GET, "/1/keys", ADMIN_KEY, None).await;
    assert_eq!(status, StatusCode::OK);
    let keys_array = body["keys"]
        .as_array()
        .expect("list response must include keys array");
    let still_present = keys_array.iter().any(|k| k["value"].as_str() == Some(&key));
    assert!(
        !still_present,
        "deleted key must not appear in keys list after deletion"
    );
}

/// Verify that an object survives a full create → partial update → read → delete
/// lifecycle with each step using the correct mutation parity status code.
#[tokio::test]
async fn object_lifecycle_create_partial_update_read_delete() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let index_name = "lifecycle_objects";

    // Step 1: Create object via batch.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "addObject",
                    "body": {
                        "objectID": "lifecycle-doc-1",
                        "title": "Original Title",
                        "category": "books"
                    }
                }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "batch create failed: {body}");
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local_with_key(&app, task_id, ADMIN_KEY).await;

    // Step 2: Partial update the title.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/indexes/{index_name}/lifecycle-doc-1/partial"),
        ADMIN_KEY,
        Some(json!({ "title": "Updated Title" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "partial update failed: {body}");
    assert_eq!(body["objectID"], "lifecycle-doc-1");
    assert!(
        body["taskID"].is_number(),
        "partial update must return numeric taskID"
    );
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local_with_key(&app, task_id, ADMIN_KEY).await;

    // Step 3: Read the object and verify the update took effect.
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/{index_name}/lifecycle-doc-1"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get object failed: {body}");
    assert_eq!(
        body["title"], "Updated Title",
        "partial update must have taken effect on the stored document"
    );
    // Category should still be present from the original batch write.
    assert_eq!(
        body["category"], "books",
        "partial update must not erase unmodified fields"
    );

    // Step 4: Delete the object.
    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        &format!("/1/indexes/{index_name}/lifecycle-doc-1"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete object failed: {body}");
    assert!(
        body["deletedAt"].is_string(),
        "delete response must include deletedAt"
    );
    assert!(
        body["taskID"].is_number(),
        "delete response must include numeric taskID"
    );
}

// ── Nonexistent resource error paths ──

/// Requesting a nonexistent API key for update should return a well-formed error.
#[tokio::test]
async fn update_nonexistent_key_returns_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/keys/nonexistent-key-00000000",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "should fail"
        })),
    )
    .await;

    // Algolia returns 404 for key-not-found on update.
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "updating a nonexistent key should return 404, got: {status} {body}"
    );
    assert!(
        body["message"].is_string(),
        "error response must include a message field"
    );
    assert_eq!(
        body["status"], 404,
        "error response status field must match HTTP status"
    );
}

/// Deleting a nonexistent API key should return a well-formed error.
#[tokio::test]
async fn delete_nonexistent_key_returns_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        "/1/keys/nonexistent-key-delete-00000000",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "deleting a nonexistent key should return 404, got: {status} {body}"
    );
    assert!(
        body["message"].is_string(),
        "error response must include a message field"
    );
}

/// Getting a nonexistent experiment should return a well-formed error.
#[tokio::test]
async fn get_nonexistent_experiment_returns_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) =
        common::send_json(&app, Method::GET, "/2/abtests/99999", ADMIN_KEY, None).await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "getting a nonexistent experiment should return 404, got: {status} {body}"
    );
    assert!(
        body["message"].is_string(),
        "error response must include a message field"
    );
}

/// Deleting a nonexistent experiment should return a well-formed error.
#[tokio::test]
async fn delete_nonexistent_experiment_returns_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) =
        common::send_json(&app, Method::DELETE, "/2/abtests/99999", ADMIN_KEY, None).await;

    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "deleting a nonexistent experiment should return 404, got: {status} {body}"
    );
    assert!(
        body["message"].is_string(),
        "error response must include a message field"
    );
}

// ── Duplicate / conflict handling ──

/// Restoring an API key that was never deleted should produce a well-formed error
/// (it's not valid to restore a key that still exists).
#[tokio::test]
async fn restore_non_deleted_key_returns_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a key (still alive — not deleted).
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "restore conflict test"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create key failed: {body}");
    let key = body["key"].as_str().unwrap().to_string();

    // Try to restore a key that is still alive — should fail.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/keys/{key}/restore"),
        ADMIN_KEY,
        None,
    )
    .await;

    // Algolia returns 404 when trying to restore a non-deleted key.
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "restoring a non-deleted key should return 404, got: {status} {body}"
    );
    assert!(
        body["message"].is_string(),
        "error response must include a message field"
    );
}
