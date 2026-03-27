/// Stage 1 SDK contract tests: Core CRUD and index lifecycle wire-format parity.
///
/// Each test asserts Algolia-exact response shapes for the endpoints used by
/// the Algolia JavaScript/Rust/Python SDKs. Tests are written red-first; any
/// handler response-shape gap is fixed in the handler, not here.
///
/// Algolia API reference used: https://www.algolia.com/doc/api-reference/api-methods/
use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

// Must match the key used by `common::wait_for_task_local` (hardcoded to "test-admin-key-parity").
const ADMIN_KEY: &str = "test-admin-key-parity";

// ── create via add-object (POST /1/indexes/{index}) ────────────────────────

/// POST /1/indexes/{index} — auto-id create — must return objectID + taskID + createdAt.
/// Algolia reference: saveObject() → { objectID, taskID, createdAt }
#[tokio::test]
async fn create_via_add_object_returns_algolia_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products",
        ADMIN_KEY,
        Some(json!({ "name": "Laptop", "price": 999 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // objectID must be a non-empty string
    let object_id = body["objectID"]
        .as_str()
        .unwrap_or_else(|| panic!("missing string 'objectID' in: {body}"));
    assert!(
        !object_id.is_empty(),
        "'objectID' must not be empty: {body}"
    );

    // taskID + createdAt envelope (also rejects snake_case task_id)
    common::assert_write_task_envelope(&body, "createdAt");

    // Must NOT include snake_case objectID variant
    assert!(
        body.get("object_id").is_none(),
        "must not have 'object_id': {body}"
    );
}

// ── index lifecycle ────────────────────────────────────────────────────────

/// GET /1/indexes — list indices — must return { items: [...], nbPages: <int> }
/// Each item must include: name, createdAt, updatedAt, entries, dataSize, fileSize,
/// lastBuildTimeS, pendingTask; and optionally: primary, replicas, virtual.
#[tokio::test]
async fn list_indices_returns_algolia_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed a document so the index exists.
    let (seed_status, seed_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/test-list-idx",
        ADMIN_KEY,
        Some(json!({ "objectID": "doc1", "title": "Test" })),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let (status, body) = common::send_json(&app, Method::GET, "/1/indexes", ADMIN_KEY, None).await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    assert!(body["items"].is_array(), "'items' must be an array: {body}");
    assert!(
        body["nbPages"].is_u64() || body["nbPages"].is_i64(),
        "'nbPages' must be an integer: {body}"
    );

    let items = body["items"].as_array().unwrap();
    assert!(
        !items.is_empty(),
        "expected at least one item in 'items': {body}"
    );

    let item = items
        .iter()
        .find(|i| i["name"] == "test-list-idx")
        .unwrap_or_else(|| panic!("index 'test-list-idx' not found in items: {body}"));

    // Required string fields
    for field in &["name", "createdAt", "updatedAt"] {
        assert!(
            item[field].is_string(),
            "items[].{} must be a string: {item}",
            field
        );
    }
    // createdAt / updatedAt must be valid ISO-8601
    common::assert_iso8601(item["createdAt"].as_str().unwrap(), "items[].createdAt");
    common::assert_iso8601(item["updatedAt"].as_str().unwrap(), "items[].updatedAt");

    // Required integer fields
    for field in &["entries", "dataSize", "fileSize", "lastBuildTimeS"] {
        assert!(
            item[field].is_u64() || item[field].is_i64(),
            "items[].{} must be an integer: {item}",
            field
        );
    }

    // pendingTask must be a bool
    assert!(
        item["pendingTask"].is_boolean(),
        "items[].pendingTask must be a boolean: {item}"
    );
}

/// DELETE /1/indexes/{index} — must return { taskID, deletedAt }.
/// Algolia reference: deleteIndex() → { taskID, deletedAt }
#[tokio::test]
async fn delete_index_returns_task_id_and_deleted_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create the index first.
    let (seed_status, seed_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/to-delete-idx",
        ADMIN_KEY,
        Some(json!({ "objectID": "d1", "v": 1 })),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        "/1/indexes/to-delete-idx",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    common::assert_write_task_envelope(&body, "deletedAt");
    // Must NOT use updatedAt for delete responses
    assert!(
        body.get("updatedAt").is_none(),
        "delete-index must not return 'updatedAt', got: {body}"
    );
}

/// POST /1/indexes/{index}/clear — must return { taskID, updatedAt }.
/// Algolia reference: clearIndex() → { taskID, updatedAt }
#[tokio::test]
async fn clear_index_returns_task_id_and_updated_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed some data.
    let (seed_status, seed_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/to-clear-idx",
        ADMIN_KEY,
        Some(json!({ "objectID": "c1", "v": 1 })),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/to-clear-idx/clear",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    common::assert_write_task_envelope(&body, "updatedAt");
}

// ── object CRUD response shapes ────────────────────────────────────────────

/// PUT /1/indexes/{index}/{objectID} — save/replace — must return { objectID, taskID, updatedAt }.
/// Algolia reference: saveObject() with explicit objectID → { objectID, taskID, updatedAt }
#[tokio::test]
async fn put_object_returns_object_id_task_id_updated_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/products/obj-123",
        ADMIN_KEY,
        Some(json!({ "name": "Keyboard", "price": 79 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    assert_eq!(
        body["objectID"].as_str(),
        Some("obj-123"),
        "'objectID' must equal path param: {body}"
    );
    common::assert_write_task_envelope(&body, "updatedAt");
}

/// GET /1/indexes/{index}/{objectID} — retrieve — must include objectID in response.
/// Algolia reference: getObject() → { objectID, ...fields }
#[tokio::test]
async fn get_object_includes_object_id() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed
    let (seed_status, seed_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/products/item-42",
        ADMIN_KEY,
        Some(json!({ "title": "Widget", "qty": 10 })),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/item-42",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(
        body["objectID"].as_str(),
        Some("item-42"),
        "'objectID' must be present: {body}"
    );
    assert_eq!(body["title"].as_str(), Some("Widget"));
}

/// POST /1/indexes/{index}/{objectID}/partial — partial update — must return { objectID, taskID, updatedAt }.
/// Algolia reference: partialUpdateObject() → { objectID, taskID, updatedAt }
#[tokio::test]
async fn partial_update_returns_object_id_task_id_updated_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/part-99/partial",
        ADMIN_KEY,
        Some(json!({ "price": 55 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    assert_eq!(
        body["objectID"].as_str(),
        Some("part-99"),
        "'objectID' must equal path param: {body}"
    );
    common::assert_write_task_envelope(&body, "updatedAt");
}

/// DELETE /1/indexes/{index}/{objectID} — must return { taskID, deletedAt }.
/// Algolia reference: deleteObject() → { taskID, deletedAt }
#[tokio::test]
async fn delete_object_returns_task_id_and_deleted_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed the object first.
    let (seed_status, seed_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/products/del-obj-1",
        ADMIN_KEY,
        Some(json!({ "title": "ToDelete" })),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        "/1/indexes/products/del-obj-1",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    common::assert_write_task_envelope(&body, "deletedAt");
    // Must NOT use updatedAt for delete responses
    assert!(
        body.get("updatedAt").is_none(),
        "delete-object must not return 'updatedAt', got: {body}"
    );
}

// ── get-missing-object 404 ─────────────────────────────────────────────────

/// GET /1/indexes/{index}/{objectID} with unknown ID must return 404 with
/// Algolia error envelope: { "message": "...", "status": 404 }.
#[tokio::test]
async fn get_missing_object_returns_json_404_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/nonexistent-xyz",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND, "expected 404: {body}");
    common::assert_error_envelope(&body, 404);
}

// ── batch operations ───────────────────────────────────────────────────────

/// POST /1/indexes/{index}/batch — mixed actions — must return { taskID, objectIDs }.
/// Algolia reference: batch() → { taskID, objectIDs }
#[tokio::test]
async fn batch_mixed_actions_return_task_id_and_object_ids() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "action": "addObject", "body": { "objectID": "b1", "v": 1 } },
                { "action": "addObject", "body": { "objectID": "b2", "v": 2 } },
                { "action": "updateObject", "body": { "objectID": "b1", "v": 10 } }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    assert!(
        body["taskID"].is_i64() || body["taskID"].is_u64(),
        "'taskID' must be an integer: {body}"
    );
    let object_ids = body["objectIDs"]
        .as_array()
        .unwrap_or_else(|| panic!("'objectIDs' must be an array: {body}"));
    assert_eq!(
        object_ids.len(),
        3,
        "'objectIDs' must contain 3 entries: {body}"
    );

    // Must NOT use snake_case variants
    assert!(
        body.get("task_id").is_none(),
        "must not have 'task_id': {body}"
    );
    assert!(
        body.get("object_ids").is_none(),
        "must not have 'object_ids': {body}"
    );
}

// ── multi-index retrieval ──────────────────────────────────────────────────

/// POST /1/indexes/*/objects — multi-get by objectID across indices.
/// Algolia reference: multipleGetObjects() → { results: [...] }
/// The path uses literal `*` as indexName; the handler reads indexName from
/// each request body entry rather than the path parameter.
#[tokio::test]
async fn multi_index_get_objects_returns_results_array() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed two different indices.
    let (s1_status, s1_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/alpha-idx/obj-a1",
        ADMIN_KEY,
        Some(json!({ "label": "Alpha" })),
    )
    .await;
    assert_eq!(s1_status, StatusCode::OK);

    let (s2_status, s2_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/beta-idx/obj-b1",
        ADMIN_KEY,
        Some(json!({ "label": "Beta" })),
    )
    .await;
    assert_eq!(s2_status, StatusCode::OK);

    common::wait_for_task_local(&app, common::extract_task_id(&s1_body)).await;
    common::wait_for_task_local(&app, common::extract_task_id(&s2_body)).await;

    // Multi-get via * path.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/objects",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "indexName": "alpha-idx", "objectID": "obj-a1" },
                { "indexName": "beta-idx",  "objectID": "obj-b1" },
                { "indexName": "alpha-idx", "objectID": "nonexistent" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let results = body["results"]
        .as_array()
        .unwrap_or_else(|| panic!("'results' must be an array: {body}"));
    assert_eq!(
        results.len(),
        3,
        "'results' length must equal request count: {body}"
    );

    // First two must be non-null objects with objectID
    assert!(
        results[0].is_object() && results[0]["objectID"] == "obj-a1",
        "results[0] must contain objectID 'obj-a1': {}",
        results[0]
    );
    assert!(
        results[1].is_object() && results[1]["objectID"] == "obj-b1",
        "results[1] must contain objectID 'obj-b1': {}",
        results[1]
    );
    // Missing object must be null
    assert!(
        results[2].is_null(),
        "results[2] for missing object must be null: {}",
        results[2]
    );
}

// ── task-status routes ─────────────────────────────────────────────────────

/// GET /1/tasks/{taskID} — global task route — must return
/// { "status": "published", "pendingTask": false } after task completes.
#[tokio::test]
async fn global_task_status_returns_published_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a task via a write.
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/task-test-idx/task-obj-1",
        ADMIN_KEY,
        Some(json!({ "val": 1 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local(&app, task_id).await;

    // Poll the global task route.
    let (status, task_body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/tasks/{}", task_id),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {task_body}");
    common::assert_published_task_shape(&task_body);
}

/// GET /1/task/{taskID} — singular global task route — must return
/// the exact Algolia waitTask payload after task completion.
#[tokio::test]
async fn global_task_singular_route_returns_published_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create a task from a batch write to verify compatibility with batch task IDs.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/task-singular-idx/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "action": "addObject", "body": { "objectID": "s1", "v": 1 } }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local(&app, task_id).await;

    let (status, task_body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/task/{}", task_id),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {task_body}");
    common::assert_published_task_shape(&task_body);
}

/// GET /1/indexes/{index}/task/{taskID} — index-scoped task route — same contract.
/// Algolia reference: waitTask() polls this route until status === 'published'.
#[tokio::test]
async fn index_task_status_returns_published_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/task-idx-scoped/scoped-obj-1",
        ADMIN_KEY,
        Some(json!({ "val": 42 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local(&app, task_id).await;

    let (status, task_body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/task-idx-scoped/task/{}", task_id),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {task_body}");
    common::assert_published_task_shape(&task_body);
}

/// GET /1/indexes/{index}/task/{taskID} — Algolia SDK sends a numeric taskID
/// in the URL path. The server must resolve it correctly.
#[tokio::test]
async fn index_task_status_resolves_numeric_task_id() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed and capture the numeric task ID.
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/numeric-task-idx/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "action": "addObject", "body": { "objectID": "n1", "x": 1 } }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = common::extract_task_id(&body);
    // task_id is already i64 from the response.
    common::wait_for_task_local(&app, task_id).await;

    let (status, task_body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/numeric-task-idx/task/{}", task_id),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {task_body}");
    common::assert_published_task_shape(&task_body);
}

/// GET /1/indexes/{index}/task/{taskID} must reject task IDs owned by a different index
/// with a canonical Algolia 404 error envelope (no extra fields).
#[tokio::test]
async fn index_task_status_rejects_task_id_from_other_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/task-source-idx/source-obj-1",
        ADMIN_KEY,
        Some(json!({ "v": 7 })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = common::extract_task_id(&body);
    common::wait_for_task_local(&app, task_id).await;

    let response = common::send_authed_response(
        &app,
        Method::GET,
        &format!("/1/indexes/task-other-idx/task/{}", task_id),
        ADMIN_KEY,
        "test",
        &[],
        None,
    )
    .await;
    common::assert_error_contract_from_oneshot(response, 404).await;
}
