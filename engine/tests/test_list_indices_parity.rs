use axum::{
    http::{Method, StatusCode},
    Router,
};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-list-indices";

async fn seed_index(app: &Router, index: &str, docs: Vec<serde_json::Value>) {
    common::seed_docs(app, index, ADMIN_KEY, docs).await;
}

async fn create_empty_index(app: &Router, index: &str) {
    let (status, _) = common::send_json(
        app,
        Method::POST,
        "/1/indexes",
        ADMIN_KEY,
        Some(json!({ "uid": index })),
    )
    .await;
    assert!(status.is_success(), "create index must succeed for {index}");
}

async fn list_indices(app: &Router, query: &str) -> serde_json::Value {
    let uri = if query.is_empty() {
        "/1/indexes".to_string()
    } else {
        format!("/1/indexes?{query}")
    };
    let (status, body) = common::send_json(app, Method::GET, &uri, ADMIN_KEY, None).await;
    assert_eq!(status, StatusCode::OK, "list_indices failed");
    body
}

#[tokio::test]
async fn test_list_indices_created_at_is_real_and_stable() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(&app, "ts_test", vec![json!({"objectID": "1", "x": 1})]).await;

    let body1 = list_indices(&app, "").await;
    let items1 = body1["items"].as_array().unwrap();
    let idx1 = items1.iter().find(|i| i["name"] == "ts_test").unwrap();
    let created1 = idx1["createdAt"].as_str().unwrap();

    assert_ne!(
        created1, "2024-01-01T00:00:00Z",
        "createdAt should not be hardcoded"
    );

    let parsed = chrono::DateTime::parse_from_rfc3339(created1);
    assert!(
        parsed.is_ok(),
        "createdAt should be valid RFC3339, got: {}",
        created1
    );

    let body2 = list_indices(&app, "").await;
    let items2 = body2["items"].as_array().unwrap();
    let idx2 = items2.iter().find(|i| i["name"] == "ts_test").unwrap();
    let created2 = idx2["createdAt"].as_str().unwrap();

    assert_eq!(
        created1, created2,
        "createdAt should be stable across repeated calls"
    );
}

#[tokio::test]
async fn test_list_indices_created_at_empty_for_empty_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    create_empty_index(&app, "empty_idx").await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "empty_idx").unwrap();

    assert_eq!(
        idx["createdAt"].as_str().unwrap(),
        "",
        "createdAt should be empty string for index with no records"
    );
}

#[tokio::test]
async fn test_list_indices_last_build_time_s() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(&app, "build_test", vec![json!({"objectID": "1", "x": 1})]).await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "build_test").unwrap();

    assert!(
        idx.get("lastBuildTimeS").is_some(),
        "lastBuildTimeS field should be present"
    );
    assert!(
        idx["lastBuildTimeS"].is_u64() || idx["lastBuildTimeS"].is_i64(),
        "lastBuildTimeS should be integer, got: {:?}",
        idx["lastBuildTimeS"]
    );
    idx["lastBuildTimeS"]
        .as_u64()
        .expect("lastBuildTimeS should be a non-negative integer");
}

#[tokio::test]
async fn test_list_indices_last_build_time_s_zero_for_empty() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    create_empty_index(&app, "never_built").await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "never_built").unwrap();

    assert_eq!(
        idx["lastBuildTimeS"].as_u64().unwrap(),
        0,
        "lastBuildTimeS should be 0 for never-built index"
    );
}

#[tokio::test]
async fn test_list_indices_no_replica_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(&app, "norepl", vec![json!({"objectID": "1", "x": 1})]).await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "norepl").unwrap();

    assert!(
        idx.get("primary").is_none(),
        "primary field should be absent"
    );
    assert!(
        idx.get("replicas").is_none(),
        "replicas field should be absent"
    );
    assert!(
        idx.get("virtual").is_none(),
        "virtual field should be absent"
    );
}

#[tokio::test]
async fn test_list_indices_unpaginated_returns_all() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(&app, "aaa", vec![json!({"objectID": "1"})]).await;
    seed_index(&app, "bbb", vec![json!({"objectID": "1"})]).await;
    seed_index(&app, "ccc", vec![json!({"objectID": "1"})]).await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();

    assert_eq!(items.len(), 3, "should return all 3 indices");
    assert_eq!(
        body["nbPages"].as_u64().unwrap(),
        1,
        "nbPages should be 1 when unpaginated"
    );
}

#[tokio::test]
async fn test_list_indices_paginated() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    for name in &["alpha", "bravo", "charlie", "delta", "echo"] {
        seed_index(&app, name, vec![json!({"objectID": "1"})]).await;
    }

    let body = list_indices(&app, "page=0&hitsPerPage=2").await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "page 0 should have 2 items");
    assert_eq!(
        body["nbPages"].as_u64().unwrap(),
        3,
        "5 items / 2 per page = 3 pages"
    );

    assert_eq!(items[0]["name"].as_str().unwrap(), "alpha");
    assert_eq!(items[1]["name"].as_str().unwrap(), "bravo");

    let body = list_indices(&app, "page=1&hitsPerPage=2").await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "page 1 should have 2 items");
    assert_eq!(items[0]["name"].as_str().unwrap(), "charlie");
    assert_eq!(items[1]["name"].as_str().unwrap(), "delta");

    let body = list_indices(&app, "page=2&hitsPerPage=2").await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "last page should have 1 item");
    assert_eq!(items[0]["name"].as_str().unwrap(), "echo");

    let body = list_indices(&app, "page=3&hitsPerPage=2").await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 0, "page beyond range should be empty");
}

#[tokio::test]
async fn test_list_indices_invalid_pagination() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes?page=0&hitsPerPage=0",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.get("message").is_some(),
        "error should have 'message' field"
    );
    assert_eq!(
        body["status"].as_u64().unwrap(),
        400,
        "error should have status: 400"
    );

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes?page=-1&hitsPerPage=10",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], json!(400));

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes?page=0&hitsPerPage=1001",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], json!(400));
}

#[tokio::test]
async fn test_list_indices_entries_reflects_committed_docs() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(
        &app,
        "entries_test",
        vec![
            json!({"objectID": "1", "x": 1}),
            json!({"objectID": "2", "x": 2}),
            json!({"objectID": "3", "x": 3}),
        ],
    )
    .await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "entries_test").unwrap();

    assert_eq!(
        idx["entries"].as_u64().unwrap(),
        3,
        "entries should reflect 3 committed documents"
    );
}

#[tokio::test]
async fn test_data_size_less_than_file_size() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(
        &app,
        "size_test",
        vec![
            json!({"objectID": "1", "title": "Hello World", "body": "This is a test document with some content for size measurement."}),
            json!({"objectID": "2", "title": "Foo Bar Baz", "body": "Another document with different content to ensure measurable store bytes."}),
            json!({"objectID": "3", "title": "Qux Quux", "body": "Yet another document for good measure, ensuring the store has data."}),
        ],
    )
    .await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();
    let idx = items.iter().find(|i| i["name"] == "size_test").unwrap();

    let data_size = idx["dataSize"].as_u64().expect("dataSize should be a u64");
    let file_size = idx["fileSize"].as_u64().expect("fileSize should be a u64");

    assert!(
        data_size > 0,
        "dataSize should be > 0 after indexing documents, got {}",
        data_size
    );
    assert!(
        file_size > 0,
        "fileSize should be > 0 after indexing documents, got {}",
        file_size
    );
    assert!(
        data_size < file_size,
        "dataSize (store data bytes) should be strictly less than fileSize (full dir), got dataSize={} fileSize={}",
        data_size, file_size
    );
}

fn find_index<'a>(items: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
    items
        .iter()
        .find(|i| i["name"] == name)
        .unwrap_or_else(|| panic!("index '{name}' not found in list response"))
}

/// Primary index exposes `replicas` list; standard replica exposes `primary`
/// and omits `virtual`; virtual replica exposes `primary` and `virtual: true`.
#[tokio::test]
async fn test_list_indices_replica_metadata_positive() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Create primary with records
    seed_index(&app, "primary_idx", vec![json!({"objectID": "1", "x": 1})]).await;

    // Configure replicas: one standard, one virtual
    common::put_settings_and_wait(
        &app,
        "primary_idx",
        ADMIN_KEY,
        json!({
            "replicas": ["std_replica", "virtual(virt_replica)"]
        }),
        false,
    )
    .await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();

    // Primary: should expose replicas, no primary, no virtual
    let primary = find_index(items, "primary_idx");
    let replicas = primary["replicas"]
        .as_array()
        .expect("primary should expose replicas array");
    assert!(
        replicas.iter().any(|r| r == "std_replica"),
        "replicas should include std_replica: {replicas:?}"
    );
    assert!(
        replicas.iter().any(|r| r == "virtual(virt_replica)"),
        "replicas should include virtual(virt_replica): {replicas:?}"
    );
    assert!(
        primary.get("primary").is_none(),
        "primary index should not have 'primary' field"
    );
    assert!(
        primary.get("virtual").is_none(),
        "primary index should not have 'virtual' field"
    );

    // Standard replica: should expose primary, no replicas, no virtual
    let std_repl = find_index(items, "std_replica");
    assert_eq!(
        std_repl["primary"].as_str().unwrap(),
        "primary_idx",
        "standard replica should expose primary"
    );
    assert!(
        std_repl.get("replicas").is_none(),
        "standard replica should not have replicas field"
    );
    assert!(
        std_repl.get("virtual").is_none(),
        "standard replica should not have virtual field"
    );

    // Virtual replica: should expose primary and virtual: true
    let virt_repl = find_index(items, "virt_replica");
    assert_eq!(
        virt_repl["primary"].as_str().unwrap(),
        "primary_idx",
        "virtual replica should expose primary"
    );
    assert!(
        virt_repl["virtual"].as_bool().unwrap(),
        "virtual replica should have virtual: true"
    );
    assert!(
        virt_repl.get("replicas").is_none(),
        "virtual replica should not have replicas field"
    );
}

/// After a copy operation, the destination index should appear in list indices
/// with correct entries count.
#[tokio::test]
async fn test_list_indices_coherent_after_copy() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(
        &app,
        "copy_src",
        vec![
            json!({"objectID": "1", "x": 1}),
            json!({"objectID": "2", "x": 2}),
        ],
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/copy_src/operation",
        ADMIN_KEY,
        Some(json!({"operation": "copy", "destination": "copy_dst"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    common::wait_for_task_local_with_key(&app, common::extract_task_id(&op_body), ADMIN_KEY).await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();

    // Both source and destination should appear
    let src = find_index(items, "copy_src");
    let dst = find_index(items, "copy_dst");

    assert_eq!(
        src["entries"].as_u64().unwrap(),
        2,
        "source should still have 2 entries"
    );
    assert_eq!(
        dst["entries"].as_u64().unwrap(),
        2,
        "destination should have 2 entries after copy"
    );
}

/// After a move operation, the source should be gone from list indices and the
/// destination should have the correct entries count.
#[tokio::test]
async fn test_list_indices_coherent_after_move() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    seed_index(
        &app,
        "move_src",
        vec![
            json!({"objectID": "1", "x": 1}),
            json!({"objectID": "2", "x": 2}),
        ],
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/move_src/operation",
        ADMIN_KEY,
        Some(json!({"operation": "move", "destination": "move_dst"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    common::wait_for_task_local_with_key(&app, common::extract_task_id(&op_body), ADMIN_KEY).await;

    let body = list_indices(&app, "").await;
    let items = body["items"].as_array().unwrap();

    assert!(
        items.iter().all(|i| i["name"] != "move_src"),
        "source should be absent from list after move"
    );

    let dst = find_index(items, "move_dst");
    assert_eq!(
        dst["entries"].as_u64().unwrap(),
        2,
        "destination should have 2 entries after move"
    );
}
