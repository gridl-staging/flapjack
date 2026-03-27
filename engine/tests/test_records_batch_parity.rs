use axum::http::{Method, StatusCode};
use serde_json::json;
#[allow(unused_imports)]
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

#[tokio::test]
async fn single_index_batch_clear_without_body_clears_records_and_returns_algolia_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let seed_req = common::authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "addObject", "body": {"objectID": "p1", "title": "Laptop"}},
                {"action": "addObject", "body": {"objectID": "p2", "title": "Mouse"}}
            ]
        })),
    );
    let resp = app.clone().oneshot(seed_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let seed_body = common::body_json(resp).await;
    common::wait_for_task_local(&app, common::extract_task_id(&seed_body)).await;

    let clear_req = common::authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "clear"}
            ]
        })),
    );
    let resp = app.clone().oneshot(clear_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let clear_body = common::body_json(resp).await;

    assert!(
        clear_body["taskID"].is_i64(),
        "expected numeric taskID: {clear_body}"
    );
    assert_eq!(clear_body["objectIDs"], json!([]));
    assert!(
        clear_body.get("task_id").is_none(),
        "snake_case task_id must not exist: {clear_body}"
    );
    assert!(
        clear_body.get("object_ids").is_none(),
        "snake_case object_ids must not exist: {clear_body}"
    );

    common::wait_for_task_local(&app, common::extract_task_id(&clear_body)).await;

    let (status, query_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/query",
        ADMIN_KEY,
        Some(json!({"query": "", "hitsPerPage": 10})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(query_body["nbHits"], json!(0));
    assert_eq!(query_body["hits"], json!([]));
}

#[tokio::test]
async fn single_index_batch_delete_alias_behaves_like_delete_object() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "keep", "title": "Keep Me"}),
            json!({"objectID": "delete-me", "title": "Delete Me"}),
        ],
    )
    .await;

    let delete_req = common::authed_request(
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "delete", "body": {"objectID": "delete-me"}}
            ]
        })),
    );
    let resp = app.clone().oneshot(delete_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let delete_body = common::body_json(resp).await;

    assert!(
        delete_body["taskID"].is_i64(),
        "expected numeric taskID: {delete_body}"
    );
    assert_eq!(delete_body["objectIDs"], json!(["delete-me"]));
    assert!(delete_body.get("task_id").is_none());
    assert!(delete_body.get("object_ids").is_none());

    common::wait_for_task_local(&app, common::extract_task_id(&delete_body)).await;

    let (deleted_status, _) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/delete-me",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(deleted_status, StatusCode::NOT_FOUND);

    let (kept_status, _) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/products/keep",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(kept_status, StatusCode::OK);
}

#[tokio::test]
async fn multi_index_batch_with_clear_returns_task_map_and_applies_clear() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "idx_a",
        ADMIN_KEY,
        vec![json!({"objectID": "a1", "title": "Alpha"})],
    )
    .await;
    common::seed_docs(
        &app,
        "idx_b",
        ADMIN_KEY,
        vec![json!({"objectID": "b1", "title": "Beta One"})],
    )
    .await;

    let multi_req = common::authed_request(
        Method::POST,
        "/1/indexes/*/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "clear", "indexName": "idx_a"},
                {"action": "addObject", "indexName": "idx_b", "body": {"objectID": "b2", "title": "Beta Two"}}
            ]
        })),
    );

    let resp = app.clone().oneshot(multi_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = common::body_json(resp).await;

    assert!(
        body["taskID"].is_object(),
        "multi-index taskID must be object: {body}"
    );
    assert!(
        body["taskID"]["idx_a"].is_i64(),
        "missing idx_a taskID: {body}"
    );
    assert!(
        body["taskID"]["idx_b"].is_i64(),
        "missing idx_b taskID: {body}"
    );
    assert_eq!(body["objectIDs"], json!(["b2"]));

    common::wait_for_task_local(&app, body["taskID"]["idx_a"].as_i64().unwrap()).await;
    common::wait_for_task_local(&app, body["taskID"]["idx_b"].as_i64().unwrap()).await;

    let (_, query_a_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/idx_a/query",
        ADMIN_KEY,
        Some(json!({"query": "", "hitsPerPage": 10})),
    )
    .await;
    assert_eq!(query_a_body["nbHits"], json!(0), "idx_a should be cleared");

    let (_, query_b_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/idx_b/query",
        ADMIN_KEY,
        Some(json!({"query": "", "hitsPerPage": 10})),
    )
    .await;
    assert_eq!(
        query_b_body["nbHits"],
        json!(2),
        "idx_b should retain b1 and add b2"
    );
}

#[tokio::test]
async fn single_index_add_object_without_body_returns_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, err) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "addObject"}
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        err["message"]
            .as_str()
            .unwrap_or("")
            .contains("Missing body in addObject"),
        "unexpected error payload: {err}"
    );
    assert_eq!(err["status"], json!(400));
}

/// Cross-index batch: deleteObject routed by indexName removes only the
/// targeted record while addObject on a different index creates its record.
#[tokio::test]
async fn multi_index_batch_delete_routes_by_index_name() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "del_a",
        ADMIN_KEY,
        vec![
            json!({"objectID": "a1", "title": "Keep"}),
            json!({"objectID": "a2", "title": "Remove"}),
        ],
    )
    .await;
    common::seed_docs(
        &app,
        "del_b",
        ADMIN_KEY,
        vec![json!({"objectID": "b1", "title": "Existing"})],
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/batch",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {"action": "deleteObject", "indexName": "del_a", "body": {"objectID": "a2"}},
                {"action": "addObject", "indexName": "del_b", "body": {"objectID": "b2", "title": "New"}}
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["taskID"].is_object(),
        "multi-index taskID must be object"
    );
    assert!(body["taskID"]["del_a"].is_i64());
    assert!(body["taskID"]["del_b"].is_i64());

    common::wait_for_task_local(&app, body["taskID"]["del_a"].as_i64().unwrap()).await;
    common::wait_for_task_local(&app, body["taskID"]["del_b"].as_i64().unwrap()).await;

    // a1 should still exist, a2 should be gone
    let (a1_status, _) =
        common::send_json(&app, Method::GET, "/1/indexes/del_a/a1", ADMIN_KEY, None).await;
    assert_eq!(a1_status, StatusCode::OK, "a1 should still exist");

    let (a2_status, _) =
        common::send_json(&app, Method::GET, "/1/indexes/del_a/a2", ADMIN_KEY, None).await;
    assert_eq!(a2_status, StatusCode::NOT_FOUND, "a2 should be deleted");

    // b1 and b2 should both exist
    let (_, query_b) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/del_b/query",
        ADMIN_KEY,
        Some(json!({"query": "", "hitsPerPage": 10})),
    )
    .await;
    assert_eq!(query_b["nbHits"], json!(2), "del_b should have b1 + b2");
}
