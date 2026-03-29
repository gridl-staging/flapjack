use axum::{
    body::Body,
    http::{Method, StatusCode},
};
use serde_json::json;

mod common;

async fn query_hits(
    app: &axum::Router,
    index_name: &str,
    query: &str,
) -> axum::http::Response<Body> {
    common::send_oneshot(
        app,
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        &[("content-type", "application/json")],
        Body::from(json!({ "query": query }).to_string()),
    )
    .await
}

#[tokio::test]
async fn invalid_snapshot_import_returns_500_json_and_preserves_existing_index_data() {
    let (app, tmp) = common::build_test_app_for_local_requests(None);
    let index_name = "snapshot-invalid-import-contract";
    common::seed_doc_local(&app, index_name).await;

    let before = query_hits(&app, index_name, "alpha").await;
    assert_eq!(before.status(), StatusCode::OK);
    let before_body = common::parse_response_json(before).await;
    assert_eq!(before_body["nbHits"], json!(1), "seed precondition failed");
    assert_eq!(before_body["hits"][0]["objectID"], json!("doc-1"));

    let import_resp = common::send_oneshot(
        &app,
        Method::POST,
        &format!("/1/indexes/{index_name}/import"),
        &[("content-type", "application/gzip")],
        Body::from("not-a-valid-snapshot".as_bytes().to_vec()),
    )
    .await;
    assert_eq!(import_resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let import_ct = import_resp
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        import_ct.contains("application/json"),
        "expected middleware-wrapped JSON error content-type, got: {import_ct}"
    );

    let import_body = common::parse_response_json(import_resp).await;
    assert_eq!(import_body["status"], json!(500));
    let import_message = import_body["message"]
        .as_str()
        .expect("expected string message on import failure");
    assert_eq!(import_message, "Internal server error");
    assert!(
        !import_message.contains("Import failed:"),
        "500 response must not leak internal prefix text: {import_message}"
    );
    assert!(
        !import_message.contains("not-a-valid-snapshot"),
        "500 response must not leak raw backend text: {import_message}"
    );

    let after = query_hits(&app, index_name, "alpha").await;
    assert_eq!(
        after.status(),
        StatusCode::OK,
        "invalid import must not make existing index unreadable"
    );
    let after_body = common::parse_response_json(after).await;
    assert_eq!(
        after_body["nbHits"],
        json!(1),
        "invalid import must not remove existing documents"
    );
    assert_eq!(after_body["hits"][0]["objectID"], json!("doc-1"));

    // Validate durability, not just in-memory visibility: a fresh app over the same
    // on-disk data directory must still see the original document after failed import.
    let restarted_app = common::build_test_app_for_existing_data_dir(tmp.path(), None);
    let after_restart = query_hits(&restarted_app, index_name, "alpha").await;
    assert_eq!(
        after_restart.status(),
        StatusCode::OK,
        "invalid import must not make index unreadable after restart"
    );
    let after_restart_body = common::parse_response_json(after_restart).await;
    assert_eq!(
        after_restart_body["nbHits"],
        json!(1),
        "invalid import must preserve existing on-disk documents"
    );
    assert_eq!(after_restart_body["hits"][0]["objectID"], json!("doc-1"));
}
