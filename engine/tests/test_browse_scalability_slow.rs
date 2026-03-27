use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-browse-slow";

/// Read slow browse doc count from env with default 100000.
fn browse_docs_slow_count() -> usize {
    common::env_usize_or_default("BROWSE_SLOW_DOC_COUNT", 100_000)
}

#[tokio::test]
#[ignore = "slow test - run manually with cargo test -p flapjack --test test_browse_scalability_slow -- --ignored"]
async fn browse_scalability_slow_returns_all_docs_exactly_once() {
    const HITS_PER_PAGE: usize = 1_000;
    let doc_count = browse_docs_slow_count();

    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs_bulk(&app, "scalability_slow", ADMIN_KEY, doc_count).await;

    let (status, first_page) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/scalability_slow/browse",
        ADMIN_KEY,
        Some(json!({"hitsPerPage": HITS_PER_PAGE})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_page["nbHits"], json!(doc_count));
    assert_eq!(first_page["hitsPerPage"], json!(HITS_PER_PAGE));

    let (visited, last_body, page_count) =
        common::browse_all_cursor_pages(&app, "scalability_slow", ADMIN_KEY, HITS_PER_PAGE).await;

    common::assert_browse_exactly_once_invariants(
        &visited,
        &last_body,
        page_count,
        doc_count,
        HITS_PER_PAGE,
    );
}

#[tokio::test]
async fn bug5_created_at_shapes_for_key_endpoints() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (create_status, create_body) = common::send_json(
        &app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "BUG-5 createdAt verification",
            "validity": 3600
        })),
    )
    .await;
    assert_eq!(create_status, StatusCode::OK);

    let key_value = create_body["key"]
        .as_str()
        .expect("POST /1/keys should return key");
    let post_created_at = create_body["createdAt"]
        .as_str()
        .expect("POST /1/keys createdAt should be string");
    common::assert_iso8601(post_created_at, "createdAt");

    let (get_status, get_body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/keys/{key_value}"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);

    common::assert_integer_value(&get_body["createdAt"], "createdAt");

    let (list_status, list_body) =
        common::send_json(&app, Method::GET, "/1/keys", ADMIN_KEY, None).await;
    assert_eq!(list_status, StatusCode::OK);

    let keys = list_body["keys"]
        .as_array()
        .expect("GET /1/keys should return keys array");
    let created_key = keys
        .iter()
        .find(|k| k["value"].as_str() == Some(key_value))
        .expect("created key should be present in list response");

    common::assert_integer_value(&created_key["createdAt"], "createdAt");
}
