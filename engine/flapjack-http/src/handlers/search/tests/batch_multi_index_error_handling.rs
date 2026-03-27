//! Batch-search invalid-input regressions kept separate so the main multi-index file stays below the hard size limit.
use super::*;

type SeedDoc = Vec<(&'static str, &'static str)>;
type SeedDocs = Vec<SeedDoc>;

async fn batch_validation_error_response(
    request_body: Value,
    seeded_index: Option<(&str, SeedDocs)>,
) -> Value {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    if let Some((index_name, docs)) = seeded_index {
        create_index_with_docs(&state, index_name, docs).await;
    }
    let app = batch_router(state);
    let response = post_batch_search(&app, request_body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    body_json(response).await
}

fn assert_standard_batch_validation_error(body: &Value) {
    assert!(
        body["message"].as_str().is_some(),
        "error response must include message field"
    );
    assert_eq!(body["status"], json!(400));
    assert!(
        body.get("results").is_none(),
        "invalid batch must reject whole batch without partial results"
    );
}

/// Verify that an unknown `type` value in a request returns a 400 error with an error message.
#[tokio::test]
async fn batch_search_unknown_type_returns_error() {
    let body = batch_validation_error_response(
        json!({
            "requests": [
                { "indexName": "type_err_idx", "query": "test", "type": "unknown_type" }
            ]
        }),
        Some(("type_err_idx", vec![vec![("title", "test item")]])),
    )
    .await;
    assert_standard_batch_validation_error(&body);
}

/// Verify that an invalid `strategy` value returns a 400 error with an error message.
#[tokio::test]
async fn batch_search_invalid_strategy_returns_error() {
    let body = batch_validation_error_response(
        json!({
            "requests": [
                { "indexName": "strat_err_idx", "query": "test" }
            ],
            "strategy": "invalidStrategy"
        }),
        Some(("strat_err_idx", vec![vec![("title", "test item")]])),
    )
    .await;
    assert_standard_batch_validation_error(&body);
}

/// Verify that a bad `type` in any request rejects the whole batch before any query executes.
/// The first request intentionally targets a missing index; if execution started before
/// upfront validation, this would fail with an index-not-found style error instead.
#[tokio::test]
async fn batch_search_unknown_type_rejects_batch_before_execution() {
    let body = batch_validation_error_response(
        json!({
            "requests": [
                { "indexName": "missing_idx", "query": "would-fail-if-executed" },
                { "indexName": "missing_idx", "query": "ignored", "type": "unknown_type" }
            ]
        }),
        None,
    )
    .await;
    assert_eq!(body["status"], json!(400));
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("Invalid query type"),
        "expected upfront query-type validation error, got: {body}"
    );
    assert!(
        !message.contains("not found"),
        "batch should fail on invalid type before any index execution path: {body}"
    );
}
