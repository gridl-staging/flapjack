/// Stage 3 SDK contract tests: Rules and Synonyms wire-format parity.
///
/// Each test asserts Algolia-exact response shapes for the rules and synonyms
/// endpoints used by the Algolia JavaScript/Rust/Python SDKs. Tests are written
/// red-first; any handler response-shape gap is fixed in the handler, not here.
///
/// Algolia API reference: https://www.algolia.com/doc/api-reference/api-methods/
use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

async fn configure_primary_replica_pair(
    app: &axum::Router,
    primary_index: &str,
    replica_index: &str,
) {
    // Ensure both index directories exist before forwarding writes.
    common::seed_docs(
        app,
        primary_index,
        ADMIN_KEY,
        vec![json!({ "objectID": "p1" })],
    )
    .await;
    common::seed_docs(
        app,
        replica_index,
        ADMIN_KEY,
        vec![json!({ "objectID": "r1" })],
    )
    .await;

    common::put_settings_and_wait(
        app,
        primary_index,
        ADMIN_KEY,
        json!({ "replicas": [replica_index] }),
        false,
    )
    .await;
}

// ── Rules: Save single rule ─────────────────────────────────────────────────

/// PUT /1/indexes/{index}/rules/{objectID} — save rule — must return { taskID, updatedAt, id }.
#[tokio::test]
async fn save_rule_returns_algolia_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x", "v": 1 })],
    )
    .await;

    // Save a rule
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/rules-test/rules/my-rule",
        ADMIN_KEY,
        Some(json!({
            "objectID": "my-rule",
            "conditions": [{ "pattern": "query", "context": "search" }],
            "consequence": { "params": { "query": "alt" } },
            "enabled": true,
            "description": "Test rule"
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID + updatedAt (uses shared helper for DRY + snake_case rejection)
    common::assert_write_task_envelope(&body, "updatedAt");

    // Must have id (the rule objectID)
    assert!(body.get("id").is_some(), "must have id: {body}");
}

// ── Rules: Get rule ─────────────────────────────────────────────────────────

/// GET /1/indexes/{index}/rules/{objectID} — get rule — must return rule object.
#[tokio::test]
async fn get_rule_returns_rule_object() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-get-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // First save a rule
    let (_save_status, _save_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/rules-get-test/rules/rule-1",
        ADMIN_KEY,
        Some(json!({
            "objectID": "rule-1",
            "conditions": [{ "pattern": "test" }],
            "enabled": true,
            "consequence": { "params": {} }
        })),
    )
    .await;

    // Get the rule
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/rules-get-test/rules/rule-1",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have objectID
    assert_eq!(
        body["objectID"].as_str(),
        Some("rule-1"),
        "objectID must match: {body}"
    );

    // Must have enabled
    assert!(body.get("enabled").is_some(), "must have enabled: {body}");
}

// ── Rules: Get missing rule 404 ─────────────────────────────────────────────

/// GET /1/indexes/{index}/rules/{objectID} for non-existent rule must return 404.
#[tokio::test]
async fn get_missing_rule_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-404-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Try to get non-existent rule
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/rules-404-test/rules/does-not-exist",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND, "expected 404: {body}");
    common::assert_error_envelope(&body, 404);
}

// ── Rules: Delete rule ───────────────────────────────────────────────────────

/// DELETE /1/indexes/{index}/rules/{objectID} — delete rule — must return { taskID, deletedAt }.
#[tokio::test]
async fn delete_rule_returns_task_id_and_deleted_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-del-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a rule first
    common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/rules-del-test/rules/to-delete",
        ADMIN_KEY,
        Some(json!({ "objectID": "to-delete", "enabled": true, "consequence": { "params": {} } })),
    )
    .await;

    // Delete the rule
    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        "/1/indexes/rules-del-test/rules/to-delete",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID + deletedAt (uses shared helper for DRY + snake_case rejection)
    common::assert_write_task_envelope(&body, "deletedAt");
}

// ── Rules: Batch save rules ─────────────────────────────────────────────────

/// POST /1/indexes/{index}/rules/batch — batch save rules — must return { taskID, updatedAt }.
#[tokio::test]
async fn batch_save_rules_returns_task_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-batch-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Batch save rules
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-batch-test/rules/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "batch-1", "enabled": true, "consequence": { "params": {} } },
            { "objectID": "batch-2", "enabled": false, "consequence": { "params": {} } }
        ])),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID and updatedAt
    common::assert_write_task_envelope(&body, "updatedAt");
}

// ── Rules: Clear rules ───────────────────────────────────────────────────────

/// POST /1/indexes/{index}/rules/clear — clear all rules — must return { taskID, updatedAt }.
#[tokio::test]
async fn clear_rules_returns_task_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-clear-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a rule first
    common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/rules-clear-test/rules/rule-to-clear",
        ADMIN_KEY,
        Some(json!({ "objectID": "rule-to-clear", "consequence": { "params": {} } })),
    )
    .await;

    // Clear all rules
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-clear-test/rules/clear",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID and updatedAt
    common::assert_write_task_envelope(&body, "updatedAt");
}

// ── Rules: Search rules ─────────────────────────────────────────────────────

/// POST /1/indexes/{index}/rules/search — search rules — must return { hits, nbHits, page, nbPages }.
#[tokio::test]
async fn search_rules_returns_search_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "rules-search-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save some rules via batch endpoint
    let (save_status, save_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-search-test/rules/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "search-rule-1", "description": "First rule", "consequence": { "params": {} } },
            { "objectID": "search-rule-2", "description": "Second rule", "consequence": { "params": {} } }
        ])),
    )
    .await;
    assert_eq!(
        save_status,
        StatusCode::OK,
        "rule batch save failed: {save_body}"
    );

    // Search rules
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-search-test/rules/search",
        ADMIN_KEY,
        Some(json!({ "query": "rule" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have search response shape
    assert!(body["hits"].is_array(), "hits must be an array: {body}");
    assert!(body["nbHits"].is_u64(), "nbHits must be an integer: {body}");
    assert!(
        body["page"].is_u64() || body["page"].is_i64(),
        "page must be an integer: {body}"
    );
    assert!(
        body["nbPages"].is_u64() || body["nbPages"].is_i64(),
        "nbPages must be an integer: {body}"
    );
}

/// POST /1/indexes/{index}/rules/search with hitsPerPage=0 must not panic.
/// Contract behavior here is stable/explicit for flapjack: return 200 with nbPages=0.
#[tokio::test]
async fn search_rules_hits_per_page_zero_returns_zero_pages() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "rules-zero-hpp-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    let (save_status, save_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-zero-hpp-test/rules/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "search-rule-1", "description": "First rule", "consequence": { "params": {} } }
        ])),
    )
    .await;
    assert_eq!(
        save_status,
        StatusCode::OK,
        "rule batch save failed: {save_body}"
    );

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-zero-hpp-test/rules/search",
        ADMIN_KEY,
        Some(json!({ "query": "rule", "hitsPerPage": 0 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(
        body["page"].as_u64(),
        Some(0),
        "page should default to 0: {body}"
    );
    assert_eq!(
        body["nbPages"].as_u64(),
        Some(0),
        "nbPages must be 0 when hitsPerPage=0: {body}"
    );
}

/// Rules save endpoint must reject invalid index names at the HTTP boundary.
#[tokio::test]
async fn save_rule_rejects_invalid_index_name_with_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/bad..index/rules/rule-1",
        ADMIN_KEY,
        Some(json!({
            "objectID": "rule-1",
            "consequence": { "params": {} }
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "expected 400 for invalid index: {body}"
    );
    common::assert_error_envelope(&body, 400);
}

/// Corrupt rule storage must return sanitized 500 message (no internal details leakage).
#[tokio::test]
async fn search_rules_corrupt_store_returns_sanitized_500() {
    let (app, tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_dir = tmp.path().join("rules-corrupt-store");
    std::fs::create_dir_all(&index_dir).expect("create index dir");
    std::fs::write(index_dir.join("rules.json"), "{ not-valid-json").expect("write corrupt rules");

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/rules-corrupt-store/rules/search",
        ADMIN_KEY,
        Some(json!({ "query": "x" })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "expected 500: {body}"
    );
    common::assert_error_envelope(&body, 500);
    assert_eq!(
        body["message"],
        json!("Internal server error"),
        "error must be sanitized: {body}"
    );
}

// ── Synonyms: Save synonym ─────────────────────────────────────────────────

/// PUT /1/indexes/{index}/synonyms/{objectID} — save synonym — must return { taskID, updatedAt, id }.
#[tokio::test]
async fn save_synonym_returns_algolia_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a synonym
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-test/synonyms/my-synonym",
        ADMIN_KEY,
        Some(json!({
            "objectID": "my-synonym",
            "type": "synonym",
            "synonyms": ["phone", "telephone", "mobile"]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID + updatedAt (uses shared helper for DRY + snake_case rejection)
    common::assert_write_task_envelope(&body, "updatedAt");

    // Must have id
    assert!(body.get("id").is_some(), "must have id: {body}");
}

// ── Synonyms: Get synonym ───────────────────────────────────────────────────

/// GET /1/indexes/{index}/synonyms/{objectID} — get synonym — must return synonym object.
#[tokio::test]
async fn get_synonym_returns_synonym_object() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-get-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a synonym first
    common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-get-test/synonyms/syn-1",
        ADMIN_KEY,
        Some(json!({
            "objectID": "syn-1",
            "type": "synonym",
            "synonyms": ["test", "exam"]
        })),
    )
    .await;

    // Get the synonym
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/synonyms-get-test/synonyms/syn-1",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have objectID
    assert_eq!(
        body["objectID"].as_str(),
        Some("syn-1"),
        "objectID must match: {body}"
    );

    // Must have type
    assert!(body.get("type").is_some(), "must have type: {body}");
}

// ── Synonyms: Get missing synonym 404 ─────────────────────────────────────

/// GET /1/indexes/{index}/synonyms/{objectID} for non-existent synonym must return 404.
#[tokio::test]
async fn get_missing_synonym_returns_404() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-404-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Try to get non-existent synonym
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/synonyms-404-test/synonyms/does-not-exist",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND, "expected 404: {body}");
    common::assert_error_envelope(&body, 404);
}

// ── Synonyms: Delete synonym ───────────────────────────────────────────────

/// DELETE /1/indexes/{index}/synonyms/{objectID} — delete synonym — must return { taskID, deletedAt }.
#[tokio::test]
async fn delete_synonym_returns_task_id_and_deleted_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-del-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a synonym first
    let (save_status, save_body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-del-test/synonyms/to-delete",
        ADMIN_KEY,
        Some(json!({ "objectID": "to-delete", "type": "synonym", "synonyms": ["a", "b"] })),
    )
    .await;
    assert_eq!(
        save_status,
        StatusCode::OK,
        "synonym save failed: {save_body}"
    );
    common::wait_for_task_local(&app, common::extract_task_id(&save_body)).await;

    // Delete the synonym
    let (status, body) = common::send_json(
        &app,
        Method::DELETE,
        "/1/indexes/synonyms-del-test/synonyms/to-delete",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID + deletedAt (uses shared helper for DRY + snake_case rejection)
    common::assert_write_task_envelope(&body, "deletedAt");
}

// ── Synonyms: Batch save synonyms ─────────────────────────────────────────

/// POST /1/indexes/{index}/synonyms/batch — batch save synonyms — must return { taskID, updatedAt }.
#[tokio::test]
async fn batch_save_synonyms_returns_task_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-batch-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Batch save synonyms
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-batch-test/synonyms/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "batch-syn-1", "type": "synonym", "synonyms": ["a", "b"] },
            { "objectID": "batch-syn-2", "type": "onewaysynonym", "input": "c", "synonyms": ["d"] }
        ])),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID and updatedAt
    common::assert_write_task_envelope(&body, "updatedAt");
}

// ── Synonyms: Clear synonyms ───────────────────────────────────────────────

/// POST /1/indexes/{index}/synonyms/clear — clear all synonyms — must return { taskID, updatedAt }.
#[tokio::test]
async fn clear_synonyms_returns_task_envelope() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-clear-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save a synonym first
    common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-clear-test/synonyms/to-clear",
        ADMIN_KEY,
        Some(json!({ "objectID": "to-clear", "synonyms": ["x", "y"] })),
    )
    .await;

    // Clear all synonyms
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-clear-test/synonyms/clear",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have taskID and updatedAt
    common::assert_write_task_envelope(&body, "updatedAt");
}

// ── Synonyms: Search synonyms ─────────────────────────────────────────────

/// POST /1/indexes/{index}/synonyms/search — search synonyms — must return { hits, nbHits, page, nbPages }.
/// Handler fixed in review to include page/nbPages (was missing, unlike search_rules handler).
#[tokio::test]
async fn search_synonyms_returns_search_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-search-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Save some synonyms
    common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-search-test/synonyms/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "search-syn-1", "synonyms": ["hello", "hi"] },
            { "objectID": "search-syn-2", "synonyms": ["bye", "goodbye"] }
        ])),
    )
    .await;

    // Search synonyms
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-search-test/synonyms/search",
        ADMIN_KEY,
        Some(json!({ "query": "hello" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Must have search response shape
    assert!(body["hits"].is_array(), "hits must be an array: {body}");
    assert!(body["nbHits"].is_u64(), "nbHits must be an integer: {body}");
    assert!(
        body["page"].is_u64() || body["page"].is_i64(),
        "page must be an integer: {body}"
    );
    assert!(
        body["nbPages"].is_u64() || body["nbPages"].is_i64(),
        "nbPages must be an integer: {body}"
    );
}

/// POST /1/indexes/{index}/synonyms/search with hitsPerPage=0 must not panic.
/// Contract behavior here is stable/explicit for flapjack: return 200 with nbPages=0.
#[tokio::test]
async fn search_synonyms_hits_per_page_zero_returns_zero_pages() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "synonyms-zero-hpp-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    let (save_status, save_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-zero-hpp-test/synonyms/batch",
        ADMIN_KEY,
        Some(json!([
            { "objectID": "search-syn-1", "type": "synonym", "synonyms": ["hello", "hi"] }
        ])),
    )
    .await;
    assert_eq!(
        save_status,
        StatusCode::OK,
        "synonym batch save failed: {save_body}"
    );

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-zero-hpp-test/synonyms/search",
        ADMIN_KEY,
        Some(json!({ "query": "hello", "hitsPerPage": 0 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(
        body["page"].as_u64(),
        Some(0),
        "page should default to 0: {body}"
    );
    assert_eq!(
        body["nbPages"].as_u64(),
        Some(0),
        "nbPages must be 0 when hitsPerPage=0: {body}"
    );
}

/// Synonyms save endpoint must reject invalid index names at the HTTP boundary.
#[tokio::test]
async fn save_synonym_rejects_invalid_index_name_with_400() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/bad..index/synonyms/syn-1",
        ADMIN_KEY,
        Some(json!({
            "objectID": "syn-1",
            "type": "synonym",
            "synonyms": ["a", "b"]
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "expected 400 for invalid index: {body}"
    );
    common::assert_error_envelope(&body, 400);
}

/// Corrupt synonym storage must return sanitized 500 message (no internal details leakage).
#[tokio::test]
async fn search_synonyms_corrupt_store_returns_sanitized_500() {
    let (app, tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let index_dir = tmp.path().join("synonyms-corrupt-store");
    std::fs::create_dir_all(&index_dir).expect("create index dir");
    std::fs::write(index_dir.join("synonyms.json"), "{ not-valid-json")
        .expect("write corrupt synonyms");

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/synonyms-corrupt-store/synonyms/search",
        ADMIN_KEY,
        Some(json!({ "query": "x" })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "expected 500: {body}"
    );
    common::assert_error_envelope(&body, 500);
    assert_eq!(
        body["message"],
        json!("Internal server error"),
        "error must be sanitized: {body}"
    );
}

// ── Synonym types coverage ─────────────────────────────────────────────────

/// Save synonym with different types: synonym, oneWaySynonym, altCorrection1, altCorrection2, placeholder.
#[tokio::test]
async fn save_synonym_various_types() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data so index exists
    common::seed_docs(
        &app,
        "synonyms-types-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "x" })],
    )
    .await;

    // Test synonym type
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-types-test/synonyms/type-synonym",
        ADMIN_KEY,
        Some(json!({ "objectID": "type-synonym", "type": "synonym", "synonyms": ["a", "b"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "synonym type failed: {body}");

    // Test oneWaySynonym type (lowercase in API)
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-types-test/synonyms/type-oneway",
        ADMIN_KEY,
        Some(json!({ "objectID": "type-oneway", "type": "onewaysynonym", "input": "c", "synonyms": ["d"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "oneWaySynonym type failed: {body}");

    // Test altCorrection1 type (lowercase)
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-types-test/synonyms/type-alt1",
        ADMIN_KEY,
        Some(json!({ "objectID": "type-alt1", "type": "altcorrection1", "word": "test", "corrections": ["alt1"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "altCorrection1 type failed: {body}");

    // Test altCorrection2 type (lowercase)
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-types-test/synonyms/type-alt2",
        ADMIN_KEY,
        Some(json!({ "objectID": "type-alt2", "type": "altcorrection2", "word": "test", "corrections": ["alt2"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "altCorrection2 type failed: {body}");

    // Test placeholder type (lowercase)
    let (status, body) = common::send_json(
        &app,
        Method::PUT,
        "/1/indexes/synonyms-types-test/synonyms/type-placeholder",
        ADMIN_KEY,
        Some(json!({ "objectID": "type-placeholder", "type": "placeholder", "placeholder": "test", "replacements": ["x"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "placeholder type failed: {body}");
}

// ── forwardToReplicas contract coverage ─────────────────────────────────────

/// PUT /1/indexes/{index}/settings?forwardToReplicas=true propagates settings to replica.
#[tokio::test]
async fn forward_to_replicas_settings_propagates_to_replica_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let primary = "forward-settings-primary";
    let replica = "forward-settings-replica";

    configure_primary_replica_pair(&app, primary, replica).await;

    common::put_settings_and_wait(
        &app,
        primary,
        ADMIN_KEY,
        json!({ "searchableAttributes": ["name", "description"] }),
        true,
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/{replica}/settings"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(
        body["searchableAttributes"],
        json!(["name", "description"]),
        "replica searchableAttributes must be updated when forwardToReplicas=true: {body}"
    );
}

/// POST /1/indexes/{index}/synonyms/batch?forwardToReplicas=true propagates synonyms.
#[tokio::test]
async fn forward_to_replicas_synonyms_batch_propagates_to_replica_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let primary = "forward-synonyms-primary";
    let replica = "forward-synonyms-replica";

    configure_primary_replica_pair(&app, primary, replica).await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/indexes/{primary}/synonyms/batch?forwardToReplicas=true"),
        ADMIN_KEY,
        Some(json!([
            { "objectID": "syn-forward", "type": "synonym", "synonyms": ["phone", "mobile"] }
        ])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    common::assert_write_task_envelope(&body, "updatedAt");
    common::wait_for_task_local(&app, common::extract_task_id(&body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/{replica}/synonyms/syn-forward"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "replica synonym fetch failed: {body}"
    );
    assert_eq!(
        body["objectID"].as_str(),
        Some("syn-forward"),
        "objectID must match: {body}"
    );
    assert_eq!(
        body["type"].as_str(),
        Some("synonym"),
        "type must match: {body}"
    );
}

/// POST /1/indexes/{index}/rules/batch?forwardToReplicas=true propagates rules.
#[tokio::test]
async fn forward_to_replicas_rules_batch_propagates_to_replica_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
    let primary = "forward-rules-primary";
    let replica = "forward-rules-replica";

    configure_primary_replica_pair(&app, primary, replica).await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        &format!("/1/indexes/{primary}/rules/batch?forwardToReplicas=true"),
        ADMIN_KEY,
        Some(json!([
            {
                "objectID": "rule-forward",
                "conditions": [{ "pattern": "phone", "anchoring": "contains" }],
                "consequence": { "params": { "query": "smartphone" } }
            }
        ])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    common::assert_write_task_envelope(&body, "updatedAt");
    common::wait_for_task_local(&app, common::extract_task_id(&body)).await;

    let (status, body) = common::send_json(
        &app,
        Method::GET,
        &format!("/1/indexes/{replica}/rules/rule-forward"),
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "replica rule fetch failed: {body}");
    assert_eq!(
        body["objectID"].as_str(),
        Some("rule-forward"),
        "objectID must match: {body}"
    );
}
