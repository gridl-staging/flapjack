use axum::{
    http::{Method, StatusCode},
    Router,
};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

async fn assert_operation_response_and_wait(app: &Router, body: &serde_json::Value) {
    let task_id = body["taskID"]
        .as_i64()
        .expect("operation response must include numeric taskID");
    let updated_at = body["updatedAt"]
        .as_str()
        .expect("operation response must include updatedAt string");
    chrono::DateTime::parse_from_rfc3339(updated_at).expect("operation updatedAt must be RFC3339");
    common::wait_for_task_local(app, task_id).await;
}

async fn put_rule(app: &Router, index_name: &str, rule_id: &str, payload: serde_json::Value) {
    let (status, body) = common::send_json(
        app,
        Method::PUT,
        &format!("/1/indexes/{index_name}/rules/{rule_id}"),
        ADMIN_KEY,
        Some(payload),
    )
    .await;
    assert!(
        status.is_success(),
        "PUT rule failed for {index_name}/{rule_id}: status={status}, body={body}"
    );
    common::wait_for_task_local(app, common::extract_task_id(&body)).await;
}

async fn put_synonym(app: &Router, index_name: &str, synonym_id: &str, payload: serde_json::Value) {
    let (status, body) = common::send_json(
        app,
        Method::PUT,
        &format!("/1/indexes/{index_name}/synonyms/{synonym_id}"),
        ADMIN_KEY,
        Some(payload),
    )
    .await;
    assert!(
        status.is_success(),
        "PUT synonym failed for {index_name}/{synonym_id}: status={status}, body={body}"
    );
    common::wait_for_task_local(app, common::extract_task_id(&body)).await;
}

async fn query_index(app: &Router, index_name: &str) -> (StatusCode, serde_json::Value) {
    common::send_json(
        app,
        Method::POST,
        &format!("/1/indexes/{index_name}/query"),
        ADMIN_KEY,
        Some(json!({"query": "", "hitsPerPage": 20})),
    )
    .await
}

#[tokio::test]
async fn copy_without_scope_copies_records_and_operation_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_copy_all",
        ADMIN_KEY,
        vec![
            json!({"objectID": "p1", "title": "Laptop"}),
            json!({"objectID": "p2", "title": "Mouse"}),
        ],
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_copy_all/operation",
        ADMIN_KEY,
        Some(json!({"operation": "copy", "destination": "dst_copy_all"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (dst_status, dst_query) = query_index(&app, "dst_copy_all").await;
    assert_eq!(dst_status, StatusCode::OK);
    assert_eq!(dst_query["nbHits"], json!(2));

    let (get_status, get_body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_copy_all/p1",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body["objectID"], json!("p1"));
}

#[tokio::test]
async fn copy_scope_settings_copies_settings_only_and_not_records() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_scope_settings",
        ADMIN_KEY,
        vec![json!({"objectID": "p1", "title": "Seed"})],
    )
    .await;
    common::put_settings_and_wait(
        &app,
        "src_scope_settings",
        ADMIN_KEY,
        json!({
            "searchableAttributes": ["title", "description"],
            "attributesForFaceting": ["category"]
        }),
        false,
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_scope_settings/operation",
        ADMIN_KEY,
        Some(json!({
            "operation": "copy",
            "destination": "dst_scope_settings",
            "scope": ["settings"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (dst_query_status, dst_query) = query_index(&app, "dst_scope_settings").await;
    assert_eq!(dst_query_status, StatusCode::OK);
    assert_eq!(dst_query["nbHits"], json!(0));

    let (settings_status, dst_settings) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_scope_settings/settings",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert_eq!(
        dst_settings["searchableAttributes"],
        json!(["title", "description"])
    );
    assert_eq!(dst_settings["attributesForFaceting"], json!(["category"]));
}

#[tokio::test]
async fn copy_scope_rules_copies_rules_only_with_default_destination_settings() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_scope_rules",
        ADMIN_KEY,
        vec![json!({"objectID": "p1", "title": "Seed"})],
    )
    .await;
    common::put_settings_and_wait(
        &app,
        "src_scope_rules",
        ADMIN_KEY,
        json!({"searchableAttributes": ["title"]}),
        false,
    )
    .await;
    put_rule(
        &app,
        "src_scope_rules",
        "rule-copy",
        json!({
            "objectID": "rule-copy",
            "conditions": [{"anchoring": "contains", "pattern": "laptop"}],
            "consequence": {"params": {"query": "laptop computer"}}
        }),
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_scope_rules/operation",
        ADMIN_KEY,
        Some(json!({
            "operation": "copy",
            "destination": "dst_scope_rules",
            "scope": ["rules"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (dst_query_status, dst_query) = query_index(&app, "dst_scope_rules").await;
    assert_eq!(dst_query_status, StatusCode::OK);
    assert_eq!(dst_query["nbHits"], json!(0));

    let (rule_status, rule_body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_scope_rules/rules/rule-copy",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(rule_status, StatusCode::OK);
    assert_eq!(rule_body["objectID"], json!("rule-copy"));

    let (settings_status, dst_settings) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_scope_rules/settings",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert_ne!(
        dst_settings["searchableAttributes"],
        json!(["title"]),
        "rules-only copy must not copy source settings"
    );
}

#[tokio::test]
async fn copy_scope_synonyms_copies_synonyms_only() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_scope_synonyms",
        ADMIN_KEY,
        vec![json!({"objectID": "p1", "title": "Seed"})],
    )
    .await;
    common::put_settings_and_wait(
        &app,
        "src_scope_synonyms",
        ADMIN_KEY,
        json!({"searchableAttributes": ["title"]}),
        false,
    )
    .await;
    put_synonym(
        &app,
        "src_scope_synonyms",
        "syn-copy",
        json!({
            "objectID": "syn-copy",
            "type": "synonym",
            "synonyms": ["tv", "television"]
        }),
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_scope_synonyms/operation",
        ADMIN_KEY,
        Some(json!({
            "operation": "copy",
            "destination": "dst_scope_synonyms",
            "scope": ["synonyms"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (dst_query_status, dst_query) = query_index(&app, "dst_scope_synonyms").await;
    assert_eq!(dst_query_status, StatusCode::OK);
    assert_eq!(dst_query["nbHits"], json!(0));

    let (syn_status, syn_body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_scope_synonyms/synonyms/syn-copy",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(syn_status, StatusCode::OK);
    assert_eq!(syn_body["objectID"], json!("syn-copy"));

    let (settings_status, dst_settings) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_scope_synonyms/settings",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert_ne!(
        dst_settings["searchableAttributes"],
        json!(["title"]),
        "synonyms-only copy must not copy source settings"
    );
}

#[tokio::test]
async fn move_operation_moves_records_and_deletes_source_index() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_move",
        ADMIN_KEY,
        vec![
            json!({"objectID": "m1", "title": "Alpha"}),
            json!({"objectID": "m2", "title": "Beta"}),
        ],
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_move/operation",
        ADMIN_KEY,
        Some(json!({"operation": "move", "destination": "dst_move"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (src_status, src_body) = query_index(&app, "src_move").await;
    assert_eq!(src_status, StatusCode::NOT_FOUND);
    assert_eq!(src_body["status"], json!(404));

    let (dst_status, dst_body) = query_index(&app, "dst_move").await;
    assert_eq!(dst_status, StatusCode::OK);
    assert_eq!(dst_body["nbHits"], json!(2));
}

#[tokio::test]
async fn move_nonexistent_source_is_noop_and_keeps_destination_intact() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "existing_destination",
        ADMIN_KEY,
        vec![json!({"objectID": "d1", "title": "Keep"})],
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/missing_source/operation",
        ADMIN_KEY,
        Some(json!({"operation": "move", "destination": "existing_destination"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    let (dst_status, dst_body) = query_index(&app, "existing_destination").await;
    assert_eq!(dst_status, StatusCode::OK);
    assert_eq!(dst_body["nbHits"], json!(1));
}

/// Multi-scope copy (settings + rules) copies both resources without leaking
/// records or unscoped resources (synonyms) to the destination.
#[tokio::test]
async fn copy_scope_settings_and_rules_copies_both_without_records_or_synonyms() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "src_multi_scope",
        ADMIN_KEY,
        vec![json!({"objectID": "p1", "title": "Seed"})],
    )
    .await;
    common::put_settings_and_wait(
        &app,
        "src_multi_scope",
        ADMIN_KEY,
        json!({
            "searchableAttributes": ["title", "body"],
            "attributesForFaceting": ["brand"]
        }),
        false,
    )
    .await;
    put_rule(
        &app,
        "src_multi_scope",
        "promo-rule",
        json!({
            "objectID": "promo-rule",
            "conditions": [{"anchoring": "contains", "pattern": "sale"}],
            "consequence": {"params": {"query": "sale discount"}}
        }),
    )
    .await;
    put_synonym(
        &app,
        "src_multi_scope",
        "syn-multi",
        json!({
            "objectID": "syn-multi",
            "type": "synonym",
            "synonyms": ["phone", "mobile"]
        }),
    )
    .await;

    let (status, op_body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/src_multi_scope/operation",
        ADMIN_KEY,
        Some(json!({
            "operation": "copy",
            "destination": "dst_multi_scope",
            "scope": ["settings", "rules"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_operation_response_and_wait(&app, &op_body).await;

    // No records copied
    let (dst_query_status, dst_query) = query_index(&app, "dst_multi_scope").await;
    assert_eq!(dst_query_status, StatusCode::OK);
    assert_eq!(dst_query["nbHits"], json!(0), "records must not be copied");

    // Settings copied
    let (settings_status, dst_settings) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_multi_scope/settings",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert_eq!(
        dst_settings["searchableAttributes"],
        json!(["title", "body"]),
        "settings should be copied"
    );

    // Rule copied
    let (rule_status, rule_body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_multi_scope/rules/promo-rule",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(rule_status, StatusCode::OK);
    assert_eq!(
        rule_body["objectID"],
        json!("promo-rule"),
        "rule should be copied"
    );

    // Synonym NOT copied
    let (syn_status, _) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/dst_multi_scope/synonyms/syn-multi",
        ADMIN_KEY,
        None,
    )
    .await;
    assert_eq!(
        syn_status,
        StatusCode::NOT_FOUND,
        "synonyms must not be copied in settings+rules scope"
    );
}
