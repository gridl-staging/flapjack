use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

#[derive(Debug)]
struct MutationCaseResult {
    status: StatusCode,
    body: serde_json::Value,
}

fn sorted_keys(body: &serde_json::Value) -> Vec<String> {
    let mut keys: Vec<String> = body
        .as_object()
        .unwrap_or_else(|| panic!("response body must be an object: {body}"))
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}

async fn create_key(app: &axum::Router) -> String {
    let (status, body) = common::send_json(
        app,
        Method::POST,
        "/1/keys",
        ADMIN_KEY,
        Some(json!({
            "acl": ["search"],
            "description": "mutation parity key"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "failed to create key: {body}");
    body["key"].as_str().unwrap().to_string()
}

async fn create_experiment(app: &axum::Router, index_name: &str) -> i64 {
    let (status, body) = common::send_json(
        app,
        Method::POST,
        "/2/abtests",
        ADMIN_KEY,
        Some(json!({
            "name": format!("Parity experiment for {index_name}"),
            "variants": [
                { "index": index_name, "trafficPercentage": 50 },
                { "index": format!("{index_name}_variant"), "trafficPercentage": 50 }
            ],
            "endAt": "2026-04-30T00:00:00Z"
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "failed to create experiment: {body}"
    );
    body["abTestID"].as_i64().unwrap()
}

async fn seed_object(app: &axum::Router, index_name: &str, object_id: &str) {
    let (status, body) = common::send_json(
        app,
        Method::POST,
        &format!("/1/indexes/{index_name}/batch"),
        ADMIN_KEY,
        Some(json!({
            "requests": [
                {
                    "action": "addObject",
                    "body": {
                        "objectID": object_id,
                        "title": format!("Document {object_id}")
                    }
                }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "failed to seed object: {body}");
    common::wait_for_task_local(app, common::extract_task_id(&body)).await;
}

async fn run_key_case(
    app: &axum::Router,
    case: &flapjack_http::mutation_parity::MutationParityCase,
) -> MutationCaseResult {
    match case.id {
        "keys.create" => {
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/1/keys",
                ADMIN_KEY,
                Some(json!({
                    "acl": ["search"],
                    "description": "mutation parity create"
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "keys.update" => {
            let key = create_key(app).await;
            let (status, body) = common::send_json(
                app,
                Method::PUT,
                &format!("/1/keys/{key}"),
                ADMIN_KEY,
                Some(json!({
                    "acl": ["search", "browse"],
                    "description": "updated mutation parity key"
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "keys.delete" => {
            let key = create_key(app).await;
            let (status, body) = common::send_json(
                app,
                Method::DELETE,
                &format!("/1/keys/{key}"),
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        "keys.restore" => {
            let key = create_key(app).await;
            let (delete_status, delete_body) = common::send_json(
                app,
                Method::DELETE,
                &format!("/1/keys/{key}"),
                ADMIN_KEY,
                None,
            )
            .await;
            assert_eq!(
                delete_status,
                StatusCode::OK,
                "failed to delete key before restore: {delete_body}"
            );
            let (status, body) = common::send_json(
                app,
                Method::POST,
                &format!("/1/keys/{key}/restore"),
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        _ => panic!("unexpected key mutation case {}", case.id),
    }
}

async fn run_abtest_case(
    app: &axum::Router,
    case: &flapjack_http::mutation_parity::MutationParityCase,
) -> MutationCaseResult {
    match case.id {
        "abtests.create" => {
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/2/abtests",
                ADMIN_KEY,
                Some(json!({
                    "name": "Create parity experiment",
                    "variants": [
                        { "index": "products", "trafficPercentage": 50 },
                        { "index": "products_variant", "trafficPercentage": 50 }
                    ],
                    "endAt": "2026-04-30T00:00:00Z"
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "abtests.update" => {
            let id = create_experiment(app, "products").await;
            let (status, body) = common::send_json(
                app,
                Method::PUT,
                &format!("/2/abtests/{id}"),
                ADMIN_KEY,
                Some(json!({
                    "name": "Updated parity experiment",
                    "indexName": "products",
                    "trafficSplit": 0.5,
                    "control": { "name": "control" },
                    "variant": {
                        "name": "variant",
                        "queryOverrides": {
                            "enableSynonyms": false
                        }
                    },
                    "primaryMetric": "ctr",
                    "minimumDays": 14
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "abtests.delete" => {
            let id = create_experiment(app, "delete_products").await;
            let (status, body) = common::send_json(
                app,
                Method::DELETE,
                &format!("/2/abtests/{id}"),
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        "abtests.start" => {
            let id = create_experiment(app, "start_products").await;
            let (status, body) = common::send_json(
                app,
                Method::POST,
                &format!("/2/abtests/{id}/start"),
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        "abtests.stop" => {
            let id = create_experiment(app, "stop_products").await;
            let (start_status, start_body) = common::send_json(
                app,
                Method::POST,
                &format!("/2/abtests/{id}/start"),
                ADMIN_KEY,
                None,
            )
            .await;
            assert_eq!(
                start_status,
                StatusCode::OK,
                "failed to start experiment before stop: {start_body}"
            );
            let (status, body) = common::send_json(
                app,
                Method::POST,
                &format!("/2/abtests/{id}/stop"),
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        "abtests.conclude" => {
            let id = create_experiment(app, "conclude_products").await;
            let (start_status, start_body) = common::send_json(
                app,
                Method::POST,
                &format!("/2/abtests/{id}/start"),
                ADMIN_KEY,
                None,
            )
            .await;
            assert_eq!(
                start_status,
                StatusCode::OK,
                "failed to start experiment before conclude: {start_body}"
            );
            let (status, body) = common::send_json(
                app,
                Method::POST,
                &format!("/2/abtests/{id}/conclude"),
                ADMIN_KEY,
                Some(json!({
                    "winner": "control",
                    "reason": "control kept better conversion",
                    "controlMetric": 0.32,
                    "variantMetric": 0.29,
                    "confidence": 0.95,
                    "significant": true,
                    "promoted": false
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        _ => panic!("unexpected abtest mutation case {}", case.id),
    }
}

async fn run_index_case(
    app: &axum::Router,
    case: &flapjack_http::mutation_parity::MutationParityCase,
) -> MutationCaseResult {
    match case.id {
        "indexes.create" => {
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/1/indexes",
                ADMIN_KEY,
                Some(json!({ "uid": "matrix-created-index" })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "indexes.delete" => {
            let (create_status, create_body) = common::send_json(
                app,
                Method::POST,
                "/1/indexes",
                ADMIN_KEY,
                Some(json!({ "uid": "matrix-delete-index" })),
            )
            .await;
            assert_eq!(
                create_status,
                StatusCode::OK,
                "failed to create index before delete: {create_body}"
            );
            let (status, body) = common::send_json(
                app,
                Method::DELETE,
                "/1/indexes/matrix-delete-index",
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        _ => panic!("unexpected index mutation case {}", case.id),
    }
}

async fn run_object_case(
    app: &axum::Router,
    case: &flapjack_http::mutation_parity::MutationParityCase,
) -> MutationCaseResult {
    match case.id {
        "objects.save_auto_id" => {
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/1/indexes/matrix-products",
                ADMIN_KEY,
                Some(json!({ "title": "Auto-created from parity test" })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "objects.batch" => {
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/1/indexes/matrix-batch/batch",
                ADMIN_KEY,
                Some(json!({
                    "requests": [
                        { "action": "addObject", "body": { "objectID": "one", "title": "One" } },
                        { "action": "addObject", "body": { "objectID": "two", "title": "Two" } }
                    ]
                })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        "objects.delete" => {
            seed_object(app, "matrix-delete-objects", "dead-doc").await;
            let (status, body) = common::send_json(
                app,
                Method::DELETE,
                "/1/indexes/matrix-delete-objects/dead-doc",
                ADMIN_KEY,
                None,
            )
            .await;
            MutationCaseResult { status, body }
        }
        "objects.partial" => {
            seed_object(app, "matrix-partial-objects", "partial-doc").await;
            let (status, body) = common::send_json(
                app,
                Method::POST,
                "/1/indexes/matrix-partial-objects/partial-doc/partial",
                ADMIN_KEY,
                Some(json!({ "title": "Updated by parity" })),
            )
            .await;
            MutationCaseResult { status, body }
        }
        _ => panic!("unexpected object mutation case {}", case.id),
    }
}

async fn run_case(
    app: &axum::Router,
    case: &flapjack_http::mutation_parity::MutationParityCase,
) -> MutationCaseResult {
    if case.id.starts_with("keys.") {
        return run_key_case(app, case).await;
    }
    if case.id.starts_with("abtests.") {
        return run_abtest_case(app, case).await;
    }
    if case.id.starts_with("indexes.") {
        return run_index_case(app, case).await;
    }
    if case.id.starts_with("objects.") {
        return run_object_case(app, case).await;
    }
    panic!("unhandled mutation parity case {}", case.id);
}

#[tokio::test]
async fn high_risk_mutation_endpoints_match_expected_status_and_envelopes() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    for case in flapjack_http::mutation_parity::HIGH_RISK_MUTATION_PARITY_CASES {
        let result = run_case(&app, case).await;

        assert_eq!(
            result.status,
            StatusCode::from_u16(case.expected_status).unwrap(),
            "{} {} returned unexpected status: {}",
            case.method,
            case.path,
            result.body
        );

        for field in case.required_fields {
            assert!(
                result.body.get(*field).is_some(),
                "{} {} missing required field '{}': {}",
                case.method,
                case.path,
                field,
                result.body
            );
        }

        if let Some(exact_fields) = case.exact_fields {
            let expected: Vec<String> = exact_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect();
            assert_eq!(
                sorted_keys(&result.body),
                expected,
                "{} {} returned unexpected top-level fields: {}",
                case.method,
                case.path,
                result.body
            );
        }
    }
}
