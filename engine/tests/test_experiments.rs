use axum::{
    body::Body,
    http::{Method, StatusCode},
    Router,
};
use serde_json::{json, Value};
mod common;
use common::body_json;

fn build_experiments_app() -> (Router, common::TempDir) {
    common::build_test_app_for_local_requests(None)
}

fn create_experiment_body(index_name: &str) -> Value {
    json!({
        "name": format!("Ranking Test {index_name}"),
        "variants": [
            {
                "index": index_name,
                "trafficPercentage": 50,
                "description": "control"
            },
            {
                "index": format!("{index_name}_v2"),
                "trafficPercentage": 50,
                "description": "variant"
            }
        ],
        "endAt": "2099-01-01T00:00:00Z",
        "metrics": [{ "name": "clickThroughRate" }]
    })
}

async fn send_json_request(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> axum::http::Response<Body> {
    common::send_json_response(app, method, uri, Some(body)).await
}

async fn send_empty_request(app: &Router, method: Method, uri: &str) -> axum::http::Response<Body> {
    common::send_empty_response(app, method, uri).await
}

/// Creates an experiment via the Algolia-format endpoint and returns the numeric abTestID.
async fn create_experiment(app: &Router, index_name: &str) -> i64 {
    let response = send_json_request(
        app,
        Method::POST,
        "/2/abtests",
        create_experiment_body(index_name),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    json["abTestID"]
        .as_i64()
        .expect("create response must include abTestID")
}

#[tokio::test]
async fn test_create_and_get_experiment() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    let response = send_empty_request(&app, Method::GET, &format!("/2/abtests/{ab_test_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let fetched = body_json(response).await;
    assert_eq!(fetched["abTestID"], ab_test_id);
    // Algolia maps Draft to "active" since it has no draft concept.
    assert_eq!(fetched["status"], "active");
}

#[tokio::test]
async fn test_list_experiments() {
    let (app, _tmp) = build_experiments_app();

    create_experiment(&app, "products").await;

    let response = send_empty_request(&app, Method::GET, "/2/abtests").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let abtests = body["abtests"].as_array().unwrap();
    assert_eq!(abtests.len(), 1);
    assert_eq!(body["count"], 1);
    assert_eq!(body["total"], 1);
}

#[tokio::test]
async fn test_list_experiments_filters_by_index_prefix() {
    let (app, _tmp) = build_experiments_app();

    create_experiment(&app, "products").await;
    create_experiment(&app, "categories").await;

    // indexPrefix=prod should match only the "products" experiment.
    let response = send_empty_request(&app, Method::GET, "/2/abtests?indexPrefix=prod").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let abtests = body["abtests"].as_array().unwrap();
    assert_eq!(abtests.len(), 1);
    assert_eq!(body["count"], 1);
    assert_eq!(body["total"], 1);
    assert_eq!(abtests[0]["variants"][0]["index"], "products");
}

#[tokio::test]
async fn test_start_stop_lifecycle() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    // Start returns an action response (abTestID, index, taskID), not the experiment.
    let start_response = send_empty_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/start"),
    )
    .await;
    assert_eq!(start_response.status(), StatusCode::OK);
    let started = body_json(start_response).await;
    assert_eq!(started["abTestID"], ab_test_id);
    assert_eq!(started["index"], "products");

    // Verify via GET that status is now "active" (Running maps to active).
    let get_resp = send_empty_request(&app, Method::GET, &format!("/2/abtests/{ab_test_id}")).await;
    let get_json = body_json(get_resp).await;
    assert_eq!(get_json["status"], "active");

    // Stop also returns action response.
    let stop_response =
        send_empty_request(&app, Method::POST, &format!("/2/abtests/{ab_test_id}/stop")).await;
    assert_eq!(stop_response.status(), StatusCode::OK);
    let stopped = body_json(stop_response).await;
    assert_eq!(stopped["abTestID"], ab_test_id);

    // Verify via GET that status is now "stopped".
    let get_resp2 =
        send_empty_request(&app, Method::GET, &format!("/2/abtests/{ab_test_id}")).await;
    let get_json2 = body_json(get_resp2).await;
    assert_eq!(get_json2["status"], "stopped");
}

#[tokio::test]
async fn test_delete_draft() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    let delete_response =
        send_empty_request(&app, Method::DELETE, &format!("/2/abtests/{ab_test_id}")).await;
    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_json = body_json(delete_response).await;
    assert_eq!(delete_json["abTestID"], ab_test_id);

    let get_response =
        send_empty_request(&app, Method::GET, &format!("/2/abtests/{ab_test_id}")).await;
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_results_response_structure() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    let response = send_empty_request(
        &app,
        Method::GET,
        &format!("/2/abtests/{ab_test_id}/results"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    // experimentID is the internal UUID, not the numeric ID.
    assert!(body["experimentID"].as_str().is_some());
    assert_eq!(body["name"], "Ranking Test products");
    // Results endpoint uses internal status values.
    assert_eq!(body["status"], "draft");
    assert_eq!(body["indexName"], "products");
    assert!(body["gate"].is_object());
    assert_eq!(body["gate"]["readyToRead"], false);
    assert_eq!(body["control"]["searches"], 0);
    assert_eq!(body["variant"]["searches"], 0);
    assert!(body["significance"].is_null());
    assert_eq!(body["sampleRatioMismatch"], false);
}

#[tokio::test]
async fn test_conclude_from_running() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    send_empty_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/start"),
    )
    .await;

    let body = json!({
        "winner": "variant",
        "reason": "Significant",
        "controlMetric": 0.10,
        "variantMetric": 0.15,
        "confidence": 0.95,
        "significant": true,
        "promoted": false
    });
    let resp = send_json_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/conclude"),
        body,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let concluded = body_json(resp).await;
    assert_eq!(concluded["status"], "concluded");
    assert_eq!(concluded["conclusion"]["winner"], "variant");
}

#[tokio::test]
async fn test_conclude_from_stopped() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    send_empty_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/start"),
    )
    .await;
    send_empty_request(&app, Method::POST, &format!("/2/abtests/{ab_test_id}/stop")).await;

    let body = json!({
        "reason": "Inconclusive",
        "controlMetric": 0.10,
        "variantMetric": 0.11,
        "confidence": 0.60,
        "significant": false,
        "promoted": false
    });
    let resp = send_json_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/conclude"),
        body,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let concluded = body_json(resp).await;
    assert_eq!(concluded["status"], "concluded");
    assert!(concluded["conclusion"]["winner"].is_null());
}

#[tokio::test]
async fn test_conclude_draft_returns_409() {
    let (app, _tmp) = build_experiments_app();

    let ab_test_id = create_experiment(&app, "products").await;

    let body = json!({
        "winner": "variant",
        "reason": "Early call",
        "controlMetric": 0.10,
        "variantMetric": 0.15,
        "confidence": 0.95,
        "significant": true,
        "promoted": false
    });
    let resp = send_json_request(
        &app,
        Method::POST,
        &format!("/2/abtests/{ab_test_id}/conclude"),
        body,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}
