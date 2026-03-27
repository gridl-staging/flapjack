use axum::{
    http::{Method, StatusCode},
    Router,
};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-stage5-params";

async fn set_attributes_for_faceting(app: &Router, index_name: &str, attributes: Vec<&str>) {
    let (status, body) = common::send_json(
        app,
        Method::PUT,
        &format!("/1/indexes/{index_name}/settings"),
        ADMIN_KEY,
        Some(json!({ "attributesForFaceting": attributes })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "settings update should succeed");
    common::wait_for_task_local_with_key(app, common::extract_task_id(&body), ADMIN_KEY).await;
}

#[tokio::test]
async fn single_search_params_override_and_mixed_payload_preserve_top_level_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    set_attributes_for_faceting(&app, "products", vec!["searchable(brand)"]).await;
    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "a1", "title": "alpha shoes", "brand": "Nike"}),
            json!({"objectID": "b1", "title": "beta shoes", "brand": "Adidas"}),
        ],
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/query",
        ADMIN_KEY,
        Some(json!({
            "query": "beta",
            "hitsPerPage": 10,
            "params": "query=alpha&facets=%5B%22brand%22%5D"
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["query"], json!("alpha"));
    assert_eq!(body["hitsPerPage"], json!(10));
    assert_eq!(body["nbHits"], json!(1));
    assert_eq!(body["hits"][0]["objectID"], json!("a1"));
    assert!(
        body["facets"]["brand"].get("Nike").is_some(),
        "facets from params must be applied: {body}"
    );
}

#[tokio::test]
async fn batch_search_params_override_and_preserve_top_level_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "a1", "title": "alpha shoes"}),
            json!({"objectID": "b1", "title": "beta shoes"}),
        ],
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/queries",
        ADMIN_KEY,
        Some(json!({
            "requests": [{
                "indexName": "products",
                "query": "beta",
                "hitsPerPage": 7,
                "params": "query=alpha"
            }]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let result = &body["results"][0];
    assert_eq!(result["query"], json!("alpha"));
    assert_eq!(result["hitsPerPage"], json!(7));
    assert_eq!(result["nbHits"], json!(1));
    assert_eq!(result["hits"][0]["objectID"], json!("a1"));
}

#[tokio::test]
async fn browse_params_override_and_preserve_top_level_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "a1", "title": "alpha shoes"}),
            json!({"objectID": "b1", "title": "beta shoes"}),
        ],
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/browse",
        ADMIN_KEY,
        Some(json!({
            "query": "beta",
            "hitsPerPage": 2,
            "params": "query=alpha"
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hitsPerPage"], json!(2));
    assert_eq!(body["nbHits"], json!(1));
    assert_eq!(body["hits"][0]["objectID"], json!("a1"));
}

#[tokio::test]
async fn facet_search_params_override_preserves_top_level_filters_and_max_facet_hits() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    set_attributes_for_faceting(
        &app,
        "products",
        vec!["searchable(category)", "searchable(brand)"],
    )
    .await;
    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "apple-1", "category": "Laptop", "brand": "Apple"}),
            json!({"objectID": "samsung-1", "category": "Lamp", "brand": "Samsung"}),
        ],
    )
    .await;

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/facets/category/query",
        ADMIN_KEY,
        Some(json!({
            "facetQuery": "lamp",
            "filters": "brand:Apple",
            "maxFacetHits": 1,
            "params": "facetQuery=la"
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let hits = body["facetHits"].as_array().unwrap();
    assert_eq!(
        hits.len(),
        1,
        "maxFacetHits from top-level JSON must be preserved: {body}"
    );
    assert_eq!(
        hits[0]["value"],
        json!("Laptop"),
        "top-level filters must remain applied when params does not provide filters: {body}"
    );
}

#[tokio::test]
async fn single_search_params_decodes_reserved_characters_and_utf8() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    set_attributes_for_faceting(
        &app,
        "products",
        vec!["searchable(brand)", "searchable(tag)"],
    )
    .await;
    common::seed_docs(
        &app,
        "products",
        ADMIN_KEY,
        vec![
            json!({"objectID": "red1", "title": "red shoe", "brand": "Apple", "tag": "A&B=C"}),
            json!({"objectID": "cpp1", "title": "c++ guide", "brand": "Samsung", "tag": "A&B=C"}),
            json!({"objectID": "cn1", "title": "中文 guide", "brand": "Apple", "tag": "A&B=C"}),
        ],
    )
    .await;

    for (encoded, expected) in [
        ("query=red+shoe", "red shoe"),
        ("query=red%20shoe", "red shoe"),
        ("query=c%2B%2B", "c++"),
        ("query=%E4%B8%AD%E6%96%87%26%3D", "中文&="),
    ] {
        let (status, body) = common::send_json(
            &app,
            Method::POST,
            "/1/indexes/products/query",
            ADMIN_KEY,
            Some(json!({ "params": encoded })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "encoded query case should succeed");
        assert_eq!(
            body["query"],
            json!(expected),
            "query decoding mismatch for params={encoded}: {body}"
        );
    }

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/products/query",
        ADMIN_KEY,
        Some(json!({
            "params": "query=red+shoe&filters=tag%3A%22A%26B%3DC%22&facetFilters=%5B%22brand%3AApple%22%5D&optionalFilters=%5B%22brand%3AApple%3Cscore%3D5%3E%22%5D"
        })),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "encoded filters/facetFilters/optionalFilters should parse"
    );
    assert_eq!(
        body["nbHits"],
        json!(1),
        "filters + facetFilters should be applied"
    );
    assert_eq!(body["hits"][0]["objectID"], json!("red1"));
}
