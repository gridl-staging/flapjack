/// Stage 2 SDK contract tests: Search wire-format parity.
///
/// Each test asserts Algolia-exact response shapes for the search endpoints
/// used by the Algolia JavaScript/Rust/Python SDKs. Tests are written red-first;
/// any handler response-shape gap is fixed in the handler, not here.
///
/// Algolia API reference used: https://www.algolia.com/doc/api-reference/api-methods/search/
use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-parity";

// ── Single Search POST /1/indexes/{index}/query ───────────────────────────────

/// POST /1/indexes/{index}/query — single search — must return core fields.
/// Required: hits, nbHits, page, nbPages, hitsPerPage, processingTimeMS, query,
/// params, exhaustive.nbHits, exhaustive.typo, index, parsedQuery.
#[tokio::test]
async fn single_search_returns_algolia_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data using batch endpoint
    common::seed_docs(
        &app,
        "search-test",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "name": "Laptop", "brand": "Apple" }),
            json!({ "objectID": "2", "name": "Phone", "brand": "Samsung" }),
            json!({ "objectID": "3", "name": "Tablet", "brand": "Apple" }),
        ],
    )
    .await;

    // Execute search
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/search-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "laptop" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Core response fields
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
    assert!(
        body["hitsPerPage"].is_u64() || body["hitsPerPage"].is_i64(),
        "hitsPerPage must be an integer: {body}"
    );
    assert!(
        body["processingTimeMS"].is_u64() || body["processingTimeMS"].is_i64(),
        "processingTimeMS must be an integer: {body}"
    );
    assert!(body["query"].is_string(), "query must be a string: {body}");
    assert!(
        body["params"].is_string(),
        "params must be a string: {body}"
    );

    // Exhaustive object
    assert!(
        body["exhaustive"].is_object(),
        "exhaustive must be an object: {body}"
    );
    assert!(
        body["exhaustive"]["nbHits"].is_boolean(),
        "exhaustive.nbHits must be boolean: {body}"
    );
    assert!(
        body["exhaustive"]["typo"].is_boolean(),
        "exhaustive.typo must be boolean: {body}"
    );

    assert!(body["index"].is_string(), "index must be a string: {body}");
    assert!(
        body["parsedQuery"].is_string(),
        "parsedQuery must be a string: {body}"
    );

    // Must NOT include snake_case variants
    assert!(
        body.get("nb_hits").is_none(),
        "must not have snake_case 'nb_hits': {body}"
    );
    assert!(
        body.get("processing_time_ms").is_none(),
        "must not have snake_case 'processing_time_ms': {body}"
    );
}

// ── Single Search GET /1/indexes/{index} (legacy route) ─────────────────────

/// GET /1/indexes/{index} — legacy search route — must return same shape as POST.
#[tokio::test]
async fn get_search_returns_algolia_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "get-search-test",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "name": "Widget" }),
            json!({ "objectID": "2", "name": "Gadget" }),
        ],
    )
    .await;

    // Execute GET search via query string params
    let (status, body) = common::send_json(
        &app,
        Method::GET,
        "/1/indexes/get-search-test?query=widget",
        ADMIN_KEY,
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Same core fields as POST
    assert!(body["hits"].is_array(), "hits must be an array: {body}");
    assert!(body["nbHits"].is_u64(), "nbHits must be an integer: {body}");
    assert!(body["index"].is_string(), "index must be a string: {body}");
    assert!(body["query"].is_string(), "query must be a string: {body}");
}

// ── Click Analytics: queryID in response ────────────────────────────────────

/// POST with clickAnalytics:true must return queryID in response.
#[tokio::test]
async fn search_with_click_analytics_returns_query_id() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "click-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "1", "name": "Test" })],
    )
    .await;

    // Search with clickAnalytics
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/click-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "test", "clickAnalytics": true })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert!(
        body["queryID"].is_string(),
        "queryID must be present when clickAnalytics=true: {body}"
    );
    let query_id = body["queryID"].as_str().unwrap();
    assert!(!query_id.is_empty(), "queryID must not be empty: {body}");
}

// ── Facets Stats ────────────────────────────────────────────────────────────

/// Search with numeric facets must return facets_stats for numeric attributes.
#[tokio::test]
async fn search_with_facets_returns_facets_or_stats() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Numeric facets require attributesForFaceting settings.
    common::put_settings_and_wait(
        &app,
        "facets-stats-test",
        ADMIN_KEY,
        json!({ "attributesForFaceting": ["price", "brand"] }),
        false,
    )
    .await;

    // Seed data with numeric field after faceting settings are applied.
    common::seed_docs(
        &app,
        "facets-stats-test",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "name": "Item A", "price": 100, "brand": "Acme" }),
            json!({ "objectID": "2", "name": "Item B", "price": 200, "brand": "Acme" }),
            json!({ "objectID": "3", "name": "Item C", "price": 300, "brand": "Globex" }),
        ],
    )
    .await;

    // Search requesting facets on numeric field
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/facets-stats-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "", "facets": ["price", "brand"], "maxValuesPerFacet": 2 })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Should have facets when requested
    assert!(
        body.get("facets").is_some(),
        "facets must be present when requested: {body}"
    );

    let facets_stats = body
        .get("facets_stats")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("facets_stats must be present for numeric facets: {body}"));
    let price_stats = facets_stats["price"]
        .as_object()
        .expect("facets_stats.price must be an object");
    assert_eq!(price_stats["min"].as_f64(), Some(100.0));
    assert_eq!(price_stats["max"].as_f64(), Some(300.0));
    assert_eq!(price_stats["avg"].as_f64(), Some(200.0));
    assert_eq!(price_stats["sum"].as_f64(), Some(600.0));
}

// ── Batch Search POST /1/indexes/*/queries ──────────────────────────────────

/// POST /1/indexes/*/queries — batch search — must return { results: [...] }.
/// Each result must have indexName from request body, not URL path.
#[tokio::test]
async fn batch_search_returns_algolia_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data for two indices
    common::seed_docs(
        &app,
        "batch-a",
        ADMIN_KEY,
        vec![json!({ "objectID": "1", "name": "Test" })],
    )
    .await;
    common::seed_docs(
        &app,
        "batch-b",
        ADMIN_KEY,
        vec![json!({ "objectID": "1", "name": "Test" })],
    )
    .await;

    // Batch search with per-request indexName
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/queries",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "indexName": "batch-a", "query": "test" },
                { "indexName": "batch-b", "query": "test" }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Top-level must be { results: [...] }
    assert!(
        body["results"].is_array(),
        "batch response must have 'results' array: {body}"
    );

    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "should have 2 results: {body}");

    // Each result should have core search fields
    for (i, result) in results.iter().enumerate() {
        assert!(
            result["hits"].is_array(),
            "result[{}] hits must be array: {result}",
            i
        );
        assert!(
            result["index"].is_string(),
            "result[{}] must have index: {result}",
            i
        );
    }
}

// ── Batch Search with indexName in body ─────────────────────────────────────

/// Batch search should use indexName from request body, not URL path.
#[tokio::test]
async fn batch_search_body_index_name_overrides_path() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data for specific index
    common::seed_docs(
        &app,
        "override-test",
        ADMIN_KEY,
        vec![json!({ "objectID": "1", "name": "Override" })],
    )
    .await;

    // Batch search with indexName in body
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/queries",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "indexName": "override-test", "query": "override" }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]["index"].as_str(),
        Some("override-test"),
        "indexName from body should be used: {body}"
    );
}

// ── Batch Search Strategy: stopIfEnoughMatches ─────────────────────────────

/// Batch search with strategy="stopIfEnoughMatches" should skip later queries
/// when a query returns >= hitsPerPage results.
#[tokio::test]
async fn batch_search_stop_if_enough_matches_strategy() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data for two indices
    common::seed_docs(
        &app,
        "stop-a",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "name": "Item 1" }),
            json!({ "objectID": "2", "name": "Item 2" }),
            json!({ "objectID": "3", "name": "Item 3" }),
        ],
    )
    .await;
    common::seed_docs(
        &app,
        "stop-b",
        ADMIN_KEY,
        vec![json!({ "objectID": "1", "name": "Item B" })],
    )
    .await;

    // Batch search with stopIfEnoughMatches and high hitsPerPage
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/queries",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "indexName": "stop-a", "query": "", "hitsPerPage": 3 },
                { "indexName": "stop-b", "query": "" }
            ],
            "strategy": "stopIfEnoughMatches"
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // First result should have processed: true (or no processed field)
    let first = &results[0];
    assert!(
        first["nbHits"].as_u64().unwrap_or(0) >= 3,
        "first query should have enough hits: {first}"
    );

    // Second result should have processed: false
    let second = &results[1];
    assert_eq!(
        second.get("processed").and_then(|v| v.as_bool()),
        Some(false),
        "second query should be marked as not processed: {second}"
    );
}

// ── Batch Search: Mixed query types (default + facet) ────────────────────────

/// Batch search can mix query types: "default" and "facet".
#[tokio::test]
async fn batch_search_mixed_query_types() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "mixed-types",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "category": "electronics", "brand": "Apple" }),
            json!({ "objectID": "2", "category": "electronics", "brand": "Samsung" }),
        ],
    )
    .await;

    // Set up faceting for the brand attribute
    common::put_settings_and_wait(
        &app,
        "mixed-types",
        ADMIN_KEY,
        json!({ "attributesForFaceting": ["brand"] }),
        false,
    )
    .await;

    // Batch search with mixed types
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/*/queries",
        ADMIN_KEY,
        Some(json!({
            "requests": [
                { "indexName": "mixed-types", "query": "apple", "type": "default" },
                { "indexName": "mixed-types", "query": "electronics", "type": "facet", "facetQuery": "brand" }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // Default type returns hits
    assert!(
        results[0]["hits"].is_array(),
        "default type should return hits: {}",
        results[0]
    );
    // Facet type returns facetHits (or hits depending on implementation)
    assert!(
        results[1].get("facetHits").is_some() || results[1]["hits"].is_array(),
        "facet type should return facetHits or hits: {}",
        results[1]
    );
}

// ── URL-encoded params string format ────────────────────────────────────────

/// Search request with params string (URL-encoded) should be decoded correctly.
#[tokio::test]
async fn search_with_params_string_format() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "params-test",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "brand": "Apple", "category": "laptop" }),
            json!({ "objectID": "2", "brand": "Samsung", "category": "phone" }),
        ],
    )
    .await;

    // Search using params string format
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/params-test/query",
        ADMIN_KEY,
        Some(json!({ "params": "query=apple&hitsPerPage=10&facets=%5B%22brand%22%5D" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Response params should echo the params
    let params = body["params"].as_str().unwrap();
    assert!(
        params.contains("query=apple"),
        "params should echo query: {params}"
    );
    assert!(
        params.contains("hitsPerPage=10"),
        "params should echo hitsPerPage: {params}"
    );
}

// ── Facet Search POST /1/indexes/{index}/facets/{facet}/query ───────────────

/// POST /1/indexes/{index}/facets/{facet}/query — dedicated facet search endpoint.
/// Must set searchable attribute for faceting first.
#[tokio::test]
async fn facet_search_returns_algolia_response_shape() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "facet-search",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "category": "electronics" }),
            json!({ "objectID": "2", "category": "electronics" }),
            json!({ "objectID": "3", "category": "clothing" }),
        ],
    )
    .await;

    // Set up faceting settings - use searchable() format for dedicated facet search
    common::put_settings_and_wait(
        &app,
        "facet-search",
        ADMIN_KEY,
        json!({ "attributesForFaceting": ["searchable(category)"] }),
        false,
    )
    .await;

    // Facet search
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/facet-search/facets/category/query",
        ADMIN_KEY,
        Some(json!({ "query": "elec" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Dedicated facet search response shape:
    // { facetHits: [{ value, highlighted, count }], exhaustiveFacetsCount, processingTimeMS }
    let facet_hits = body["facetHits"]
        .as_array()
        .expect("facet search should return facetHits array");
    for facet_hit in facet_hits {
        assert!(
            facet_hit["value"].is_string(),
            "facetHit.value must be string: {facet_hit}"
        );
        assert!(
            facet_hit["highlighted"].is_string(),
            "facetHit.highlighted must be string: {facet_hit}"
        );
        assert!(
            facet_hit["count"].is_u64() || facet_hit["count"].is_i64(),
            "facetHit.count must be integer: {facet_hit}"
        );
    }
    assert!(
        body["exhaustiveFacetsCount"].is_boolean(),
        "exhaustiveFacetsCount must be boolean: {body}"
    );
    let proc_time = body.get("processingTimeMS").unwrap();
    assert!(
        proc_time.is_u64() || proc_time.is_i64(),
        "processingTimeMS must be an integer: {body}"
    );
}

// ── Empty query search ───────────────────────────────────────────────────────

/// Search with empty query should return all records (no filtering).
#[tokio::test]
async fn search_with_empty_query_returns_all() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data
    common::seed_docs(
        &app,
        "empty-query",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "name": "Item A" }),
            json!({ "objectID": "2", "name": "Item B" }),
        ],
    )
    .await;

    // Search with empty query
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/empty-query/query",
        ADMIN_KEY,
        Some(json!({ "query": "" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Should return all hits
    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2, "empty query should return all: {body}");
}

// ── Search without facets ───────────────────────────────────────────────────

/// Search without facets request should not include facets or facets_stats.
#[tokio::test]
async fn search_without_facets_excludes_facets_fields() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    // Seed data with faceted attribute
    common::seed_docs(
        &app,
        "no-facets-test",
        ADMIN_KEY,
        vec![
            json!({ "objectID": "1", "category": "A" }),
            json!({ "objectID": "2", "category": "B" }),
        ],
    )
    .await;

    // Search without facets
    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/no-facets-test/query",
        ADMIN_KEY,
        Some(json!({ "query": "" })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    // Should not have facets_stats when facets aren't requested.
    assert!(
        body.get("facets_stats").is_none(),
        "facets_stats must be omitted when facets are not requested: {body}"
    );

    // facets should be omitted, null, or an empty object.
    assert!(
        body.get("facets").is_none()
            || body["facets"].is_null()
            || body["facets"].as_object().is_some_and(|obj| obj.is_empty()),
        "facets must be absent, null, or empty when not requested: {body}"
    );
}
