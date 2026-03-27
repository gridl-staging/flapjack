//! Multi-index batch search integration tests covering index routing, search strategies (none, stopIfEnoughMatches), facet searches, params string handling, experiment annotations, and error cases.
use super::*;

// ── Path & indexName handling ──

/// Verify that the literal '*' in the URL path correctly routes requests to the indexes specified in each request's `indexName` field.
#[tokio::test]
async fn batch_search_star_path_uses_body_index_name() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "idx_alpha", vec![vec![("title", "alpha laptop")]]).await;
    create_index_with_docs(&state, "idx_beta", vec![vec![("title", "beta phone")]]).await;
    let app = batch_router(state);

    // POST to /1/indexes/*/queries — the `*` in the path is literal
    // and each request's body indexName determines which index to search
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "idx_alpha", "query": "laptop" },
                { "indexName": "idx_beta", "query": "phone" }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2);
    assert!(
        results[0]["nbHits"].as_u64().unwrap() >= 1,
        "alpha index should find 'laptop'"
    );
    assert!(
        results[1]["nbHits"].as_u64().unwrap() >= 1,
        "beta index should find 'phone'"
    );
}

/// Verify that the `indexName` specified in the request body takes precedence over any index name in the URL path.
#[tokio::test]
async fn batch_search_body_index_overrides_path_index() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "real_idx", vec![vec![("title", "real data")]]).await;
    let app = batch_router(state);

    // Path says "some_other_index" but body says "real_idx"
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/1/indexes/some_other_index/queries")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "requests": [
                            { "indexName": "real_idx", "query": "real" }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(
        body["results"][0]["nbHits"].as_u64().unwrap() >= 1,
        "should search the body indexName, not the path"
    );
}

// ── Strategy: default (none) ──

/// Verify that strategy='none' executes all queries without including the `processed` field in any result.
#[tokio::test]
async fn batch_search_strategy_none_returns_all_results_no_processed_field() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "strat_idx",
        vec![
            vec![("title", "laptop computer")],
            vec![("title", "phone mobile")],
        ],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "strat_idx", "query": "laptop" },
                { "indexName": "strat_idx", "query": "phone" }
            ],
            "strategy": "none"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    // Both queries should be executed
    assert!(results[0]["nbHits"].as_u64().unwrap() >= 1);
    assert!(results[1]["nbHits"].as_u64().unwrap() >= 1);
    // `processed` field must be ABSENT on all results
    assert!(
        results[0].get("processed").is_none(),
        "processed must be absent for strategy=none"
    );
    assert!(
        results[1].get("processed").is_none(),
        "processed must be absent for strategy=none"
    );
}

/// Verify that omitting the strategy field defaults to behavior equivalent to strategy='none', with no `processed` field in results.
#[tokio::test]
async fn batch_search_default_strategy_no_processed_field() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "def_idx", vec![vec![("title", "test item")]]).await;
    let app = batch_router(state);

    // No strategy field at all — should behave same as "none"
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "def_idx", "query": "test" }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert!(
        results[0].get("processed").is_none(),
        "processed must be absent when no strategy"
    );
}

// ── Strategy: stopIfEnoughMatches ──

/// Verify that strategy='stopIfEnoughMatches' skips subsequent queries after one returns at least `hitsPerPage` hits, returning a stub result with `processed: false` for skipped queries.
#[tokio::test]
async fn batch_search_stop_if_enough_matches_skips_later_queries() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    // Index with enough docs so first query returns >= hitsPerPage hits
    create_index_with_docs(
        &state,
        "stop_idx",
        vec![
            vec![("title", "laptop pro")],
            vec![("title", "laptop air")],
            vec![("title", "laptop mini")],
        ],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "stop_idx", "query": "laptop", "hitsPerPage": 1 },
                { "indexName": "stop_idx", "query": "laptop", "hitsPerPage": 1 }
            ],
            "strategy": "stopIfEnoughMatches"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // First query executed — no `processed` field
    assert!(results[0]["nbHits"].as_u64().unwrap() >= 1);
    assert!(
        results[0].get("processed").is_none(),
        "executed query must NOT have processed field"
    );

    // Second query was SKIPPED — stub response with processed: false
    assert_eq!(
        results[1]["processed"], false,
        "skipped query must have processed: false"
    );
    assert_eq!(results[1]["hits"].as_array().unwrap().len(), 0);
    assert_eq!(results[1]["nbHits"], 0);
    assert_eq!(results[1]["page"], 0);
    assert_eq!(results[1]["nbPages"], 0);
    assert_eq!(results[1]["hitsPerPage"], 0);
}

/// Verify that strategy='stopIfEnoughMatches' continues processing queries if a query returns zero hits, and only stops after finding a query with sufficient hits.
#[tokio::test]
async fn batch_search_stop_if_enough_matches_zero_hits_continues() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "cont_idx", vec![vec![("title", "laptop computer")]]).await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "cont_idx", "query": "zzzznotfound", "hitsPerPage": 1 },
                { "indexName": "cont_idx", "query": "laptop", "hitsPerPage": 1 },
                { "indexName": "cont_idx", "query": "laptop", "hitsPerPage": 1 }
            ],
            "strategy": "stopIfEnoughMatches"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);

    // First query: 0 hits, still executed — no processed field
    assert_eq!(results[0]["nbHits"], 0);
    assert!(
        results[0].get("processed").is_none(),
        "executed 0-hit query must NOT have processed"
    );

    // Second query: finds laptop, >= hitsPerPage — no processed field
    assert!(results[1]["nbHits"].as_u64().unwrap() >= 1);
    assert!(
        results[1].get("processed").is_none(),
        "executed matching query must NOT have processed"
    );

    // Third query: SKIPPED — stub with processed: false
    assert_eq!(results[2]["processed"], false);
    assert_eq!(results[2]["hits"].as_array().unwrap().len(), 0);
    assert_eq!(results[2]["nbHits"], 0);
}

/// Verify that SecuredKeyRestrictions.hits_per_page cap is applied when determining whether enough matches have been found for strategy='stopIfEnoughMatches'.
#[tokio::test]
async fn batch_search_stop_if_enough_uses_secured_hits_per_page_cap_threshold() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "cap_idx", vec![vec![("title", "laptop computer")]]).await;
    let app = batch_router(state);

    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/1/indexes/*/queries")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "requests": [
                    { "indexName": "cap_idx", "query": "laptop", "hitsPerPage": 2 },
                    { "indexName": "cap_idx", "query": "laptop", "hitsPerPage": 2 }
                ],
                "strategy": "stopIfEnoughMatches"
            })
            .to_string(),
        ))
        .unwrap();
    request
        .extensions_mut()
        .insert(crate::auth::SecuredKeyRestrictions {
            hits_per_page: Some(1),
            ..Default::default()
        });

    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    assert_eq!(results[0]["hitsPerPage"].as_u64(), Some(1));
    assert!(
        results[0].get("processed").is_none(),
        "first query should execute"
    );
    assert_eq!(
        results[1]["processed"], false,
        "second query should be skipped once secured cap threshold is met"
    );
}

// ── Type: facet in batch ──

/// Verify that a single batch can contain both regular search requests and facet search requests (type='facet'), each returning appropriate response fields.
#[tokio::test]
async fn batch_search_mixed_type_default_and_facet() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "mix_idx",
        vec![
            vec![("title", "laptop"), ("category", "electronics")],
            vec![("title", "phone"), ("category", "electronics")],
            vec![("title", "shirt"), ("category", "clothing")],
        ],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "mix_idx", "query": "laptop" },
                { "indexName": "mix_idx", "type": "facet", "facet": "category", "facetQuery": "elec" }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // First result: regular search — has hits, nbHits
    assert!(
        results[0].get("hits").is_some(),
        "default type must return hits"
    );
    assert!(
        results[0].get("nbHits").is_some(),
        "default type must return nbHits"
    );

    // Second result: facet search — has facetHits, exhaustiveFacetsCount
    assert!(
        results[1].get("facetHits").is_some(),
        "facet type must return facetHits"
    );
    assert!(
        results[1].get("exhaustiveFacetsCount").is_some(),
        "facet type must return exhaustiveFacetsCount"
    );
    assert!(
        results[1].get("processingTimeMS").is_some(),
        "facet type must return processingTimeMS"
    );
    // Facet result should NOT have regular search fields
    assert!(
        results[1].get("hits").is_none(),
        "facet type must not return hits"
    );
}

/// Verify that an invalid `sortFacetValuesBy` value in the request JSON returns a 400 error with a validation message.
#[tokio::test]
async fn batch_search_type_facet_rejects_invalid_sort_facet_values_by() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "facet_sort_invalid_idx",
        vec![vec![("title", "laptop"), ("category", "electronics")]],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                {
                    "indexName": "facet_sort_invalid_idx",
                    "type": "facet",
                    "facet": "category",
                    "facetQuery": "elec",
                    "sortFacetValuesBy": "invalid"
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("sortFacetValuesBy"),
        "expected sortFacetValuesBy validation error, got: {msg}"
    );
}

/// Verify that an invalid `sortFacetValuesBy` value in the params string returns a 400 error with a validation message.
#[tokio::test]
async fn batch_search_type_facet_rejects_invalid_sort_facet_values_by_in_params() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "facet_sort_invalid_params_idx",
        vec![vec![("title", "laptop"), ("category", "electronics")]],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                {
                    "indexName": "facet_sort_invalid_params_idx",
                    "type": "facet",
                    "params": "facet=category&facetQuery=elec&sortFacetValuesBy=invalid"
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("sortFacetValuesBy"),
        "expected sortFacetValuesBy validation error, got: {msg}"
    );
}

// ── Params string handling ──

/// Verify that search queries can be specified via URL-encoded params string.
#[tokio::test]
async fn batch_search_params_string_sets_query() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "params_idx",
        vec![vec![("title", "laptop computer")]],
    )
    .await;
    let app = batch_router(state);

    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "params_idx", "params": "query=laptop&hitsPerPage=5" }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(
        body["results"][0]["nbHits"].as_u64().unwrap() >= 1,
        "params string query should find results"
    );
}

/// Verify that URL-encoded params string values take precedence over top-level JSON values for overlapping parameters.
#[tokio::test]
async fn batch_search_params_string_overrides_top_level_json() {
    // Verified against live Algolia API (2026-02-23): params string wins
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "override_idx",
        vec![
            vec![("title", "laptop computer")],
            vec![("title", "phone mobile")],
        ],
    )
    .await;
    let app = batch_router(state);

    // params query="phone" overrides top-level JSON query="laptop"
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "override_idx", "query": "laptop", "params": "query=phone" }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let hits = body["results"][0]["hits"].as_array().unwrap();
    // Should find phone, not laptop — params string overrides top-level JSON
    assert!(
        hits.iter()
            .any(|h| { h["title"].as_str().is_some_and(|t| t.contains("phone")) }),
        "params string query must override top-level JSON query"
    );
}

// ── Params string: facet-related fields via params ──

/// Verify that facet search parameters can be specified via URL-encoded params string in type='facet' requests.
#[tokio::test]
async fn batch_search_params_string_facet_fields_in_type_facet() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "pfacet_idx",
        vec![
            vec![("title", "laptop"), ("category", "electronics")],
            vec![("title", "phone"), ("category", "electronics")],
        ],
    )
    .await;
    let app = batch_router(state);

    // type=facet with facet name as top-level JSON, query params via URL-encoded string
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                {
                    "indexName": "pfacet_idx",
                    "type": "facet",
                    "facet": "category",
                    "params": "facetQuery=elec&maxFacetHits=5"
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let facet_hits = body["results"][0]["facetHits"]
        .as_array()
        .expect("facet type must return facetHits array");
    assert!(
        !facet_hits.is_empty(),
        "facetQuery=elec should match 'electronics'"
    );
    assert!(
        facet_hits[0]["value"]
            .as_str()
            .unwrap()
            .contains("electron"),
        "facet hit should be 'electronics'"
    );
}

/// Verify that facet name and all facet search parameters can be specified via URL-encoded params string in type='facet' requests.
#[tokio::test]
async fn batch_search_facet_name_via_params_string() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "pfacet2_idx",
        vec![vec![("title", "laptop"), ("category", "electronics")]],
    )
    .await;
    let app = batch_router(state);

    // All facet fields sent via URL-encoded params string, including facet name
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                {
                    "indexName": "pfacet2_idx",
                    "type": "facet",
                    "params": "facet=category&facetQuery=elec&maxFacetHits=5"
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let facet_hits = body["results"][0]["facetHits"]
        .as_array()
        .expect("facet type must return facetHits array");
    assert!(
        !facet_hits.is_empty(),
        "facet=category via params string should find 'electronics'"
    );
}

// ── Regression: experiment annotations with strategy ──

/// Verify that experiment annotations are preserved on executed queries when using strategy='stopIfEnoughMatches'.
#[tokio::test]
async fn batch_search_experiment_annotations_with_stop_if_enough() {
    let tmp = TempDir::new().unwrap();
    let state = make_search_experiment_state(&tmp).await;
    let app = Router::new()
        .route("/1/indexes/:indexName/queries", post(batch_search))
        .with_state(state);

    // Use stopIfEnoughMatches with the experiment index — annotations must survive
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "products", "query": "shoe", "hitsPerPage": 1 },
                { "indexName": "products", "query": "running", "hitsPerPage": 1 }
            ],
            "strategy": "stopIfEnoughMatches"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // First query: executed, should have experiment annotation
    assert_eq!(
        results[0]["abTestID"], "exp-mode-a",
        "executed query in stopIfEnoughMatches must retain abTestID"
    );
    let variant_id = results[0]["abTestVariantID"].as_str().unwrap();
    assert!(
        variant_id == "control" || variant_id == "variant",
        "executed query abTestVariantID must be 'control' or 'variant'"
    );

    // Second query: skipped — stub response, no experiment annotation expected
    assert_eq!(
        results[1]["processed"], false,
        "second query should be skipped"
    );
}

/// Verify that experiment annotations (abTestID, abTestVariantID) are preserved on all executed queries when strategy='none'.
#[tokio::test]
async fn batch_search_experiment_annotations_parallel_strategy() {
    let tmp = TempDir::new().unwrap();
    let state = make_search_experiment_state(&tmp).await;
    let app = Router::new()
        .route("/1/indexes/:indexName/queries", post(batch_search))
        .with_state(state);

    // Parallel strategy — all queries executed, all should have annotations
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                { "indexName": "products", "query": "shoe" },
                { "indexName": "products", "query": "running" }
            ],
            "strategy": "none"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();

    for (i, result) in results.iter().enumerate() {
        assert_eq!(
            result["abTestID"], "exp-mode-a",
            "query {i} with strategy=none must retain abTestID"
        );
        assert!(
            result.get("processed").is_none(),
            "query {i} with strategy=none must NOT have processed field"
        );
    }
}

// ── End-to-end integration: combining all features ──

/// Verify that batch search correctly handles a complex request combining multiple features: literal '*' path, multiple indexes, mixed request types, params string parameters, and stopIfEnoughMatches strategy.
#[tokio::test]
async fn batch_search_combined_e2e_star_path_mixed_type_params_strategy() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    // Create two different indexes
    create_index_with_docs(
        &state,
        "e2e_alpha",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop air"), ("category", "electronics")],
        ],
    )
    .await;
    create_index_with_docs(
        &state,
        "e2e_beta",
        vec![vec![("title", "shirt cotton"), ("category", "clothing")]],
    )
    .await;
    let app = batch_router(state);

    // Combined payload: * path, two indexes, mixed type, params string, stopIfEnoughMatches
    let resp = post_batch_search(
        &app,
        json!({
            "requests": [
                {
                    "indexName": "e2e_alpha",
                    "params": "query=laptop&hitsPerPage=1"
                },
                {
                    "indexName": "e2e_beta",
                    "query": "shirt"
                },
                {
                    "indexName": "e2e_alpha",
                    "type": "facet",
                    "facet": "category",
                    "facetQuery": "elec"
                }
            ],
            "strategy": "stopIfEnoughMatches"
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 3, "must return one result per request");

    // Request 0: e2e_alpha search via params string — should find >= 1 laptop hit
    // Since hitsPerPage=1 and there are 2 laptop docs, nbHits >= 1 satisfies threshold
    assert!(
        results[0]["nbHits"].as_u64().unwrap() >= 1,
        "first query should find laptops from e2e_alpha"
    );
    assert!(
        results[0].get("processed").is_none(),
        "executed query must NOT have processed field"
    );

    // Request 1: e2e_beta search — should be SKIPPED (first query satisfied threshold)
    assert_eq!(
        results[1]["processed"], false,
        "second query should be skipped"
    );
    assert_eq!(results[1]["hits"].as_array().unwrap().len(), 0);
    assert_eq!(
        results[1]["index"], "e2e_beta",
        "stub must include correct index name"
    );

    // Request 2: facet search — also SKIPPED
    assert_eq!(
        results[2]["processed"], false,
        "third query (facet) should be skipped"
    );
}
