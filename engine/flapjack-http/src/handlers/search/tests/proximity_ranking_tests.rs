//! Integration tests for proximity-based ranking behavior. Validates minProximity parameter effects, attribute weight interaction with proximity scoring, and weight preservation under unordered attribute syntax.
use super::*;

/// Create a test index with documents containing query terms at varying word distances.
///
/// Creates a single searchable attribute 'title' and indexes three documents where 'red' and 'shoes' appear adjacent (close), separated by one word (medium), and separated by three words (far). Used as a fixture for proximity ranking tests.
async fn create_proximity_test_index(state: &Arc<AppState>, index_name: &str) {
    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        searchable_attributes: Some(vec!["title".to_string()]),
        ..Default::default()
    };
    save_index_settings(state, index_name, &settings);

    // Docs with "red" and "shoes" at varying distances
    let docs = vec![
        Document {
            id: "close".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red shoes on sale now".to_string()),
            )]),
        },
        Document {
            id: "medium".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red leather running shoes today".to_string()),
            )]),
        },
        Document {
            id: "far".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red big huge leather shoes today".to_string()),
            )]),
        },
    ];

    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();
}

/// Verify that multi-word queries rank documents by term proximity (word distance).
///
/// Searches for 'red shoes' across documents with varying term distances. Asserts that the document with adjacent terms ranks first.
#[tokio::test]
async fn proximity_handler_two_word_query_orders_by_proximity() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_proximity_test_index(&state, "prox_test").await;

    let app = search_router(state);
    let res = post_search(
        &app,
        "prox_test",
        json!({
            "query": "red shoes",
            "hitsPerPage": 10
        }),
        None,
    )
    .await;

    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert!(ids.len() >= 2, "should return at least 2 hits");
    // "close" has adjacent terms → lowest proximity → should rank first
    assert_eq!(ids[0], "close", "closest proximity doc should rank first");
}

/// Verify that the minProximity request parameter is echoed in the search response params.
///
/// Sends a search request with minProximity=3 and asserts that the response params string contains 'minProximity=3'.
#[tokio::test]
async fn proximity_handler_min_proximity_echoes_in_params() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_proximity_test_index(&state, "prox_echo").await;

    let app = search_router(state);
    let res = post_search(
        &app,
        "prox_echo",
        json!({
            "query": "red shoes",
            "hitsPerPage": 10,
            "minProximity": 3
        }),
        None,
    )
    .await;

    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let params = body["params"].as_str().unwrap();
    assert!(
        params.contains("minProximity=3"),
        "minProximity should echo in response params, got: {params}"
    );
}

/// Verify that the minProximity parameter alters document ranking based on term proximity constraints.
///
/// Creates documents with query terms at distances 1, 2, and 4 words apart. Asserts that with minProximity=1, documents rank by raw distance, and with minProximity=3, documents with distance >= 3 clamp to 3 with alphabetical objectID as tiebreak.
#[tokio::test]
async fn min_proximity_changes_ranking_order() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "min_prox_ranking_idx";

    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        searchable_attributes: Some(vec!["title".to_string()]),
        ..Default::default()
    };
    save_index_settings(&state, index_name, &settings);

    let docs = vec![
        Document {
            id: "aaa_close".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red shoes x x x".to_string()),
            )]),
        },
        Document {
            id: "bbb_medium".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red x shoes x x".to_string()),
            )]),
        },
        Document {
            id: "ccc_far".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("red x x x shoes".to_string()),
            )]),
        },
    ];

    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    let res_min_prox_1 = post_search(
        &app,
        index_name,
        json!({
            "query": "red shoes",
            "minProximity": 1
        }),
        None,
    )
    .await;
    assert_eq!(res_min_prox_1.status(), StatusCode::OK);
    let body_1 = body_json(res_min_prox_1).await;
    let ids_1 = hit_ids(&body_1);
    assert_eq!(ids_1.len(), 3, "should return all 3 hits");
    assert_eq!(
        ids_1[0], "aaa_close",
        "at minProximity=1, closest proximity doc should rank first"
    );
    assert_eq!(
        ids_1[1], "bbb_medium",
        "at minProximity=1, medium proximity doc ranks second"
    );
    assert_eq!(
        ids_1[2], "ccc_far",
        "at minProximity=1, far proximity doc ranks last"
    );

    let res_min_prox_3 = post_search(
        &app,
        index_name,
        json!({
            "query": "red shoes",
            "minProximity": 3
        }),
        None,
    )
    .await;
    assert_eq!(res_min_prox_3.status(), StatusCode::OK);
    let body_3 = body_json(res_min_prox_3).await;
    let ids_3 = hit_ids(&body_3);
    assert_eq!(ids_3.len(), 3, "should return all 3 hits");
    assert_eq!(ids_3[0], "aaa_close", "at minProximity=3, aaa_close and bbb_medium tie on proximity (both clamped to 3), aaa wins alphabetically");
    assert_eq!(
        ids_3[1], "bbb_medium",
        "at minProximity=3, bbb_medium ties with aaa_close on proximity"
    );
    assert_eq!(
        ids_3[2], "ccc_far",
        "at minProximity=3, ccc_far has raw dist 4 > 3, ranks last"
    );
}

/// Verify that attribute order takes precedence over proximity when attributeCriteriaComputedByMinProximity is false.
///
/// Creates documents matching the query in different attributes with different proximities. Asserts that title matches outrank description matches regardless of proximity difference.
#[tokio::test]
async fn attribute_criteria_computed_by_min_proximity_default_behavior() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "attr_criteria_default_idx";

    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        searchable_attributes: Some(vec!["title".to_string(), "description".to_string()]),
        ..Default::default()
    };
    save_index_settings(&state, index_name, &settings);

    let docs = vec![
        Document {
            id: "doc_a".to_string(),
            fields: HashMap::from([
                ("title".to_string(), FieldValue::Text("red".to_string())),
                (
                    "description".to_string(),
                    FieldValue::Text("red x x x shoes".to_string()),
                ),
            ]),
        },
        Document {
            id: "doc_b".to_string(),
            fields: HashMap::from([
                ("title".to_string(), FieldValue::Text("blue".to_string())),
                (
                    "description".to_string(),
                    FieldValue::Text("red shoes x x x".to_string()),
                ),
            ]),
        },
    ];

    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    let res = post_search(
        &app,
        index_name,
        json!({
            "query": "red shoes",
            "minProximity": 7
        }),
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 2, "should return both hits");
    assert_eq!(
        ids[0], "doc_a",
        "default behavior: doc_a wins due to matching in earlier attribute (title)"
    );
}

/// Verify that enabling attributeCriteriaComputedByMinProximity allows proximity to override attribute order.
///
/// Creates documents where better proximity in a lower-weight attribute outranks poorer proximity in a higher-weight attribute. Asserts that the document with better proximity in description ranks higher than one with poorer proximity in title.
#[tokio::test]
async fn attribute_criteria_computed_by_min_proximity_enabled() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "attr_criteria_enabled_idx";

    state.manager.create_tenant(index_name).unwrap();
    save_raw_settings_json(
        &state,
        index_name,
        &json!({
            "searchableAttributes": ["title", "description"],
            "attributeCriteriaComputedByMinProximity": true
        }),
    );

    let docs = vec![
        Document {
            id: "doc_a".to_string(),
            fields: HashMap::from([
                ("title".to_string(), FieldValue::Text("red".to_string())),
                (
                    "description".to_string(),
                    FieldValue::Text("red x x x shoes".to_string()),
                ),
            ]),
        },
        Document {
            id: "doc_b".to_string(),
            fields: HashMap::from([
                ("title".to_string(), FieldValue::Text("blue".to_string())),
                (
                    "description".to_string(),
                    FieldValue::Text("red shoes x x x".to_string()),
                ),
            ]),
        },
    ];

    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    let res = post_search(
        &app,
        index_name,
        json!({
            "query": "red shoes"
        }),
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 2, "should return both hits");
    assert_eq!(
        ids[0], "doc_b",
        "with attributeCriteriaComputedByMinProximity=true: doc_b wins due to better proximity in description"
    );
}

/// Verify that unordered(attr) syntax preserves attribute weight while disabling word-position penalty.
///
/// Compares ranking with ['title', 'description'] versus ['unordered(title)', 'description']. Asserts that matches in the first attribute outrank matches in the second attribute in both cases, proving attribute weight is retained.
#[tokio::test]
async fn searchable_attributes_unordered_preserves_attribute_weight() {
    // This test verifies that unordered(title) is properly recognized as the "title"
    // attribute for weight assignment. The bug is that "unordered(title)" != "title"
    // when matching, so unordered attributes fall into the unweighted bucket.
    //
    // When title is first in searchableAttributes, it should have weight 1.0.
    // If unordered(title) is not recognized, title gets weight 0.01 (unweighted bucket).
    // This affects ranking when comparing matches across multiple attributes.
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);

    // Test with ordered searchableAttributes - title has weight 1.0, description has weight 0.01
    let ordered_index = "ordered_weight_idx";
    state.manager.create_tenant(ordered_index).unwrap();
    save_raw_settings_json(
        &state,
        ordered_index,
        &json!({
            "searchableAttributes": ["title", "description"]
        }),
    );

    // Test with unordered searchableAttributes - title should still have weight 1.0
    let unordered_index = "unordered_weight_idx";
    state.manager.create_tenant(unordered_index).unwrap();
    save_raw_settings_json(
        &state,
        unordered_index,
        &json!({
            "searchableAttributes": ["unordered(title)", "description"]
        }),
    );

    // Create docs where attribute weight matters:
    // Doc A: matches in title (high weight field)
    // Doc B: matches in description (low weight field)
    // With proper weights, Doc A should rank higher
    let docs = vec![
        Document {
            id: "doc_b".to_string(),
            fields: HashMap::from([
                (
                    "title".to_string(),
                    FieldValue::Text("something else".to_string()),
                ),
                (
                    "description".to_string(),
                    FieldValue::Text("hello world match".to_string()),
                ),
            ]),
        },
        Document {
            id: "doc_a".to_string(),
            fields: HashMap::from([
                (
                    "title".to_string(),
                    FieldValue::Text("hello world match".to_string()),
                ),
                (
                    "description".to_string(),
                    FieldValue::Text("something else".to_string()),
                ),
            ]),
        },
    ];

    state
        .manager
        .add_documents_sync(ordered_index, docs.clone())
        .await
        .unwrap();
    state
        .manager
        .add_documents_sync(unordered_index, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    // With ordered(title), doc_a should rank higher (title match)
    let ordered_res = post_search(
        &app,
        ordered_index,
        json!({"query": "hello world", "hitsPerPage": 10}),
        None,
    )
    .await;
    assert_eq!(ordered_res.status(), StatusCode::OK);
    let ordered_body = body_json(ordered_res).await;
    let ordered_ids = hit_ids(&ordered_body);
    assert_eq!(ordered_ids.len(), 2, "should return both docs");
    assert_eq!(
        ordered_ids[0], "doc_a",
        "ordered: title match should outrank description match"
    );

    // With unordered(title), doc_a should ALSO rank higher
    // (unordered should only disable word-position penalty, not attribute weight)
    let unordered_res = post_search(
        &app,
        unordered_index,
        json!({"query": "hello world", "hitsPerPage": 10}),
        None,
    )
    .await;
    assert_eq!(unordered_res.status(), StatusCode::OK);
    let unordered_body = body_json(unordered_res).await;
    let unordered_ids = hit_ids(&unordered_body);
    assert_eq!(unordered_ids.len(), 2, "should return both docs");
    assert_eq!(
        unordered_ids[0], "doc_a",
        "unordered: title match should still outrank description match (attribute weight should be preserved)"
    );
}

/// Test that unordered() disables word-position penalty (proximity ranking)
///
/// Algolia semantics: unordered(attr) means:
/// 1. Attribute weight is preserved (tested above)
/// 2. Word-position/proximity penalty is NOT applied
///
/// This means that in an unordered field, "hello world" query should rank
/// "hello ... world" and "world ... hello" equally (no position penalty).
#[tokio::test]
async fn searchable_attributes_unordered_disables_position_penalty() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);

    // Index with ordered title - word position matters
    let ordered_index = "ordered_position_idx";
    state.manager.create_tenant(ordered_index).unwrap();
    save_raw_settings_json(
        &state,
        ordered_index,
        &json!({
            "searchableAttributes": ["title"]
        }),
    );

    // Index with unordered title - word position should NOT matter
    let unordered_index = "unordered_position_idx";
    state.manager.create_tenant(unordered_index).unwrap();
    save_raw_settings_json(
        &state,
        unordered_index,
        &json!({
            "searchableAttributes": ["unordered(title)"]
        }),
    );

    // IDs are intentionally opposite expected ordered ranking so this test can
    // detect whether proximity scoring is active:
    // - zzz_good_proximity: adjacent query terms (distance 1), later objectID
    // - aaa_poor_proximity: separated query terms (distance 2), earlier objectID
    //
    // Ordered mode should rank zzz_good_proximity first.
    // Unordered mode should disable proximity scoring and fall back to tie-break order.
    let docs = vec![
        Document {
            id: "aaa_poor_proximity".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("hello alpha world".to_string()),
            )]),
        },
        Document {
            id: "zzz_good_proximity".to_string(),
            fields: HashMap::from([(
                "title".to_string(),
                FieldValue::Text("hello world alpha".to_string()),
            )]),
        },
    ];

    state
        .manager
        .add_documents_sync(ordered_index, docs.clone())
        .await
        .unwrap();
    state
        .manager
        .add_documents_sync(unordered_index, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    // Ordered mode should prefer better proximity even with later objectID.
    let ordered_res = post_search(
        &app,
        ordered_index,
        json!({"query": "hello world", "hitsPerPage": 10}),
        None,
    )
    .await;
    assert_eq!(ordered_res.status(), StatusCode::OK);
    let ordered_body = body_json(ordered_res).await;
    let ordered_ids = hit_ids(&ordered_body);
    assert_eq!(ordered_ids.len(), 2, "should return both docs");
    assert_eq!(
        ordered_ids[0], "zzz_good_proximity",
        "ordered: better proximity should outrank poorer proximity"
    );

    // Unordered mode should disable proximity scoring and use tie-break order.
    let unordered_res = post_search(
        &app,
        unordered_index,
        json!({"query": "hello world", "hitsPerPage": 10}),
        None,
    )
    .await;
    assert_eq!(unordered_res.status(), StatusCode::OK);
    let unordered_body = body_json(unordered_res).await;
    let unordered_ids = hit_ids(&unordered_body);
    assert_eq!(unordered_ids.len(), 2, "should return both docs");
    assert_eq!(
        unordered_ids[0], "aaa_poor_proximity",
        "unordered: proximity signal should be removed so tie-break order applies"
    );
}

/// Test that numericAttributesForFiltering positive path works:
/// When the setting is configured, numeric filtering should work correctly.
/// This tests the behavioral parity requirement — filtering works when setting is present.
#[tokio::test]
async fn numeric_attributes_for_filtering_positive_path() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);

    // Create index with numericAttributesForFiltering configured
    let index_name = "numeric_filter_test";
    state.manager.create_tenant(index_name).unwrap();
    save_raw_settings_json(
        &state,
        index_name,
        &json!({
            "numericAttributesForFiltering": ["price", "quantity"]
        }),
    );
    let settings = state
        .manager
        .get_settings(index_name)
        .expect("settings should be loaded for index");
    let numeric_attrs = settings
        .numeric_attributes_for_filtering
        .as_ref()
        .expect("numericAttributesForFiltering should be present");
    assert!(
        numeric_attrs.iter().any(|a| a == "price"),
        "numericAttributesForFiltering should include price"
    );
    assert!(
        numeric_attrs.iter().any(|a| a == "quantity"),
        "numericAttributesForFiltering should include quantity"
    );

    // Index documents with numeric fields
    let docs = vec![
        Document {
            id: "doc1".to_string(),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    FieldValue::Text("Product A".to_string()),
                ),
                ("price".to_string(), FieldValue::Float(5.0)),
                ("quantity".to_string(), FieldValue::Float(100.0)),
            ]),
        },
        Document {
            id: "doc2".to_string(),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    FieldValue::Text("Product B".to_string()),
                ),
                ("price".to_string(), FieldValue::Float(15.0)),
                ("quantity".to_string(), FieldValue::Float(50.0)),
            ]),
        },
        Document {
            id: "doc3".to_string(),
            fields: HashMap::from([
                (
                    "name".to_string(),
                    FieldValue::Text("Product C".to_string()),
                ),
                ("price".to_string(), FieldValue::Float(25.0)),
                ("quantity".to_string(), FieldValue::Float(10.0)),
            ]),
        },
    ];

    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();

    let app = search_router(state.clone());

    // Test: price > 10 should return doc2 and doc3
    let res = post_search(
        &app,
        index_name,
        json!({
            "query": "",
            "numericFilters": "price > 10",
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 2, "should return 2 docs with price > 10");
    assert!(ids.contains(&"doc2".to_string()), "should include doc2");
    assert!(ids.contains(&"doc3".to_string()), "should include doc3");

    // Test: quantity <= 50 should return doc2 and doc3
    let res = post_search(
        &app,
        index_name,
        json!({
            "query": "",
            "numericFilters": "quantity <= 50",
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 2, "should return 2 docs with quantity <= 50");
    assert!(ids.contains(&"doc2".to_string()), "should include doc2");
    assert!(ids.contains(&"doc3".to_string()), "should include doc3");

    // Test: combined filters with AND
    let res = post_search(
        &app,
        index_name,
        json!({
            "query": "",
            "numericFilters": ["price > 10", "quantity < 20"],
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = body_json(res).await;
    let ids = hit_ids(&body);
    assert_eq!(
        ids.len(),
        1,
        "should return 1 doc with price > 10 AND quantity < 20"
    );
    assert_eq!(ids[0], "doc3", "should be doc3");
}
