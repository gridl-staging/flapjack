//! Integration tests for search API request validation, query-after-removal strategies, and rendering content merging from settings and rules.
use super::*;
use crate::dto::{MAX_HITS_PER_PAGE, MAX_QUERY_BYTES};

fn make_state(tmp: &TempDir) -> Arc<AppState> {
    let state = crate::test_helpers::TestStateBuilder::new(tmp).build_shared();
    state.manager.create_tenant("test_idx").unwrap();
    state
}

fn save_rules_json(state: &Arc<AppState>, index_name: &str, rules: &Value) {
    let dir = state.manager.base_path.join(index_name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("rules.json"),
        serde_json::to_string_pretty(rules).unwrap(),
    )
    .unwrap();
    state.manager.invalidate_rules_cache(index_name);
}

/// Reject search requests with query strings exceeding MAX_QUERY_BYTES with BAD_REQUEST status and appropriate error message.
#[tokio::test]
async fn search_rejects_query_over_512_bytes() {
    let tmp = TempDir::new().unwrap();
    let state = make_state(&tmp);
    let app = search_router(state);

    let long_query = "a".repeat(MAX_QUERY_BYTES + 1);
    let resp = post_search(&app, "test_idx", json!({"query": long_query}), None).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("Query exceeds maximum length"),
        "expected query length error, got: {msg}"
    );
}

#[tokio::test]
async fn search_accepts_query_at_512_bytes() {
    let tmp = TempDir::new().unwrap();
    let state = make_state(&tmp);
    let app = search_router(state);

    let ok_query = "a".repeat(MAX_QUERY_BYTES);
    let resp = post_search(&app, "test_idx", json!({"query": ok_query}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Reject search requests with hitsPerPage exceeding MAX_HITS_PER_PAGE with BAD_REQUEST status and appropriate error message.
#[tokio::test]
async fn search_rejects_hits_per_page_over_1000() {
    let tmp = TempDir::new().unwrap();
    let state = make_state(&tmp);
    let app = search_router(state);

    let resp = post_search(
        &app,
        "test_idx",
        json!({"query": "test", "hitsPerPage": MAX_HITS_PER_PAGE + 1}),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = body_json(resp).await;
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("hitsPerPage exceeds maximum"),
        "expected hitsPerPage error, got: {msg}"
    );
}

#[tokio::test]
async fn batch_search_rejects_query_over_512_bytes() {
    let tmp = TempDir::new().unwrap();
    let state = make_state(&tmp);
    let app = Router::new()
        .route("/1/indexes/:indexName/queries", post(batch_search))
        .with_state(state);

    let long_query = "a".repeat(MAX_QUERY_BYTES + 1);
    let resp = post_batch_search(
        &app,
        json!({"requests": [{"indexName": "test_idx", "query": long_query}]}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Stage 2: queryAfterRemoval tests ──

/// Return queryAfterRemoval with removed words wrapped in <em> tags when using lastWords removal strategy on no-result queries.
#[tokio::test]
async fn response_query_after_removal_last_words() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "qar_last_words_idx";

    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        remove_words_if_no_results: "lastWords".to_string(),
        ..Default::default()
    };
    save_index_settings(&state, index_name, &settings);

    // Only "laptop" is indexed — "laptop xyznonexistent" should get 0 hits,
    // then retry with "laptop" which gets 1 hit. The removed word
    // "xyznonexistent" should be wrapped in <em> tags.
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), FieldValue::Text("laptop".to_string()));
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "laptop xyznonexistent",
            "removeWordsIfNoResults": "lastWords"
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert!(
        body["nbHits"].as_u64().unwrap() > 0,
        "should find results after word removal"
    );
    assert_eq!(
        body["queryAfterRemoval"].as_str(),
        Some("laptop <em>xyznonexistent</em>"),
        "queryAfterRemoval must show removed words in <em> tags (lastWords)"
    );
    assert!(
        body.get("parsedQuery").is_none(),
        "parsedQuery must be absent when queryAfterRemoval is present"
    );
}

/// Return queryAfterRemoval with removed words wrapped in <em> tags when using firstWords removal strategy on no-result queries.
#[tokio::test]
async fn response_query_after_removal_first_words() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "qar_first_words_idx";

    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        remove_words_if_no_results: "firstWords".to_string(),
        ..Default::default()
    };
    save_index_settings(&state, index_name, &settings);

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), FieldValue::Text("laptop".to_string()));
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "xyznonexistent laptop",
            "removeWordsIfNoResults": "firstWords"
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert!(
        body["nbHits"].as_u64().unwrap() > 0,
        "should find results after word removal"
    );
    assert_eq!(
        body["queryAfterRemoval"].as_str(),
        Some("<em>xyznonexistent</em> laptop"),
        "queryAfterRemoval must show removed words in <em> tags (firstWords)"
    );
    assert!(
        body.get("parsedQuery").is_none(),
        "parsedQuery must be absent when queryAfterRemoval is present"
    );
}

/// Omit queryAfterRemoval from response when original query matches documents.
#[tokio::test]
async fn response_query_after_removal_absent_when_original_matches() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "qar_absent_idx";

    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        remove_words_if_no_results: "lastWords".to_string(),
        ..Default::default()
    };
    save_index_settings(&state, index_name, &settings);

    let mut fields = HashMap::new();
    fields.insert(
        "title".to_string(),
        FieldValue::Text("laptop computer".to_string()),
    );
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    let app = search_router(state);
    // Both words match, so no removal should happen
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "laptop",
            "removeWordsIfNoResults": "lastWords"
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert!(
        body["nbHits"].as_u64().unwrap() > 0,
        "original query should find results"
    );
    assert!(
        body.get("queryAfterRemoval").is_none(),
        "queryAfterRemoval must be absent when original query returns results"
    );
}

/// Echo renderingContent from index settings in the search response.
#[tokio::test]
async fn response_echoes_rendering_content_from_settings() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rendering_content_echo_idx";

    state.manager.create_tenant(index_name).unwrap();
    let rendering_content = serde_json::json!({
        "facetOrdering": {
            "facets": { "order": ["brand", "category"] },
            "values": {
                "brand": {
                    "order": ["Apple", "Samsung"],
                    "sortRemainingBy": "alpha",
                    "hide": ["Unknown"]
                }
            }
        }
    });
    save_raw_settings_json(
        &state,
        index_name,
        &serde_json::json!({
            "renderingContent": rendering_content
        }),
    );

    let mut fields = HashMap::new();
    fields.insert(
        "title".to_string(),
        FieldValue::Text("laptop computer".to_string()),
    );
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    let app = search_router(state);
    let resp = post_search(&app, index_name, json!({"query": "laptop"}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["renderingContent"], rendering_content);
}

/// Include redirect URLs from matched rules in renderingContent of the response.
#[tokio::test]
async fn response_includes_rule_redirect_in_rendering_content() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rendering_content_rule_redirect_idx";
    state.manager.create_tenant(index_name).unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "title".to_string(),
        FieldValue::Text("gaming laptop".to_string()),
    );
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "redirect-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": {
                    "renderingContent": {
                        "redirect": { "url": "https://example.com/support" }
                    }
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(&app, index_name, json!({"query": "laptop"}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(
        body["renderingContent"]["redirect"]["url"].as_str(),
        Some("https://example.com/support")
    );
}

/// Include banner content from matched rules in renderingContent of the response.
#[tokio::test]
async fn response_includes_rule_banners_in_rendering_content() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rendering_content_rule_banners_idx";
    state.manager.create_tenant(index_name).unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "title".to_string(),
        FieldValue::Text("laptop deals".to_string()),
    );
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    let banners = json!([{
        "image": {
            "urls": [{ "url": "https://example.com/banner.jpg" }],
            "title": "Sale!"
        },
        "link": { "url": "https://example.com/sale" }
    }]);

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "banner-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": {
                    "renderingContent": {
                        "widgets": {
                            "banners": banners
                        }
                    }
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(&app, index_name, json!({"query": "laptop"}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["renderingContent"]["widgets"]["banners"], banners);
}

/// Merge renderingContent from both index settings and matched rules in the response.
#[tokio::test]
async fn response_merges_settings_and_rule_rendering_content() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rendering_content_settings_rule_merge_idx";
    state.manager.create_tenant(index_name).unwrap();

    let facet_ordering = json!({
        "facets": { "order": ["brand", "category"] }
    });
    save_raw_settings_json(
        &state,
        index_name,
        &json!({
            "renderingContent": {
                "facetOrdering": facet_ordering
            }
        }),
    );

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), FieldValue::Text("laptop".to_string()));
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "redirect-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": {
                    "renderingContent": {
                        "redirect": { "url": "https://example.com/help" }
                    }
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(&app, index_name, json!({"query": "laptop"}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["renderingContent"]["facetOrdering"], facet_ordering);
    assert_eq!(
        body["renderingContent"]["redirect"]["url"].as_str(),
        Some("https://example.com/help")
    );
}

/// Apply renderingContent from query rewrite rules alongside the query rewrite.
#[tokio::test]
async fn response_rewrite_rule_also_applies_rendering_content() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rendering_content_rewrite_rule_idx";
    state.manager.create_tenant(index_name).unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "title".to_string(),
        FieldValue::Text("television".to_string()),
    );
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "rewrite-rule",
            "conditions": [{ "pattern": "tv", "anchoring": "is" }],
            "consequence": {
                "params": {
                    "query": "television",
                    "renderingContent": {
                        "redirect": { "url": "https://example.com/tv" }
                    }
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(&app, index_name, json!({"query": "tv"}), None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(
        body["parsedQuery"].as_str(),
        Some("television"),
        "query rewrite should still apply"
    );
    assert_eq!(
        body["renderingContent"]["redirect"]["url"].as_str(),
        Some("https://example.com/tv"),
        "renderingContent from the same rewrite rule should be present"
    );
    assert_eq!(
        body["appliedRules"],
        json!([{ "objectID": "rewrite-rule" }]),
        "rewrite rule should be recorded as applied"
    );
}

/// Apply geo filtering parameters from matched rules to override request geo parameters and affect ranking.
#[tokio::test]
async fn response_applies_rule_geo_overrides_to_geo_filtering() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "rule_geo_override_http_idx";
    state.manager.create_tenant(index_name).unwrap();

    let mut la_fields = HashMap::new();
    la_fields.insert("title".to_string(), FieldValue::Text("laptop".to_string()));
    let mut la_geoloc = HashMap::new();
    la_geoloc.insert("lat".to_string(), FieldValue::Float(34.0522));
    la_geoloc.insert("lng".to_string(), FieldValue::Float(-118.2437));
    la_fields.insert("_geoloc".to_string(), FieldValue::Object(la_geoloc));

    let mut ny_fields = HashMap::new();
    ny_fields.insert(
        "title".to_string(),
        FieldValue::Text("laptop laptop laptop laptop laptop".to_string()),
    );
    let mut ny_geoloc = HashMap::new();
    ny_geoloc.insert("lat".to_string(), FieldValue::Float(40.7128));
    ny_geoloc.insert("lng".to_string(), FieldValue::Float(-74.0060));
    ny_fields.insert("_geoloc".to_string(), FieldValue::Object(ny_geoloc));

    state
        .manager
        .add_documents_sync(
            index_name,
            vec![
                Document {
                    id: "la".to_string(),
                    fields: la_fields,
                },
                Document {
                    id: "ny".to_string(),
                    fields: ny_fields,
                },
            ],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "geo-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": {
                    "aroundLatLng": "34.0522, -118.2437",
                    "aroundRadius": 5000
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({ "query": "laptop", "hitsPerPage": 1 }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["nbHits"].as_u64(), Some(1));
    assert_eq!(body["hits"].as_array().map(|h| h.len()), Some(1));
    assert_eq!(body["hits"][0]["objectID"].as_str(), Some("la"));
    assert_eq!(body["appliedRules"], json!([{ "objectID": "geo-rule" }]));
}

/// Apply secured hitsPerPage cap from SecuredKeyRestrictions, preventing rules from exceeding the restriction.
#[tokio::test]
async fn response_secured_hits_per_page_cap_overrides_rule_hits_per_page() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "secured_hpp_rule_override_idx";
    state.manager.create_tenant(index_name).unwrap();

    let make_doc = |id: &str| {
        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldValue::Text(format!("laptop {}", id)),
        );
        Document {
            id: id.to_string(),
            fields,
        }
    };
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![make_doc("a"), make_doc("b"), make_doc("c")],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "hpp-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": { "hitsPerPage": 50 }
            }
        }]),
    );

    let app = search_router(state);
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/1/indexes/{index_name}/query"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"query": "laptop"}).to_string()))
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

    assert_eq!(body["hitsPerPage"].as_u64(), Some(1));
    assert_eq!(
        body["hits"].as_array().map(|hits| hits.len()),
        Some(1),
        "secured hitsPerPage cap must not be weakened by rule params.hitsPerPage"
    );
    assert_eq!(body["appliedRules"], json!([{ "objectID": "hpp-rule" }]));
}

/// Combine queryAfterRemoval and renderingContent from multiple sources (settings and rules) in the response.
#[tokio::test]
async fn response_combines_query_after_removal_and_rendering_content_sources() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "stage2_combined_idx";
    state.manager.create_tenant(index_name).unwrap();

    save_raw_settings_json(
        &state,
        index_name,
        &json!({
            "removeWordsIfNoResults": "lastWords",
            "renderingContent": {
                "facetOrdering": {
                    "facets": { "order": ["brand"] }
                }
            }
        }),
    );

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), FieldValue::Text("laptop".to_string()));
    state
        .manager
        .add_documents_sync(
            index_name,
            vec![Document {
                id: "1".to_string(),
                fields,
            }],
        )
        .await
        .unwrap();

    save_rules_json(
        &state,
        index_name,
        &json!([{
            "objectID": "redirect-rule",
            "conditions": [{ "pattern": "laptop", "anchoring": "contains" }],
            "consequence": {
                "params": {
                    "renderingContent": {
                        "redirect": { "url": "https://example.com/redirect" }
                    }
                }
            }
        }]),
    );

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "laptop xyznonexistent",
            "removeWordsIfNoResults": "lastWords"
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(
        body["queryAfterRemoval"].as_str(),
        Some("laptop <em>xyznonexistent</em>")
    );
    assert_eq!(
        body["renderingContent"]["facetOrdering"]["facets"]["order"],
        json!(["brand"])
    );
    assert_eq!(
        body["renderingContent"]["redirect"]["url"].as_str(),
        Some("https://example.com/redirect")
    );
}
