use super::*;

/// Assert that a responseFields-filtered search response preserved the listed fields
/// and correctly excluded experiment-injected metadata (except abTestVariantID which
/// is non-excludable per Algolia contract).
fn assert_response_fields_excluded_experiment_metadata(
    body: &serde_json::Value,
    expected_excluded_field: &str,
    expected_variant_id: &str,
) {
    assert!(body.get("hits").is_some(), "hits must be preserved");
    assert!(body.get("nbHits").is_some(), "nbHits must be preserved");
    assert!(
        body.get(expected_excluded_field).is_none(),
        "{expected_excluded_field} must be omitted when not listed in responseFields"
    );
    assert!(
        body.get("abTestID").is_none(),
        "abTestID must be omitted when not listed in responseFields"
    );
    assert_eq!(
        body["abTestVariantID"], expected_variant_id,
        "abTestVariantID remains non-excludable"
    );
}

/// Ensure responseFields removes excludable top-level keys while preserving non-excludable A/B variant metadata.
#[tokio::test]
async fn search_response_fields_filter_excludes_ab_test_id_but_keeps_variant_id() {
    let tmp = TempDir::new().unwrap();
    let state = make_search_experiment_state(&tmp).await;
    let app = search_router(state);

    let resp = post_search(
        &app,
        "products",
        json!({
            "query": "shoe",
            "responseFields": ["hits", "nbHits"]
        }),
        Some("user-a"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert!(body.get("hits").is_some(), "hits must be preserved");
    assert!(body.get("nbHits").is_some(), "nbHits must be preserved");
    assert!(
        body.get("page").is_none(),
        "page must be removed when not listed in responseFields"
    );
    assert!(
        body.get("processingTimeMS").is_none(),
        "processingTimeMS must be removed when not listed in responseFields"
    );
    assert!(
        body.get("abTestID").is_none(),
        "abTestID must be removed when not listed in responseFields"
    );

    let variant_id = body["abTestVariantID"]
        .as_str()
        .expect("abTestVariantID is non-excludable and must be present");
    assert!(
        variant_id == "control" || variant_id == "variant",
        "abTestVariantID must be 'control' or 'variant', got: {variant_id}"
    );
}

/// Ensure responseFields can exclude indexUsed even when Mode B reroutes to a variant index.
#[tokio::test]
async fn mode_b_variant_response_fields_can_exclude_index_used() {
    let tmp = TempDir::new().unwrap();
    let state = make_search_experiment_state(&tmp).await;
    let experiment = state
        .experiment_store
        .as_ref()
        .unwrap()
        .get("exp-mode-b")
        .unwrap();
    let variant_token = find_user_token_for_arm(&experiment, "variant");
    let app = search_router(state);

    let resp = post_search(
        &app,
        "products_mode_b",
        json!({
            "query": "document",
            "responseFields": ["hits", "nbHits"]
        }),
        Some(&variant_token),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_response_fields_excluded_experiment_metadata(&body, "indexUsed", "variant");
}

/// Ensure responseFields can exclude interleavedTeams while retaining non-excludable variant metadata.
#[tokio::test]
async fn interleaving_response_fields_can_exclude_interleaved_teams() {
    let tmp = TempDir::new().unwrap();
    let state = make_search_experiment_state(&tmp).await;
    let app = search_router(state);

    let resp = post_search(
        &app,
        "products_interleave",
        json!({
            "query": "interleave",
            "responseFields": ["hits", "nbHits"]
        }),
        Some("user-a"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_response_fields_excluded_experiment_metadata(&body, "interleavedTeams", "interleaved");
}
