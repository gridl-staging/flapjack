//! Integration tests for relevancy strictness, which controls the balance between BM25 text relevance and custom ranking in search result ordering.
use super::*;

/// Set up a test index with three documents having conflicting text relevance and custom ranking (priority) to test strictness behavior.
async fn setup_relevancy_strictness_index(state: &Arc<AppState>, index_name: &str) {
    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        searchable_attributes: Some(vec!["title".to_string()]),
        custom_ranking: Some(vec!["desc(priority)".to_string()]),
        ..Default::default()
    };
    save_index_settings(state, index_name, &settings);

    let docs = vec![
        {
            let mut f = HashMap::new();
            f.insert("title".to_string(), FieldValue::Text("foo".into()));
            f.insert("priority".to_string(), FieldValue::Integer(1));
            Document {
                id: "doc_best_text".into(),
                fields: f,
            }
        },
        {
            let mut f = HashMap::new();
            f.insert("title".to_string(), FieldValue::Text("foo bar".into()));
            f.insert("priority".to_string(), FieldValue::Integer(50));
            Document {
                id: "doc_mid".into(),
                fields: f,
            }
        },
        {
            let mut f = HashMap::new();
            let long_tail = "lorem ipsum dolor sit amet ".repeat(4000);
            f.insert(
                "title".to_string(),
                FieldValue::Text(format!("foo {long_tail}")),
            );
            f.insert("priority".to_string(), FieldValue::Integer(100));
            Document {
                id: "doc_best_prio".into(),
                fields: f,
            }
        },
    ];
    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();
}

/// Verify that with strictness=0, custom ranking (priority) completely dominates result ordering, overriding BM25 text relevance.
#[tokio::test]
async fn relevancy_strictness_zero_custom_ranking_dominates() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "stage5a_strictness_zero_idx";
    setup_relevancy_strictness_index(&state, index_name).await;

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "foo",
            "relevancyStrictness": 0,
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids[0], "doc_best_prio",
        "strictness=0: highest priority doc must rank first (despite worst BM25)"
    );
    assert_eq!(
        ids[1], "doc_mid",
        "strictness=0: mid priority doc must rank second"
    );
    assert_eq!(
        ids[2], "doc_best_text",
        "strictness=0: lowest priority doc must rank last (despite best BM25)"
    );
}

/// Verify that with strictness=100, BM25 text relevance completely dominates result ordering, overriding custom ranking.
#[tokio::test]
async fn relevancy_strictness_hundred_textual_relevance_dominates() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "stage5a_strictness_hundred_idx";
    setup_relevancy_strictness_index(&state, index_name).await;

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "foo",
            "relevancyStrictness": 100,
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids = hit_ids(&body);
    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids[0], "doc_best_text",
        "strictness=100: best text match must rank first (despite lowest priority)"
    );
    assert_ne!(
        ids[0], "doc_best_prio",
        "strictness=100: worst text match must not rank first (despite highest priority)"
    );
}

/// Set up a test index with three documents having varying term frequencies and custom ranking to test intermediate strictness levels.
async fn setup_relevancy_strictness_intermediate_index(state: &Arc<AppState>, index_name: &str) {
    state.manager.create_tenant(index_name).unwrap();
    let settings = flapjack::index::settings::IndexSettings {
        searchable_attributes: Some(vec!["title".to_string()]),
        custom_ranking: Some(vec!["desc(priority)".to_string()]),
        ..Default::default()
    };
    save_index_settings(state, index_name, &settings);

    let docs = vec![
        {
            let mut f = HashMap::new();
            f.insert(
                "title".to_string(),
                FieldValue::Text("foo foo foo foo foo foo foo foo foo bar".into()),
            );
            f.insert("priority".to_string(), FieldValue::Integer(1));
            Document {
                id: "doc_high_tf".into(),
                fields: f,
            }
        },
        {
            let mut f = HashMap::new();
            f.insert(
                "title".to_string(),
                FieldValue::Text("foo foo bar bar bar bar bar bar bar bar".into()),
            );
            f.insert("priority".to_string(), FieldValue::Integer(50));
            Document {
                id: "doc_mid_tf".into(),
                fields: f,
            }
        },
        {
            let mut f = HashMap::new();
            f.insert(
                "title".to_string(),
                FieldValue::Text("foo bar baz qux dux lux mux nux pux rux".into()),
            );
            f.insert("priority".to_string(), FieldValue::Integer(100));
            Document {
                id: "doc_low_tf".into(),
                fields: f,
            }
        },
    ];
    state
        .manager
        .add_documents_sync(index_name, docs)
        .await
        .unwrap();
}

/// Verify that with strictness=95, results are filtered to include only documents exceeding a textual relevance threshold, regardless of custom ranking.
#[tokio::test]
async fn relevancy_strictness_intermediate_filters_low_relevance() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let index_name = "stage5a_strictness_intermediate_idx";
    setup_relevancy_strictness_intermediate_index(&state, index_name).await;

    let app = search_router(state);
    let resp = post_search(
        &app,
        index_name,
        json!({
            "query": "foo",
            "relevancyStrictness": 95,
            "hitsPerPage": 10
        }),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids = hit_ids(&body);
    assert_eq!(
        ids.len(),
        1,
        "strictness=95: only doc_high_tf should survive, got {} results: {:?}",
        ids.len(),
        ids
    );
    assert_eq!(
        ids[0], "doc_high_tf",
        "strictness=95: the highest BM25 doc must be the sole survivor"
    );
}
