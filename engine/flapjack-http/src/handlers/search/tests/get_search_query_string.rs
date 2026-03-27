//! Integration tests for GET search endpoint covering query parsing, response structure, filtering, faceting, and parameter echoing.
use super::*;

/// Verify GET search endpoint returns expected response structure with hits, nbHits, page, and params fields, matching POST endpoint behavior for identical queries.
#[tokio::test]
async fn get_search_basic_query_string() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "get_basic_idx",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop air"), ("category", "electronics")],
            vec![("title", "phone mini"), ("category", "electronics")],
        ],
    )
    .await;
    let app = search_router(state);

    let get_resp = get_search(&app, "/1/indexes/get_basic_idx?query=laptop&hitsPerPage=2").await;
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_body = body_json(get_resp).await;

    let post_resp = post_search(
        &app,
        "get_basic_idx",
        json!({"query": "laptop", "hitsPerPage": 2}),
        None,
    )
    .await;
    assert_eq!(post_resp.status(), StatusCode::OK);
    let post_body = body_json(post_resp).await;

    assert!(get_body.get("hits").is_some());
    assert!(get_body.get("nbHits").is_some());
    assert!(get_body.get("page").is_some());
    assert!(get_body.get("params").is_some());
    assert_eq!(get_body["hitsPerPage"], 2);
    assert_eq!(get_body["nbHits"], post_body["nbHits"]);
    assert_eq!(get_body["hits"].as_array().unwrap().len(), 2);
    assert_eq!(get_body["params"], post_body["params"]);
}

/// Verify category filters reduce results, facet aggregations compute correctly, and `attributesToRetrieve` restricts returned fields.
#[tokio::test]
async fn get_search_with_filters_and_facets() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "get_filter_idx",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop air"), ("category", "electronics")],
            vec![("title", "laptop tee"), ("category", "clothing")],
        ],
    )
    .await;
    let app = search_router(state);

    let uri = "/1/indexes/get_filter_idx?query=laptop&hitsPerPage=10&attributesToRetrieve=%5B%22title%22%5D&filters=category%3Aelectronics&facets=%5B%22category%22%5D";
    let resp = get_search(&app, uri).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["nbHits"], 2);
    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    for hit in hits {
        assert!(hit.get("title").is_some());
        assert!(hit.get("category").is_none());
    }
    assert_eq!(body["facets"]["category"]["electronics"], 2);
}

/// Verify omitted or empty query parameters return all documents in the index.
#[tokio::test]
async fn get_search_empty_query_returns_all() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "get_empty_idx",
        vec![
            vec![("title", "laptop pro")],
            vec![("title", "phone max")],
            vec![("title", "tablet mini")],
        ],
    )
    .await;
    let app = search_router(state);

    let missing_query = get_search(&app, "/1/indexes/get_empty_idx?hitsPerPage=10").await;
    assert_eq!(missing_query.status(), StatusCode::OK);
    let missing_body = body_json(missing_query).await;
    assert_eq!(missing_body["nbHits"], 3);

    let empty_query = get_search(&app, "/1/indexes/get_empty_idx?query=&hitsPerPage=10").await;
    assert_eq!(empty_query.status(), StatusCode::OK);
    let empty_body = body_json(empty_query).await;
    assert_eq!(empty_body["nbHits"], 3);
}

/// Verify the response `params` field echoes back the exact query string parameters received.
#[tokio::test]
async fn get_search_params_echo() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "get_echo_idx",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "phone max"), ("category", "electronics")],
        ],
    )
    .await;
    let app = search_router(state);

    let params = "query=laptop&hitsPerPage=2&filters=category%3Aelectronics";
    let uri = format!("/1/indexes/get_echo_idx?{params}");
    let resp = get_search(&app, &uri).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["params"].as_str(), Some(params));
}
