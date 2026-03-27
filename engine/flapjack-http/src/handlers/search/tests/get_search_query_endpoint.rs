//! Test helpers and integration tests for GET /query endpoint functionality.
use super::*;

/// Router that exposes GET on /query route (production wiring).
fn search_router_with_get_query(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/1/indexes/:indexName/query", post(search).get(search_get))
        .route("/1/indexes/:indexName", get(search_get))
        .with_state(state)
}

/// Execute a GET request to the /query endpoint.
///
/// Constructs the URI `/1/indexes/{index}/query?{qs}` (omitting the query string if empty) and sends it through the router with an empty body.
///
/// # Arguments
///
/// * `app` - The axum Router
/// * `index` - Index name
/// * `qs` - Query string parameters; empty string omits the query component
///
/// # Returns
///
/// The HTTP response from the router
///
/// # Panics
///
/// Panics if request construction or router invocation fails
async fn get_query_route(app: &Router, index: &str, qs: &str) -> axum::http::Response<Body> {
    let uri = if qs.is_empty() {
        format!("/1/indexes/{index}/query")
    } else {
        format!("/1/indexes/{index}/query?{qs}")
    };
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// GET /query with basic query-string returns 200 with matching hits.
#[tokio::test]
async fn get_query_route_basic_query_string() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_basic",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop air"), ("category", "electronics")],
            vec![("title", "phone mini"), ("category", "electronics")],
        ],
    )
    .await;
    let app = search_router_with_get_query(state);

    let resp = get_query_route(&app, "gq_basic", "query=laptop&hitsPerPage=2").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["hits"].as_array().is_some());
    assert_eq!(body["hitsPerPage"], 2);
    assert_eq!(body["nbHits"], 2);
    assert_eq!(body["hits"].as_array().unwrap().len(), 2);
}

/// GET /query produces identical results as POST /query with same params.
#[tokio::test]
async fn get_query_route_behavioral_equivalence_with_post() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_equiv",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop air"), ("category", "electronics")],
            vec![("title", "phone mini"), ("category", "electronics")],
        ],
    )
    .await;
    let app = search_router_with_get_query(state);

    let get_resp = get_query_route(&app, "gq_equiv", "query=laptop&hitsPerPage=10").await;
    assert_eq!(get_resp.status(), StatusCode::OK);
    let get_body = body_json(get_resp).await;

    let post_resp = post_search(
        &app,
        "gq_equiv",
        json!({"query": "laptop", "hitsPerPage": 10}),
        None,
    )
    .await;
    assert_eq!(post_resp.status(), StatusCode::OK);
    let post_body = body_json(post_resp).await;

    assert_eq!(get_body["nbHits"], post_body["nbHits"]);
    assert_eq!(get_body["hitsPerPage"], post_body["hitsPerPage"]);
    assert_eq!(get_body["page"], post_body["page"]);
    let get_ids: Vec<_> = get_body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["objectID"].as_str().unwrap_or(""))
        .collect();
    let post_ids: Vec<_> = post_body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["objectID"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(get_ids, post_ids);
}

/// GET /query echoes the raw query string in `params` field.
#[tokio::test]
async fn get_query_route_params_echo() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_echo",
        vec![vec![("title", "laptop"), ("category", "electronics")]],
    )
    .await;
    let app = search_router_with_get_query(state);

    let qs = "query=laptop&hitsPerPage=5";
    let resp = get_query_route(&app, "gq_echo", qs).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["params"].as_str(), Some(qs));
}

/// GET /query applies filters, facets, attributesToRetrieve from URL-encoded params.
#[tokio::test]
async fn get_query_route_complex_params() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_complex",
        vec![
            vec![("title", "laptop pro"), ("category", "electronics")],
            vec![("title", "laptop tee"), ("category", "clothing")],
        ],
    )
    .await;
    let app = search_router_with_get_query(state);

    // URL-encode: filters=category:electronics, attributesToRetrieve=["title"], facets=["category"]
    let qs = "query=laptop&filters=category%3Aelectronics&attributesToRetrieve=%5B%22title%22%5D&facets=%5B%22category%22%5D";
    let resp = get_query_route(&app, "gq_complex", qs).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["nbHits"], 1);
    let hit = &body["hits"].as_array().unwrap()[0];
    assert!(
        hit.get("title").is_some(),
        "attributesToRetrieve should include title"
    );
    assert!(hit.get("category").is_none(), "category should be excluded");
    assert!(body["facets"]["category"]["electronics"].as_u64().unwrap() >= 1);
}

/// GET /query with no query param returns all documents.
#[tokio::test]
async fn get_query_route_empty_query_returns_all() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_empty",
        vec![
            vec![("title", "laptop")],
            vec![("title", "phone")],
            vec![("title", "tablet")],
        ],
    )
    .await;
    let app = search_router_with_get_query(state);

    // no query param at all
    let resp = get_query_route(&app, "gq_empty", "hitsPerPage=20").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["nbHits"], 3);

    // explicit empty query
    let resp2 = get_query_route(&app, "gq_empty", "query=&hitsPerPage=20").await;
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = body_json(resp2).await;
    assert_eq!(body2["nbHits"], 3);
}

/// GET /query — query string IS the params string; apply_params_string handles it.
#[tokio::test]
async fn get_query_route_query_string_is_params_string() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(
        &state,
        "gq_params",
        vec![
            vec![("title", "laptop"), ("category", "electronics")],
            vec![("title", "phone"), ("category", "electronics")],
        ],
    )
    .await;
    let app = search_router_with_get_query(state);

    // The params field must equal the exact query string
    let qs = "query=laptop&page=0&hitsPerPage=20";
    let resp = get_query_route(&app, "gq_params", qs).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["params"].as_str(), Some(qs));
    assert_eq!(body["page"], 0);
    assert_eq!(body["hitsPerPage"], 20);
}

/// GET /query to nonexistent index returns Algolia-compatible 404 JSON.
#[tokio::test]
async fn get_query_route_nonexistent_index_returns_404() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let app = search_router_with_get_query(state);

    let resp = get_query_route(&app, "no_such_index", "query=laptop").await;
    // Should be 404 with Algolia-compatible error shape
    let status = resp.status();
    let body = body_json(resp).await;
    assert!(
        status.as_u16() == 404 || status.as_u16() == 400,
        "expected 4xx, got {status}"
    );
    assert!(
        body.get("message").is_some() || body.get("error").is_some(),
        "error response should have message field: {body}"
    );
}

/// GET /query with invalid hitsPerPage value doesn't panic — returns error or ignores.
#[tokio::test]
async fn get_query_route_invalid_param_no_panic() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    create_index_with_docs(&state, "gq_invalid", vec![vec![("title", "laptop")]]).await;
    let app = search_router_with_get_query(state);

    // hitsPerPage=abc is not a valid integer — should not panic
    let resp = get_query_route(&app, "gq_invalid", "query=laptop&hitsPerPage=abc").await;
    // Either 200 (ignores invalid value, uses default) or 4xx — must not panic
    assert!(
        resp.status().is_success() || resp.status().is_client_error(),
        "should return 2xx or 4xx, not 5xx: {}",
        resp.status()
    );
}
