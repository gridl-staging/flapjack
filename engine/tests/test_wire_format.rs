mod common;

use axum::http::{Method, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

const ADMIN_KEY: &str = "test-admin-key-wire-format";

/// Seed an index with two documents and wait for indexing to complete.
async fn seed_index(app: &axum::Router, index_name: &str) {
    common::seed_docs(
        app,
        index_name,
        ADMIN_KEY,
        vec![
            json!({"objectID": "wf-1", "title": "wire-format doc 1"}),
            json!({"objectID": "wf-2", "title": "wire-format doc 2"}),
        ],
    )
    .await;
}

// ── Item 1: createdAt wire format on key endpoints ──────────────────────────

mod created_at {
    use super::*;

    async fn create_key(app: &axum::Router) -> Value {
        let req = common::authed_request(
            Method::POST,
            "/1/keys",
            ADMIN_KEY,
            Some(json!({ "acl": ["search"], "description": "wire-format test key" })),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        common::body_json(resp).await
    }

    #[tokio::test]
    async fn post_key_created_at_is_iso8601() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let body = create_key(&app).await;
        common::assert_iso8601_value(&body["createdAt"], "POST /1/keys");
    }

    #[tokio::test]
    async fn get_key_created_at_is_epoch_integer() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let created = create_key(&app).await;
        let key_value = created["key"].as_str().unwrap();

        let req = common::authed_request(
            Method::GET,
            &format!("/1/keys/{key_value}"),
            ADMIN_KEY,
            None,
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = common::body_json(resp).await;
        common::assert_integer_value(&body["createdAt"], "GET /1/keys/{key}");
    }

    #[tokio::test]
    async fn list_keys_created_at_is_epoch_integer() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        create_key(&app).await;

        let req = common::authed_request(Method::GET, "/1/keys", ADMIN_KEY, None);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = common::body_json(resp).await;

        let keys = body["keys"].as_array().expect("keys should be an array");
        assert!(!keys.is_empty(), "should have at least one key");
        for key in keys {
            common::assert_integer_value(&key["createdAt"], "GET /1/keys list item");
        }
    }
}

// ── Item 2: GET search route alias parity ────────────────────────────────────

mod get_search {
    use super::*;

    #[tokio::test]
    async fn get_search_returns_same_shape_as_post() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let index_name = "get-search-shape-idx";
        seed_index(&app, index_name).await;

        // POST search
        let post_req = common::authed_request(
            Method::POST,
            &format!("/1/indexes/{index_name}/query"),
            ADMIN_KEY,
            Some(json!({ "query": "wire-format" })),
        );
        let post_resp = app.clone().oneshot(post_req).await.unwrap();
        assert_eq!(post_resp.status(), StatusCode::OK);
        let post_body = common::body_json(post_resp).await;

        // GET search
        let get_req = common::authed_request(
            Method::GET,
            &format!("/1/indexes/{index_name}/query?query=wire-format"),
            ADMIN_KEY,
            None,
        );
        let get_resp = app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let get_body = common::body_json(get_resp).await;

        // Both should share core top-level keys
        let core_keys = ["hits", "nbHits", "page", "hitsPerPage", "processingTimeMS"];
        for key in &core_keys {
            assert!(
                post_body.get(key).is_some(),
                "POST response missing core key '{key}'"
            );
            assert!(
                get_body.get(key).is_some(),
                "GET response missing core key '{key}'"
            );
        }

        // Same hit count
        assert_eq!(post_body["nbHits"], get_body["nbHits"]);
    }

    #[tokio::test]
    async fn get_search_accepts_query_string_params() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let index_name = "get-search-params-idx";
        seed_index(&app, index_name).await;

        let req = common::authed_request(
            Method::GET,
            &format!("/1/indexes/{index_name}/query?query=wire-format&hitsPerPage=1"),
            ADMIN_KEY,
            None,
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = common::body_json(resp).await;

        assert_eq!(body["hitsPerPage"], 1);
        let hits = body["hits"].as_array().expect("hits should be an array");
        assert!(hits.len() <= 1, "hitsPerPage=1 but got {} hits", hits.len());
    }

    #[tokio::test]
    async fn get_search_on_nonexistent_index_returns_404() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

        let req = common::authed_request(
            Method::GET,
            "/1/indexes/no-such-index/query?query=x",
            ADMIN_KEY,
            None,
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("application/json"),
            "expected application/json, got: {content_type}"
        );

        let body = common::body_json(resp).await;
        let obj = body.as_object().expect("response should be a JSON object");
        assert!(obj.contains_key("message"), "missing 'message' key: {body}");
        assert!(obj.contains_key("status"), "missing 'status' key: {body}");
        assert!(
            obj.get("message").and_then(|v| v.as_str()).is_some(),
            "message should be a JSON string, got: {body}"
        );
        assert_eq!(
            obj.len(),
            2,
            "error response should contain exactly {{message,status}}, got: {body}"
        );
        assert_eq!(obj["status"], 404);
    }
}

// ── Item 3: dataSize / fileSize in GET /1/indexes ────────────────────────────

mod index_sizes {
    use super::*;

    async fn list_indexes(app: &axum::Router) -> Value {
        let req = common::authed_request(Method::GET, "/1/indexes", ADMIN_KEY, None);
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        common::body_json(resp).await
    }

    fn find_index_item<'a>(items: &'a [Value], index_name: &str) -> &'a Value {
        items
            .iter()
            .find(|item| item["name"].as_str() == Some(index_name))
            .unwrap_or_else(|| panic!("{index_name} should appear in list"))
    }

    #[tokio::test]
    async fn list_indexes_data_size_and_file_size_are_integers() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let index_name = "size-int-idx";
        seed_index(&app, index_name).await;

        let body = list_indexes(&app).await;

        let items = body["items"].as_array().expect("items should be an array");
        assert!(!items.is_empty(), "should have at least one index");

        for item in items {
            let ds = &item["dataSize"];
            let fs = &item["fileSize"];
            assert!(ds.is_u64(), "dataSize should be a JSON integer, got: {ds}");
            assert!(fs.is_u64(), "fileSize should be a JSON integer, got: {fs}");
        }

        let seeded = find_index_item(items, index_name);
        assert!(
            seeded["dataSize"].as_u64().unwrap() > 0,
            "dataSize should be > 0 for populated index {index_name}"
        );
        assert!(
            seeded["fileSize"].as_u64().unwrap() > 0,
            "fileSize should be > 0 for populated index {index_name}"
        );
    }

    #[tokio::test]
    async fn list_indexes_file_size_gte_data_size() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));
        let index_name = "size-gte-idx";
        seed_index(&app, index_name).await;

        let body = list_indexes(&app).await;

        let items = body["items"].as_array().expect("items should be an array");
        for item in items {
            let data_size = item["dataSize"].as_u64().unwrap();
            let file_size = item["fileSize"].as_u64().unwrap();
            assert!(
                file_size >= data_size,
                "fileSize ({file_size}) should be >= dataSize ({data_size}) for index {}",
                item["name"]
            );
        }
    }

    #[tokio::test]
    async fn list_indexes_empty_index_has_zero_or_positive_sizes() {
        let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

        // Create an empty index (no documents)
        let create_req = common::authed_request(
            Method::POST,
            "/1/indexes",
            ADMIN_KEY,
            Some(json!({ "uid": "empty-size-idx" })),
        );
        let create_resp = app.clone().oneshot(create_req).await.unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);

        let body = list_indexes(&app).await;

        let items = body["items"].as_array().expect("items should be an array");
        let empty_idx = find_index_item(items, "empty-size-idx");

        let ds = &empty_idx["dataSize"];
        let fs = &empty_idx["fileSize"];
        assert!(ds.is_u64(), "dataSize should be a JSON integer, got: {ds}");
        assert!(fs.is_u64(), "fileSize should be a JSON integer, got: {fs}");
        // Empty index may have 0 or small positive size (Tantivy metadata files)
        // Just verify they're non-negative integers (which u64 guarantees)
    }
}
