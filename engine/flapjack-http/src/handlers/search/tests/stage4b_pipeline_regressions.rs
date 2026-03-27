use super::*;
use crate::test_helpers::body_json;
use axum::http::StatusCode;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn hybrid_fetch_window_uses_shared_formula() {
    assert_eq!(hybrid_fetch_window(20, 0), 200);
    assert_eq!(hybrid_fetch_window(80, 1), 210);
    assert_eq!(hybrid_fetch_window(100, 4), 550);
}

#[test]
fn measure_pipeline_elapsed_includes_closure_runtime() {
    let start = std::time::Instant::now();
    let ((left, right), elapsed) = measure_pipeline_elapsed(start, || {
        std::thread::sleep(Duration::from_millis(20));
        ("fallback", "transforms")
    });

    assert_eq!(left, "fallback");
    assert_eq!(right, "transforms");
    assert!(
        elapsed >= Duration::from_millis(20),
        "elapsed {elapsed:?} should include work done inside the measured phase"
    );
}

/// TODO: Document dense_geoloc_points.
fn dense_geoloc_points(doc_seed: usize, points_per_doc: usize) -> flapjack::types::FieldValue {
    flapjack::types::FieldValue::Array(
        (0..points_per_doc)
            .map(|offset| {
                let jitter = offset as f64 * 0.00001;
                let mut geoloc = HashMap::new();
                geoloc.insert(
                    "lat".to_string(),
                    flapjack::types::FieldValue::Float(40.7128 + doc_seed as f64 * 0.0001 + jitter),
                );
                geoloc.insert(
                    "lng".to_string(),
                    flapjack::types::FieldValue::Float(
                        -74.0060 + doc_seed as f64 * 0.0001 - jitter,
                    ),
                );
                flapjack::types::FieldValue::Object(geoloc)
            })
            .collect(),
    )
}

/// Regression test: build a transform-heavy geo search while keeping response
/// formatting cheap. If post-search transforms ever move out of the `search`
/// timing bucket again, `processingTimingsMS.total` will significantly exceed
/// `processingTimingsMS.search` and this ratio check will fail.
#[tokio::test]
async fn search_response_includes_processing_timings_envelope() {
    let tmp = TempDir::new().unwrap();
    let state = make_basic_search_state(&tmp);
    let idx = "timing_envelope_idx";
    state.manager.create_tenant(idx).unwrap();

    let doc_count = 400;
    let points_per_doc = 128;
    let docs: Vec<flapjack::types::Document> = (0..doc_count)
        .map(|doc_id| {
            let mut fields = HashMap::new();
            fields.insert(
                "title".to_string(),
                flapjack::types::FieldValue::Text("hello world".to_string()),
            );
            fields.insert(
                "_geoloc".to_string(),
                dense_geoloc_points(doc_id, points_per_doc),
            );

            flapjack::types::Document {
                id: format!("doc{doc_id}"),
                fields,
            }
        })
        .collect();
    state.manager.add_documents_sync(idx, docs).await.unwrap();

    let app = search_router(state);
    let resp = post_search(
        &app,
        idx,
        json!({
            "query": "hello",
            "hitsPerPage": 1,
            "aroundLatLng": "40.7128, -74.0060",
            "attributesToRetrieve": ["title"],
            "attributesToHighlight": []
        }),
        None,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["hits"].as_array().map(|hits| hits.len()), Some(1));

    assert!(
        body.get("processingTimeMS").is_some(),
        "response must include processingTimeMS"
    );
    assert!(
        body.get("serverTimeMS").is_some(),
        "response must include serverTimeMS"
    );

    let timings = body
        .get("processingTimingsMS")
        .expect("response must include processingTimingsMS");
    assert!(
        timings.get("queue").is_some(),
        "processingTimingsMS must include 'queue'"
    );
    assert!(
        timings.get("search").is_some(),
        "processingTimingsMS must include 'search'"
    );
    assert!(
        timings.get("highlight").is_some(),
        "processingTimingsMS must include 'highlight'"
    );
    assert!(
        timings.get("total").is_some(),
        "processingTimingsMS must include 'total'"
    );

    let queue_us = timings["queue"].as_u64().unwrap();
    let search_us = timings["search"].as_u64().unwrap();
    let highlight_us = timings["highlight"].as_u64().unwrap();
    let total_us = timings["total"].as_u64().unwrap();

    assert!(total_us >= search_us);
    assert!(
        search_us * 10 >= total_us * 8,
        "search bucket must retain transform-heavy geo time; queue={queue_us} search={search_us} highlight={highlight_us} total={total_us}"
    );
}
