use super::*;
use crate::analytics::collector::AnalyticsCollector;
use crate::analytics::schema::InsightEvent;
use tempfile::TempDir;

fn test_analytics_config(temp_dir: &TempDir) -> AnalyticsConfig {
    AnalyticsConfig {
        enabled: true,
        data_dir: temp_dir.path().to_path_buf(),
        flush_interval_secs: 60,
        flush_size: 10_000,
        retention_days: 90,
    }
}

/// Construct a click `InsightEvent` test fixture for the given index and object ID, with no query ID, a default user token, and position of 1.
fn click_event(index: &str, object_id: &str) -> InsightEvent {
    InsightEvent {
        event_type: "click".to_string(),
        event_subtype: None,
        event_name: "Product Clicked".to_string(),
        index: index.to_string(),
        user_token: "user-1".to_string(),
        authenticated_user_token: None,
        query_id: None,
        object_ids: vec![object_id.to_string()],
        object_ids_alt: vec![],
        positions: Some(vec![1]),
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        value: None,
        currency: None,
        interleaving_team: None,
    }
}

/// Construct a click `InsightEvent` test fixture for the given index, object ID, and query ID, with a default user token and position of 1.
fn click_event_with_query_id(index: &str, object_id: &str, query_id: &str) -> InsightEvent {
    InsightEvent {
        event_type: "click".to_string(),
        event_subtype: None,
        event_name: "Product Clicked".to_string(),
        index: index.to_string(),
        user_token: "user-1".to_string(),
        authenticated_user_token: None,
        query_id: Some(query_id.to_string()),
        object_ids: vec![object_id.to_string()],
        object_ids_alt: vec![],
        positions: Some(vec![1]),
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        value: None,
        currency: None,
        interleaving_team: None,
    }
}

/// Construct a `SearchEvent` test fixture for the given index, query text, and optional query ID, with sensible defaults (1 hit, 5ms processing time, user-1 token).
fn search_event(
    index: &str,
    query: &str,
    query_id: Option<&str>,
) -> crate::analytics::schema::SearchEvent {
    crate::analytics::schema::SearchEvent {
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        query: query.to_string(),
        query_id: query_id.map(str::to_string),
        index_name: index.to_string(),
        nb_hits: 1,
        processing_time_ms: 5,
        user_token: Some("user-1".to_string()),
        user_ip: None,
        filters: None,
        facets: None,
        analytics_tags: None,
        page: 0,
        hits_per_page: 20,
        has_results: true,
        country: None,
        region: None,
        experiment_id: None,
        variant_id: None,
        assignment_method: None,
    }
}

// ── date_to_start_ms ──

#[test]
fn date_to_start_ms_valid() {
    let ms = date_to_start_ms("2024-01-15").unwrap();
    // 2024-01-15 00:00:00 UTC
    assert_eq!(ms, 1705276800000);
}

#[test]
fn date_to_start_ms_epoch() {
    let ms = date_to_start_ms("1970-01-01").unwrap();
    assert_eq!(ms, 0);
}

#[test]
fn date_to_start_ms_invalid() {
    assert!(date_to_start_ms("not-a-date").is_err());
}

#[test]
fn date_to_start_ms_wrong_format() {
    assert!(date_to_start_ms("01/15/2024").is_err());
}

// ── date_to_end_ms ──

#[test]
fn date_to_end_ms_valid() {
    let ms = date_to_end_ms("2024-01-15").unwrap();
    // 2024-01-15 23:59:59 UTC
    assert_eq!(ms, 1705363199000);
}

#[test]
fn date_to_end_ms_after_start() {
    let start = date_to_start_ms("2024-01-15").unwrap();
    let end = date_to_end_ms("2024-01-15").unwrap();
    assert!(end > start);
    // Difference should be 23h59m59s = 86399 seconds
    assert_eq!(end - start, 86399000);
}

#[test]
fn date_to_end_ms_invalid() {
    assert!(date_to_end_ms("garbage").is_err());
}

// ── ms_to_date_string ──

#[test]
fn ms_to_date_string_epoch() {
    assert_eq!(ms_to_date_string(0), "1970-01-01");
}

#[test]
fn ms_to_date_string_roundtrip() {
    let ms = date_to_start_ms("2024-06-15").unwrap();
    assert_eq!(ms_to_date_string(ms), "2024-06-15");
}

#[test]
fn ms_to_date_string_mid_day() {
    // 2024-01-15 12:00:00 UTC = start + 12h
    let ms = date_to_start_ms("2024-01-15").unwrap() + 12 * 3600 * 1000;
    assert_eq!(ms_to_date_string(ms), "2024-01-15");
}

// ── find_parquet_files ──

#[test]
fn find_parquet_files_nonexistent_dir() {
    let files = find_parquet_files(std::path::Path::new("/nonexistent/path")).unwrap();
    assert!(files.is_empty());
}

#[test]
fn find_parquet_files_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let files = find_parquet_files(dir.path()).unwrap();
    assert!(files.is_empty());
}

#[test]
fn find_parquet_files_finds_parquet() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data.parquet"), b"fake").unwrap();
    std::fs::write(dir.path().join("other.txt"), b"text").unwrap();
    let files = find_parquet_files(dir.path()).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].extension().unwrap() == "parquet");
}

#[test]
fn find_parquet_files_nested() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("nested.parquet"), b"fake").unwrap();
    std::fs::write(dir.path().join("top.parquet"), b"fake").unwrap();
    let files = find_parquet_files(dir.path()).unwrap();
    assert_eq!(files.len(), 2);
}

// ── arrow_value_at ──

#[test]
fn arrow_value_at_int64() {
    use arrow::array::Int64Array;
    let arr = Int64Array::from(vec![42, 99]);
    let val = arrow_value_at(&arr, 0);
    assert_eq!(val, serde_json::json!(42));
}

#[test]
fn arrow_value_at_float64() {
    use arrow::array::Float64Array;
    let arr = Float64Array::from(vec![2.5]);
    let val = arrow_value_at(&arr, 0);
    assert_eq!(val, serde_json::json!(2.5));
}

#[test]
fn arrow_value_at_string() {
    use arrow::array::StringArray;
    let arr = StringArray::from(vec!["hello"]);
    let val = arrow_value_at(&arr, 0);
    assert_eq!(val, serde_json::json!("hello"));
}

#[test]
fn arrow_value_at_bool() {
    use arrow::array::BooleanArray;
    let arr = BooleanArray::from(vec![true, false]);
    assert_eq!(arrow_value_at(&arr, 0), serde_json::json!(true));
    assert_eq!(arrow_value_at(&arr, 1), serde_json::json!(false));
}

#[test]
fn arrow_value_at_null() {
    use arrow::array::Int64Array;
    let arr = Int64Array::from(vec![Some(1), None]);
    assert_eq!(arrow_value_at(&arr, 1), serde_json::Value::Null);
}

#[test]
fn arrow_value_at_int32() {
    use arrow::array::Int32Array;
    let arr = Int32Array::from(vec![7]);
    assert_eq!(arrow_value_at(&arr, 0), serde_json::json!(7));
}

#[test]
fn arrow_value_at_uint64() {
    use arrow::array::UInt64Array;
    let arr = UInt64Array::from(vec![u64::MAX]);
    assert_eq!(arrow_value_at(&arr, 0), serde_json::json!(u64::MAX));
}

// ── batches_to_json ──

#[test]
fn batches_to_json_empty() {
    let rows = batches_to_json(&[]).unwrap();
    assert!(rows.is_empty());
}

/// Verify that `batches_to_json` correctly converts a single `RecordBatch` with string and integer columns into the expected JSON row objects.
#[test]
fn batches_to_json_single_batch() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["alice", "bob"])),
            std::sync::Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let rows = batches_to_json(&[batch]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], "alice");
    assert_eq!(rows[0]["count"], 10);
    assert_eq!(rows[1]["name"], "bob");
    assert_eq!(rows[1]["count"], 20);
}

/// Verify that `get_click_counts_for_objects` returns correct per-object click counts after flushing insight events, and omits object IDs with no recorded clicks.
#[tokio::test]
async fn get_click_counts_for_objects_returns_expected_counts() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_analytics_config(&temp_dir);
    let collector = AnalyticsCollector::new(config.clone());

    collector.record_insight(click_event("products", "nike-1"));
    collector.record_insight(click_event("products", "nike-1"));
    collector.record_insight(click_event("products", "nike-1"));
    collector.record_insight(click_event("products", "adidas-1"));
    collector.record_insight(click_event("products", "other-1"));
    collector.flush_insights();

    let engine = AnalyticsQueryEngine::new(config);
    let object_ids = vec![
        "nike-1".to_string(),
        "adidas-1".to_string(),
        "missing-1".to_string(),
    ];
    let counts = engine
        .get_click_counts_for_objects("products", &object_ids)
        .await
        .unwrap();

    assert_eq!(counts.get("nike-1"), Some(&3));
    assert_eq!(counts.get("adidas-1"), Some(&1));
    assert!(!counts.contains_key("missing-1"));
}

#[tokio::test]
async fn get_click_counts_for_objects_empty_data_returns_empty_map() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_analytics_config(&temp_dir);
    let engine = AnalyticsQueryEngine::new(config);

    let object_ids = vec!["nike-1".to_string(), "adidas-1".to_string()];
    let counts = engine
        .get_click_counts_for_objects("products", &object_ids)
        .await
        .unwrap();

    assert!(counts.is_empty());
}

/// Verify that `no_click_searches` safely handles query IDs containing SQL-special characters (e.g. single quotes) when building the `IN` clause, preventing SQL injection.
#[tokio::test]
async fn no_click_searches_escapes_query_ids_from_events_before_in_clause() {
    let temp_dir = TempDir::new().unwrap();
    let config = test_analytics_config(&temp_dir);
    let collector = AnalyticsCollector::new(config.clone());

    let malicious_qid = "qid'withquote";
    collector.record_search(search_event("products", "shoes", Some(malicious_qid)));
    collector.record_insight(click_event_with_query_id(
        "products",
        "shoe-1",
        malicious_qid,
    ));
    collector.flush_searches();
    collector.flush_insights();

    let engine = AnalyticsQueryEngine::new(config);
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let result = engine
        .no_click_searches("products", &today, &today, 10)
        .await
        .unwrap();

    let searches = result
        .get("searches")
        .and_then(serde_json::Value::as_array)
        .expect("searches array");
    assert!(
        searches.is_empty(),
        "clicked query should not be returned as no-click even with quote in query_id"
    );
}

/// TODO: Document aggregate_counts_by_query_id_merges_multiple_query_ids_into_same_query.
#[test]
fn aggregate_counts_by_query_id_merges_multiple_query_ids_into_same_query() {
    let rows = vec![
        serde_json::json!({"query_id": "qid-1", "click_count": 2}),
        serde_json::json!({"query_id": "qid-2", "click_count": 3}),
        serde_json::json!({"query_id": "qid-3", "click_count": 1}),
    ];
    let qid_to_query = HashMap::from([
        ("qid-1".to_string(), "boots".to_string()),
        ("qid-2".to_string(), "boots".to_string()),
        ("qid-3".to_string(), "hats".to_string()),
    ]);

    let aggregated = aggregate_counts_by_query_id(&rows, &qid_to_query, "click_count");

    assert_eq!(aggregated.get("boots"), Some(&5));
    assert_eq!(aggregated.get("hats"), Some(&1));
}

/// TODO: Document enrich_rows_with_click_metrics_adds_expected_fields.
#[test]
fn enrich_rows_with_click_metrics_adds_expected_fields() {
    let rows = vec![serde_json::json!({"search": "boots"})];
    let tracked = HashMap::from([("boots".to_string(), 4_i64)]);
    let clicks = HashMap::from([("boots".to_string(), 3_i64)]);
    let conversions = HashMap::from([("boots".to_string(), 1_i64)]);
    let position_sums = HashMap::from([("boots".to_string(), 13.0_f64)]);
    let position_counts = HashMap::from([("boots".to_string(), 4_i64)]);

    let enriched = enrich_rows_with_click_metrics(
        rows,
        &tracked,
        &clicks,
        &conversions,
        &position_sums,
        &position_counts,
    );

    let row = enriched.first().expect("enriched row");
    assert_eq!(row.get("clickCount"), Some(&serde_json::json!(3)));
    assert_eq!(row.get("trackedSearchCount"), Some(&serde_json::json!(4)));
    assert_eq!(row.get("conversionCount"), Some(&serde_json::json!(1)));
    assert_eq!(row.get("clickThroughRate"), Some(&serde_json::json!(0.75)));
    assert_eq!(row.get("conversionRate"), Some(&serde_json::json!(0.25)));
    assert_eq!(
        row.get("averageClickPosition"),
        Some(&serde_json::json!(3.3))
    );
}
