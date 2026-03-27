//! Analytics handler tests.
use super::*;
use crate::test_helpers::body_json;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    routing::get,
    Router,
};
use flapjack::analytics::{
    schema::{InsightEvent, SearchEvent},
    AnalyticsCollector, AnalyticsConfig,
};
use tempfile::TempDir;
use tower::ServiceExt;

fn test_analytics_config(tmp: &TempDir) -> AnalyticsConfig {
    AnalyticsConfig {
        enabled: true,
        data_dir: tmp.path().join("analytics"),
        flush_interval_secs: 3600,
        flush_size: 10_000,
        retention_days: 90,
    }
}

/// TODO: Document make_search.
fn make_search(query: &str, index: &str, query_id: &str) -> SearchEvent {
    SearchEvent {
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        query: query.to_string(),
        query_id: Some(query_id.to_string()),
        index_name: index.to_string(),
        nb_hits: 10,
        processing_time_ms: 5,
        user_token: Some("user_1".to_string()),
        user_ip: Some("127.0.0.1".to_string()),
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

/// TODO: Document make_conversion.
fn make_conversion(query_id: &str, index: &str, subtype: &str) -> InsightEvent {
    InsightEvent {
        event_type: "conversion".to_string(),
        event_subtype: Some(subtype.to_string()),
        event_name: "Conversion".to_string(),
        index: index.to_string(),
        user_token: "user_1".to_string(),
        authenticated_user_token: None,
        query_id: Some(query_id.to_string()),
        object_ids: vec!["obj1".to_string()],
        object_ids_alt: vec![],
        positions: None,
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        value: Some(10.0),
        currency: Some("USD".to_string()),
        interleaving_team: None,
    }
}

/// Verify the `/2/conversions/addToCartRate` endpoint uses `addToCartCount` (not `conversionCount`) in both top-level and per-date entries, and computes rate as addToCartCount / trackedSearchCount.
#[tokio::test]
async fn add_to_cart_rate_response_uses_add_to_cart_count_field() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let qid_a = "a".repeat(32);
    let qid_b = "b".repeat(32);

    collector.record_search(make_search("iphone", "products", &qid_a));
    collector.record_search(make_search("iphone", "products", &qid_b));
    collector.record_insight(make_conversion(&qid_a, "products", "addToCart"));
    collector.record_insight(make_conversion(&qid_b, "products", "purchase"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/addToCartRate", get(get_add_to_cart_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/addToCartRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert_eq!(body["addToCartCount"], 1);
    assert_eq!(body["trackedSearchCount"], 2);
    assert!((body["rate"].as_f64().unwrap() - 0.5).abs() < 0.001);
    assert!(body.get("conversionCount").is_none());
    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 1);
    assert_eq!(dates[0]["addToCartCount"], 1);
    assert_eq!(dates[0]["trackedSearchCount"], 2);
    assert!(dates[0].get("conversionCount").is_none());
}

/// Verify the `/2/conversions/purchaseRate` endpoint uses `purchaseCount` (not `conversionCount`) in both top-level and per-date entries, matching the Algolia contract.
#[tokio::test]
async fn purchase_rate_response_uses_purchase_count_field() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let qid_a = "a".repeat(32);
    let qid_b = "b".repeat(32);

    collector.record_search(make_search("iphone", "products", &qid_a));
    collector.record_search(make_search("iphone", "products", &qid_b));
    collector.record_insight(make_conversion(&qid_a, "products", "addToCart"));
    collector.record_insight(make_conversion(&qid_b, "products", "purchase"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/purchaseRate", get(get_purchase_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/purchaseRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert_eq!(body["purchaseCount"], 1);
    assert_eq!(body["trackedSearchCount"], 2);
    assert!((body["rate"].as_f64().unwrap() - 0.5).abs() < 0.001);
    assert!(body.get("conversionCount").is_none());
    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 1);
    assert_eq!(dates[0]["purchaseCount"], 1);
    assert_eq!(dates[0]["trackedSearchCount"], 2);
    assert!(dates[0].get("conversionCount").is_none());
}

/// Verify the add-to-cart rate endpoint returns `null` for `rate`, zero for `trackedSearchCount`, and zero for `addToCartCount` when no search events exist.
#[tokio::test]
async fn add_to_cart_rate_returns_null_rate_when_no_tracked_searches() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let app = Router::new()
        .route("/2/conversions/addToCartRate", get(get_add_to_cart_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/addToCartRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert!(body["rate"].is_null());
    assert_eq!(body["trackedSearchCount"], 0);
    assert_eq!(body["addToCartCount"], 0);
}

/// Verify the `/2/conversions/revenue` endpoint returns a `currencies` object keyed by currency code, each containing `currency` and `revenue` fields, with a per-day `dates` array mirroring the same structure.
#[tokio::test]
async fn revenue_handler_returns_currency_map_structure() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let qid = "a".repeat(32);

    collector.record_search(make_search("iphone", "products", &qid));
    collector.record_insight(make_conversion(&qid, "products", "purchase"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/revenue", get(get_revenue))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/revenue?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let currencies = body["currencies"].as_object().unwrap();
    assert!(currencies.contains_key("USD"), "make_conversion uses USD");
    assert_eq!(currencies["USD"]["currency"], "USD");
    assert!(currencies["USD"]["revenue"].as_f64().unwrap() > 0.0);

    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 1);
    let day_currencies = dates[0]["currencies"].as_object().unwrap();
    assert!(day_currencies.contains_key("USD"));
}

/// Verify the revenue endpoint returns an empty `currencies` object and empty `dates` array when no purchase events exist.
#[tokio::test]
async fn revenue_handler_empty_returns_empty_structure() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let app = Router::new()
        .route("/2/conversions/revenue", get(get_revenue))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/revenue?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let currencies = body["currencies"].as_object().unwrap();
    assert!(currencies.is_empty());
    let dates = body["dates"].as_array().unwrap();
    assert!(dates.is_empty());
}

/// Verify the `/2/countries` endpoint does not include a `total` field in the response, matching the Algolia contract.
#[tokio::test]
async fn countries_handler_omits_total_field() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let qid = "a".repeat(32);

    let mut search = make_search("iphone", "products", &qid);
    search.country = Some("US".to_string());
    collector.record_search(search);
    collector.flush_all();

    let app = Router::new()
        .route("/2/countries", get(get_countries))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/countries?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let countries = body["countries"].as_array().unwrap();
    assert_eq!(countries.len(), 1);
    assert_eq!(countries[0]["country"], "US");
    assert_eq!(countries[0]["count"], 1);
    // Must NOT have "total" field — Algolia doesn't include it
    assert!(
        body.get("total").is_none(),
        "countries endpoint should not include 'total' field"
    );
}

// ── Helpers for multi-day and multi-type tests ──

fn make_search_at(query: &str, index: &str, query_id: &str, timestamp_ms: i64) -> SearchEvent {
    SearchEvent {
        timestamp_ms,
        ..make_search(query, index, query_id)
    }
}

fn make_conversion_at(
    query_id: &str,
    index: &str,
    subtype: &str,
    timestamp_ms: i64,
) -> InsightEvent {
    InsightEvent {
        timestamp: Some(timestamp_ms),
        ..make_conversion(query_id, index, subtype)
    }
}

fn make_conversion_with_currency(
    query_id: &str,
    index: &str,
    value: f64,
    currency: &str,
    timestamp_ms: i64,
) -> InsightEvent {
    InsightEvent {
        value: Some(value),
        currency: Some(currency.to_string()),
        timestamp: Some(timestamp_ms),
        ..make_conversion(query_id, index, "purchase")
    }
}

/// Create a test `InsightEvent` of type "click" with position 3, linked to a query by ID.
///
/// # Arguments
///
/// * `query_id` - The query ID to associate the click with.
/// * `index` - The index name.
///
/// # Returns
///
/// An `InsightEvent` with one object ID and a single click position at index 3.
fn make_click(query_id: &str, index: &str) -> InsightEvent {
    InsightEvent {
        event_type: "click".to_string(),
        event_subtype: None,
        event_name: "Click".to_string(),
        index: index.to_string(),
        user_token: "user_1".to_string(),
        authenticated_user_token: None,
        query_id: Some(query_id.to_string()),
        object_ids: vec!["obj1".to_string()],
        object_ids_alt: vec![],
        positions: Some(vec![3]),
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        value: None,
        currency: None,
        interleaving_team: None,
    }
}

fn yesterday_date() -> String {
    (chrono::Utc::now() - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string()
}

fn yesterday_ms() -> i64 {
    (chrono::Utc::now() - chrono::Duration::days(1)).timestamp_millis()
}

fn today_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ── B1: addToCartRate multi-day + date format ──

/// Verify the add-to-cart rate endpoint returns per-day breakdowns across a two-day span with correct `addToCartCount` and `trackedSearchCount` fields in YYYY-MM-DD date format.
#[tokio::test]
async fn add_to_cart_rate_multi_day_dates_array() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let yesterday = yesterday_date();

    // Day 1 (yesterday): 1 search, 1 addToCart
    let qid_y = "y".repeat(32);
    collector.record_search(make_search_at("iphone", "products", &qid_y, yesterday_ms()));
    collector.record_insight(make_conversion_at(
        &qid_y,
        "products",
        "addToCart",
        yesterday_ms(),
    ));

    // Day 2 (today): 2 searches, 1 addToCart
    let qid_t1 = "t".repeat(32);
    let qid_t2 = "u".repeat(32);
    collector.record_search(make_search_at("iphone", "products", &qid_t1, today_ms()));
    collector.record_search(make_search_at("iphone", "products", &qid_t2, today_ms()));
    collector.record_insight(make_conversion_at(
        &qid_t1,
        "products",
        "addToCart",
        today_ms(),
    ));

    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/addToCartRate", get(get_add_to_cart_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/addToCartRate?index=products&startDate={yesterday}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    // Top-level totals: 3 tracked searches, 2 addToCart
    assert_eq!(body["trackedSearchCount"], 3);
    assert_eq!(body["addToCartCount"], 2);

    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 2, "should have entries for both days");

    // Each date entry has correct fields
    for entry in dates {
        assert!(entry.get("date").is_some());
        assert!(entry.get("rate").is_some());
        assert!(entry.get("addToCartCount").is_some());
        assert!(entry.get("trackedSearchCount").is_some());

        // Date format must be YYYY-MM-DD (not ISO timestamp)
        let date_str = entry["date"].as_str().unwrap();
        assert_eq!(
            date_str.len(),
            10,
            "date should be YYYY-MM-DD format: {date_str}"
        );
        assert!(
            chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").is_ok(),
            "date should parse as YYYY-MM-DD: {date_str}"
        );
    }
}

// ── B2: purchaseRate multi-day ──

/// Verify the purchase rate endpoint returns per-day breakdowns across a two-day span, using `purchaseCount` (never `conversionCount`) in both top-level and per-date entries.
#[tokio::test]
async fn purchase_rate_multi_day_dates_array() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let yesterday = yesterday_date();

    // Day 1: 1 search, 1 purchase
    let qid_y = "y".repeat(32);
    collector.record_search(make_search_at("laptop", "products", &qid_y, yesterday_ms()));
    collector.record_insight(make_conversion_at(
        &qid_y,
        "products",
        "purchase",
        yesterday_ms(),
    ));

    // Day 2: 2 searches, 1 purchase
    let qid_t1 = "t".repeat(32);
    let qid_t2 = "u".repeat(32);
    collector.record_search(make_search_at("laptop", "products", &qid_t1, today_ms()));
    collector.record_search(make_search_at("laptop", "products", &qid_t2, today_ms()));
    collector.record_insight(make_conversion_at(
        &qid_t1,
        "products",
        "purchase",
        today_ms(),
    ));

    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/purchaseRate", get(get_purchase_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/purchaseRate?index=products&startDate={yesterday}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert_eq!(body["trackedSearchCount"], 3);
    assert_eq!(body["purchaseCount"], 2);
    // Must be purchaseCount, NOT conversionCount
    assert!(
        body.get("conversionCount").is_none(),
        "purchaseRate must not have conversionCount"
    );

    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 2);
    for entry in dates {
        assert!(entry.get("purchaseCount").is_some());
        assert!(entry.get("trackedSearchCount").is_some());
        assert!(
            entry.get("conversionCount").is_none(),
            "dates entries must use purchaseCount"
        );
    }
}

// ── B3: revenue multi-currency ──

/// Verify the revenue endpoint returns independent currency entries for USD and EUR with correct numeric revenue values when purchases use different currencies.
#[tokio::test]
async fn revenue_multi_currency_usd_and_eur() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let qid_a = "a".repeat(32);
    let qid_b = "b".repeat(32);

    collector.record_search(make_search("iphone", "products", &qid_a));
    collector.record_search(make_search("iphone", "products", &qid_b));
    collector.record_insight(make_conversion_with_currency(
        &qid_a,
        "products",
        29.99,
        "USD",
        today_ms(),
    ));
    collector.record_insight(make_conversion_with_currency(
        &qid_b,
        "products",
        25.50,
        "EUR",
        today_ms(),
    ));

    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/revenue", get(get_revenue))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/revenue?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let currencies = body["currencies"].as_object().unwrap();

    // Both currencies present with independent totals
    assert!(currencies.contains_key("USD"), "should have USD key");
    assert!(currencies.contains_key("EUR"), "should have EUR key");
    assert_eq!(currencies["USD"]["currency"].as_str().unwrap(), "USD");
    assert_eq!(currencies["EUR"]["currency"].as_str().unwrap(), "EUR");

    // Revenue is numeric float (not string)
    let usd_rev = currencies["USD"]["revenue"].as_f64();
    assert!(usd_rev.is_some(), "revenue should be numeric float");
    assert!((usd_rev.unwrap() - 29.99).abs() < 0.01);

    let eur_rev = currencies["EUR"]["revenue"].as_f64();
    assert!(eur_rev.is_some(), "revenue should be numeric float");
    assert!((eur_rev.unwrap() - 25.50).abs() < 0.01);
}

/// Verify the revenue endpoint returns separate date entries for a two-day span, each with per-currency breakdown using YYYY-MM-DD date format.
#[tokio::test]
async fn revenue_multi_day_per_currency_breakdown() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let yesterday = yesterday_date();

    let qid_a = "a".repeat(32);
    let qid_b = "b".repeat(32);

    collector.record_search(make_search_at("iphone", "products", &qid_a, yesterday_ms()));
    collector.record_search(make_search_at("iphone", "products", &qid_b, today_ms()));
    collector.record_insight(make_conversion_with_currency(
        &qid_a,
        "products",
        10.0,
        "USD",
        yesterday_ms(),
    ));
    collector.record_insight(make_conversion_with_currency(
        &qid_b,
        "products",
        20.0,
        "USD",
        today_ms(),
    ));

    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/revenue", get(get_revenue))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/revenue?index=products&startDate={yesterday}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 2, "should have entries for both days");

    for entry in dates {
        let date_str = entry["date"].as_str().unwrap();
        assert_eq!(date_str.len(), 10, "date should be YYYY-MM-DD");
        let day_currencies = entry["currencies"].as_object().unwrap();
        assert!(
            day_currencies.contains_key("USD"),
            "each day should have USD"
        );
        let day_usd = &day_currencies["USD"];
        assert!(
            day_usd["revenue"].as_f64().is_some(),
            "revenue should be numeric"
        );
        assert_eq!(day_usd["currency"].as_str().unwrap(), "USD");
    }
}

// ── B4: countries multi-country ──

/// Verify the `/2/countries` endpoint returns multiple countries ordered by search count descending, with each entry containing exactly two fields (`country`, `count`) and no top-level `total` field.
#[tokio::test]
async fn countries_multi_country_ordered_by_count_desc() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // 3 searches from US, 1 from DE
    for i in 0..3 {
        let qid = "a".repeat(31) + &i.to_string();
        let mut search = make_search("iphone", "products", &qid);
        search.country = Some("US".to_string());
        collector.record_search(search);
    }
    let qid_de = "d".repeat(32);
    let mut search_de = make_search("laptop", "products", &qid_de);
    search_de.country = Some("DE".to_string());
    collector.record_search(search_de);

    collector.flush_all();

    let app = Router::new()
        .route("/2/countries", get(get_countries))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/countries?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let countries = body["countries"].as_array().unwrap();
    assert_eq!(countries.len(), 2);

    // Ordered by count descending: US (3) first, DE (1) second
    assert_eq!(countries[0]["country"].as_str().unwrap(), "US");
    assert_eq!(countries[0]["count"].as_i64().unwrap(), 3);
    assert_eq!(countries[1]["country"].as_str().unwrap(), "DE");
    assert_eq!(countries[1]["count"].as_i64().unwrap(), 1);

    // No extra fields per entry — only country (string) and count (integer)
    for entry in countries {
        let obj = entry.as_object().unwrap();
        assert_eq!(
            obj.len(),
            2,
            "each entry should have exactly 2 fields: country, count"
        );
        assert!(entry["country"].is_string());
        assert!(entry["count"].is_i64());
    }

    // No total at top level
    assert!(body.get("total").is_none());
}

// ── Geo regions endpoint ──

/// Verify the `/2/geo/:country/regions` endpoint returns region-level counts filtered to a single country, ordered by count descending, and excludes regions from other countries.
#[tokio::test]
async fn geo_regions_returns_region_breakdown_for_country() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // 2 searches from US/CA, 1 from US/NY, 1 from DE (should not appear)
    for i in 0..2 {
        let qid = "c".repeat(31) + &i.to_string();
        let mut search = make_search("iphone", "products", &qid);
        search.country = Some("US".to_string());
        search.region = Some("CA".to_string());
        collector.record_search(search);
    }
    let qid_ny = "n".repeat(32);
    let mut search_ny = make_search("laptop", "products", &qid_ny);
    search_ny.country = Some("US".to_string());
    search_ny.region = Some("NY".to_string());
    collector.record_search(search_ny);

    let qid_de = "e".repeat(32);
    let mut search_de = make_search("tablet", "products", &qid_de);
    search_de.country = Some("DE".to_string());
    search_de.region = Some("BY".to_string());
    collector.record_search(search_de);

    collector.flush_all();

    let app = Router::new()
        .route("/2/geo/:country/regions", get(get_geo_regions))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/geo/US/regions?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert_eq!(body["country"].as_str().unwrap(), "US");

    let regions = body["regions"].as_array().unwrap();
    assert_eq!(regions.len(), 2);

    // Ordered by count descending: CA (2) first, NY (1) second
    assert_eq!(regions[0]["region"].as_str().unwrap(), "CA");
    assert_eq!(regions[0]["count"].as_i64().unwrap(), 2);
    assert_eq!(regions[1]["region"].as_str().unwrap(), "NY");
    assert_eq!(regions[1]["count"].as_i64().unwrap(), 1);
}

// ── B5a: top searches response shape ──

/// Verify the `/2/searches` endpoint returns entries with `search`, `count`, and `nbHits` fields when `clickAnalytics` is not enabled.
#[tokio::test]
async fn top_searches_response_shape_without_click_analytics() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let qid = "a".repeat(32);
    collector.record_search(make_search("iphone", "products", &qid));
    collector.flush_all();

    let app = Router::new()
        .route("/2/searches", get(get_top_searches))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/searches?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let searches = body["searches"].as_array().unwrap();
    assert_eq!(searches.len(), 1);

    let entry = &searches[0];
    // Algolia shape: { search (string), count (int), nbHits (int) }
    assert_eq!(entry["search"].as_str().unwrap(), "iphone");
    assert!(entry["count"].as_i64().is_some(), "count should be integer");
    assert!(entry.get("nbHits").is_some(), "should have nbHits field");
    assert!(
        entry["nbHits"].as_i64().is_some(),
        "nbHits should be integer"
    );
}

/// Verify the `/2/searches` endpoint includes `trackedSearchCount`, `clickCount`, `clickThroughRate`, `conversionRate`, `conversionCount`, and `averageClickPosition` when `clickAnalytics=true`.
#[tokio::test]
async fn top_searches_with_click_analytics_has_enriched_fields() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let qid = "a".repeat(32);
    collector.record_search(make_search("iphone", "products", &qid));
    collector.record_insight(make_click(&qid, "products"));
    collector.record_insight(make_conversion(&qid, "products", "purchase"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/searches", get(get_top_searches))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/searches?index=products&startDate={today}&endDate={today}&clickAnalytics=true"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    let searches = body["searches"].as_array().unwrap();
    assert!(!searches.is_empty());

    let entry = &searches[0];
    // Algolia enriched fields with clickAnalytics=true
    assert!(
        entry.get("trackedSearchCount").is_some(),
        "missing trackedSearchCount"
    );
    assert!(entry.get("clickCount").is_some(), "missing clickCount");
    assert!(
        entry.get("clickThroughRate").is_some(),
        "missing clickThroughRate"
    );
    assert!(
        entry.get("conversionRate").is_some(),
        "missing conversionRate"
    );
    assert!(
        entry.get("conversionCount").is_some(),
        "missing conversionCount"
    );
    assert!(
        entry.get("averageClickPosition").is_some(),
        "missing averageClickPosition"
    );
}

// ── B5b: clickThroughRate response shape ──

/// Verify the `/2/clicks/clickThroughRate` endpoint returns `rate`, `clickCount`, `trackedSearchCount`, and a `dates` array with per-day breakdowns including the same fields.
#[tokio::test]
async fn click_through_rate_response_shape_with_data() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let qid = "a".repeat(32);
    collector.record_search(make_search("iphone", "products", &qid));
    collector.record_insight(make_click(&qid, "products"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/clicks/clickThroughRate", get(get_click_through_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/clicks/clickThroughRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    // Algolia shape: { rate, clickCount, trackedSearchCount, dates }
    assert!(body.get("rate").is_some(), "missing rate");
    assert_eq!(body["clickCount"].as_i64().unwrap(), 1);
    assert_eq!(body["trackedSearchCount"].as_i64().unwrap(), 1);
    assert!((body["rate"].as_f64().unwrap() - 1.0).abs() < 0.001);

    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 1);
    assert!(dates[0].get("date").is_some());
    assert!(dates[0].get("rate").is_some());
    assert!(dates[0].get("clickCount").is_some());
    assert!(dates[0].get("trackedSearchCount").is_some());
}

/// Verify the click-through rate endpoint returns `null` for `rate` (not zero) with zero tracked searches, matching Algolia's null-rate convention.
#[tokio::test]
async fn click_through_rate_empty_returns_null_rate() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let app = Router::new()
        .route("/2/clicks/clickThroughRate", get(get_click_through_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/clicks/clickThroughRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    // Algolia: rate is null (not 0) when no data
    assert!(
        body["rate"].is_null(),
        "rate should be null with no data, got: {}",
        body["rate"]
    );
    assert_eq!(body["clickCount"].as_i64().unwrap(), 0);
    assert_eq!(body["trackedSearchCount"].as_i64().unwrap(), 0);
}

// ── B5c: conversionRate response shape ──

/// Verify the `/2/conversions/conversionRate` endpoint uses `conversionCount` (not subtype-specific names) in both top-level and per-date entries, matching the Algolia contract.
#[tokio::test]
async fn conversion_rate_response_uses_conversion_count_field() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let qid = "a".repeat(32);

    collector.record_search(make_search("iphone", "products", &qid));
    collector.record_insight(make_conversion(&qid, "products", "purchase"));
    collector.flush_all();

    let app = Router::new()
        .route("/2/conversions/conversionRate", get(get_conversion_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/conversionRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    // Algolia shape: { rate, trackedSearchCount, conversionCount, dates }
    assert!(
        body.get("conversionCount").is_some(),
        "must have conversionCount field"
    );
    assert_eq!(body["conversionCount"].as_i64().unwrap(), 1);
    assert_eq!(body["trackedSearchCount"].as_i64().unwrap(), 1);
    assert!((body["rate"].as_f64().unwrap() - 1.0).abs() < 0.001);

    let dates = body["dates"].as_array().unwrap();
    assert_eq!(dates.len(), 1);
    assert!(
        dates[0].get("conversionCount").is_some(),
        "dates must use conversionCount"
    );
    assert!(dates[0].get("trackedSearchCount").is_some());
}

/// Verify the conversion rate endpoint returns `null` for `rate` (not zero) with zero tracked searches, matching Algolia's null-rate convention.
#[tokio::test]
async fn conversion_rate_empty_returns_null_rate() {
    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let engine = Arc::new(AnalyticsQueryEngine::new(config));
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let app = Router::new()
        .route("/2/conversions/conversionRate", get(get_conversion_rate))
        .with_state(engine);

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/2/conversions/conversionRate?index=products&startDate={today}&endDate={today}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_json(resp).await;
    assert!(body["rate"].is_null(), "rate should be null with no data");
    assert_eq!(body["conversionCount"].as_i64().unwrap(), 0);
    assert_eq!(body["trackedSearchCount"].as_i64().unwrap(), 0);
}

mod stage5_analytics_integration_tests {
    use super::*;

    /// Create a minimal test `SearchEvent` with an optional country field for GeoIP integration tests.
    ///
    /// # Arguments
    ///
    /// * `index` - The index name.
    /// * `country` - Optional two-letter country code to simulate GeoIP enrichment.
    fn make_search_event(index: &str, country: Option<&str>) -> SearchEvent {
        SearchEvent {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            query: "test".to_string(),
            query_id: Some("a".repeat(32)),
            index_name: index.to_string(),
            nb_hits: 1,
            processing_time_ms: 5,
            user_token: Some("user_1".to_string()),
            user_ip: Some("8.8.8.8".to_string()),
            filters: None,
            facets: None,
            analytics_tags: None,
            page: 0,
            hits_per_page: 20,
            has_results: true,
            country: country.map(|s| s.to_string()),
            region: None,
            experiment_id: None,
            variant_id: None,
            assignment_method: None,
        }
    }

    async fn fetch_countries_json(app: &Router, index: &str, date: &str) -> serde_json::Value {
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index={index}&startDate={date}&endDate={date}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// TODO: Document countries_endpoint_reflects_enriched_country_field.
    #[tokio::test]
    async fn countries_endpoint_reflects_enriched_country_field() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // Simulate what the search handler does when GeoIP enrichment resolves "DE"
        collector.record_search(make_search_event("enrichment_idx", Some("DE")));
        collector.flush_all();

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .with_state(engine);

        let body = fetch_countries_json(&app, "enrichment_idx", &today).await;
        let countries = body["countries"].as_array().unwrap();
        assert_eq!(countries.len(), 1, "should have one country entry");
        assert_eq!(
            countries[0]["country"], "DE",
            "country should be DE from enrichment"
        );
        assert_eq!(countries[0]["count"], 1);
    }

    /// TODO: Document analytics_event_persisted_without_country_when_geoip_unavailable.
    #[tokio::test]
    async fn analytics_event_persisted_without_country_when_geoip_unavailable() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // country=None simulates missing GeoIP reader
        collector.record_search(make_search_event("no_geoip_idx", None));
        collector.flush_all();

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .route("/2/searches", get(get_top_searches))
            .with_state(engine);

        let body = fetch_countries_json(&app, "no_geoip_idx", &today).await;

        // No country entry — but also no error. Empty array is correct.
        let countries = body["countries"].as_array().unwrap();
        assert!(
            countries.is_empty(),
            "countries should be empty when no GeoIP enrichment, got: {countries:?}"
        );

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/searches?index=no_geoip_idx&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let searches_body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let searches = searches_body["searches"].as_array().unwrap();
        assert_eq!(searches.len(), 1, "search event should still be persisted");
        assert_eq!(searches[0]["search"], "test");
        assert_eq!(searches[0]["count"], 1);
    }
}

mod stage_b_contract_parity_tests {
    use super::*;

    fn make_search_with_country_and_tags(
        query: &str,
        index: &str,
        query_id: &str,
        country: &str,
        tags: Option<&str>,
    ) -> SearchEvent {
        SearchEvent {
            country: Some(country.to_string()),
            analytics_tags: tags.map(|t| t.to_string()),
            ..make_search(query, index, query_id)
        }
    }

    fn make_conversion_with_currency(
        query_id: &str,
        index: &str,
        value: f64,
        currency: &str,
    ) -> InsightEvent {
        InsightEvent {
            value: Some(value),
            currency: Some(currency.to_string()),
            ..make_conversion(query_id, index, "purchase")
        }
    }

    // ── B.1: countries limit/offset/orderBy ──

    /// TODO: Document countries_supports_limit_offset_order_by_direction_consistently.
    #[tokio::test]
    async fn countries_supports_limit_offset_order_by_direction_consistently() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // Seed 4 countries with distinct counts: US=4, DE=3, FR=2, JP=1
        for i in 0..4 {
            let qid = format!("us{:030}", i);
            let mut s = make_search("q", "idx", &qid);
            s.country = Some("US".to_string());
            collector.record_search(s);
        }
        for i in 0..3 {
            let qid = format!("de{:030}", i);
            let mut s = make_search("q", "idx", &qid);
            s.country = Some("DE".to_string());
            collector.record_search(s);
        }
        for i in 0..2 {
            let qid = format!("fr{:030}", i);
            let mut s = make_search("q", "idx", &qid);
            s.country = Some("FR".to_string());
            collector.record_search(s);
        }
        {
            let qid = format!("jp{:030}", 0);
            let mut s = make_search("q", "idx", &qid);
            s.country = Some("JP".to_string());
            collector.record_search(s);
        }
        collector.flush_all();

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .with_state(engine);

        // (a) limit=2 → only top 2
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index=idx&startDate={today}&endDate={today}&limit=2"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let countries = body["countries"].as_array().unwrap();
        assert_eq!(countries.len(), 2, "limit=2 should return 2 entries");
        assert_eq!(countries[0]["country"], "US");
        assert_eq!(countries[1]["country"], "DE");

        // (b) offset=1&limit=2 → skip first, take next 2
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index=idx&startDate={today}&endDate={today}&limit=2&offset=1"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let countries = body["countries"].as_array().unwrap();
        assert_eq!(
            countries.len(),
            2,
            "offset=1&limit=2 should return 2 entries"
        );
        assert_eq!(
            countries[0]["country"], "DE",
            "offset=1 should skip US and start at DE"
        );
        assert_eq!(countries[1]["country"], "FR");

        // (c) orderBy=count:asc → ascending order
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index=idx&startDate={today}&endDate={today}&orderBy=count:asc"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let countries = body["countries"].as_array().unwrap();
        assert!(countries.len() >= 4, "should return all 4 countries");
        // Ascending: JP (1) should be first
        assert_eq!(
            countries[0]["country"], "JP",
            "orderBy=count:asc should put lowest count first"
        );
        assert_eq!(countries[0]["count"], 1);
    }

    // ── B.2: countries invalid date range → 400 ──

    /// TODO: Document countries_invalid_date_range_returns_algolia_shaped_400.
    #[tokio::test]
    async fn countries_invalid_date_range_returns_algolia_shaped_400() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let engine = Arc::new(AnalyticsQueryEngine::new(config));

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .with_state(engine);

        // (a) endDate before startDate
        let req = Request::builder()
            .method(Method::GET)
            .uri("/2/countries?index=idx&startDate=2026-02-20&endDate=2026-02-10")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "inverted date range must return 400"
        );
        let body = body_json(resp).await;
        assert_eq!(body["status"], 400);
        assert!(
            body["message"].as_str().is_some(),
            "error body must have a message field"
        );

        // (b) malformed date
        let req = Request::builder()
            .method(Method::GET)
            .uri("/2/countries?index=idx&startDate=not-a-date&endDate=2026-02-20")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "malformed date must return 400"
        );
        let body = body_json(resp).await;
        assert_eq!(body["status"], 400);
    }

    // ── B.3: countries tags filter ──

    /// TODO: Document countries_tags_filter_applies_when_tags_present.
    #[tokio::test]
    async fn countries_tags_filter_applies_when_tags_present() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        // 2 US searches with tag "platform:mobile", 1 US search with tag "platform:desktop"
        for i in 0..2 {
            let qid = format!("mob{:029}", i);
            collector.record_search(make_search_with_country_and_tags(
                "q",
                "idx",
                &qid,
                "US",
                Some("platform:mobile"),
            ));
        }
        {
            let qid = format!("dsk{:029}", 0);
            collector.record_search(make_search_with_country_and_tags(
                "q",
                "idx",
                &qid,
                "US",
                Some("platform:desktop"),
            ));
        }
        // 1 DE search with tag "platform:mobile"
        {
            let qid = format!("dem{:029}", 0);
            collector.record_search(make_search_with_country_and_tags(
                "q",
                "idx",
                &qid,
                "DE",
                Some("platform:mobile"),
            ));
        }
        collector.flush_all();

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .with_state(engine);

        // Filter by tags=platform:mobile → US:2 + DE:1, no desktop searches
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index=idx&startDate={today}&endDate={today}&tags=platform:mobile"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp).await;
        let countries = body["countries"].as_array().unwrap();

        // Total mobile searches: US=2, DE=1 → 2 country entries
        assert_eq!(countries.len(), 2, "should have US and DE with mobile tag");

        let us = countries.iter().find(|c| c["country"] == "US").unwrap();
        let de = countries.iter().find(|c| c["country"] == "DE").unwrap();
        assert_eq!(us["count"], 2, "US should have 2 mobile searches");
        assert_eq!(de["count"], 1, "DE should have 1 mobile search");

        // The desktop-only search should NOT be counted
        let total: i64 = countries.iter().map(|c| c["count"].as_i64().unwrap()).sum();
        assert_eq!(total, 3, "total should be 3 (only mobile-tagged), not 4");
    }

    // ── B.4: conversion date bucket format consistency ──

    /// TODO: Document conversion_endpoints_share_identical_date_bucket_format.
    #[tokio::test]
    async fn conversion_endpoints_share_identical_date_bucket_format() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let qid = "a".repeat(32);

        collector.record_search(make_search("q", "products", &qid));
        collector.record_insight(make_conversion(&qid, "products", "addToCart"));
        collector.record_insight(make_conversion(&qid, "products", "purchase"));
        collector.flush_all();

        let app = Router::new()
            .route("/2/conversions/addToCartRate", get(get_add_to_cart_rate))
            .route("/2/conversions/purchaseRate", get(get_purchase_rate))
            .route("/2/conversions/conversionRate", get(get_conversion_rate))
            .route("/2/conversions/revenue", get(get_revenue))
            .with_state(engine);

        let endpoints = [
            "/2/conversions/addToCartRate",
            "/2/conversions/purchaseRate",
            "/2/conversions/conversionRate",
            "/2/conversions/revenue",
        ];

        let mut all_dates: Vec<(String, String)> = Vec::new();

        for endpoint in &endpoints {
            let req = Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "{endpoint}?index=products&startDate={today}&endDate={today}"
                ))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "endpoint {endpoint} should return 200"
            );
            let body = body_json(resp).await;
            let dates = body["dates"].as_array().unwrap();

            for entry in dates {
                let date_str = entry["date"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{endpoint}: date field should be string"));
                // Verify YYYY-MM-DD format (10 chars, parseable)
                assert_eq!(
                    date_str.len(),
                    10,
                    "{endpoint}: date '{date_str}' should be YYYY-MM-DD (10 chars)"
                );
                assert!(
                    chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").is_ok(),
                    "{endpoint}: date '{date_str}' should parse as YYYY-MM-DD"
                );
                all_dates.push((endpoint.to_string(), date_str.to_string()));
            }
        }

        // All endpoints should use the same date string for the same day
        let unique_dates: std::collections::HashSet<&str> =
            all_dates.iter().map(|(_, d)| d.as_str()).collect();
        assert!(
            unique_dates.len() <= 1,
            "all conversion endpoints should use identical date format, got: {:?}",
            all_dates
        );
    }

    // ── B.5: revenue contract strictness ──

    /// TODO: Document revenue_endpoint_currency_map_and_totals_match_contract.
    #[tokio::test]
    async fn revenue_endpoint_currency_map_and_totals_match_contract() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let qid_a = "a".repeat(32);
        let qid_b = "b".repeat(32);

        collector.record_search(make_search("q", "products", &qid_a));
        collector.record_search(make_search("q", "products", &qid_b));
        collector.record_insight(make_conversion_with_currency(
            &qid_a, "products", 49.99, "USD",
        ));
        collector.record_insight(make_conversion_with_currency(
            &qid_b, "products", 35.00, "EUR",
        ));
        collector.flush_all();

        let app = Router::new()
            .route("/2/conversions/revenue", get(get_revenue))
            .with_state(engine);

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/conversions/revenue?index=products&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp).await;
        let top = body.as_object().unwrap();

        // Strict top-level fields: only "currencies" and "dates"
        let allowed_top: std::collections::HashSet<&str> =
            ["currencies", "dates"].iter().copied().collect();
        for key in top.keys() {
            assert!(
                allowed_top.contains(key.as_str()),
                "unexpected top-level field in revenue response: '{key}'"
            );
        }

        // currencies is an object (not array)
        let currencies = body["currencies"].as_object().unwrap();
        assert!(currencies.len() >= 2);

        // Each currency entry: exactly { currency, revenue }
        for (code, entry) in currencies {
            let obj = entry.as_object().unwrap();
            assert_eq!(
                obj.len(),
                2,
                "currency entry '{code}' should have exactly 2 fields, got: {obj:?}"
            );
            assert_eq!(
                obj["currency"].as_str().unwrap(),
                code,
                "currency field should match key"
            );
            assert!(
                obj["revenue"].as_f64().is_some(),
                "revenue should be numeric"
            );
        }

        // dates array entries: exactly { date, currencies }
        let dates = body["dates"].as_array().unwrap();
        assert!(!dates.is_empty());
        for entry in dates {
            let obj = entry.as_object().unwrap();
            assert_eq!(
                obj.len(),
                2,
                "dates entry should have exactly 2 fields (date, currencies), got: {obj:?}"
            );
            assert!(obj.contains_key("date"), "dates entry must have 'date'");
            assert!(
                obj.contains_key("currencies"),
                "dates entry must have 'currencies'"
            );
        }
    }

    // ── B.6: no extra top-level fields ──

    /// TODO: Document analytics_endpoints_never_emit_extra_top_level_fields.
    #[tokio::test]
    async fn analytics_endpoints_never_emit_extra_top_level_fields() {
        let tmp = TempDir::new().unwrap();
        let config = test_analytics_config(&tmp);
        let collector = AnalyticsCollector::new(config.clone());
        let engine = Arc::new(AnalyticsQueryEngine::new(config));
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let qid = "a".repeat(32);

        let mut s = make_search("q", "products", &qid);
        s.country = Some("US".to_string());
        collector.record_search(s);
        collector.record_insight(make_conversion(&qid, "products", "addToCart"));
        collector.record_insight(make_conversion(&qid, "products", "purchase"));
        collector.flush_all();

        let app = Router::new()
            .route("/2/countries", get(get_countries))
            .route("/2/conversions/addToCartRate", get(get_add_to_cart_rate))
            .route("/2/conversions/purchaseRate", get(get_purchase_rate))
            .route("/2/conversions/conversionRate", get(get_conversion_rate))
            .with_state(engine);

        // countries: only { countries }
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/countries?index=products&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let keys: Vec<&str> = body
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(
            keys,
            vec!["countries"],
            "countries response must have exactly one top-level field 'countries', got: {keys:?}"
        );

        // addToCartRate: { rate, trackedSearchCount, addToCartCount, dates }
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/conversions/addToCartRate?index=products&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let mut keys: Vec<String> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        let mut expected = vec!["addToCartCount", "dates", "rate", "trackedSearchCount"];
        expected.sort();
        assert_eq!(keys, expected, "addToCartRate unexpected fields");

        // purchaseRate: { rate, trackedSearchCount, purchaseCount, dates }
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/conversions/purchaseRate?index=products&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let mut keys: Vec<String> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        let mut expected = vec!["dates", "purchaseCount", "rate", "trackedSearchCount"];
        expected.sort();
        assert_eq!(keys, expected, "purchaseRate unexpected fields");

        // conversionRate: { rate, trackedSearchCount, conversionCount, dates }
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/2/conversions/conversionRate?index=products&startDate={today}&endDate={today}"
            ))
            .body(Body::empty())
            .unwrap();
        let body = body_json(app.clone().oneshot(req).await.unwrap()).await;
        let mut keys: Vec<String> = body.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        let mut expected = vec!["conversionCount", "dates", "rate", "trackedSearchCount"];
        expected.sort();
        assert_eq!(keys, expected, "conversionRate unexpected fields");
    }
}
