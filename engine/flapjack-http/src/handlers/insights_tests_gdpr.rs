use super::*;

/// Verify that the GDPR delete endpoint invokes `send_gdpr_confirmation` on the global notification service.
#[tokio::test]
async fn delete_usertoken_sends_gdpr_notification() {
    // Initialize global notifier (OnceLock — only first call wins, which is fine)
    let service = Arc::new(crate::notifications::NotificationService::disabled());
    crate::notifications::init_global_notifier(Arc::clone(&service));

    // Get reference to the global service for counter checks
    let notifier = crate::notifications::global_notifier().expect("notifier should be set");
    let before = notifier
        .gdpr_call_count
        .load(std::sync::atomic::Ordering::Relaxed);

    let tmp = TempDir::new().unwrap();
    let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
    let app = app_router(collector);

    let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/user_test_gdpr").await;
    assert_eq!(response.status(), StatusCode::OK);

    let after = notifier
        .gdpr_call_count
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "send_gdpr_confirmation should have been called: before={before}, after={after}"
    );
}

/// Verify that the `limit` parameter caps the number of returned debug events.
#[tokio::test]
async fn debug_endpoint_respects_limit() {
    let tmp = TempDir::new().unwrap();
    let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
    let app = app_router(collector);

    let events: Vec<_> = (0..5)
        .map(|i| {
            json!({
                "eventType": "view",
                "eventName": format!("V{i}"),
                "index": "products",
                "userToken": "user_a",
                "objectIDs": [format!("o{i}")]
            })
        })
        .collect();
    send_json_request(&app, Method::POST, "/1/events", json!({ "events": events })).await;

    let resp = send_empty_request(&app, Method::GET, "/1/events/debug?limit=2").await;
    let body = body_json(resp).await;
    assert_eq!(body["count"], 2);
}

// ── Stage D: GDPR Multi-Store Deletion Tests ──

/// Verify that GDPR delete removes the target user's personalization profile from disk.
#[tokio::test]
async fn gdpr_delete_usertoken_removes_personalization_profile_cache() {
    use flapjack::personalization::{PersonalizationProfile, PersonalizationProfileStore};
    use std::collections::BTreeMap;

    let tmp = TempDir::new().unwrap();
    let collector = AnalyticsCollector::new(test_analytics_config(&tmp));

    let store = PersonalizationProfileStore::new(tmp.path());
    let mut brand_scores = BTreeMap::new();
    brand_scores.insert("Nike".to_string(), 15);
    let mut scores = BTreeMap::new();
    scores.insert("brand".to_string(), brand_scores);
    store
        .save_profile(&PersonalizationProfile {
            user_token: "gdpr-target".to_string(),
            last_event_at: Some(chrono::Utc::now().to_rfc3339()),
            scores,
        })
        .unwrap();
    assert!(
        store.load_profile("gdpr-target").unwrap().is_some(),
        "profile should exist before GDPR delete"
    );

    let app = app_router_with_base(collector, tmp.path().to_path_buf());

    let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/gdpr-target").await;
    assert_eq!(response.status(), StatusCode::OK);

    assert!(
        store.load_profile("gdpr-target").unwrap().is_none(),
        "personalization profile should be deleted after GDPR usertoken delete"
    );
}

/// Verify that GDPR delete for one user leaves another user's analytics events and personalization profile intact.
#[tokio::test]
async fn gdpr_delete_usertoken_keeps_other_users_data() {
    use flapjack::personalization::{PersonalizationProfile, PersonalizationProfileStore};
    use std::collections::BTreeMap;

    let tmp = TempDir::new().unwrap();
    let config = test_analytics_config(&tmp);
    let collector = AnalyticsCollector::new(config.clone());
    let app = app_router_with_base(Arc::clone(&collector), tmp.path().to_path_buf());

    let ingest_body = json!({
        "events": [
            {
                "eventType": "view",
                "eventName": "Viewed",
                "index": "products",
                "userToken": "target-user",
                "objectIDs": ["obj1"]
            },
            {
                "eventType": "view",
                "eventName": "Viewed",
                "index": "products",
                "userToken": "safe-user",
                "objectIDs": ["obj2"]
            }
        ]
    });
    let resp = send_json_request(&app, Method::POST, "/1/events", ingest_body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    collector.flush_all();

    let store = PersonalizationProfileStore::new(tmp.path());
    for token in &["target-user", "safe-user"] {
        let mut scores = BTreeMap::new();
        let mut brand = BTreeMap::new();
        brand.insert("Nike".to_string(), 10);
        scores.insert("brand".to_string(), brand);
        store
            .save_profile(&PersonalizationProfile {
                user_token: token.to_string(),
                last_event_at: Some(chrono::Utc::now().to_rfc3339()),
                scores,
            })
            .unwrap();
    }

    let response = send_empty_request(&app, Method::DELETE, "/1/usertokens/target-user").await;
    assert_eq!(response.status(), StatusCode::OK);

    let engine = AnalyticsQueryEngine::new(config);
    let rows = engine
        .query_events(
            "products",
            "SELECT user_token, COUNT(*) as count FROM events GROUP BY user_token ORDER BY user_token",
        )
        .await
        .unwrap();
    assert!(
        !rows
            .iter()
            .any(|row| row.get("user_token") == Some(&json!("target-user"))),
        "target-user events should be purged: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row.get("user_token") == Some(&json!("safe-user"))),
        "safe-user events should remain: {rows:?}"
    );

    assert!(
        store.load_profile("safe-user").unwrap().is_some(),
        "safe-user profile should remain after target-user GDPR delete"
    );
    assert!(
        store.load_profile("target-user").unwrap().is_none(),
        "target-user profile should be removed"
    );
}

/// Verify that deleting the same user token twice succeeds both times and returns `deletedAt` on each call.
#[tokio::test]
async fn gdpr_delete_usertoken_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
    let app = app_router_with_base(collector, tmp.path().to_path_buf());

    let r1 = send_empty_request(&app, Method::DELETE, "/1/usertokens/nonexistent-user").await;
    assert_eq!(r1.status(), StatusCode::OK);
    let body1 = body_json(r1).await;
    assert_eq!(body1["status"], json!(200));

    let r2 = send_empty_request(&app, Method::DELETE, "/1/usertokens/nonexistent-user").await;
    assert_eq!(r2.status(), StatusCode::OK);
    let body2 = body_json(r2).await;
    assert_eq!(body2["status"], json!(200));
    assert!(
        body2["deletedAt"].as_str().is_some(),
        "second delete should still return deletedAt"
    );
}

/// Stage 3 §4: When all events in a batch are invalid, the error response must use the
/// standard `{ "message": "...", "status": N }` shape with no extra fields leaked.
#[tokio::test]
async fn all_invalid_events_rejected_with_standard_error_body_shape() {
    let tmp = TempDir::new().unwrap();
    let collector = AnalyticsCollector::new(test_analytics_config(&tmp));
    let app = app_router(collector);

    let body = json!({
        "events": [{
            "eventType": "click",
            "eventName": "Bad Click",
            "index": "products",
            "userToken": "user@invalid",
            "objectIDs": ["obj1"],
            "positions": [1]
        }]
    });

    let response = send_json_request(&app, Method::POST, "/1/events", body).await;
    assert_rejected_event_status(response.status());

    let error_body = body_json(response).await;
    assert!(
        error_body["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "error body must contain a non-empty message field"
    );
    let status_code = error_body["status"]
        .as_u64()
        .expect("error body must contain a numeric status field");
    assert!(
        status_code == 400 || status_code == 422,
        "status field should match HTTP status: {status_code}"
    );
    let keys: Vec<&str> = error_body
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert!(
        keys.iter().all(|k| *k == "message" || *k == "status"),
        "error body must not contain extra fields: {keys:?}"
    );
}
