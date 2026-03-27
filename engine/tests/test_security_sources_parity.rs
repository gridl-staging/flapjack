use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;

mod common;

const ADMIN_KEY: &str = "test-admin-key-security-sources";

fn encode_cidr_for_path(cidr: &str) -> String {
    cidr.replace('/', "%2F")
}

#[tokio::test]
async fn get_security_sources_is_empty_by_default() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) =
        common::send_json(&app, Method::GET, "/1/security/sources", ADMIN_KEY, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn append_then_get_roundtrips_source_and_description_with_created_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (append_status, append_body) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({
            "source": "10.0.0.0/24",
            "description": "Office"
        })),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);
    let created_at = append_body["createdAt"]
        .as_str()
        .expect("append response must include createdAt");
    chrono::DateTime::parse_from_rfc3339(created_at).expect("append createdAt must be RFC3339");

    let (get_status, get_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/security/sources",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.0.0.10")],
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(
        get_body,
        json!([
            {
                "source": "10.0.0.0/24",
                "description": "Office"
            }
        ])
    );
}

#[tokio::test]
async fn put_replaces_entire_list_and_returns_updated_at() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (_append_status, _append_body) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "10.0.0.0/24", "description": "Office"})),
    )
    .await;

    let (put_status, put_body) = common::send_json_with_headers(
        &app,
        Method::PUT,
        "/1/security/sources",
        ADMIN_KEY,
        Some(json!([
            {"source": "192.168.0.0/24", "description": "HQ"},
            {"source": "172.16.0.0/16", "description": "VPN"}
        ])),
        &[("x-real-ip", "10.0.0.10")],
    )
    .await;
    assert_eq!(put_status, StatusCode::OK);
    let updated_at = put_body["updatedAt"]
        .as_str()
        .expect("replace response must include updatedAt");
    chrono::DateTime::parse_from_rfc3339(updated_at).expect("replace updatedAt must be RFC3339");

    let (get_status, get_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/security/sources",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "192.168.0.10")],
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(
        get_body,
        json!([
            {"source": "192.168.0.0/24", "description": "HQ"},
            {"source": "172.16.0.0/16", "description": "VPN"}
        ])
    );
}

#[tokio::test]
async fn append_duplicate_is_idempotent() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    for _ in 0..2 {
        let (status, _) = common::send_json_with_headers(
            &app,
            Method::POST,
            "/1/security/sources/append",
            ADMIN_KEY,
            Some(json!({"source": "10.1.0.0/16", "description": "Corp"})),
            &[("x-real-ip", "10.1.2.3")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let (get_status, get_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/security/sources",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.1.2.3")],
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);

    let arr = get_body.as_array().expect("GET body must be an array");
    assert_eq!(arr.len(), 1, "duplicate append must not create duplicates");
    assert_eq!(
        arr[0],
        json!({"source": "10.1.0.0/16", "description": "Corp"})
    );
}

#[tokio::test]
async fn delete_removes_entry_and_is_noop_for_missing() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (append_status, _) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "10.9.0.0/16", "description": "Temp"})),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);

    let cidr_path = encode_cidr_for_path("10.9.0.0/16");
    let (delete_status, delete_body) = common::send_json_with_headers(
        &app,
        Method::DELETE,
        &format!("/1/security/sources/{cidr_path}"),
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.9.1.1")],
    )
    .await;
    assert_eq!(delete_status, StatusCode::OK);
    let deleted_at = delete_body["deletedAt"]
        .as_str()
        .expect("delete response must include deletedAt");
    chrono::DateTime::parse_from_rfc3339(deleted_at).expect("delete deletedAt must be RFC3339");

    let (get_status, get_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/security/sources",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.9.1.1")],
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body, json!([]));

    let (missing_delete_status, missing_delete_body) = common::send_json_with_headers(
        &app,
        Method::DELETE,
        "/1/security/sources/203.0.113.0%2F24",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.9.1.1")],
    )
    .await;
    assert_eq!(missing_delete_status, StatusCode::OK);
    let missing_deleted_at = missing_delete_body["deletedAt"]
        .as_str()
        .expect("missing delete response must include deletedAt");
    chrono::DateTime::parse_from_rfc3339(missing_deleted_at)
        .expect("missing delete deletedAt must be RFC3339");
}

#[tokio::test]
async fn malformed_cidr_returns_400_parity_json_error() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, body) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "not-a-cidr", "description": "Bad"})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], json!(400));
    assert!(
        body["message"].as_str().unwrap_or("").contains("CIDR"),
        "malformed CIDR error should mention CIDR: {body}"
    );
}

#[tokio::test]
async fn allowlist_empty_allows_all_ips_on_protected_routes() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (status, _body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "203.0.113.55")],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn allowlist_enforces_cidr_and_returns_forbidden_for_unlisted_ip() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (append_status, _) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "10.0.0.0/8", "description": "corp"})),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);

    let (deny_status, deny_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "203.0.113.10")],
    )
    .await;
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(deny_body, json!({"message": "Forbidden", "status": 403}));

    let (allow_status, _allow_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "10.4.5.6")],
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);
}

#[tokio::test]
async fn forwarded_for_rightmost_untrusted_takes_precedence_over_real_ip_for_allowlist_matching() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (append_status, _) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "10.0.0.0/8", "description": "corp"})),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);

    // Allowlist matching uses the first untrusted IP from the RIGHT side of XFF
    // when the peer is a trusted proxy. Here that selected XFF hop is inside
    // allowlist, while x-real-ip is outside.
    let (allow_status, _allow_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[
            ("x-forwarded-for", "203.0.113.9, 10.2.2.2"),
            ("x-real-ip", "203.0.113.9"),
        ],
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);

    // The selected XFF hop is outside allowlist, even though x-real-ip is inside.
    // This proves XFF selection takes precedence over x-real-ip for source matching.
    let (deny_status, deny_body) = common::send_json_with_headers(
        &app,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[
            ("x-forwarded-for", "10.2.2.2, 203.0.113.9"),
            ("x-real-ip", "10.2.2.2"),
        ],
    )
    .await;
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(deny_body, json!({"message": "Forbidden", "status": 403}));
}

#[tokio::test]
async fn allowlist_persists_across_app_rebuild_using_same_data_dir() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let app_a = common::build_test_app_for_existing_data_dir(tmp.path(), Some(ADMIN_KEY));

    let (append_status, _) = common::send_json(
        &app_a,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "192.168.0.0/16", "description": "hq"})),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);
    drop(app_a);

    let app_b = common::build_test_app_for_existing_data_dir(tmp.path(), Some(ADMIN_KEY));

    let (allow_status, _allow_body) = common::send_json_with_headers(
        &app_b,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "192.168.10.42")],
    )
    .await;
    assert_eq!(allow_status, StatusCode::OK);

    let (deny_status, deny_body) = common::send_json_with_headers(
        &app_b,
        Method::GET,
        "/1/indexes",
        ADMIN_KEY,
        None,
        &[("x-real-ip", "203.0.113.10")],
    )
    .await;
    assert_eq!(deny_status, StatusCode::FORBIDDEN);
    assert_eq!(deny_body, json!({"message": "Forbidden", "status": 403}));
}

#[tokio::test]
async fn health_route_is_not_subject_to_allowlist_middleware() {
    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let (append_status, _) = common::send_json(
        &app,
        Method::POST,
        "/1/security/sources/append",
        ADMIN_KEY,
        Some(json!({"source": "10.0.0.0/8", "description": "corp"})),
    )
    .await;
    assert_eq!(append_status, StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/health")
                .header("x-real-ip", "203.0.113.1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
