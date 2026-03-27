#![allow(deprecated)] // Command::cargo_bin — macro alternative requires same-package binary

//! Integration tests for flapjack admin key lifecycle flows.
mod support;

use support::{
    admin_auth_headers, admin_entry_exists_in_json, extract_admin_key_hash_from_json, flapjack_cmd,
    http_request_with_headers, RunningServer, TempDir,
};

#[test]
fn env_var_key_overrides_existing_keys_json() {
    let tmp = TempDir::new("fj_test_env_override");

    let output1 = flapjack_cmd()
        .env("FLAPJACK_ENV", "development")
        .env("FLAPJACK_BIND_ADDR", "127.0.0.1:0")
        .env("FLAPJACK_DATA_DIR", tmp.path())
        .timeout(std::time::Duration::from_secs(3))
        .output()
        .expect("failed to run");
    let stdout1 = String::from_utf8_lossy(&output1.stdout);
    assert!(
        stdout1.contains("Admin API Key:"),
        "First start must show auto-generated key"
    );

    let keys_json_before = std::fs::read_to_string(tmp.root().join("keys.json"))
        .expect("keys.json should exist after first start");
    let hash_before = extract_admin_key_hash_from_json(&keys_json_before);

    let custom_key = "rotated_key_abcdef0123456789";
    let output2 = flapjack_cmd()
        .env("FLAPJACK_ENV", "development")
        .env("FLAPJACK_ADMIN_KEY", custom_key)
        .env("FLAPJACK_BIND_ADDR", "127.0.0.1:0")
        .env("FLAPJACK_DATA_DIR", tmp.path())
        .timeout(std::time::Duration::from_secs(3))
        .output()
        .expect("failed to run");
    let stdout2 = String::from_utf8_lossy(&output2.stdout);

    assert!(
        stdout2.contains("Flapjack"),
        "Should start with overridden key, got: {}",
        stdout2
    );

    let keys_json_after = std::fs::read_to_string(tmp.root().join("keys.json")).unwrap();
    let hash_after = extract_admin_key_hash_from_json(&keys_json_after);
    assert_ne!(
        hash_before, hash_after,
        "Admin key hash must change after FLAPJACK_ADMIN_KEY rotation"
    );
    assert!(
        admin_entry_exists_in_json(&keys_json_after),
        "Admin API Key entry must still exist after rotation"
    );
}

#[test]
fn rotate_admin_key_endpoint_rewrites_admin_key_file_and_invalidates_old_key() {
    let tmp = TempDir::new("fj_test_rotate_admin_key_real_server");
    let server = RunningServer::spawn_auth_auto_port(tmp.path());

    let old_key_path = tmp.root().join(".admin_key");
    let old_key = std::fs::read_to_string(&old_key_path)
        .expect("startup should persist initial admin key to .admin_key")
        .trim()
        .to_string();
    assert!(
        old_key.starts_with("fj_admin_"),
        "initial admin key should use fj_admin_ prefix, got: {}",
        old_key
    );

    let rotate_response = http_request_with_headers(
        server.bind_addr(),
        "POST",
        "/internal/rotate-admin-key",
        &admin_auth_headers(old_key.as_str()),
        None,
    )
    .expect("rotate-admin-key request should return an HTTP response");
    assert_eq!(
        rotate_response.status, 200,
        "expected rotate-admin-key success, body: {}",
        rotate_response.body
    );

    let rotate_payload: serde_json::Value = serde_json::from_str(&rotate_response.body)
        .expect("rotation response should be valid JSON");
    let new_key = rotate_payload["key"]
        .as_str()
        .expect("rotation response should include `key`")
        .to_string();
    assert_ne!(
        old_key, new_key,
        "rotation should return a different admin key"
    );

    let admin_key_on_disk = std::fs::read_to_string(&old_key_path)
        .expect(".admin_key should still exist after rotation");
    assert_eq!(
        admin_key_on_disk.trim(),
        new_key,
        ".admin_key should be rewritten with rotated key"
    );

    let old_key_metrics = http_request_with_headers(
        server.bind_addr(),
        "GET",
        "/metrics",
        &admin_auth_headers(old_key.as_str()),
        None,
    )
    .expect("old-key metrics request should return an HTTP response");
    assert_eq!(
        old_key_metrics.status, 403,
        "old key should be rejected after rotation, body: {}",
        old_key_metrics.body
    );

    let new_key_metrics = http_request_with_headers(
        server.bind_addr(),
        "GET",
        "/metrics",
        &admin_auth_headers(new_key.as_str()),
        None,
    )
    .expect("new-key metrics request should return an HTTP response");
    assert_eq!(
        new_key_metrics.status, 200,
        "new key should be accepted after rotation, body: {}",
        new_key_metrics.body
    );
}

#[test]
fn reset_admin_key_works() {
    let tmp = TempDir::new("fj_test_reset_key");

    let bootstrap_server = RunningServer::spawn_auth_auto_port(tmp.path());
    drop(bootstrap_server);

    let keys_before = std::fs::read_to_string(tmp.root().join("keys.json"))
        .expect("keys.json should exist after server start");
    let hash_before = extract_admin_key_hash_from_json(&keys_before);

    let output = flapjack_cmd()
        .arg("--data-dir")
        .arg(tmp.path())
        .arg("reset-admin-key")
        .output()
        .expect("failed to run");

    assert!(output.status.success(), "reset-admin-key should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let new_key = stdout.trim();

    assert!(
        new_key.starts_with("fj_admin_"),
        "Expected fj_admin_ prefixed key, got: {}",
        new_key
    );
    assert_eq!(
        new_key.len(),
        41,
        "Key should be 41 chars (fj_admin_ + 32 hex), got {} chars: {}",
        new_key.len(),
        new_key
    );
    assert!(
        new_key[9..].chars().all(|c| c.is_ascii_hexdigit()),
        "Key suffix (after fj_admin_) should be hex, got: {}",
        new_key
    );

    let keys_after = std::fs::read_to_string(tmp.root().join("keys.json"))
        .expect("keys.json should still exist after reset");
    let hash_after = extract_admin_key_hash_from_json(&keys_after);
    assert_ne!(
        hash_before, hash_after,
        "Admin key hash must change after reset-admin-key"
    );
    assert!(
        admin_entry_exists_in_json(&keys_after),
        "Admin API Key entry must still exist in keys.json after reset"
    );
}

#[test]
fn reset_admin_key_fails_without_keys_json() {
    let tmp = TempDir::new("fj_test_reset_no_file");

    let output = flapjack_cmd()
        .arg("--data-dir")
        .arg(tmp.path())
        .arg("reset-admin-key")
        .output()
        .expect("failed to run");

    assert!(
        !output.status.success(),
        "reset-admin-key should fail without keys.json"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No keys.json found"),
        "Expected 'No keys.json found' error, got: {}",
        stderr
    );
}
