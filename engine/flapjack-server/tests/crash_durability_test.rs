#![allow(deprecated)] // Command::cargo_bin — macro alternative requires same-package binary

//! Real-server crash/restart durability test for acknowledged batch writes.
mod support;

use serde_json::json;
use std::time::Duration;
use support::{RunningServer, TempDir};

#[test]
fn acknowledged_batch_write_remains_searchable_after_crash_restart() {
    let tmp = TempDir::new("fj_test_crash_durability");
    let index_name = "crash_durability_idx";
    let object_id = "durability-doc-1";
    let query_token = "durability-proof-token";

    let mut server = RunningServer::spawn_no_auth_auto_port(tmp.path());

    let task_id = server.add_documents_batch(
        index_name,
        json!({
            "requests": [
                {
                    "action": "addObject",
                    "body": {
                        "objectID": object_id,
                        "title": "Crash durability proof",
                        "token": query_token
                    }
                }
            ]
        }),
    );

    let task = server.wait_for_task_published(index_name, task_id, Duration::from_secs(10));
    assert_eq!(task["status"], json!("published"));
    assert_eq!(task["pendingTask"], json!(false));

    let pre_crash_search = server.search(index_name, json!({ "query": query_token }));
    let pre_crash_hits = pre_crash_search["hits"]
        .as_array()
        .expect("search response must contain hits array before crash");
    assert!(
        pre_crash_hits
            .iter()
            .any(|hit| hit["objectID"] == json!(object_id)),
        "pre-crash search must contain acknowledged document: {}",
        pre_crash_search
    );

    server.kill_and_restart_no_auth_auto_port(tmp.path());

    let post_restart_search = server.search(index_name, json!({ "query": query_token }));
    let post_restart_hits = post_restart_search["hits"]
        .as_array()
        .expect("search response must contain hits array after restart");
    assert!(
        post_restart_hits
            .iter()
            .any(|hit| hit["objectID"] == json!(object_id)),
        "post-restart search must contain acknowledged document: {}",
        post_restart_search
    );
}

#[test]
fn nontrivial_acknowledged_dataset_survives_crash_restart() {
    let tmp = TempDir::new("fj_test_crash_durability_nontrivial");
    let index_name = "crash_durability_nontrivial_idx";
    let total_docs = 180usize;
    let batch_size = 30usize;

    let mut server = RunningServer::spawn_no_auth_auto_port(tmp.path());

    // Use repeated shared tokens plus deterministic per-doc values so the proof
    // checks both corpus-wide recovery and a specific targeted lookup.
    for batch_start in (0..total_docs).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total_docs);
        let requests = (batch_start..batch_end)
            .map(|doc_index| {
                let tier = if doc_index % 2 == 0 { "alpha" } else { "beta" };
                let family = doc_index % 3;
                json!({
                    "action": "addObject",
                    "body": {
                        "objectID": format!("durability-doc-{doc_index:03}"),
                        "title": format!("Crash durability batch document {doc_index:03}"),
                        "token": "nontrivial-durability-proof",
                        "tier": tier,
                        "family": format!("family-{family}"),
                        "marker": format!("marker{doc_index:03}"),
                    }
                })
            })
            .collect::<Vec<_>>();

        let task_id = server.add_documents_batch(index_name, json!({ "requests": requests }));
        let task = server.wait_for_task_published(index_name, task_id, Duration::from_secs(20));
        assert_eq!(task["status"], json!("published"));
        assert_eq!(task["pendingTask"], json!(false));
    }

    let pre_crash_all = server.search(index_name, json!({ "query": "" }));
    assert_eq!(
        pre_crash_all["nbHits"],
        json!(total_docs),
        "expected all seeded docs before crash: {pre_crash_all}"
    );

    let pre_crash_targeted = server.search(index_name, json!({ "query": "alpha" }));
    assert_eq!(
        pre_crash_targeted["nbHits"],
        json!(total_docs / 2),
        "expected deterministic tier subset before crash: {pre_crash_targeted}"
    );

    server.kill_and_restart_no_auth_auto_port(tmp.path());

    let post_restart_all = server.search(index_name, json!({ "query": "" }));
    assert_eq!(
        post_restart_all["nbHits"],
        json!(total_docs),
        "expected all seeded docs after restart: {post_restart_all}"
    );

    let post_restart_targeted = server.search(index_name, json!({ "query": "alpha" }));
    assert_eq!(
        post_restart_targeted["nbHits"],
        json!(total_docs / 2),
        "expected deterministic tier subset after restart: {post_restart_targeted}"
    );

    let post_restart_specific = server.search(index_name, json!({ "query": "marker121" }));
    let specific_hits = post_restart_specific["hits"]
        .as_array()
        .expect("targeted post-restart search must contain hits");
    assert!(
        specific_hits
            .iter()
            .any(|hit| hit["objectID"] == json!("durability-doc-121")),
        "post-restart search must still contain durability-doc-121 via marker121: {post_restart_specific}"
    );
}
