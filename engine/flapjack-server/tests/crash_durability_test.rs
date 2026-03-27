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
