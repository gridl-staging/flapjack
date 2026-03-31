//! Stub summary for write_queue_tests.rs.
use super::*;
use crate::index::memory::{MemoryBudget, MemoryBudgetConfig};
use std::collections::HashMap;

/// Core helper: create a write queue wired to the given index.
fn setup_write_queue_with_index(
    tmp: &tempfile::TempDir,
    tenant_id: &str,
    index: Arc<crate::index::Index>,
) -> (
    WriteQueue,
    tokio::task::JoinHandle<crate::error::Result<()>>,
    Arc<dashmap::DashMap<String, TaskInfo>>,
) {
    let writers = Arc::new(dashmap::DashMap::new());
    let tasks: Arc<dashmap::DashMap<String, TaskInfo>> = Arc::new(dashmap::DashMap::new());
    let facet_cache = Arc::new(dashmap::DashMap::new());
    let lww_map = Arc::new(dashmap::DashMap::new());

    #[cfg(feature = "vector-search")]
    let vector_ctx = VectorWriteContext::new(Arc::new(dashmap::DashMap::new()));
    #[cfg(not(feature = "vector-search"))]
    let vector_ctx = VectorWriteContext::new();

    let (tx, handle) = create_write_queue(WriteQueueContext {
        tenant_id: tenant_id.to_string(),
        index,
        _writers: writers,
        tasks: Arc::clone(&tasks),
        base_path: tmp.path().to_path_buf(),
        oplog: None,
        facet_cache,
        lww_map,
        vector_ctx,
    });

    (tx, handle, tasks)
}

/// Convenience helper: create an index in a tenant subdirectory and wire up a queue.
fn setup_write_queue(
    tmp: &tempfile::TempDir,
    tenant_id: &str,
) -> (
    WriteQueue,
    tokio::task::JoinHandle<crate::error::Result<()>>,
    Arc<dashmap::DashMap<String, TaskInfo>>,
) {
    let tenant_path = tmp.path().join(tenant_id);
    std::fs::create_dir_all(&tenant_path).unwrap();
    let schema = crate::index::schema::Schema::builder().build();
    let index = Arc::new(crate::index::Index::create(&tenant_path, schema).unwrap());
    setup_write_queue_with_index(tmp, tenant_id, index)
}

/// Verify that two write queues sharing a memory budget with `max_concurrent_writers=1` both make progress and commit their documents within a 3-second deadline.
#[tokio::test]
async fn test_multiple_queues_progress_under_tight_writer_budget() {
    let tmp = tempfile::TempDir::new().unwrap();
    let shared_budget = Arc::new(MemoryBudget::new(MemoryBudgetConfig {
        max_concurrent_writers: 1,
        ..Default::default()
    }));
    let schema = crate::index::schema::Schema::builder().build();

    let tenant_a = tmp.path().join("budget_a");
    let tenant_b = tmp.path().join("budget_b");
    std::fs::create_dir_all(&tenant_a).unwrap();
    std::fs::create_dir_all(&tenant_b).unwrap();

    let index_a = Arc::new(
        crate::index::Index::create_with_budget(
            &tenant_a,
            schema.clone(),
            Arc::clone(&shared_budget),
        )
        .unwrap(),
    );
    let index_b = Arc::new(
        crate::index::Index::create_with_budget(&tenant_b, schema, Arc::clone(&shared_budget))
            .unwrap(),
    );

    let (tx_a, handle_a, tasks_a) =
        setup_write_queue_with_index(&tmp, "budget_a", Arc::clone(&index_a));
    let (tx_b, handle_b, tasks_b) =
        setup_write_queue_with_index(&tmp, "budget_b", Arc::clone(&index_b));

    let task_a = "budget_task_a".to_string();
    tasks_a.insert(task_a.clone(), TaskInfo::new(task_a.clone(), 1, 1));
    tx_a.send(WriteOp {
        task_id: task_a.clone(),
        actions: vec![WriteAction::Add(crate::types::Document {
            id: "a1".to_string(),
            fields: HashMap::from([(
                "name".to_string(),
                crate::types::FieldValue::Text("A".to_string()),
            )]),
        })],
    })
    .await
    .unwrap();

    let task_b = "budget_task_b".to_string();
    tasks_b.insert(task_b.clone(), TaskInfo::new(task_b.clone(), 2, 1));
    tx_b.send(WriteOp {
        task_id: task_b.clone(),
        actions: vec![WriteAction::Add(crate::types::Document {
            id: "b1".to_string(),
            fields: HashMap::from([(
                "name".to_string(),
                crate::types::FieldValue::Text("B".to_string()),
            )]),
        })],
    })
    .await
    .unwrap();

    let wait = async {
        loop {
            let a_done = tasks_a
                .get(&task_a)
                .is_some_and(|t| matches!(t.status, crate::types::TaskStatus::Succeeded));
            let b_done = tasks_b
                .get(&task_b)
                .is_some_and(|t| matches!(t.status, crate::types::TaskStatus::Succeeded));
            if a_done && b_done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    };

    tokio::time::timeout(std::time::Duration::from_secs(3), wait)
        .await
        .expect("both queues should make progress under tight writer budget");

    drop(tx_a);
    drop(tx_b);
    handle_a.await.unwrap().unwrap();
    handle_b.await.unwrap().unwrap();

    // Verify documents are actually committed and searchable — not just task status.
    let searcher_a = index_a.reader().searcher();
    let count_a: usize = searcher_a
        .segment_readers()
        .iter()
        .map(|s| s.num_docs() as usize)
        .sum();
    assert_eq!(count_a, 1, "index_a should contain 1 committed document");

    let searcher_b = index_b.reader().searcher();
    let count_b: usize = searcher_b
        .segment_readers()
        .iter()
        .map(|s| s.num_docs() as usize)
        .sum();
    assert_eq!(count_b, 1, "index_b should contain 1 committed document");
}

/// Verify that adding two documents through the write queue results in a succeeded task with `indexed_documents == 2`.
#[tokio::test]
async fn test_commit_batch_basic_add() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (tx, handle, tasks) = setup_write_queue(&tmp, "test_tenant");

    let task_id = "test_task_1".to_string();
    let task = TaskInfo::new(task_id.clone(), 1, 2);
    tasks.insert(task_id.clone(), task);

    let doc1 = crate::types::Document {
        id: "doc1".to_string(),
        fields: HashMap::from([(
            "name".to_string(),
            crate::types::FieldValue::Text("Alice".to_string()),
        )]),
    };
    let doc2 = crate::types::Document {
        id: "doc2".to_string(),
        fields: HashMap::from([(
            "name".to_string(),
            crate::types::FieldValue::Text("Bob".to_string()),
        )]),
    };

    tx.send(WriteOp {
        task_id: task_id.clone(),
        actions: vec![WriteAction::Add(doc1), WriteAction::Add(doc2)],
    })
    .await
    .unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    let final_task = tasks.get(&task_id).unwrap();
    assert!(
        matches!(final_task.status, crate::types::TaskStatus::Succeeded),
        "task should succeed, got: {:?}",
        final_task.status
    );
    assert_eq!(final_task.indexed_documents, 2);
}

/// Verify that upserting an existing document succeeds and counts as one indexed document.
#[tokio::test]
async fn test_commit_batch_upsert() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (tx, handle, tasks) = setup_write_queue(&tmp, "upsert_tenant");

    // Add a document first
    let task_id_1 = "upsert_task_1".to_string();
    tasks.insert(task_id_1.clone(), TaskInfo::new(task_id_1.clone(), 1, 1));
    let doc = crate::types::Document {
        id: "doc1".to_string(),
        fields: HashMap::from([(
            "name".to_string(),
            crate::types::FieldValue::Text("Alice".to_string()),
        )]),
    };
    tx.send(WriteOp {
        task_id: task_id_1.clone(),
        actions: vec![WriteAction::Add(doc)],
    })
    .await
    .unwrap();

    // Give the write queue time to process
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Upsert the same doc with updated content
    let task_id_2 = "upsert_task_2".to_string();
    tasks.insert(task_id_2.clone(), TaskInfo::new(task_id_2.clone(), 2, 1));
    let doc_updated = crate::types::Document {
        id: "doc1".to_string(),
        fields: HashMap::from([(
            "name".to_string(),
            crate::types::FieldValue::Text("Alice Updated".to_string()),
        )]),
    };
    tx.send(WriteOp {
        task_id: task_id_2.clone(),
        actions: vec![WriteAction::Upsert(doc_updated)],
    })
    .await
    .unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    let final_task = tasks.get(&task_id_2).unwrap();
    assert!(
        matches!(final_task.status, crate::types::TaskStatus::Succeeded),
        "upsert task should succeed, got: {:?}",
        final_task.status
    );
    assert_eq!(final_task.indexed_documents, 1);
}

/// Verify that deleting a previously added document succeeds and counts as one indexed document in the task status.
#[tokio::test]
async fn test_commit_batch_delete() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (tx, handle, tasks) = setup_write_queue(&tmp, "delete_tenant");

    // Add a document first
    let task_id_1 = "del_task_1".to_string();
    tasks.insert(task_id_1.clone(), TaskInfo::new(task_id_1.clone(), 1, 1));
    let doc = crate::types::Document {
        id: "doc1".to_string(),
        fields: HashMap::from([(
            "name".to_string(),
            crate::types::FieldValue::Text("Alice".to_string()),
        )]),
    };
    tx.send(WriteOp {
        task_id: task_id_1.clone(),
        actions: vec![WriteAction::Add(doc)],
    })
    .await
    .unwrap();

    // Give the write queue time to process
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Delete the doc
    let task_id_2 = "del_task_2".to_string();
    tasks.insert(task_id_2.clone(), TaskInfo::new(task_id_2.clone(), 2, 1));
    tx.send(WriteOp {
        task_id: task_id_2.clone(),
        actions: vec![WriteAction::Delete("doc1".to_string())],
    })
    .await
    .unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    let final_task = tasks.get(&task_id_2).unwrap();
    assert!(
        matches!(final_task.status, crate::types::TaskStatus::Succeeded),
        "delete task should succeed, got: {:?}",
        final_task.status
    );
    // Delete counts as 1 indexed document (it's a successful write operation)
    assert_eq!(final_task.indexed_documents, 1);
}

/// Verify that `VectorWriteContext` shares the same `DashMap` instance via `Arc`, so mutations through the map are visible through the context.
#[cfg(feature = "vector-search")]
#[tokio::test]
async fn test_vector_write_context_shares_dashmap() {
    // Verify that VectorWriteContext properly shares the same DashMap instance
    let vector_indices: Arc<
        dashmap::DashMap<String, Arc<std::sync::RwLock<crate::vector::index::VectorIndex>>>,
    > = Arc::new(dashmap::DashMap::new());

    let ctx = VectorWriteContext::new(Arc::clone(&vector_indices));

    // Insert into the shared DashMap
    let vi = crate::vector::index::VectorIndex::new(3, usearch::ffi::MetricKind::Cos).unwrap();
    vector_indices.insert(
        "test_tenant".to_string(),
        Arc::new(std::sync::RwLock::new(vi)),
    );

    // The context should see the same data (same Arc)
    assert!(ctx.vector_indices.contains_key("test_tenant"));
    assert_eq!(ctx.vector_indices.len(), 1);
}

/// Verify that `create_write_queue` accepts a `VectorWriteContext` with vector indices and successfully processes a document add through the full pipeline.
#[cfg(feature = "vector-search")]
#[tokio::test]
async fn test_create_write_queue_with_vector_indices() {
    let tmp = tempfile::TempDir::new().unwrap();
    let tenant_id = "vec_tenant";
    let tenant_path = tmp.path().join(tenant_id);
    std::fs::create_dir_all(&tenant_path).unwrap();
    let schema = crate::index::schema::Schema::builder().build();
    let index = Arc::new(crate::index::Index::create(&tenant_path, schema).unwrap());

    let writers = Arc::new(dashmap::DashMap::new());
    let tasks: Arc<dashmap::DashMap<String, TaskInfo>> = Arc::new(dashmap::DashMap::new());
    let facet_cache = Arc::new(dashmap::DashMap::new());
    let lww_map = Arc::new(dashmap::DashMap::new());
    let vector_indices: Arc<
        dashmap::DashMap<String, Arc<std::sync::RwLock<crate::vector::index::VectorIndex>>>,
    > = Arc::new(dashmap::DashMap::new());

    let vector_ctx = VectorWriteContext::new(vector_indices);

    let (tx, handle) = create_write_queue(WriteQueueContext {
        tenant_id: tenant_id.to_string(),
        index,
        _writers: writers,
        tasks: Arc::clone(&tasks),
        base_path: tmp.path().to_path_buf(),
        oplog: None,
        facet_cache,
        lww_map,
        vector_ctx,
    });

    let task_id = "vec_task_1".to_string();
    let task = TaskInfo::new(task_id.clone(), 1, 1);
    tasks.insert(task_id.clone(), task);

    let doc = crate::types::Document {
        id: "doc1".to_string(),
        fields: HashMap::from([(
            "title".to_string(),
            crate::types::FieldValue::Text("Hello vectors".to_string()),
        )]),
    };

    tx.send(WriteOp {
        task_id: task_id.clone(),
        actions: vec![WriteAction::Add(doc)],
    })
    .await
    .unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    let final_task = tasks.get(&task_id).unwrap();
    assert!(
        matches!(final_task.status, crate::types::TaskStatus::Succeeded),
        "task should succeed with vector_indices plumbing, got: {:?}",
        final_task.status
    );
    assert_eq!(final_task.indexed_documents, 1);
}

// ── Auto-embedding integration tests (7.11) ──

#[cfg(feature = "vector-search")]
mod auto_embed_tests {
    use super::*;
    use crate::types::FieldValue;
    use serial_test::serial;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    type VectorIndicesMap =
        Arc<dashmap::DashMap<String, Arc<std::sync::RwLock<crate::vector::index::VectorIndex>>>>;
    type EmbedderWriteQueueSetup = (
        WriteQueue,
        tokio::task::JoinHandle<crate::error::Result<()>>,
        Arc<dashmap::DashMap<String, TaskInfo>>,
        VectorIndicesMap,
    );
    type OplogWriteQueueSetup = (
        WriteQueue,
        tokio::task::JoinHandle<crate::error::Result<()>>,
        Arc<dashmap::DashMap<String, TaskInfo>>,
        VectorIndicesMap,
        Arc<crate::index::oplog::OpLog>,
    );

    /// Shared core: creates tenant dir, writes settings, builds WriteQueueContext.
    fn setup_write_queue_core(
        tmp: &tempfile::TempDir,
        tenant_id: &str,
        embedder_settings: Option<HashMap<String, serde_json::Value>>,
        oplog: Option<Arc<crate::index::oplog::OpLog>>,
    ) -> (
        WriteQueue,
        tokio::task::JoinHandle<crate::error::Result<()>>,
        Arc<dashmap::DashMap<String, TaskInfo>>,
        VectorIndicesMap,
    ) {
        let tenant_path = tmp.path().join(tenant_id);
        std::fs::create_dir_all(&tenant_path).unwrap();

        let settings = crate::index::settings::IndexSettings {
            embedders: embedder_settings,
            ..Default::default()
        };
        let settings_json = serde_json::to_string_pretty(&settings).unwrap();
        std::fs::write(tenant_path.join("settings.json"), settings_json).unwrap();

        let schema = crate::index::schema::Schema::builder().build();
        let index = Arc::new(crate::index::Index::create(&tenant_path, schema).unwrap());

        let writers = Arc::new(dashmap::DashMap::new());
        let tasks: Arc<dashmap::DashMap<String, TaskInfo>> = Arc::new(dashmap::DashMap::new());
        let facet_cache = Arc::new(dashmap::DashMap::new());
        let lww_map = Arc::new(dashmap::DashMap::new());
        let vector_indices: VectorIndicesMap = Arc::new(dashmap::DashMap::new());
        let vector_ctx = VectorWriteContext::new(Arc::clone(&vector_indices));

        let (tx, handle) = create_write_queue(WriteQueueContext {
            tenant_id: tenant_id.to_string(),
            index,
            _writers: writers,
            tasks: Arc::clone(&tasks),
            base_path: tmp.path().to_path_buf(),
            oplog,
            facet_cache,
            lww_map,
            vector_ctx,
        });

        (tx, handle, tasks, vector_indices)
    }

    /// Helper to create a write queue with embedder settings (no oplog).
    fn setup_write_queue_with_embedder(
        tmp: &tempfile::TempDir,
        tenant_id: &str,
        embedder_settings: Option<HashMap<String, serde_json::Value>>,
    ) -> EmbedderWriteQueueSetup {
        setup_write_queue_core(tmp, tenant_id, embedder_settings, None)
    }

    /// Create REST embedder config JSON (single-input template).
    fn rest_embedder_config(server_uri: &str, dimensions: usize) -> serde_json::Value {
        serde_json::json!({
            "source": "rest",
            "url": format!("{}/embed", server_uri),
            "request": {"input": "{{text}}"},
            "response": {"embedding": "{{embedding}}"},
            "dimensions": dimensions
        })
    }

    /// Create batch REST embedder config JSON.
    fn rest_embedder_batch_config(server_uri: &str, dimensions: usize) -> serde_json::Value {
        serde_json::json!({
            "source": "rest",
            "url": format!("{}/embed", server_uri),
            "request": {"inputs": ["{{text}}", "{{..}}"]},
            "response": {"embeddings": ["{{embedding}}", "{{..}}"]},
            "dimensions": dimensions
        })
    }

    // ── Add/Upsert tests ──

    /// Verify that adding a document with a REST embedder configured automatically embeds the document and stores the resulting vector in the auto-created VectorIndex.
    #[tokio::test]
    async fn test_auto_embed_on_add() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.1, 0.2, 0.3]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "embed_t", Some(embedders));

        let task_id = "embed_add_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("Hello vectors".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(
            matches!(final_task.status, TaskStatus::Succeeded),
            "task should succeed, got: {:?}",
            final_task.status
        );

        // Verify vector index was auto-created and has the document
        assert!(
            vector_indices.contains_key("embed_t"),
            "vector index should be auto-created"
        );
        let vi_lock = vector_indices.get("embed_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 1, "vector index should have 1 document");

        let results = vi.search(&[0.1, 0.2, 0.3], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "doc1");
    }

    /// Verify that upserting a document re-embeds it and replaces the previous vector in the VectorIndex rather than duplicating the entry.
    #[tokio::test]
    async fn test_auto_embed_on_upsert_replaces_vector() {
        use wiremock::matchers::body_string_contains;

        let server = MockServer::start().await;
        // Use body content matching to return different vectors for
        // each request — deterministic, no reliance on mock ordering.
        Mock::given(method("POST"))
            .and(body_string_contains("first version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [1.0, 0.0, 0.0]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("updated version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.0, 0.0, 1.0]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "upsert_t", Some(embedders));

        // Add initial doc — body contains "first version" → gets [1,0,0]
        let task1 = "upsert_vec_t1".to_string();
        tasks.insert(task1.clone(), TaskInfo::new(task1.clone(), 1, 1));
        tx.send(WriteOp {
            task_id: task1.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("first version".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Verify initial vector is [1,0,0]
        {
            let vi_lock = vector_indices.get("upsert_t").unwrap();
            let vi = vi_lock.read().unwrap();
            assert_eq!(vi.len(), 1);
            let results = vi.search(&[1.0, 0.0, 0.0], 1).unwrap();
            assert_eq!(results[0].doc_id, "doc1");
            assert!(
                results[0].distance < 0.01,
                "initial vector should be close to [1,0,0], distance={}",
                results[0].distance
            );
        }

        // Upsert same doc — body contains "updated version" → gets [0,0,1]
        let task2 = "upsert_vec_t2".to_string();
        tasks.insert(task2.clone(), TaskInfo::new(task2.clone(), 2, 1));
        tx.send(WriteOp {
            task_id: task2.clone(),
            actions: vec![WriteAction::Upsert(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("updated version".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let vi_lock = vector_indices.get("upsert_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 1, "should still have just 1 document");

        // Vector should now be [0,0,1] — verify it actually changed
        let results = vi.search(&[0.0, 0.0, 1.0], 1).unwrap();
        assert_eq!(results[0].doc_id, "doc1");
        assert!(
            results[0].distance < 0.01,
            "upserted vector should be close to [0,0,1], distance={}",
            results[0].distance
        );
    }

    /// Verify that multiple documents in a single write operation are embedded in one batched HTTP request and all stored in the VectorIndex.
    #[tokio::test]
    async fn test_batch_embed_multiple_docs() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embeddings": [
                    [0.1, 0.0, 0.0],
                    [0.0, 0.2, 0.0],
                    [0.0, 0.0, 0.3],
                    [0.4, 0.0, 0.0],
                    [0.0, 0.5, 0.0]
                ]
            })))
            .expect(1) // Exactly 1 HTTP request for all 5 docs
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_batch_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "batch_t", Some(embedders));

        let task_id = "batch_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 5));

        let actions: Vec<WriteAction> = (1..=5)
            .map(|i| {
                WriteAction::Add(crate::types::Document {
                    id: format!("doc{i}"),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text(format!("Document {i}")),
                    )]),
                })
            })
            .collect();

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions,
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let vi_lock = vector_indices.get("batch_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 5, "all 5 docs should be in vector index");
    }

    /// Verify that the VectorIndex DashMap entry is lazily created on the first embedded document when no prior vector index exists for the tenant.
    #[tokio::test]
    async fn test_vector_index_auto_created() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.1, 0.2, 0.3]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "autocreate_t", Some(embedders));

        // No VectorIndex exists yet
        assert!(!vector_indices.contains_key("autocreate_t"));

        let task_id = "autocreate_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("first doc".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        assert!(
            vector_indices.contains_key("autocreate_t"),
            "VectorIndex should be auto-created on first doc"
        );
        let vi_lock = vector_indices.get("autocreate_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.dimensions(), 3, "dimensions should match embedding size");
        assert_eq!(vi.len(), 1);
    }

    // ── User-provided vector tests ──

    /// Verify that a `userProvided` embedder stores the `_vectors` field directly in the VectorIndex without making any HTTP embedding requests.
    #[tokio::test]
    async fn test_vectors_field_used_directly() {
        let server = MockServer::start().await;
        // Zero HTTP requests expected for userProvided
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "userprov_t", Some(embedders));

        let task_id = "userprov_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        let mut fields = HashMap::new();
        fields.insert("title".to_string(), FieldValue::Text("Hello".to_string()));
        let mut vectors_map = HashMap::new();
        vectors_map.insert(
            "default".to_string(),
            FieldValue::Array(vec![
                FieldValue::Float(0.1),
                FieldValue::Float(0.2),
                FieldValue::Float(0.3),
            ]),
        );
        fields.insert("_vectors".to_string(), FieldValue::Object(vectors_map));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields,
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Vector should be stored directly from _vectors
        assert!(vector_indices.contains_key("userprov_t"));
        let vi_lock = vector_indices.get("userprov_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 1);
        let results = vi.search(&[0.1, 0.2, 0.3], 1).unwrap();
        assert_eq!(results[0].doc_id, "doc1");
    }

    /// Verify that a document whose `_vectors` field has the wrong number of dimensions is rejected while correctly-dimensioned documents in the same batch succeed.
    #[tokio::test]
    async fn test_vectors_field_wrong_dimensions_rejected() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "wrongdim_t", Some(embedders));

        let task_id = "wrongdim_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 2));

        // Good doc: correct dimensions
        let mut fields_ok = HashMap::new();
        fields_ok.insert(
            "title".to_string(),
            FieldValue::Text("Good doc".to_string()),
        );
        let mut vectors_ok = HashMap::new();
        vectors_ok.insert(
            "default".to_string(),
            FieldValue::Array(vec![
                FieldValue::Float(0.1),
                FieldValue::Float(0.2),
                FieldValue::Float(0.3),
            ]),
        );
        fields_ok.insert("_vectors".to_string(), FieldValue::Object(vectors_ok));

        // Bad doc: wrong dimensions (2 instead of 3)
        let mut fields_bad = HashMap::new();
        fields_bad.insert("title".to_string(), FieldValue::Text("Bad doc".to_string()));
        let mut vectors_bad = HashMap::new();
        vectors_bad.insert(
            "default".to_string(),
            FieldValue::Array(vec![FieldValue::Float(0.1), FieldValue::Float(0.2)]),
        );
        fields_bad.insert("_vectors".to_string(), FieldValue::Object(vectors_bad));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![
                WriteAction::Add(crate::types::Document {
                    id: "good".to_string(),
                    fields: fields_ok,
                }),
                WriteAction::Add(crate::types::Document {
                    id: "bad".to_string(),
                    fields: fields_bad,
                }),
            ],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(matches!(final_task.status, TaskStatus::Succeeded));

        // Good doc should be in vector index
        let vi_lock = vector_indices.get("wrongdim_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 1, "only good doc should be in vector index");

        // Bad doc should be rejected
        assert!(
            !final_task.rejected_documents.is_empty(),
            "bad doc should be rejected"
        );
    }

    // ── Fallback/error tests ──

    /// Verify that no VectorIndex is created and documents are indexed normally when no embedder configuration exists in settings.
    #[tokio::test]
    async fn test_no_embed_without_embedder_config() {
        let tmp = tempfile::TempDir::new().unwrap();

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "noembed_t", None);

        let task_id = "noembed_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("no embedder".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(matches!(final_task.status, TaskStatus::Succeeded));
        assert_eq!(final_task.indexed_documents, 1);

        // No VectorIndex should be created
        assert!(
            !vector_indices.contains_key("noembed_t"),
            "no vector index without embedder config"
        );
    }

    /// Verify that when the embedding server returns an error, the document is still indexed in Tantivy and the task succeeds; only the VectorIndex entry is skipped.
    #[tokio::test]
    async fn test_embed_failure_does_not_block_tantivy() {
        let server = MockServer::start().await;
        // Server returns 500 — embedding fails
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "fail_t", Some(embedders));

        let task_id = "fail_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("failing embed".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Document should still be indexed in Tantivy
        let final_task = tasks.get(&task_id).unwrap();
        assert!(
            matches!(final_task.status, TaskStatus::Succeeded),
            "task should succeed despite embed failure"
        );
        assert_eq!(
            final_task.indexed_documents, 1,
            "doc should be indexed in Tantivy"
        );

        // VectorIndex should NOT have the doc
        let vi_count = vector_indices
            .get("fail_t")
            .map(|r| r.read().unwrap().len())
            .unwrap_or(0);
        assert_eq!(
            vi_count, 0,
            "vector index should be empty after embed failure"
        );
    }

    /// Verify that a `userProvided` embedder silently skips documents that lack a `_vectors` field, indexing them in Tantivy without error.
    #[tokio::test]
    async fn test_user_provided_source_no_vectors_field_skipped() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "novec_t", Some(embedders));

        let task_id = "novec_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        // Document without _vectors field + userProvided source
        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("no vectors".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(matches!(final_task.status, TaskStatus::Succeeded));
        assert_eq!(final_task.indexed_documents, 1);

        // No vector stored
        let vi_count = vector_indices
            .get("novec_t")
            .map(|r| r.read().unwrap().len())
            .unwrap_or(0);
        assert_eq!(vi_count, 0, "no vectors should be stored");
    }

    // ── Delete tests ──

    /// Verify that deleting a document removes its entry from the VectorIndex, leaving the index empty.
    #[tokio::test]
    async fn test_delete_removes_from_vector_index() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.5, 0.5, 0.5]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "del_vec_t", Some(embedders));

        // Add a document
        let task1 = "del_vec_t1".to_string();
        tasks.insert(task1.clone(), TaskInfo::new(task1.clone(), 1, 1));
        tx.send(WriteOp {
            task_id: task1.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("to be deleted".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Delete the document
        let task2 = "del_vec_t2".to_string();
        tasks.insert(task2.clone(), TaskInfo::new(task2.clone(), 2, 1));
        tx.send(WriteOp {
            task_id: task2.clone(),
            actions: vec![WriteAction::Delete("doc1".to_string())],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let vi_lock = vector_indices.get("del_vec_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(
            vi.len(),
            0,
            "doc should be removed from vector index after delete"
        );
    }

    /// Verify that deleting a document ID that was never added to the VectorIndex succeeds silently without errors.
    #[tokio::test]
    async fn test_delete_nonexistent_in_vector_index_silent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );

        let (tx, handle, tasks, _vector_indices) =
            setup_write_queue_with_embedder(&tmp, "delnone_t", Some(embedders));

        // Delete a doc that was never added
        let task_id = "delnone_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));
        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Delete("nonexistent".to_string())],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(
            matches!(final_task.status, TaskStatus::Succeeded),
            "delete should succeed even for nonexistent doc"
        );
    }

    // ── Stripping test ──

    /// Verify that `_vectors` is stripped from the Tantivy document so large float arrays are not stored in the full-text index, while the vector is still written to the VectorIndex.
    #[tokio::test]
    async fn test_vectors_field_stripped_from_tantivy() {
        let tmp = tempfile::TempDir::new().unwrap();
        let tenant_id = "strip_t";
        let tenant_path = tmp.path().join(tenant_id);
        std::fs::create_dir_all(&tenant_path).unwrap();

        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );
        let settings = crate::index::settings::IndexSettings {
            embedders: Some(embedders),
            ..Default::default()
        };
        std::fs::write(
            tenant_path.join("settings.json"),
            serde_json::to_string_pretty(&settings).unwrap(),
        )
        .unwrap();

        let schema = crate::index::schema::Schema::builder().build();
        let index = Arc::new(crate::index::Index::create(&tenant_path, schema).unwrap());

        let writers = Arc::new(dashmap::DashMap::new());
        let tasks: Arc<dashmap::DashMap<String, TaskInfo>> = Arc::new(dashmap::DashMap::new());
        let facet_cache = Arc::new(dashmap::DashMap::new());
        let lww_map = Arc::new(dashmap::DashMap::new());
        let vector_indices: VectorIndicesMap = Arc::new(dashmap::DashMap::new());
        let vector_ctx = VectorWriteContext::new(Arc::clone(&vector_indices));

        let (tx, handle) = create_write_queue(WriteQueueContext {
            tenant_id: tenant_id.to_string(),
            index: Arc::clone(&index),
            _writers: writers,
            tasks: Arc::clone(&tasks),
            base_path: tmp.path().to_path_buf(),
            oplog: None,
            facet_cache,
            lww_map,
            vector_ctx,
        });

        let task_id = "strip_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldValue::Text("test stripping".to_string()),
        );
        let mut vectors_map = HashMap::new();
        vectors_map.insert(
            "default".to_string(),
            FieldValue::Array(vec![
                FieldValue::Float(0.1),
                FieldValue::Float(0.2),
                FieldValue::Float(0.3),
            ]),
        );
        fields.insert("_vectors".to_string(), FieldValue::Object(vectors_map));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields,
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Vector should be in VectorIndex
        assert!(vector_indices.contains_key(tenant_id));

        // Read back from Tantivy — _vectors should NOT be stored
        index.reader().reload().unwrap();
        let searcher = index.reader().searcher();
        let top_docs = searcher
            .search(
                &tantivy::query::AllQuery,
                &tantivy::collector::TopDocs::with_limit(10),
            )
            .unwrap();
        assert_eq!(top_docs.len(), 1, "should have 1 document in Tantivy");

        let doc: tantivy::TantivyDocument = searcher.doc(top_docs[0].1).unwrap();
        let tantivy_schema = index.inner().schema();
        // Import the Document trait for to_json()
        use tantivy::schema::document::Document as TantivyDocTrait;
        let doc_json_str = doc.to_json(&tantivy_schema);
        assert!(
            !doc_json_str.contains("_vectors"),
            "_vectors should be stripped from Tantivy document, got: {doc_json_str}"
        );
    }

    // ── Vector index disk persistence tests (8.1) ──

    /// Verify that `index.usearch` and `id_map.json` are written to the `vectors/` directory after a commit and that the loaded index is searchable with the correct dimensions.
    #[tokio::test]
    async fn test_vector_index_saved_after_commit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.1, 0.2, 0.3]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vector_indices) =
            setup_write_queue_with_embedder(&tmp, "save_t", Some(embedders));

        let task_id = "save_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 2));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![
                WriteAction::Add(crate::types::Document {
                    id: "doc1".to_string(),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text("First document".to_string()),
                    )]),
                }),
                WriteAction::Add(crate::types::Document {
                    id: "doc2".to_string(),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text("Second document".to_string()),
                    )]),
                }),
            ],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Verify vector files exist on disk
        let vectors_dir = tmp.path().join("save_t").join("vectors");
        assert!(
            vectors_dir.join("index.usearch").exists(),
            "index.usearch should exist on disk after commit"
        );
        assert!(
            vectors_dir.join("id_map.json").exists(),
            "id_map.json should exist on disk after commit"
        );

        // Load from disk and verify searchable with correct dimensions
        let loaded =
            crate::vector::index::VectorIndex::load(&vectors_dir, usearch::ffi::MetricKind::Cos)
                .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.dimensions(), 3);

        let results = loaded.search(&[0.1, 0.2, 0.3], 2).unwrap();
        assert_eq!(results.len(), 2);
    }

    /// Verify that deleting a document persists the removal to disk so that loading the saved VectorIndex excludes the deleted document.
    #[tokio::test]
    async fn test_vector_index_save_reflects_deletes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.5, 0.5, 0.5]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vector_indices) =
            setup_write_queue_with_embedder(&tmp, "savedel_t", Some(embedders));

        // Add two docs
        let task1 = "savedel_t1".to_string();
        tasks.insert(task1.clone(), TaskInfo::new(task1.clone(), 1, 2));
        tx.send(WriteOp {
            task_id: task1.clone(),
            actions: vec![
                WriteAction::Add(crate::types::Document {
                    id: "doc1".to_string(),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text("First".to_string()),
                    )]),
                }),
                WriteAction::Add(crate::types::Document {
                    id: "doc2".to_string(),
                    fields: HashMap::from([(
                        "title".to_string(),
                        FieldValue::Text("Second".to_string()),
                    )]),
                }),
            ],
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Delete one doc
        let task2 = "savedel_t2".to_string();
        tasks.insert(task2.clone(), TaskInfo::new(task2.clone(), 2, 1));
        tx.send(WriteOp {
            task_id: task2.clone(),
            actions: vec![WriteAction::Delete("doc1".to_string())],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Load from disk and verify doc1 is not in the index
        let vectors_dir = tmp.path().join("savedel_t").join("vectors");
        let loaded =
            crate::vector::index::VectorIndex::load(&vectors_dir, usearch::ffi::MetricKind::Cos)
                .unwrap();
        assert_eq!(loaded.len(), 1, "only doc2 should remain after delete");

        let results = loaded.search(&[0.5, 0.5, 0.5], 1).unwrap();
        assert_eq!(results[0].doc_id, "doc2");
    }

    /// Verify that no `vectors/` directory is created on disk when no embedder is configured and no vector mutations occur.
    #[tokio::test]
    async fn test_vector_save_skipped_when_no_vector_changes() {
        let tmp = tempfile::TempDir::new().unwrap();
        // No embedder configured
        let (tx, handle, tasks, _vector_indices) =
            setup_write_queue_with_embedder(&tmp, "novec_save_t", None);

        let task_id = "novec_save_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("no vectors".into()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // No vectors/ directory should exist
        let vectors_dir = tmp.path().join("novec_save_t").join("vectors");
        assert!(
            !vectors_dir.exists(),
            "vectors/ directory should not be created without embedder"
        );
    }

    /// Verify that upserting a document replaces its vector on disk so that loading the persisted VectorIndex returns only the updated embedding.
    #[tokio::test]
    async fn test_vector_index_save_reflects_upserts() {
        let server = MockServer::start().await;
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .respond_with(move |_req: &wiremock::Request| {
                let n = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // First call returns [0.1, 0.2, 0.3], second returns [0.9, 0.8, 0.7]
                let vec = if n == 0 {
                    vec![0.1, 0.2, 0.3]
                } else {
                    vec![0.9, 0.8, 0.7]
                };
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "embedding": vec
                }))
            })
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vector_indices) =
            setup_write_queue_with_embedder(&tmp, "upsert_save_t", Some(embedders));

        // Add doc1
        let task1 = "upsert_t1".to_string();
        tasks.insert(task1.clone(), TaskInfo::new(task1.clone(), 1, 1));
        tx.send(WriteOp {
            task_id: task1.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("original".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Upsert doc1 with new content (gets new embedding)
        let task2 = "upsert_t2".to_string();
        tasks.insert(task2.clone(), TaskInfo::new(task2.clone(), 2, 1));
        tx.send(WriteOp {
            task_id: task2.clone(),
            actions: vec![WriteAction::Upsert(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("updated".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Load from disk and verify only 1 doc with updated vector
        let vectors_dir = tmp.path().join("upsert_save_t").join("vectors");
        let loaded =
            crate::vector::index::VectorIndex::load(&vectors_dir, usearch::ffi::MetricKind::Cos)
                .unwrap();
        assert_eq!(loaded.len(), 1, "upsert should replace, not duplicate");

        let results = loaded.search(&[0.9, 0.8, 0.7], 1).unwrap();
        assert_eq!(results[0].doc_id, "doc1");
    }

    // ── Oplog vector storage tests (8.7) ──

    /// TODO: Document setup_write_queue_with_oplog.
    fn setup_write_queue_with_oplog(
        tmp: &tempfile::TempDir,
        tenant_id: &str,
        embedder_settings: Option<HashMap<String, serde_json::Value>>,
    ) -> OplogWriteQueueSetup {
        let tenant_path = tmp.path().join(tenant_id);
        std::fs::create_dir_all(&tenant_path).unwrap();

        let oplog_dir = tenant_path.join("oplog");
        let oplog =
            Arc::new(crate::index::oplog::OpLog::open(&oplog_dir, tenant_id, "test_node").unwrap());

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_core(tmp, tenant_id, embedder_settings, Some(Arc::clone(&oplog)));

        (tx, handle, tasks, vector_indices, oplog)
    }

    /// Extract vectors for a named embedder from the first upsert entry in the oplog.
    fn extract_oplog_vectors(oplog: &crate::index::oplog::OpLog, embedder_name: &str) -> Vec<f64> {
        let entries = oplog.read_since(0).unwrap();
        let upsert = entries
            .iter()
            .find(|e| e.op_type == "upsert")
            .expect("should have an upsert entry");
        let body = upsert.payload.get("body").expect("upsert should have body");
        let vectors = body.get("_vectors").expect("body should contain _vectors");
        let embedder_vec = vectors
            .get(embedder_name)
            .unwrap_or_else(|| panic!("_vectors should have '{embedder_name}' embedder"));
        embedder_vec
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect()
    }

    /// TODO: Document test_computed_vectors_stored_in_oplog.
    #[tokio::test]
    async fn test_computed_vectors_stored_in_oplog() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.1, 0.2, 0.3]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vi, oplog) =
            setup_write_queue_with_oplog(&tmp, "oplog_vec_t", Some(embedders));

        let task_id = "oplog_vec_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("test oplog vectors".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Read oplog and verify computed vectors are stored
        let vec_array = extract_oplog_vectors(&oplog, "default");
        assert_eq!(vec_array.len(), 3);
        assert!((vec_array[0] - 0.1).abs() < 0.01);
        assert!((vec_array[1] - 0.2).abs() < 0.01);
        assert!((vec_array[2] - 0.3).abs() < 0.01);
    }

    /// TODO: Document test_user_provided_vectors_preserved_in_oplog.
    #[tokio::test]
    async fn test_user_provided_vectors_preserved_in_oplog() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({
                "source": "userProvided",
                "dimensions": 3
            }),
        );

        let (tx, handle, tasks, _vi, oplog) =
            setup_write_queue_with_oplog(&tmp, "oplog_user_t", Some(embedders));

        let task_id = "oplog_user_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        let mut fields = HashMap::new();
        fields.insert(
            "title".to_string(),
            FieldValue::Text("user vectors".to_string()),
        );
        let mut vectors_map = HashMap::new();
        vectors_map.insert(
            "default".to_string(),
            FieldValue::Array(vec![
                FieldValue::Float(1.0),
                FieldValue::Float(0.0),
                FieldValue::Float(0.0),
            ]),
        );
        fields.insert("_vectors".to_string(), FieldValue::Object(vectors_map));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields,
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Read oplog and verify user-provided vectors are preserved
        let vec_array = extract_oplog_vectors(&oplog, "default");
        assert_eq!(vec_array, vec![1.0, 0.0, 0.0]);
    }

    /// TODO: Document test_oplog_vectors_contain_all_embedder_results.
    #[tokio::test]
    async fn test_oplog_vectors_contain_all_embedder_results() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.5, 0.5, 0.5]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        // Two REST embedders with different names
        embedders.insert(
            "embedder_a".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );
        embedders.insert(
            "embedder_b".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vi, oplog) =
            setup_write_queue_with_oplog(&tmp, "oplog_multi_t", Some(embedders));

        let task_id = "oplog_multi_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("multi embedder doc".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Read oplog and verify both embedders' vectors are present
        let vec_a = extract_oplog_vectors(&oplog, "embedder_a");
        assert_eq!(vec_a.len(), 3);

        let vec_b = extract_oplog_vectors(&oplog, "embedder_b");
        assert_eq!(vec_b.len(), 3);
    }

    /// Verify that an `EmbedderFingerprint` JSON file is written to the vectors directory alongside the usearch index after a commit that modifies vectors.
    #[tokio::test]
    async fn test_fingerprint_saved_alongside_vector_index() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "embedding": [0.1, 0.2, 0.3]
            })))
            .mount(&server)
            .await;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            rest_embedder_config(&server.uri(), 3),
        );

        let (tx, handle, tasks, _vi, _oplog) =
            setup_write_queue_with_oplog(&tmp, "fp_save_t", Some(embedders));

        let task_id = "fp_save_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("fingerprint test".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Verify fingerprint.json exists alongside vector files
        let vectors_dir = tmp.path().join("fp_save_t").join("vectors");
        assert!(
            vectors_dir.join("index.usearch").exists(),
            "index.usearch should exist"
        );
        assert!(
            vectors_dir.join("fingerprint.json").exists(),
            "fingerprint.json should exist alongside vector files"
        );

        // Load and verify fingerprint content
        let fp = crate::vector::config::EmbedderFingerprint::load(&vectors_dir).unwrap();
        assert_eq!(fp.version, 1);
        assert_eq!(fp.embedders.len(), 1);
        assert_eq!(fp.embedders[0].name, "default");
        assert_eq!(
            fp.embedders[0].source,
            crate::vector::config::EmbedderSource::Rest
        );
        assert_eq!(fp.embedders[0].dimensions, 3);
    }

    // ── FastEmbed integration tests (9.16) ──

    /// Verify that the local FastEmbed model (BGESmallENV15) automatically embeds a document on add and produces 384-dimensional vectors in the VectorIndex.
    #[cfg(feature = "vector-search-local")]
    #[tokio::test]
    // Concurrent ONNX model cache initialization can race and flake with
    // "Failed to retrieve onnx/model.onnx" when these tests run in parallel.
    /// TODO: Document test_fastembed_auto_embed_on_add.
    #[serial]
    async fn test_fastembed_auto_embed_on_add() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({ "source": "fastEmbed" }),
        );

        let (tx, handle, tasks, vector_indices) =
            setup_write_queue_with_embedder(&tmp, "fe_embed_t", Some(embedders));

        let task_id = "fe_embed_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("Hello local embedding".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        let final_task = tasks.get(&task_id).unwrap();
        assert!(
            matches!(final_task.status, TaskStatus::Succeeded),
            "task should succeed, got: {:?}",
            final_task.status
        );

        // Verify vector index was auto-created with correct dimensions
        assert!(
            vector_indices.contains_key("fe_embed_t"),
            "vector index should be auto-created for fastembed"
        );
        let vi_lock = vector_indices.get("fe_embed_t").unwrap();
        let vi = vi_lock.read().unwrap();
        assert_eq!(vi.len(), 1, "vector index should have 1 document");
        assert_eq!(
            vi.dimensions(),
            384,
            "BGESmallENV15 default model should produce 384-dim vectors"
        );
    }

    /// TODO: Document test_fastembed_vectors_in_oplog.
    #[cfg(feature = "vector-search-local")]
    #[tokio::test]
    #[serial]
    async fn test_fastembed_vectors_in_oplog() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut embedders = HashMap::new();
        embedders.insert(
            "default".to_string(),
            serde_json::json!({ "source": "fastEmbed" }),
        );

        let (tx, handle, tasks, _vi, oplog) =
            setup_write_queue_with_oplog(&tmp, "fe_oplog_t", Some(embedders));

        let task_id = "fe_oplog_task".to_string();
        tasks.insert(task_id.clone(), TaskInfo::new(task_id.clone(), 1, 1));

        tx.send(WriteOp {
            task_id: task_id.clone(),
            actions: vec![WriteAction::Add(crate::types::Document {
                id: "doc1".to_string(),
                fields: HashMap::from([(
                    "title".to_string(),
                    FieldValue::Text("oplog fastembed test".to_string()),
                )]),
            })],
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();

        // Read oplog and verify computed vectors are stored
        let vec_array = extract_oplog_vectors(&oplog, "default");
        assert_eq!(
            vec_array.len(),
            384,
            "fastembed BGESmallENV15 should produce 384-dim vectors in oplog"
        );
    }
}
