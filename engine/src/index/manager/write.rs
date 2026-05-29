use super::*;
use std::time::{Duration, Instant};

/// Default deadline for durable HTTP writes, in milliseconds.
///
/// Overridable at runtime via `FLAPJACK_WRITE_DURABLE_TIMEOUT_MS`. Bounds how long
/// a durable write handler will wait for the write-queue consumer to commit before
/// returning a retriable `WriteAckTimeout` (503). The default is generous so normal
/// commits never trip it; the bound exists only to keep an HTTP request from hanging
/// forever if the consumer task dies mid-restart (PL-13 silent-drop failure mode).
const DEFAULT_WRITE_DURABLE_TIMEOUT_MS: u64 = 30_000;

impl super::IndexManager {
    /// Get or create a write queue for the given tenant.
    ///
    /// DRY helper — all write paths (add, delete, compact) go through this.
    /// Handles oplog creation, write queue spawning, and vector context setup.
    fn get_or_create_write_queue(&self, tenant_id: &str, index: &Arc<Index>) -> WriteQueue {
        self.write_queues
            .entry(tenant_id.to_string())
            .or_insert_with(|| {
                let oplog = self.get_or_create_oplog(tenant_id);
                #[cfg(feature = "vector-search")]
                let vector_ctx = VectorWriteContext::new(Arc::clone(&self.vector_indices));
                #[cfg(not(feature = "vector-search"))]
                let vector_ctx = VectorWriteContext::new();
                let (queue, handle) = create_write_queue(WriteQueueContext {
                    tenant_id: tenant_id.to_string(),
                    index: Arc::clone(index),
                    _writers: Arc::clone(&self.writers),
                    tasks: Arc::clone(&self.tasks),
                    base_path: self.base_path.clone(),
                    oplog,
                    facet_cache: Arc::clone(&self.facet_cache),
                    lww_map: Arc::clone(&self.lww_map),
                    vector_ctx,
                });
                self.write_task_handles
                    .insert(tenant_id.to_string(), handle);
                queue
            })
            .clone()
    }

    /// Add documents to a tenant's index.
    ///
    /// Creates a writer, adds documents, and commits immediately.
    /// For production, this should be batched via background commit thread.
    pub fn add_documents_insert(&self, tenant_id: &str, docs: Vec<Document>) -> Result<TaskInfo> {
        self.add_documents_inner(tenant_id, docs, false, false)
    }

    pub fn add_documents(&self, tenant_id: &str, docs: Vec<Document>) -> Result<TaskInfo> {
        self.add_documents_inner(tenant_id, docs, true, false)
    }

    /// Like `add_documents` but uses `UpsertNoLwwUpdate` so the write_queue does NOT
    /// overwrite lww_map entries — for use by replication (apply_ops_to_manager) which
    /// has already recorded the correct op timestamp in lww_map before calling this.
    pub fn add_documents_for_replication(
        &self,
        tenant_id: &str,
        docs: Vec<Document>,
    ) -> Result<TaskInfo> {
        self.add_documents_inner(tenant_id, docs, true, true)
    }

    /// Core document-add path: load the tenant index, create a task, evict stale
    /// tasks, and send an Add/Upsert/UpsertNoLwwUpdate `WriteOp` to the write
    /// queue. Returns `QueueFull` on backpressure.
    fn add_documents_inner(
        &self,
        tenant_id: &str,
        docs: Vec<Document>,
        upsert: bool,
        no_lww_update: bool,
    ) -> Result<TaskInfo> {
        let index = self.get_or_load(tenant_id)?;

        let numeric_id = self.next_numeric_task_id();
        let task_id = format!("task_{}_{}", tenant_id, uuid::Uuid::new_v4());
        let task = TaskInfo::new(task_id.clone(), numeric_id, docs.len());
        self.tasks.insert(task_id.clone(), task.clone());
        self.tasks.insert(numeric_id.to_string(), task.clone());

        self.evict_old_tasks(tenant_id, MAX_TASKS_PER_TENANT);

        let tx = self.get_or_create_write_queue(tenant_id, &index);

        let actions = if upsert {
            if no_lww_update {
                docs.into_iter()
                    .map(WriteAction::UpsertNoLwwUpdate)
                    .collect()
            } else {
                docs.into_iter().map(WriteAction::Upsert).collect()
            }
        } else {
            docs.into_iter().map(WriteAction::Add).collect()
        };
        if tx
            .try_send(WriteOp {
                task_id: task_id.clone(),
                actions,
            })
            .is_err()
        {
            self.tasks.alter(&task_id, |_, mut t| {
                t.status = TaskStatus::Failed("Queue full".to_string());
                t
            });
            return Err(FlapjackError::QueueFull);
        }

        Ok(task)
    }

    /// Queue document deletions by object ID with LWW tracking. Creates a task
    /// and sends `Delete` actions to the tenant's write queue.
    pub fn delete_documents(&self, tenant_id: &str, object_ids: Vec<String>) -> Result<TaskInfo> {
        let index = self.get_or_load(tenant_id)?;

        let numeric_id = self.next_numeric_task_id();
        let task_id = format!("task_{}_{}", tenant_id, uuid::Uuid::new_v4());
        let task = TaskInfo::new(task_id.clone(), numeric_id, object_ids.len());
        self.tasks.insert(task_id.clone(), task.clone());
        self.tasks.insert(numeric_id.to_string(), task.clone());

        self.evict_old_tasks(tenant_id, MAX_TASKS_PER_TENANT);

        let tx = self.get_or_create_write_queue(tenant_id, &index);

        let actions = object_ids.into_iter().map(WriteAction::Delete).collect();
        if tx
            .try_send(WriteOp {
                task_id: task_id.clone(),
                actions,
            })
            .is_err()
        {
            self.tasks.alter(&task_id, |_, mut t| {
                t.status = TaskStatus::Failed("Queue full".to_string());
                t
            });
            return Err(FlapjackError::QueueFull);
        }

        Ok(task)
    }

    /// Queue document deletions without updating the LWW map — the caller
    /// (replication) has already recorded the correct timestamps before queuing.
    pub fn delete_documents_for_replication(
        &self,
        tenant_id: &str,
        object_ids: Vec<String>,
    ) -> Result<TaskInfo> {
        let index = self.get_or_load(tenant_id)?;

        let numeric_id = self.next_numeric_task_id();
        let task_id = format!("task_{}_{}", tenant_id, uuid::Uuid::new_v4());
        let task = TaskInfo::new(task_id.clone(), numeric_id, object_ids.len());
        self.tasks.insert(task_id.clone(), task.clone());
        self.tasks.insert(numeric_id.to_string(), task.clone());

        self.evict_old_tasks(tenant_id, MAX_TASKS_PER_TENANT);

        let tx = self.get_or_create_write_queue(tenant_id, &index);

        let actions = object_ids
            .into_iter()
            .map(WriteAction::DeleteNoLwwUpdate)
            .collect();
        if tx
            .try_send(WriteOp {
                task_id: task_id.clone(),
                actions,
            })
            .is_err()
        {
            self.tasks.alter(&task_id, |_, mut t| {
                t.status = TaskStatus::Failed("Queue full".to_string());
                t
            });
            return Err(FlapjackError::QueueFull);
        }

        Ok(task)
    }

    /// Queue a segment compaction for the tenant. Creates a task and sends a
    /// single `Compact` action to the write queue.
    pub fn compact_index(&self, tenant_id: &str) -> Result<TaskInfo> {
        let index = self.get_or_load(tenant_id)?;

        let numeric_id = self.next_numeric_task_id();
        let task_id = format!("task_{}_{}", tenant_id, uuid::Uuid::new_v4());
        let task = TaskInfo::new(task_id.clone(), numeric_id, 0);
        self.tasks.insert(task_id.clone(), task.clone());
        self.tasks.insert(numeric_id.to_string(), task.clone());

        let tx = self.get_or_create_write_queue(tenant_id, &index);

        if tx
            .try_send(WriteOp {
                task_id: task_id.clone(),
                actions: vec![WriteAction::Compact],
            })
            .is_err()
        {
            self.tasks.alter(&task_id, |_, mut t| {
                t.status = TaskStatus::Failed("Queue full".to_string());
                t
            });
            return Err(FlapjackError::QueueFull);
        }

        Ok(task)
    }

    /// Resolve the durable-write deadline from `FLAPJACK_WRITE_DURABLE_TIMEOUT_MS`,
    /// falling back to [`DEFAULT_WRITE_DURABLE_TIMEOUT_MS`] when unset or unparseable.
    fn durable_write_timeout() -> Duration {
        let ms = std::env::var("FLAPJACK_WRITE_DURABLE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_WRITE_DURABLE_TIMEOUT_MS);
        Duration::from_millis(ms)
    }

    /// Poll a task's status until it reaches a terminal state, sleeping 10ms between
    /// checks. This is the single source of truth for the "wait until durable" loop
    /// shared by the unbounded `*_sync` helpers and the bounded `*_durable` paths.
    ///
    /// `timeout` selects the waiting policy:
    /// - `None` — poll indefinitely (internal callers and tests rely on this).
    /// - `Some(d)` — return [`FlapjackError::WriteAckTimeout`] if the task has not
    ///   reached a terminal state by `now + d`, so an HTTP write handler cannot hang
    ///   forever when the write-queue consumer dies before committing (PL-13).
    ///
    /// A terminal `Succeeded` resolves to `Ok(())`; a terminal `Failed` propagates the
    /// underlying message as [`FlapjackError::Tantivy`], which maps to a 5xx response.
    async fn await_task_terminal(&self, task_id: &str, timeout: Option<Duration>) -> Result<()> {
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            let status = self.get_task(task_id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            return Err(FlapjackError::WriteAckTimeout);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }

    /// Wait until a queued write task is durably committed to Tantivy, bounded by
    /// `FLAPJACK_WRITE_DURABLE_TIMEOUT_MS` (default 30s).
    ///
    /// This is the bounded counterpart to the unbounded `*_sync` poll. HTTP write
    /// handlers call it after enqueuing so they can report durability while still
    /// holding the enqueued [`TaskInfo`] — letting a failure response carry the
    /// `taskID` per the Algolia write contract. Returns
    /// [`FlapjackError::WriteAckTimeout`] (503) if the consumer does not ack within
    /// the deadline, or the underlying commit error (5xx) if the commit failed.
    pub async fn wait_for_write_durable(&self, task_id: &str) -> Result<()> {
        self.await_task_terminal(task_id, Some(Self::durable_write_timeout()))
            .await
    }

    /// Add documents and wait until the write queue has durably committed them to
    /// Tantivy, bounded by `FLAPJACK_WRITE_DURABLE_TIMEOUT_MS` (default 30s).
    ///
    /// HTTP add handlers use this instead of fire-and-forget [`add_documents`] so a
    /// 200 response means the write is on disk — closing the PL-13 silent-drop where
    /// an enqueued-but-uncommitted write was ACKed before the consumer committed it.
    /// Returns [`FlapjackError::QueueFull`] (429) on backpressure,
    /// [`FlapjackError::WriteAckTimeout`] (503) if the consumer does not ack in time,
    /// or the underlying commit error (5xx). Replication paths intentionally keep
    /// using the fire-and-forget variant and are not routed through here.
    pub async fn add_documents_durable(
        &self,
        tenant_id: &str,
        docs: Vec<Document>,
    ) -> Result<TaskInfo> {
        let task = self.add_documents(tenant_id, docs)?;
        self.wait_for_write_durable(&task.id).await?;
        Ok(task)
    }

    /// Compact an index and wait for the operation to complete.
    pub async fn compact_index_sync(&self, tenant_id: &str) -> Result<()> {
        let task = self.compact_index(tenant_id)?;
        self.await_task_terminal(&task.id, None).await
    }

    /// Insert documents (non-upsert) and poll until the task succeeds or fails.
    /// Async wrapper around `add_documents_insert`.
    pub async fn add_documents_insert_sync(
        &self,
        tenant_id: &str,
        docs: Vec<Document>,
    ) -> Result<()> {
        let task = self.add_documents_insert(tenant_id, docs)?;
        self.await_task_terminal(&task.id, None).await
    }

    pub async fn add_documents_sync(&self, tenant_id: &str, docs: Vec<Document>) -> Result<()> {
        let task = self.add_documents(tenant_id, docs)?;
        self.await_task_terminal(&task.id, None).await
    }

    /// Delete documents and poll until the task succeeds or fails. Async wrapper
    /// around `delete_documents`.
    pub async fn delete_documents_sync(
        &self,
        tenant_id: &str,
        object_ids: Vec<String>,
    ) -> Result<()> {
        let task = self.delete_documents(tenant_id, object_ids)?;
        self.await_task_terminal(&task.id, None).await
    }

    /// Like `delete_documents_sync` but skips lww_map update in write_queue — for replication.
    pub async fn delete_documents_sync_for_replication(
        &self,
        tenant_id: &str,
        object_ids: Vec<String>,
    ) -> Result<()> {
        let task = self.delete_documents_for_replication(tenant_id, object_ids)?;
        self.await_task_terminal(&task.id, None).await
    }
}
