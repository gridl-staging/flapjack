use super::*;

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

    /// TODO: Document IndexManager.add_documents_inner.
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

    /// TODO: Document IndexManager.delete_documents.
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

    /// TODO: Document IndexManager.delete_documents_for_replication.
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

    /// TODO: Document IndexManager.compact_index.
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

    /// Compact an index and wait for the operation to complete.
    pub async fn compact_index_sync(&self, tenant_id: &str) -> Result<()> {
        let task = self.compact_index(tenant_id)?;

        loop {
            let status = self.get_task(&task.id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }

    /// TODO: Document IndexManager.add_documents_insert_sync.
    pub async fn add_documents_insert_sync(
        &self,
        tenant_id: &str,
        docs: Vec<Document>,
    ) -> Result<()> {
        let task = self.add_documents_insert(tenant_id, docs)?;

        loop {
            let status = self.get_task(&task.id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }

    pub async fn add_documents_sync(&self, tenant_id: &str, docs: Vec<Document>) -> Result<()> {
        let task = self.add_documents(tenant_id, docs)?;

        loop {
            let status = self.get_task(&task.id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }

    /// TODO: Document IndexManager.delete_documents_sync.
    pub async fn delete_documents_sync(
        &self,
        tenant_id: &str,
        object_ids: Vec<String>,
    ) -> Result<()> {
        let task = self.delete_documents(tenant_id, object_ids)?;

        loop {
            let status = self.get_task(&task.id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }

    /// Like `delete_documents_sync` but skips lww_map update in write_queue — for replication.
    pub async fn delete_documents_sync_for_replication(
        &self,
        tenant_id: &str,
        object_ids: Vec<String>,
    ) -> Result<()> {
        let task = self.delete_documents_for_replication(tenant_id, object_ids)?;

        loop {
            let status = self.get_task(&task.id)?;
            match status.status {
                TaskStatus::Enqueued | TaskStatus::Processing => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                TaskStatus::Succeeded => return Ok(()),
                TaskStatus::Failed(e) => return Err(FlapjackError::Tantivy(e)),
            }
        }
    }
}
