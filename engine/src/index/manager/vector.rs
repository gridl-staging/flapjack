use super::*;

// ── Vector index storage (behind vector-search feature) ──

#[cfg(feature = "vector-search")]
impl IndexManager {
    /// Get the vector index for a tenant, if one has been stored.
    pub fn get_vector_index(
        &self,
        tenant_id: &str,
    ) -> Option<Arc<std::sync::RwLock<crate::vector::index::VectorIndex>>> {
        self.vector_indices.get(tenant_id).map(|r| Arc::clone(&r))
    }

    /// Return total memory used by all loaded vector indices, in bytes.
    pub fn vector_memory_usage(&self) -> usize {
        let mut total = 0usize;
        for entry in self.vector_indices.iter() {
            if let Ok(guard) = entry.value().read() {
                total += guard.memory_usage();
            }
        }
        total
    }

    /// Store a vector index for a tenant, wrapping it in Arc<RwLock<_>>.
    pub fn set_vector_index(&self, tenant_id: &str, index: crate::vector::index::VectorIndex) {
        self.vector_indices.insert(
            tenant_id.to_string(),
            Arc::new(std::sync::RwLock::new(index)),
        );
    }

    /// Load a vector index from disk for a tenant if one exists.
    ///
    /// Checks for the sentinel file `{tenant_path}/vectors/id_map.json`.
    /// Skips if already loaded (e.g., by oplog recovery).
    /// Checks embedder fingerprint against current settings — skips stale vectors.
    /// Logs warning and skips on failure — tenant is BM25-only.
    pub(super) fn load_vector_index(&self, tenant_id: &str, tenant_path: &Path) {
        if self.vector_indices.contains_key(tenant_id) {
            return;
        }

        let Some(vectors_dir) = Self::persisted_vectors_dir(tenant_path) else {
            return;
        };

        let current_configs = Self::configured_embedder_fingerprints(tenant_path);
        if !Self::vector_fingerprint_matches(tenant_id, &vectors_dir, &current_configs) {
            return;
        }

        self.load_vector_index_from_disk(tenant_id, &vectors_dir);
    }

    fn persisted_vectors_dir(tenant_path: &Path) -> Option<PathBuf> {
        let vectors_dir = tenant_path.join("vectors");
        vectors_dir
            .join("id_map.json")
            .exists()
            .then_some(vectors_dir)
    }

    /// Read embedder configurations from the tenant's `settings.json` and parse them
    /// into `(name, EmbedderConfig)` pairs. Returns an empty vec if settings are missing
    /// or contain no valid embedders.
    fn configured_embedder_fingerprints(
        tenant_path: &Path,
    ) -> Vec<(String, crate::vector::config::EmbedderConfig)> {
        let settings_path = tenant_path.join("settings.json");
        settings_path
            .exists()
            .then(|| IndexSettings::load(&settings_path).ok())
            .flatten()
            .and_then(|settings| {
                settings.embedders.as_ref().map(|embedders| {
                    embedders
                        .iter()
                        .filter_map(|(name, json)| {
                            (!json.is_null())
                                .then(|| {
                                    serde_json::from_value::<crate::vector::config::EmbedderConfig>(
                                        json.clone(),
                                    )
                                    .ok()
                                    .map(|config| (name.clone(), config))
                                })
                                .flatten()
                        })
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    /// Check whether the persisted embedder fingerprint matches the current embedder
    /// configurations. Returns `false` (skip load) if no embedders are configured or
    /// the fingerprint indicates stale vectors.
    fn vector_fingerprint_matches(
        tenant_id: &str,
        vectors_dir: &Path,
        current_configs: &[(String, crate::vector::config::EmbedderConfig)],
    ) -> bool {
        if current_configs.is_empty() {
            tracing::info!(
                "[LOAD {}] no embedders configured, skipping vector index load",
                tenant_id
            );
            return false;
        }

        match crate::vector::config::EmbedderFingerprint::load(vectors_dir) {
            Ok(fingerprint) => {
                if fingerprint.matches_configs(current_configs) {
                    true
                } else {
                    tracing::warn!(
                        "[LOAD {}] embedder fingerprint mismatch — vectors are stale, skipping load (BM25 fallback)",
                        tenant_id
                    );
                    false
                }
            }
            Err(_) => true,
        }
    }

    /// Load a VectorIndex from the tenant's `vectors/` directory using cosine similarity.
    /// Logs the vector count on success or a warning on failure.
    fn load_vector_index_from_disk(&self, tenant_id: &str, vectors_dir: &Path) {
        match crate::vector::index::VectorIndex::load(vectors_dir, usearch::ffi::MetricKind::Cos) {
            Ok(vi) => {
                let count = vi.len();
                self.set_vector_index(tenant_id, vi);
                tracing::info!(
                    "[LOAD {}] loaded vector index from disk ({} vectors)",
                    tenant_id,
                    count
                );
            }
            Err(e) => {
                tracing::warn!("[LOAD {}] failed to load vector index: {}", tenant_id, e);
            }
        }
    }
}
