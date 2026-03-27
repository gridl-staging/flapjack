//! Persistent, file-backed store for A/B experiment lifecycle management with atomic writes, numeric ID mapping, and single-active-per-index enforcement.
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

use super::config::{Experiment, ExperimentConclusion, ExperimentError, ExperimentStatus};

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct ExperimentFilter {
    pub index_name: Option<String>,
    pub status: Option<ExperimentStatus>,
}

pub struct ExperimentStore {
    experiments: DashMap<String, Experiment>,
    active_by_index: DashMap<String, String>,
    /// Maps UUID string IDs → sequential integer IDs (Algolia-compatible).
    id_to_numeric: DashMap<String, i64>,
    /// Reverse map: integer ID → UUID string.
    numeric_to_id: DashMap<i64, String>,
    /// Next integer ID to assign.
    next_numeric_id: AtomicI64,
    dir: PathBuf,
}

impl ExperimentStore {
    pub fn new(data_dir: &std::path::Path) -> Result<Self, ExperimentError> {
        let dir = data_dir.join(".experiments");
        std::fs::create_dir_all(&dir)?;
        let store = Self {
            experiments: DashMap::new(),
            active_by_index: DashMap::new(),
            id_to_numeric: DashMap::new(),
            numeric_to_id: DashMap::new(),
            next_numeric_id: AtomicI64::new(1),
            dir,
        };
        store.load_all()?;
        Ok(store)
    }

    /// Load all persisted experiment JSON files and the numeric ID mapping from the store directory.
    ///
    /// Applies backward-compatibility fixups for older records that stored stop time in `ended_at`,
    /// validates each experiment, registers running experiments in the active-by-index map, and
    /// assigns numeric IDs to any experiments that lack one.
    ///
    /// # Errors
    ///
    /// Returns an error if any persisted experiment fails validation or if multiple running
    /// experiments target the same index.
    fn load_all(&self) -> Result<(), ExperimentError> {
        // Load the numeric ID mapping if it exists.
        let mapping_path = self.dir.join("_id_map.json");
        if mapping_path.exists() {
            let data = std::fs::read_to_string(&mapping_path)?;
            let map: std::collections::HashMap<String, i64> = serde_json::from_str(&data)?;
            let mut max_id = 0i64;
            for (uuid, numeric) in &map {
                self.id_to_numeric.insert(uuid.clone(), *numeric);
                self.numeric_to_id.insert(*numeric, uuid.clone());
                if *numeric > max_id {
                    max_id = *numeric;
                }
            }
            self.next_numeric_id.store(max_id + 1, Ordering::SeqCst);
        }

        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json")
                && !path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with('_') || n.ends_with(".tmp"))
            {
                let data = std::fs::read_to_string(&path)?;
                let mut experiment: Experiment = serde_json::from_str(&data)?;
                if (experiment.status == ExperimentStatus::Stopped
                    || experiment.status == ExperimentStatus::Concluded)
                    && experiment.stopped_at.is_none()
                {
                    // Backward compatibility: older persisted records stored stop time in ended_at.
                    experiment.stopped_at = experiment.ended_at;
                }
                experiment.validate()?;
                self.register_active_if_running(&experiment)?;
                // Assign a numeric ID if not already mapped.
                if !self.id_to_numeric.contains_key(&experiment.id) {
                    self.assign_numeric_id(&experiment.id);
                }
                self.experiments.insert(experiment.id.clone(), experiment);
            }
        }
        // Persist mapping in case new IDs were assigned during load.
        self.persist_id_map()?;
        Ok(())
    }

    /// Record the experiment as the active experiment for its index if it is in `Running` status.
    ///
    /// Enforces the single-active-experiment-per-index invariant: returns `InvalidConfig` if a
    /// different experiment is already active on the same index. No-ops for non-running experiments
    /// or if the same experiment is already registered.
    fn register_active_if_running(&self, experiment: &Experiment) -> Result<(), ExperimentError> {
        if experiment.status != ExperimentStatus::Running {
            return Ok(());
        }
        if let Some(existing_id) = self.active_by_index.get(&experiment.index_name) {
            if existing_id.value() != &experiment.id {
                return Err(ExperimentError::InvalidConfig(format!(
                    "multiple running experiments for index '{}': '{}' and '{}'",
                    experiment.index_name,
                    existing_id.value(),
                    experiment.id
                )));
            }
            return Ok(());
        }
        self.active_by_index
            .insert(experiment.index_name.clone(), experiment.id.clone());
        Ok(())
    }

    fn unregister_active_if_running(&self, experiment: &Experiment) {
        if experiment.status != ExperimentStatus::Running {
            return;
        }
        let should_remove = self
            .active_by_index
            .get(&experiment.index_name)
            .is_some_and(|entry| entry.value() == &experiment.id);
        if should_remove {
            self.active_by_index.remove(&experiment.index_name);
        }
    }

    fn assign_numeric_id(&self, uuid: &str) -> i64 {
        let id = self.next_numeric_id.fetch_add(1, Ordering::SeqCst);
        self.id_to_numeric.insert(uuid.to_string(), id);
        self.numeric_to_id.insert(id, uuid.to_string());
        id
    }

    fn persist_id_map(&self) -> Result<(), ExperimentError> {
        let map: std::collections::HashMap<String, i64> = self
            .id_to_numeric
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        let tmp_path = self.dir.join("_id_map.json.tmp");
        let final_path = self.dir.join("_id_map.json");
        let data = serde_json::to_string_pretty(&map)?;
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    /// Get the numeric (Algolia-compatible) integer ID for a UUID experiment.
    pub fn get_numeric_id(&self, uuid: &str) -> Option<i64> {
        self.id_to_numeric.get(uuid).map(|v| *v)
    }

    /// Get the UUID for a numeric ID.
    pub fn get_uuid_for_numeric(&self, numeric_id: i64) -> Option<String> {
        self.numeric_to_id.get(&numeric_id).map(|v| v.clone())
    }

    /// Look up an experiment by its integer ID.
    pub fn get_by_numeric_id(&self, numeric_id: i64) -> Result<Experiment, ExperimentError> {
        let uuid = self
            .get_uuid_for_numeric(numeric_id)
            .ok_or_else(|| ExperimentError::NotFound(numeric_id.to_string()))?;
        self.get(&uuid)
    }

    /// Returns the persisted experiment file modification time (ms since Unix epoch).
    pub fn get_last_updated_ms(&self, id: &str) -> Option<i64> {
        let path = self.dir.join(format!("{id}.json"));
        let modified = std::fs::metadata(path).ok()?.modified().ok()?;
        let millis = modified
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis();
        i64::try_from(millis).ok()
    }

    fn atomic_write(&self, experiment: &Experiment) -> Result<(), ExperimentError> {
        let tmp_path = self.dir.join(format!("{}.json.tmp", experiment.id));
        let final_path = self.dir.join(format!("{}.json", experiment.id));
        let data = serde_json::to_string_pretty(experiment)?;
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    pub fn create(&self, experiment: Experiment) -> Result<Experiment, ExperimentError> {
        experiment.validate()?;
        if self.experiments.contains_key(&experiment.id) {
            return Err(ExperimentError::AlreadyExists(experiment.id));
        }
        self.register_active_if_running(&experiment)?;
        self.atomic_write(&experiment)?;
        self.assign_numeric_id(&experiment.id);
        self.persist_id_map()?;
        self.experiments
            .insert(experiment.id.clone(), experiment.clone());
        Ok(experiment)
    }

    pub fn get(&self, id: &str) -> Result<Experiment, ExperimentError> {
        self.experiments
            .get(id)
            .map(|e| e.clone())
            .ok_or_else(|| ExperimentError::NotFound(id.to_string()))
    }

    /// Return all experiments, optionally filtered by index name and/or status.
    ///
    /// # Arguments
    ///
    /// * `filter` — When `None`, returns every experiment. When `Some`, only experiments matching
    ///   all specified filter fields are included.
    pub fn list(&self, filter: Option<ExperimentFilter>) -> Vec<Experiment> {
        self.experiments
            .iter()
            .filter(|entry| {
                if let Some(ref f) = filter {
                    if let Some(ref idx) = f.index_name {
                        if &entry.value().index_name != idx {
                            return false;
                        }
                    }
                    if let Some(ref status) = f.status {
                        if &entry.value().status != status {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn update(&self, experiment: Experiment) -> Result<Experiment, ExperimentError> {
        let existing = self.get(&experiment.id)?;
        if existing.status != ExperimentStatus::Draft {
            return Err(ExperimentError::InvalidStatus(format!(
                "{:?}",
                existing.status
            )));
        }
        experiment.validate()?;
        self.atomic_write(&experiment)?;
        self.experiments
            .insert(experiment.id.clone(), experiment.clone());
        Ok(experiment)
    }

    /// Transition a draft experiment to `Running` status and register it as active for its index.
    ///
    /// Sets `started_at` to the current wall-clock time.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStatus` if the experiment is not in `Draft` status or if another
    /// experiment is already running on the same index.
    pub fn start(&self, id: &str) -> Result<Experiment, ExperimentError> {
        let mut experiment = self.get(id)?;
        if experiment.status != ExperimentStatus::Draft {
            return Err(ExperimentError::InvalidStatus(format!(
                "{:?}",
                experiment.status
            )));
        }
        // Prevent multiple active experiments on the same index
        if self.get_active_for_index(&experiment.index_name).is_some() {
            return Err(ExperimentError::InvalidStatus(format!(
                "index '{}' already has a running experiment",
                experiment.index_name
            )));
        }
        experiment.status = ExperimentStatus::Running;
        experiment.started_at = Some(now_ms());
        self.atomic_write(&experiment)?;
        self.experiments.insert(id.to_string(), experiment.clone());
        self.active_by_index
            .insert(experiment.index_name.clone(), experiment.id.clone());
        Ok(experiment)
    }

    /// Transition an experiment from `Running` or `Draft` to `Stopped` status.
    ///
    /// Sets `stopped_at` to the current wall-clock time and removes the experiment from the
    /// active-by-index map if it was running.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStatus` if the experiment is already `Stopped` or `Concluded`.
    pub fn stop(&self, id: &str) -> Result<Experiment, ExperimentError> {
        let mut experiment = self.get(id)?;
        if experiment.status != ExperimentStatus::Running
            && experiment.status != ExperimentStatus::Draft
        {
            return Err(ExperimentError::InvalidStatus(format!(
                "{:?}",
                experiment.status
            )));
        }
        let running_snapshot = experiment.clone();
        experiment.status = ExperimentStatus::Stopped;
        experiment.stopped_at = Some(now_ms());
        self.atomic_write(&experiment)?;
        self.experiments.insert(id.to_string(), experiment.clone());
        self.unregister_active_if_running(&running_snapshot);
        Ok(experiment)
    }

    /// Transition an experiment to `Concluded` status and attach the statistical conclusion.
    ///
    /// Accepts experiments in `Running` or `Stopped` status. If the experiment is still running,
    /// it is unregistered from the active-by-index map. The `stopped_at` timestamp is preserved
    /// if already set (from a prior stop), otherwise set to now.
    ///
    /// # Arguments
    ///
    /// * `id` — Experiment UUID.
    /// * `conclusion` — Statistical results and winner declaration.
    ///
    /// # Returns
    ///
    /// The updated experiment with `Concluded` status.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStatus` if the experiment is in `Draft` or already `Concluded`.
    pub fn conclude(
        &self,
        id: &str,
        conclusion: ExperimentConclusion,
    ) -> Result<Experiment, ExperimentError> {
        let mut experiment = self.get(id)?;
        if experiment.status != ExperimentStatus::Running
            && experiment.status != ExperimentStatus::Stopped
        {
            return Err(ExperimentError::InvalidStatus(format!(
                "{:?}",
                experiment.status
            )));
        }
        if experiment.status == ExperimentStatus::Running {
            self.unregister_active_if_running(&experiment);
        }
        experiment.status = ExperimentStatus::Concluded;
        if experiment.stopped_at.is_none() {
            experiment.stopped_at = Some(now_ms());
        }
        experiment.conclusion = Some(conclusion);
        self.atomic_write(&experiment)?;
        self.experiments.insert(id.to_string(), experiment.clone());
        Ok(experiment)
    }

    /// Remove an experiment from the store, its on-disk JSON file, and the numeric ID mapping.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStatus` if the experiment is currently `Running` (stop it first).
    /// Returns `NotFound` if no experiment with the given ID exists.
    pub fn delete(&self, id: &str) -> Result<(), ExperimentError> {
        let experiment = self.get(id)?;
        if experiment.status == ExperimentStatus::Running {
            return Err(ExperimentError::InvalidStatus(format!(
                "{:?}",
                experiment.status
            )));
        }
        let path = self.dir.join(format!("{}.json", id));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        if let Some((_, removed)) = self.experiments.remove(id) {
            self.unregister_active_if_running(&removed);
        }
        // Clean up numeric ID mapping.
        if let Some((_, numeric)) = self.id_to_numeric.remove(id) {
            self.numeric_to_id.remove(&numeric);
        }
        self.persist_id_map()?;
        Ok(())
    }

    pub fn get_active_for_index(&self, index_name: &str) -> Option<Experiment> {
        let active_id = self.active_by_index.get(index_name)?.value().clone();
        let experiment = self.experiments.get(&active_id).map(|entry| entry.clone());
        match experiment {
            Some(exp)
                if exp.status == ExperimentStatus::Running && exp.index_name == index_name =>
            {
                Some(exp)
            }
            _ => {
                self.active_by_index.remove(index_name);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiments::config::*;
    use tempfile::TempDir;

    /// Create a minimal draft `Experiment` fixture for testing with the given ID and index name.
    ///
    /// Uses a 50/50 traffic split, CTR as the primary metric, and a variant arm that disables synonyms.
    fn make_experiment(id: &str, index: &str) -> Experiment {
        Experiment {
            id: id.to_string(),
            name: "test".to_string(),
            index_name: index.to_string(),
            status: ExperimentStatus::Draft,
            traffic_split: 0.5,
            control: ExperimentArm {
                name: "control".to_string(),
                query_overrides: None,
                index_name: None,
            },
            variant: ExperimentArm {
                name: "variant".to_string(),
                query_overrides: Some(QueryOverrides {
                    enable_synonyms: Some(false),
                    ..Default::default()
                }),
                index_name: None,
            },
            primary_metric: PrimaryMetric::Ctr,
            created_at: 1700000000000,
            started_at: None,
            ended_at: None,
            stopped_at: None,
            minimum_days: 14,
            winsorization_cap: None,
            conclusion: None,
            interleaving: None,
        }
    }

    #[test]
    fn create_and_get_succeeds() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        let exp = make_experiment("abc-123", "products");
        store.create(exp.clone()).unwrap();
        let loaded = store.get("abc-123").unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.index_name, "products");
    }

    #[test]
    fn create_duplicate_id_fails() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        let exp = make_experiment("dup-id", "products");
        store.create(exp.clone()).unwrap();
        assert!(matches!(
            store.create(exp),
            Err(ExperimentError::AlreadyExists(_))
        ));
    }

    #[test]
    fn get_nonexistent_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        assert!(matches!(
            store.get("ghost"),
            Err(ExperimentError::NotFound(_))
        ));
    }

    #[test]
    fn list_returns_all_experiments() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.create(make_experiment("e2", "articles")).unwrap();
        let list = store.list(None);
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn list_filters_by_index() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.create(make_experiment("e2", "articles")).unwrap();
        let list = store.list(Some(ExperimentFilter {
            index_name: Some("products".to_string()),
            status: None,
        }));
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "e1");
    }

    #[test]
    fn update_draft_succeeds() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        let mut updated = make_experiment("e1", "products");
        updated.name = "updated name".to_string();
        store.update(updated).unwrap();
        assert_eq!(store.get("e1").unwrap().name, "updated name");
    }

    #[test]
    fn update_running_experiment_returns_invalid_status() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        let mut exp = store.get("e1").unwrap();
        exp.name = "new name".to_string();
        assert!(matches!(
            store.update(exp),
            Err(ExperimentError::InvalidStatus(_))
        ));
    }

    #[test]
    fn start_transitions_draft_to_running() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        let started = store.start("e1").unwrap();
        assert_eq!(started.status, ExperimentStatus::Running);
        assert!(started.started_at.is_some());
    }

    #[test]
    fn start_already_running_returns_invalid_status() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        assert!(matches!(
            store.start("e1"),
            Err(ExperimentError::InvalidStatus(_))
        ));
    }

    #[test]
    fn stop_transitions_running_to_stopped() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        let stopped = store.stop("e1").unwrap();
        assert_eq!(stopped.status, ExperimentStatus::Stopped);
        assert!(stopped.stopped_at.is_some());
    }

    #[test]
    fn stop_transitions_draft_to_stopped() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        let stopped = store.stop("e1").unwrap();
        assert_eq!(stopped.status, ExperimentStatus::Stopped);
        assert!(stopped.stopped_at.is_some());
    }

    /// Verify that concluding a running experiment transitions it to `Concluded`, sets `stopped_at`, and attaches the provided conclusion with winner and statistical details.
    #[test]
    fn conclude_running_experiment_sets_status_and_conclusion() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();

        let conclusion = ExperimentConclusion {
            winner: Some("variant".to_string()),
            reason: "Statistically significant result".to_string(),
            control_metric: 0.12,
            variant_metric: 0.14,
            confidence: 0.97,
            significant: true,
            promoted: false,
        };

        let concluded = store.conclude("e1", conclusion.clone()).unwrap();
        assert_eq!(concluded.status, ExperimentStatus::Concluded);
        assert!(concluded.stopped_at.is_some());
        assert_eq!(
            concluded.conclusion.as_ref().unwrap().winner,
            conclusion.winner
        );
        assert_eq!(
            concluded.conclusion.as_ref().unwrap().reason,
            conclusion.reason
        );
    }

    /// Verify that concluding a stopped experiment succeeds and preserves the original `stopped_at` timestamp rather than overwriting it.
    #[test]
    fn conclude_stopped_experiment_succeeds() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        let stopped = store.stop("e1").unwrap();
        let stopped_at = stopped.stopped_at;
        assert!(stopped_at.is_some());

        let conclusion = ExperimentConclusion {
            winner: None,
            reason: "Inconclusive — ending experiment".to_string(),
            control_metric: 0.10,
            variant_metric: 0.11,
            confidence: 0.60,
            significant: false,
            promoted: false,
        };

        let concluded = store.conclude("e1", conclusion).unwrap();
        assert_eq!(concluded.status, ExperimentStatus::Concluded);
        // stopped_at must be preserved from the stop transition, not overwritten
        assert_eq!(concluded.stopped_at, stopped_at);
        assert!(concluded.conclusion.is_some());
        assert!(concluded.conclusion.as_ref().unwrap().winner.is_none());
    }

    /// Verify that attempting to conclude an already-concluded experiment returns `InvalidStatus`, preventing conclusion overwrites.
    #[test]
    fn conclude_already_concluded_returns_invalid_status() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();

        let conclusion = ExperimentConclusion {
            winner: Some("variant".to_string()),
            reason: "First conclusion".to_string(),
            control_metric: 0.12,
            variant_metric: 0.14,
            confidence: 0.97,
            significant: true,
            promoted: false,
        };
        store.conclude("e1", conclusion).unwrap();

        let second = ExperimentConclusion {
            winner: Some("control".to_string()),
            reason: "Trying to override".to_string(),
            control_metric: 0.12,
            variant_metric: 0.14,
            confidence: 0.97,
            significant: true,
            promoted: false,
        };
        assert!(matches!(
            store.conclude("e1", second),
            Err(ExperimentError::InvalidStatus(_))
        ));
    }

    /// Verify that concluding a draft experiment (never started) returns `InvalidStatus`.
    #[test]
    fn conclude_draft_experiment_returns_invalid_status() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();

        let conclusion = ExperimentConclusion {
            winner: Some("variant".to_string()),
            reason: "Statistically significant result".to_string(),
            control_metric: 0.12,
            variant_metric: 0.14,
            confidence: 0.97,
            significant: true,
            promoted: false,
        };

        assert!(matches!(
            store.conclude("e1", conclusion),
            Err(ExperimentError::InvalidStatus(_))
        ));
    }

    #[test]
    fn delete_draft_succeeds() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.delete("e1").unwrap();
        assert!(matches!(store.get("e1"), Err(ExperimentError::NotFound(_))));
    }

    #[test]
    fn delete_running_experiment_returns_invalid_status() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        assert!(matches!(
            store.delete("e1"),
            Err(ExperimentError::InvalidStatus(_))
        ));
    }

    #[test]
    fn get_active_for_index_returns_running_experiment() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        assert!(store.get_active_for_index("products").is_some());
        assert!(store.get_active_for_index("articles").is_none());
    }

    #[test]
    fn get_active_for_index_returns_none_for_draft() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        assert!(store.get_active_for_index("products").is_none());
    }

    /// Verify that starting a second experiment on an index that already has a running experiment returns `InvalidStatus` mentioning the index name.
    #[test]
    fn start_second_experiment_on_same_index_fails() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.create(make_experiment("e2", "products")).unwrap();
        store.start("e1").unwrap();
        let result = store.start("e2");
        assert!(
            result.is_err(),
            "starting a second experiment on the same index should fail"
        );
        match result {
            Err(ExperimentError::InvalidStatus(msg)) => {
                assert!(
                    msg.contains("products"),
                    "error should mention the index name"
                );
            }
            other => panic!("expected InvalidStatus, got: {:?}", other),
        }
    }

    #[test]
    fn start_experiment_on_different_index_succeeds() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.create(make_experiment("e2", "articles")).unwrap();
        store.start("e1").unwrap();
        assert!(
            store.start("e2").is_ok(),
            "starting experiment on different index should succeed"
        );
    }

    #[test]
    fn get_active_for_index_returns_none_for_stopped() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        assert!(store.get_active_for_index("products").is_some());
        store.stop("e1").unwrap();
        assert!(
            store.get_active_for_index("products").is_none(),
            "stopped experiment must not be returned as active"
        );
    }

    /// Verify that `get_active_for_index` returns `None` after an experiment is concluded, ensuring concluded experiments are not treated as active.
    #[test]
    fn get_active_for_index_returns_none_for_concluded() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.start("e1").unwrap();
        assert!(store.get_active_for_index("products").is_some());
        let conclusion = ExperimentConclusion {
            winner: Some("variant".to_string()),
            reason: "test".to_string(),
            control_metric: 0.1,
            variant_metric: 0.2,
            confidence: 0.95,
            significant: true,
            promoted: false,
        };
        store.conclude("e1", conclusion).unwrap();
        assert!(
            store.get_active_for_index("products").is_none(),
            "concluded experiment must not be returned as active"
        );
    }

    #[test]
    fn experiments_persist_across_store_restart() {
        let tmp = TempDir::new().unwrap();
        {
            let store = ExperimentStore::new(tmp.path()).unwrap();
            store.create(make_experiment("e1", "products")).unwrap();
        }
        let store2 = ExperimentStore::new(tmp.path()).unwrap();
        let loaded = store2.get("e1").unwrap();
        assert_eq!(loaded.id, "e1");
    }

    /// Verify that the `interleaving` flag on an experiment survives serialization and is correctly restored when a new store is constructed from the same data directory.
    #[test]
    fn interleaving_flag_persists_across_store_restart() {
        let tmp = TempDir::new().unwrap();
        {
            let store = ExperimentStore::new(tmp.path()).unwrap();
            let mut exp = make_experiment("e-il", "products_il");
            exp.interleaving = Some(true);
            exp.variant.query_overrides = None;
            exp.variant.index_name = Some("products_il_v2".to_string());
            store.create(exp).unwrap();
        }
        let store2 = ExperimentStore::new(tmp.path()).unwrap();
        let loaded = store2.get("e-il").unwrap();
        assert_eq!(
            loaded.interleaving,
            Some(true),
            "interleaving flag must survive persistence"
        );
    }

    /// Verify that constructing a store fails with `InvalidConfig` when a persisted experiment file contains an experiment that fails validation.
    #[test]
    fn new_store_rejects_invalid_experiment_from_disk() {
        let tmp = TempDir::new().unwrap();
        let experiments_dir = tmp.path().join(".experiments");
        std::fs::create_dir_all(&experiments_dir).unwrap();

        let mut invalid = make_experiment("bad1", "products");
        invalid.variant.index_name = Some("products_variant".to_string());
        let path = experiments_dir.join("bad1.json");
        std::fs::write(path, serde_json::to_string_pretty(&invalid).unwrap()).unwrap();

        let result = ExperimentStore::new(tmp.path());
        assert!(
            matches!(result, Err(ExperimentError::InvalidConfig(_))),
            "invalid persisted experiments must fail store startup with InvalidConfig"
        );
    }

    #[test]
    fn create_assigns_sequential_numeric_ids() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        store.create(make_experiment("e2", "articles")).unwrap();
        let id1 = store.get_numeric_id("e1").unwrap();
        let id2 = store.get_numeric_id("e2").unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn get_by_numeric_id_returns_experiment() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        let exp = store.get_by_numeric_id(1).unwrap();
        assert_eq!(exp.id, "e1");
    }

    #[test]
    fn get_by_numeric_id_nonexistent_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        assert!(store.get_by_numeric_id(999).is_err());
    }

    #[test]
    fn numeric_ids_persist_across_store_restart() {
        let tmp = TempDir::new().unwrap();
        {
            let store = ExperimentStore::new(tmp.path()).unwrap();
            store.create(make_experiment("e1", "products")).unwrap();
            store.create(make_experiment("e2", "articles")).unwrap();
        }
        let store2 = ExperimentStore::new(tmp.path()).unwrap();
        assert_eq!(store2.get_numeric_id("e1"), Some(1));
        assert_eq!(store2.get_numeric_id("e2"), Some(2));
        // New experiments should continue from where we left off
        store2.create(make_experiment("e3", "blog")).unwrap();
        assert_eq!(store2.get_numeric_id("e3"), Some(3));
    }

    #[test]
    fn delete_removes_numeric_id_mapping() {
        let tmp = TempDir::new().unwrap();
        let store = ExperimentStore::new(tmp.path()).unwrap();
        store.create(make_experiment("e1", "products")).unwrap();
        assert!(store.get_numeric_id("e1").is_some());
        store.delete("e1").unwrap();
        assert!(store.get_numeric_id("e1").is_none());
        assert!(store.get_by_numeric_id(1).is_err());
    }

    /// Verify that constructing a store fails with `InvalidConfig` when the persisted data contains two running experiments targeting the same index.
    #[test]
    fn new_store_rejects_multiple_running_experiments_for_same_index() {
        let tmp = TempDir::new().unwrap();
        let experiments_dir = tmp.path().join(".experiments");
        std::fs::create_dir_all(&experiments_dir).unwrap();

        let mut running_a = make_experiment("run-a", "products");
        running_a.status = ExperimentStatus::Running;
        running_a.started_at = Some(1700000000000);

        let mut running_b = make_experiment("run-b", "products");
        running_b.status = ExperimentStatus::Running;
        running_b.started_at = Some(1700000001000);

        std::fs::write(
            experiments_dir.join("run-a.json"),
            serde_json::to_string_pretty(&running_a).unwrap(),
        )
        .unwrap();
        std::fs::write(
            experiments_dir.join("run-b.json"),
            serde_json::to_string_pretty(&running_b).unwrap(),
        )
        .unwrap();

        let result = ExperimentStore::new(tmp.path());
        assert!(
            matches!(result, Err(ExperimentError::InvalidConfig(_))),
            "store startup must reject multiple running experiments for the same index"
        );
    }
}
