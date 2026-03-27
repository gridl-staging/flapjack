//! Provides the DictionaryManager, the single entry point for all dictionary operations including batch mutations, search, settings, and per-language counts across a multi-tenant data directory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};

use super::persistence::DictionaryStore;
use super::{
    BatchAction, BatchDictionaryRequest, CompoundEntry, DictionaryCount, DictionaryError,
    DictionaryName, DictionarySearchRequest, DictionarySearchResponse, DictionarySettings,
    EntryState, LanguageDictionaryCounts, MutationResponse, PluralEntry, StopwordEntry,
};

mod batch;
mod effective;
mod search;

/// Application-level dictionary manager.
///
/// In a multi-tenant setup, dictionaries are per-tenant. The manager uses the
/// data directory root and resolves per-tenant stores on demand.
pub struct DictionaryManager {
    data_dir: PathBuf,
    next_task_id: AtomicI64,
}

impl DictionaryManager {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            next_task_id: AtomicI64::new(1),
        }
    }

    fn store_for(&self, tenant_id: &str) -> Result<DictionaryStore, DictionaryError> {
        Self::validate_tenant_id(tenant_id)?;
        DictionaryStore::for_tenant(&self.data_dir, tenant_id)
    }

    /// Reject tenant IDs that could cause path traversal or filesystem issues.
    fn validate_tenant_id(tenant_id: &str) -> Result<(), DictionaryError> {
        if tenant_id.is_empty() {
            return Err(DictionaryError::InvalidEntry(
                "tenant ID must not be empty".to_string(),
            ));
        }
        if tenant_id.contains("..")
            || tenant_id.contains('/')
            || tenant_id.contains('\\')
            || tenant_id.contains('\0')
        {
            return Err(DictionaryError::InvalidEntry(
                "tenant ID contains invalid characters".to_string(),
            ));
        }
        Ok(())
    }

    fn make_mutation_response(&self) -> MutationResponse {
        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);
        MutationResponse {
            task_id,
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn normalize_language(&self, raw: &str) -> Result<String, DictionaryError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(DictionaryError::InvalidEntry(
                "language is required".to_string(),
            ));
        }

        let parsed = trimmed
            .parse::<crate::language::LanguageCode>()
            .map_err(|_| {
                DictionaryError::InvalidEntry(format!("unsupported language code '{}'", raw))
            })?;
        Ok(parsed.as_str().to_string())
    }

    // ── Settings ──────────────────────────────────────────────────────

    pub fn get_settings(&self, tenant_id: &str) -> Result<DictionarySettings, DictionaryError> {
        self.store_for(tenant_id)?.load_settings()
    }

    /// Persist dictionary settings for a tenant, normalizing all language codes.
    ///
    /// # Returns
    ///
    /// A `MutationResponse` with a monotonically increasing task ID and an RFC 3339 timestamp.
    pub fn set_settings(
        &self,
        tenant_id: &str,
        settings: &DictionarySettings,
    ) -> Result<MutationResponse, DictionaryError> {
        let mut normalized = DictionarySettings::default();
        for (dict_name, language_map) in &settings.disable_standard_entries {
            let normalized_map = normalized
                .disable_standard_entries
                .entry(*dict_name)
                .or_default();
            for (language, disabled) in language_map {
                let normalized_language = self.normalize_language(language)?;
                normalized_map.insert(normalized_language, *disabled);
            }
        }

        self.store_for(tenant_id)?.save_settings(&normalized)?;
        Ok(self.make_mutation_response())
    }

    // ── Languages ─────────────────────────────────────────────────────

    /// Returns per-language custom entry counts across all dictionary types.
    pub fn list_languages(
        &self,
        tenant_id: &str,
    ) -> Result<HashMap<String, LanguageDictionaryCounts>, DictionaryError> {
        let store = self.store_for(tenant_id)?;
        let counts = store.count_entries_by_language()?;

        let mut result = HashMap::new();
        for (lang, type_counts) in counts {
            let stopwords_count = type_counts.get(&DictionaryName::Stopwords).copied();
            let plurals_count = type_counts.get(&DictionaryName::Plurals).copied();
            let compounds_count = type_counts.get(&DictionaryName::Compounds).copied();

            result.insert(
                lang,
                LanguageDictionaryCounts {
                    stopwords: stopwords_count.map(|n| DictionaryCount {
                        nb_custom_entries: n,
                    }),
                    plurals: plurals_count.map(|n| DictionaryCount {
                        nb_custom_entries: n,
                    }),
                    compounds: compounds_count.map(|n| DictionaryCount {
                        nb_custom_entries: n,
                    }),
                },
            );
        }

        Ok(result)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
