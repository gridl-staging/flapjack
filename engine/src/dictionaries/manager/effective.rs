use super::*;

impl DictionaryManager {
    /// Get effective stopwords for a language, merging built-in + custom.
    /// If `disableStandardEntries` is true for this language, only custom entries are returned.
    pub fn effective_stopwords(
        &self,
        tenant_id: &str,
        lang: &str,
    ) -> Result<std::collections::HashSet<String>, DictionaryError> {
        let store = self.store_for(tenant_id)?;
        let settings = store.load_settings()?;
        let mut words = std::collections::HashSet::new();

        // Add built-in stopwords unless disabled
        if !settings.is_standard_disabled(DictionaryName::Stopwords, lang) {
            if let Some(builtin) = crate::query::stopwords::stopwords_for_lang(lang) {
                words.extend(builtin.into_iter().map(|s| s.to_string()));
            }
        }

        // Load custom entries once, then partition into enabled/disabled
        let custom = store.load_stopwords()?;
        for entry in &custom {
            if entry.language == lang && entry.state == EntryState::Enabled {
                words.insert(entry.word.clone());
            }
        }
        // Remove words that have a disabled custom entry (overrides built-in too)
        for entry in &custom {
            if entry.language == lang && entry.state == EntryState::Disabled {
                words.remove(&entry.word);
            }
        }

        Ok(words)
    }

    /// Get custom plural equivalence sets for a language.
    ///
    /// Built-in plurals are rule-based (via `expand_plurals_for_lang`) and not
    /// returned here. The query pipeline checks `is_standard_disabled` separately
    /// to decide whether to run the built-in expander.
    pub fn custom_plural_sets(
        &self,
        tenant_id: &str,
        lang: &str,
    ) -> Result<Vec<Vec<String>>, DictionaryError> {
        let store = self.store_for(tenant_id)?;
        let custom = store.load_plurals()?;
        Ok(custom
            .into_iter()
            .filter(|e| e.language == lang)
            .map(|e| e.words)
            .collect())
    }

    /// Check if the built-in plural expander should be used for this language.
    pub fn use_builtin_plurals(
        &self,
        tenant_id: &str,
        lang: &str,
    ) -> Result<bool, DictionaryError> {
        let settings = self.get_settings(tenant_id)?;
        Ok(!settings.is_standard_disabled(DictionaryName::Plurals, lang))
    }

    /// Get effective compound decompositions for a language, merging built-in + custom.
    pub fn effective_compounds(
        &self,
        tenant_id: &str,
        lang: &str,
    ) -> Result<HashMap<String, Vec<String>>, DictionaryError> {
        let store = self.store_for(tenant_id)?;
        let mut decompositions: HashMap<String, Vec<String>> = HashMap::new();

        // Built-in decompound data is handled by the GermanDecompounder etc.
        // Custom entries override/augment. If standard is disabled, the query
        // pipeline should skip the built-in decompounder for this language.
        // We only return the custom decomposition map here; the pipeline
        // checks `is_standard_disabled` separately.

        let custom = store.load_compounds()?;
        for entry in custom {
            if entry.language == lang {
                decompositions.insert(entry.word.clone(), entry.decomposition);
            }
        }

        Ok(decompositions)
    }

    /// Check if standard entries are disabled for a dictionary type + language.
    pub fn is_standard_disabled(
        &self,
        tenant_id: &str,
        dict_name: DictionaryName,
        lang: &str,
    ) -> Result<bool, DictionaryError> {
        let settings = self.get_settings(tenant_id)?;
        Ok(settings.is_standard_disabled(dict_name, lang))
    }
}
