use super::*;

mod plural_expansion;
mod query_execution;

#[cfg(feature = "decompound")]
use plural_expansion::apply_decompound;
use plural_expansion::build_plural_map;
use query_execution::{
    execute_expanded_queries, maybe_cache_facets, resolve_total_hits,
    ExpandedQueryExecutionContext, PreparedSearchFilters,
};

/// Resolved index, searcher, and settings for a tenant search.
pub(super) struct ResolvedSearch {
    pub index: Arc<Index>,
    pub searcher: tantivy::Searcher,
    pub settings: Option<Arc<IndexSettings>>,
    pub relevance_config: RelevanceConfig,
}

/// Preprocessed query state produced before the main search loop.
pub(super) struct PreprocessedQuery {
    pub query_text_stopped: String,
    pub plural_map: Option<HashMap<String, Vec<String>>>,
    pub custom_normalization: Vec<(char, String)>,
    pub _effective_query_languages: Vec<String>,
    pub query_type: String,
    pub effective_exact_on_single_word_query: String,
    pub effective_disable_exact_on_attributes: Vec<String>,
    #[cfg(feature = "decompound")]
    pub decompound_enabled: bool,
    #[cfg(feature = "decompound")]
    pub decompound_langs: Vec<String>,
}

impl super::IndexManager {
    /// Load the tenant index, acquire a searcher, and resolve settings.
    pub(super) fn resolve_search_settings(
        &self,
        tenant_id: &str,
        settings_override: Option<&IndexSettings>,
    ) -> Result<ResolvedSearch> {
        let index = self.get_or_load(tenant_id)?;
        let reader = index.reader();
        let searcher = reader.searcher();
        let settings = settings_override
            .map(|s| Arc::new(s.clone()))
            .or_else(|| self.get_settings(tenant_id));
        if let Some(ref s) = settings {
            tracing::debug!("[SEARCH] Loaded settings query_type={}", s.query_type);
        }
        let relevance_config = RelevanceConfig {
            searchable_attributes: settings
                .as_ref()
                .and_then(|s| s.searchable_attributes.clone()),
            attribute_weights: HashMap::new(),
        };
        Ok(ResolvedSearch {
            index,
            searcher,
            settings,
            relevance_config,
        })
    }
}

/// Preprocess the query: resolve stop words, plurals, decompound,
/// custom normalization, and effective query/exact settings.
pub(super) fn preprocess_query(
    tenant_id: &str,
    settings: &Option<Arc<IndexSettings>>,
    query_text: &str,
    opts: &SearchOptions<'_>,
    dictionary_manager: Option<&Arc<crate::dictionaries::manager::DictionaryManager>>,
) -> PreprocessedQuery {
    let qt = opts
        .query_type
        .unwrap_or_else(|| {
            settings
                .as_ref()
                .map(|s| s.query_type.as_str())
                .unwrap_or("prefixLast")
        })
        .to_string();

    let effective_exact_on_single_word_query = opts
        .exact_on_single_word_query
        .or(settings
            .as_ref()
            .map(|s| s.exact_on_single_word_query.as_str()))
        .unwrap_or("attribute")
        .to_string();

    let effective_disable_exact_on_attributes = opts
        .disable_exact_on_attributes
        .or(settings
            .as_ref()
            .and_then(|s| s.disable_exact_on_attributes.as_deref()))
        .unwrap_or(&[])
        .to_vec();

    let effective_stop_words = opts
        .remove_stop_words
        .or(settings.as_ref().map(|s| &s.remove_stop_words));
    let raw_effective_query_languages = opts
        .query_languages
        .map(|v| v.as_slice())
        .or(settings.as_ref().map(|s| s.query_languages.as_slice()))
        .unwrap_or(&[]);
    let effective_query_languages = normalize_query_languages(raw_effective_query_languages);

    let query_text_stopped = match effective_stop_words {
        Some(sw) => remove_stop_words_with_dictionary_manager(
            query_text,
            sw,
            &qt,
            &effective_query_languages,
            dictionary_manager,
            tenant_id,
        ),
        None => query_text.to_string(),
    };

    let custom_normalization: Vec<(char, String)> = settings
        .as_deref()
        .map(IndexSettings::flatten_custom_normalization)
        .unwrap_or_default();

    let plural_map = build_plural_map(
        tenant_id,
        settings,
        opts.ignore_plurals,
        &effective_query_languages,
        &query_text_stopped,
        dictionary_manager,
    );

    // Decompound processing: split compound words in Germanic languages.
    #[cfg(feature = "decompound")]
    let decompound_keep_diacritics = settings
        .as_ref()
        .map(|s| s.keep_diacritics_on_characters.as_str())
        .unwrap_or("");
    #[cfg(feature = "decompound")]
    let decompound_langs: Vec<String> = effective_query_languages
        .iter()
        .filter(|l| crate::query::decompound::supports_decompound(l))
        .cloned()
        .collect();
    #[cfg(feature = "decompound")]
    let decompound_enabled = opts.decompound_query.unwrap_or(true);
    #[cfg(feature = "decompound")]
    let plural_map = if decompound_enabled && !decompound_langs.is_empty() {
        apply_decompound(
            tenant_id,
            &query_text_stopped,
            plural_map,
            &decompound_langs,
            decompound_keep_diacritics,
            &custom_normalization,
            dictionary_manager,
        )
    } else {
        plural_map
    };
    #[cfg(not(feature = "decompound"))]
    let _ = opts.decompound_query; // suppress unused warning

    PreprocessedQuery {
        query_text_stopped,
        plural_map,
        custom_normalization,
        _effective_query_languages: effective_query_languages,
        query_type: qt,
        effective_exact_on_single_word_query,
        effective_disable_exact_on_attributes,
        #[cfg(feature = "decompound")]
        decompound_enabled,
        #[cfg(feature = "decompound")]
        decompound_langs,
    }
}

/// Prepare rule-filter merges, searchable-path weighting, and parser inputs.
pub(super) fn prepare_search_filters(
    manager: &super::IndexManager,
    tenant_id: &str,
    query_text: &str,
    resolved: &ResolvedSearch,
    preprocessed: &PreprocessedQuery,
    opts: &SearchOptions<'_>,
) -> Result<PreparedSearchFilters> {
    let SearchOptions {
        filter,
        limit,
        offset,
        enable_synonyms,
        enable_rules,
        rule_contexts,
        restrict_searchable_attrs,
        optional_filter_specs,
        sum_or_filters_scores,
        secured_hits_per_page_cap,
        ..
    } = *opts;

    let rules_enabled = enable_rules.unwrap_or(true);
    let synonyms_enabled = enable_synonyms.unwrap_or(true);
    let synonym_store = if synonyms_enabled {
        manager.get_synonyms(tenant_id)
    } else {
        None
    };

    let (query_text_rewritten, rule_effects) = if rules_enabled {
        if let Some(store) = manager.get_rules(tenant_id) {
            let effects =
                store.apply_rules(query_text, rule_contexts, filter, synonym_store.as_deref());
            let rewritten = effects
                .rewritten_query
                .clone()
                .unwrap_or_else(|| query_text.to_string());
            (rewritten, Some(effects))
        } else {
            (query_text.to_string(), None)
        }
    } else {
        (query_text.to_string(), None)
    };

    let configured_facet_set = resolved.settings.as_ref().map(|s| s.facet_set());
    let effective_params = build_effective_search_params(&SearchParamsInput {
        request_filter: filter,
        request_limit: limit,
        request_offset: offset,
        request_restrict_searchable_attrs: restrict_searchable_attrs,
        request_optional_filter_specs: optional_filter_specs,
        sum_or_filters_scores,
        exact_on_single_word_query_override: Some(
            preprocessed.effective_exact_on_single_word_query.as_str(),
        ),
        disable_exact_on_attributes_override: Some(
            preprocessed
                .effective_disable_exact_on_attributes
                .as_slice(),
        ),
        configured_facet_set: configured_facet_set.as_ref(),
        rule_effects: rule_effects.as_ref(),
        hits_per_page_cap: secured_hits_per_page_cap,
    })?;

    let parsed_query = query_text_rewritten.clone();
    let expanded_queries = if synonyms_enabled {
        if let Some(store) = synonym_store.as_ref() {
            store.expand_query(&query_text_rewritten)
        } else {
            vec![query_text_rewritten.clone()]
        }
    } else {
        vec![query_text_rewritten.clone()]
    };

    let schema = resolved.index.inner().schema();
    let json_search_field = schema
        .get_field("_json_search")
        .map_err(|_| FlapjackError::FieldNotFound("_json_search".to_string()))?;
    let json_exact_field = schema
        .get_field("_json_exact")
        .map_err(|_| FlapjackError::FieldNotFound("_json_exact".to_string()))?;

    let all_searchable_paths: Vec<String> = resolved.index.searchable_paths();
    let (searchable_paths, field_weights): (Vec<String>, Vec<f32>) =
        build_searchable_paths_with_weights(
            &all_searchable_paths,
            resolved.relevance_config.searchable_attributes.as_deref(),
        );

    let (searchable_paths, field_weights) = apply_restrict_searchable_attrs(
        searchable_paths,
        field_weights,
        effective_params.restrict_searchable_attrs.as_deref(),
    );

    Ok(PreparedSearchFilters {
        effective_params,
        query_text_rewritten,
        parsed_query,
        expanded_queries,
        searchable_paths,
        field_weights,
        schema,
        json_search_field,
        json_exact_field,
        rule_effects,
        synonym_store,
    })
}

/// Execute the parsed queries, collect docs/facets, apply ranking and rules,
/// and assemble the pre-fallback search result.
pub(super) fn execute_search_query(
    manager: &super::IndexManager,
    tenant_id: &str,
    query_text: &str,
    resolved: &ResolvedSearch,
    preprocessed: &PreprocessedQuery,
    prepared: &PreparedSearchFilters,
    opts: &SearchOptions<'_>,
) -> Result<SearchResult> {
    let SearchOptions {
        sort,
        facets,
        distinct,
        max_values_per_facet,
        typo_tolerance: typo_tolerance_override,
        advanced_syntax: advanced_syntax_override,
        advanced_syntax_features: advanced_syntax_features_override,
        all_query_words_optional,
        relevancy_strictness,
        min_proximity,
        ranking_synonym_store,
        ranking_plural_map,
        ..
    } = *opts;

    let index = &resolved.index;
    let searcher = &resolved.searcher;
    let settings = &resolved.settings;
    let effective_params = &prepared.effective_params;

    let parsed_query = prepared.parsed_query.clone();
    let query_text_rewritten = prepared.query_text_rewritten.clone();
    let expanded_queries = prepared.expanded_queries.clone();
    let searchable_paths = prepared.searchable_paths.clone();
    let field_weights = prepared.field_weights.clone();
    let schema = prepared.schema.clone();
    let json_search_field = prepared.json_search_field;
    let json_exact_field = prepared.json_exact_field;

    let qt = preprocessed.query_type.as_str();
    let custom_normalization = &preprocessed.custom_normalization;
    let plural_map = &preprocessed.plural_map;

    let default_sort_owned = if sort.is_none() && query_text.trim().is_empty() {
        Some(Sort::ByField {
            field: "objectID".to_string(),
            order: crate::types::SortOrder::Desc,
        })
    } else {
        None
    };
    let effective_sort: Option<&Sort> = sort.or(default_sort_owned.as_ref());

    let typo_enabled = typo_tolerance_override.unwrap_or(true);
    let min_word_1_typo = settings
        .as_ref()
        .map(|s| s.min_word_size_for_1_typo as usize)
        .unwrap_or(4);
    let min_word_2_typos = settings
        .as_ref()
        .map(|s| s.min_word_size_for_2_typos as usize)
        .unwrap_or(8);
    let disable_typo_words = settings
        .as_ref()
        .and_then(|s| s.disable_typo_tolerance_on_words.as_deref())
        .unwrap_or(&[]);
    let disable_typo_attrs = settings
        .as_ref()
        .and_then(|s| s.disable_typo_tolerance_on_attributes.as_deref())
        .unwrap_or(&[]);
    let adv_syntax = advanced_syntax_override.unwrap_or(false);
    let stemmer_lang = settings.as_ref().and_then(|s| {
        let cjk = crate::index::Index::needs_cjk_tokenizer(&s.index_languages);
        if cjk {
            None
        } else {
            crate::index::Index::stemmer_language_for_index(&s.index_languages)
        }
    });

    let searchable_fields = vec![json_search_field];
    let parser = QueryParser::new_with_weights(
        &schema,
        searchable_fields,
        field_weights.clone(),
        searchable_paths.clone(),
    )
    .with_exact_field(json_exact_field)
    .with_indexed_separators(
        settings
            .as_ref()
            .map(|s| s.separators_to_index.chars().collect())
            .unwrap_or_default(),
    )
    .with_query_type(qt)
    .with_typo_tolerance(typo_enabled)
    .with_disabled_typo_words(disable_typo_words.to_vec())
    .with_disabled_typo_attrs(disable_typo_attrs.to_vec())
    .with_keep_diacritics_on_characters(
        settings
            .as_ref()
            .map(|s| s.keep_diacritics_on_characters.as_str())
            .unwrap_or(""),
    )
    .with_custom_normalization(custom_normalization.clone())
    .with_min_word_size_for_1_typo(min_word_1_typo)
    .with_min_word_size_for_2_typos(min_word_2_typos)
    .with_advanced_syntax(adv_syntax)
    .with_all_optional(all_query_words_optional)
    .with_plural_map(plural_map.clone())
    .with_stemmer_language(stemmer_lang);
    let parser = if let Some(features) = advanced_syntax_features_override {
        parser.with_advanced_syntax_features(features.to_vec())
    } else if let Some(ref s) = settings {
        if let Some(ref features) = s.advanced_syntax_features {
            parser.with_advanced_syntax_features(features.clone())
        } else {
            parser
        }
    } else {
        parser
    };

    let (facet_cache_key, facet_result) = if let Some(facet_reqs) = facets {
        let mut facet_keys: Vec<String> = facet_reqs.iter().map(|r| r.field.clone()).collect();
        facet_keys.sort();
        let filter_hash = effective_params
            .filter
            .as_ref()
            .map(|f| format!("{:?}", f))
            .unwrap_or_default();
        let cache_key = format!("{}:{}:{}", tenant_id, filter_hash, facet_keys.join(","));
        let cached_result = manager.facet_cache.get(&cache_key).and_then(|cached| {
            let (timestamp, count, facets_map, facets_stats, cached_exhaustive_facets) =
                cached.as_ref();
            if timestamp.elapsed() < std::time::Duration::from_secs(5) {
                tracing::debug!(
                    "[FACET_CACHE] HIT ({}ms old)",
                    timestamp.elapsed().as_millis()
                );
                let executor = QueryExecutor::new(index.converter(), schema.clone())
                    .with_settings(settings.clone())
                    .with_query(query_text_rewritten.clone())
                    .with_max_values_per_facet(max_values_per_facet);
                let trimmed = executor.trim_facet_counts(facets_map.clone(), facet_reqs);
                let trimmed_further = facets_map.iter().any(|(field, original_counts)| {
                    let trimmed_len = trimmed.get(field).map(|values| values.len()).unwrap_or(0);
                    trimmed_len < original_counts.len()
                });
                Some((
                    *count,
                    trimmed,
                    facets_stats.clone(),
                    *cached_exhaustive_facets && !trimmed_further,
                ))
            } else {
                tracing::debug!(
                    "[FACET_CACHE] STALE ({}ms old)",
                    timestamp.elapsed().as_millis()
                );
                None
            }
        });
        (Some(cache_key), cached_result)
    } else {
        (None, None)
    };

    let effective_around_lat_lng = effective_params.around_lat_lng.clone();
    let effective_around_radius = effective_params.around_radius.clone();
    if effective_params.limit == 0 {
        let (total, facets_map, facets_stats, exhaustive_facet_values) = match facet_result {
            Some((count, facets, stats, exhaustive_facets)) => {
                (count, facets, stats, exhaustive_facets)
            }
            None => {
                let primary_query = crate::types::Query {
                    text: query_text_rewritten.clone(),
                };
                let parsed = parser.parse(&primary_query)?;
                let executor = QueryExecutor::new(index.converter(), schema.clone())
                    .with_settings(settings.clone())
                    .with_query(query_text_rewritten.clone())
                    .with_max_values_per_facet(max_values_per_facet);
                let expanded = executor.expand_short_query_with_searcher(parsed, searcher)?;
                let final_query =
                    executor.apply_filter(expanded, effective_params.filter.as_ref())?;
                if let Some(facet_reqs) = facets {
                    let mut facet_collector =
                        tantivy::collector::FacetCollector::for_field("_facets");
                    for req in facet_reqs {
                        facet_collector.add_facet(&req.path);
                    }
                    let (count, facet_counts) = searcher.search(
                        final_query.as_ref(),
                        &(tantivy::collector::Count, facet_collector),
                    )?;
                    let (facets_map, facets_stats, exhaustive_facets) =
                        executor.extract_facet_counts_and_stats(&facet_counts, facet_reqs);
                    maybe_cache_facets(
                        manager,
                        facet_cache_key.as_ref(),
                        count,
                        &facets_map,
                        &facets_stats,
                        exhaustive_facets,
                    );
                    (count, facets_map, facets_stats, exhaustive_facets)
                } else {
                    let count =
                        searcher.search(final_query.as_ref(), &tantivy::collector::Count)?;
                    (count, HashMap::new(), HashMap::new(), true)
                }
            }
        };
        return Ok(SearchResult {
            documents: Vec::new(),
            total,
            facets: facets_map,
            facets_stats,
            user_data: Vec::new(),
            applied_rules: Vec::new(),
            parsed_query,
            exhaustive_facet_values,
            exhaustive_rules_match: true,
            query_after_removal: None,
            rendering_content: None,
            effective_around_lat_lng,
            effective_around_radius,
        });
    }

    let hidden_window_padding = prepared
        .rule_effects
        .as_ref()
        .map(|effects| effects.hidden.iter().collect::<HashSet<_>>().len())
        .unwrap_or(0);
    let effective_limit = effective_params
        .limit
        .saturating_add(effective_params.offset)
        .saturating_add(hidden_window_padding);
    #[cfg(feature = "decompound")]
    let allow_split_alternatives =
        preprocessed.decompound_enabled || preprocessed.decompound_langs.is_empty();
    #[cfg(not(feature = "decompound"))]
    let allow_split_alternatives = true;

    let (mut all_results, query_totals, facet_result) = execute_expanded_queries(
        expanded_queries,
        facet_result,
        &ExpandedQueryExecutionContext {
            manager,
            index,
            searcher,
            settings,
            schema: &schema,
            parser: &parser,
            effective_sort,
            effective_filter: effective_params.filter.as_ref(),
            optional_filter_specs: effective_params.optional_filter_specs.as_deref(),
            facets,
            distinct,
            max_values_per_facet,
            effective_limit,
            facet_cache_key: facet_cache_key.as_ref(),
            allow_split_alternatives,
            query_text,
            json_exact_field,
            searchable_paths: &searchable_paths,
        },
    )?;

    if matches!(effective_sort, None | Some(Sort::ByRelevance)) {
        sort_results_with_stage2_ranking(
            &mut all_results,
            Stage2RankingContext {
                query_text: &parsed_query,
                searchable_paths: &searchable_paths,
                settings: settings.as_deref(),
                synonym_store: ranking_synonym_store.or(prepared.synonym_store.as_deref()),
                plural_map: ranking_plural_map.or(plural_map.as_ref()),
                query_type: qt,
                optional_filter_groups: effective_params.optional_filter_specs.as_deref(),
                sum_or_filters_scores: effective_params.sum_or_filters_scores,
                exact_on_single_word_query: &effective_params.exact_on_single_word_query,
                disable_exact_on_attributes: &effective_params.disable_exact_on_attributes,
                custom_normalization,
                keep_diacritics_on_characters: settings
                    .as_ref()
                    .map(|s| s.keep_diacritics_on_characters.as_str())
                    .unwrap_or(""),
                camel_case_attributes: settings
                    .as_ref()
                    .map(|s| s.camel_case_attributes.as_slice())
                    .unwrap_or(&[]),
                all_query_words_optional,
                relevancy_strictness,
                min_proximity,
            },
        );
    }

    let result_count = all_results.len();
    let mut total = resolve_total_hits(&query_totals, result_count, effective_limit);

    let (ruled_results, user_data, applied_rules, rendering_content) = if let Some(ref effects) =
        prepared.rule_effects
    {
        let hidden_count = if effects.hidden.is_empty() {
            0
        } else {
            let hidden_ids: HashSet<&str> = effects.hidden.iter().map(|id| id.as_str()).collect();
            all_results
                .iter()
                .filter(|doc| hidden_ids.contains(doc.document.id.as_str()))
                .count()
        };

        let executor = QueryExecutor::new(index.converter(), schema.clone())
            .with_settings(settings.clone())
            .with_query(query_text.to_string())
            .with_max_values_per_facet(max_values_per_facet);
        let docs = executor.apply_rules_to_results(
            searcher,
            all_results,
            effects,
            effective_params.filter.as_ref(),
        )?;

        total = total.saturating_sub(hidden_count);
        (
            docs,
            effects.user_data.clone(),
            effects.applied_rules.clone(),
            effects.rendering_content.clone(),
        )
    } else {
        (all_results, Vec::new(), Vec::new(), None)
    };

    let ruled_count = ruled_results.len();
    let start = effective_params.offset.min(ruled_count);
    let end = (start + effective_params.limit).min(ruled_count);
    let final_docs = ruled_results[start..end].to_vec();

    let (facets_map, facets_stats, exhaustive_facet_values) = match facet_result {
        Some((_, facets, stats, exhaustive_facets)) => (facets, stats, exhaustive_facets),
        None => (HashMap::new(), HashMap::new(), true),
    };

    Ok(SearchResult {
        documents: final_docs,
        total,
        facets: facets_map,
        facets_stats,
        user_data,
        applied_rules,
        parsed_query,
        exhaustive_facet_values,
        exhaustive_rules_match: true,
        query_after_removal: None,
        rendering_content,
        effective_around_lat_lng,
        effective_around_radius,
    })
}

/// TODO: Document apply_restrict_searchable_attrs.
fn apply_restrict_searchable_attrs(
    searchable_paths: Vec<String>,
    field_weights: Vec<f32>,
    restrict_searchable_attrs: Option<&[String]>,
) -> (Vec<String>, Vec<f32>) {
    if let Some(restrict) = restrict_searchable_attrs {
        let mut filtered_paths = Vec::new();
        let mut filtered_weights = Vec::new();
        for (index, path) in searchable_paths.iter().enumerate() {
            if restrict.iter().any(|value| value == path) {
                filtered_paths.push(path.clone());
                filtered_weights.push(field_weights[index]);
            }
        }
        if filtered_paths.is_empty() {
            (searchable_paths, field_weights)
        } else {
            (filtered_paths, filtered_weights)
        }
    } else {
        (searchable_paths, field_weights)
    }
}

/// TODO: Document build_searchable_paths_with_weights.
pub(super) fn build_searchable_paths_with_weights(
    all_searchable_paths: &[String],
    searchable_attributes: Option<&[String]>,
) -> (Vec<String>, Vec<f32>) {
    match searchable_attributes {
        Some(attrs) => {
            let mut weighted: Vec<(String, f32)> = Vec::new();
            let mut unweighted: Vec<String> = Vec::new();
            let mut configured_ranks: HashMap<String, usize> = HashMap::new();
            let mut next_rank = 0usize;
            let mut unordered_rank: Option<usize> = None;
            for attr in attrs {
                let stripped = strip_unordered_prefix(attr);
                if configured_ranks.contains_key(stripped) {
                    continue;
                }

                let rank = if stripped != attr {
                    *unordered_rank.get_or_insert_with(|| {
                        let current = next_rank;
                        next_rank += 1;
                        current
                    })
                } else {
                    let current = next_rank;
                    next_rank += 1;
                    current
                };

                configured_ranks.insert(stripped.to_string(), rank);
            }

            for path in all_searchable_paths {
                if let Some(pos) = configured_ranks.get(path) {
                    weighted.push((path.clone(), 100_f32.powi(-(*pos as i32))));
                } else {
                    unweighted.push(path.clone());
                }
            }

            weighted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            let mut paths: Vec<String> = weighted.iter().map(|(p, _)| p.clone()).collect();
            let mut weights: Vec<f32> = weighted.iter().map(|(_, w)| *w).collect();
            let min_weight = weights.last().copied().unwrap_or(1.0) * 0.01;
            for path in unweighted {
                paths.push(path);
                weights.push(min_weight);
            }
            (paths, weights)
        }
        None => {
            let weights = vec![1.0; all_searchable_paths.len()];
            (all_searchable_paths.to_vec(), weights)
        }
    }
}

/// Build the query after removal markup for removeWordsIfNoResults retry.
/// Kept words are plain, removed words are wrapped in `<em>` tags.
pub(super) fn apply_remove_words_fallback(
    manager: &super::IndexManager,
    tenant_id: &str,
    query_text: &str,
    remove_strategy: &str,
    opts: &SearchOptions<'_>,
) -> Option<SearchResult> {
    let words: Vec<&str> = query_text.split_whitespace().collect();
    if words.len() <= 1 {
        return None;
    }
    let fallback_queries: Vec<(String, usize)> = match remove_strategy {
        "lastWords" => (1..words.len())
            .map(|drop| (words[..words.len() - drop].join(" "), drop))
            .collect(),
        "firstWords" => (1..words.len())
            .map(|drop| (words[drop..].join(" "), drop))
            .collect(),
        _ => return None,
    };
    for (fallback_q, drop_count) in &fallback_queries {
        let retry_opts = SearchOptions {
            remove_words_if_no_results: Some("none"),
            ..*opts
        };
        if let Ok(mut retry) = manager.search_full_with_stop_words_with_hits_per_page_cap(
            tenant_id,
            fallback_q,
            &retry_opts,
        ) {
            if retry.total > 0 {
                retry.query_after_removal = Some(build_query_after_removal_markup(
                    &words,
                    remove_strategy,
                    *drop_count,
                ));
                return Some(retry);
            }
        }
    }
    None
}
