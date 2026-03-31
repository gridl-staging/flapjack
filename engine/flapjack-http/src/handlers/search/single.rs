//! Stub summary for single.rs.
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use flapjack::error::FlapjackError;
use flapjack::experiments::interleaving::{team_draft_interleave, Team};
use flapjack::index::DEFAULT_RELEVANCY_STRICTNESS;
use flapjack::types::SearchResult;

use crate::dto::SearchRequest;
use crate::handlers::replicas::resolve_search_target;
use crate::handlers::AppState;

use super::experiments::{resolve_experiment_context, ExperimentContext};
use super::geo::{apply_rule_geo_overrides, resolve_around_lat_lng_via_ip};
use super::hybrid::{
    apply_hybrid_fusion, resolve_hybrid_search_inputs, HybridFusionContext, HybridSearchInputs,
};
use super::personalization::{resolve_personalization_context, PersonalizationContext};
use super::pipeline::{hybrid_fetch_window, measure_pipeline_elapsed, PreparedSearchParams};
use super::request::{
    apply_key_restrictions, can_see_unretrievable_attributes, compute_hits_cap,
    extract_analytics_headers, merge_secured_filters,
};
use super::response::{format_search_response, ResponseAssemblyContext};
use super::single_support::{
    apply_similar_query_override, build_facet_requests, normalize_query_languages,
    resolve_distinct_count, resolve_effective_relevancy_strictness, resolve_optional_filter_groups,
    resolve_search_sort, resolve_search_window, resolve_typo_tolerance,
};
use super::transforms::apply_reranking_and_transforms;

pub async fn search_single(
    State(state): State<Arc<AppState>>,
    index_name: String,
    req: SearchRequest,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    search_single_with_secured_hits_per_page_cap(State(state), index_name, req, None, false, None)
        .await
}

// ---------------------------------------------------------------------------
// Phase 1: Prepare search parameters
// ---------------------------------------------------------------------------

/// Resolves all search parameters from the request and index settings into a
/// `PreparedSearchParams` struct: filters, sort, facets, pagination, typo tolerance, and rules.
fn prepare_search_params(
    state: &AppState,
    effective_index: &str,
    req: &mut SearchRequest,
    has_personalization: bool,
) -> PreparedSearchParams {
    let filter = req.build_combined_filter();
    let sort = resolve_search_sort(req);
    let loaded_settings = state.manager.get_settings(effective_index);
    let facet_requests = build_facet_requests(req, loaded_settings.as_deref());
    let distinct_count = resolve_distinct_count(req, loaded_settings.as_deref());
    let geo_params = req.build_geo_params();
    let optional_filter_groups = resolve_optional_filter_groups(req);
    let search_window = resolve_search_window(
        req,
        has_personalization && sort.is_none() && !geo_params.has_geo_filter(),
        geo_params.has_geo_filter(),
        optional_filter_groups.is_some(),
    );
    let typo_tolerance = resolve_typo_tolerance(req);
    let sum_or_filters_scores = req.sum_or_filters_scores.unwrap_or(false);
    let effective_enable_rules = req
        .enable_rules
        .or(loaded_settings.as_ref().and_then(|s| s.enable_rules));
    let pagination_limited_exceeded = loaded_settings.as_ref().is_some_and(|settings| {
        req.page.saturating_mul(search_window.hits_per_page)
            >= settings.pagination_limited_to as usize
    });
    let all_query_words_optional = apply_similar_query_override(req);
    normalize_query_languages(req);

    PreparedSearchParams {
        filter,
        sort,
        loaded_settings,
        effective_relevancy_strictness: req
            .relevancy_strictness
            .unwrap_or(DEFAULT_RELEVANCY_STRICTNESS),
        facet_requests,
        distinct_count,
        geo_params,
        hits_per_page: search_window.hits_per_page,
        fetch_limit: search_window.limit,
        fetch_offset: search_window.offset,
        typo_tolerance,
        optional_filter_groups,
        sum_or_filters_scores,
        effective_enable_rules,
        pagination_limited_exceeded,
        all_query_words_optional,
        should_window_for_personalization: search_window.should_window_for_personalization,
        is_hybrid_active: false, // set by execute_core_search
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Execute core search (with interleaving)
// ---------------------------------------------------------------------------

struct CoreSearchContext<'a> {
    state: &'a Arc<AppState>,
    effective_index: &'a str,
    dictionary_lookup_tenant: &'a str,
    req: &'a SearchRequest,
    params: &'a mut PreparedSearchParams,
    experiment_ctx: Option<ExperimentContext>,
    assignment_query_id: &'a str,
    hybrid_inputs: HybridSearchInputs<'a>,
    secured_hits_per_page_cap: Option<usize>,
}

#[derive(Clone, Copy)]
struct SearchInvocation<'a> {
    state: &'a Arc<AppState>,
    req: &'a SearchRequest,
    params: &'a PreparedSearchParams,
    secured_hits_per_page_cap: Option<usize>,
    dictionary_lookup_tenant: &'a str,
}

impl SearchInvocation<'_> {
    /// Executes a search against the given tenant index, resolving virtual replica
    /// settings and dictionary scope before delegating to the core search engine.
    fn run(
        self,
        tenant_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult, FlapjackError> {
        let resolved_target = resolve_search_target(self.state, tenant_id);
        let dictionary_lookup_tenant = if resolved_target.settings_override.is_some() {
            // Virtual replica requests should read dictionary entries scoped to
            // the requested replica name rather than the primary index.
            tenant_id
        } else {
            self.dictionary_lookup_tenant
        };
        let effective_relevancy_strictness = resolve_effective_relevancy_strictness(
            self.req,
            resolved_target.settings_override.as_ref(),
        );

        self.state
            .manager
            .search_full_with_stop_words_with_hits_per_page_cap(
                &resolved_target.data_index,
                &self.req.query,
                &flapjack::index::SearchOptions {
                    filter: self.params.filter.as_ref(),
                    sort: self.params.sort.as_ref(),
                    limit,
                    offset,
                    facets: self.params.facet_requests.as_deref(),
                    distinct: self.params.distinct_count,
                    max_values_per_facet: self.req.max_values_per_facet,
                    remove_stop_words: self.req.remove_stop_words.as_ref(),
                    ignore_plurals: self.req.ignore_plurals.as_ref(),
                    query_languages: self.req.query_languages.as_ref(),
                    query_type: self.req.query_type_prefix.as_deref(),
                    typo_tolerance: self.params.typo_tolerance,
                    advanced_syntax: self.req.advanced_syntax,
                    remove_words_if_no_results: self.req.remove_words_if_no_results.as_deref(),
                    optional_filter_specs: self.params.optional_filter_groups.as_deref(),
                    sum_or_filters_scores: self.params.sum_or_filters_scores,
                    exact_on_single_word_query: self.req.exact_on_single_word_query.as_deref(),
                    disable_exact_on_attributes: self.req.disable_exact_on_attributes.as_deref(),
                    enable_synonyms: self.req.enable_synonyms,
                    enable_rules: self.params.effective_enable_rules,
                    rule_contexts: self.req.rule_contexts.as_deref(),
                    restrict_searchable_attrs: self.req.restrict_searchable_attributes.as_deref(),
                    secured_hits_per_page_cap: self.secured_hits_per_page_cap,
                    decompound_query: self.req.decompound_query,
                    settings_override: resolved_target.settings_override.as_ref(),
                    dictionary_lookup_tenant: Some(dictionary_lookup_tenant),
                    all_query_words_optional: self.params.all_query_words_optional,
                    relevancy_strictness: Some(effective_relevancy_strictness),
                    min_proximity: self.req.min_proximity,
                    advanced_syntax_features: self.req.advanced_syntax_features.as_deref(),
                    ranking_synonym_store: None,
                    ranking_plural_map: None,
                },
            )
    }
}

struct CoreSearchOutcome {
    result: SearchResult,
    experiment_ctx: Option<ExperimentContext>,
    is_interleaving: bool,
}

struct InterleavingRequest<'a> {
    effective_index: &'a str,
    variant_index: String,
    assignment_query_id: &'a str,
}

struct InterleavingPage<'a> {
    interleave_k: usize,
    hits_per_page: usize,
    page: usize,
    assignment_query_id: &'a str,
}

/// Carries all state needed to run the synchronous search pipeline on a blocking
/// thread: resolved params, auth context, experiment/personalization/hybrid state.
struct SearchSyncContext {
    state: Arc<AppState>,
    index_name: String,
    effective_index: String,
    dictionary_lookup_tenant: String,
    req: SearchRequest,
    secured_hits_per_page_cap: Option<usize>,
    can_see_unretrievable_attributes: bool,
    enqueue_time: Instant,
    query_id: Option<String>,
    assignment_query_id: String,
    experiment_ctx: Option<ExperimentContext>,
    personalization_ctx: Option<PersonalizationContext>,
    query_vector: Option<Vec<f32>>,
    hybrid_params: Option<super::hybrid::HybridParams>,
}

fn apply_hybrid_fetch_window(
    req: &SearchRequest,
    params: &mut PreparedSearchParams,
    hybrid_inputs: HybridSearchInputs<'_>,
) {
    params.is_hybrid_active = hybrid_inputs.is_hybrid_active();
    if params.is_hybrid_active {
        params.fetch_limit = hybrid_fetch_window(params.hits_per_page, req.page);
        params.fetch_offset = 0;
    }
}

/// Merges control and variant search results using team-draft interleaving for
/// A/B test experiments, paginating the interleaved list and recording team assignments.
fn build_interleaved_result(
    control_result: SearchResult,
    variant_result: SearchResult,
    page: InterleavingPage<'_>,
    experiment_ctx: Option<&mut ExperimentContext>,
) -> SearchResult {
    let control_ids: Vec<&str> = control_result
        .documents
        .iter()
        .map(|doc| doc.document.id.as_str())
        .collect();
    let variant_ids: Vec<&str> = variant_result
        .documents
        .iter()
        .map(|doc| doc.document.id.as_str())
        .collect();
    let interleaved = team_draft_interleave(
        &control_ids,
        &variant_ids,
        page.interleave_k,
        experiment_ctx
            .as_ref()
            .map(|ctx| ctx.experiment_id.as_str())
            .unwrap_or(""),
        page.assignment_query_id,
    );

    let control_map: HashMap<String, flapjack::types::ScoredDocument> = control_result
        .documents
        .iter()
        .cloned()
        .map(|doc| (doc.document.id.clone(), doc))
        .collect();
    let variant_map: HashMap<String, flapjack::types::ScoredDocument> = variant_result
        .documents
        .iter()
        .cloned()
        .map(|doc| (doc.document.id.clone(), doc))
        .collect();

    let interleaved_docs_with_team: Vec<(flapjack::types::ScoredDocument, String)> = interleaved
        .into_iter()
        .filter_map(|item| {
            let team_label = match item.team {
                Team::A => "control",
                Team::B => "variant",
            };
            let doc = match item.team {
                Team::A => control_map
                    .get(&item.doc_id)
                    .cloned()
                    .or_else(|| variant_map.get(&item.doc_id).cloned()),
                Team::B => variant_map
                    .get(&item.doc_id)
                    .cloned()
                    .or_else(|| control_map.get(&item.doc_id).cloned()),
            };
            doc.map(|scored_doc| (scored_doc, team_label.to_string()))
        })
        .collect();

    let total_interleaved = interleaved_docs_with_team.len();
    let page_start = (page.page * page.hits_per_page).min(total_interleaved);
    let page_end = (page_start + page.hits_per_page).min(total_interleaved);
    let page_slice = &interleaved_docs_with_team[page_start..page_end];

    if let Some(ctx) = experiment_ctx {
        ctx.interleaved_teams = Some(
            page_slice
                .iter()
                .map(|(doc, team)| (doc.document.id.clone(), team.clone()))
                .collect(),
        );
    }

    SearchResult {
        documents: page_slice.iter().map(|(doc, _)| doc.clone()).collect(),
        total: total_interleaved,
        facets: control_result.facets,
        facets_stats: control_result.facets_stats,
        user_data: control_result.user_data,
        applied_rules: control_result.applied_rules,
        parsed_query: control_result.parsed_query,
        exhaustive_facet_values: control_result.exhaustive_facet_values,
        exhaustive_rules_match: control_result.exhaustive_rules_match,
        query_after_removal: control_result.query_after_removal,
        rendering_content: control_result.rendering_content,
        effective_around_lat_lng: control_result.effective_around_lat_lng,
        effective_around_radius: control_result.effective_around_radius,
    }
}

/// Runs both control and variant searches for an interleaving experiment, falling
/// back to control-only results if the variant index is not found.
fn run_interleaved_search(
    invocation: SearchInvocation<'_>,
    request: InterleavingRequest<'_>,
    mut experiment_ctx: Option<ExperimentContext>,
) -> Result<CoreSearchOutcome, FlapjackError> {
    let interleave_k = invocation
        .params
        .fetch_limit
        .saturating_add(invocation.params.fetch_offset)
        .max(invocation.params.hits_per_page);
    let control_result = invocation.run(request.effective_index, interleave_k, 0)?;

    match invocation.run(&request.variant_index, interleave_k, 0) {
        Ok(variant_result) => Ok(CoreSearchOutcome {
            result: build_interleaved_result(
                control_result,
                variant_result,
                InterleavingPage {
                    interleave_k,
                    hits_per_page: invocation.params.hits_per_page,
                    page: invocation.req.page,
                    assignment_query_id: request.assignment_query_id,
                },
                experiment_ctx.as_mut(),
            ),
            experiment_ctx,
            is_interleaving: true,
        }),
        Err(FlapjackError::TenantNotFound(_)) => Ok(CoreSearchOutcome {
            result: invocation.run(
                request.effective_index,
                invocation.params.fetch_limit,
                invocation.params.fetch_offset,
            )?,
            experiment_ctx: None,
            is_interleaving: false,
        }),
        Err(err) => Err(err),
    }
}

/// Dispatches the initial search: returns empty results if pagination limit is exceeded,
/// runs interleaved search if an experiment is active, or performs a standard search.
fn run_initial_search(
    invocation: SearchInvocation<'_>,
    effective_index: &str,
    experiment_ctx: Option<ExperimentContext>,
    assignment_query_id: &str,
) -> Result<CoreSearchOutcome, FlapjackError> {
    if invocation.params.pagination_limited_exceeded {
        let mut limited = invocation.run(effective_index, 0, 0)?;
        limited.documents.clear();
        return Ok(CoreSearchOutcome {
            result: limited,
            experiment_ctx,
            is_interleaving: false,
        });
    }

    if let Some(variant_index) = experiment_ctx
        .as_ref()
        .and_then(|ctx| ctx.interleaving_variant_index.clone())
    {
        return run_interleaved_search(
            invocation,
            InterleavingRequest {
                effective_index,
                variant_index,
                assignment_query_id,
            },
            experiment_ctx,
        );
    }

    Ok(CoreSearchOutcome {
        result: invocation.run(
            effective_index,
            invocation.params.fetch_limit,
            invocation.params.fetch_offset,
        )?,
        experiment_ctx,
        is_interleaving: false,
    })
}

/// Re-executes the search with a wider fetch window when query rules inject geo
/// parameters (aroundLatLng/aroundRadius) that were not present in the original request.
fn rerun_search_for_rule_geo(
    invocation: SearchInvocation<'_>,
    effective_index: &str,
    mut result: SearchResult,
    is_interleaving: bool,
) -> Result<SearchResult, FlapjackError> {
    if invocation.params.pagination_limited_exceeded
        || is_interleaving
        || invocation.params.geo_params.has_geo_filter()
    {
        return Ok(result);
    }

    let rule_geo_params = apply_rule_geo_overrides(
        invocation.params.geo_params.clone(),
        result.effective_around_lat_lng.as_deref(),
        result.effective_around_radius.as_ref(),
    );
    if !rule_geo_params.has_geo_filter() {
        return Ok(result);
    }

    let geo_fetch_limit = (invocation.params.hits_per_page
        + invocation.req.page * invocation.params.hits_per_page)
        .saturating_mul(10)
        .max(1000);
    result = invocation.run(effective_index, geo_fetch_limit, 0)?;
    Ok(result)
}

/// Orchestrates the full core search: applies hybrid fetch window, resolves
/// relevancy strictness, runs initial search, and re-runs for rule-injected geo.
fn execute_core_search(
    context: CoreSearchContext<'_>,
) -> Result<(SearchResult, Option<ExperimentContext>, bool), FlapjackError> {
    let CoreSearchContext {
        state,
        effective_index,
        dictionary_lookup_tenant,
        req,
        params,
        experiment_ctx,
        assignment_query_id,
        hybrid_inputs,
        secured_hits_per_page_cap,
    } = context;

    apply_hybrid_fetch_window(req, params, hybrid_inputs);
    params.effective_relevancy_strictness = resolve_effective_relevancy_strictness(
        req,
        resolve_search_target(state, effective_index)
            .settings_override
            .as_ref(),
    );

    let invocation = SearchInvocation {
        state,
        req,
        params,
        secured_hits_per_page_cap,
        dictionary_lookup_tenant,
    };
    let CoreSearchOutcome {
        result,
        experiment_ctx,
        is_interleaving,
    } = run_initial_search(
        invocation,
        effective_index,
        experiment_ctx,
        assignment_query_id,
    )?;
    let result = rerun_search_for_rule_geo(invocation, effective_index, result, is_interleaving)?;

    Ok((result, experiment_ctx, is_interleaving))
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Top-level single-index search orchestrator: validates the request, resolves
/// experiment/personalization/hybrid context, executes search on a blocking thread,
/// applies post-search transforms, and assembles the Algolia-compatible JSON response.
pub(super) async fn search_single_with_secured_hits_per_page_cap(
    State(state): State<Arc<AppState>>,
    index_name: String,
    mut req: SearchRequest,
    secured_hits_per_page_cap: Option<usize>,
    can_see_unretrievable: bool,
    dictionary_lookup_tenant: Option<String>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    req.validate()?;
    resolve_around_lat_lng_via_ip(&mut req, &state.geoip_reader);

    let enqueue_time = Instant::now();

    let query_id = if req.click_analytics == Some(true) {
        Some(hex::encode(uuid::Uuid::new_v4().as_bytes()))
    } else {
        None
    };
    let assignment_query_id = query_id
        .clone()
        .unwrap_or_else(|| hex::encode(uuid::Uuid::new_v4().as_bytes()));
    let (effective_index, experiment_ctx) =
        resolve_experiment_context(&state, &index_name, &mut req, &assignment_query_id);

    let (query_vector, hybrid_params) =
        resolve_hybrid_search_inputs(&state, &effective_index, &req).await;
    let dictionary_lookup_tenant = dictionary_lookup_tenant.unwrap_or_else(|| index_name.clone());

    let index_settings = state.manager.get_settings(&effective_index);
    let personalization_ctx =
        resolve_personalization_context(&state, &req, index_settings.as_deref()).await;

    tokio::task::spawn_blocking(move || {
        search_single_sync(SearchSyncContext {
            state,
            index_name,
            effective_index,
            dictionary_lookup_tenant,
            req,
            secured_hits_per_page_cap,
            can_see_unretrievable_attributes: can_see_unretrievable,
            enqueue_time,
            query_id,
            assignment_query_id,
            experiment_ctx,
            personalization_ctx,
            query_vector,
            hybrid_params,
        })
    })
    .await
    .map_err(|e| FlapjackError::InvalidQuery(format!("spawn_blocking join error: {}", e)))?
}

/// Top-level synchronous search orchestrator. Calls the extracted phase functions
/// in sequence, threading `PreparedSearchParams` and intermediate results between them.
fn search_single_sync(
    context: SearchSyncContext,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    let SearchSyncContext {
        state,
        index_name,
        effective_index,
        dictionary_lookup_tenant,
        mut req,
        secured_hits_per_page_cap,
        can_see_unretrievable_attributes,
        enqueue_time,
        query_id,
        assignment_query_id,
        experiment_ctx,
        personalization_ctx,
        query_vector,
        hybrid_params,
    } = context;

    let queue_wait = enqueue_time.elapsed();
    let start = Instant::now();

    // Phase 1: Prepare search parameters
    let mut params = prepare_search_params(
        &state,
        &effective_index,
        &mut req,
        personalization_ctx.is_some(),
    );

    // Phase 2: Execute core search (with interleaving)
    let (mut result, experiment_ctx, is_interleaving) = execute_core_search(CoreSearchContext {
        state: &state,
        effective_index: &effective_index,
        dictionary_lookup_tenant: &dictionary_lookup_tenant,
        req: &req,
        params: &mut params,
        experiment_ctx,
        assignment_query_id: &assignment_query_id,
        hybrid_inputs: HybridSearchInputs {
            query_vector: &query_vector,
            hybrid_params: &hybrid_params,
        },
        secured_hits_per_page_cap,
    })?;

    let ((fallback_message, transform_outputs), search_elapsed) =
        measure_pipeline_elapsed(start, || {
            let fallback_message = apply_hybrid_fusion(
                &HybridFusionContext {
                    state: &state,
                    effective_index: &effective_index,
                    hybrid_inputs: HybridSearchInputs {
                        query_vector: &query_vector,
                        hybrid_params: &hybrid_params,
                    },
                    hits_per_page: params.hits_per_page,
                    page: req.page,
                    is_interleaving,
                    pagination_limited_exceeded: params.pagination_limited_exceeded,
                },
                &mut result,
            );

            let transform_outputs = apply_reranking_and_transforms(
                &state,
                &req,
                &params,
                &mut result,
                &effective_index,
                personalization_ctx.as_ref(),
                is_interleaving,
            );

            (fallback_message, transform_outputs)
        });

    let applied_relevancy_strictness = Some(params.effective_relevancy_strictness);
    let is_virtual_replica_search = resolve_search_target(&state, &effective_index)
        .settings_override
        .is_some();

    // Phase 4: Format response
    format_search_response(
        result,
        &req,
        &params,
        ResponseAssemblyContext {
            state: &state,
            index_name: &index_name,
            effective_index: &effective_index,
            queue_wait,
            search_elapsed,
            start,
            query_id,
            experiment_ctx,
            fallback_message,
            transform_outputs,
            can_see_unretrievable_attributes,
            applied_relevancy_strictness,
            is_virtual_replica_search,
        },
    )
}

// ---------------------------------------------------------------------------
// Public endpoint handlers
// ---------------------------------------------------------------------------

/// Shared auth/analytics extraction from an incoming request.
struct ExtractedRequestContext {
    secured_restrictions: Option<crate::auth::SecuredKeyRestrictions>,
    api_key: Option<crate::auth::ApiKey>,
    dictionary_lookup_tenant: Option<String>,
    user_token_header: Option<String>,
    user_ip: Option<String>,
    session_id_header: Option<String>,
}

impl ExtractedRequestContext {
    /// TODO: Document ExtractedRequestContext.from_request.
    fn from_request(request: &axum::extract::Request) -> Self {
        let secured_restrictions = request
            .extensions()
            .get::<crate::auth::SecuredKeyRestrictions>()
            .cloned();
        let api_key = request.extensions().get::<crate::auth::ApiKey>().cloned();
        let dictionary_lookup_tenant = request
            .extensions()
            .get::<crate::auth::AuthenticatedAppId>()
            .map(|id| id.0.clone());
        let (user_token_header, user_ip, session_id_header) = extract_analytics_headers(request);
        Self {
            secured_restrictions,
            api_key,
            dictionary_lookup_tenant,
            user_token_header,
            user_ip,
            session_id_header,
        }
    }

    /// Apply auth restrictions and analytics headers to the search request.
    fn apply_to(&self, req: &mut SearchRequest) {
        if let Some(ref restrictions) = self.secured_restrictions {
            merge_secured_filters(req, restrictions);
        }
        apply_key_restrictions(req, &self.api_key);
        if req.user_token.is_none() {
            req.user_token = self.user_token_header.clone();
        }
        if req.session_id.is_none() {
            req.session_id = self.session_id_header.clone();
        }
        req.user_ip = self.user_ip.clone();
    }

    fn hits_cap(&self) -> Option<usize> {
        compute_hits_cap(&self.api_key, &self.secured_restrictions)
    }

    fn can_see_unretrievable(&self) -> bool {
        can_see_unretrievable_attributes(&self.api_key)
    }
}

/// Search an index with full-text query and filters
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/query",
    tag = "search",
    params(
        ("indexName" = String, Path, description = "Index to search")
    ),
    request_body(content = SearchRequest, description = "Search parameters including query, filters, facets, and pagination"),
    responses(
        (status = 200, description = "Search results with hits and facets", body = crate::dto::SearchResponse),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn search(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    request: axum::extract::Request,
) -> Result<axum::response::Response, FlapjackError> {
    let ctx = ExtractedRequestContext::from_request(&request);
    let body_bytes = axum::body::to_bytes(request.into_body(), 10_000_000)
        .await
        .map_err(|e| FlapjackError::InvalidQuery(format!("Failed to read body: {}", e)))?;
    let mut req: SearchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| FlapjackError::InvalidQuery(format!("Invalid JSON: {}", e)))?;
    req.apply_params_string();
    ctx.apply_to(&mut req);
    let Json(response) = search_single_with_secured_hits_per_page_cap(
        State(state),
        index_name,
        req,
        ctx.hits_cap(),
        ctx.can_see_unretrievable(),
        ctx.dictionary_lookup_tenant,
    )
    .await?;

    if let Some(qid) = response.get("queryID").and_then(|v| v.as_str()) {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-algolia-query-id", qid.parse().unwrap());
        Ok((headers, Json(response)).into_response())
    } else {
        Ok(Json(response).into_response())
    }
}

/// Search an index using query-string parameters on GET /1/indexes/{indexName}
#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}",
    tag = "search",
    params(
        ("indexName" = String, Path, description = "Index to search")
    ),
    responses(
        (status = 200, description = "Search results with hits and facets", body = crate::dto::SearchResponse),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn search_get(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    let ctx = ExtractedRequestContext::from_request(&request);
    let raw_query = request.uri().query().unwrap_or("").to_string();

    let mut req = SearchRequest {
        params: Some(raw_query.clone()),
        ..Default::default()
    };
    req.apply_params_string();
    ctx.apply_to(&mut req);
    let mut response = search_single_with_secured_hits_per_page_cap(
        State(state),
        index_name,
        req,
        ctx.hits_cap(),
        ctx.can_see_unretrievable(),
        ctx.dictionary_lookup_tenant,
    )
    .await?
    .0;
    response["params"] = serde_json::Value::String(raw_query);
    Ok(Json(response))
}
