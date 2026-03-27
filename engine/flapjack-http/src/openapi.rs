use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Flapjack API",
        version = "0.1.0",
        description = "Drop-in replacement for Algolia search. Provides full-text search with filters, facets, geo-search, and more.",
        license(name = "MIT OR Apache-2.0"),
        contact(
            name = "Flapjack",
            url = "https://github.com/stuartcrobinson/flapjack"
        )
    ),
    servers(
        (url = "http://localhost:7700", description = "Local development"),
        (url = "https://fj-us-west-1.flapjack.foo", description = "Production US West")
    ),
    paths(
        crate::handlers::health::health,
        crate::handlers::indices::create_index,
        crate::handlers::indices::delete_index,
        crate::handlers::indices::list_indices,
        crate::handlers::indices::clear_index,
        crate::handlers::indices::operation_index,
        crate::handlers::search::search,
        crate::handlers::search::search_get,
        crate::handlers::search::batch_search,
        crate::handlers::objects::add_documents,
        crate::handlers::objects::get_object,
        crate::handlers::objects::delete_object,
        crate::handlers::objects::put_object,
        crate::handlers::objects::get_objects,
        crate::handlers::objects::delete_by_query,
        crate::handlers::browse::browse_index,
        crate::handlers::facets::search_facet_values,
        crate::handlers::settings::get_settings,
        crate::handlers::settings::set_settings,
        crate::handlers::settings::set_settings_put_doc,
        crate::handlers::tasks::get_task,
        crate::handlers::tasks::get_task_for_index,
        crate::handlers::synonyms::get_synonym,
        crate::handlers::synonyms::save_synonym,
        crate::handlers::synonyms::delete_synonym,
        crate::handlers::synonyms::save_synonyms,
        crate::handlers::synonyms::clear_synonyms,
        crate::handlers::synonyms::search_synonyms,
        crate::handlers::rules::get_rule,
        crate::handlers::rules::save_rule,
        crate::handlers::rules::delete_rule,
        crate::handlers::rules::save_rules,
        crate::handlers::rules::clear_rules,
        crate::handlers::rules::search_rules,
        crate::handlers::keys::create_key,
        crate::handlers::keys::list_keys,
        crate::handlers::keys::get_key,
        crate::handlers::keys::update_key,
        crate::handlers::keys::delete_key,
        crate::handlers::keys::restore_key,
        crate::handlers::keys::generate_secured_key,
        crate::handlers::snapshot::export_snapshot,
        crate::handlers::snapshot::import_snapshot,
        crate::handlers::snapshot::snapshot_to_s3,
        crate::handlers::snapshot::restore_from_s3,
        crate::handlers::snapshot::list_s3_snapshots,
        crate::handlers::security_sources::get_security_sources,
        crate::handlers::security_sources::replace_security_sources,
        crate::handlers::security_sources::append_security_source,
        crate::handlers::security_sources::delete_security_source,
        // Stage 7: Recommendations
        crate::handlers::recommend::recommend,
        // Stage 7: Personalization
        crate::handlers::personalization::set_personalization_strategy,
        crate::handlers::personalization::get_personalization_strategy,
        crate::handlers::personalization::get_user_profile,
        crate::handlers::personalization::delete_user_profile,
        // Stage 7: Experiments
        crate::handlers::experiments::create_experiment,
        crate::handlers::experiments::list_experiments,
        crate::handlers::experiments::get_experiment,
        crate::handlers::experiments::update_experiment,
        crate::handlers::experiments::delete_experiment,
        crate::handlers::experiments::start_experiment,
        crate::handlers::experiments::stop_experiment,
        crate::handlers::experiments::conclude_experiment,
        crate::handlers::experiments::estimate::estimate_ab_test,
        crate::handlers::experiments::results::get_experiment_results,
        // Stage 7: Analytics (read)
        crate::handlers::analytics::get_top_searches,
        crate::handlers::analytics::get_search_count,
        crate::handlers::analytics::get_no_results,
        crate::handlers::analytics::get_no_result_rate,
        crate::handlers::analytics::get_no_clicks,
        crate::handlers::analytics::get_no_click_rate,
        crate::handlers::analytics::get_click_through_rate,
        crate::handlers::analytics::get_average_click_position,
        crate::handlers::analytics::get_click_positions,
        crate::handlers::analytics::get_add_to_cart_rate,
        crate::handlers::analytics::get_purchase_rate,
        crate::handlers::analytics::get_conversion_rate,
        crate::handlers::analytics::get_top_hits,
        crate::handlers::analytics::get_top_filters,
        crate::handlers::analytics::get_filter_values,
        crate::handlers::analytics::get_filters_no_results,
        crate::handlers::analytics::get_users_count,
        crate::handlers::analytics::get_overview,
        crate::handlers::analytics::get_device_breakdown,
        crate::handlers::analytics::get_geo_breakdown,
        crate::handlers::analytics::get_geo_top_searches,
        crate::handlers::analytics::get_geo_regions,
        crate::handlers::analytics::get_revenue,
        crate::handlers::analytics::get_countries,
        crate::handlers::analytics::get_analytics_status,
        // Stage 7: Analytics (mutations)
        crate::handlers::analytics::seed_analytics,
        crate::handlers::analytics::flush_analytics,
        crate::handlers::analytics::clear_analytics,
        crate::handlers::analytics::cleanup_analytics,
        // Stage 7: Query Suggestions
        crate::handlers::query_suggestions::list_configs,
        crate::handlers::query_suggestions::create_config,
        crate::handlers::query_suggestions::get_config,
        crate::handlers::query_suggestions::update_config,
        crate::handlers::query_suggestions::delete_config,
        crate::handlers::query_suggestions::get_status,
        crate::handlers::query_suggestions::get_logs,
        crate::handlers::query_suggestions::trigger_build,
        // Stage 7: Dictionaries
        crate::handlers::dictionaries::dictionary_batch,
        crate::handlers::dictionaries::dictionary_search,
        crate::handlers::dictionaries::dictionary_get_settings,
        crate::handlers::dictionaries::dictionary_set_settings,
        crate::handlers::dictionaries::dictionary_list_languages,
        // Stage 7: Insights
        crate::handlers::insights::post_events,
        crate::handlers::insights::get_debug_events,
        crate::handlers::insights::delete_usertoken,
        // Stage 7: Migration
        crate::handlers::migration::migrate_from_algolia,
        crate::handlers::migration::list_algolia_indexes,
        // Stage 7: Usage
        crate::handlers::usage::usage_global,
        crate::handlers::usage::usage_per_index,
        // Stage 7: Chat
        crate::handlers::chat::chat_index,
    ),
    components(
        schemas(
            crate::dto::CreateIndexRequest,
            crate::dto::IndexSchema,
            crate::handlers::indices::CreateIndexResponse,
            crate::handlers::indices::ListIndexItem,
            crate::handlers::indices::ListIndicesResponse,
            crate::handlers::indices::OperationIndexRequest,
            crate::dto::SearchRequest,
            crate::dto::FederationOptions,
            crate::dto::BatchSearchRequest,
            crate::dto::BatchSearchLegacyResponse,
            crate::dto::BatchSearchResponse,
            crate::dto::SearchResponse,
            crate::dto::SearchExhaustive,
            crate::dto::SearchProcessingTimings,
            crate::dto::SearchFacetStatsSummary,
            crate::dto::SearchAppliedRule,
            crate::federation::FederationConfig,
            crate::federation::FederatedHit,
            crate::federation::FederationMeta,
            crate::federation::FederatedResponse,
            crate::dto::AddDocumentsRequest,
            crate::dto::BatchOperation,
            crate::dto::AddDocumentsResponse,
            crate::dto::GetObjectsRequest,
            crate::dto::GetObjectRequest,
            crate::dto::GetObjectsResponse,
            crate::dto::DeleteByQueryRequest,
            crate::dto::SearchFacetValuesRequest,
            crate::dto::SearchFacetValuesResponse,
            crate::dto::FacetHit,
            crate::handlers::tasks::AlgoliaTaskResponse,
            crate::auth::KeyApiResponse,
            crate::handlers::keys::CreateKeyRequest,
            crate::handlers::keys::CreateKeyResponse,
            crate::handlers::keys::ListKeysResponse,
            crate::handlers::keys::UpdateKeyResponse,
            crate::handlers::keys::DeleteKeyResponse,
            crate::handlers::keys::RestoreKeyResponse,
            crate::handlers::keys::GenerateSecuredKeyRequest,
            crate::handlers::keys::GenerateSecuredKeyResponse,
            crate::handlers::keys::SecuredKeyRestrictions,
            crate::security_sources::SecuritySourceEntry,
            crate::handlers::security_sources::SourceMutationTimestampResponse,
            // Stage 7: Recommendations
            crate::handlers::recommend::RecommendBatchRequest,
            crate::handlers::recommend::RecommendRequest,
            crate::handlers::recommend::RecommendBatchResponse,
            crate::handlers::recommend::RecommendResult,
            // Stage 7: Personalization
            flapjack::personalization::PersonalizationStrategy,
            flapjack::personalization::EventScoring,
            flapjack::personalization::FacetScoring,
            flapjack::personalization::PersonalizationProfile,
            crate::handlers::personalization::SetStrategyResponse,
            crate::handlers::personalization::DeleteProfileResponse,
            // Stage 7: Experiments (Algolia DTOs)
            crate::handlers::dto_algolia::AlgoliaCreateAbTestRequest,
            crate::handlers::dto_algolia::AlgoliaCreateAbTestResponse,
            crate::handlers::dto_algolia::AlgoliaAbTestActionResponse,
            crate::handlers::dto_algolia::AlgoliaListAbTestsResponse,
            crate::handlers::dto_algolia::AlgoliaAbTest,
            crate::handlers::dto_algolia::AlgoliaVariant,
            crate::handlers::dto_algolia::AlgoliaConfiguration,
            crate::handlers::dto_algolia::AlgoliaMinimumDetectableEffect,
            crate::handlers::dto_algolia::AlgoliaOutliersSetting,
            crate::handlers::dto_algolia::AlgoliaEmptySearchSetting,
            crate::handlers::dto_algolia::AlgoliaFeatureFilters,
            crate::handlers::dto_algolia::AlgoliaFilterEffects,
            crate::handlers::dto_algolia::AlgoliaFilterEffectsEntry,
            crate::handlers::dto_algolia::AlgoliaCreateVariant,
            crate::handlers::dto_algolia::AlgoliaCreateConfiguration,
            crate::handlers::dto_algolia::AlgoliaMetricDef,
            crate::handlers::dto_algolia::AlgoliaEstimateRequest,
            crate::handlers::dto_algolia::AlgoliaEstimateConfiguration,
            crate::handlers::dto_algolia::AlgoliaEstimateVariant,
            crate::handlers::dto_algolia::AlgoliaEstimateResponse,
            crate::handlers::experiments::CreateExperimentRequest,
            crate::handlers::experiments::ConcludeExperimentRequest,
            crate::handlers::experiments::ResultsResponse,
            crate::handlers::experiments::InterleavingResponse,
            crate::handlers::experiments::GuardRailAlertResponse,
            crate::handlers::experiments::GateResponse,
            crate::handlers::experiments::ArmResponse,
            crate::handlers::experiments::SignificanceResponse,
            crate::handlers::experiments::BayesianResponse,
            flapjack::experiments::config::Experiment,
            flapjack::experiments::config::ExperimentStatus,
            flapjack::experiments::config::ExperimentArm,
            flapjack::experiments::config::QueryOverrides,
            flapjack::experiments::config::PrimaryMetric,
            flapjack::experiments::config::ExperimentConclusion,
            // Stage 7: Analytics
            crate::handlers::analytics::SeedRequest,
            crate::handlers::analytics::AnalyticsDateCount,
            crate::handlers::analytics::AnalyticsTopSearchEntry,
            crate::handlers::analytics::AnalyticsTopSearchesResponse,
            crate::handlers::analytics::AnalyticsTopHitsResponse,
            crate::handlers::analytics::AnalyticsHitEntry,
            crate::handlers::analytics::AnalyticsFiltersResponse,
            crate::handlers::analytics::AnalyticsFilterEntry,
            crate::handlers::analytics::AnalyticsFilterValuesResponse,
            crate::handlers::analytics::AnalyticsValueCount,
            crate::handlers::analytics::AnalyticsCountWithDatesResponse,
            crate::handlers::analytics::AnalyticsRateDateEntry,
            crate::handlers::analytics::AnalyticsRateWithDatesResponse,
            crate::handlers::analytics::AnalyticsAverageClickPositionDate,
            crate::handlers::analytics::AnalyticsAverageClickPositionResponse,
            crate::handlers::analytics::AnalyticsClickPositionBucket,
            crate::handlers::analytics::AnalyticsClickPositionsResponse,
            crate::handlers::analytics::AnalyticsCountryCount,
            crate::handlers::analytics::AnalyticsCountriesResponse,
            crate::handlers::analytics::AnalyticsGeoBreakdownResponse,
            crate::handlers::analytics::AnalyticsPlatformCount,
            crate::handlers::analytics::AnalyticsPlatformDateCount,
            crate::handlers::analytics::AnalyticsDeviceBreakdownResponse,
            crate::handlers::analytics::AnalyticsGeoTopSearchesResponse,
            crate::handlers::analytics::AnalyticsRegionCount,
            crate::handlers::analytics::AnalyticsGeoRegionsResponse,
            crate::handlers::analytics::AnalyticsCurrencyRevenue,
            crate::handlers::analytics::AnalyticsRevenueDateEntry,
            crate::handlers::analytics::AnalyticsRevenueResponse,
            crate::handlers::analytics::AnalyticsOverviewIndexSummary,
            crate::handlers::analytics::AnalyticsOverviewResponse,
            crate::handlers::analytics::AnalyticsSeedResponse,
            crate::handlers::analytics::AnalyticsFlushResponse,
            crate::handlers::analytics::AnalyticsClearResponse,
            crate::handlers::analytics::AnalyticsStatusResponse,
            crate::handlers::analytics::AnalyticsCleanupResponse,
            // Stage 7: Query Suggestions (core crate types)
            flapjack::query_suggestions::QsConfig,
            flapjack::query_suggestions::QsSourceIndex,
            flapjack::query_suggestions::config::QsFacet,
            flapjack::query_suggestions::BuildStatus,
            flapjack::query_suggestions::LogEntry,
            crate::handlers::query_suggestions::QsMutationResponse,
            // Stage 7: Dictionaries (core crate types)
            flapjack::dictionaries::BatchAction,
            flapjack::dictionaries::BatchRequest,
            flapjack::dictionaries::BatchDictionaryRequest,
            flapjack::dictionaries::DictionaryName,
            flapjack::dictionaries::MutationResponse,
            flapjack::dictionaries::DictionarySearchRequest,
            flapjack::dictionaries::DictionarySearchResponse,
            flapjack::dictionaries::DictionarySettings,
            flapjack::dictionaries::LanguageDictionaryCounts,
            flapjack::dictionaries::DictionaryCount,
            // Stage 7: Insights
            crate::handlers::insights::InsightsRequest,
            flapjack::analytics::schema::InsightEvent,
            // Stage 7: Migration
            crate::handlers::migration::MigrateFromAlgoliaRequest,
            crate::handlers::migration::MigrateFromAlgoliaResponse,
            crate::handlers::migration::MigrateCount,
            crate::handlers::migration::ListAlgoliaIndexesRequest,
            crate::handlers::migration::AlgoliaIndexInfo,
            crate::handlers::migration::ListAlgoliaIndexesResponse,
            // Stage 7: Chat
            crate::handlers::chat::ChatRequest,
            crate::handlers::chat::ChatResponse,
        )
    ),
    tags(
        (name = "health", description = "Health check"),
        (name = "indices", description = "Index management operations"),
        (name = "search", description = "Search and query operations"),
        (name = "documents", description = "Document CRUD operations"),
        (name = "settings", description = "Index settings"),
        (name = "synonyms", description = "Synonym management"),
        (name = "rules", description = "Query rules"),
        (name = "keys", description = "API key management"),
        (name = "security", description = "Security source allowlist"),
        (name = "snapshots", description = "Backup and restore operations"),
        (name = "tasks", description = "Task status endpoints"),
        (name = "recommendations", description = "Recommendation endpoints"),
        (name = "personalization", description = "Personalization strategy and profile endpoints"),
        (name = "experiments", description = "A/B test experiment endpoints"),
        (name = "analytics", description = "Analytics read endpoints"),
        (name = "analytics-operations", description = "Analytics mutation/operations endpoints"),
        (name = "query-suggestions", description = "Query suggestions configuration and build endpoints"),
        (name = "dictionaries", description = "Dictionary management endpoints"),
        (name = "insights", description = "Insights event endpoints"),
        (name = "migration", description = "Algolia migration endpoints"),
        (name = "usage", description = "Usage statistics endpoints"),
        (name = "chat", description = "AI chat endpoints"),
    ),
    modifiers(&SecurityAddon, &ChatSseAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "api_key",
                utoipa::openapi::security::SecurityScheme::ApiKey(
                    utoipa::openapi::security::ApiKey::Header(
                        utoipa::openapi::security::ApiKeyValue::new("x-algolia-api-key"),
                    ),
                ),
            );
        }
    }
}

/// Adds `text/event-stream` as an additional 200-response content type on the
/// chat endpoint, since `utoipa::path` proc macro only supports a single
/// content type per response status.
struct ChatSseAddon;

impl utoipa::Modify for ChatSseAddon {
    /// TODO: Document ChatSseAddon.modify.
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::Content;

        let chat_path = "/1/indexes/{indexName}/chat";
        if let Some(path_item) = openapi.paths.paths.get_mut(chat_path) {
            if let Some(op) = path_item.post.as_mut() {
                if let Some(utoipa::openapi::RefOr::T(resp)) = op.responses.responses.get_mut("200")
                {
                    let sse_content = Content::new(Some(
                        utoipa::openapi::ObjectBuilder::new()
                            .schema_type(utoipa::openapi::schema::Type::String)
                            .description(Some("Server-Sent Events stream of chat chunks"))
                            .build(),
                    ));
                    resp.content
                        .insert("text/event-stream".to_string(), sse_content);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi_test_helpers::{schema_composition_refs, schema_ref};

    fn openapi_json() -> serde_json::Value {
        serde_json::to_value(ApiDoc::openapi()).unwrap()
    }

    /// Verify that all key-management endpoints reference concrete `$ref` schema components rather than inline definitions.
    #[test]
    fn key_endpoints_use_concrete_schema_components() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer("/paths/~11~1keys/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ListKeysResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys~1{key}/get/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/KeyApiResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1keys/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/CreateKeyRequest")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys/post/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/CreateKeyResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys~1{key}/put/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/UpdateKeyResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys~1{key}/delete/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/DeleteKeyResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys~1{key}~1restore/post/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/RestoreKeyResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1keys~1generateSecuredApiKey/post/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/GenerateSecuredKeyResponse")
        );
    }

    /// Verify that the list-indices endpoint uses a concrete `ListIndicesResponse` schema with properly typed `ListIndexItem` fields.
    #[test]
    fn list_indices_uses_concrete_response_schema() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer(
                "/paths/~11~1indexes/get/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/ListIndicesResponse")
        );
        assert_eq!(
            doc.pointer("/components/schemas/ListIndicesResponse/properties/items/items/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ListIndexItem")
        );
        assert_eq!(
            doc.pointer("/components/schemas/ListIndexItem/properties/lastBuildTimeS/type")
                .and_then(|v| v.as_str()),
            Some("integer")
        );
    }

    /// Helper: assert that a given path key exists in the generated spec.
    fn assert_path_exists(doc: &serde_json::Value, path_key: &str) {
        let paths = doc.get("paths").expect("spec must have paths");
        assert!(
            paths.get(path_key).is_some(),
            "expected path '{}' in OpenAPI spec, found: {:?}",
            path_key,
            paths.as_object().map(|m| m.keys().collect::<Vec<_>>())
        );
    }

    /// Helper: assert that a given HTTP method exists on a path.
    fn assert_path_method(doc: &serde_json::Value, path_key: &str, method: &str) {
        let paths = doc.get("paths").expect("spec must have paths");
        let path_obj = paths
            .get(path_key)
            .unwrap_or_else(|| panic!("path '{}' missing from spec", path_key));
        assert!(
            path_obj.get(method).is_some(),
            "expected method '{}' on path '{}', found: {:?}",
            method,
            path_key,
            path_obj.as_object().map(|m| m.keys().collect::<Vec<_>>())
        );
    }

    /// Stage 7: Verify recommendation endpoint appears in the generated spec.
    #[test]
    fn recommendations_endpoint_is_documented() {
        let doc = openapi_json();
        assert_path_exists(&doc, "/1/indexes/*/recommendations");
        assert_path_method(&doc, "/1/indexes/*/recommendations", "post");
        assert_eq!(
            doc.pointer(
                "/paths/~11~1indexes~1*~1recommendations/post/requestBody/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/RecommendBatchRequest")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1indexes~1*~1recommendations/post/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/RecommendBatchResponse")
        );
        let paths = doc.get("paths").expect("spec must have paths");
        assert!(
            paths
                .get("/1/indexes/{indexName}/recommendations")
                .is_none(),
            "unexpected parameterized recommendations path; router exposes wildcard variant"
        );
    }

    /// Stage 7: Verify personalization strategy endpoints appear in the generated spec.
    #[test]
    fn personalization_strategy_endpoints_are_documented() {
        let doc = openapi_json();
        assert_path_exists(&doc, "/1/strategies/personalization");
        assert_path_method(&doc, "/1/strategies/personalization", "post");
        assert_path_method(&doc, "/1/strategies/personalization", "get");
    }

    /// Stage 7: Verify personalization profile endpoints appear in the generated spec.
    #[test]
    fn personalization_profile_endpoints_are_documented() {
        let doc = openapi_json();
        assert_path_exists(&doc, "/1/profiles/personalization/{userToken}");
        assert_path_method(&doc, "/1/profiles/personalization/{userToken}", "get");
        assert_path_exists(&doc, "/1/profiles/{userToken}");
        assert_path_method(&doc, "/1/profiles/{userToken}", "delete");
    }

    /// Stage 7: Verify experiment lifecycle endpoints appear in the generated spec.
    #[test]
    fn experiments_endpoints_are_documented() {
        let doc = openapi_json();

        // CRUD
        assert_path_exists(&doc, "/2/abtests");
        assert_path_method(&doc, "/2/abtests", "post");
        assert_path_method(&doc, "/2/abtests", "get");

        // Estimate
        assert_path_exists(&doc, "/2/abtests/estimate");
        assert_path_method(&doc, "/2/abtests/estimate", "post");

        // Single experiment
        assert_path_exists(&doc, "/2/abtests/{id}");
        assert_path_method(&doc, "/2/abtests/{id}", "get");
        assert_path_method(&doc, "/2/abtests/{id}", "put");
        assert_path_method(&doc, "/2/abtests/{id}", "delete");

        // Lifecycle actions
        assert_path_exists(&doc, "/2/abtests/{id}/start");
        assert_path_method(&doc, "/2/abtests/{id}/start", "post");
        assert_path_exists(&doc, "/2/abtests/{id}/stop");
        assert_path_method(&doc, "/2/abtests/{id}/stop", "post");
        assert_path_exists(&doc, "/2/abtests/{id}/conclude");
        assert_path_method(&doc, "/2/abtests/{id}/conclude", "post");

        // Results
        assert_path_exists(&doc, "/2/abtests/{id}/results");
        assert_path_method(&doc, "/2/abtests/{id}/results", "get");
    }

    /// Stage 7: Verify SearchRequest schema includes vector/hybrid fields.
    #[test]
    fn search_request_schema_includes_vector_hybrid_fields() {
        let doc = openapi_json();
        let search_schema = doc
            .pointer("/components/schemas/SearchRequest/properties")
            .expect("SearchRequest schema should have properties");

        assert!(
            search_schema.get("mode").is_some(),
            "SearchRequest should document 'mode' field"
        );
        assert!(
            search_schema.get("hybrid").is_some(),
            "SearchRequest should document 'hybrid' field"
        );
        assert!(
            search_schema.get("relevancyStrictness").is_some(),
            "SearchRequest should document 'relevancyStrictness' field"
        );
    }

    /// Stage 7: Verify recommendation and experiment schemas are registered as components.
    #[test]
    fn stage7_schemas_are_registered() {
        let doc = openapi_json();
        let schemas = doc
            .pointer("/components/schemas")
            .expect("spec must have component schemas");

        // Recommend
        assert!(
            schemas.get("RecommendBatchRequest").is_some(),
            "RecommendBatchRequest schema missing"
        );
        assert!(
            schemas.get("RecommendBatchResponse").is_some(),
            "RecommendBatchResponse schema missing"
        );

        // Experiments (Algolia DTOs)
        assert!(
            schemas.get("AlgoliaCreateAbTestRequest").is_some(),
            "AlgoliaCreateAbTestRequest schema missing"
        );
        assert!(
            schemas.get("AlgoliaListAbTestsResponse").is_some(),
            "AlgoliaListAbTestsResponse schema missing"
        );
    }

    /// Stage 7: Verify analytics read endpoints appear in the generated spec.
    #[test]
    fn analytics_read_endpoints_are_documented() {
        let doc = openapi_json();

        let analytics_paths = [
            ("/2/searches", "get"),
            ("/2/searches/count", "get"),
            ("/2/searches/noResults", "get"),
            ("/2/searches/noResultRate", "get"),
            ("/2/searches/noClicks", "get"),
            ("/2/searches/noClickRate", "get"),
            ("/2/clicks/clickThroughRate", "get"),
            ("/2/clicks/averageClickPosition", "get"),
            ("/2/clicks/positions", "get"),
            ("/2/conversions/conversionRate", "get"),
            ("/2/conversions/addToCartRate", "get"),
            ("/2/conversions/purchaseRate", "get"),
            ("/2/conversions/revenue", "get"),
            ("/2/hits", "get"),
            ("/2/filters", "get"),
            ("/2/filters/noResults", "get"),
            ("/2/filters/{attribute}", "get"),
            ("/2/users/count", "get"),
            ("/2/status", "get"),
            ("/2/devices", "get"),
            ("/2/countries", "get"),
            ("/2/geo", "get"),
            ("/2/geo/{country}", "get"),
            ("/2/geo/{country}/regions", "get"),
            ("/2/overview", "get"),
        ];

        for (path, method) in &analytics_paths {
            assert_path_exists(&doc, path);
            assert_path_method(&doc, path, method);
        }
    }

    /// Stage 7: Verify analytics mutation endpoints appear in the generated spec.
    #[test]
    fn analytics_mutation_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/2/analytics/seed");
        assert_path_method(&doc, "/2/analytics/seed", "post");
        assert_path_exists(&doc, "/2/analytics/clear");
        assert_path_method(&doc, "/2/analytics/clear", "delete");
        assert_path_exists(&doc, "/2/analytics/flush");
        assert_path_method(&doc, "/2/analytics/flush", "post");
        assert_path_exists(&doc, "/2/analytics/cleanup");
        assert_path_method(&doc, "/2/analytics/cleanup", "post");
    }

    /// Stage 7: Verify query suggestions endpoints appear in the generated spec.
    #[test]
    fn query_suggestions_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/configs");
        assert_path_method(&doc, "/1/configs", "get");
        assert_path_method(&doc, "/1/configs", "post");

        assert_path_exists(&doc, "/1/configs/{indexName}");
        assert_path_method(&doc, "/1/configs/{indexName}", "get");
        assert_path_method(&doc, "/1/configs/{indexName}", "put");
        assert_path_method(&doc, "/1/configs/{indexName}", "delete");

        assert_path_exists(&doc, "/1/configs/{indexName}/status");
        assert_path_method(&doc, "/1/configs/{indexName}/status", "get");

        assert_path_exists(&doc, "/1/configs/{indexName}/build");
        assert_path_method(&doc, "/1/configs/{indexName}/build", "post");

        assert_path_exists(&doc, "/1/logs/{indexName}");
        assert_path_method(&doc, "/1/logs/{indexName}", "get");
    }

    /// Stage 7: Verify dictionary endpoints appear in the generated spec.
    #[test]
    fn dictionaries_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/dictionaries/{dictionaryName}/batch");
        assert_path_method(&doc, "/1/dictionaries/{dictionaryName}/batch", "post");

        assert_path_exists(&doc, "/1/dictionaries/{dictionaryName}/search");
        assert_path_method(&doc, "/1/dictionaries/{dictionaryName}/search", "post");

        assert_path_exists(&doc, "/1/dictionaries/{_wildcard}/settings");
        assert_path_method(&doc, "/1/dictionaries/{_wildcard}/settings", "get");
        assert_path_method(&doc, "/1/dictionaries/{_wildcard}/settings", "put");

        assert_path_exists(&doc, "/1/dictionaries/{_wildcard}/languages");
        assert_path_method(&doc, "/1/dictionaries/{_wildcard}/languages", "get");
    }

    /// Stage 7: Ensure dictionaryName path params stay constrained to DictionaryName enum.
    #[test]
    fn dictionaries_dictionary_name_param_uses_enum_schema() {
        let doc = openapi_json();

        let param_schema_ref = doc
            .pointer(
                "/paths/~11~1dictionaries~1{dictionaryName}~1batch/post/parameters/0/schema/$ref",
            )
            .expect("dictionaryName path parameter schema ref should exist");

        assert_eq!(
            param_schema_ref, "#/components/schemas/DictionaryName",
            "dictionaryName must reference DictionaryName enum schema instead of free-form string"
        );
    }

    /// Stage 7: Ensure typed request bodies are wired to shared component schemas.
    #[test]
    fn stage7_typed_request_bodies_use_component_schemas() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer(
                "/paths/~11~1configs/post/requestBody/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/QsConfig")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1configs~1{indexName}/put/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/QsConfig")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{dictionaryName}~1batch/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/BatchDictionaryRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{dictionaryName}~1search/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/DictionarySearchRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{_wildcard}~1settings/put/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/DictionarySettings")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1events/post/requestBody/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/InsightsRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1migrate-from-algolia/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/MigrateFromAlgoliaRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1algolia-list-indexes/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ListAlgoliaIndexesRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1indexes~1{indexName}~1chat/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ChatRequest")
        );
    }

    /// Stage 7: Ensure typed responses use shared schemas instead of anonymous objects.
    #[test]
    fn stage7_typed_responses_use_component_schemas() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer(
                "/paths/~11~1configs/get/responses/200/content/application~1json/schema/type"
            )
            .and_then(|v| v.as_str()),
            Some("array")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1configs/get/responses/200/content/application~1json/schema/items/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/QsConfig")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1configs~1{indexName}/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/QsConfig")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1configs~1{indexName}~1status/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/BuildStatus")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1logs~1{indexName}/get/responses/200/content/application~1json/schema/type")
                .and_then(|v| v.as_str()),
            Some("array")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1logs~1{indexName}/get/responses/200/content/application~1json/schema/items/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/LogEntry")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{dictionaryName}~1batch/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/MutationResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{dictionaryName}~1search/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/DictionarySearchResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{_wildcard}~1settings/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/DictionarySettings")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1dictionaries~1{_wildcard}~1settings/put/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/MutationResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1migrate-from-algolia/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/MigrateFromAlgoliaResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1algolia-list-indexes/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ListAlgoliaIndexesResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1indexes~1{indexName}~1chat/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ChatResponse")
        );
    }

    /// Stage 7: Verify insights endpoints appear in the generated spec.
    #[test]
    fn insights_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/events");
        assert_path_method(&doc, "/1/events", "post");

        assert_path_exists(&doc, "/1/events/debug");
        assert_path_method(&doc, "/1/events/debug", "get");

        assert_path_exists(&doc, "/1/usertokens/{userToken}");
        assert_path_method(&doc, "/1/usertokens/{userToken}", "delete");
    }

    /// Stage 7: Verify migration endpoints appear in the generated spec.
    #[test]
    fn migration_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/migrate-from-algolia");
        assert_path_method(&doc, "/1/migrate-from-algolia", "post");

        assert_path_exists(&doc, "/1/algolia-list-indexes");
        assert_path_method(&doc, "/1/algolia-list-indexes", "post");
    }

    /// Stage 7: Verify usage endpoints appear in the generated spec.
    #[test]
    fn usage_endpoints_are_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/usage/{statistic}");
        assert_path_method(&doc, "/1/usage/{statistic}", "get");

        assert_path_exists(&doc, "/1/usage/{statistic}/{indexName}");
        assert_path_method(&doc, "/1/usage/{statistic}/{indexName}", "get");
    }

    /// Stage 7: Verify chat endpoint appears in the generated spec.
    #[test]
    fn chat_endpoint_is_documented() {
        let doc = openapi_json();

        assert_path_exists(&doc, "/1/indexes/{indexName}/chat");
        assert_path_method(&doc, "/1/indexes/{indexName}/chat", "post");
    }

    /// Stage 7 bug fix: QS mutation endpoints must use shared QsMutationResponse schema,
    /// not anonymous serde_json::Value.
    #[test]
    fn qs_mutation_responses_use_shared_schema() {
        let doc = openapi_json();

        let mutation_paths = [
            ("/paths/~11~1configs/post/responses/200/content/application~1json/schema/$ref", "create_config"),
            ("/paths/~11~1configs~1{indexName}/put/responses/200/content/application~1json/schema/$ref", "update_config"),
            ("/paths/~11~1configs~1{indexName}/delete/responses/200/content/application~1json/schema/$ref", "delete_config"),
            ("/paths/~11~1configs~1{indexName}~1build/post/responses/200/content/application~1json/schema/$ref", "trigger_build"),
        ];

        for (pointer, label) in &mutation_paths {
            assert_eq!(
                doc.pointer(pointer).and_then(|v| v.as_str()),
                Some("#/components/schemas/QsMutationResponse"),
                "{} 200 response must reference QsMutationResponse component schema",
                label
            );
        }
    }

    /// Stage 7 bug fix: Chat endpoint must document text/event-stream as an additional
    /// 200-response content type alongside application/json.
    #[test]
    fn chat_endpoint_documents_sse_content_type() {
        let doc = openapi_json();

        let chat_200 = doc
            .pointer("/paths/~11~1indexes~1{indexName}~1chat/post/responses/200/content")
            .expect("chat 200 response should have content map");

        assert!(
            chat_200.get("application/json").is_some(),
            "chat 200 must include application/json content type"
        );
        assert!(
            chat_200.get("text/event-stream").is_some(),
            "chat 200 must include text/event-stream content type"
        );
    }

    /// Stage 7 bug fix: personalization + experiment lifecycle/results endpoints must
    /// use shared typed OpenAPI schemas instead of generic serde_json::Value bodies.
    #[test]
    fn personalization_and_experiment_lifecycle_use_typed_schemas() {
        let doc = openapi_json();

        // Personalization
        assert_eq!(
            doc.pointer("/paths/~11~1strategies~1personalization/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/PersonalizationStrategy")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1strategies~1personalization/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/SetStrategyResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1strategies~1personalization/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/PersonalizationStrategy")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1profiles~1personalization~1{userToken}/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/PersonalizationProfile")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1profiles~1{userToken}/delete/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/DeleteProfileResponse")
        );

        // Experiment lifecycle + results
        assert_eq!(
            doc.pointer(
                "/paths/~12~1abtests~1{id}/put/requestBody/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/CreateExperimentRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~12~1abtests~1{id}~1conclude/post/requestBody/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ConcludeExperimentRequest")
        );
        assert_eq!(
            doc.pointer("/paths/~12~1abtests~1{id}~1conclude/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/Experiment")
        );
        assert_eq!(
            doc.pointer("/paths/~12~1abtests~1{id}~1results/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/ResultsResponse")
        );
    }

    /// Stage 7 bug fix: search endpoint responses must use a shared SearchResponse schema
    /// documenting response-side metadata fields (not just request-side vector/hybrid fields).
    #[test]
    fn search_response_schema_documents_metadata_fields() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer("/paths/~11~1indexes~1{indexName}~1query/post/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/SearchResponse")
        );
        assert_eq!(
            doc.pointer("/paths/~11~1indexes~1{indexName}/get/responses/200/content/application~1json/schema/$ref")
                .and_then(|v| v.as_str()),
            Some("#/components/schemas/SearchResponse")
        );

        let props = doc
            .pointer("/components/schemas/SearchResponse/properties")
            .expect("SearchResponse must expose properties");

        for key in [
            "processingTimingsMS",
            "abTestID",
            "abTestVariantID",
            "interleavedTeams",
            "indexUsed",
            "appliedRelevancyStrictness",
        ] {
            assert!(
                props.get(key).is_some(),
                "SearchResponse missing metadata field '{}'",
                key
            );
        }
    }

    /// Stage 4: batch search must publish a typed federation contract for request + response.
    #[test]
    fn batch_search_endpoint_documents_federation_contract_shapes() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer(
                "/paths/~11~1indexes~1{indexName}~1queries/post/requestBody/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/BatchSearchRequest")
        );

        assert_eq!(
            schema_ref(
                &doc,
                "/components/schemas/BatchSearchRequest/properties/federation"
            ),
            Some("#/components/schemas/FederationConfig")
        );

        assert_eq!(
            schema_ref(
                &doc,
                "/components/schemas/SearchRequest/properties/federationOptions"
            ),
            Some("#/components/schemas/FederationOptions")
        );

        let response_schema_pointer =
            "/paths/~11~1indexes~1{indexName}~1queries/post/responses/200/content/application~1json/schema";
        let response_schema_ref = schema_ref(&doc, response_schema_pointer);
        let variant_refs =
            if response_schema_ref == Some("#/components/schemas/BatchSearchResponse") {
                schema_composition_refs(&doc, "/components/schemas/BatchSearchResponse")
            } else {
                schema_composition_refs(&doc, response_schema_pointer)
            };
        assert!(
            !variant_refs.is_empty(),
            "batch_search 200 schema must declare response variants via oneOf/anyOf/allOf"
        );
        assert!(
            variant_refs.contains(&"#/components/schemas/BatchSearchLegacyResponse"),
            "batch_search oneOf must include legacy results[] response"
        );
        assert!(
            variant_refs.contains(&"#/components/schemas/FederatedResponse"),
            "batch_search oneOf must include federated hits response"
        );

        assert_eq!(
            doc.pointer("/components/schemas/BatchSearchLegacyResponse/properties/results/type")
                .and_then(|v| v.as_str()),
            Some("array")
        );
        assert_eq!(
            schema_ref(
                &doc,
                "/components/schemas/FederatedResponse/properties/hits/items"
            ),
            Some("#/components/schemas/FederatedHit")
        );
        assert_eq!(
            schema_ref(
                &doc,
                "/components/schemas/FederatedHit/properties/_federation"
            ),
            Some("#/components/schemas/FederationMeta")
        );

        for key in [
            "hits",
            "estimatedTotalHits",
            "offset",
            "limit",
            "processingTimeMS",
        ] {
            assert!(
                doc.pointer(&format!(
                    "/components/schemas/FederatedResponse/properties/{key}"
                ))
                .is_some(),
                "FederatedResponse must include '{}' in schema",
                key
            );
        }

        let summary = doc
            .pointer("/paths/~11~1indexes~1{indexName}~1queries/post/summary")
            .and_then(|value| value.as_str())
            .expect("batch_search summary must exist");
        assert_ne!(summary, "TODO: Document batch_search.");

        let search_request_description = doc
            .pointer("/components/schemas/SearchRequest/description")
            .and_then(|value| value.as_str())
            .expect("SearchRequest description must exist");
        assert_ne!(search_request_description, "TODO: Document SearchRequest.");
    }

    /// Stage 7 bug fix: analytics/reporting endpoints must reference shared typed
    /// response schemas instead of generic/implicit response bodies.
    #[test]
    fn analytics_endpoints_use_shared_response_schemas() {
        let doc = openapi_json();

        let checks = [
            (
                "/paths/~12~1searches/get/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsTopSearchesResponse",
            ),
            (
                "/paths/~12~1clicks~1clickThroughRate/get/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsRateWithDatesResponse",
            ),
            (
                "/paths/~12~1conversions~1revenue/get/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsRevenueResponse",
            ),
            (
                "/paths/~12~1countries/get/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsCountriesResponse",
            ),
            (
                "/paths/~12~1status/get/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsStatusResponse",
            ),
            (
                "/paths/~12~1analytics~1seed/post/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsSeedResponse",
            ),
            (
                "/paths/~12~1analytics~1cleanup/post/responses/200/content/application~1json/schema/$ref",
                "#/components/schemas/AnalyticsCleanupResponse",
            ),
        ];

        for (pointer, expected) in checks {
            assert_eq!(
                doc.pointer(pointer).and_then(|v| v.as_str()),
                Some(expected),
                "missing shared analytics response schema at {}",
                pointer
            );
        }
    }

    // --- Stage 7 Security Review: regression guards for auth-correct OpenAPI exposure ---

    /// Security review: `/health` must have NO security requirement in the spec
    /// (it is a public endpoint, see `is_public_path` in auth.rs).
    #[test]
    fn health_endpoint_has_no_security_requirement() {
        let doc = openapi_json();
        let health_get = doc
            .pointer("/paths/~1health/get")
            .expect("/health GET should exist");

        // No "security" key, or empty array, means public
        match health_get.get("security") {
            None => {} // correct — no security declared
            Some(arr) => {
                assert!(
                    arr.as_array().is_none_or(|a| a.is_empty()),
                    "/health must not require auth, found: {:?}",
                    arr
                );
            }
        }
    }

    /// Security review: no `/internal/*` paths should appear in the public OpenAPI spec.
    /// Internal endpoints are intentionally excluded from `ApiDoc` to avoid accidental exposure.
    #[test]
    fn no_internal_paths_in_openapi_spec() {
        let doc = openapi_json();
        let paths = doc
            .get("paths")
            .and_then(|p| p.as_object())
            .expect("spec must have paths");

        let internal_paths: Vec<&String> = paths
            .keys()
            .filter(|k| k.starts_with("/internal/"))
            .collect();

        assert!(
            internal_paths.is_empty(),
            "internal paths must not appear in public spec, found: {:?}",
            internal_paths
        );
    }

    /// Security review: all documented customer-facing endpoints (everything except `/health`)
    /// must declare a security requirement referencing the `api_key` scheme.
    #[test]
    fn all_non_health_endpoints_require_api_key_security() {
        let doc = openapi_json();
        let paths = doc
            .get("paths")
            .and_then(|p| p.as_object())
            .expect("spec must have paths");

        let mut missing_security = Vec::new();

        for (path_key, path_item) in paths {
            if path_key == "/health" {
                continue; // health is intentionally public
            }
            let methods = path_item.as_object().unwrap();
            for (method, operation) in methods {
                // Skip non-operation keys (e.g. "parameters")
                if !["get", "post", "put", "delete", "patch", "head", "options"]
                    .contains(&method.as_str())
                {
                    continue;
                }
                let has_api_key = operation
                    .get("security")
                    .and_then(|s| s.as_array())
                    .map(|arr| {
                        arr.iter().any(|item| {
                            item.as_object()
                                .map(|obj| obj.contains_key("api_key"))
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);

                if !has_api_key {
                    missing_security.push(format!("{} {}", method.to_uppercase(), path_key));
                }
            }
        }

        assert!(
            missing_security.is_empty(),
            "endpoints missing api_key security requirement: {:?}",
            missing_security
        );
    }

    /// Security review: the `api_key` security scheme must be defined in the spec
    /// as an API key in the `x-algolia-api-key` header.
    #[test]
    fn api_key_security_scheme_is_defined() {
        let doc = openapi_json();

        let scheme_type = doc
            .pointer("/components/securitySchemes/api_key/type")
            .and_then(|v| v.as_str());
        assert_eq!(
            scheme_type,
            Some("apiKey"),
            "api_key scheme must be type apiKey"
        );

        let scheme_name = doc
            .pointer("/components/securitySchemes/api_key/name")
            .and_then(|v| v.as_str());
        assert_eq!(
            scheme_name,
            Some("x-algolia-api-key"),
            "api_key scheme must use x-algolia-api-key header"
        );

        let scheme_in = doc
            .pointer("/components/securitySchemes/api_key/in")
            .and_then(|v| v.as_str());
        assert_eq!(
            scheme_in,
            Some("header"),
            "api_key scheme must be in header"
        );
    }

    /// Verify that security-sources CRUD endpoints appear in the generated OpenAPI spec with correct request/response schemas.
    #[test]
    fn security_sources_endpoints_are_documented() {
        let doc = openapi_json();

        assert_eq!(
            doc.pointer(
                "/paths/~11~1security~1sources/get/responses/200/content/application~1json/schema/type"
            )
            .and_then(|v| v.as_str()),
            Some("array")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1security~1sources/put/requestBody/content/application~1json/schema/type"
            )
            .and_then(|v| v.as_str()),
            Some("array")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1security~1sources~1append/post/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/SourceMutationTimestampResponse")
        );
        assert_eq!(
            doc.pointer(
                "/paths/~11~1security~1sources~1{source}/delete/responses/200/content/application~1json/schema/$ref"
            )
            .and_then(|v| v.as_str()),
            Some("#/components/schemas/SourceMutationTimestampResponse")
        );
    }
}
