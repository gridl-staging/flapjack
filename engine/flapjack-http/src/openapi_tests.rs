use super::*;
use crate::openapi_test_helpers::{
    assert_high_risk_mutation_contracts, schema_composition_refs, schema_ref,
};

fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).unwrap()
}

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
        doc.pointer("/paths/~11~1keys/post/responses/200/content/application~1json/schema/$ref")
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
        doc.pointer("/paths/~11~1indexes/get/responses/200/content/application~1json/schema/$ref")
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

#[test]
fn high_risk_mutation_openapi_contracts_match_shared_matrix() {
    let doc = openapi_json();
    assert_high_risk_mutation_contracts(&doc);
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
        .pointer("/paths/~11~1dictionaries~1{dictionaryName}~1batch/post/parameters/0/schema/$ref")
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
        doc.pointer("/paths/~11~1configs/post/requestBody/content/application~1json/schema/$ref")
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
        doc.pointer("/paths/~11~1events/post/requestBody/content/application~1json/schema/$ref")
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
        doc.pointer("/paths/~11~1configs/get/responses/200/content/application~1json/schema/type")
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
        doc.pointer(
            "/paths/~11~1logs~1{indexName}/get/responses/200/content/application~1json/schema/type"
        )
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

#[path = "openapi_tests_endpoints.rs"]
mod endpoints;
