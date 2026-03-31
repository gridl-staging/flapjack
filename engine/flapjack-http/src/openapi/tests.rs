//! Stub summary for tests.rs.
use super::*;

fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).unwrap()
}

/// TODO: Document schema_contains_type.
fn schema_contains_type(schema: &serde_json::Value, expected_type: &str) -> bool {
    match schema {
        serde_json::Value::Object(map) => {
            if map
                .get("type")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value == expected_type)
            {
                return true;
            }

            if map
                .get("type")
                .and_then(|value| value.as_array())
                .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected_type)))
            {
                return true;
            }

            map.values()
                .any(|value| schema_contains_type(value, expected_type))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| schema_contains_type(value, expected_type)),
        _ => false,
    }
}

/// TODO: Document schema_contains_ref.
fn schema_contains_ref(schema: &serde_json::Value, expected_ref: &str) -> bool {
    match schema {
        serde_json::Value::Object(map) => {
            if map
                .get("$ref")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value == expected_ref)
            {
                return true;
            }

            map.values()
                .any(|value| schema_contains_ref(value, expected_ref))
        }
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| schema_contains_ref(value, expected_ref)),
        _ => false,
    }
}

fn assert_schema_ref(doc: &serde_json::Value, pointer: &str, expected_ref: &str) {
    assert_eq!(
        doc.pointer(pointer).and_then(|value| value.as_str()),
        Some(expected_ref),
        "expected {pointer} to reference {expected_ref}"
    );
}

fn parameters_at<'a>(doc: &'a serde_json::Value, pointer: &str) -> &'a [serde_json::Value] {
    doc.pointer(pointer)
        .and_then(|value| value.as_array())
        .map(Vec::as_slice)
        .expect("parameters should exist")
}

fn named_parameter<'a>(parameters: &'a [serde_json::Value], name: &str) -> &'a serde_json::Value {
    parameters
        .iter()
        .find(|parameter| parameter.get("name").and_then(|value| value.as_str()) == Some(name))
        .unwrap_or_else(|| panic!("expected parameter {name}"))
}

fn assert_parameter_in(parameters: &[serde_json::Value], name: &str, expected_in: &str) {
    let parameter = named_parameter(parameters, name);
    assert_eq!(
        parameter.get("in").and_then(|value| value.as_str()),
        Some(expected_in),
        "expected {name} to be a {expected_in} parameter"
    );
}

/// TODO: Document assert_required_string_path_parameter.
fn assert_required_string_path_parameter(parameters: &[serde_json::Value], name: &str) {
    let parameter = named_parameter(parameters, name);
    assert_eq!(
        parameter.get("in").and_then(|value| value.as_str()),
        Some("path")
    );
    assert_eq!(
        parameter
            .pointer("/schema/type")
            .and_then(|value| value.as_str()),
        Some("string")
    );
    assert_eq!(
        parameter.get("required").and_then(|value| value.as_bool()),
        Some(true)
    );
}

fn assert_experiment_operation_tag(doc: &serde_json::Value, pointer: &str) {
    assert_eq!(
        doc.pointer(pointer).and_then(|value| value.as_str()),
        Some("experiments"),
        "operation at {pointer} should be tagged as experiments"
    );
}

/// TODO: Document key_endpoints_use_concrete_schema_components.
#[test]
fn key_endpoints_use_concrete_schema_components() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~11~1keys/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/ListKeysResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys~1{key}/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/KeyApiResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/CreateKeyRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/CreateKeyResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys~1{key}/put/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/UpdateKeyResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys~1{key}/delete/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/DeleteKeyResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys~1{key}~1restore/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/RestoreKeyResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1keys~1generateSecuredApiKey/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/GenerateSecuredKeyResponse",
    );
}

/// Verify that the list-indices endpoint uses a concrete `ListIndicesResponse` schema with properly typed `ListIndexItem` fields.
#[test]
fn list_indices_uses_concrete_response_schema() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~11~1indexes/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/ListIndicesResponse",
    );
    assert_schema_ref(
        &doc,
        "/components/schemas/ListIndicesResponse/properties/items/items/$ref",
        "#/components/schemas/ListIndexItem",
    );
    assert_eq!(
        doc.pointer("/components/schemas/ListIndexItem/properties/lastBuildTimeS/type")
            .and_then(|value| value.as_str()),
        Some("integer")
    );
}

/// Verify that the recommend endpoint uses concrete `$ref` schemas for request body and response.
#[test]
fn recommend_endpoint_uses_concrete_schemas() {
    let doc = openapi_json();

    assert!(
        doc.pointer("/paths/~11~1indexes~1*~1recommendations/post/parameters")
            .is_none(),
        "wildcard recommend endpoint should not document a path parameter"
    );

    assert_schema_ref(
        &doc,
        "/paths/~11~1indexes~1*~1recommendations/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/RecommendBatchRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1indexes~1*~1recommendations/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/RecommendBatchResponse",
    );
}

#[test]
fn settings_endpoint_documents_put_for_sdk_compat() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~11~1indexes~1{indexName}~1settings/put/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/SetSettingsRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1indexes~1{indexName}~1settings/put/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/SetSettingsResponse",
    );
}

/// Verify that all four personalization endpoints use concrete `$ref` schemas.
#[test]
fn personalization_endpoints_use_concrete_schemas() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~11~1strategies~1personalization/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/PersonalizationStrategy",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1strategies~1personalization/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/SetStrategyResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1strategies~1personalization/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/PersonalizationStrategy",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1profiles~1personalization~1{userToken}/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/PersonalizationProfile",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1profiles~1{userToken}/delete/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/DeleteProfileResponse",
    );
}

/// Verify that serde rename attributes produce correct camelCase property names in recommend/personalization schemas.
#[test]
fn recommend_and_personalization_schema_property_names() {
    let doc = openapi_json();

    let req_props = doc
        .pointer("/components/schemas/RecommendRequest/properties")
        .expect("RecommendRequest properties should exist");
    assert!(req_props.get("indexName").is_some(), "should be camelCase indexName");
    assert!(req_props.get("objectID").is_some(), "should use explicit rename objectID");
    assert!(req_props.get("maxRecommendations").is_some(), "should be camelCase maxRecommendations");
    assert!(req_props.get("queryParameters").is_some(), "should be camelCase queryParameters");

    let result_props = doc
        .pointer("/components/schemas/RecommendResult/properties")
        .expect("RecommendResult properties should exist");
    assert!(result_props.get("processingTimeMS").is_some(), "should use explicit rename processingTimeMS");

    let strategy_props = doc
        .pointer("/components/schemas/PersonalizationStrategy/properties")
        .expect("PersonalizationStrategy properties should exist");
    assert!(strategy_props.get("eventsScoring").is_some(), "should be camelCase eventsScoring");
    assert!(strategy_props.get("facetsScoring").is_some(), "should be camelCase facetsScoring");
    assert!(strategy_props.get("personalizationImpact").is_some(), "should be camelCase personalizationImpact");

    let profile_props = doc
        .pointer("/components/schemas/PersonalizationProfile/properties")
        .expect("PersonalizationProfile properties should exist");
    assert!(profile_props.get("userToken").is_some(), "should be camelCase userToken");
    assert!(profile_props.get("lastEventAt").is_some(), "should be camelCase lastEventAt");

    let query_parameters = req_props
        .get("queryParameters")
        .expect("queryParameters schema should exist");
    assert!(
        schema_contains_type(query_parameters, "object"),
        "queryParameters should be documented as an object-valued schema"
    );

    let fallback_parameters = req_props
        .get("fallbackParameters")
        .expect("fallbackParameters schema should exist");
    assert!(
        schema_contains_type(fallback_parameters, "object"),
        "fallbackParameters should be documented as an object-valued schema"
    );

    let hits_items = doc
        .pointer("/components/schemas/RecommendResult/properties/hits/items")
        .expect("RecommendResult hits items should exist");
    assert!(
        schema_contains_type(hits_items, "object"),
        "hits items should be documented as object-valued entries"
    );

    let scores_schema = doc
        .pointer("/components/schemas/PersonalizationProfile/properties/scores")
        .expect("PersonalizationProfile scores schema should exist");
    let facet_scores = scores_schema
        .get("additionalProperties")
        .expect("scores should use additionalProperties for facet names");
    let score_value = facet_scores
        .get("additionalProperties")
        .expect("scores should use nested additionalProperties for facet values");
    assert_eq!(
        score_value.get("type").and_then(|value| value.as_str()),
        Some("integer")
    );
}

/// Verify that security-sources CRUD endpoints appear in the generated OpenAPI spec with correct request/response schemas.
#[test]
fn security_sources_endpoints_are_documented() {
    let doc = openapi_json();

    assert_eq!(
        doc.pointer("/paths/~11~1security~1sources/get/responses/200/content/application~1json/schema/type")
            .and_then(|value| value.as_str()),
        Some("array")
    );
    assert_eq!(
        doc.pointer("/paths/~11~1security~1sources/put/requestBody/content/application~1json/schema/type")
            .and_then(|value| value.as_str()),
        Some("array")
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1security~1sources~1append/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/SourceMutationTimestampResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~11~1security~1sources~1{source}/delete/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/SourceMutationTimestampResponse",
    );
}

/// TODO: Document experiment_crud_lifecycle_endpoints_use_concrete_schemas.
#[test]
fn experiment_crud_lifecycle_endpoints_use_concrete_schemas() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaCreateAbTestRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaCreateAbTestResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaListAbTestsResponse",
    );

    let list_parameters = parameters_at(&doc, "/paths/~12~1abtests/get/parameters");
    for name in ["offset", "limit", "indexPrefix", "indexSuffix"] {
        assert_parameter_in(list_parameters, name, "query");
    }

    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaAbTest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}/put/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/CreateExperimentRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}/put/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaAbTest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}/delete/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaAbTestActionResponse",
    );

    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}~1start/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaAbTestActionResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}~1stop/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaAbTestActionResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}~1conclude/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/ConcludeExperimentRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}~1conclude/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/Experiment",
    );

    for pointer in [
        "/paths/~12~1abtests~1{id}/get/parameters",
        "/paths/~12~1abtests~1{id}/put/parameters",
        "/paths/~12~1abtests~1{id}/delete/parameters",
        "/paths/~12~1abtests~1{id}~1start/post/parameters",
        "/paths/~12~1abtests~1{id}~1stop/post/parameters",
        "/paths/~12~1abtests~1{id}~1conclude/post/parameters",
    ] {
        assert_required_string_path_parameter(parameters_at(&doc, pointer), "id");
    }
}

/// Verify estimate/results experiment endpoints and nested result schemas stay concrete.
#[test]
fn experiment_estimate_and_results_endpoints_use_concrete_schemas() {
    let doc = openapi_json();

    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1estimate/post/requestBody/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaEstimateRequest",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1estimate/post/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/AlgoliaEstimateResponse",
    );
    assert_schema_ref(
        &doc,
        "/paths/~12~1abtests~1{id}~1results/get/responses/200/content/application~1json/schema/$ref",
        "#/components/schemas/ResultsResponse",
    );

    for (pointer, expected_ref, description) in [
        (
            "/components/schemas/ResultsResponse/properties/gate",
            "#/components/schemas/GateResponse",
            "gate",
        ),
        (
            "/components/schemas/ResultsResponse/properties/control",
            "#/components/schemas/ArmResponse",
            "control",
        ),
        (
            "/components/schemas/ResultsResponse/properties/variant",
            "#/components/schemas/ArmResponse",
            "variant",
        ),
        (
            "/components/schemas/ResultsResponse/properties/interleaving",
            "#/components/schemas/InterleavingResponse",
            "interleaving",
        ),
    ] {
        let schema = doc.pointer(pointer).expect("results property should exist");
        assert!(
            schema_contains_ref(schema, expected_ref),
            "{description} should reference {expected_ref}"
        );
    }

    for (pointer, description) in [
        (
            "/components/schemas/QueryOverrides/properties/typoTolerance",
            "queryOverrides.typoTolerance",
        ),
        (
            "/components/schemas/AlgoliaVariant/properties/customSearchParameters",
            "customSearchParameters",
        ),
        (
            "/components/schemas/AlgoliaVariant/properties/currencies",
            "currencies",
        ),
        (
            "/components/schemas/AlgoliaAbTest/properties/revenueSignificance",
            "revenueSignificance",
        ),
    ] {
        let schema = doc.pointer(pointer).expect("dynamic object field should exist");
        assert!(
            schema_contains_type(schema, "object"),
            "{description} should be documented as object-valued"
        );
    }
}

/// Verify serde-renamed properties and enum shapes on experiment schemas match their expected wire names.
#[test]
fn experiment_schema_renamed_properties_and_enum_shapes() {
    let doc = openapi_json();

    let ab_test_props = doc
        .pointer("/components/schemas/AlgoliaAbTest/properties")
        .expect("AlgoliaAbTest properties should exist");
    assert!(ab_test_props.get("abTestID").is_some(), "should use explicit rename abTestID");
    assert!(ab_test_props.get("createdAt").is_some(), "should be camelCase createdAt");

    let create_resp_props = doc
        .pointer("/components/schemas/AlgoliaCreateAbTestResponse/properties")
        .expect("AlgoliaCreateAbTestResponse properties should exist");
    assert!(create_resp_props.get("abTestID").is_some(), "should use explicit rename abTestID");
    assert!(create_resp_props.get("taskID").is_some(), "should use explicit rename taskID");

    let action_resp_props = doc
        .pointer("/components/schemas/AlgoliaAbTestActionResponse/properties")
        .expect("AlgoliaAbTestActionResponse properties should exist");
    assert!(action_resp_props.get("abTestID").is_some(), "action response should use abTestID");
    assert!(action_resp_props.get("taskID").is_some(), "action response should use taskID");

    let variant_props = doc
        .pointer("/components/schemas/AlgoliaVariant/properties")
        .expect("AlgoliaVariant properties should exist");
    assert!(variant_props.get("trafficPercentage").is_some(), "should be camelCase trafficPercentage");
    assert!(variant_props.get("customSearchParameters").is_some(), "should be camelCase customSearchParameters");

    let config_props = doc
        .pointer("/components/schemas/AlgoliaConfiguration/properties")
        .expect("AlgoliaConfiguration properties should exist");
    assert!(config_props.get("minimumDetectableEffect").is_some(), "should be camelCase minimumDetectableEffect");

    let results_props = doc
        .pointer("/components/schemas/ResultsResponse/properties")
        .expect("ResultsResponse properties should exist");
    assert!(results_props.get("experimentID").is_some(), "should use explicit rename experimentID");
    assert!(results_props.get("trafficSplit").is_some(), "should be camelCase trafficSplit");
    assert!(results_props.get("cupedApplied").is_some(), "should be camelCase cupedApplied");
    assert!(results_props.get("guardRailAlerts").is_some(), "should be camelCase guardRailAlerts");

    let qo_props = doc
        .pointer("/components/schemas/QueryOverrides/properties")
        .expect("QueryOverrides properties should exist");
    assert!(qo_props.get("typoTolerance").is_some(), "should be camelCase typoTolerance");
    assert!(qo_props.get("enableSynonyms").is_some(), "should be camelCase enableSynonyms");
    assert!(qo_props.get("customRanking").is_some(), "should be camelCase customRanking");
    assert!(qo_props.get("attributeWeights").is_some(), "should be camelCase attributeWeights");

    let primary_metric = doc
        .pointer("/components/schemas/PrimaryMetric")
        .expect("PrimaryMetric schema should exist");
    let enum_values = primary_metric
        .get("enum")
        .and_then(|value| value.as_array())
        .expect("PrimaryMetric should have enum values");
    let enum_strings: Vec<&str> = enum_values.iter().filter_map(|value| value.as_str()).collect();
    for value in [
        "ctr",
        "conversionRate",
        "revenuePerSearch",
        "zeroResultRate",
        "abandonmentRate",
    ] {
        assert!(
            enum_strings.contains(&value),
            "PrimaryMetric should contain {value}"
        );
    }

    let exp_status = doc
        .pointer("/components/schemas/ExperimentStatus")
        .expect("ExperimentStatus schema should exist");
    let status_values = exp_status
        .get("enum")
        .and_then(|value| value.as_array())
        .expect("ExperimentStatus should have enum values");
    let status_strings: Vec<&str> = status_values.iter().filter_map(|value| value.as_str()).collect();
    for value in ["draft", "running", "stopped", "concluded"] {
        assert!(
            status_strings.contains(&value),
            "ExperimentStatus should contain {value}"
        );
    }
}

/// Verify all ten experiment operations are present under the `experiments` tag and no duplicate internal list schemas are exposed.
#[test]
fn experiment_surface_is_fully_tagged_without_duplicate_list_components() {
    let doc = openapi_json();

    for pointer in [
        "/paths/~12~1abtests/post/tags/0",
        "/paths/~12~1abtests/get/tags/0",
        "/paths/~12~1abtests~1estimate/post/tags/0",
        "/paths/~12~1abtests~1{id}/get/tags/0",
        "/paths/~12~1abtests~1{id}/put/tags/0",
        "/paths/~12~1abtests~1{id}/delete/tags/0",
        "/paths/~12~1abtests~1{id}~1start/post/tags/0",
        "/paths/~12~1abtests~1{id}~1stop/post/tags/0",
        "/paths/~12~1abtests~1{id}~1conclude/post/tags/0",
        "/paths/~12~1abtests~1{id}~1results/get/tags/0",
    ] {
        assert_experiment_operation_tag(&doc, pointer);
    }

    assert!(
        doc.pointer("/components/schemas/ListExperimentsQuery")
            .is_none(),
        "internal ListExperimentsQuery should not be exposed as a public component schema"
    );
    assert!(
        doc.pointer("/components/schemas/ListExperimentsResponse")
            .is_none(),
        "internal ListExperimentsResponse should not be exposed as a public component schema"
    );
}
