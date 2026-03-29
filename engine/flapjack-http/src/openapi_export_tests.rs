use std::path::PathBuf;

use crate::openapi_test_helpers::{schema_composition_refs, schema_ref};
use tempfile::TempDir;
use utoipa::OpenApi;

#[test]
fn export_writes_apidoc_json_to_target_file() {
    let temp_dir = TempDir::new().expect("temp dir must be created");
    let output_path = temp_dir.path().join("openapi.json");

    crate::openapi_export::write_openapi_json(&output_path)
        .expect("export should write openapi.json");

    let written = std::fs::read_to_string(&output_path).expect("openapi.json must be readable");
    let expected = serde_json::to_string_pretty(&crate::openapi::ApiDoc::openapi())
        .expect("ApiDoc must serialize");
    assert_eq!(written, expected, "export must be sourced from ApiDoc");
}

#[test]
fn export_output_is_deterministic_across_runs() {
    let temp_dir = TempDir::new().expect("temp dir must be created");
    let output_path = temp_dir.path().join("openapi.json");

    crate::openapi_export::write_openapi_json(&output_path).expect("first export should succeed");
    let first = std::fs::read(&output_path).expect("first export output must exist");

    crate::openapi_export::write_openapi_json(&output_path).expect("second export should succeed");
    let second = std::fs::read(&output_path).expect("second export output must exist");

    assert_eq!(first, second, "export bytes must be deterministic");
}

#[test]
fn export_creates_missing_parent_directories() {
    let temp_dir = TempDir::new().expect("temp dir must be created");
    let output_path = temp_dir.path().join("nested/docs/openapi.json");

    crate::openapi_export::write_openapi_json(&output_path)
        .expect("export should create parent directories");

    assert!(
        output_path.is_file(),
        "export should create missing parent directories before writing"
    );
}

#[test]
fn default_output_path_targets_engine_docs2_openapi_json() {
    let expected = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../docs2")
        .join("openapi.json");

    assert_eq!(
        crate::openapi_export::default_docs2_output_path(),
        expected,
        "default exporter target must be engine/docs2/openapi.json"
    );
}

#[test]
fn committed_docs2_openapi_matches_export_output() {
    let committed_path = crate::openapi_export::default_docs2_output_path();
    let committed = std::fs::read_to_string(&committed_path)
        .expect("committed engine/docs2/openapi.json must be readable");
    let expected = serde_json::to_string_pretty(&crate::openapi::ApiDoc::openapi())
        .expect("ApiDoc must serialize");

    assert_eq!(
        committed, expected,
        "committed engine/docs2/openapi.json must be regenerated from current ApiDoc when routes or schemas change"
    );
}
#[test]
fn export_output_covers_recommend_personalization_and_experiments_routes() {
    let temp_dir = TempDir::new().expect("temp dir must be created");
    let output_path = temp_dir.path().join("openapi.json");
    crate::openapi_export::write_openapi_json(&output_path).expect("export should succeed");

    let exported = std::fs::read_to_string(&output_path).expect("export output must be readable");
    let doc: serde_json::Value =
        serde_json::from_str(&exported).expect("exported openapi must be valid json");

    for pointer in [
        "/paths/~11~1indexes~1*~1recommendations/post",
        "/paths/~11~1strategies~1personalization/get",
        "/paths/~11~1strategies~1personalization/post",
        "/paths/~12~1abtests/get",
        "/paths/~12~1abtests/post",
    ] {
        assert!(
            doc.pointer(pointer).is_some(),
            "expected exported openapi to include operation at {pointer}"
        );
    }
}
#[test]
fn export_output_includes_federated_batch_contract_components() {
    let temp_dir = TempDir::new().expect("temp dir must be created");
    let output_path = temp_dir.path().join("openapi.json");
    crate::openapi_export::write_openapi_json(&output_path).expect("export should succeed");

    let exported = std::fs::read_to_string(&output_path).expect("export output must be readable");
    let doc: serde_json::Value =
        serde_json::from_str(&exported).expect("exported openapi must be valid json");

    assert_eq!(
        doc.pointer("/paths/~11~1indexes~1{indexName}~1queries/post/requestBody/content/application~1json/schema/$ref")
            .and_then(|v| v.as_str()),
        Some("#/components/schemas/BatchSearchRequest"),
        "batch search request must use shared BatchSearchRequest schema"
    );

    let merge_facets_schema = doc
        .pointer("/components/schemas/FederationConfig/properties/mergeFacets")
        .expect("federation.mergeFacets field must be documented in request schema");
    let merge_facets_required = doc
        .pointer("/components/schemas/FederationConfig/required")
        .and_then(|v| v.as_array())
        .map(|required| {
            required
                .iter()
                .filter_map(|value| value.as_str())
                .any(|name| name == "mergeFacets")
        })
        .unwrap_or(false);
    assert!(
        merge_facets_schema.is_object() && !merge_facets_required,
        "federation.mergeFacets must be present and optional in request schema"
    );

    assert_eq!(
        schema_ref(
            &doc,
            "/components/schemas/SearchRequest/properties/federationOptions"
        ),
        Some("#/components/schemas/FederationOptions"),
        "per-query federationOptions must reference FederationOptions schema"
    );

    assert!(
        doc.pointer("/components/schemas/FederationMeta/properties/indexName")
            .is_some(),
        "_federation metadata schema must be exported"
    );
    assert_eq!(
        schema_ref(
            &doc,
            "/components/schemas/FederatedResponse/properties/hits/items"
        ),
        Some("#/components/schemas/FederatedHit"),
        "federated hits must use an explicit hit schema"
    );
    assert_eq!(
        schema_ref(
            &doc,
            "/components/schemas/FederatedHit/properties/_federation"
        ),
        Some("#/components/schemas/FederationMeta"),
        "federated hits must expose _federation metadata"
    );

    let response_schema_pointer =
        "/paths/~11~1indexes~1{indexName}~1queries/post/responses/200/content/application~1json/schema";
    let response_schema_ref = schema_ref(&doc, response_schema_pointer);
    let refs = if response_schema_ref == Some("#/components/schemas/BatchSearchResponse") {
        schema_composition_refs(&doc, "/components/schemas/BatchSearchResponse")
    } else {
        schema_composition_refs(&doc, response_schema_pointer)
    };
    assert!(
        !refs.is_empty(),
        "batch_search response must declare typed legacy/federated variants"
    );
    assert!(
        refs.contains(&"#/components/schemas/BatchSearchLegacyResponse"),
        "batch_search response must include legacy results[] schema"
    );
    assert!(
        refs.contains(&"#/components/schemas/FederatedResponse"),
        "batch_search response must include federated hits schema"
    );

    let summary = doc
        .pointer("/paths/~11~1indexes~1{indexName}~1queries/post/summary")
        .and_then(|value| value.as_str())
        .expect("batch_search summary must exist");
    assert_ne!(
        summary, "TODO: Document batch_search.",
        "batch_search summary should not ship as a placeholder"
    );

    let search_request_description = doc
        .pointer("/components/schemas/SearchRequest/description")
        .and_then(|value| value.as_str())
        .expect("SearchRequest description must exist");
    assert_ne!(
        search_request_description, "TODO: Document SearchRequest.",
        "SearchRequest description should not ship as a placeholder"
    );
}
