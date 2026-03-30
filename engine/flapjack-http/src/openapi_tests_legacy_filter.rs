use crate::openapi::ApiDoc;
use crate::openapi_test_helpers::assert_high_risk_mutation_contracts;
use utoipa::OpenApi;

/// Compatibility guard for the Stage 1 legacy mutation-filter command.
/// Keep this test path stable so historical checklist commands continue to run.
#[test]
fn high_risk_mutation_openapi_contracts_match_shared_matrix() {
    let doc = serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI export should serialize");
    assert_high_risk_mutation_contracts(&doc);
}
