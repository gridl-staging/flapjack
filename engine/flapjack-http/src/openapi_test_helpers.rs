use crate::mutation_parity::HIGH_RISK_MUTATION_PARITY_CASES;

pub(crate) fn schema_ref<'a>(doc: &'a serde_json::Value, schema_pointer: &str) -> Option<&'a str> {
    if let Some(reference) = doc
        .pointer(&format!("{schema_pointer}/$ref"))
        .and_then(|value| value.as_str())
    {
        return Some(reference);
    }

    for composition_key in ["allOf", "anyOf", "oneOf"] {
        if let Some(reference) = doc
            .pointer(&format!("{schema_pointer}/{composition_key}"))
            .and_then(|value| value.as_array())
            .and_then(|variants| {
                variants
                    .iter()
                    .find_map(|variant| variant.get("$ref").and_then(|value| value.as_str()))
            })
        {
            return Some(reference);
        }
    }

    None
}

pub(crate) fn schema_composition_refs<'a>(
    doc: &'a serde_json::Value,
    schema_pointer: &str,
) -> Vec<&'a str> {
    for composition_key in ["oneOf", "anyOf", "allOf"] {
        if let Some(variants) = doc
            .pointer(&format!("{schema_pointer}/{composition_key}"))
            .and_then(|value| value.as_array())
        {
            return variants
                .iter()
                .filter_map(|variant| variant.get("$ref").and_then(|value| value.as_str()))
                .collect();
        }
    }

    Vec::new()
}

/// Shared assertion for the highest-risk mutation endpoints that Stage 1 uses as an OpenAPI guard.
pub(crate) fn assert_high_risk_mutation_contracts(doc: &serde_json::Value) {
    let paths = doc
        .get("paths")
        .and_then(|value| value.as_object())
        .expect("spec must have paths object");

    for case in HIGH_RISK_MUTATION_PARITY_CASES {
        let method = case.method.to_ascii_lowercase();
        let path_item = paths
            .get(case.path)
            .and_then(|value| value.as_object())
            .unwrap_or_else(|| panic!("expected path {} in OpenAPI doc", case.path));
        assert!(
            path_item.contains_key(&method),
            "expected method {} on path {} in OpenAPI doc",
            method,
            case.path
        );

        let response_schema_ref = doc
            .pointer(case.openapi_response_pointer)
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "expected OpenAPI response schema ref for {} {} at {}",
                    case.method, case.path, case.openapi_response_pointer
                )
            });

        let schema_pointer = response_schema_ref.trim_start_matches('#');
        let required_fields = doc
            .pointer(schema_pointer)
            .and_then(|schema| schema.get("required"))
            .and_then(|value| value.as_array())
            .unwrap_or_else(|| {
                panic!("expected required fields at schema pointer {schema_pointer}")
            });

        for field in case.required_fields {
            assert!(
                required_fields
                    .iter()
                    .any(|value| value.as_str() == Some(*field)),
                "expected required field {field} for {} {}",
                case.method,
                case.path
            );
        }
    }
}
