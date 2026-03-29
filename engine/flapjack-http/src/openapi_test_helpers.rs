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
