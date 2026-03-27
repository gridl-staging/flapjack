use super::*;

/// Verify that serde-optional settings fields are persisted through save/load.
#[test]
fn test_settings_roundtrip_preserves_optional_serde_fields() {
    let mut custom_normalization = HashMap::new();
    let mut default_map = HashMap::new();
    default_map.insert("ß".to_string(), "ss".to_string());
    custom_normalization.insert("default".to_string(), default_map);

    let original = IndexSettings {
        unretrievable_attributes: Some(vec!["internal_id".to_string()]),
        separators_to_index: "#+".to_string(),
        disable_typo_tolerance_on_words: Some(vec!["iphonne".to_string()]),
        disable_typo_tolerance_on_attributes: Some(vec!["sku".to_string()]),
        keep_diacritics_on_characters: "ø".to_string(),
        custom_normalization: Some(custom_normalization),
        numeric_attributes_for_filtering: Some(vec!["price".to_string(), "quantity".to_string()]),
        ..Default::default()
    };

    let loaded = roundtrip_settings(&original);

    assert_eq!(
        loaded.unretrievable_attributes, original.unretrievable_attributes,
        "unretrievable_attributes mismatch"
    );
    assert_eq!(
        loaded.separators_to_index, original.separators_to_index,
        "separators_to_index mismatch"
    );
    assert_eq!(
        loaded.disable_typo_tolerance_on_words, original.disable_typo_tolerance_on_words,
        "disable_typo_tolerance_on_words mismatch"
    );
    assert_eq!(
        loaded.disable_typo_tolerance_on_attributes, original.disable_typo_tolerance_on_attributes,
        "disable_typo_tolerance_on_attributes mismatch"
    );
    assert_eq!(
        loaded.keep_diacritics_on_characters, original.keep_diacritics_on_characters,
        "keep_diacritics_on_characters mismatch"
    );
    assert_eq!(
        loaded.custom_normalization, original.custom_normalization,
        "custom_normalization mismatch"
    );
    assert_eq!(
        loaded.numeric_attributes_for_filtering, original.numeric_attributes_for_filtering,
        "numeric_attributes_for_filtering mismatch"
    );
}

/// Verify that `customNormalization` mappings survive a JSON save/load cycle without data loss.
#[test]
fn test_custom_normalization_roundtrip() {
    let mut scripts = HashMap::new();
    let mut default_map = HashMap::new();
    default_map.insert("ğ".to_string(), "g".to_string());
    default_map.insert("ß".to_string(), "ss".to_string());
    scripts.insert("default".to_string(), default_map);

    let original = IndexSettings {
        custom_normalization: Some(scripts),
        ..Default::default()
    };

    let loaded = roundtrip_settings(&original);

    assert_eq!(
        loaded.custom_normalization, original.custom_normalization,
        "customNormalization should roundtrip"
    );
}

#[test]
fn flatten_custom_normalization_lowercases_keys() {
    let mut scripts = HashMap::new();
    let mut default_map = HashMap::new();
    default_map.insert("Q".to_string(), "k".to_string());
    scripts.insert("default".to_string(), default_map);
    let settings = IndexSettings {
        custom_normalization: Some(scripts),
        ..Default::default()
    };

    let flattened = IndexSettings::flatten_custom_normalization(&settings);
    assert_eq!(flattened, vec![('q', "k".to_string())]);
}

#[test]
fn flatten_custom_normalization_lowercases_replacement_values() {
    let mut scripts = HashMap::new();
    let mut default_map = HashMap::new();
    default_map.insert("Q".to_string(), "K".to_string());
    scripts.insert("default".to_string(), default_map);
    let settings = IndexSettings {
        custom_normalization: Some(scripts),
        ..Default::default()
    };

    let flattened = IndexSettings::flatten_custom_normalization(&settings);
    assert_eq!(flattened, vec![('q', "k".to_string())]);
}

// ── numericAttributesForFiltering parity tests ──

#[test]
fn test_numeric_attributes_for_filtering_deserialize() {
    let json = r#"{"numericAttributesForFiltering": ["price", "quantity"]}"#;
    let settings: IndexSettings = serde_json::from_str(json).unwrap();
    assert_eq!(
        settings.numeric_attributes_for_filtering,
        Some(vec!["price".to_string(), "quantity".to_string()])
    );
}

#[test]
fn test_numeric_attributes_for_filtering_legacy_alias() {
    let json = r#"{"numericAttributesToIndex": ["price"]}"#;
    let settings: IndexSettings = serde_json::from_str(json).unwrap();
    assert_eq!(
        settings.numeric_attributes_for_filtering,
        Some(vec!["price".to_string()])
    );
}

#[test]
fn test_numeric_attributes_for_filtering_serializes_canonical_name() {
    let settings = IndexSettings {
        numeric_attributes_for_filtering: Some(vec!["price".to_string()]),
        ..Default::default()
    };
    let json = serde_json::to_string(&settings).unwrap();
    assert!(json.contains("numericAttributesForFiltering"));
    assert!(!json.contains("numericAttributesToIndex"));
}
