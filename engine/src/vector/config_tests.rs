use super::*;

// ── Config validation tests (3.5) ──

#[test]
fn test_openai_config_requires_api_key() {
    let config = EmbedderConfig {
        source: EmbedderSource::OpenAi,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        VectorError::EmbeddingError(_)
    ));
}

/// Verify that Rest embedder validation fails when missing url, request, or response fields.
#[test]
fn test_rest_config_requires_url_and_templates() {
    // Missing all three
    let config = EmbedderConfig {
        source: EmbedderSource::Rest,
        ..Default::default()
    };
    assert!(config.validate().is_err());

    // Has url but missing request and response
    let config = EmbedderConfig {
        source: EmbedderSource::Rest,
        url: Some("http://example.com".into()),
        ..Default::default()
    };
    assert!(config.validate().is_err());

    // Has url + request but missing response
    let config = EmbedderConfig {
        source: EmbedderSource::Rest,
        url: Some("http://example.com".into()),
        request: Some(serde_json::json!({"input": "{{text}}"})),
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn test_user_provided_requires_dimensions() {
    let config = EmbedderConfig {
        source: EmbedderSource::UserProvided,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

/// Verify that correctly configured embedders pass validation for each source type.
#[test]
fn test_valid_configs_pass_validation() {
    let openai = EmbedderConfig {
        source: EmbedderSource::OpenAi,
        api_key: Some("sk-test".into()),
        ..Default::default()
    };
    assert!(openai.validate().is_ok());

    let rest = EmbedderConfig {
        source: EmbedderSource::Rest,
        url: Some("http://example.com/embed".into()),
        request: Some(serde_json::json!({"input": "{{text}}"})),
        response: Some(serde_json::json!({"embedding": "{{embedding}}"})),
        ..Default::default()
    };
    assert!(rest.validate().is_ok());

    let user_provided = EmbedderConfig {
        source: EmbedderSource::UserProvided,
        dimensions: Some(384),
        ..Default::default()
    };
    assert!(user_provided.validate().is_ok());
}

/// Verify that EmbedderConfig serializes to camelCase JSON and deserializes back without data loss.
#[test]
fn test_config_serde_roundtrip() {
    let config = EmbedderConfig {
        source: EmbedderSource::OpenAi,
        api_key: Some("sk-test".into()),
        model: Some("text-embedding-3-small".into()),
        dimensions: Some(1536),
        ..Default::default()
    };
    let json = serde_json::to_string(&config).unwrap();
    // Verify camelCase serialization
    assert!(json.contains("apiKey"));
    assert!(json.contains("openAi"));
    assert!(!json.contains("api_key"));

    let roundtripped: EmbedderConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(roundtripped.source, EmbedderSource::OpenAi);
    assert_eq!(roundtripped.api_key.as_deref(), Some("sk-test"));
    assert_eq!(
        roundtripped.model.as_deref(),
        Some("text-embedding-3-small")
    );
    assert_eq!(roundtripped.dimensions, Some(1536));
}

// ── Document template tests (3.26) ──

#[test]
fn test_template_field_substitution() {
    let tmpl = DocumentTemplate::new(Some("{{doc.title}} {{doc.body}}".into()), None);
    let doc = serde_json::json!({
        "title": "MacBook Pro",
        "body": "The new MacBook is fast"
    });
    assert_eq!(tmpl.render(&doc), "MacBook Pro The new MacBook is fast");
}

#[test]
fn test_template_missing_field() {
    let tmpl = DocumentTemplate::new(Some("{{doc.title}} by {{doc.author}}".into()), None);
    let doc = serde_json::json!({"title": "Hello"});
    assert_eq!(tmpl.render(&doc), "Hello by ");
}

#[test]
fn test_template_default_all_fields() {
    let tmpl = DocumentTemplate::new(None, None);
    let doc = serde_json::json!({
        "title": "Hello",
        "body": "World",
        "count": 42
    });
    let result = tmpl.render(&doc);
    // Should concatenate string fields, skip non-strings
    assert!(result.contains("Hello"));
    assert!(result.contains("World"));
    assert!(!result.contains("42"));
}

/// Verify that the default template rendering skips _id and objectID metadata fields.
#[test]
fn test_template_default_excludes_id_fields() {
    let tmpl = DocumentTemplate::new(None, None);
    let doc = serde_json::json!({
        "_id": "abc-123-uuid",
        "objectID": "obj456",
        "title": "Hello",
        "body": "World"
    });
    let result = tmpl.render(&doc);
    // _id and objectID should be excluded — they are metadata, not content
    assert!(
        !result.contains("abc-123-uuid"),
        "default template should exclude _id"
    );
    assert!(
        !result.contains("obj456"),
        "default template should exclude objectID"
    );
    assert!(result.contains("Hello"));
    assert!(result.contains("World"));
}

#[test]
fn test_template_max_bytes_truncation() {
    let tmpl = DocumentTemplate::new(None, Some(10));
    let doc = serde_json::json!({
        "body": "This is a long text that should be truncated"
    });
    let result = tmpl.render(&doc);
    assert!(result.len() <= 10);
}

#[test]
fn test_template_nested_field() {
    let tmpl = DocumentTemplate::new(Some("{{doc.meta.author}}".into()), None);
    let doc = serde_json::json!({
        "meta": {"author": "Stuart"}
    });
    assert_eq!(tmpl.render(&doc), "Stuart");
}

#[test]
fn test_template_unclosed_placeholder() {
    let tmpl = DocumentTemplate::new(Some("Hello {{doc.title and more text".into()), None);
    let doc = serde_json::json!({"title": "World"});
    let result = tmpl.render(&doc);
    // Unclosed placeholder should be preserved literally, no duplication
    assert_eq!(result, "Hello {{doc.title and more text");
}

// ── EmbedderConfig::document_template tests (7.4) ──

/// Verify that DocumentTemplate applies the template string and max_bytes from EmbedderConfig.
#[test]
fn test_document_template_from_embedder_config() {
    let config = EmbedderConfig {
        source: EmbedderSource::Rest,
        url: Some("http://example.com/embed".into()),
        request: Some(serde_json::json!({"input": "{{text}}"})),
        response: Some(serde_json::json!({"embedding": "{{embedding}}"})),
        document_template: Some("{{doc.title}} {{doc.body}}".into()),
        document_template_max_bytes: Some(200),
        ..Default::default()
    };
    let tmpl = config.document_template();
    let doc = serde_json::json!({
        "title": "MacBook Pro",
        "body": "The new MacBook is fast"
    });
    assert_eq!(tmpl.render(&doc), "MacBook Pro The new MacBook is fast");
    assert_eq!(tmpl.max_bytes, 200);
}

/// Verify that DocumentTemplate uses default settings (no template, 400 bytes max) when EmbedderConfig omits these fields.
#[test]
fn test_document_template_from_embedder_config_defaults() {
    let config = EmbedderConfig {
        source: EmbedderSource::UserProvided,
        dimensions: Some(384),
        ..Default::default()
    };
    let tmpl = config.document_template();
    // No template set → default behavior (all string fields, 400 bytes max)
    assert!(tmpl.template.is_none());
    assert_eq!(tmpl.max_bytes, 400);
    let doc = serde_json::json!({
        "title": "Hello",
        "body": "World"
    });
    let result = tmpl.render(&doc);
    assert!(result.contains("Hello"));
    assert!(result.contains("World"));
}

#[test]
fn test_template_default_non_string_fields_only() {
    let tmpl = DocumentTemplate::new(None, None);
    let doc = serde_json::json!({
        "count": 42,
        "active": true,
        "price": 9.99
    });
    let result = tmpl.render(&doc);
    assert!(
        result.is_empty(),
        "default template should skip non-string fields, got: {result:?}"
    );
}

/// Verify that the default template rendering excludes vector objects and only concatenates string values.
#[test]
fn test_template_default_excludes_vectors_object() {
    // _vectors as an object should not be rendered (as_str returns None for objects)
    let tmpl = DocumentTemplate::new(None, None);
    let doc = serde_json::json!({
        "title": "Hello",
        "_vectors": { "default": [0.1, 0.2, 0.3] }
    });
    let result = tmpl.render(&doc);
    assert!(result.contains("Hello"));
    assert!(
        !result.contains("0.1"),
        "vectors object should not be rendered as text"
    );
    assert!(
        !result.contains("default"),
        "vectors embedder name should not leak into text"
    );
}

/// Verify that template rendering truncates at UTF-8 character boundaries without breaking multi-byte characters.
#[test]
fn test_template_utf8_truncation_boundary() {
    // Multi-byte UTF-8: each emoji is 4 bytes
    let tmpl = DocumentTemplate::new(None, Some(5));
    let doc = serde_json::json!({
        "text": "\u{1F600}\u{1F601}\u{1F602}"  // 3 emojis = 12 bytes
    });
    let result = tmpl.render(&doc);
    // max_bytes=5, first emoji is 4 bytes, second starts at byte 4 and ends at byte 8
    // So only 1 emoji fits (4 bytes <= 5, but 8 bytes > 5)
    assert!(result.len() <= 5);
    assert_eq!(
        result.chars().count(),
        1,
        "should truncate to 1 emoji at char boundary"
    );
}

// ── EmbedderFingerprint tests (8.13) ──

/// Verify that fingerprints are built correctly from configs and embedders are sorted by name.
#[test]
fn test_fingerprint_from_configs() {
    let configs = vec![
        (
            "beta".to_string(),
            EmbedderConfig {
                source: EmbedderSource::Rest,
                model: Some("model-b".into()),
                dimensions: Some(768),
                ..Default::default()
            },
        ),
        (
            "alpha".to_string(),
            EmbedderConfig {
                source: EmbedderSource::OpenAi,
                model: Some("model-a".into()),
                dimensions: Some(1536),
                ..Default::default()
            },
        ),
    ];
    let fp = EmbedderFingerprint::from_configs(&configs, 1536);
    assert_eq!(fp.version, 1);
    assert_eq!(fp.embedders.len(), 2);
    // Should be sorted by name
    assert_eq!(fp.embedders[0].name, "alpha");
    assert_eq!(fp.embedders[1].name, "beta");
    assert_eq!(fp.embedders[0].source, EmbedderSource::OpenAi);
    assert_eq!(fp.embedders[1].source, EmbedderSource::Rest);
}

#[test]
fn test_fingerprint_matches_same_configs() {
    let configs = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::Rest,
            model: Some("text-embedding-3-small".into()),
            dimensions: Some(1536),
            document_template: Some("{{doc.title}}".into()),
            document_template_max_bytes: Some(400),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs, 1536);
    assert!(fp.matches_configs(&configs));
}

/// Verify that changing an embedder's model name between fingerprints causes mismatch detection.
#[test]
fn test_fingerprint_mismatch_different_model() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("text-embedding-3-small".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("text-embedding-3-large".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    assert!(!fp.matches_configs(&configs_v2));
}

/// Verify that changing an embedder's source type causes fingerprint mismatch detection.
#[test]
fn test_fingerprint_mismatch_different_source() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::Rest,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    assert!(!fp.matches_configs(&configs_v2));
}

/// Verify that a config specifying different dimensions causes fingerprint mismatch.
#[test]
fn test_fingerprint_mismatch_different_dimensions() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(768),
            ..Default::default()
        },
    )];
    assert!(!fp.matches_configs(&configs_v2));
}

/// Verify that a config with dimensions=None skips dimension validation and matches any stored dimensions.
#[test]
fn test_fingerprint_dimensions_none_in_config_matches_any() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    // Config with dimensions=None (auto-detect) should match any fingerprint dimensions
    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: None,
            ..Default::default()
        },
    )];
    assert!(fp.matches_configs(&configs_v2));
}

/// Verify that adding a new embedder to the config list causes fingerprint mismatch.
#[test]
fn test_fingerprint_mismatch_embedder_added() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![
        (
            "default".to_string(),
            EmbedderConfig {
                source: EmbedderSource::OpenAi,
                model: Some("model-a".into()),
                dimensions: Some(1536),
                ..Default::default()
            },
        ),
        (
            "secondary".to_string(),
            EmbedderConfig {
                source: EmbedderSource::Rest,
                model: Some("model-b".into()),
                dimensions: Some(768),
                ..Default::default()
            },
        ),
    ];
    assert!(!fp.matches_configs(&configs_v2));
}

/// Verify that removing an embedder from the config list causes fingerprint mismatch.
#[test]
fn test_fingerprint_mismatch_embedder_removed() {
    let configs_v1 = vec![
        (
            "default".to_string(),
            EmbedderConfig {
                source: EmbedderSource::OpenAi,
                model: Some("model-a".into()),
                dimensions: Some(1536),
                ..Default::default()
            },
        ),
        (
            "secondary".to_string(),
            EmbedderConfig {
                source: EmbedderSource::Rest,
                model: Some("model-b".into()),
                dimensions: Some(768),
                ..Default::default()
            },
        ),
    ];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::OpenAi,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            ..Default::default()
        },
    )];
    assert!(!fp.matches_configs(&configs_v2));
}

/// Verify that changing the document template causes fingerprint mismatch.
#[test]
fn test_fingerprint_mismatch_template_changed() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::Rest,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            document_template: Some("{{doc.title}} {{doc.body}}".into()),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 1536);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::Rest,
            model: Some("model-a".into()),
            dimensions: Some(1536),
            document_template: Some("{{doc.title}}".into()),
            ..Default::default()
        },
    )];
    assert!(!fp.matches_configs(&configs_v2));
}

// ── FastEmbed source tests (9.2) ──

#[test]
fn test_fastembed_source_serde() {
    let source = EmbedderSource::FastEmbed;
    let json = serde_json::to_string(&source).unwrap();
    assert_eq!(json, "\"fastEmbed\"");
    let deserialized: EmbedderSource = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, EmbedderSource::FastEmbed);
}

#[test]
fn test_fastembed_config_validate_ok() {
    let config = EmbedderConfig {
        source: EmbedderSource::FastEmbed,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_fastembed_config_validate_no_mandatory_fields() {
    let config = EmbedderConfig {
        source: EmbedderSource::FastEmbed,
        model: None,
        api_key: None,
        dimensions: None,
        url: None,
        request: None,
        response: None,
        headers: None,
        document_template: None,
        document_template_max_bytes: None,
    };
    assert!(config.validate().is_ok());
}

/// Verify that EmbedderFingerprint persists to disk and deserializes back with full fidelity.
#[test]
fn test_fingerprint_save_and_load_roundtrip() {
    let configs = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::Rest,
            model: Some("text-embedding-3-small".into()),
            dimensions: Some(1536),
            document_template: Some("{{doc.title}} {{doc.body}}".into()),
            document_template_max_bytes: Some(400),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs, 1536);

    let tmp = tempfile::TempDir::new().unwrap();
    fp.save(tmp.path()).unwrap();

    let loaded = EmbedderFingerprint::load(tmp.path()).unwrap();
    assert_eq!(fp, loaded);
    assert!(loaded.matches_configs(&configs));
}

// ── FastEmbed fingerprint tests (9.13) ──

/// Verify that FastEmbed source type is correctly preserved when fingerprints are saved and loaded.
#[test]
fn test_fingerprint_fastembed_source() {
    let configs = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::FastEmbed,
            model: Some("bge-small-en-v1.5".into()),
            dimensions: Some(384),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs, 384);

    let tmp = tempfile::TempDir::new().unwrap();
    fp.save(tmp.path()).unwrap();

    let loaded = EmbedderFingerprint::load(tmp.path()).unwrap();
    assert_eq!(fp, loaded);
    assert!(loaded.matches_configs(&configs));
    assert_eq!(loaded.embedders[0].source, EmbedderSource::FastEmbed);
}

/// Verify that changing the FastEmbed model name between fingerprints causes mismatch detection.
#[test]
fn test_fingerprint_fastembed_model_change_mismatch() {
    let configs_v1 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::FastEmbed,
            model: Some("bge-small-en-v1.5".into()),
            dimensions: Some(384),
            ..Default::default()
        },
    )];
    let fp = EmbedderFingerprint::from_configs(&configs_v1, 384);

    let configs_v2 = vec![(
        "default".to_string(),
        EmbedderConfig {
            source: EmbedderSource::FastEmbed,
            model: Some("all-MiniLM-L6-v2".into()),
            dimensions: Some(384),
            ..Default::default()
        },
    )];
    assert!(
        !fp.matches_configs(&configs_v2),
        "different model should not match"
    );
}
