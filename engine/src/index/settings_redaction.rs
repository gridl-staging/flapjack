//! Manages redaction and restoration of sensitive fields in JSON user settings and embedder configurations, replacing secrets with placeholders on export and restoring them from cached state on import.
use std::collections::HashMap;

pub const REDACTED_SECRET: &str = "<redacted>";

fn redact_object_field(object: &mut serde_json::Map<String, serde_json::Value>, field: &str) {
    if object.contains_key(field) {
        object.insert(
            field.to_string(),
            serde_json::Value::String(REDACTED_SECRET.to_string()),
        );
    }
}

/// Restore a redacted field from the previous object state, or remove it if no previous value exists.
///
/// # Arguments
///
/// * `object` - The JSON object to restore the field in
/// * `previous` - The previous object state to restore the field value from
/// * `field` - The name of the field to restore
///
/// # Behavior
///
/// If the field in the current object is redacted (equals REDACTED_SECRET), restores it from the previous object. If no previous object or previous field value exists, removes the field entirely.
fn restore_redacted_object_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    previous: Option<&serde_json::Map<String, serde_json::Value>>,
    field: &str,
) {
    let should_restore = matches!(
        object.get(field),
        Some(serde_json::Value::String(value)) if value == REDACTED_SECRET
    );
    if !should_restore {
        return;
    }

    if let Some(previous_value) = previous.and_then(|previous| previous.get(field)).cloned() {
        object.insert(field.to_string(), previous_value);
    } else {
        object.remove(field);
    }
}

fn redact_ai_provider_api_key(user_data: &mut serde_json::Value) {
    let Some(ai_provider) = user_data
        .get_mut("aiProvider")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    redact_object_field(ai_provider, "apiKey");
}

/// Restore the redacted API key in the aiProvider configuration of user data.
///
/// # Arguments
///
/// * `user_data` - The user data JSON containing the aiProvider configuration
/// * `previous_user_data` - The previous user data state to restore values from
///
/// # Behavior
///
/// Extracts the aiProvider object and restores its redacted apiKey field from the previous state if available. Does nothing if aiProvider is not present or is not a JSON object.
fn restore_ai_provider_api_key(
    user_data: &mut serde_json::Value,
    previous_user_data: Option<&serde_json::Value>,
) {
    let Some(ai_provider) = user_data
        .get_mut("aiProvider")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };

    let previous_ai_provider = previous_user_data
        .and_then(|value| value.get("aiProvider"))
        .and_then(serde_json::Value::as_object);
    restore_redacted_object_field(ai_provider, previous_ai_provider, "apiKey");
}

fn redact_embedder_api_key(config: &mut serde_json::Value) {
    let Some(object) = config.as_object_mut() else {
        return;
    };

    redact_object_field(object, "apiKey");
}

fn restore_embedder_api_key(config: &mut serde_json::Value, previous: Option<&serde_json::Value>) {
    let Some(object) = config.as_object_mut() else {
        return;
    };

    let previous_object = previous.and_then(serde_json::Value::as_object);
    restore_redacted_object_field(object, previous_object, "apiKey");
}

pub(super) fn redact_user_data_secrets(user_data: &mut Option<serde_json::Value>) {
    if let Some(user_data) = user_data.as_mut() {
        redact_ai_provider_api_key(user_data);
    }
}

pub(super) fn restore_user_data_secrets(
    user_data: &mut Option<serde_json::Value>,
    previous_user_data: Option<&serde_json::Value>,
) {
    if let Some(user_data) = user_data.as_mut() {
        restore_ai_provider_api_key(user_data, previous_user_data);
    }
}

pub(super) fn redact_embedder_secrets(embedders: &mut Option<HashMap<String, serde_json::Value>>) {
    if let Some(embedders) = embedders.as_mut() {
        for config in embedders.values_mut() {
            redact_embedder_api_key(config);
        }
    }
}

pub(super) fn restore_embedder_secrets(
    embedders: &mut Option<HashMap<String, serde_json::Value>>,
    previous_embedders: Option<&HashMap<String, serde_json::Value>>,
) {
    if let Some(embedders) = embedders.as_mut() {
        for (name, config) in embedders.iter_mut() {
            let previous_config = previous_embedders.and_then(|previous| previous.get(name));
            restore_embedder_api_key(config, previous_config);
        }
    }
}
