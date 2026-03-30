//! Embedder configuration and document templating with fingerprinting to detect incompatible embedder changes.
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::VectorError;

/// Source type for an embedder configuration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EmbedderSource {
    OpenAi,
    Rest,
    #[default]
    UserProvided,
    FastEmbed,
}

/// Configuration for creating an embedder.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct EmbedderConfig {
    pub source: EmbedderSource,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub dimensions: Option<usize>,
    pub url: Option<String>,
    pub request: Option<serde_json::Value>,
    pub response: Option<serde_json::Value>,
    pub headers: Option<HashMap<String, String>>,
    pub document_template: Option<String>,
    pub document_template_max_bytes: Option<usize>,
}

impl EmbedderConfig {
    /// Build a DocumentTemplate from this embedder's template configuration.
    pub fn document_template(&self) -> DocumentTemplate {
        DocumentTemplate::new(
            self.document_template.clone(),
            self.document_template_max_bytes,
        )
    }

    /// Validate that required fields are present for the given source type.
    pub fn validate(&self) -> Result<(), VectorError> {
        match self.source {
            EmbedderSource::OpenAi => {
                if self.api_key.is_none() {
                    return Err(VectorError::EmbeddingError(
                        "openAi embedder requires `apiKey`".into(),
                    ));
                }
            }
            EmbedderSource::Rest => {
                let mut missing = Vec::new();
                if self.url.is_none() {
                    missing.push("`url`");
                }
                if self.request.is_none() {
                    missing.push("`request`");
                }
                if self.response.is_none() {
                    missing.push("`response`");
                }
                if !missing.is_empty() {
                    return Err(VectorError::EmbeddingError(format!(
                        "rest embedder requires {}",
                        missing.join(", ")
                    )));
                }
            }
            EmbedderSource::UserProvided => {
                if self.dimensions.is_none() {
                    return Err(VectorError::EmbeddingError(
                        "userProvided embedder requires `dimensions`".into(),
                    ));
                }
            }
            EmbedderSource::FastEmbed => {
                // No mandatory fields — model defaults to bge-small-en-v1.5.
                // Dimension validation happens in FastEmbedEmbedder::new() where
                // the model info is available.
            }
        }
        Ok(())
    }
}

/// A single entry in the embedder fingerprint, capturing the semantic-relevant
/// fields of one embedder configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EmbedderFingerprintEntry {
    pub name: String,
    pub source: EmbedderSource,
    pub model: Option<String>,
    pub dimensions: usize,
    pub document_template: Option<String>,
    pub document_template_max_bytes: Option<usize>,
}

/// Fingerprint capturing all embedder configurations for a tenant.
/// Used to detect when embedder settings change, invalidating stored vectors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbedderFingerprint {
    pub version: u32,
    pub embedders: Vec<EmbedderFingerprintEntry>,
}

impl EmbedderFingerprint {
    /// Build a fingerprint from the current embedder configs and actual dimensions
    /// from the VectorIndex.
    pub fn from_configs(configs: &[(String, EmbedderConfig)], actual_dimensions: usize) -> Self {
        let mut entries: Vec<EmbedderFingerprintEntry> = configs
            .iter()
            .map(|(name, config)| EmbedderFingerprintEntry {
                name: name.clone(),
                source: config.source,
                model: config.model.clone(),
                dimensions: actual_dimensions,
                document_template: config.document_template.clone(),
                document_template_max_bytes: config.document_template_max_bytes,
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            version: 1,
            embedders: entries,
        }
    }

    /// Check whether the current embedder configs match this fingerprint.
    /// Returns true if all semantic fields match (name, source, model, template).
    /// Dimensions: if config.dimensions is Some(n), checks n == entry.dimensions.
    /// If config.dimensions is None (auto-detect), skips dimension check.
    pub fn matches_configs(&self, configs: &[(String, EmbedderConfig)]) -> bool {
        let mut sorted: Vec<(String, &EmbedderConfig)> =
            configs.iter().map(|(n, c)| (n.clone(), c)).collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        if sorted.len() != self.embedders.len() {
            return false;
        }

        for (entry, (name, config)) in self.embedders.iter().zip(sorted.iter()) {
            if entry.name != *name {
                return false;
            }
            if entry.source != config.source {
                return false;
            }
            if entry.model != config.model {
                return false;
            }
            if entry.document_template != config.document_template {
                return false;
            }
            if entry.document_template_max_bytes != config.document_template_max_bytes {
                return false;
            }
            // Dimensions: only check if config specifies them (Some).
            // None means auto-detect — matches any stored dimensions.
            if let Some(dim) = config.dimensions {
                if dim != entry.dimensions {
                    return false;
                }
            }
        }

        true
    }

    /// Save fingerprint to `{dir}/fingerprint.json`.
    pub fn save(&self, dir: &std::path::Path) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("fingerprint.json");
        let json = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(&path, json)
    }

    /// Load fingerprint from `{dir}/fingerprint.json`.
    pub fn load(dir: &std::path::Path) -> Result<Self, std::io::Error> {
        let path = dir.join("fingerprint.json");
        let data = std::fs::read_to_string(&path)?;
        serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

/// Document template for rendering searchable text from JSON documents.
pub struct DocumentTemplate {
    pub template: Option<String>,
    pub max_bytes: usize,
}

impl Default for DocumentTemplate {
    fn default() -> Self {
        Self {
            template: None,
            max_bytes: 400,
        }
    }
}

impl DocumentTemplate {
    pub fn new(template: Option<String>, max_bytes: Option<usize>) -> Self {
        Self {
            template,
            max_bytes: max_bytes.unwrap_or(400),
        }
    }

    /// Render a JSON document into a searchable text string.
    ///
    /// If a template is set, substitute `{{doc.field_name}}` patterns.
    /// If no template, concatenate all string values separated by `. `.
    /// Truncate to `max_bytes` at a UTF-8 boundary.
    pub fn render(&self, document: &serde_json::Value) -> String {
        let result = match &self.template {
            Some(tmpl) => Self::render_template(tmpl, document),
            None => Self::render_default(document),
        };
        truncate_utf8(&result, self.max_bytes)
    }

    /// Substitute `{{doc.field.path}}` placeholders with values from the document.
    fn render_template(template: &str, document: &serde_json::Value) -> String {
        let mut result = String::new();
        let mut rest = template;
        while let Some(start) = rest.find("{{doc.") {
            result.push_str(&rest[..start]);
            let after_open = &rest[start + 6..]; // skip "{{doc."
            if let Some(end) = after_open.find("}}") {
                let field_path = &after_open[..end];
                let value = resolve_path(document, field_path);
                result.push_str(value);
                rest = &after_open[end + 2..];
            } else {
                // No closing }}, copy the remainder literally and stop
                result.push_str(&rest[start..]);
                return result;
            }
        }
        result.push_str(rest);
        result
    }

    /// Default: concatenate all top-level user string values separated by `. `.
    /// Skips internal fields (`_id`, `objectID`) which carry no semantic meaning.
    fn render_default(document: &serde_json::Value) -> String {
        let obj = match document.as_object() {
            Some(o) => o,
            None => return String::new(),
        };
        let parts: Vec<&str> = obj
            .iter()
            .filter(|(k, _)| *k != "_id" && *k != "objectID")
            .filter_map(|(_, v)| v.as_str())
            .collect();
        parts.join(". ")
    }
}

/// Navigate a dot-separated path into a JSON value, returning the string value or "".
fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> &'a str {
    let mut current = value;
    for key in path.split('.') {
        match current.get(key) {
            Some(v) => current = v,
            None => return "",
        }
    }
    current.as_str().unwrap_or("")
}

/// Truncate a string to at most `max_bytes` at a UTF-8 char boundary.
fn truncate_utf8(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_owned();
    }
    // Find the last valid char boundary at or before max_bytes
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
