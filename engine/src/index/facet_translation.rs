//! Utilities for translating between Algolia-style hierarchical facet values and Tantivy facet path representations.
use crate::error::{FlapjackError, Result};
use serde_json::{Map, Value};

pub fn is_hierarchical_facet(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.keys().any(|k| k.starts_with("lvl")),
        _ => false,
    }
}

pub fn algolia_to_tantivy_path(field_name: &str, algolia_value: &str) -> String {
    let path = algolia_value.replace(" > ", "/");
    format!("/{}/{}", field_name, path)
}

pub fn tantivy_to_algolia_path(tantivy_path: &str) -> String {
    tantivy_path.trim_start_matches('/').replace('/', " > ")
}

/// Return the string value associated with the highest-numbered `lvlN` key in a JSON object map.
///
/// Iterates all keys matching the pattern `lvl<N>` where `N` is a parseable integer, and returns the string value of the key with the largest `N`.
///
/// # Arguments
///
/// * `obj` - A JSON object map expected to contain hierarchical level keys (e.g. `lvl0`, `lvl1`, `lvl2`).
///
/// # Returns
///
/// `Some(value)` for the deepest level found, or `None` if no `lvlN` keys exist or the deepest value is not a string.
pub fn extract_deepest_level(obj: &Map<String, Value>) -> Option<String> {
    let mut max_level = -1;
    let mut deepest = None;

    for (key, val) in obj {
        if let Some(suffix) = key.strip_prefix("lvl") {
            if let Ok(level) = suffix.parse::<i32>() {
                if level > max_level {
                    max_level = level;
                    deepest = val.as_str().map(String::from);
                }
            }
        }
    }

    deepest
}

/// Convert a facet value into Tantivy-style path strings based on its JSON type.
///
/// For objects with hierarchical levels (e.g. `lvl0`, `lvl1`), each key-value pair produces a path like `/<field>.<key>/<value>`. For plain strings, delegates to `algolia_to_tantivy_path`. For arrays, converts each string element independently.
///
/// # Arguments
///
/// * `field_name` - The facet field name used as the path prefix.
/// * `value` - A JSON value that may be an object (hierarchical), string, or array of strings.
///
/// # Returns
///
/// A vector of Tantivy-style facet path strings.
///
/// # Errors
///
/// Returns `FlapjackError::InvalidDocument` if `value` is not an object, string, or array.
pub fn extract_facet_paths(field_name: &str, value: &Value) -> Result<Vec<String>> {
    match value {
        Value::Object(map) => {
            let mut paths = Vec::new();
            let _is_hier = map.keys().any(|k| k.starts_with("lvl"));
            for (key, val) in map {
                if let Some(s) = val.as_str() {
                    let nested_field = format!("{}.{}", field_name, key);
                    paths.push(format!("/{}/{}", nested_field, s));
                }
            }
            Ok(paths)
        }
        Value::String(s) => Ok(vec![algolia_to_tantivy_path(field_name, s)]),
        Value::Array(arr) => {
            let mut paths = Vec::new();
            for item in arr {
                if let Some(s) = item.as_str() {
                    paths.push(algolia_to_tantivy_path(field_name, s));
                }
            }
            Ok(paths)
        }
        _ => Err(FlapjackError::InvalidDocument(format!(
            "Invalid facet value type for field {}",
            field_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_algolia_to_tantivy() {
        assert_eq!(
            algolia_to_tantivy_path("categories", "Electronics > Computers"),
            "/categories/Electronics/Computers"
        );
    }

    #[test]
    fn test_deepest_level_extraction() {
        let obj = json!({
            "lvl0": "Electronics",
            "lvl1": "Electronics > Computers",
            "lvl2": "Electronics > Computers > Laptops"
        });

        let map = obj.as_object().unwrap();
        let deepest = extract_deepest_level(map).unwrap();
        assert_eq!(deepest, "Electronics > Computers > Laptops");
    }

    #[test]
    fn test_extract_facet_paths() {
        let obj = json!({
            "lvl0": "Electronics",
            "lvl1": "Electronics > Computers"
        });

        let paths = extract_facet_paths("categories", &obj).unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"/categories.lvl0/Electronics".to_string()));
        assert!(paths.contains(&"/categories.lvl1/Electronics > Computers".to_string()));
    }
}
