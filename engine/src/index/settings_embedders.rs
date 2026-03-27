use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq)]
pub enum EmbedderChange {
    Added(String),
    Removed(String),
    Modified(String),
}

/// Diff two optional embedder configuration maps and return a list of added, removed, or modified embedder names.
///
/// An embedder is considered modified when any of `source`, `model`, or `dimensions` differ between old and new.
///
/// # Arguments
///
/// * `old` - Previous embedder map (or `None`).
/// * `new` - Updated embedder map (or `None`).
///
/// # Returns
///
/// A `Vec<EmbedderChange>` describing each detected change.
pub fn detect_embedder_changes(
    old: &Option<HashMap<String, serde_json::Value>>,
    new: &Option<HashMap<String, serde_json::Value>>,
) -> Vec<EmbedderChange> {
    let empty = HashMap::new();
    let old_map = old.as_ref().unwrap_or(&empty);
    let new_map = new.as_ref().unwrap_or(&empty);

    let mut changes = Vec::new();
    let mut all_keys: HashSet<&String> = old_map.keys().collect();
    all_keys.extend(new_map.keys());

    for key in all_keys {
        match (old_map.get(key), new_map.get(key)) {
            (None, Some(_)) => changes.push(EmbedderChange::Added(key.clone())),
            (Some(_), None) => changes.push(EmbedderChange::Removed(key.clone())),
            (Some(old_val), Some(new_val)) => {
                let fields_changed = ["source", "model", "dimensions"]
                    .iter()
                    .any(|field| old_val.get(field) != new_val.get(field));
                if fields_changed {
                    changes.push(EmbedderChange::Modified(key.clone()));
                }
            }
            (None, None) => unreachable!(),
        }
    }

    changes
}
