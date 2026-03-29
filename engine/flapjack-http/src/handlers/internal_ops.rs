use flapjack::index::oplog::OpLogEntry;
use flapjack::index::rules::{Rule, RuleStore};
use flapjack::index::synonyms::{Synonym, SynonymStore};
use flapjack::types::Document;
use flapjack::validate_index_name;
use flapjack::IndexManager;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Document ops (upsert / delete) — accumulate into batch vectors
// ---------------------------------------------------------------------------

/// Apply an upsert replication op to in-memory batch state.
pub(crate) fn apply_upsert_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
    incoming: (u64, String),
    upserts: &mut Vec<Document>,
    final_op_type: &mut HashMap<String, &str>,
) {
    let Some(body) = op_entry.payload.get("body") else {
        tracing::warn!(
            "[REPL {}] upsert seq {} missing body field",
            tenant_id,
            op_entry.seq
        );
        return;
    };

    let object_id = resolve_upsert_object_id(body);
    if should_skip_stale_upsert(manager, tenant_id, object_id, &incoming) {
        return;
    }

    match Document::from_json(body) {
        Ok(doc) => {
            if let Some(object_id) = object_id {
                manager.record_lww(tenant_id, object_id, incoming.0, incoming.1.clone());
                final_op_type.insert(object_id.to_string(), "upsert");
            }
            upserts.push(doc);
        }
        Err(e) => tracing::warn!(
            "[REPL {}] failed to parse upsert seq {}: {}",
            tenant_id,
            op_entry.seq,
            e
        ),
    }
}

fn resolve_upsert_object_id(body: &Value) -> Option<&str> {
    body.get("_id")
        .and_then(|value| value.as_str())
        .or_else(|| body.get("objectID").and_then(|value| value.as_str()))
        .filter(|object_id| !object_id.is_empty())
}

/// Returns true if an incoming upsert should be skipped because the local index already has a newer version of the same object (by oplog sequence).
fn should_skip_stale_upsert(
    manager: &IndexManager,
    tenant_id: &str,
    object_id: Option<&str>,
    incoming: &(u64, String),
) -> bool {
    let Some(object_id) = object_id else {
        return false;
    };

    let Some(existing) = manager.get_lww(tenant_id, object_id) else {
        return false;
    };

    if existing < *incoming {
        return false;
    }

    tracing::debug!(
        "[REPL {}] skipping stale upsert for {}/{} (existing={:?} >= incoming={:?})",
        tenant_id,
        tenant_id,
        object_id,
        existing,
        incoming
    );
    true
}

/// Apply a delete replication op to in-memory batch state.
pub(crate) fn apply_delete_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
    incoming: (u64, String),
    deletes: &mut Vec<String>,
    final_op_type: &mut HashMap<String, &str>,
) {
    let Some(id) = op_entry.payload.get("objectID").and_then(|v| v.as_str()) else {
        tracing::warn!(
            "[REPL {}] delete seq {} missing objectID field",
            tenant_id,
            op_entry.seq
        );
        return;
    };

    if let Some(existing) = manager.get_lww(tenant_id, id) {
        if existing > incoming {
            tracing::debug!(
                "[REPL {}] skipping stale delete for {}/{} (existing={:?} > incoming={:?})",
                tenant_id,
                tenant_id,
                id,
                existing,
                incoming
            );
            return;
        }
    }

    manager.record_lww(tenant_id, id, incoming.0, incoming.1.clone());
    final_op_type.insert(id.to_string(), "delete");
    deletes.push(id.to_string());
}

// ---------------------------------------------------------------------------
// Index-level ops (move / copy / clear)
// ---------------------------------------------------------------------------

/// Apply a move-index replication op.
pub(crate) async fn apply_move_index_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) -> Result<(), String> {
    let Some(source) = op_entry.payload.get("source").and_then(|v| v.as_str()) else {
        return Err(format!(
            "[REPL {}] move_index seq {} missing source field",
            tenant_id, op_entry.seq
        ));
    };
    let Some(destination) = op_entry.payload.get("destination").and_then(|v| v.as_str()) else {
        return Err(format!(
            "[REPL {}] move_index seq {} missing destination field",
            tenant_id, op_entry.seq
        ));
    };

    manager
        .move_index(source, destination)
        .await
        .map(|_| ())
        .map_err(|error| {
            format!(
                "[REPL {}] move_index seq {} failed ({} -> {}): {}",
                tenant_id, op_entry.seq, source, destination, error
            )
        })
}

pub(crate) struct ScopedJsonFileCopy<'a, F>
where
    F: FnOnce(&IndexManager, &str),
{
    pub manager: &'a IndexManager,
    pub tenant_id: &'a str,
    pub seq: u64,
    pub destination: &'a str,
    pub payload: &'a Value,
    pub payload_key: &'a str,
    pub filename: &'a str,
    pub invalidate_cache: F,
}

/// Copy JSON file payload to a destination tenant index file and invalidate cache.
pub(crate) fn copy_scoped_json_file<F>(copy: ScopedJsonFileCopy<'_, F>) -> Result<(), String>
where
    F: FnOnce(&IndexManager, &str),
{
    let ScopedJsonFileCopy {
        manager,
        tenant_id,
        seq,
        destination,
        payload,
        payload_key,
        filename,
        invalidate_cache,
    } = copy;

    let destination_path = manager.base_path.join(destination).join(filename);

    match serde_json::to_vec(payload) {
        Ok(bytes) => {
            std::fs::write(&destination_path, bytes).map_err(|error| {
                format!(
                    "[REPL {}] copy_index seq {} failed to write destination {} for {}: {}",
                    tenant_id, seq, filename, destination, error
                )
            })?;
            invalidate_cache(manager, destination);
            Ok(())
        }
        Err(error) => Err(format!(
            "[REPL {}] copy_index seq {} failed to serialize {} payload for {}: {}",
            tenant_id, seq, payload_key, destination, error
        )),
    }
}

/// Apply a copy-index replication op, including indexed scope payload handling.
pub(crate) async fn apply_copy_index_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) -> Result<(), String> {
    let source = copy_index_endpoint(tenant_id, op_entry, "source")?;
    let destination = copy_index_endpoint(tenant_id, op_entry, "destination")?;
    let scope = parse_copy_scope(tenant_id, op_entry)?;

    manager
        .copy_index(source, destination, scope.as_deref())
        .await
        .map_err(|error| {
            format!(
                "[REPL {}] copy_index seq {} failed ({} -> {}): {}",
                tenant_id, op_entry.seq, source, destination, error
            )
        })?;

    if scope_includes(scope.as_deref(), "settings") {
        copy_scoped_payload_if_present(
            manager,
            tenant_id,
            op_entry,
            destination,
            "source_settings",
            "settings.json",
            |index_manager, index_name| index_manager.invalidate_settings_cache(index_name),
        );
    }

    if scope_includes(scope.as_deref(), "synonyms") {
        copy_scoped_payload_if_present(
            manager,
            tenant_id,
            op_entry,
            destination,
            "source_synonyms",
            "synonyms.json",
            |index_manager, index_name| index_manager.invalidate_synonyms_cache(index_name),
        );
    }

    if scope_includes(scope.as_deref(), "rules") {
        copy_scoped_payload_if_present(
            manager,
            tenant_id,
            op_entry,
            destination,
            "source_rules",
            "rules.json",
            |index_manager, index_name| index_manager.invalidate_rules_cache(index_name),
        );
    }

    Ok(())
}

/// Extracts a string field (source or destination index name) from a copy/move operation payload, returning an error if missing.
fn copy_index_endpoint<'a>(
    tenant_id: &str,
    op_entry: &'a OpLogEntry,
    field_name: &str,
) -> Result<&'a str, String> {
    op_entry
        .payload
        .get(field_name)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            format!(
                "[REPL {}] copy_index seq {} missing {} field",
                tenant_id, op_entry.seq, field_name
            )
        })
}

/// Parses the optional `scope` field from a copy operation payload.
fn parse_copy_scope(tenant_id: &str, op_entry: &OpLogEntry) -> Result<Option<Vec<String>>, String> {
    let Some(scope_value) = op_entry.payload.get("scope") else {
        return Ok(None);
    };
    if scope_value.is_null() {
        return Ok(None);
    }

    serde_json::from_value(scope_value.clone())
        .map(Some)
        .map_err(|error| {
            format!(
                "[REPL {}] copy_index seq {} has invalid scope payload: {}",
                tenant_id, op_entry.seq, error
            )
        })
}

fn scope_includes(scope: Option<&[String]>, field_name: &str) -> bool {
    match scope {
        Some(values) => values.iter().any(|value| value == field_name),
        None => true,
    }
}

/// Copies a scoped data file (rules, synonyms, etc.) from the operation payload to the destination index directory, invalidating the relevant cache.
fn copy_scoped_payload_if_present<F>(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
    destination: &str,
    payload_key: &str,
    filename: &str,
    invalidate_cache: F,
) where
    F: FnOnce(&IndexManager, &str),
{
    let Some(payload) = op_entry
        .payload
        .get(payload_key)
        .filter(|value| !value.is_null())
    else {
        return;
    };

    if let Err(error) = copy_scoped_json_file(ScopedJsonFileCopy {
        manager,
        tenant_id,
        seq: op_entry.seq,
        destination,
        payload,
        payload_key,
        filename,
        invalidate_cache,
    }) {
        tracing::warn!("{}", error);
    }
}

/// Apply a clear-index replication op.
pub(crate) async fn apply_clear_index_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) -> Result<(), String> {
    let Some(index_name) = op_entry.payload.get("index_name").and_then(|v| v.as_str()) else {
        return Err(format!(
            "[REPL {}] clear_index seq {} missing index_name field",
            tenant_id, op_entry.seq
        ));
    };

    validate_index_name(index_name).map_err(|error| {
        format!(
            "[REPL {}] clear_index seq {} invalid index_name '{}': {}",
            tenant_id, op_entry.seq, index_name, error
        )
    })?;

    let index_path = manager.base_path.join(index_name);
    let settings_path = index_path.join("settings.json");
    let relevance_path = index_path.join("relevance.json");

    let settings = if settings_path.exists() {
        std::fs::read(&settings_path).ok()
    } else {
        None
    };
    let relevance = if relevance_path.exists() {
        std::fs::read(&relevance_path).ok()
    } else {
        None
    };

    manager
        .delete_tenant(&index_name.to_string())
        .await
        .map_err(|error| {
            format!(
                "[REPL {}] clear_index seq {} delete_tenant failed for {}: {}",
                tenant_id, op_entry.seq, index_name, error
            )
        })?;

    manager.create_tenant(index_name).map_err(|error| {
        format!(
            "[REPL {}] clear_index seq {} create_tenant failed for {}: {}",
            tenant_id, op_entry.seq, index_name, error
        )
    })?;

    if let Some(data) = settings {
        if let Err(error) = std::fs::write(&settings_path, data).map_err(|error| {
            format!(
                "[REPL {}] clear_index seq {} failed to restore settings for {}: {}",
                tenant_id, op_entry.seq, index_name, error
            )
        }) {
            tracing::warn!("{}", error);
        }
    }

    if let Some(data) = relevance {
        if let Err(error) = std::fs::write(&relevance_path, data).map_err(|error| {
            format!(
                "[REPL {}] clear_index seq {} failed to restore relevance for {}: {}",
                tenant_id, op_entry.seq, index_name, error
            )
        }) {
            tracing::warn!("{}", error);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Synonym ops — core store helpers + dispatcher wrappers
// ---------------------------------------------------------------------------

fn synonym_save(manager: &IndexManager, tenant_id: &str, synonym: Synonym) -> Result<(), String> {
    manager
        .create_tenant(tenant_id)
        .map_err(|e| e.to_string())?;
    let synonyms_path = manager.base_path.join(tenant_id).join("synonyms.json");
    let mut store = if synonyms_path.exists() {
        SynonymStore::load(&synonyms_path).map_err(|e| e.to_string())?
    } else {
        SynonymStore::new()
    };
    store.insert(synonym);
    store.save(&synonyms_path).map_err(|e| e.to_string())?;
    manager.invalidate_synonyms_cache(tenant_id);
    Ok(())
}

/// Applies a batch of synonym operations (add or replace-all) to a tenant index.
fn synonyms_batch(
    manager: &IndexManager,
    tenant_id: &str,
    synonyms: Vec<Synonym>,
    replace: bool,
) -> Result<(), String> {
    manager
        .create_tenant(tenant_id)
        .map_err(|e| e.to_string())?;
    let synonyms_path = manager.base_path.join(tenant_id).join("synonyms.json");
    let mut store = if replace || !synonyms_path.exists() {
        SynonymStore::new()
    } else {
        SynonymStore::load(&synonyms_path).map_err(|e| e.to_string())?
    };
    for synonym in synonyms {
        store.insert(synonym);
    }
    store.save(&synonyms_path).map_err(|e| e.to_string())?;
    manager.invalidate_synonyms_cache(tenant_id);
    Ok(())
}

fn synonym_delete(manager: &IndexManager, tenant_id: &str, object_id: &str) -> Result<(), String> {
    let synonyms_path = manager.base_path.join(tenant_id).join("synonyms.json");
    if !synonyms_path.exists() {
        return Ok(());
    }
    let mut store = SynonymStore::load(&synonyms_path).map_err(|e| e.to_string())?;
    store.remove(object_id);
    store.save(&synonyms_path).map_err(|e| e.to_string())?;
    manager.invalidate_synonyms_cache(tenant_id);
    Ok(())
}

fn synonyms_clear(manager: &IndexManager, tenant_id: &str) -> Result<(), String> {
    let synonyms_path = manager.base_path.join(tenant_id).join("synonyms.json");
    if synonyms_path.exists() {
        std::fs::remove_file(&synonyms_path).map_err(|e| e.to_string())?;
    }
    manager.invalidate_synonyms_cache(tenant_id);
    Ok(())
}

/// Dispatcher wrapper: parse payload and apply a single synonym save.
pub(crate) fn apply_save_synonym_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) {
    match serde_json::from_value::<Synonym>(op_entry.payload.clone()) {
        Ok(synonym) => {
            if let Err(e) = synonym_save(manager, tenant_id, synonym) {
                tracing::warn!(
                    "[REPL {}] save_synonym seq {} failed: {}",
                    tenant_id,
                    op_entry.seq,
                    e
                );
            }
        }
        Err(e) => tracing::warn!(
            "[REPL {}] save_synonym seq {} invalid payload: {}",
            tenant_id,
            op_entry.seq,
            e
        ),
    }
}

/// Dispatcher wrapper: parse payload and apply a batch synonym save.
pub(crate) fn apply_save_synonyms_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) {
    let replace = op_entry
        .payload
        .get("replace")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let synonyms = match op_entry.payload.get("synonyms") {
        Some(v) => serde_json::from_value::<Vec<Synonym>>(v.clone()),
        None => Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing synonyms field",
        ))),
    };
    match synonyms {
        Ok(synonyms) => {
            if let Err(e) = synonyms_batch(manager, tenant_id, synonyms, replace) {
                tracing::warn!(
                    "[REPL {}] save_synonyms seq {} failed: {}",
                    tenant_id,
                    op_entry.seq,
                    e
                );
            }
        }
        Err(e) => tracing::warn!(
            "[REPL {}] save_synonyms seq {} invalid payload: {}",
            tenant_id,
            op_entry.seq,
            e
        ),
    }
}

/// Dispatcher wrapper: extract objectID and delete a synonym.
pub(crate) fn apply_delete_synonym_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) {
    let Some(object_id) = op_entry.payload.get("objectID").and_then(|v| v.as_str()) else {
        tracing::warn!(
            "[REPL {}] delete_synonym seq {} missing objectID field",
            tenant_id,
            op_entry.seq
        );
        return;
    };
    if let Err(e) = synonym_delete(manager, tenant_id, object_id) {
        tracing::warn!(
            "[REPL {}] delete_synonym seq {} failed: {}",
            tenant_id,
            op_entry.seq,
            e
        );
    }
}

/// Dispatcher wrapper: clear all synonyms for a tenant.
pub(crate) fn apply_clear_synonyms_op(
    manager: &IndexManager,
    tenant_id: &str,
    op_entry: &OpLogEntry,
) {
    if let Err(e) = synonyms_clear(manager, tenant_id) {
        tracing::warn!(
            "[REPL {}] clear_synonyms seq {} failed: {}",
            tenant_id,
            op_entry.seq,
            e
        );
    }
}

// ---------------------------------------------------------------------------
// Rule ops — core store helpers + dispatcher wrappers
// ---------------------------------------------------------------------------

fn rule_save(manager: &IndexManager, tenant_id: &str, rule: Rule) -> Result<(), String> {
    manager
        .create_tenant(tenant_id)
        .map_err(|e| e.to_string())?;
    let rules_path = manager.base_path.join(tenant_id).join("rules.json");
    let mut store = if rules_path.exists() {
        RuleStore::load(&rules_path).map_err(|e| e.to_string())?
    } else {
        RuleStore::new()
    };
    store.insert(rule);
    store.save(&rules_path).map_err(|e| e.to_string())?;
    manager.invalidate_rules_cache(tenant_id);
    Ok(())
}

/// Applies a batch of rule operations (add or replace-all) to a tenant index.
fn rules_batch(
    manager: &IndexManager,
    tenant_id: &str,
    rules: Vec<Rule>,
    clear_existing: bool,
) -> Result<(), String> {
    manager
        .create_tenant(tenant_id)
        .map_err(|e| e.to_string())?;
    let rules_path = manager.base_path.join(tenant_id).join("rules.json");
    let mut store = if clear_existing || !rules_path.exists() {
        RuleStore::new()
    } else {
        RuleStore::load(&rules_path).map_err(|e| e.to_string())?
    };
    for rule in rules {
        store.insert(rule);
    }
    store.save(&rules_path).map_err(|e| e.to_string())?;
    manager.invalidate_rules_cache(tenant_id);
    Ok(())
}

fn rule_delete(manager: &IndexManager, tenant_id: &str, object_id: &str) -> Result<(), String> {
    let rules_path = manager.base_path.join(tenant_id).join("rules.json");
    if !rules_path.exists() {
        return Ok(());
    }
    let mut store = RuleStore::load(&rules_path).map_err(|e| e.to_string())?;
    store.remove(object_id);
    store.save(&rules_path).map_err(|e| e.to_string())?;
    manager.invalidate_rules_cache(tenant_id);
    Ok(())
}

fn rules_clear(manager: &IndexManager, tenant_id: &str) -> Result<(), String> {
    let rules_path = manager.base_path.join(tenant_id).join("rules.json");
    if rules_path.exists() {
        std::fs::remove_file(&rules_path).map_err(|e| e.to_string())?;
    }
    manager.invalidate_rules_cache(tenant_id);
    Ok(())
}

/// Dispatcher wrapper: parse payload and apply a single rule save.
pub(crate) fn apply_save_rule_op(manager: &IndexManager, tenant_id: &str, op_entry: &OpLogEntry) {
    match serde_json::from_value::<Rule>(op_entry.payload.clone()) {
        Ok(rule) => {
            if let Err(e) = rule_save(manager, tenant_id, rule) {
                tracing::warn!(
                    "[REPL {}] save_rule seq {} failed: {}",
                    tenant_id,
                    op_entry.seq,
                    e
                );
            }
        }
        Err(e) => tracing::warn!(
            "[REPL {}] save_rule seq {} invalid payload: {}",
            tenant_id,
            op_entry.seq,
            e
        ),
    }
}

/// Dispatcher wrapper: parse payload and apply a batch rule save.
pub(crate) fn apply_save_rules_op(manager: &IndexManager, tenant_id: &str, op_entry: &OpLogEntry) {
    let clear_existing = op_entry
        .payload
        .get("clearExisting")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let rules = match op_entry.payload.get("rules") {
        Some(v) => serde_json::from_value::<Vec<Rule>>(v.clone()),
        None => Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "missing rules field",
        ))),
    };
    match rules {
        Ok(rules) => {
            if let Err(e) = rules_batch(manager, tenant_id, rules, clear_existing) {
                tracing::warn!(
                    "[REPL {}] save_rules seq {} failed: {}",
                    tenant_id,
                    op_entry.seq,
                    e
                );
            }
        }
        Err(e) => tracing::warn!(
            "[REPL {}] save_rules seq {} invalid payload: {}",
            tenant_id,
            op_entry.seq,
            e
        ),
    }
}

/// Dispatcher wrapper: extract objectID and delete a rule.
pub(crate) fn apply_delete_rule_op(manager: &IndexManager, tenant_id: &str, op_entry: &OpLogEntry) {
    let Some(object_id) = op_entry.payload.get("objectID").and_then(|v| v.as_str()) else {
        tracing::warn!(
            "[REPL {}] delete_rule seq {} missing objectID field",
            tenant_id,
            op_entry.seq
        );
        return;
    };
    if let Err(e) = rule_delete(manager, tenant_id, object_id) {
        tracing::warn!(
            "[REPL {}] delete_rule seq {} failed: {}",
            tenant_id,
            op_entry.seq,
            e
        );
    }
}

/// Dispatcher wrapper: clear all rules for a tenant.
pub(crate) fn apply_clear_rules_op(manager: &IndexManager, tenant_id: &str, op_entry: &OpLogEntry) {
    if let Err(e) = rules_clear(manager, tenant_id) {
        tracing::warn!(
            "[REPL {}] clear_rules seq {} failed: {}",
            tenant_id,
            op_entry.seq,
            e
        );
    }
}

// ---------------------------------------------------------------------------
// Batch flush — resolve ordering conflicts + flush upserts/deletes
// ---------------------------------------------------------------------------

/// Resolve batch ordering, deduplicate upserts, and flush documents to the index.
///
/// When the same doc ID appears in both upserts and deletes within one batch,
/// only the final operation (by LWW timestamp) is applied. Upserts are further
/// deduplicated so only the last version per doc ID is indexed.
pub(crate) async fn flush_document_batch(
    manager: &IndexManager,
    tenant_id: &str,
    mut upserts: Vec<Document>,
    mut deletes: Vec<String>,
    final_op_type: HashMap<String, &str>,
) -> Result<(), String> {
    // Resolve batch ordering: when the same doc ID appears in both upserts and
    // deletes, only the final operation (by LWW timestamp) should be applied.
    upserts.retain(|doc| final_op_type.get(&doc.id).copied().unwrap_or("upsert") == "upsert");
    deletes.retain(|id| final_op_type.get(id.as_str()).copied().unwrap_or("delete") == "delete");

    // Deduplicate upserts: keep only the last version for each doc ID.
    // tantivy's delete_term only affects pre-existing docs, so adding two
    // docs with the same ID in one batch leaves both in the index.
    {
        let mut seen = HashSet::new();
        let mut deduped = Vec::with_capacity(upserts.len());
        for doc in upserts.into_iter().rev() {
            if seen.insert(doc.id.clone()) {
                deduped.push(doc);
            }
        }
        deduped.reverse();
        upserts = deduped;
    }

    if !upserts.is_empty() {
        manager
            .add_documents_for_replication(tenant_id, upserts)
            .map_err(|e| format!("add_documents failed: {}", e))?;
    }

    if !deletes.is_empty() {
        manager
            .delete_documents_sync_for_replication(tenant_id, deletes)
            .await
            .map_err(|e| format!("delete_documents failed: {}", e))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy_index_entry(payload: serde_json::Value) -> OpLogEntry {
        OpLogEntry {
            seq: 9,
            timestamp_ms: 42,
            node_id: "node-a".to_string(),
            tenant_id: "tenant-a".to_string(),
            op_type: "copy_index".to_string(),
            payload,
        }
    }

    #[test]
    fn parse_copy_scope_defaults_to_none_when_missing_or_null() {
        let missing_scope = copy_index_entry(serde_json::json!({
            "source": "src",
            "destination": "dst"
        }));
        let null_scope = copy_index_entry(serde_json::json!({
            "source": "src",
            "destination": "dst",
            "scope": null
        }));

        assert_eq!(parse_copy_scope("tenant-a", &missing_scope).unwrap(), None);
        assert_eq!(parse_copy_scope("tenant-a", &null_scope).unwrap(), None);
    }

    #[test]
    fn parse_copy_scope_parses_string_list() {
        let scoped_entry = copy_index_entry(serde_json::json!({
            "source": "src",
            "destination": "dst",
            "scope": ["settings", "rules"]
        }));

        assert_eq!(
            parse_copy_scope("tenant-a", &scoped_entry).unwrap(),
            Some(vec!["settings".to_string(), "rules".to_string()])
        );
    }

    #[test]
    fn resolve_upsert_object_id_prefers_id_field_before_object_id() {
        let preferred_id = serde_json::json!({
            "_id": "primary-id",
            "objectID": "secondary-id"
        });
        let fallback_object_id = serde_json::json!({
            "objectID": "secondary-id"
        });

        assert_eq!(resolve_upsert_object_id(&preferred_id), Some("primary-id"));
        assert_eq!(
            resolve_upsert_object_id(&fallback_object_id),
            Some("secondary-id")
        );
    }

    #[test]
    fn resolve_upsert_object_id_ignores_empty_values() {
        let empty_id = serde_json::json!({
            "_id": ""
        });
        let empty_object_id = serde_json::json!({
            "objectID": ""
        });

        assert_eq!(resolve_upsert_object_id(&empty_id), None);
        assert_eq!(resolve_upsert_object_id(&empty_object_id), None);
    }
}
