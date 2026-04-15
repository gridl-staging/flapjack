use super::super::index_resource_store::{
    clear_resource_store, delete_resource_item, save_resource_batch, save_resource_item,
};
use flapjack::index::oplog::OpLogEntry;
use flapjack::index::rules::{Rule, RuleStore};
use flapjack::index::synonyms::{Synonym, SynonymStore};
use flapjack::IndexManager;

fn synonym_save(manager: &IndexManager, tenant_id: &str, synonym: Synonym) -> Result<(), String> {
    save_resource_item::<SynonymStore>(manager, tenant_id, synonym).map_err(|e| e.to_string())
}

/// Applies a batch of synonym operations (add or replace-all) to a tenant index.
fn synonyms_batch(
    manager: &IndexManager,
    tenant_id: &str,
    synonyms: Vec<Synonym>,
    replace: bool,
) -> Result<(), String> {
    save_resource_batch::<SynonymStore, _>(manager, tenant_id, synonyms, replace)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn synonym_delete(manager: &IndexManager, tenant_id: &str, object_id: &str) -> Result<(), String> {
    delete_resource_item::<SynonymStore>(manager, tenant_id, object_id)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn synonyms_clear(manager: &IndexManager, tenant_id: &str) -> Result<(), String> {
    clear_resource_store::<SynonymStore>(manager, tenant_id).map_err(|e| e.to_string())
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

fn rule_save(manager: &IndexManager, tenant_id: &str, rule: Rule) -> Result<(), String> {
    save_resource_item::<RuleStore>(manager, tenant_id, rule).map_err(|e| e.to_string())
}

/// Applies a batch of rule operations (add or replace-all) to a tenant index.
fn rules_batch(
    manager: &IndexManager,
    tenant_id: &str,
    rules: Vec<Rule>,
    clear_existing: bool,
) -> Result<(), String> {
    save_resource_batch::<RuleStore, _>(manager, tenant_id, rules, clear_existing)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn rule_delete(manager: &IndexManager, tenant_id: &str, object_id: &str) -> Result<(), String> {
    delete_resource_item::<RuleStore>(manager, tenant_id, object_id)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn rules_clear(manager: &IndexManager, tenant_id: &str) -> Result<(), String> {
    clear_resource_store::<RuleStore>(manager, tenant_id).map_err(|e| e.to_string())
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
