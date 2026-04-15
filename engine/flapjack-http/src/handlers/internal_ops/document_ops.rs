use flapjack::index::oplog::OpLogEntry;
use flapjack::types::Document;
use flapjack::IndexManager;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

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

pub(super) fn resolve_upsert_object_id(body: &Value) -> Option<&str> {
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
