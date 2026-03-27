//! Helpers for managing replica indexes: resolving virtual-vs-physical search targets, persisting and clearing primary links, and mirroring document writes/deletes to standard replicas.
use super::AppState;
use flapjack::error::FlapjackError;
use flapjack::index::replica::{parse_replica_entry, standard_replica_names, ReplicaEntry};
use flapjack::index::settings::IndexSettings;
use flapjack::types::Document;
use std::collections::HashSet;
use std::sync::Arc;

/// Load standard (non-virtual) replica names configured on a primary index.
pub(crate) fn standard_replicas_for_primary(
    state: &Arc<AppState>,
    primary_index_name: &str,
) -> Result<Vec<String>, FlapjackError> {
    let settings_path = state
        .manager
        .base_path
        .join(primary_index_name)
        .join("settings.json");
    if !settings_path.exists() {
        return Ok(Vec::new());
    }

    let settings = IndexSettings::load(&settings_path)?;
    Ok(settings
        .replicas
        .as_ref()
        .map(|replicas| standard_replica_names(replicas))
        .unwrap_or_default())
}

pub(crate) fn has_physical_index_data(state: &Arc<AppState>, index_name: &str) -> bool {
    state
        .manager
        .base_path
        .join(index_name)
        .join("meta.json")
        .exists()
}

pub(crate) fn is_virtual_settings_only_index(state: &Arc<AppState>, index_name: &str) -> bool {
    let Some(settings) = state.manager.get_settings(index_name) else {
        return false;
    };
    settings.primary.is_some() && !has_physical_index_data(state, index_name)
}

pub(crate) fn reject_writes_to_virtual_replica(
    state: &Arc<AppState>,
    index_name: &str,
) -> Result<(), FlapjackError> {
    if is_virtual_settings_only_index(state, index_name) {
        return Err(FlapjackError::InvalidQuery(
            "Virtual replica indices are read-only. Write to the primary index instead."
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) struct ResolvedSearchTarget {
    pub data_index: String,
    pub settings_override: Option<IndexSettings>,
}

/// Determine which physical index to query and whether to apply a settings override.
///
/// For a virtual replica (has a `primary` link but no physical Tantivy data),
/// redirects the search to the primary index's data while returning the
/// virtual replica's settings as an override. For all other indexes the
/// requested name is used directly with no override.
///
/// # Arguments
///
/// * `state` - Shared application state containing the index manager.
/// * `requested_index` - The index name the caller asked to search.
///
/// # Returns
///
/// A `ResolvedSearchTarget` with the physical `data_index` to read from and
/// an optional `settings_override` when the request targeted a virtual replica.
pub(crate) fn resolve_search_target(
    state: &Arc<AppState>,
    requested_index: &str,
) -> ResolvedSearchTarget {
    let Some(settings) = state.manager.get_settings(requested_index) else {
        return ResolvedSearchTarget {
            data_index: requested_index.to_string(),
            settings_override: None,
        };
    };

    let Some(primary_index) = &settings.primary else {
        return ResolvedSearchTarget {
            data_index: requested_index.to_string(),
            settings_override: None,
        };
    };

    if has_physical_index_data(state, requested_index) {
        return ResolvedSearchTarget {
            data_index: requested_index.to_string(),
            settings_override: None,
        };
    }

    ResolvedSearchTarget {
        data_index: primary_index.clone(),
        settings_override: Some(settings.as_ref().clone()),
    }
}

/// Persist the read-only `primary` link for each configured replica.
///
/// Standard replicas are physical indexes (create_tenant), virtual replicas are
/// settings-only directories with `settings.json` and no Tantivy data files.
pub(crate) fn persist_replica_primary_links(
    state: &Arc<AppState>,
    primary_index_name: &str,
    replicas: &[ReplicaEntry],
) -> Result<(), FlapjackError> {
    for replica in replicas {
        let replica_name = replica.name();
        match replica {
            ReplicaEntry::Standard(_) => state.manager.create_tenant(replica_name)?,
            ReplicaEntry::Virtual(_) => {
                std::fs::create_dir_all(state.manager.base_path.join(replica_name))?;
            }
        }

        let settings_path = state
            .manager
            .base_path
            .join(replica_name)
            .join("settings.json");
        let mut settings = if settings_path.exists() {
            IndexSettings::load(&settings_path)?
        } else {
            IndexSettings::default()
        };
        settings.primary = Some(primary_index_name.to_string());
        settings.save(&settings_path)?;
        state.manager.invalidate_settings_cache(replica_name);
        state.manager.invalidate_facet_cache(replica_name);
    }
    Ok(())
}

/// Remove `primary` link from replicas removed from a primary's replicas list.
pub(crate) fn clear_removed_replica_primary_links(
    state: &Arc<AppState>,
    primary_index_name: &str,
    previous_replicas: Option<&[String]>,
    next_replicas: &[ReplicaEntry],
) -> Result<(), FlapjackError> {
    let previous = previous_replicas.unwrap_or(&[]);
    if previous.is_empty() {
        return Ok(());
    }

    let next_names: HashSet<String> = next_replicas
        .iter()
        .map(|entry| entry.name().to_string())
        .collect();

    for old in previous {
        let Ok(parsed) = parse_replica_entry(old) else {
            continue;
        };
        let replica_name = parsed.name();
        if next_names.contains(replica_name) {
            continue;
        }

        let settings_path = state
            .manager
            .base_path
            .join(replica_name)
            .join("settings.json");
        if !settings_path.exists() {
            continue;
        }

        let mut settings = IndexSettings::load(&settings_path)?;
        if settings.primary.as_deref() == Some(primary_index_name) {
            settings.primary = None;
            settings.save(&settings_path)?;
            state.manager.invalidate_settings_cache(replica_name);
            state.manager.invalidate_facet_cache(replica_name);
        }
    }
    Ok(())
}

/// Mirror add/update writes from primary to all configured standard replicas.
pub(crate) async fn sync_add_documents_to_standard_replicas(
    state: &Arc<AppState>,
    primary_index_name: &str,
    documents: &[Document],
) -> Result<(), FlapjackError> {
    if documents.is_empty() {
        return Ok(());
    }

    let replica_names = standard_replicas_for_primary(state, primary_index_name)?;
    for replica_name in replica_names {
        state.manager.create_tenant(&replica_name)?;
        state
            .manager
            .add_documents_sync(&replica_name, documents.to_vec())
            .await?;
    }
    Ok(())
}

/// Mirror deletes from primary to all configured standard replicas.
pub(crate) async fn sync_delete_documents_to_standard_replicas(
    state: &Arc<AppState>,
    primary_index_name: &str,
    object_ids: &[String],
) -> Result<(), FlapjackError> {
    if object_ids.is_empty() {
        return Ok(());
    }

    let replica_names = standard_replicas_for_primary(state, primary_index_name)?;
    for replica_name in replica_names {
        state.manager.create_tenant(&replica_name)?;
        state
            .manager
            .delete_documents_sync(&replica_name, object_ids.to_vec())
            .await?;
    }
    Ok(())
}
