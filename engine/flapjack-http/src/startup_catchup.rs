use crate::handlers::internal::apply_ops_to_manager;
use crate::handlers::AppState;
use flapjack::index::oplog::read_committed_seq;
use flapjack::index::snapshot::import_from_bytes;
use flapjack_replication::types::GetOpsResponse;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Legacy helper that triggers delayed startup catch-up in the background.
/// Production bootstrap now uses `run_pre_serve_catchup` before serving.
pub fn spawn_startup_catchup(state: Arc<AppState>) {
    tokio::spawn(async move {
        run_startup_catchup(state).await;
    });
}

/// Legacy delayed startup catch-up path. Public for testing.
pub async fn run_startup_catchup(state: Arc<AppState>) {
    if state.replication_manager.is_none() {
        return; // Standalone mode — nothing to do
    }
    delayed_catchup_from_peers(&state).await;
}

/// Sleep briefly then run best-effort catch-up from peers. Used by the legacy
/// delayed startup path so the server is already accepting requests before
/// catch-up traffic begins.
async fn delayed_catchup_from_peers(state: &AppState) {
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    tracing::info!("[REPL-catchup] Starting startup catch-up from peers");
    if let Err(error) = catchup_all_tenants(state, "REPL-catchup", false).await {
        tracing::debug!("[REPL-catchup] delayed startup catch-up skipped: {}", error);
    }
    tracing::info!("[REPL-catchup] Startup catch-up complete");
}

pub async fn run_pre_serve_catchup(state: &AppState) -> Result<(), String> {
    let has_peers = state
        .replication_manager
        .as_ref()
        .is_some_and(|manager| manager.peer_count() > 0);
    if !has_peers {
        return Ok(());
    }

    let timeout_secs: u64 = std::env::var("FLAPJACK_STARTUP_CATCHUP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let strict_bootstrap = startup_catchup_strict_bootstrap_enabled();
    let timeout = tokio::time::Duration::from_secs(timeout_secs);

    tracing::info!(
        "[REPL-catchup] Pre-serve catch-up starting (timeout={}s)",
        timeout_secs
    );
    if !strict_bootstrap {
        tracing::warn!(
            "[REPL-catchup] strict bootstrap disabled via FLAPJACK_STARTUP_CATCHUP_STRICT; node may start before all peers are reachable"
        );
    }

    execute_timed_catchup(state, timeout, timeout_secs).await?;
    wait_for_write_queues(state, timeout, timeout_secs).await
}

/// Run `catchup_all_tenants` with a timeout, returning a descriptive error
/// on catch-up failure or timeout.
async fn execute_timed_catchup(
    state: &AppState,
    timeout: tokio::time::Duration,
    timeout_secs: u64,
) -> Result<(), String> {
    match tokio::time::timeout(timeout, catchup_all_tenants(state, "REPL-catchup", true)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            tracing::error!("[REPL-catchup] Pre-serve catch-up failed: {}", error);
            Err(error)
        }
        Err(_) => {
            let error = format!(
                "pre-serve catch-up timed out after {}s; refusing to serve stale data",
                timeout_secs
            );
            tracing::error!("[REPL-catchup] {}", error);
            Err(error)
        }
    }
}

/// Wait for all write queues to commit enqueued catch-up ops before serving.
async fn wait_for_write_queues(
    state: &AppState,
    timeout: tokio::time::Duration,
    timeout_secs: u64,
) -> Result<(), String> {
    if state.manager.wait_for_pending_tasks(timeout).await {
        tracing::info!("[REPL-catchup] Pre-serve catch-up complete");
        Ok(())
    } else {
        let error = format!(
            "write queues did not drain within {}s after pre-serve catch-up",
            timeout_secs
        );
        tracing::error!("[REPL-catchup] {}", error);
        Err(error)
    }
}

fn startup_catchup_strict_bootstrap_enabled() -> bool {
    parse_strict_bootstrap_override(
        std::env::var("FLAPJACK_STARTUP_CATCHUP_STRICT")
            .ok()
            .as_deref(),
    )
}

fn parse_strict_bootstrap_override(raw_value: Option<&str>) -> bool {
    match raw_value
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("0") | Some("false") => false,
        _ => true,
    }
}

/// Run one round of catch-up from peers for all local tenants.
/// This is the core anti-entropy sync logic used by both startup catch-up
/// and the periodic background task (P0).
/// Public for testing.
pub async fn run_periodic_catchup(state: Arc<AppState>) {
    if state.replication_manager.is_none() {
        return;
    }
    if let Err(error) = catchup_all_tenants(&state, "REPL-sync", false).await {
        tracing::debug!("[REPL-sync] periodic catch-up skipped: {}", error);
    }
}

/// Spawn a background task that runs catch-up from peers on a timer.
/// This is the P0 fix for network partition recovery without restart.
/// Configurable via FLAPJACK_SYNC_INTERVAL_SECS (default 60).
pub fn spawn_periodic_sync(state: Arc<AppState>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
        // If a sync cycle runs longer than the interval, skip missed ticks
        // rather than bursting back-to-back syncs.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await; // skip first immediate tick

        loop {
            interval.tick().await;
            tracing::debug!("[REPL-sync] Periodic sync starting");
            run_periodic_catchup(Arc::clone(&state)).await;
        }
    });
}

/// Core catch-up logic shared by startup and periodic sync.
/// Iterates tenant IDs discovered locally and from peers, compares local oplog
/// sequence against peers, and pulls any missed ops.
async fn catchup_all_tenants(
    state: &AppState,
    log_prefix: &str,
    strict_bootstrap: bool,
) -> Result<(), String> {
    let repl_mgr = match &state.replication_manager {
        Some(r) => Arc::clone(r),
        None => return Ok(()),
    };
    if repl_mgr.peer_count() == 0 {
        return Ok(());
    }

    let mut tenant_ids: BTreeSet<String> =
        match crate::tenant_dirs::visible_tenant_dir_names(&state.manager.base_path) {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                let error = format!("cannot read data dir for startup catch-up: {e}");
                if strict_bootstrap {
                    return Err(error);
                }
                tracing::warn!("[{}] {}", log_prefix, error);
                return Ok(());
            }
        };
    let peer_tenants = if strict_bootstrap {
        repl_mgr.discover_tenants_from_peers_strict().await?
    } else {
        repl_mgr.discover_tenants_from_peers().await
    };
    tenant_ids.extend(peer_tenants);

    for tenant_id in tenant_ids {
        if let Err(error) = validate_tenant_id(&tenant_id) {
            if strict_bootstrap {
                return Err(format!("[{}] {}", log_prefix, error));
            }
            tracing::warn!("[{}] skipping invalid tenant id: {}", log_prefix, error);
            continue;
        }
        catchup_single_tenant(state, &repl_mgr, &tenant_id, log_prefix, strict_bootstrap).await?;
    }

    Ok(())
}

fn retention_gap_detected(local_seq: u64, response: &GetOpsResponse) -> bool {
    if local_seq == 0 {
        return false;
    }

    response
        .oldest_retained_seq
        .is_some_and(|oldest_retained| local_seq < oldest_retained)
}

/// Downloads a snapshot from a replication peer and installs it as the tenant's
/// data directory, used during startup catchup when the local tenant is missing or stale.
async fn restore_tenant_from_snapshot(
    state: &AppState,
    repl_mgr: &Arc<flapjack_replication::manager::ReplicationManager>,
    tenant_id: &str,
    log_prefix: &str,
) -> Result<(), String> {
    let snapshot_bytes = match repl_mgr.download_snapshot_from_peer(tenant_id).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(format!(
                "[{}] failed to download snapshot for tenant '{}': {}",
                log_prefix, tenant_id, error
            ));
        }
    };

    if let Err(error) = install_snapshot_bytes(&state.manager, tenant_id, &snapshot_bytes) {
        return Err(format!(
            "[{}] failed to install snapshot for tenant '{}': {}",
            log_prefix, tenant_id, error
        ));
    }

    tracing::info!(
        "[{}] Restored tenant '{}' from peer snapshot ({} bytes)",
        log_prefix,
        tenant_id,
        snapshot_bytes.len()
    );
    Ok(())
}

/// Atomically installs a compressed snapshot as a tenant's data directory: extracts
/// to a staging path, backs up the existing tenant dir, then renames staging into place.
pub(crate) fn install_snapshot_bytes(
    manager: &flapjack::IndexManager,
    tenant_id: &str,
    snapshot_bytes: &[u8],
) -> Result<(), String> {
    validate_tenant_id(tenant_id)?;
    let tenant_path = manager.base_path.join(tenant_id);
    let staging_path = manager
        .base_path
        .join(format!(".{tenant_id}.snapshot_restore_staging"));
    let backup_path = manager
        .base_path
        .join(format!(".{tenant_id}.snapshot_restore_backup"));

    remove_path_if_exists(&staging_path)?;
    recover_interrupted_snapshot_restore(&tenant_path, &backup_path)?;

    if let Err(error) = import_from_bytes(snapshot_bytes, &staging_path) {
        let _ = remove_path_if_exists(&staging_path);
        return Err(format!(
            "import snapshot bytes into staging failed: {error}"
        ));
    }

    manager.unload_tenant(tenant_id);
    let tenant_existed = tenant_path.exists();
    if tenant_existed {
        std::fs::rename(&tenant_path, &backup_path).map_err(|error| {
            let _ = remove_path_if_exists(&staging_path);
            format!(
                "move existing tenant dir '{}' to backup '{}' failed: {}",
                tenant_path.display(),
                backup_path.display(),
                error
            )
        })?;
    }

    if let Err(error) = std::fs::rename(&staging_path, &tenant_path) {
        let _ = remove_path_if_exists(&staging_path);
        if tenant_existed {
            let _ = std::fs::rename(&backup_path, &tenant_path);
        }
        return Err(format!(
            "activate staged snapshot '{}' -> '{}' failed: {}",
            staging_path.display(),
            tenant_path.display(),
            error
        ));
    }

    if tenant_existed {
        let _ = remove_path_if_exists(&backup_path);
    }
    manager.unload_tenant(tenant_id);
    Ok(())
}

fn validate_tenant_id(tenant_id: &str) -> Result<(), String> {
    flapjack::validate_index_name(tenant_id)
        .map_err(|error| format!("invalid tenant id '{}': {}", tenant_id, error))
}
/// Recovers from a crash during snapshot restore: if a backup dir exists but the
/// tenant dir is missing, restores from the backup to avoid data loss.
fn recover_interrupted_snapshot_restore(
    tenant_path: &std::path::Path,
    backup_path: &std::path::Path,
) -> Result<(), String> {
    if !backup_path.exists() {
        return Ok(());
    }

    if tenant_path.exists() {
        return remove_path_if_exists(backup_path);
    }

    std::fs::rename(backup_path, tenant_path).map_err(|error| {
        format!(
            "restore interrupted snapshot backup '{}' -> '{}' failed: {}",
            backup_path.display(),
            tenant_path.display(),
            error
        )
    })
}

fn remove_path_if_exists(path: &std::path::Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_dir() {
        std::fs::remove_dir_all(path)
            .map_err(|error| format!("remove dir '{}' failed: {}", path.display(), error))
    } else {
        std::fs::remove_file(path)
            .map_err(|error| format!("remove file '{}' failed: {}", path.display(), error))
    }
}

/// Handle a fetch-ops failure: strict mode returns an error to abort catch-up,
/// lenient mode logs and returns `Ok(())` so the caller can skip this tenant.
fn handle_fetch_error(
    error: String,
    log_prefix: &str,
    tenant_id: &str,
    strict_bootstrap: bool,
) -> Result<(), String> {
    if strict_bootstrap {
        Err(format!(
            "[{}] failed to fetch missed ops for tenant '{}': {}",
            log_prefix, tenant_id, error
        ))
    } else {
        tracing::debug!(
            "[{}] Could not reach peer for '{}': {}",
            log_prefix,
            tenant_id,
            error
        );
        Ok(())
    }
}

/// Catches up a single tenant by fetching missed oplog entries from a replication
/// peer, falling back to full snapshot download if the peer's log has been truncated.
async fn catchup_single_tenant(
    state: &AppState,
    repl_mgr: &Arc<flapjack_replication::manager::ReplicationManager>,
    tenant_id: &str,
    log_prefix: &str,
    strict_bootstrap: bool,
) -> Result<(), String> {
    validate_tenant_id(tenant_id).map_err(|error| format!("[{}] {}", log_prefix, error))?;
    let tenant_path = state.manager.base_path.join(tenant_id);
    let local_seq = read_committed_seq(&tenant_path);
    let response = match fetch_missed_ops(repl_mgr, tenant_id, local_seq).await {
        Ok(response) => response,
        Err(error) => {
            return handle_fetch_error(error, log_prefix, tenant_id, strict_bootstrap);
        }
    };

    if retention_gap_detected(local_seq, &response) {
        tracing::warn!(
            "[{}] Tenant '{}' is behind retained peer history (local_seq={}, peer_seq={}, oldest_retained_seq={:?}); restoring full snapshot",
            log_prefix,
            tenant_id,
            local_seq,
            response.current_seq,
            response.oldest_retained_seq
        );
        return restore_tenant_from_snapshot(state, repl_mgr, tenant_id, log_prefix).await;
    }

    if response.ops.is_empty() {
        tracing::debug!("[{}] Tenant '{}' is up-to-date", log_prefix, tenant_id);
        return Ok(());
    }

    tracing::info!(
        "[{}] {} missed ops for tenant '{}' (local_seq={})",
        log_prefix,
        response.ops.len(),
        tenant_id,
        local_seq
    );
    apply_and_log_ops(&state.manager, tenant_id, &response.ops, log_prefix).await
}

async fn fetch_missed_ops(
    repl_mgr: &Arc<flapjack_replication::manager::ReplicationManager>,
    tenant_id: &str,
    local_seq: u64,
) -> Result<GetOpsResponse, String> {
    repl_mgr
        .catch_up_from_peer_with_metadata(tenant_id, local_seq)
        .await
}

/// Applies a batch of oplog entries to a tenant via the index manager and logs
/// the resulting sequence number or error.
async fn apply_and_log_ops(
    manager: &flapjack::IndexManager,
    tenant_id: &str,
    ops: &[flapjack::index::oplog::OpLogEntry],
    log_prefix: &str,
) -> Result<(), String> {
    match apply_ops_to_manager(manager, tenant_id, ops).await {
        Ok(seq) => {
            tracing::info!(
                "[{}] Applied ops up to seq {} for tenant '{}'",
                log_prefix,
                seq,
                tenant_id
            );
            Ok(())
        }
        Err(error) => Err(format!(
            "[{}] failed to apply ops for '{}': {}",
            log_prefix, tenant_id, error
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{install_snapshot_bytes, parse_strict_bootstrap_override, retention_gap_detected};
    use flapjack::index::snapshot::export_to_bytes;
    use flapjack_replication::types::GetOpsResponse;
    use tempfile::TempDir;

    #[test]
    fn strict_bootstrap_override_defaults_true() {
        assert!(parse_strict_bootstrap_override(None));
    }

    #[test]
    fn strict_bootstrap_override_accepts_false_values() {
        assert!(!parse_strict_bootstrap_override(Some("0")));
        assert!(!parse_strict_bootstrap_override(Some("false")));
        assert!(!parse_strict_bootstrap_override(Some("FALSE")));
    }

    #[test]
    fn strict_bootstrap_override_keeps_true_for_other_values() {
        assert!(parse_strict_bootstrap_override(Some("1")));
        assert!(parse_strict_bootstrap_override(Some("true")));
        assert!(parse_strict_bootstrap_override(Some("unexpected")));
    }

    #[test]
    fn retention_gap_true_when_local_seq_before_oldest_retained() {
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: Vec::new(),
            current_seq: 250,
            oldest_retained_seq: Some(200),
        };
        assert!(retention_gap_detected(150, &response));
    }

    #[test]
    fn retention_gap_false_for_brand_new_replica() {
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: Vec::new(),
            current_seq: 250,
            oldest_retained_seq: Some(200),
        };
        assert!(!retention_gap_detected(0, &response));
    }

    #[test]
    fn retention_gap_false_when_metadata_missing() {
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: Vec::new(),
            current_seq: 25,
            oldest_retained_seq: None,
        };
        assert!(!retention_gap_detected(10, &response));
    }

    #[test]
    fn retention_gap_false_when_local_seq_equals_oldest_retained() {
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: Vec::new(),
            current_seq: 200,
            oldest_retained_seq: Some(200),
        };
        // local_seq == oldest_retained means we are exactly caught up
        // to the oldest retained entry — not a gap.
        assert!(!retention_gap_detected(200, &response));
    }

    #[test]
    fn retention_gap_false_when_local_seq_above_oldest_retained() {
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: Vec::new(),
            current_seq: 300,
            oldest_retained_seq: Some(200),
        };
        // local_seq > oldest_retained means we are ahead of the
        // oldest retained entry — definitely not a gap.
        assert!(!retention_gap_detected(250, &response));
    }
    #[test]
    fn retention_gap_true_even_when_ops_present() {
        let dummy_op = flapjack::index::oplog::OpLogEntry {
            seq: 201,
            tenant_id: "t1".to_string(),
            node_id: "n1".to_string(),
            op_type: "upsert".to_string(),
            payload: serde_json::json!({"x": 1}),
            timestamp_ms: 0,
        };
        let response = GetOpsResponse {
            tenant_id: "t1".to_string(),
            ops: vec![dummy_op],
            current_seq: 300,
            oldest_retained_seq: Some(200),
        };
        // Even when partial ops are returned (from oldest_retained
        // onwards), the gap is real — ops 151-199 are missing and a
        // full snapshot restore is needed.
        assert!(retention_gap_detected(150, &response));
    }
    #[tokio::test]
    async fn install_snapshot_bytes_keeps_existing_tenant_on_invalid_snapshot() {
        let tmp = TempDir::new().unwrap();
        let manager = flapjack::IndexManager::new(tmp.path());
        let tenant_id = "restore_target";
        let tenant_path = manager.base_path.join(tenant_id);
        std::fs::create_dir_all(&tenant_path).unwrap();
        let marker_path = tenant_path.join("marker.txt");
        std::fs::write(&marker_path, "keep-me").unwrap();

        let result = install_snapshot_bytes(&manager, tenant_id, b"not-a-valid-snapshot");
        assert!(result.is_err(), "invalid snapshot import should fail");
        assert!(
            marker_path.exists(),
            "existing tenant data should remain if snapshot import fails"
        );
    }
    #[tokio::test]
    async fn install_snapshot_bytes_replaces_existing_tenant_on_valid_snapshot() {
        let tmp = TempDir::new().unwrap();
        let manager = flapjack::IndexManager::new(tmp.path());
        let tenant_id = "restore_target";
        let tenant_path = manager.base_path.join(tenant_id);
        std::fs::create_dir_all(&tenant_path).unwrap();
        let old_marker = tenant_path.join("old.txt");
        std::fs::write(&old_marker, "old").unwrap();

        let snapshot_src = TempDir::new().unwrap();
        let new_marker_name = "new.txt";
        std::fs::write(snapshot_src.path().join(new_marker_name), "new").unwrap();
        let snapshot_bytes = export_to_bytes(snapshot_src.path()).unwrap();

        let result = install_snapshot_bytes(&manager, tenant_id, &snapshot_bytes);
        assert!(result.is_ok(), "valid snapshot install should succeed");
        assert!(
            !old_marker.exists(),
            "old tenant data should be replaced after successful snapshot install"
        );
        assert!(
            tenant_path.join(new_marker_name).exists(),
            "restored tenant should contain snapshot content"
        );
    }
    #[tokio::test]
    async fn install_snapshot_bytes_restores_backup_before_retrying_failed_snapshot() {
        let tmp = TempDir::new().unwrap();
        let manager = flapjack::IndexManager::new(tmp.path());
        let tenant_id = "restore_target";
        let tenant_path = manager.base_path.join(tenant_id);
        let backup_path = manager
            .base_path
            .join(format!(".{tenant_id}.snapshot_restore_backup"));
        std::fs::create_dir_all(&backup_path).unwrap();
        let marker_path = backup_path.join("marker.txt");
        std::fs::write(&marker_path, "keep-me").unwrap();

        let result = install_snapshot_bytes(&manager, tenant_id, b"not-a-valid-snapshot");
        assert!(result.is_err(), "invalid snapshot import should fail");
        assert!(
            tenant_path.join("marker.txt").exists(),
            "retry should restore the interrupted backup before attempting a new snapshot install"
        );
        assert!(
            !backup_path.exists(),
            "restored backup should be moved back to the active tenant path"
        );
    }
    #[tokio::test]
    async fn install_snapshot_bytes_rejects_path_traversal_tenant_id() {
        let tmp = TempDir::new().unwrap();
        let manager = flapjack::IndexManager::new(tmp.path());
        let victim = TempDir::new().unwrap();
        let victim_name = victim
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let marker = victim.path().join("marker.txt");
        std::fs::write(&marker, "keep-me").unwrap();

        let snapshot_src = TempDir::new().unwrap();
        std::fs::write(snapshot_src.path().join("new.txt"), "new").unwrap();
        let snapshot_bytes = export_to_bytes(snapshot_src.path()).unwrap();

        let malicious_tenant_id = format!("../{}", victim_name);
        let result = install_snapshot_bytes(&manager, &malicious_tenant_id, &snapshot_bytes);
        assert!(result.is_err(), "path traversal tenant id must be rejected");
        assert!(
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.contains("invalid tenant id")),
            "error should identify invalid tenant id, got: {:?}",
            result
        );
        assert!(
            marker.exists(),
            "rejecting traversal tenant id must not modify sibling paths"
        );
        assert_eq!(
            std::fs::read_to_string(marker).unwrap(),
            "keep-me",
            "victim marker content must remain unchanged"
        );
    }
}
