use crate::handlers::AppState;
use crate::server_init::InfrastructureState;
use crate::tenant_dirs::{has_visible_tenant_dirs, visible_tenant_dir_names};
use std::sync::Arc;

/// Restore all tenant indexes from S3 snapshots when the data directory is empty.
///
/// Skip restoration if any tenant subdirectory already exists. Otherwise, list
/// all `snapshots/<tenant>/` prefixes in the configured S3 bucket, download the
/// latest snapshot for each tenant, and import it into the local data directory.
/// Failures for individual tenants are logged but do not abort the remaining
/// restores.
pub(crate) async fn auto_restore_from_s3(
    data_dir: &str,
    s3_config: &flapjack::index::s3::S3Config,
    _manager: &std::sync::Arc<flapjack::IndexManager>,
) {
    let data_path = std::path::Path::new(data_dir);
    let has_tenants = has_visible_tenant_dirs(data_path).unwrap_or(false);
    if has_tenants {
        tracing::info!("Data dir has existing tenants, skipping S3 auto-restore");
        return;
    }

    tracing::info!("Empty data dir detected, attempting S3 auto-restore...");
    let tenant_ids = match list_s3_tenant_snapshots(s3_config).await {
        Some(ids) => ids,
        None => return,
    };

    for tid in &tenant_ids {
        restore_tenant_from_s3(s3_config, tid, data_path).await;
    }
}

async fn list_s3_tenant_snapshots(
    s3_config: &flapjack::index::s3::S3Config,
) -> Option<Vec<String>> {
    let tenant_ids = fetch_s3_tenant_prefixes(s3_config).await?;
    if tenant_ids.is_empty() {
        tracing::info!("S3 auto-restore: no snapshots found");
        return None;
    }
    tracing::info!(
        "S3 auto-restore: found {} tenants: {:?}",
        tenant_ids.len(),
        tenant_ids
    );
    Some(tenant_ids)
}

/// Fetches tenant prefixes from S3 for discovery of remotely-backed tenants.
async fn fetch_s3_tenant_prefixes(
    s3_config: &flapjack::index::s3::S3Config,
) -> Option<Vec<String>> {
    let bucket = s3_config
        .clone()
        .bucket_internal()
        .map_err(|e| {
            tracing::warn!("S3 auto-restore: couldn't create bucket client: {}", e);
        })
        .ok()?;
    let results = bucket
        .list("snapshots/".to_string(), Some("/".to_string()))
        .await
        .map_err(|e| tracing::warn!("S3 auto-restore: list failed: {}", e))
        .ok()?;
    let mut ids: Vec<String> = results
        .iter()
        .flat_map(|r| r.common_prefixes.iter().flatten())
        .filter_map(|p| {
            p.prefix
                .strip_prefix("snapshots/")
                .and_then(|s| s.strip_suffix("/"))
                .map(|s| s.to_string())
        })
        .collect();
    ids.sort();
    ids.dedup();
    Some(ids)
}

/// Downloads and imports the latest S3 snapshot for a tenant during startup,
/// logging errors but not failing the boot sequence.
async fn restore_tenant_from_s3(
    s3_config: &flapjack::index::s3::S3Config,
    tid: &str,
    data_path: &std::path::Path,
) {
    match flapjack::index::s3::download_latest_snapshot(s3_config, tid).await {
        Ok((key, data)) => {
            let index_path = data_path.join(tid);
            if let Err(e) = flapjack::index::snapshot::import_from_bytes(&data, &index_path) {
                tracing::error!("S3 auto-restore: failed to import {}: {}", tid, e);
                return;
            }
            tracing::info!(
                "S3 auto-restore: restored {} from {} ({} bytes)",
                tid,
                key,
                data.len()
            );
        }
        Err(e) => tracing::warn!("S3 auto-restore: no snapshot for {}: {}", tid, e),
    }
}

/// Run an infinite loop that periodically snapshots every tenant index to S3.
pub(crate) async fn scheduled_s3_backups(
    data_dir: String,
    s3_config: flapjack::index::s3::S3Config,
    _manager: std::sync::Arc<flapjack::IndexManager>,
    interval_secs: u64,
) {
    let data_path = std::path::PathBuf::from(data_dir);
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
    interval.tick().await;
    loop {
        interval.tick().await;
        run_scheduled_s3_backup_pass(&s3_config, data_path.as_path()).await;
    }
}

/// Runs a single scheduled S3 backup pass: iterates all tenant directories and
/// uploads a fresh snapshot for each to the configured S3 bucket.
async fn run_scheduled_s3_backup_pass(
    s3_config: &flapjack::index::s3::S3Config,
    data_path: &std::path::Path,
) {
    tracing::info!("[BACKUP] Starting scheduled S3 snapshot...");

    let tenant_dirs = match visible_tenant_dir_names(data_path) {
        Ok(dirs) => dirs,
        Err(error) => {
            tracing::error!("[BACKUP] Failed to read data dir: {}", error);
            return;
        }
    };

    for tenant in &tenant_dirs {
        backup_tenant_to_s3(s3_config, tenant, data_path).await;
    }

    tracing::info!(
        "[BACKUP] Scheduled snapshot complete ({} tenants)",
        tenant_dirs.len()
    );
}

/// Backs up a single tenant's index data to S3 via snapshot.
async fn backup_tenant_to_s3(
    s3_config: &flapjack::index::s3::S3Config,
    tenant: &str,
    data_path: &std::path::Path,
) {
    let bytes = match export_tenant_snapshot(tenant, data_path) {
        Some(b) => b,
        None => return,
    };
    match flapjack::index::s3::upload_snapshot(s3_config, tenant, &bytes).await {
        Ok(key) => {
            enforce_backup_retention(s3_config, tenant).await;
            tracing::info!("[BACKUP] {} -> {} ({} bytes)", tenant, key, bytes.len());
        }
        Err(e) => tracing::error!("[BACKUP] upload {} failed: {}", tenant, e),
    }
}

fn export_tenant_snapshot(tenant: &str, data_path: &std::path::Path) -> Option<Vec<u8>> {
    let index_path = data_path.join(tenant);
    match flapjack::index::snapshot::export_to_bytes(&index_path) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::error!("[BACKUP] export {} failed: {}", tenant, e);
            None
        }
    }
}

async fn enforce_backup_retention(s3_config: &flapjack::index::s3::S3Config, tenant: &str) {
    let retention = std::env::var("FLAPJACK_SNAPSHOT_RETENTION")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(24);
    let _ = flapjack::index::s3::enforce_retention(s3_config, tenant, retention).await;
}

/// Spawn all server background tasks, including analytics, replication, and maintenance loops.
pub(crate) fn spawn_background_tasks(state: &Arc<AppState>, infrastructure: &InfrastructureState) {
    spawn_ssl_renewal(infrastructure);
    spawn_analytics_tasks(infrastructure);
    spawn_s3_backup_task(infrastructure);
    spawn_replication_tasks(state, infrastructure);
    spawn_usage_rollup_task(state);
    spawn_metrics_refresh_task(state);
    spawn_usage_alert_task(state);
}

fn spawn_ssl_renewal(infrastructure: &InfrastructureState) {
    if let Some(ssl_manager) = infrastructure.ssl_manager.as_ref() {
        let ssl_manager = Arc::clone(ssl_manager);
        tokio::spawn(async move { ssl_manager.start_renewal_loop().await });
        tracing::info!("[SSL] Auto-renewal enabled (checks every 24h)");
    }
}

/// Spawns background tasks for analytics rollup and retention cleanup.
fn spawn_analytics_tasks(infrastructure: &InfrastructureState) {
    if !infrastructure.analytics_config.enabled {
        tracing::info!("[analytics] Analytics disabled");
        return;
    }

    let collector = Arc::clone(&infrastructure.analytics_collector);
    tokio::spawn(async move { collector.run_flush_loop().await });

    let retention_dir = infrastructure.analytics_config.data_dir.clone();
    let retention_days = infrastructure.analytics_config.retention_days;
    tokio::spawn(async move {
        flapjack::analytics::retention::run_retention_loop(retention_dir, retention_days).await;
    });

    tracing::info!(
        "[analytics] Analytics enabled (flush every {}s, retain {}d)",
        infrastructure.analytics_config.flush_interval_secs,
        infrastructure.analytics_config.retention_days
    );

    if let Some(cluster) = crate::analytics_cluster::get_global_cluster() {
        let rollup_interval_secs = std::env::var("FLAPJACK_ROLLUP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let local_node_id = cluster.node_id().to_string();
        crate::rollup_broadcaster::spawn_rollup_broadcaster(
            Arc::clone(&infrastructure.analytics_engine),
            infrastructure.analytics_config.clone(),
            cluster,
            local_node_id,
            rollup_interval_secs,
        );
        tracing::info!(
            "[ROLLUP-BROADCAST] Broadcaster started (interval={}s)",
            rollup_interval_secs
        );
    }
}

/// Spawns a periodic S3 snapshot backup task for all tenants.
fn spawn_s3_backup_task(infrastructure: &InfrastructureState) {
    if let Some(s3_config) = infrastructure.s3_config.as_ref() {
        if let Some(interval_secs) = infrastructure.s3_snapshot_interval_secs {
            let data_dir = infrastructure
                .manager
                .base_path
                .to_string_lossy()
                .to_string();
            let manager = Arc::clone(&infrastructure.manager);
            let config = s3_config.clone();
            tokio::spawn(async move {
                scheduled_s3_backups(data_dir, config, manager, interval_secs).await;
            });
            tracing::info!("Scheduled S3 backups every {}s", interval_secs);
        }
    }
}

/// Spawns replication health probe and periodic peer-sync tasks.
fn spawn_replication_tasks(state: &Arc<AppState>, infrastructure: &InfrastructureState) {
    if let Some(replication_manager) = infrastructure.replication_manager.as_ref() {
        replication_manager.start_health_probe(10);
        // NOTE: One-shot startup catch-up moved to server.rs as a pre-serve barrier
        // (run_pre_serve_catchup). Only periodic sync remains as a background task.
        let sync_interval: u64 = std::env::var("FLAPJACK_SYNC_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);
        crate::startup_catchup::spawn_periodic_sync(Arc::clone(state), sync_interval);
        tracing::info!(
            "[REPL-sync] Periodic anti-entropy sync enabled (interval={}s)",
            sync_interval
        );
    }
}

/// Spawns a periodic task to flush in-memory usage counters to disk.
fn spawn_usage_rollup_task(state: &Arc<AppState>) {
    if let Some(persistence) = state.usage_persistence.clone() {
        let counters = Arc::clone(&state.usage_counters);
        tokio::spawn(async move {
            loop {
                let now = chrono::Utc::now();
                let tomorrow = (now + chrono::Duration::days(1))
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc();
                let secs_until_midnight = (tomorrow - now).num_seconds().max(1) as u64;
                tokio::time::sleep(tokio::time::Duration::from_secs(secs_until_midnight)).await;

                let rollup_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
                match persistence.rollup(&rollup_date, &counters) {
                    Ok(()) => {
                        tracing::info!("[usage] Daily rollup complete (date={})", rollup_date)
                    }
                    Err(e) => {
                        tracing::error!("[usage] Daily rollup failed (date={}): {}", rollup_date, e)
                    }
                }
            }
        });
    }
}

/// Spawns a periodic task to refresh per-index Prometheus metric gauges.
fn spawn_metrics_refresh_task(state: &Arc<AppState>) {
    let manager = Arc::clone(&state.manager);
    if let Some(ms) = state.metrics_state.clone() {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let storage = manager.all_tenant_storage();
                ms.storage_gauges.clear();
                for (tenant, bytes) in storage {
                    ms.storage_gauges.insert(tenant, bytes);
                }
            }
        });
    }
}

/// Spawns a periodic task to check usage thresholds and send alert notifications.
fn spawn_usage_alert_task(state: &Arc<AppState>) {
    let search_threshold: u64 = std::env::var("FLAPJACK_USAGE_ALERT_THRESHOLD_SEARCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let write_threshold: u64 = std::env::var("FLAPJACK_USAGE_ALERT_THRESHOLD_WRITES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if search_threshold == 0 && write_threshold == 0 {
        return;
    }
    let counters = Arc::clone(&state.usage_counters);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Some(notifier) = crate::notifications::global_notifier() {
                crate::notifications::check_usage_thresholds(
                    notifier,
                    &counters,
                    search_threshold,
                    write_threshold,
                );
            }
        }
    });
    tracing::info!(
        "[notifications] Usage threshold alerts enabled (searches={}, writes={})",
        search_threshold,
        write_threshold
    );
}
