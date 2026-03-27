use std::path::Path;
use std::sync::Arc;

use crate::background_tasks::spawn_background_tasks;
use crate::router::build_router;
use crate::server_init::{
    initialize_infrastructure, initialize_state, log_startup_summary, StartupSummary,
};
use crate::startup::{
    cors_origins_from_env, initialize_key_store, load_server_config, print_startup_banner,
    shutdown_signal, shutdown_timeout_secs_from_env, AuthStatus, CorsMode,
};

/// TODO: Document serve.
pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let startup_start = std::time::Instant::now();

    let server_config = load_server_config();
    let cors_mode = cors_origins_from_env();
    let shutdown_timeout_secs = shutdown_timeout_secs_from_env();
    let data_dir = Path::new(&server_config.data_dir);
    let (key_store, admin_key, key_is_new) = initialize_key_store(&server_config, data_dir);

    let infrastructure =
        initialize_infrastructure(&server_config, data_dir, admin_key.clone()).await?;
    tracing::info!(
        env_mode = %server_config.env_mode,
        replication_peers = infrastructure.node_config.peers.len(),
        trusted_proxy_ranges = infrastructure.trusted_proxy_matcher.len(),
        "Server infrastructure initialized"
    );
    match &cors_mode {
        CorsMode::Permissive => tracing::info!("CORS: permissive (all origins)"),
        CorsMode::Restricted(origins) => {
            let configured_origins = origins
                .iter()
                .filter_map(|origin| origin.to_str().ok())
                .collect::<Vec<_>>()
                .join(", ");
            tracing::info!("CORS: restricted to [{}]", configured_origins);
        }
    }
    log_startup_summary(&StartupSummary::from_infrastructure(
        &infrastructure,
        !server_config.no_auth,
    ));

    let state = initialize_state(
        &infrastructure,
        key_store.clone(),
        &server_config.data_dir,
        startup_start,
    )?;

    // Pre-serve barrier: catch up from peers before accepting traffic so a
    // rebooted node never serves stale data.
    crate::startup_catchup::run_pre_serve_catchup(&state)
        .await
        .map_err(std::io::Error::other)?;

    spawn_background_tasks(&state, &infrastructure);

    let app = build_router(
        Arc::clone(&state),
        key_store,
        Arc::clone(&infrastructure.analytics_collector),
        Arc::clone(&infrastructure.trusted_proxy_matcher),
        cors_mode,
        data_dir,
    );

    let listener = tokio::net::TcpListener::bind(&infrastructure.bind_addr).await?;
    let auth_status = resolve_auth_status(&server_config, key_is_new, admin_key);
    print_startup_banner(
        &listener.local_addr()?.to_string(),
        auth_status,
        startup_start.elapsed().as_millis(),
        &server_config.data_dir,
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    let _ = run_graceful_shutdown(&infrastructure, &state, shutdown_timeout_secs).await;
    Ok(())
}

fn resolve_auth_status(
    config: &crate::startup::ServerConfig,
    key_is_new: bool,
    admin_key: Option<String>,
) -> AuthStatus {
    if config.no_auth {
        AuthStatus::Disabled
    } else if key_is_new {
        AuthStatus::NewKey(admin_key.unwrap_or_default())
    } else {
        AuthStatus::KeyInFile
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownWaitOutcome {
    Drained,
    TimedOut,
}

/// TODO: Document flush_then_wait_for_manager_shutdown.
async fn flush_then_wait_for_manager_shutdown<FlushFn, ShutdownFuture>(
    shutdown_timeout_secs: u64,
    flush_analytics: FlushFn,
    manager_shutdown: ShutdownFuture,
) -> ShutdownWaitOutcome
where
    FlushFn: FnOnce(),
    ShutdownFuture: std::future::Future<Output = ()>,
{
    flush_analytics();

    match tokio::time::timeout(
        tokio::time::Duration::from_secs(shutdown_timeout_secs),
        manager_shutdown,
    )
    .await
    {
        Ok(()) => ShutdownWaitOutcome::Drained,
        Err(_) => ShutdownWaitOutcome::TimedOut,
    }
}

/// TODO: Document run_graceful_shutdown.
async fn run_graceful_shutdown(
    infrastructure: &crate::server_init::InfrastructureState,
    state: &Arc<crate::handlers::AppState>,
    shutdown_timeout_secs: u64,
) -> ShutdownWaitOutcome {
    tracing::info!(
        timeout_env_var = "FLAPJACK_SHUTDOWN_TIMEOUT_SECS",
        timeout_secs = shutdown_timeout_secs,
        "[shutdown] Server stopped accepting connections, cleaning up..."
    );

    let outcome = flush_then_wait_for_manager_shutdown(
        shutdown_timeout_secs,
        || flush_analytics_on_shutdown(infrastructure),
        state.manager.graceful_shutdown(),
    )
    .await;

    match outcome {
        ShutdownWaitOutcome::Drained => {
            tracing::info!(
                timeout_env_var = "FLAPJACK_SHUTDOWN_TIMEOUT_SECS",
                timeout_secs = shutdown_timeout_secs,
                "[shutdown] All write queues drained before deadline"
            );
        }
        ShutdownWaitOutcome::TimedOut => {
            tracing::warn!(
                timeout_env_var = "FLAPJACK_SHUTDOWN_TIMEOUT_SECS",
                timeout_secs = shutdown_timeout_secs,
                "[shutdown] Write queue drain timed out; forcing exit may drop queued writes (data-loss risk)"
            );
        }
    }

    outcome
}

fn flush_analytics_on_shutdown(infrastructure: &crate::server_init::InfrastructureState) {
    if infrastructure.analytics_config.enabled {
        infrastructure.analytics_collector.shutdown();
        tracing::info!("[shutdown] Analytics buffers flushed");
    }
}

#[cfg(test)]
mod tests {
    use super::{flush_then_wait_for_manager_shutdown, ShutdownWaitOutcome};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// TODO: Document shutdown_wait_helper_returns_drained_when_manager_completes_before_deadline.
    #[tokio::test]
    async fn shutdown_wait_helper_returns_drained_when_manager_completes_before_deadline() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let flush_events = Arc::clone(&events);
        let manager_events = Arc::clone(&events);

        let outcome = flush_then_wait_for_manager_shutdown(
            1,
            move || flush_events.lock().unwrap().push("analytics-flushed"),
            async move {
                manager_events.lock().unwrap().push("manager-wait-begins");
                tokio::time::sleep(Duration::from_millis(10)).await;
            },
        )
        .await;

        assert_eq!(outcome, ShutdownWaitOutcome::Drained);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["analytics-flushed", "manager-wait-begins"]
        );
    }

    /// TODO: Document shutdown_wait_helper_returns_timed_out_when_manager_exceeds_deadline.
    #[tokio::test]
    async fn shutdown_wait_helper_returns_timed_out_when_manager_exceeds_deadline() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let flush_events = Arc::clone(&events);
        let manager_events = Arc::clone(&events);

        let outcome = flush_then_wait_for_manager_shutdown(
            1,
            move || flush_events.lock().unwrap().push("analytics-flushed"),
            async move {
                manager_events.lock().unwrap().push("manager-wait-begins");
                tokio::time::sleep(Duration::from_secs(5)).await;
            },
        )
        .await;

        assert_eq!(outcome, ShutdownWaitOutcome::TimedOut);
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["analytics-flushed", "manager-wait-begins"]
        );
    }
}
