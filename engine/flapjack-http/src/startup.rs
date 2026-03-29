use axum::http::HeaderValue;
use fs2::FileExt;
use std::fs::OpenOptions;
use std::path::Path;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use crate::admin_key_persistence::{
    ensure_admin_key_permissions, persist_admin_key_file, PermissionFailureMode,
};
use crate::auth::{generate_admin_key, generate_hex_key, KeyStore};
use std::sync::Arc;

/// Controls log output format: human-readable text (default) or structured JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsMode {
    Permissive,
    Restricted(Vec<HeaderValue>),
}

const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Parse an optional raw string into a `LogFormat`.
/// Only the value `"json"` (case-insensitive) selects JSON mode;
/// all other values (including `None`, empty, or invalid) default to `Text`.
pub(crate) fn log_format_from_value(raw: Option<&str>) -> LogFormat {
    match raw {
        Some(val) if val.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

/// Read `FLAPJACK_LOG_FORMAT` from the environment and parse into `LogFormat`.
pub(crate) fn log_format_from_env() -> LogFormat {
    log_format_from_value(std::env::var("FLAPJACK_LOG_FORMAT").ok().as_deref())
}

/// Parse `FLAPJACK_ALLOWED_ORIGINS`-style values.
///
/// - `None`/empty/whitespace-only: permissive mode.
/// - comma-separated origins: restricted mode using trimmed, valid `HeaderValue`s.
/// - invalid/empty segments are ignored.
pub(crate) fn cors_origins_from_value(raw: Option<&str>) -> CorsMode {
    let origins = raw
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| HeaderValue::from_str(value).ok())
        .collect::<Vec<_>>();

    if origins.is_empty() {
        CorsMode::Permissive
    } else {
        CorsMode::Restricted(origins)
    }
}

/// Read `FLAPJACK_ALLOWED_ORIGINS` and parse into a typed CORS mode.
pub(crate) fn cors_origins_from_env() -> CorsMode {
    cors_origins_from_value(std::env::var("FLAPJACK_ALLOWED_ORIGINS").ok().as_deref())
}

/// Parse `FLAPJACK_SHUTDOWN_TIMEOUT_SECS`-style values.
///
/// Missing, empty, invalid, or non-positive values use the safe default of 30s.
pub(crate) fn shutdown_timeout_secs_from_value(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_SECS)
}

/// Read `FLAPJACK_SHUTDOWN_TIMEOUT_SECS` and parse into shutdown timeout seconds.
pub(crate) fn shutdown_timeout_secs_from_env() -> u64 {
    shutdown_timeout_secs_from_value(
        std::env::var("FLAPJACK_SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

fn build_log_layer_with_writer<S, W>(writer: W) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    match log_format_from_env() {
        LogFormat::Json => Box::new(tracing_subscriber::fmt::layer().json().with_writer(writer)),
        LogFormat::Text => Box::new(tracing_subscriber::fmt::layer().with_writer(writer)),
    }
}

fn build_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
}

fn install_global_tracing_dispatch(dispatch: tracing::Dispatch) {
    tracing::dispatcher::set_global_default(dispatch)
        .expect("global tracing subscriber already set");
}

/// Build a tracing subscriber `Dispatch` suitable for use with
/// `tracing::dispatcher::set_global_default`. Composes `EnvFilter` (from
/// `RUST_LOG`) and the fmt layer. Accepts a writer for testability.
#[cfg(not(feature = "otel"))]
pub(crate) fn build_tracing_subscriber<W>(make_writer: W) -> tracing::Dispatch
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    let subscriber = tracing_subscriber::registry()
        .with(build_env_filter())
        .with(build_log_layer_with_writer(make_writer));
    tracing::Dispatch::new(subscriber)
}

/// Build a tracing subscriber `Dispatch` with an optional OTEL layer composed in.
/// Returns the dispatch and the OTEL shutdown guard (None when endpoint is unset).
#[cfg(feature = "otel")]
pub(crate) fn build_tracing_subscriber<W>(
    make_writer: W,
) -> (tracing::Dispatch, Option<crate::otel::OtelGuard>)
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    let (otel_layer, guard) = crate::otel::try_init_otel_layer().unzip();

    let subscriber = tracing_subscriber::registry()
        .with(build_env_filter())
        .with(build_log_layer_with_writer(make_writer))
        .with(otel_layer);

    (tracing::Dispatch::new(subscriber), guard)
}

/// Initialize the global tracing subscriber. Call once at server startup,
/// before any tracing macros are used.
#[cfg(not(feature = "otel"))]
pub(crate) fn init_tracing() {
    install_global_tracing_dispatch(build_tracing_subscriber(std::io::stdout));
}

/// Initialize the global tracing subscriber with optional OTEL layer.
/// Returns the OTEL shutdown guard when `OTEL_EXPORTER_OTLP_ENDPOINT` is configured.
#[cfg(feature = "otel")]
pub(crate) fn init_tracing() -> Option<crate::otel::OtelGuard> {
    let (dispatch, guard) = build_tracing_subscriber(std::io::stdout);
    install_global_tracing_dispatch(dispatch);
    log_otel_startup_status(guard.is_some());
    guard
}

#[cfg(feature = "otel")]
fn log_otel_startup_status(otel_enabled: bool) {
    let status_message = if otel_enabled {
        "[otel] OTEL tracing initialized from OTEL_EXPORTER_OTLP_ENDPOINT"
    } else {
        "[otel] OTEL tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT unset, empty, or invalid)"
    };
    tracing::info!("{status_message}");
}

/// Log memory allocator and budget configuration. Extracted from
/// `load_server_config` so tracing init and post-init logging are separate concerns.
pub(crate) fn log_memory_configuration() {
    let observer = flapjack::MemoryObserver::global();
    let stats = observer.stats();
    let budget = flapjack::get_global_budget();
    tracing::info!(
        allocator = stats.allocator,
        memory_limit_mb = stats.system_limit_bytes / (1024 * 1024),
        limit_source = %stats.limit_source,
        high_watermark_pct = stats.high_watermark_pct,
        critical_pct = stats.critical_pct,
        max_concurrent_writers = budget.max_concurrent_writers(),
        "Memory configuration loaded"
    );
}

pub(crate) struct ServerConfig {
    pub env_mode: String,
    pub no_auth: bool,
    pub admin_key_env: Option<String>,
    pub data_dir: String,
    pub bind_addr: String,
    pub _data_dir_lock: DataDirProcessLock,
}

/// Loads startup configuration from environment variables for mode/auth, optional
/// admin key, data directory, and bind address, then initializes logging and
/// acquires the per-process data directory lock.
pub(crate) fn load_server_config() -> ServerConfig {
    let env_mode = std::env::var("FLAPJACK_ENV").unwrap_or_else(|_| "development".into());
    let no_auth = std::env::var("FLAPJACK_NO_AUTH")
        .ok()
        .filter(|value| value == "1")
        .is_some();

    if no_auth && env_mode == "production" {
        eprintln!("ERROR: --no-auth cannot be used in production mode.");
        std::process::exit(1);
    }

    let admin_key_env = std::env::var("FLAPJACK_ADMIN_KEY")
        .ok()
        .and_then(|key| normalize_admin_key(&key));

    match (env_mode.as_str(), &admin_key_env) {
        ("production", None) => {
            let suggested = generate_hex_key();
            eprintln!("ERROR: FLAPJACK_ADMIN_KEY is required in production mode.");
            eprintln!("Suggested key: {}", suggested);
            std::process::exit(1);
        }
        ("production", Some(key)) if key.len() < 16 => {
            eprintln!("ERROR: FLAPJACK_ADMIN_KEY must be at least 16 characters in production.");
            std::process::exit(1);
        }
        _ => {}
    }

    let data_dir = std::env::var("FLAPJACK_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    let data_dir_lock = match acquire_data_dir_process_lock(Path::new(&data_dir)) {
        Ok(lock) => lock,
        Err(message) => {
            eprintln!("ERROR: {}", message);
            std::process::exit(1);
        }
    };

    let bind_addr =
        std::env::var("FLAPJACK_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:7700".to_string());

    ServerConfig {
        env_mode,
        no_auth,
        admin_key_env,
        data_dir,
        bind_addr,
        _data_dir_lock: data_dir_lock,
    }
}

/// Resolve API key storage for server startup.
///
/// - `--no-auth`: no key, no keystore.
/// - `FLAPJACK_ADMIN_KEY`: persisted into `.admin_key` and loaded into memory.
/// - existing `.admin_key`: reused.
/// - missing `.admin_key`: auto-generate and save a new key.
pub(crate) fn initialize_key_store(
    server_config: &ServerConfig,
    data_dir: &Path,
) -> (Option<Arc<KeyStore>>, Option<String>, bool) {
    let admin_key_file = data_dir.join(".admin_key");
    let (admin_key, key_is_new) = resolve_admin_key(server_config, &admin_key_file);

    let key_store = admin_key.as_ref().map(|key| {
        let key_store = Arc::new(KeyStore::load_or_create(data_dir, key));
        tracing::info!("API key authentication enabled");
        key_store
    });

    (key_store, admin_key, key_is_new)
}

/// Resolves the admin API key: from env var, persisted key file, or generates a new
/// random key. Returns the key value and whether it was newly generated.
fn resolve_admin_key(
    server_config: &ServerConfig,
    admin_key_file: &Path,
) -> (Option<String>, bool) {
    if server_config.no_auth {
        return (None, false);
    }

    if let Some(key) = server_config
        .admin_key_env
        .as_deref()
        .and_then(normalize_admin_key)
    {
        warn_on_failed_admin_key_persist(admin_key_file, &key);
        return (Some(key), false);
    }

    if admin_key_file.exists() {
        return (Some(load_existing_admin_key(admin_key_file)), false);
    }

    (Some(create_admin_key(admin_key_file)), true)
}

fn normalize_admin_key(raw_key: &str) -> Option<String> {
    let trimmed = raw_key.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn warn_on_failed_admin_key_persist(admin_key_file: &Path, key: &str) {
    if let Err(error) = persist_admin_key(admin_key_file, key) {
        tracing::warn!("Failed to save admin key to .admin_key: {}", error);
    }
}

fn load_existing_admin_key(admin_key_file: &Path) -> String {
    match read_admin_key(admin_key_file) {
        Ok(key) => {
            if let Err(error) =
                ensure_admin_key_permissions(admin_key_file, PermissionFailureMode::WarnAndContinue)
            {
                tracing::warn!("Failed to set .admin_key permissions: {}", error);
            }
            key
        }
        Err(error) => exit_with_admin_key_reset_hint(&error),
    }
}

fn read_admin_key(admin_key_file: &Path) -> Result<String, String> {
    let raw_key = std::fs::read_to_string(admin_key_file).map_err(|error| {
        format!(
            "Failed to read .admin_key file {}: {}",
            admin_key_file.display(),
            error
        )
    })?;
    normalize_admin_key(&raw_key)
        .ok_or_else(|| format!(".admin_key file {} is empty", admin_key_file.display()))
}

fn persist_admin_key(admin_key_file: &Path, key: &str) -> std::io::Result<()> {
    persist_admin_key_file(admin_key_file, key, PermissionFailureMode::ReturnError)
        .map_err(std::io::Error::other)
}

fn create_admin_key(admin_key_file: &Path) -> String {
    let key = generate_admin_key();
    ensure_admin_key_directory(admin_key_file);
    persist_admin_key_or_exit(admin_key_file, &key);
    key
}

fn ensure_admin_key_directory(admin_key_file: &Path) {
    if let Some(parent) = admin_key_file.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            exit_with_startup_error(format!("Failed to create data directory: {}", error));
        }
    }
}

fn persist_admin_key_or_exit(admin_key_file: &Path, key: &str) {
    if let Err(error) = persist_admin_key(admin_key_file, key) {
        exit_with_startup_error(format!("Failed to save admin key: {}", error));
    }
}

fn exit_with_admin_key_reset_hint(error: &str) -> ! {
    eprintln!("❌ Error: {}", error);
    eprintln!("   Run: flapjack reset-admin-key");
    std::process::exit(1);
}

fn exit_with_startup_error(message: String) -> ! {
    eprintln!("❌ Error: {}", message);
    std::process::exit(1);
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM, whichever comes first.
pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("[shutdown] Received SIGINT (Ctrl+C)"),
        _ = terminate => tracing::info!("[shutdown] Received SIGTERM"),
    }
}

pub(crate) struct DataDirProcessLock {
    file: std::fs::File,
}

impl Drop for DataDirProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Acquire an exclusive filesystem lock on the data directory.
///
/// Create `data_dir` if it does not exist, then attempt a non-blocking exclusive
/// lock on `<data_dir>/.process.lock`. The returned `DataDirProcessLock` holds the
/// lock for its lifetime and releases it on drop.
///
/// # Arguments
///
/// * `data_dir` — Path to the Flapjack data directory.
///
/// # Returns
///
/// A `DataDirProcessLock` guard on success, or a human-readable error message
/// explaining the failure (directory creation error, lock contention, or I/O error).
pub(crate) fn acquire_data_dir_process_lock(data_dir: &Path) -> Result<DataDirProcessLock, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| {
        format!(
            "Failed to create data directory {}: {}",
            data_dir.display(),
            e
        )
    })?;

    let lock_path = data_dir.join(".process.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| {
            format!(
                "Failed to open process lock file {}: {}",
                lock_path.display(),
                e
            )
        })?;

    match file.try_lock_exclusive() {
        Ok(()) => Ok(DataDirProcessLock { file }),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Err(format!(
            "Data directory already in use: {}. Use unique --data-dir per instance.",
            lock_path.display()
        )),
        Err(e) => Err(format!(
            "Failed to acquire process lock {}: {}",
            lock_path.display(),
            e
        )),
    }
}

pub(crate) enum AuthStatus {
    NewKey(String),
    KeyInFile,
    Disabled,
}

fn capability_status(enabled: bool) -> &'static str {
    if enabled {
        "enabled"
    } else {
        "disabled"
    }
}

/// Build a human-readable capabilities summary line for the startup banner.
///
/// Uses compile-time feature flags so the output is deterministic per build.
pub(crate) fn format_capabilities_line() -> String {
    format!(
        "Capabilities: vector-search: {}, local-embeddings: {}",
        capability_status(cfg!(feature = "vector-search")),
        capability_status(cfg!(feature = "vector-search-local"))
    )
}

/// Prints the first-run banner showing the auto-generated admin API key.
fn print_new_key_banner(key: &str, url: &str, data_dir: &str) {
    use colored::Colorize;

    println!();
    println!(
        "  {}",
        "! Save this API key \u{2014} it won\u{2019}t be shown again!"
            .yellow()
            .bold()
    );
    println!();
    println!(
        "  \u{1F511}  Admin API Key:  {}",
        key.cyan().bold().on_black()
    );
    println!();
    println!(
        "     {} Copy this key to a safe place (password manager, secrets vault)",
        "1.".cyan().bold()
    );
    println!(
        "     {} Use it to authenticate API requests:",
        "2.".cyan().bold()
    );
    println!(
        "        {}",
        format!("curl -H 'X-Algolia-API-Key: {}' \\", key).dimmed()
    );
    println!(
        "        {}",
        "     -H 'X-Algolia-Application-ID: flapjack' \\".dimmed()
    );
    println!("        {}", format!("     {}/1/indexes", url).dimmed());
    println!();
    println!(
        "     {} Stored in: {}",
        "\u{2713}".green(),
        format!("{}/.admin_key", data_dir).cyan()
    );
    println!(
        "     {} Keys hashed at rest {}",
        "\u{2713}".green(),
        "(SHA-256 + unique salt)".dimmed()
    );
    println!(
        "     {} Never commit {} to version control",
        "!".yellow(),
        ".admin_key".cyan()
    );
    println!(
        "     {} If lost: {}",
        "\u{2192}".dimmed(),
        "flapjack reset-admin-key".cyan()
    );
    println!(
        "     {} Production: set {} env var",
        "\u{2192}".dimmed(),
        "FLAPJACK_ADMIN_KEY".cyan()
    );
}

fn print_existing_key_banner(data_dir: &str) {
    use colored::Colorize;

    let key_file = format!("{}/.admin_key", data_dir);
    println!();
    println!(
        "  {} Auth enabled  {}",
        "\u{2713}".green(),
        format!("(loaded from {})", key_file).dimmed()
    );
}

fn print_auth_disabled_banner() {
    use colored::Colorize;

    println!();
    println!(
        "  {} {}",
        "!".yellow().bold(),
        "Auth disabled \u{2014} all routes publicly accessible".yellow()
    );
    println!(
        "    {}",
        "Only use --no-auth for local development/testing".dimmed()
    );
}

/// Prints the server startup banner with bind address, auth status, and timing.
pub(crate) fn print_startup_banner(
    bind_addr: &str,
    auth: AuthStatus,
    startup_ms: u128,
    data_dir: &str,
) {
    use colored::Colorize;
    use std::io::Write;

    let url = format!("http://{}", bind_addr);
    let version = format!("v{}", env!("CARGO_PKG_VERSION"));
    let timing = format!("ready in {}ms", startup_ms);

    println!();
    println!(
        "  {} {}  {}",
        "\u{1F95E} Flapjack".bold(),
        version.as_str().dimmed(),
        timing.as_str().dimmed(),
    );
    println!();
    println!(
        "  {}  Local:      {}",
        "\u{2192}".green(),
        url.as_str().cyan()
    );
    let dash = format!("{}/dashboard", url);
    println!(
        "  {}  Dashboard:  {}",
        "\u{2192}".green(),
        dash.as_str().cyan()
    );
    let docs = format!("{}/swagger-ui", url);
    println!(
        "  {}  API Docs:   {}",
        "\u{2192}".green(),
        docs.as_str().cyan()
    );
    println!(
        "  {}  {}",
        "\u{2192}".green(),
        format_capabilities_line().as_str().dimmed()
    );

    match auth {
        AuthStatus::NewKey(ref key) => print_new_key_banner(key, &url, data_dir),
        AuthStatus::KeyInFile => print_existing_key_banner(data_dir),
        AuthStatus::Disabled => print_auth_disabled_banner(),
    }
    println!();
    // Tests and wrappers often pipe stdout; force flush so startup lines are observable
    // before process shutdown/timeouts.
    let _ = std::io::stdout().flush();
}

#[cfg(test)]
mod tests {
    use super::{
        acquire_data_dir_process_lock, build_log_layer_with_writer, build_tracing_subscriber,
        cors_origins_from_value, initialize_key_store, log_format_from_value, normalize_admin_key,
        read_admin_key, shutdown_timeout_secs_from_value, CorsMode, LogFormat, ServerConfig,
    };
    use axum::http::HeaderValue;
    use serde_json::Value;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tracing_subscriber::layer::SubscriberExt;

    // --- Shared test writer for capturing tracing output ---

    #[derive(Clone)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl TestWriter {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }

        fn output(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn restore_env_var(name: &str, previous: Option<std::ffi::OsString>) {
        match previous {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    fn capture_log_output(action: impl FnOnce()) -> String {
        let writer = TestWriter::new();
        let subscriber =
            tracing_subscriber::registry().with(build_log_layer_with_writer(writer.clone()));

        tracing::subscriber::with_default(subscriber, action);

        writer.output()
    }

    fn with_log_format_env<T>(value: Option<&str>, action: impl FnOnce() -> T) -> T {
        let _guard = ENV_MUTEX.lock().expect("env mutex should lock");
        let previous = std::env::var("FLAPJACK_LOG_FORMAT").ok();

        match value {
            Some(value) => std::env::set_var("FLAPJACK_LOG_FORMAT", value),
            None => std::env::remove_var("FLAPJACK_LOG_FORMAT"),
        }

        let result = action();

        match previous {
            Some(value) => std::env::set_var("FLAPJACK_LOG_FORMAT", value),
            None => std::env::remove_var("FLAPJACK_LOG_FORMAT"),
        }

        result
    }

    // --- LogFormat parsing tests ---

    #[test]
    fn log_format_from_value_selects_json_for_json_input() {
        assert_eq!(log_format_from_value(Some("json")), LogFormat::Json);
        assert_eq!(log_format_from_value(Some("JSON")), LogFormat::Json);
        assert_eq!(log_format_from_value(Some("Json")), LogFormat::Json);
    }

    #[test]
    fn log_format_from_value_defaults_to_text() {
        assert_eq!(log_format_from_value(None), LogFormat::Text);
        assert_eq!(log_format_from_value(Some("")), LogFormat::Text);
        assert_eq!(log_format_from_value(Some("text")), LogFormat::Text);
        assert_eq!(log_format_from_value(Some("TEXT")), LogFormat::Text);
        assert_eq!(log_format_from_value(Some("xml")), LogFormat::Text);
        assert_eq!(log_format_from_value(Some("bogus")), LogFormat::Text);
    }
    #[test]
    fn flapjack_log_format_env_selects_json_layer() {
        with_log_format_env(Some("json"), || {
            let writer = TestWriter::new();
            let subscriber =
                tracing_subscriber::registry().with(build_log_layer_with_writer(writer.clone()));

            tracing::subscriber::with_default(subscriber, || {
                tracing::info!("env-selected json logging");
            });

            let output = writer.output();
            let line = output
                .trim()
                .lines()
                .next()
                .expect("must emit one json line");
            let parsed: Value = serde_json::from_str(line).expect("output must be valid json");
            assert_eq!(parsed["fields"]["message"], "env-selected json logging");
        });
    }
    #[test]
    fn flapjack_log_format_env_defaults_to_text_layer_for_invalid_values() {
        with_log_format_env(Some("bogus"), || {
            let writer = TestWriter::new();
            let subscriber =
                tracing_subscriber::registry().with(build_log_layer_with_writer(writer.clone()));

            tracing::subscriber::with_default(subscriber, || {
                tracing::info!("env-selected text logging");
            });

            let output = writer.output();
            assert!(
                serde_json::from_str::<Value>(output.trim()).is_err(),
                "invalid FLAPJACK_LOG_FORMAT must fall back to text output"
            );
            assert!(
                output.contains("env-selected text logging"),
                "text output must include the logged message"
            );
        });
    }

    #[test]
    fn shutdown_timeout_secs_from_value_defaults_to_30_when_unset() {
        assert_eq!(shutdown_timeout_secs_from_value(None), 30);
    }

    #[test]
    fn shutdown_timeout_secs_from_value_parses_valid_integer() {
        assert_eq!(shutdown_timeout_secs_from_value(Some("45")), 45);
        assert_eq!(shutdown_timeout_secs_from_value(Some(" 7 ")), 7);
    }

    #[test]
    fn shutdown_timeout_secs_from_value_defaults_to_30_for_invalid_empty_or_non_positive() {
        assert_eq!(shutdown_timeout_secs_from_value(Some("abc")), 30);
        assert_eq!(shutdown_timeout_secs_from_value(Some("")), 30);
        assert_eq!(shutdown_timeout_secs_from_value(Some("   ")), 30);
        assert_eq!(shutdown_timeout_secs_from_value(Some("0")), 30);
        assert_eq!(shutdown_timeout_secs_from_value(Some("-1")), 30);
    }

    #[test]
    fn cors_origins_from_value_defaults_to_permissive_when_missing_or_empty() {
        assert_eq!(cors_origins_from_value(None), CorsMode::Permissive);
        assert_eq!(cors_origins_from_value(Some("")), CorsMode::Permissive);
        assert_eq!(cors_origins_from_value(Some("   ")), CorsMode::Permissive);
    }

    #[test]
    fn cors_origins_from_value_parses_single_origin() {
        let mode = cors_origins_from_value(Some("https://allowed.example"));
        assert_eq!(
            mode,
            CorsMode::Restricted(vec![HeaderValue::from_static("https://allowed.example")])
        );
    }

    #[test]
    fn cors_origins_from_value_parses_comma_separated_origins_with_trimmed_whitespace() {
        let mode = cors_origins_from_value(Some(
            "  https://allowed.example  , https://second.example  ",
        ));
        assert_eq!(
            mode,
            CorsMode::Restricted(vec![
                HeaderValue::from_static("https://allowed.example"),
                HeaderValue::from_static("https://second.example"),
            ])
        );
    }

    #[test]
    fn cors_origins_from_value_ignores_trailing_commas_and_empty_segments() {
        let mode =
            cors_origins_from_value(Some("https://allowed.example, ,https://second.example,,"));
        assert_eq!(
            mode,
            CorsMode::Restricted(vec![
                HeaderValue::from_static("https://allowed.example"),
                HeaderValue::from_static("https://second.example"),
            ])
        );
    }

    // --- JSON output format tests ---
    #[test]
    fn json_mode_emits_valid_json_with_expected_fields() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(key = "val", "test message");
        });

        let output = writer.output();
        let lines: Vec<&str> = output.trim().lines().collect();
        assert!(!lines.is_empty(), "JSON mode must emit at least one line");

        for line in &lines {
            let parsed: serde_json::Value =
                serde_json::from_str(line).expect("each log line must be valid JSON");
            let obj = parsed
                .as_object()
                .expect("each JSON line must be an object");

            // tracing_subscriber JSON format: timestamp, level, target, fields.message
            assert!(obj.contains_key("timestamp"), "missing 'timestamp' field");
            assert!(obj.contains_key("level"), "missing 'level' field");
            assert!(obj.contains_key("target"), "missing 'target' field");
            assert!(obj.contains_key("fields"), "missing 'fields' field");

            let fields = obj["fields"]
                .as_object()
                .expect("'fields' must be an object");
            assert!(
                fields.contains_key("message"),
                "missing 'fields.message' field"
            );
            assert_eq!(fields["message"], "test message");
        }
    }
    #[test]
    fn json_mode_includes_span_context() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_span", some_field = "some_value");
            let _guard = span.enter();
            tracing::info!("inside span");
        });

        let output = writer.output();
        let line = output.trim().lines().next().expect("must emit a log line");
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        let obj = parsed.as_object().unwrap();

        // Span context appears under "spans" array in tracing_subscriber JSON format
        assert!(
            obj.contains_key("spans"),
            "JSON output with active span must include 'spans' field"
        );
        let spans = obj["spans"].as_array().expect("'spans' must be an array");
        assert!(!spans.is_empty(), "spans array must not be empty");

        let span_obj = spans[0].as_object().unwrap();
        assert_eq!(span_obj["name"], "test_span");
        assert_eq!(span_obj["some_field"], "some_value");
    }

    // --- Text output format test ---
    #[test]
    fn text_mode_emits_human_readable_non_json_output() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(writer.clone()));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("text mode message");
        });

        let output = writer.output();
        assert!(
            !output.trim().is_empty(),
            "text mode must emit at least one line"
        );
        // Text format should NOT parse as valid JSON
        let parse_result: Result<serde_json::Value, _> = serde_json::from_str(output.trim());
        assert!(
            parse_result.is_err(),
            "text mode output must not be valid JSON"
        );
        // But should contain the message text
        assert!(
            output.contains("text mode message"),
            "text output must contain the log message"
        );
    }

    // --- request_id in JSON span context test ---
    #[test]
    fn json_mode_includes_request_id_from_span() {
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            // Mirror the span shape from request_id_middleware in middleware.rs
            let span = tracing::info_span!("http_request", request_id = tracing::field::Empty);
            span.record("request_id", tracing::field::display("test-req-id-123"));
            let _guard = span.enter();
            tracing::info!("handling request");
        });

        let output = writer.output();
        let line = output.trim().lines().next().expect("must emit a log line");
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        let obj = parsed.as_object().unwrap();

        let spans = obj["spans"]
            .as_array()
            .expect("must include 'spans' in JSON output");
        let http_span = spans
            .iter()
            .find(|s| s["name"] == "http_request")
            .expect("must include http_request span");

        assert_eq!(
            http_span["request_id"], "test-req-id-123",
            "request_id must appear in the http_request span context"
        );
    }

    #[test]
    fn normalize_admin_key_rejects_blank_values() {
        assert_eq!(normalize_admin_key("  \n\t  "), None);
        assert_eq!(
            normalize_admin_key("  test-admin-key  "),
            Some("test-admin-key".to_string())
        );
    }

    #[test]
    fn read_admin_key_rejects_blank_files() {
        let temp_dir = TempDir::new().unwrap();
        let admin_key_file = temp_dir.path().join(".admin_key");
        std::fs::write(&admin_key_file, "   \n").unwrap();

        let error = read_admin_key(&admin_key_file).unwrap_err();

        assert!(
            error.contains("empty"),
            "blank .admin_key files must be rejected"
        );
    }

    #[cfg(unix)]
    #[test]
    fn initialize_key_store_persists_env_admin_key_with_restrictive_permissions() {
        let temp_dir = TempDir::new().unwrap();
        let server_config = ServerConfig {
            env_mode: "development".to_string(),
            no_auth: false,
            admin_key_env: Some("  env-admin-key  ".to_string()),
            data_dir: temp_dir.path().display().to_string(),
            bind_addr: "127.0.0.1:7700".to_string(),
            _data_dir_lock: acquire_data_dir_process_lock(temp_dir.path()).unwrap(),
        };

        let (_key_store, admin_key, key_is_new) =
            initialize_key_store(&server_config, temp_dir.path());
        let metadata = std::fs::metadata(temp_dir.path().join(".admin_key")).unwrap();

        assert_eq!(admin_key, Some("env-admin-key".to_string()));
        assert!(!key_is_new);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn shared_admin_key_persistence_sets_restrictive_permissions() {
        let temp_dir = TempDir::new().unwrap();
        let admin_key_file = temp_dir.path().join(".admin_key");

        crate::admin_key_persistence::persist_admin_key_file(
            &admin_key_file,
            "shared-persist-key",
            crate::admin_key_persistence::PermissionFailureMode::ReturnError,
        )
        .expect("shared persistence helper should write admin key");

        let metadata = std::fs::metadata(&admin_key_file).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            std::fs::read_to_string(&admin_key_file).unwrap(),
            "shared-persist-key"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_permission_enforcer_sets_restrictive_permissions_for_existing_files() {
        let temp_dir = TempDir::new().unwrap();
        let admin_key_file = temp_dir.path().join(".admin_key");
        std::fs::write(&admin_key_file, "existing-key").unwrap();

        crate::admin_key_persistence::ensure_admin_key_permissions(
            &admin_key_file,
            crate::admin_key_persistence::PermissionFailureMode::ReturnError,
        )
        .expect("permission enforcer should succeed for writable test file");

        let metadata = std::fs::metadata(&admin_key_file).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    /// Verify the capabilities line contains the correct enabled/disabled labels
    /// matching the compiled feature flags.
    #[test]
    fn startup_banner_shows_capabilities() {
        let line = super::format_capabilities_line();

        let vs_expected = if cfg!(feature = "vector-search") {
            "vector-search: enabled"
        } else {
            "vector-search: disabled"
        };
        let local_expected = if cfg!(feature = "vector-search-local") {
            "local-embeddings: enabled"
        } else {
            "local-embeddings: disabled"
        };

        assert!(
            line.contains(vs_expected),
            "capabilities line should contain '{}', got: {}",
            vs_expected,
            line
        );
        assert!(
            line.contains(local_expected),
            "capabilities line should contain '{}', got: {}",
            local_expected,
            line
        );
    }

    // --- Tracing subscriber builder tests ---

    #[test]
    fn build_tracing_subscriber_produces_working_dispatch() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let prev_rust_log = std::env::var_os("RUST_LOG");
        let prev_log_format = std::env::var_os("FLAPJACK_LOG_FORMAT");
        std::env::set_var("RUST_LOG", "info");
        std::env::remove_var("FLAPJACK_LOG_FORMAT");

        let writer = TestWriter::new();

        #[cfg(not(feature = "otel"))]
        let dispatch = build_tracing_subscriber(writer.clone());
        #[cfg(feature = "otel")]
        let (dispatch, _otel_guard) = build_tracing_subscriber(writer.clone());

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("subscriber-init-smoke-test");
        });

        let output = writer.output();
        assert!(
            output.contains("subscriber-init-smoke-test"),
            "expected subscriber to capture log output, got: {output}"
        );

        restore_env_var("RUST_LOG", prev_rust_log);
        restore_env_var("FLAPJACK_LOG_FORMAT", prev_log_format);
    }

    #[cfg(feature = "otel")]
    #[test]
    fn build_tracing_subscriber_returns_none_guard_without_endpoint() {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let prev_rust_log = std::env::var_os("RUST_LOG");
        let prev_otel = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::set_var("RUST_LOG", "info");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("FLAPJACK_LOG_FORMAT");

        let writer = TestWriter::new();
        let (_dispatch, otel_guard) = build_tracing_subscriber(writer);

        assert!(
            otel_guard.is_none(),
            "expected no OtelGuard when OTEL_EXPORTER_OTLP_ENDPOINT is unset"
        );

        restore_env_var("RUST_LOG", prev_rust_log);
        restore_env_var("OTEL_EXPORTER_OTLP_ENDPOINT", prev_otel);
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otel_startup_status_logs_initialized_message() {
        let output = capture_log_output(|| {
            super::log_otel_startup_status(true);
        });

        assert!(
            output.contains("[otel] OTEL tracing initialized"),
            "expected OTEL startup initialization log line, got: {output}"
        );
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otel_startup_status_logs_disabled_message() {
        let output = capture_log_output(|| {
            super::log_otel_startup_status(false);
        });

        assert!(
            output.contains("OTEL_EXPORTER_OTLP_ENDPOINT unset, empty, or invalid"),
            "expected OTEL disabled log line to describe all disabled cases, got: {output}"
        );
    }

    /// Verifies the doc comment on load_server_config only claims fields it actually loads.
    #[test]
    fn load_server_config_doc_lists_only_fields_loaded_here() {
        let source = include_str!("startup.rs");
        let expected_doc =
            "/// Loads startup configuration from environment variables for mode/auth, optional\n\
/// admin key, data directory, and bind address, then initializes logging and\n\
/// acquires the per-process data directory lock.";
        let stale_doc =
            "/// Loads server configuration from environment variables: data directory, bind address,\n\
/// auth mode, admin key, SSL settings, replication config, and operational flags.";
        assert!(
            source.contains(&format!("{expected_doc}\npub(crate) fn load_server_config()")),
            "load_server_config doc should describe only the config fields and setup performed here"
        );
        assert!(
            !source.contains(&format!("{stale_doc}\npub(crate) fn load_server_config()")),
            "load_server_config doc must not claim SSL/replication/operational flags loading"
        );
    }
}
