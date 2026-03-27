//! Stub summary for /Users/stuart/parallel_development/flapjack_dev/mar25_pm_14_rust_quality_leaky_test/flapjack_dev/engine/flapjack-http/src/notifications.rs.
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing;

// ---------------------------------------------------------------------------
// Global static accessor (follows analytics::init_global_collector pattern)
// ---------------------------------------------------------------------------

static GLOBAL_NOTIFIER: OnceLock<Arc<NotificationService>> = OnceLock::new();

/// Initialize the global notification service. Call once at startup.
pub fn init_global_notifier(service: Arc<NotificationService>) {
    let _ = GLOBAL_NOTIFIER.set(service);
}

/// Get the global notification service, if initialized.
pub fn global_notifier() -> Option<&'static Arc<NotificationService>> {
    GLOBAL_NOTIFIER.get()
}

const DEFAULT_COOLDOWN_MINUTES: u64 = 60;

#[derive(Debug, Clone)]
struct NotificationEnvConfig {
    enabled: bool,
    from_email: String,
    recipients: Vec<String>,
    cooldown_minutes: u64,
}

impl NotificationEnvConfig {
    fn from_environment() -> Self {
        Self {
            enabled: parse_enabled_flag(std::env::var("FLAPJACK_SES_ENABLED").ok()),
            from_email: std::env::var("FLAPJACK_SES_FROM_EMAIL").unwrap_or_default(),
            recipients: parse_alert_recipients(std::env::var("FLAPJACK_SES_ALERT_RECIPIENTS").ok()),
            cooldown_minutes: parse_cooldown_minutes(
                std::env::var("FLAPJACK_SES_COOLDOWN_MINUTES").ok(),
            ),
        }
    }
}

fn parse_enabled_flag(raw_value: Option<String>) -> bool {
    raw_value
        .as_deref()
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn parse_alert_recipients(raw_value: Option<String>) -> Vec<String> {
    raw_value
        .unwrap_or_default()
        .split(',')
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn parse_cooldown_minutes(raw_value: Option<String>) -> u64 {
    raw_value
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_COOLDOWN_MINUTES)
}

fn validate_ses_configuration(config: &NotificationEnvConfig) -> Result<(), &'static str> {
    if config.from_email.is_empty() {
        return Err("FLAPJACK_SES_FROM_EMAIL not set");
    }
    if config.recipients.is_empty() {
        return Err("FLAPJACK_SES_ALERT_RECIPIENTS not set");
    }
    Ok(())
}

/// TODO: Document build_ses_client.
async fn build_ses_client(config: &NotificationEnvConfig) -> Option<aws_sdk_sesv2::Client> {
    if !config.enabled {
        tracing::info!("[notifications] SES notifications disabled");
        return None;
    }

    if let Err(reason) = validate_ses_configuration(config) {
        tracing::warn!("[notifications] SES enabled but {} — disabling", reason);
        return None;
    }

    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    tracing::info!(
        "[notifications] SES notifications enabled (from={}, recipients={})",
        config.from_email,
        config.recipients.len()
    );
    Some(aws_sdk_sesv2::Client::new(&aws_config))
}

// ---------------------------------------------------------------------------
// NotificationService
// ---------------------------------------------------------------------------

pub struct NotificationService {
    ses_client: Option<aws_sdk_sesv2::Client>,
    from_email: String,
    recipients: Vec<String>,
    cooldown_map: DashMap<String, Instant>,
    cooldown_duration: Duration,
    /// Tracks number of times send_usage_alert was called (for testing).
    pub usage_alert_call_count: AtomicUsize,
    /// Tracks number of times send_gdpr_confirmation was called (for testing).
    pub gdpr_call_count: AtomicUsize,
    /// Tracks number of times send_key_lifecycle was called (for testing).
    pub key_lifecycle_call_count: AtomicUsize,
}

impl NotificationService {
    /// Create a new NotificationService from environment variables.
    ///
    /// Env vars:
    /// - `FLAPJACK_SES_ENABLED` (bool, default false)
    /// - `FLAPJACK_SES_FROM_EMAIL` (required when enabled)
    /// - `FLAPJACK_SES_ALERT_RECIPIENTS` (comma-separated emails)
    /// - `FLAPJACK_SES_COOLDOWN_MINUTES` (optional, default 60)
    pub async fn new_from_env() -> Self {
        let env_config = NotificationEnvConfig::from_environment();
        let ses_client = build_ses_client(&env_config).await;

        Self {
            ses_client,
            from_email: env_config.from_email,
            recipients: env_config.recipients,
            cooldown_map: DashMap::new(),
            cooldown_duration: Duration::from_secs(env_config.cooldown_minutes * 60),
            usage_alert_call_count: AtomicUsize::new(0),
            gdpr_call_count: AtomicUsize::new(0),
            key_lifecycle_call_count: AtomicUsize::new(0),
        }
    }

    /// Create a disabled NotificationService (for testing or when SES is not needed).
    pub fn disabled() -> Self {
        Self {
            ses_client: None,
            from_email: String::new(),
            recipients: Vec::new(),
            cooldown_map: DashMap::new(),
            cooldown_duration: Duration::from_secs(3600),
            usage_alert_call_count: AtomicUsize::new(0),
            gdpr_call_count: AtomicUsize::new(0),
            key_lifecycle_call_count: AtomicUsize::new(0),
        }
    }

    /// Create a NotificationService with custom cooldown (for testing).
    #[cfg(test)]
    pub fn with_cooldown(cooldown: Duration) -> Self {
        Self {
            ses_client: None,
            from_email: String::new(),
            recipients: Vec::new(),
            cooldown_map: DashMap::new(),
            cooldown_duration: cooldown,
            usage_alert_call_count: AtomicUsize::new(0),
            gdpr_call_count: AtomicUsize::new(0),
            key_lifecycle_call_count: AtomicUsize::new(0),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.ses_client.is_some()
    }

    /// Check cooldown for a given key. Returns true if the alert should fire
    /// (not within cooldown window), false if suppressed.
    /// Updates the map with current time when returning true.
    /// Uses DashMap entry API for atomic check-and-update.
    pub fn check_cooldown(&self, key: &str) -> bool {
        use dashmap::mapref::entry::Entry;
        let now = Instant::now();
        match self.cooldown_map.entry(key.to_string()) {
            Entry::Occupied(mut entry) => {
                if now.duration_since(*entry.get()) < self.cooldown_duration {
                    false
                } else {
                    entry.insert(now);
                    true
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(now);
                true
            }
        }
    }

    /// Send a usage threshold alert. Non-blocking: spawns a tokio task internally.
    /// Returns true if the alert was dispatched (passed cooldown), false if suppressed or disabled.
    pub fn send_usage_alert(&self, index: &str, metric: &str, count: u64, threshold: u64) -> bool {
        self.usage_alert_call_count.fetch_add(1, Ordering::Relaxed);
        if !self.is_enabled() {
            return false;
        }

        let cooldown_key = format!("usage:{}:{}", index, metric);
        if !self.check_cooldown(&cooldown_key) {
            return false;
        }

        let (subject, body) = format_usage_alert_email(index, metric, count, threshold);
        self.spawn_send_email(subject, body);
        true
    }

    /// Send a GDPR deletion confirmation. Non-blocking: spawns internally.
    pub fn send_gdpr_confirmation(&self, user_token: &str) {
        self.gdpr_call_count.fetch_add(1, Ordering::Relaxed);
        if !self.is_enabled() {
            return;
        }

        let (subject, body) = format_gdpr_email(user_token);
        self.spawn_send_email(subject, body);
    }

    /// Send an API key lifecycle notification. Non-blocking: spawns internally.
    pub fn send_key_lifecycle(&self, key_description: &str, action: &str) {
        self.key_lifecycle_call_count
            .fetch_add(1, Ordering::Relaxed);
        if !self.is_enabled() {
            return;
        }

        let (subject, body) = format_key_lifecycle_email(key_description, action);
        self.spawn_send_email(subject, body);
    }

    /// Spawn a background task to send an email via SES.
    fn spawn_send_email(&self, subject: String, body: String) {
        if let Some(client) = self.ses_client.clone() {
            let from = self.from_email.clone();
            let recipients = self.recipients.clone();
            tokio::spawn(async move {
                if let Err(e) = send_email(&client, &from, &recipients, &subject, &body).await {
                    tracing::error!("[notifications] Failed to send email: {}", e);
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Pure email formatting functions (easily testable, no SES client needed)
// ---------------------------------------------------------------------------

/// Format a usage threshold alert email. Returns (subject, body).
pub fn format_usage_alert_email(
    index: &str,
    metric: &str,
    count: u64,
    threshold: u64,
) -> (String, String) {
    let subject = format!("[Flapjack] Usage Alert: {} threshold exceeded", metric);
    let body = format!(
        "Usage threshold alert\n\n\
         Index: {}\n\
         Metric: {}\n\
         Current count: {}\n\
         Threshold: {}\n\n\
         The {} count for index '{}' has exceeded the configured threshold.\n\
         Please review your usage and consider adjusting limits if needed.",
        index, metric, count, threshold, metric, index
    );
    (subject, body)
}

/// TODO: Document redact_user_token.
fn redact_user_token(user_token: &str) -> String {
    const PREFIX_CHARS: usize = 4;
    const SUFFIX_CHARS: usize = 4;

    let char_count = user_token.chars().count();
    if char_count <= PREFIX_CHARS + SUFFIX_CHARS {
        return format!("[redacted token len={}]", char_count);
    }

    let prefix: String = user_token.chars().take(PREFIX_CHARS).collect();
    let suffix: String = user_token
        .chars()
        .rev()
        .take(SUFFIX_CHARS)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    format!("{}…{} (len={})", prefix, suffix, char_count)
}

/// Format a GDPR deletion confirmation email. Returns (subject, body).
/// Subject is generic and the body only includes a redacted token reference.
pub fn format_gdpr_email(user_token: &str) -> (String, String) {
    let subject = "[Flapjack] GDPR Deletion Confirmed".to_string();
    let timestamp = chrono::Utc::now().to_rfc3339();
    let redacted_token = redact_user_token(user_token);
    let body = format!(
        "GDPR data deletion confirmed\n\n\
         User token reference: {}\n\
         Deleted at: {}\n\n\
         All analytics events associated with this user token have been purged \
         in compliance with the GDPR right to erasure.",
        redacted_token, timestamp
    );
    (subject, body)
}

/// Format an API key lifecycle notification email. Returns (subject, body).
pub fn format_key_lifecycle_email(description: &str, action: &str) -> (String, String) {
    let subject = format!("[Flapjack] API Key {}", action);
    let timestamp = chrono::Utc::now().to_rfc3339();
    let body = format!(
        "API key lifecycle event\n\n\
         Action: {}\n\
         Key description: {}\n\
         Timestamp: {}\n\n\
         This is an automated notification about an API key change.",
        action, description, timestamp
    );
    (subject, body)
}

// ---------------------------------------------------------------------------
// Usage threshold checking (extracted for testability)
// ---------------------------------------------------------------------------

/// Check all usage counters against thresholds and fire alerts via the notifier.
/// Called periodically by the background task in server.rs.
pub fn check_usage_thresholds(
    notifier: &NotificationService,
    counters: &dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters>,
    search_threshold: u64,
    write_threshold: u64,
) {
    for entry in counters.iter() {
        let index = entry.key();
        let usage = entry.value();
        if search_threshold > 0 {
            let count = usage
                .search_count
                .load(std::sync::atomic::Ordering::Relaxed);
            if count >= search_threshold {
                notifier.send_usage_alert(index, "searches", count, search_threshold);
            }
        }
        if write_threshold > 0 {
            let count = usage.write_count.load(std::sync::atomic::Ordering::Relaxed);
            if count >= write_threshold {
                notifier.send_usage_alert(index, "writes", count, write_threshold);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal SES email sender
// ---------------------------------------------------------------------------

/// Send an email via AWS SES to the configured recipients.
///
/// # Arguments
///
/// * `client` - SES v2 client used to dispatch the email.
/// * `from` - Verified sender email address.
/// * `recipients` - Destination email addresses.
/// * `subject` - Email subject line.
/// * `body` - Plain-text email body.
///
/// # Returns
///
/// Ok on successful dispatch, or an error string describing the SES failure.
async fn send_email(
    client: &aws_sdk_sesv2::Client,
    from: &str,
    recipients: &[String],
    subject: &str,
    body: &str,
) -> Result<(), String> {
    use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};

    let destination = Destination::builder()
        .set_to_addresses(Some(recipients.to_vec()))
        .build();

    let subject_content = Content::builder()
        .data(subject)
        .charset("UTF-8")
        .build()
        .map_err(|e| format!("Failed to build subject: {}", e))?;

    let body_content = Content::builder()
        .data(body)
        .charset("UTF-8")
        .build()
        .map_err(|e| format!("Failed to build body: {}", e))?;

    let message = Message::builder()
        .subject(subject_content)
        .body(Body::builder().text(body_content).build())
        .build();

    let email_content = EmailContent::builder().simple(message).build();

    client
        .send_email()
        .from_email_address(from)
        .destination(destination)
        .content(email_content)
        .send()
        .await
        .map_err(|e| format!("SES SendEmail failed: {}", e))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parse_enabled_flag_accepts_expected_truthy_values() {
        assert!(parse_enabled_flag(Some("1".to_string())));
        assert!(parse_enabled_flag(Some("true".to_string())));
        assert!(parse_enabled_flag(Some("TRUE".to_string())));
        assert!(!parse_enabled_flag(Some("false".to_string())));
        assert!(!parse_enabled_flag(None));
    }

    #[test]
    fn parse_alert_recipients_trims_and_drops_empty_values() {
        let recipients = parse_alert_recipients(Some(
            "  ops@example.com, ,alerts@example.com  ,  ".to_string(),
        ));
        assert_eq!(
            recipients,
            vec![
                "ops@example.com".to_string(),
                "alerts@example.com".to_string()
            ]
        );
    }

    #[test]
    fn parse_cooldown_minutes_defaults_on_missing_or_invalid_values() {
        assert_eq!(parse_cooldown_minutes(None), 60);
        assert_eq!(parse_cooldown_minutes(Some("abc".to_string())), 60);
        assert_eq!(parse_cooldown_minutes(Some("15".to_string())), 15);
    }

    // === Disabled/graceful degradation tests ===

    #[tokio::test]
    async fn notification_service_disabled_when_ses_not_configured() {
        // With no env vars set, service should be disabled
        std::env::remove_var("FLAPJACK_SES_ENABLED");
        let service = NotificationService::new_from_env().await;
        assert!(!service.is_enabled());
    }

    #[tokio::test]
    async fn notification_service_send_usage_alert_noop_when_disabled() {
        let service = NotificationService::disabled();
        assert!(!service.is_enabled());
        // Should return false (not dispatched) and not panic
        let result = service.send_usage_alert("test_index", "searches", 1000, 500);
        assert!(!result);
    }

    #[tokio::test]
    async fn notification_service_send_gdpr_confirmation_noop_when_disabled() {
        let service = NotificationService::disabled();
        // Should not panic
        service.send_gdpr_confirmation("user_abc123");
    }

    #[tokio::test]
    async fn notification_service_send_key_lifecycle_noop_when_disabled() {
        let service = NotificationService::disabled();
        // Should not panic
        service.send_key_lifecycle("My API Key", "created");
    }

    // === Email formatting tests ===

    #[test]
    fn format_usage_alert_contains_index_and_metric() {
        let (subject, body) = format_usage_alert_email("products_idx", "searches", 15000, 10000);
        assert!(subject.contains("searches"));
        assert!(body.contains("products_idx"));
        assert!(body.contains("searches"));
        assert!(body.contains("15000"));
        assert!(body.contains("10000"));
    }

    /// Verify that the GDPR email keeps the raw user token out of both the
    /// subject and body while still including a redacted reference and an RFC
    /// 3339 timestamp.
    #[test]
    fn format_gdpr_confirmation_redacts_raw_token() {
        let (subject, body) = format_gdpr_email("user_secret_token_xyz");
        assert!(
            !subject.contains("user_secret_token_xyz"),
            "Subject should not contain user token"
        );
        assert!(
            !body.contains("user_secret_token_xyz"),
            "Body should not contain the raw user token"
        );
        assert!(
            body.contains("user…_xyz"),
            "Body should contain a redacted token reference"
        );
        assert!(
            body.contains("Deleted at: 20"),
            "Body should contain RFC 3339 timestamp (got: {})",
            body
        );
    }

    #[test]
    fn format_key_lifecycle_contains_description_and_action() {
        let (subject, body) = format_key_lifecycle_email("My Search Key", "created");
        assert!(subject.contains("created"));
        assert!(body.contains("My Search Key"));
        assert!(body.contains("created"));

        let (subject2, body2) = format_key_lifecycle_email("Admin Key", "deleted");
        assert!(subject2.contains("deleted"));
        assert!(body2.contains("Admin Key"));
        assert!(body2.contains("deleted"));
    }

    // === Cooldown deduplication tests ===

    #[test]
    fn cooldown_prevents_duplicate_alerts() {
        let service = NotificationService::with_cooldown(Duration::from_secs(3600));
        // First call should pass
        assert!(service.check_cooldown("usage:idx1:searches"));
        // Second call within cooldown should be suppressed
        assert!(!service.check_cooldown("usage:idx1:searches"));
    }

    #[test]
    fn cooldown_allows_alert_after_expiry() {
        // Use a tiny cooldown so we can test expiry without sleeping long
        let service = NotificationService::with_cooldown(Duration::from_millis(50));
        assert!(service.check_cooldown("usage:idx1:searches"));
        // Within cooldown — suppressed
        assert!(!service.check_cooldown("usage:idx1:searches"));
        // Wait past cooldown
        std::thread::sleep(Duration::from_millis(60));
        // Should now pass again
        assert!(service.check_cooldown("usage:idx1:searches"));
    }

    #[test]
    fn cooldown_separate_keys_for_different_metrics() {
        let service = NotificationService::with_cooldown(Duration::from_secs(3600));
        // Different metric keys should be independent
        assert!(service.check_cooldown("usage:idx1:searches"));
        assert!(service.check_cooldown("usage:idx1:writes"));
        // Same keys should be suppressed
        assert!(!service.check_cooldown("usage:idx1:searches"));
        assert!(!service.check_cooldown("usage:idx1:writes"));
    }

    // === Global accessor tests ===
    // Note: global state tests can't run in parallel safely.
    // These test the pattern but OnceLock can only be set once per process,
    // so we combine them into a single test.

    #[tokio::test]
    async fn global_notifier_lifecycle() {
        // Before init, should be None (may already be set by another test in same process)
        // We can't guarantee ordering, so we just verify the API works
        let service = Arc::new(NotificationService::disabled());
        init_global_notifier(service);
        assert!(global_notifier().is_some());
    }

    // === Usage threshold check tests ===

    /// Verify that `check_usage_thresholds` calls `send_usage_alert` when a search counter exceeds the configured threshold.
    #[test]
    fn usage_threshold_fires_alert_when_exceeded() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        // Insert a counter that exceeds the search threshold
        let usage = crate::usage_middleware::TenantUsageCounters::new();
        usage
            .search_count
            .store(1500, std::sync::atomic::Ordering::Relaxed);
        counters.insert("products".to_string(), usage);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        check_usage_thresholds(&notifier, &counters, 1000, 0);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert!(
            after > before,
            "send_usage_alert should have been called: before={before}, after={after}"
        );
    }

    /// Verify that no usage alert fires when the counter is below the configured threshold.
    #[test]
    fn usage_threshold_does_not_fire_below_threshold() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        let usage = crate::usage_middleware::TenantUsageCounters::new();
        usage
            .search_count
            .store(500, std::sync::atomic::Ordering::Relaxed);
        counters.insert("products".to_string(), usage);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        check_usage_thresholds(&notifier, &counters, 1000, 0);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            before, after,
            "send_usage_alert should NOT have been called below threshold"
        );
    }

    /// Verify that write counts exceeding the write threshold trigger a usage alert.
    #[test]
    fn usage_threshold_fires_for_writes_too() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        let usage = crate::usage_middleware::TenantUsageCounters::new();
        usage
            .write_count
            .store(200, std::sync::atomic::Ordering::Relaxed);
        counters.insert("orders".to_string(), usage);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        check_usage_thresholds(&notifier, &counters, 0, 100);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert!(
            after > before,
            "send_usage_alert should have been called for writes: before={before}, after={after}"
        );
    }

    /// Verify that `check_usage_thresholds` fires one alert per index when multiple indices exceed the threshold.
    #[test]
    fn usage_threshold_fires_for_multiple_indices() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        // Insert two indices, both above threshold
        let usage1 = crate::usage_middleware::TenantUsageCounters::new();
        usage1
            .search_count
            .store(2000, std::sync::atomic::Ordering::Relaxed);
        counters.insert("products".to_string(), usage1);

        let usage2 = crate::usage_middleware::TenantUsageCounters::new();
        usage2
            .search_count
            .store(3000, std::sync::atomic::Ordering::Relaxed);
        counters.insert("orders".to_string(), usage2);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        check_usage_thresholds(&notifier, &counters, 1000, 0);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            after - before,
            2,
            "send_usage_alert should have been called once per index: before={before}, after={after}"
        );
    }

    /// Verify that both search and write alerts fire independently for a single index when both counters exceed their respective thresholds.
    #[test]
    fn usage_threshold_fires_both_search_and_write_for_same_index() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        let usage = crate::usage_middleware::TenantUsageCounters::new();
        usage
            .search_count
            .store(1500, std::sync::atomic::Ordering::Relaxed);
        usage
            .write_count
            .store(300, std::sync::atomic::Ordering::Relaxed);
        counters.insert("products".to_string(), usage);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        check_usage_thresholds(&notifier, &counters, 1000, 200);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            after - before,
            2,
            "both search and write alerts should fire: before={before}, after={after}"
        );
    }

    /// Verify that a threshold value of zero disables alerting, even when counters are very large.
    #[test]
    fn usage_threshold_zero_means_disabled() {
        let notifier = NotificationService::disabled();
        let counters: dashmap::DashMap<String, crate::usage_middleware::TenantUsageCounters> =
            dashmap::DashMap::new();

        let usage = crate::usage_middleware::TenantUsageCounters::new();
        usage
            .search_count
            .store(999999, std::sync::atomic::Ordering::Relaxed);
        usage
            .write_count
            .store(999999, std::sync::atomic::Ordering::Relaxed);
        counters.insert("products".to_string(), usage);

        let before = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);
        // Both thresholds 0 = disabled
        check_usage_thresholds(&notifier, &counters, 0, 0);
        let after = notifier
            .usage_alert_call_count
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            before, after,
            "send_usage_alert should NOT be called when thresholds are 0"
        );
    }
}
