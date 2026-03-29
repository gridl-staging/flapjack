//! OpenTelemetry initialization module.
//!
//! Provides `try_init_otel_layer()` as the single entrypoint for OTEL setup.
//! Reads `OTEL_EXPORTER_OTLP_ENDPOINT` and returns `None` when unset/empty,
//! or `Some((layer, guard))` when configured.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

/// Handle that keeps the OTEL trace provider alive.
/// Dropping the guard triggers graceful provider shutdown.
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl OtelGuard {
    /// Explicitly shut down the trace provider, flushing pending spans.
    pub fn shutdown(self) -> Result<(), opentelemetry_sdk::error::OTelSdkError> {
        self.provider.shutdown()
    }
}

/// Initialize an OpenTelemetry tracing layer if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// Returns `None` when the env var is unset, empty, whitespace-only, or invalid.
/// When set, builds an OTLP gRPC exporter, a batch `SdkTracerProvider`, and a
/// `tracing_opentelemetry` layer wired to it.
///
/// The returned `OtelGuard` must be held alive for the lifetime of the application;
/// dropping it shuts down the provider and flushes pending spans.
pub fn try_init_otel_layer<S>() -> Option<(
    OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
    OtelGuard,
)>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return None;
    }

    // Keep endpoint parsing inside this helper so later stages have one
    // source of truth for OTEL configuration.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_owned())
        .build()
        .ok()?;

    // Batch processor for production throughput (async export via tokio).
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer("flapjack");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let guard = OtelGuard { provider };

    Some((layer, guard))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::OsString, sync::Mutex};

    // Serialize env-var mutations so parallel test threads don't race.
    // Use into_inner on PoisonError to recover if a prior test panicked.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct EndpointEnvGuard {
        original: Option<OsString>,
    }

    impl EndpointEnvGuard {
        fn replace(value: Option<&str>) -> Self {
            let guard = Self {
                original: std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT"),
            };
            match value {
                Some(value) => std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", value),
                None => std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT"),
            }
            guard
        }
    }

    impl Drop for EndpointEnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", value),
                None => std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT"),
            }
        }
    }

    #[test]
    fn otel_layer_returns_none_when_endpoint_unset() {
        let _lock = lock_env();
        let _guard = EndpointEnvGuard::replace(None);

        let result = try_init_otel_layer::<tracing_subscriber::Registry>();
        assert!(
            result.is_none(),
            "expected None when OTEL_EXPORTER_OTLP_ENDPOINT is unset"
        );
    }

    #[test]
    fn otel_layer_returns_none_when_endpoint_empty() {
        let _lock = lock_env();
        let _guard = EndpointEnvGuard::replace(Some(""));

        let result = try_init_otel_layer::<tracing_subscriber::Registry>();
        assert!(
            result.is_none(),
            "expected None when OTEL_EXPORTER_OTLP_ENDPOINT is empty"
        );
    }

    #[test]
    fn otel_layer_returns_none_when_endpoint_invalid() {
        let _lock = lock_env();
        let _guard = EndpointEnvGuard::replace(Some("not a valid endpoint"));

        let result = try_init_otel_layer::<tracing_subscriber::Registry>();
        assert!(
            result.is_none(),
            "expected None when OTEL_EXPORTER_OTLP_ENDPOINT is invalid"
        );
    }

    // Building the tonic exporter requires a Tokio runtime.
    #[tokio::test]
    async fn otel_layer_returns_some_when_endpoint_configured() {
        let _lock = lock_env();
        let _guard = EndpointEnvGuard::replace(Some("http://localhost:4317"));

        let result = try_init_otel_layer::<tracing_subscriber::Registry>();
        assert!(
            result.is_some(),
            "expected Some when OTEL_EXPORTER_OTLP_ENDPOINT is set"
        );

        // Clean up: shut down the provider and restore env
        if let Some((_layer, guard)) = result {
            let _ = guard.shutdown();
        }
    }
}
