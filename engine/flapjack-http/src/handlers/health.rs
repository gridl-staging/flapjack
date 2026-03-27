use axum::{extract::State, Json};
use std::sync::Arc;

use super::AppState;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Server is healthy", body = serde_json::Value)
    )
)]
pub async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let budget = flapjack::get_global_budget();
    let observer = flapjack::MemoryObserver::global();
    let mem_stats = observer.stats();

    Json(serde_json::json!({
        "status": "ok",
        "active_writers": budget.active_writers(),
        "max_concurrent_writers": budget.max_concurrent_writers(),
        "facet_cache_entries": state.manager.facet_cache.len(),
        "facet_cache_cap": state.manager.facet_cache_cap.load(std::sync::atomic::Ordering::Relaxed),
        "heap_allocated_mb": mem_stats.heap_allocated_bytes / (1024 * 1024),
        "system_limit_mb": mem_stats.system_limit_bytes / (1024 * 1024),
        "pressure_level": mem_stats.pressure_level.to_string(),
        "allocator": mem_stats.allocator,
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "capabilities": {
            "vectorSearch": cfg!(feature = "vector-search"),
            "vectorSearchLocal": cfg!(feature = "vector-search-local"),
        },
        "tenants_loaded": state.manager.loaded_count(),
        "uptime_secs": state.start_time.elapsed().as_secs(),
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::TestStateBuilder;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/health", get(health))
            .with_state(state)
    }

    /// TODO: Document request_health_json.
    async fn request_health_json(state: Arc<AppState>) -> serde_json::Value {
        let response = test_router(state)
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// Verify the health endpoint reflects the correct `tenants_loaded` count after creating tenants.
    #[tokio::test]
    async fn health_includes_tenants_loaded() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();
        state.manager.create_tenant("t1").unwrap();
        state.manager.create_tenant("t2").unwrap();

        let json = request_health_json(state).await;

        assert_eq!(json["tenants_loaded"].as_u64().unwrap(), 2);
    }

    /// Verify the health endpoint returns an `uptime_secs` field.
    #[tokio::test]
    async fn health_includes_uptime_secs() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();

        let json = request_health_json(state).await;

        assert!(
            json["uptime_secs"].as_u64().is_some(),
            "should have uptime_secs field"
        );
    }

    /// TODO: Document health_capabilities_schema_stable.
    #[tokio::test]
    async fn health_capabilities_schema_stable() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();

        let json = request_health_json(state).await;

        let caps = &json["capabilities"];
        assert!(caps.is_object(), "capabilities must be a JSON object");
        assert!(
            caps["vectorSearch"].is_boolean(),
            "capabilities.vectorSearch must be a bool"
        );
        assert!(
            caps["vectorSearchLocal"].is_boolean(),
            "capabilities.vectorSearchLocal must be a bool"
        );
    }

    /// TODO: Document health_capabilities_vector_search_matches_feature.
    #[tokio::test]
    async fn health_capabilities_vector_search_matches_feature() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();

        let json = request_health_json(state).await;

        let caps = &json["capabilities"];
        assert_eq!(
            caps["vectorSearch"].as_bool().unwrap(),
            cfg!(feature = "vector-search"),
            "vectorSearch must match cfg!(feature = \"vector-search\")"
        );
        assert_eq!(
            caps["vectorSearchLocal"].as_bool().unwrap(),
            cfg!(feature = "vector-search-local"),
            "vectorSearchLocal must match cfg!(feature = \"vector-search-local\")"
        );
    }

    /// Verify the health endpoint returns a `version` field matching `CARGO_PKG_VERSION`.
    #[tokio::test]
    async fn health_includes_version() {
        let tmp = TempDir::new().unwrap();
        let state = TestStateBuilder::new(&tmp).build_shared();

        let json = request_health_json(state).await;

        let version = json["version"].as_str().unwrap();
        assert_eq!(
            version,
            env!("CARGO_PKG_VERSION"),
            "version should match CARGO_PKG_VERSION"
        );
    }
}
