use axum::body::{Body, Bytes};
use axum::http::StatusCode;
use axum::response::Response;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// HTTP header carrying a client-supplied idempotency key (lowercased,
/// matching axum/hyper's canonical header form).
pub const IDEMPOTENCY_HEADER: &str = "x-flapjack-idempotency-key";

const DEFAULT_TTL_SECS: u64 = 300;
const MIN_TTL_SECS: u64 = 1;

/// Cached snapshot of a successful write response.
#[derive(Clone)]
pub struct IdempotencyRecord {
    pub status: u16,
    pub body: Bytes,
    inserted_at: Instant,
}

impl IdempotencyRecord {
    /// Build a record carrying a JSON-serialized response body.
    pub fn json(status: StatusCode, body: Bytes) -> Self {
        Self {
            status: status.as_u16(),
            body,
            inserted_at: Instant::now(),
        }
    }

    /// Re-emit the record as an axum response, marking it as a cache hit so
    /// clients can distinguish a replayed response from a fresh execution.
    pub fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::OK);
        Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .header("x-flapjack-idempotency-replayed", "true")
            .body(Body::from(self.body))
            .expect("static idempotency response headers are valid")
    }
}

/// Per-process cache of recent write responses keyed by client idempotency key.
///
/// Entries expire after `ttl`. Eviction is amortized so that the request hot
/// path stays near constant-time:
///   * Each `lookup` checks the targeted entry's own expiry (O(1)).
///   * A full-cache sweep runs at most once per `ttl` window, gated by a
///     compare-and-swap on `last_trim_millis`. Concurrent callers that lose
///     the CAS skip the sweep entirely.
///
/// The "one TTL window of stale entries" worst case is acceptable: stale
/// entries never produce stale reads (the per-entry expiry check rejects
/// them), and the bound on memory growth is set by traffic during one TTL
/// window rather than by total cumulative writes.
pub struct IdempotencyCache {
    entries: DashMap<String, IdempotencyRecord>,
    ttl: Duration,
    created_at: Instant,
    last_trim_millis: AtomicU64,
    trim_count: AtomicU64,
}

impl IdempotencyCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            created_at: Instant::now(),
            last_trim_millis: AtomicU64::new(0),
            trim_count: AtomicU64::new(0),
        }
    }

    /// Build the cache using the `FLAPJACK_IDEMPOTENCY_TTL_SECS` env var
    /// (falling back to 300s).
    pub fn from_env() -> Self {
        let ttl_secs = std::env::var("FLAPJACK_IDEMPOTENCY_TTL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TTL_SECS)
            .max(MIN_TTL_SECS);
        Self::new(Duration::from_secs(ttl_secs))
    }

    /// Run a full-cache sweep at most once per TTL window. The CAS makes the
    /// sweep cooperative under concurrency: at most one caller per window
    /// actually walks the map; the rest return immediately.
    fn maybe_trim(&self) {
        let interval_millis = self.ttl.as_millis() as u64;
        let now_millis = self.created_at.elapsed().as_millis() as u64;
        let last = self.last_trim_millis.load(Ordering::Relaxed);
        if now_millis.saturating_sub(last) < interval_millis {
            return;
        }
        if self
            .last_trim_millis
            .compare_exchange(last, now_millis, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let ttl = self.ttl;
        self.entries
            .retain(|_, record| record.inserted_at.elapsed() <= ttl);
        self.trim_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a cached response, returning a clone of the stored record on
    /// hit. Expired entries are evicted and treated as a miss.
    pub fn lookup(&self, key: &str) -> Option<IdempotencyRecord> {
        self.maybe_trim();
        let entry = self.entries.get(key)?;
        let elapsed = entry.inserted_at.elapsed();
        let record = entry.clone();
        drop(entry);
        if elapsed > self.ttl {
            // remove_if guards against a concurrent fresh insert at the same
            // key: only evict if the value is still expired when we re-check.
            let ttl = self.ttl;
            self.entries
                .remove_if(key, |_, r| r.inserted_at.elapsed() > ttl);
            return None;
        }
        Some(record)
    }

    /// Store a response under `key`, replacing any prior entry.
    pub fn store(&self, key: String, record: IdempotencyRecord) {
        self.maybe_trim();
        self.entries.insert(key, record);
    }

    /// Test/diagnostics: how many entries are currently held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Test/diagnostics: is the cache empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Test/diagnostics: count of full-cache sweeps performed since cache
    /// creation. Used to assert amortization (the hot path must not sweep on
    /// every access).
    pub fn trim_count(&self) -> u64 {
        self.trim_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_none_when_key_missing() {
        let cache = IdempotencyCache::new(Duration::from_secs(60));
        assert!(cache.lookup("absent").is_none());
    }

    #[test]
    fn store_then_lookup_returns_record() {
        let cache = IdempotencyCache::new(Duration::from_secs(60));
        let record = IdempotencyRecord::json(StatusCode::CREATED, Bytes::from_static(b"{}"));
        cache.store("k1".into(), record);
        let hit = cache.lookup("k1").expect("hit");
        assert_eq!(hit.status, 201);
        assert_eq!(hit.body, Bytes::from_static(b"{}"));
    }

    #[test]
    fn expired_entry_is_evicted_on_lookup() {
        let cache = IdempotencyCache::new(Duration::from_nanos(1));
        cache.store(
            "k1".into(),
            IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
        );
        std::thread::sleep(Duration::from_millis(2));
        assert!(cache.lookup("k1").is_none());
        assert!(cache.is_empty(), "expired entry must be evicted");
    }

    #[test]
    fn lookup_on_other_key_trims_expired_entries() {
        let cache = IdempotencyCache::new(Duration::from_nanos(1));
        cache.store(
            "stale-1".into(),
            IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
        );
        cache.store(
            "stale-2".into(),
            IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
        );
        std::thread::sleep(Duration::from_millis(2));

        assert!(cache.lookup("miss-key").is_none());
        assert!(
            cache.is_empty(),
            "lookup on unrelated key should still trim expired entries"
        );
    }

    #[test]
    fn lookups_within_trim_interval_do_not_resweep() {
        // Guard against the hot-path regression: under a long TTL, repeated
        // lookups must not trigger a full-cache sweep on every call.
        let cache = IdempotencyCache::new(Duration::from_secs(60));
        cache.store(
            "k1".into(),
            IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
        );
        let trims_before = cache.trim_count();
        for _ in 0..1_000 {
            let _ = cache.lookup("k1");
        }
        assert_eq!(
            cache.trim_count(),
            trims_before,
            "lookups within the trim interval must not re-sweep the cache",
        );
    }

    #[test]
    fn stores_within_trim_interval_do_not_resweep() {
        let cache = IdempotencyCache::new(Duration::from_secs(60));
        let trims_before = cache.trim_count();
        for i in 0..1_000 {
            cache.store(
                format!("k{i}"),
                IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
            );
        }
        assert_eq!(
            cache.trim_count(),
            trims_before,
            "stores within the trim interval must not re-sweep the cache",
        );
        assert_eq!(cache.len(), 1_000);
    }

    #[test]
    fn from_env_clamps_zero_ttl_to_one_second() {
        std::env::set_var("FLAPJACK_IDEMPOTENCY_TTL_SECS", "0");
        let cache = IdempotencyCache::from_env();
        std::env::remove_var("FLAPJACK_IDEMPOTENCY_TTL_SECS");

        let trims_before = cache.trim_count();
        for i in 0..100 {
            cache.store(
                format!("k{i}"),
                IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
            );
        }
        assert_eq!(
            cache.trim_count(),
            trims_before,
            "clamped TTL should preserve amortized trim behavior",
        );
    }

    #[test]
    fn from_env_invalid_ttl_falls_back_to_default() {
        std::env::set_var("FLAPJACK_IDEMPOTENCY_TTL_SECS", "invalid");
        let cache = IdempotencyCache::from_env();
        std::env::remove_var("FLAPJACK_IDEMPOTENCY_TTL_SECS");

        let trims_before = cache.trim_count();
        for i in 0..100 {
            cache.store(
                format!("k{i}"),
                IdempotencyRecord::json(StatusCode::OK, Bytes::from_static(b"{}")),
            );
        }
        assert_eq!(
            cache.trim_count(),
            trims_before,
            "invalid TTL should preserve default amortized trim behavior",
        );
    }

    #[test]
    fn into_response_marks_replay() {
        let record = IdempotencyRecord::json(StatusCode::CREATED, Bytes::from_static(b"{\"a\":1}"));
        let response = record.into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get("x-flapjack-idempotency-replayed")
                .and_then(|v| v.to_str().ok()),
            Some("true"),
        );
    }
}
