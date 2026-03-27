//! Co-occurrence engine for related-products and bought-together models.
//!
//! Builds per-user item interaction sets from insight events, computes
//! item-item co-occurrence counts, and returns scored recommendations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::analytics::AnalyticsQueryEngine;
use crate::types::Document;
use crate::IndexManager;

use super::CO_OCCURRENCE_LOOKBACK_DAYS;

/// A scored co-occurrence recommendation hit.
#[derive(Debug, Clone)]
pub struct CooccurrenceHit {
    pub object_id: String,
    pub score: u32, // 0-100
    pub document: Option<Document>,
}

/// Event type filter for co-occurrence computation.
#[derive(Debug, Clone, Copy)]
pub enum EventFilter {
    /// Click + conversion events (for related-products)
    ClickAndConversion,
    /// Purchase-only conversions (for bought-together)
    PurchaseOnly,
}

impl EventFilter {
    fn sql_where_clause(&self) -> &'static str {
        match self {
            EventFilter::ClickAndConversion => {
                "(event_type = 'click' OR event_type = 'conversion')"
            }
            EventFilter::PurchaseOnly => {
                "(event_type = 'conversion' AND event_subtype = 'purchase')"
            }
        }
    }
}

/// Compute co-occurrence recommendations for a seed objectID.
///
/// Builds per-user item sets from events within the lookback window,
/// computes how often each item co-occurs with the seed item across users,
/// normalizes to 0-100, and filters by threshold.
pub async fn compute_cooccurrence(
    analytics: &AnalyticsQueryEngine,
    manager: &Arc<IndexManager>,
    index_name: &str,
    seed_object_id: &str,
    event_filter: EventFilter,
    threshold: u32,
    max_recommendations: u32,
) -> Result<Vec<CooccurrenceHit>, String> {
    let lookback_days = CO_OCCURRENCE_LOOKBACK_DAYS;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let window_ms = (lookback_days * 24 * 60 * 60 * 1000) as i64;
    let cutoff_ms = now_ms - window_ms;

    let where_clause = event_filter.sql_where_clause();
    let sql = format!(
        "SELECT user_token, object_ids FROM events \
         WHERE {} AND timestamp_ms >= {} \
         ORDER BY user_token, timestamp_ms",
        where_clause, cutoff_ms
    );

    let rows = analytics.query_events(index_name, &sql).await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Build per-user item sets
    let mut user_items: HashMap<String, HashSet<String>> = HashMap::new();
    for row in &rows {
        let user = row
            .get("user_token")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if user.is_empty() {
            continue;
        }
        let object_ids = super::parse_object_ids(row);
        for oid in object_ids {
            if oid.is_empty() {
                continue;
            }
            user_items.entry(user.clone()).or_default().insert(oid);
        }
    }

    // Compute co-occurrence: for each user who interacted with the seed,
    // count all other items they interacted with
    let mut cooccurrence_counts: HashMap<String, u32> = HashMap::new();
    for items in user_items.values() {
        if !items.contains(seed_object_id) {
            continue;
        }
        for item in items {
            if item != seed_object_id {
                *cooccurrence_counts.entry(item.clone()).or_insert(0) += 1;
            }
        }
    }

    if cooccurrence_counts.is_empty() {
        return Ok(Vec::new());
    }

    // Normalize to 0-100
    let max_count = cooccurrence_counts.values().copied().max().unwrap_or(1);
    let mut items: Vec<(String, u32)> = cooccurrence_counts
        .into_iter()
        .map(|(oid, count)| {
            let normalized = if max_count > 0 {
                ((count as f64 / max_count as f64) * 100.0).round() as u32
            } else {
                0
            };
            (oid, normalized.min(100))
        })
        .filter(|(_, score)| *score >= threshold)
        .collect();

    // Sort by score descending, deterministic tie-breaking by objectID lexicographic
    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    items.truncate(max_recommendations as usize);

    // Hydrate with documents
    let hits = items
        .into_iter()
        .map(|(oid, score)| {
            let doc = manager.get_document(index_name, &oid).ok().flatten();
            CooccurrenceHit {
                object_id: oid,
                score,
                document: doc,
            }
        })
        .collect();

    Ok(hits)
}
