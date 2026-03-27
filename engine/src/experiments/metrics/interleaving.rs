use std::collections::HashMap;

use crate::experiments::assignment::murmurhash3_128;
use crate::experiments::stats;

use super::types::{EventRow, InterleavingMetrics};

/// Per-query interleaving click counts for preference scoring.
pub(super) struct InterleavingClickCounts {
    /// Vec of (control_clicks, variant_clicks) per query.
    pub(super) per_query: Vec<(u32, u32)>,
    /// Total queries with interleaving click data.
    pub(super) total_queries: u32,
    /// Unique query IDs (for first-team distribution quality check).
    pub(super) query_ids: Vec<String>,
}

/// Aggregate click events with team attribution into per-query click counts.
///
/// Groups click events by query_id, counts clicks per team ("control" / "variant"),
/// and returns per-query tuples suitable for `compute_preference_score`.
/// Only events with `event_type == "click"` and a non-None `interleaving_team` are counted.
pub(super) fn aggregate_interleaving_clicks(events: &[EventRow]) -> InterleavingClickCounts {
    let mut by_query: HashMap<&str, (u32, u32)> = HashMap::new();

    for e in events {
        if e.event_type != "click" {
            continue;
        }
        let team_is_control = match e.interleaving_team.as_deref() {
            Some("control") => true,
            Some("variant") => false,
            _ => continue, // ignore missing/invalid team values
        };
        let entry = by_query.entry(e.query_id.as_str()).or_insert((0, 0));
        if team_is_control {
            entry.0 += 1;
        } else {
            entry.1 += 1;
        }
    }

    let query_ids: Vec<String> = by_query.keys().map(|k| k.to_string()).collect();
    let per_query: Vec<(u32, u32)> = by_query.into_values().collect();
    let total_queries = per_query.len() as u32;
    InterleavingClickCounts {
        per_query,
        total_queries,
        query_ids,
    }
}

/// Compute interleaving preference metrics from raw event rows.
///
/// This is the pure computation path — aggregates click events by query,
/// then feeds per-query counts to `compute_preference_score`.
/// Also computes the first-team distribution quality check by re-hashing
/// each unique query_id with the experiment_id.
pub(super) fn compute_interleaving_metrics(
    events: &[EventRow],
    experiment_id: &str,
) -> InterleavingMetrics {
    let counts = aggregate_interleaving_clicks(events);
    let preference = stats::compute_preference_score(&counts.per_query);

    // Compute first-team distribution from unique query IDs.
    // Re-derive the first-team coin flip using the same hash as team_draft_interleave.
    let first_team_a_ratio = if counts.query_ids.is_empty() {
        0.5 // neutral default when no data
    } else {
        let team_a_first_count = counts
            .query_ids
            .iter()
            .filter(|qid| {
                let key = format!("{}:{}", experiment_id, qid);
                let (h1, _) = murmurhash3_128(key.as_bytes(), 0);
                h1 & 1 == 0 // same logic as team_draft_interleave
            })
            .count();
        team_a_first_count as f64 / counts.query_ids.len() as f64
    };

    InterleavingMetrics {
        preference,
        total_queries: counts.total_queries,
        first_team_a_ratio,
    }
}
