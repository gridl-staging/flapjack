//! Trending items and trending facets aggregation.
//!
//! Queries conversion events from the analytics engine and computes
//! trending scores based on frequency weighted by recency.

use std::collections::HashMap;
use std::sync::Arc;

use crate::analytics::AnalyticsQueryEngine;
use crate::types::Document;
use crate::IndexManager;

/// A scored recommendation hit for trending items.
#[derive(Debug, Clone)]
pub struct TrendingItemHit {
    pub object_id: String,
    pub score: u32, // 0-100
    pub document: Option<Document>,
}

/// A scored recommendation hit for trending facets.
#[derive(Debug, Clone)]
pub struct TrendingFacetHit {
    pub facet_name: String,
    pub facet_value: String,
    pub score: u32, // 0-100
}

/// Optional facet filter for trending-item recommendations.
#[derive(Debug, Clone, Copy)]
pub struct FacetFilter<'a> {
    pub name: &'a str,
    pub value: Option<&'a str>,
}

/// Compute trending items for an index based on recent conversion events.
///
/// Scores are computed by counting conversion events per objectID,
/// weighted by recency (more recent events count more), then normalized to 0-100.
pub async fn compute_trending_items(
    analytics: &AnalyticsQueryEngine,
    manager: &Arc<IndexManager>,
    index_name: &str,
    window_days: u64,
    facet_filter: Option<FacetFilter<'_>>,
    threshold: u32,
    max_recommendations: u32,
) -> Result<Vec<TrendingItemHit>, String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let window_ms = (window_days * 24 * 60 * 60 * 1000) as i64;
    let cutoff_ms = now_ms - window_ms;

    // Query conversion events from analytics engine
    let sql = format!(
        "SELECT object_ids, timestamp_ms FROM events \
         WHERE event_type = 'conversion' AND timestamp_ms >= {} \
         ORDER BY timestamp_ms DESC",
        cutoff_ms
    );

    let rows = analytics.query_events(index_name, &sql).await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Count per objectID, weighted by recency
    // Weight = 1.0 + (timestamp_ms - cutoff_ms) / window_ms
    // More recent events get higher weight (up to 2.0)
    let mut scores: HashMap<String, f64> = HashMap::new();
    for row in &rows {
        let object_ids = super::parse_object_ids(row);
        let ts = row
            .get("timestamp_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(cutoff_ms);
        let recency_weight = 1.0 + ((ts - cutoff_ms) as f64 / window_ms as f64);

        for oid in object_ids {
            if oid.is_empty() {
                continue;
            }
            *scores.entry(oid).or_insert(0.0) += recency_weight;
        }
    }

    if scores.is_empty() {
        return Ok(Vec::new());
    }

    // Normalize to 0-100
    let max_score = scores.values().cloned().fold(0.0_f64, f64::max);
    if max_score <= 0.0 {
        return Ok(Vec::new());
    }

    let mut items: Vec<(String, u32)> = scores
        .into_iter()
        .map(|(oid, raw)| {
            let normalized = ((raw / max_score) * 100.0).round() as u32;
            (oid, normalized.min(100))
        })
        .filter(|(_, score)| *score >= threshold)
        .collect();

    // Sort by score descending, then by objectID for deterministic tie-breaking.
    // Do NOT truncate here: facet filtering runs below and must see the full candidate set.
    // Final truncation happens after facet filtering (line below).
    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    // Hydrate with documents and optionally filter by facet
    let mut hits = Vec::new();
    for (oid, score) in items {
        let doc = manager.get_document(index_name, &oid).ok().flatten();

        // If facet filtering requested, check the document matches
        if let Some(filter) = facet_filter {
            if let Some(ref d) = doc {
                let matches = match d.fields.get(filter.name) {
                    Some(crate::types::FieldValue::Facet(v)) => {
                        filter.value.is_none_or(|fv| v == fv)
                    }
                    Some(crate::types::FieldValue::Text(v)) => {
                        filter.value.is_none_or(|fv| v == fv)
                    }
                    Some(crate::types::FieldValue::Array(arr)) => filter.value.is_none_or(|fv| {
                        arr.iter().any(|item| match item {
                            crate::types::FieldValue::Facet(v) => v == fv,
                            crate::types::FieldValue::Text(v) => v == fv,
                            _ => false,
                        })
                    }),
                    _ => filter.value.is_none(), // No facet field → only matches if no value filter
                };
                if !matches {
                    continue;
                }
            } else {
                // Document not found, can't verify facet match
                continue;
            }
        }

        hits.push(TrendingItemHit {
            object_id: oid,
            score,
            document: doc,
        });
    }

    // Re-truncate after facet filtering
    hits.truncate(max_recommendations as usize);

    Ok(hits)
}

/// Compute trending facet values for an index based on recent conversion events.
///
/// Resolves objectIDs from conversion events to documents, extracts the requested
/// facet field values, counts by value weighted by recency, and normalizes to 0-100.
pub async fn compute_trending_facets(
    analytics: &AnalyticsQueryEngine,
    manager: &Arc<IndexManager>,
    index_name: &str,
    window_days: u64,
    facet_name: &str,
    threshold: u32,
    max_recommendations: u32,
) -> Result<Vec<TrendingFacetHit>, String> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let window_ms = (window_days * 24 * 60 * 60 * 1000) as i64;
    let cutoff_ms = now_ms - window_ms;

    let sql = format!(
        "SELECT object_ids, timestamp_ms FROM events \
         WHERE event_type = 'conversion' AND timestamp_ms >= {} \
         ORDER BY timestamp_ms DESC",
        cutoff_ms
    );

    let rows = analytics.query_events(index_name, &sql).await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // For each conversion event, resolve objectID → document → facet value
    let mut facet_scores: HashMap<String, f64> = HashMap::new();
    for row in &rows {
        let object_ids = super::parse_object_ids(row);
        if object_ids.is_empty() {
            continue;
        }
        let ts = row
            .get("timestamp_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(cutoff_ms);
        let recency_weight = 1.0 + ((ts - cutoff_ms) as f64 / window_ms as f64);

        // Look up document and extract facet value
        for oid in object_ids {
            if let Ok(Some(doc)) = manager.get_document(index_name, &oid) {
                let values = extract_facet_values(&doc, facet_name);
                for val in values {
                    *facet_scores.entry(val).or_insert(0.0) += recency_weight;
                }
            }
        }
    }

    if facet_scores.is_empty() {
        return Ok(Vec::new());
    }

    let max_score = facet_scores.values().cloned().fold(0.0_f64, f64::max);
    if max_score <= 0.0 {
        return Ok(Vec::new());
    }

    let mut items: Vec<(String, u32)> = facet_scores
        .into_iter()
        .map(|(val, raw)| {
            let normalized = ((raw / max_score) * 100.0).round() as u32;
            (val, normalized.min(100))
        })
        .filter(|(_, score)| *score >= threshold)
        .collect();

    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    items.truncate(max_recommendations as usize);

    Ok(items
        .into_iter()
        .map(|(val, score)| TrendingFacetHit {
            facet_name: facet_name.to_string(),
            facet_value: val,
            score,
        })
        .collect())
}

/// Extract string values from a document field, handling Facet, Text, and Array types.
fn extract_facet_values(doc: &Document, field_name: &str) -> Vec<String> {
    match doc.fields.get(field_name) {
        Some(crate::types::FieldValue::Facet(v)) => vec![v.clone()],
        Some(crate::types::FieldValue::Text(v)) => vec![v.clone()],
        Some(crate::types::FieldValue::Array(arr)) => arr
            .iter()
            .filter_map(|item| match item {
                crate::types::FieldValue::Facet(v) => Some(v.clone()),
                crate::types::FieldValue::Text(v) => Some(v.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
