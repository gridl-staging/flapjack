//! Vector similarity engine for the looking-similar recommendation model using vector embeddings.

use std::sync::Arc;

use crate::types::Document;
use crate::IndexManager;

/// A scored recommendation hit for looking-similar.
#[derive(Debug, Clone)]
pub struct LookingSimilarHit {
    pub object_id: String,
    pub score: u32, // 0-100
    pub document: Option<Document>,
}

/// Find documents with similar vector embeddings to a seed document.
///
/// Searches the vector index for documents nearest to the seed object's embedding, filtering by minimum similarity score threshold and returning the top-K results ranked by relevance. Returns empty results if the index, seed document, or embeddings are not found. Only performs searches when the `vector-search` feature is enabled; otherwise returns an empty vector.
///
/// # Arguments
///
/// * `manager` - IndexManager containing vector indices
/// * `index_name` - Name of the search index to query
/// * `seed_object_id` - ID of the document to find similar matches for
/// * `threshold` - Minimum similarity score (0-100) for results to include
/// * `max_recommendations` - Maximum number of results to return
///
/// # Returns
///
/// Vector of `LookingSimilarHit` results ranked by similarity score (highest first), or an error string if the vector index cannot be accessed or the search fails.
#[cfg(feature = "vector-search")]
pub fn compute_looking_similar(
    manager: &Arc<IndexManager>,
    index_name: &str,
    seed_object_id: &str,
    threshold: u32,
    max_recommendations: u32,
) -> Result<Vec<LookingSimilarHit>, String> {
    let Some(vector_index) = manager.get_vector_index(index_name) else {
        return Ok(Vec::new());
    };

    let vi = vector_index
        .read()
        .map_err(|e| format!("vector index read lock poisoned: {e}"))?;
    if vi.is_empty() {
        return Ok(Vec::new());
    }

    let Some(seed_vector) = vi
        .get(seed_object_id)
        .map_err(|e| format!("failed to load seed vector: {e}"))?
    else {
        return Ok(Vec::new());
    };

    let search_limit = vi.len().max(1);
    let raw = vi
        .search(&seed_vector, search_limit)
        .map_err(|e| format!("vector search failed: {e}"))?;

    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates: Vec<(String, f32)> = raw
        .into_iter()
        .filter(|r| r.doc_id != seed_object_id)
        .map(|r| (r.doc_id, r.distance))
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let min_distance = candidates
        .iter()
        .map(|(_, d)| *d)
        .fold(f32::INFINITY, f32::min);
    let max_distance = candidates
        .iter()
        .map(|(_, d)| *d)
        .fold(f32::NEG_INFINITY, f32::max);

    let mut scored: Vec<(String, u32)> = candidates
        .drain(..)
        .map(|(doc_id, distance)| {
            let score = if (max_distance - min_distance).abs() < f32::EPSILON {
                100
            } else {
                (((max_distance - distance) / (max_distance - min_distance)) * 100.0)
                    .round()
                    .clamp(0.0, 100.0) as u32
            };
            (doc_id, score)
        })
        .filter(|(_, score)| *score >= threshold)
        .collect();

    scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(max_recommendations as usize);

    Ok(scored
        .into_iter()
        .map(|(object_id, score)| {
            let document = manager.get_document(index_name, &object_id).ok().flatten();
            LookingSimilarHit {
                object_id,
                score,
                document,
            }
        })
        .collect())
}

#[cfg(not(feature = "vector-search"))]
pub fn compute_looking_similar(
    _manager: &Arc<IndexManager>,
    _index_name: &str,
    _seed_object_id: &str,
    _threshold: u32,
    _max_recommendations: u32,
) -> Result<Vec<LookingSimilarHit>, String> {
    Ok(Vec::new())
}
