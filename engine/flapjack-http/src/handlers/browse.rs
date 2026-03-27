//! Handler for the `/1/indexes/{indexName}/browse` endpoint, implementing cursor-based iteration over all documents in an index with optional query, filter, and attribute projection support.
use axum::{
    extract::{Path, State},
    Json,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::AppState;
use crate::dto::SearchRequest;
use crate::filter_parser::parse_filter;
use crate::handlers::search::build_params_echo;
use flapjack::error::FlapjackError;
use flapjack::index::SearchOptions;

use flapjack::types::field_value_to_json_value;

/// Deserialized body for the browse endpoint, supporting cursor-based pagination, optional query/filter parameters, and an Algolia-compatible `params` query-string override.
#[derive(Deserialize)]
pub struct BrowseRequest {
    #[serde(default)]
    pub cursor: Option<String>,

    #[serde(default)]
    pub query: String,

    #[serde(default)]
    pub filters: Option<String>,

    #[serde(default = "default_browse_hits_per_page")]
    #[serde(rename = "hitsPerPage")]
    pub hits_per_page: usize,

    #[serde(default, rename = "attributesToRetrieve")]
    pub attributes_to_retrieve: Option<Vec<String>>,

    #[serde(default)]
    pub params: Option<String>,
}

fn default_browse_hits_per_page() -> usize {
    1000
}

#[derive(Serialize, Deserialize)]
struct BrowseCursor {
    offset: usize,
    generation: u64,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    filters: Option<String>,
    #[serde(default, rename = "hitsPerPage")]
    hits_per_page: Option<usize>,
}

impl BrowseRequest {
    /// Merge the URL-encoded `params` query string into the top-level fields.
    ///
    /// Delegates to `SearchRequest::apply_params_string` so that key-value pairs inside `params` (e.g. `filters=...&hitsPerPage=50`) override the corresponding fields on this request. Consumes `self.params` in the process.
    fn apply_params_string(&mut self) {
        let mut req = SearchRequest {
            query: self.query.clone(),
            filters: self.filters.clone(),
            hits_per_page: Some(self.hits_per_page),
            attributes_to_retrieve: self.attributes_to_retrieve.clone(),
            params: self.params.take(),
            ..Default::default()
        };

        req.apply_params_string();

        self.query = req.query;
        self.filters = req.filters;
        self.hits_per_page = req.hits_per_page.unwrap_or(default_browse_hits_per_page());
        self.attributes_to_retrieve = req.attributes_to_retrieve;
    }
}

/// Browse all documents in an index with pagination
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/browse",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Browse request with optional cursor"),
    responses(
        (status = 200, description = "Documents page with cursor", body = serde_json::Value),
        (status = 404, description = "Index not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn browse_index(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    Json(mut req): Json<BrowseRequest>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    req.apply_params_string();

    let index = state.manager.get_or_load(&index_name)?;
    let reader = index.reader();
    let searcher = reader.searcher();
    let current_generation = searcher
        .segment_readers()
        .iter()
        .map(|sr| sr.segment_id().uuid_string())
        .collect::<Vec<_>>()
        .join("-");
    let current_gen_hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        current_generation.hash(&mut hasher);
        hasher.finish()
    };

    let mut effective_query = req.query.clone();
    let mut effective_filters = req.filters.clone();
    let mut effective_hits_per_page = req.hits_per_page;

    let (offset, _expected_gen) = if let Some(cursor_str) = &req.cursor {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(cursor_str)
            .map_err(|_| FlapjackError::InvalidQuery("Invalid cursor".to_string()))?;
        let cursor_json = String::from_utf8(decoded)
            .map_err(|_| FlapjackError::InvalidQuery("Invalid cursor encoding".to_string()))?;
        let cursor: BrowseCursor = serde_json::from_str(&cursor_json)
            .map_err(|_| FlapjackError::InvalidQuery("Invalid cursor format".to_string()))?;

        if cursor.generation != current_gen_hash {
            return Err(FlapjackError::InvalidQuery(
                "Cursor is not valid anymore (index modified)".to_string(),
            ));
        }

        if let Some(cursor_query) = cursor.query {
            effective_query = cursor_query;
        }
        if let Some(cursor_filters) = cursor.filters {
            effective_filters = Some(cursor_filters);
        }
        if let Some(cursor_hpp) = cursor.hits_per_page {
            effective_hits_per_page = cursor_hpp;
        }

        (cursor.offset, Some(cursor.generation))
    } else {
        (0, None)
    };

    let filter = if let Some(filter_str) = &effective_filters {
        Some(
            parse_filter(filter_str)
                .map_err(|e| FlapjackError::InvalidQuery(format!("Filter parse error: {}", e)))?,
        )
    } else {
        None
    };

    let hits_per_page = effective_hits_per_page.min(1000);
    if hits_per_page == 0 {
        return Err(FlapjackError::InvalidQuery(
            "hitsPerPage must be between 1 and 1000".to_string(),
        ));
    }

    let result = state.manager.search_with_options(
        &index_name,
        &effective_query,
        &SearchOptions {
            filter: filter.as_ref(),
            limit: hits_per_page,
            offset,
            ..Default::default()
        },
    )?;
    // Browsing is cursor-based iteration across the full result set; paginationLimitedTo is
    // intentionally not enforced here (Algolia browse semantics expect all docs can be scanned).

    let total = result.total;
    let page_docs = &result.documents;

    let hits: Vec<serde_json::Value> = page_docs
        .iter()
        .map(|scored_doc| {
            let mut doc_map = serde_json::Map::new();
            doc_map.insert(
                "objectID".to_string(),
                serde_json::Value::String(scored_doc.document.id.clone()),
            );

            for (key, value) in &scored_doc.document.fields {
                if let Some(ref attrs) = req.attributes_to_retrieve {
                    if !attrs.contains(key) && !attrs.iter().any(|a| a == "*") {
                        continue;
                    }
                }
                doc_map.insert(key.clone(), field_value_to_json_value(value));
            }

            serde_json::Value::Object(doc_map)
        })
        .collect();

    let next_offset = offset + hits.len();
    let next_cursor = if next_offset < total {
        let cursor = BrowseCursor {
            offset: next_offset,
            generation: current_gen_hash,
            query: Some(effective_query.clone()),
            filters: effective_filters.clone(),
            hits_per_page: Some(hits_per_page),
        };
        let cursor_json = serde_json::to_string(&cursor).unwrap();
        Some(base64::engine::general_purpose::STANDARD.encode(cursor_json.as_bytes()))
    } else {
        None
    };

    let nb_pages = if total == 0 {
        0
    } else {
        total.div_ceil(hits_per_page)
    };
    let params_echo = build_params_echo(&SearchRequest {
        query: effective_query.clone(),
        filters: effective_filters.clone(),
        hits_per_page: Some(hits_per_page),
        attributes_to_retrieve: req.attributes_to_retrieve.clone(),
        ..Default::default()
    });

    let mut response = serde_json::Map::new();
    response.insert("hits".to_string(), serde_json::Value::Array(hits));
    response.insert("nbHits".to_string(), serde_json::json!(total));
    response.insert("page".to_string(), serde_json::json!(0));
    response.insert("nbPages".to_string(), serde_json::json!(nb_pages));
    response.insert("hitsPerPage".to_string(), serde_json::json!(hits_per_page));
    response.insert(
        "query".to_string(),
        serde_json::Value::String(effective_query),
    );
    response.insert("params".to_string(), serde_json::Value::String(params_echo));
    response.insert(
        "cursor".to_string(),
        match next_cursor {
            Some(cursor) => serde_json::Value::String(cursor),
            None => serde_json::Value::Null,
        },
    );

    Ok(Json(serde_json::Value::Object(response)))
}
