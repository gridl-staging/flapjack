use crate::index::SearchOptions;
use crate::types::{FacetRequest, Filter, SearchResult, Sort};
use crate::{error::Result, IndexManager};

/// Build `SearchOptions` from the legacy positional-argument test helpers.
fn legacy_search_options<'a>(
    filter: Option<&'a Filter>,
    sort: Option<&'a Sort>,
    limit: usize,
    offset: usize,
    facets: Option<&'a [FacetRequest]>,
    distinct: Option<u32>,
    max_values_per_facet: Option<usize>,
) -> SearchOptions<'a> {
    SearchOptions {
        filter,
        sort,
        limit,
        offset,
        facets,
        distinct,
        max_values_per_facet,
        ..Default::default()
    }
}

/// Adapt legacy integration test search patterns to the new `SearchOptions`-based API.
///
/// Provides three convenience methods (`search_with_facets`, `search_with_facets_and_distinct`,
/// `search_full`) that transform positional parameters into a `SearchOptions` struct and delegate
/// to `search_with_options`. Implemented by `IndexManager` to keep integration test call sites
/// compiling during API migration.
/// TODO: Document SearchCompat.
#[allow(clippy::too_many_arguments)] // Test-only shim preserves legacy lib-test callsites while production stays `SearchOptions`-based.
pub(crate) trait SearchCompat {
    fn search_with_facets(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
    ) -> Result<SearchResult>;

    fn search_with_facets_and_distinct(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
        distinct: Option<u32>,
    ) -> Result<SearchResult>;

    fn search_full(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
        distinct: Option<u32>,
        max_values_per_facet: Option<usize>,
    ) -> Result<SearchResult>;
}

impl SearchCompat for IndexManager {
    fn search_with_facets(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
    ) -> Result<SearchResult> {
        self.search_full(
            tenant_id, query_text, filter, sort, limit, offset, facets, None, None,
        )
    }

    fn search_with_facets_and_distinct(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
        distinct: Option<u32>,
    ) -> Result<SearchResult> {
        self.search_full(
            tenant_id, query_text, filter, sort, limit, offset, facets, distinct, None,
        )
    }

    /// Bridge the old test helper signature into `search_with_options`.
    fn search_full(
        &self,
        tenant_id: &str,
        query_text: &str,
        filter: Option<&Filter>,
        sort: Option<&Sort>,
        limit: usize,
        offset: usize,
        facets: Option<&[FacetRequest]>,
        distinct: Option<u32>,
        max_values_per_facet: Option<usize>,
    ) -> Result<SearchResult> {
        let options = legacy_search_options(
            filter,
            sort,
            limit,
            offset,
            facets,
            distinct,
            max_values_per_facet,
        );
        self.search_with_options(tenant_id, query_text, &options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldValue, SortOrder};
    use tempfile::TempDir;

    #[test]
    fn legacy_search_options_maps_all_legacy_fields() {
        let filter = Filter::Equals {
            field: "category".to_string(),
            value: FieldValue::Text("books".to_string()),
        };
        let sort = Sort::ByField {
            field: "price".to_string(),
            order: SortOrder::Desc,
        };
        let facets = vec![FacetRequest {
            field: "category".to_string(),
            path: "/category".to_string(),
        }];

        let options = legacy_search_options(
            Some(&filter),
            Some(&sort),
            17,
            3,
            Some(&facets),
            Some(9),
            Some(33),
        );

        assert!(options.filter.is_some());
        assert!(options.sort.is_some());
        assert_eq!(options.limit, 17);
        assert_eq!(options.offset, 3);
        assert_eq!(options.facets.map(|f| f.len()), Some(1));
        assert_eq!(options.distinct, Some(9));
        assert_eq!(options.max_values_per_facet, Some(33));
        assert!(options.query_type.is_none());
        assert!(options.optional_filter_specs.is_none());
        assert!(!options.sum_or_filters_scores);
    }

    #[tokio::test]
    async fn search_full_matches_explicit_search_options_path() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());
        manager.create_tenant("products").unwrap();

        let docs = [
            serde_json::json!({"objectID": "a", "title": "Laptop alpha"}),
            serde_json::json!({"objectID": "b", "title": "Laptop beta"}),
            serde_json::json!({"objectID": "c", "title": "Laptop gamma"}),
        ]
        .iter()
        .map(|json| crate::types::Document::from_json(json).unwrap())
        .collect();

        manager.add_documents_sync("products", docs).await.unwrap();

        let legacy_result = manager
            .search_full("products", "laptop", None, None, 2, 1, None, None, None)
            .unwrap();
        let explicit_options = legacy_search_options(None, None, 2, 1, None, None, None);
        let explicit_result = manager
            .search_with_options("products", "laptop", &explicit_options)
            .unwrap();

        let legacy_ids: Vec<String> = legacy_result
            .documents
            .iter()
            .map(|doc| doc.document.id.clone())
            .collect();
        let explicit_ids: Vec<String> = explicit_result
            .documents
            .iter()
            .map(|doc| doc.document.id.clone())
            .collect();

        assert_eq!(legacy_result.total, explicit_result.total);
        assert_eq!(
            legacy_result.documents.len(),
            explicit_result.documents.len()
        );
        assert_eq!(legacy_ids, explicit_ids);
    }
}
