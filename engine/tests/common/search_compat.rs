use flapjack::index::SearchOptions;
use flapjack::types::{FacetRequest, Filter, Sort};

/// Test-only helper for legacy faceted-search scenarios now routed through `SearchOptions`.
pub fn faceted_search_options<'a>(
    filter: Option<&'a Filter>,
    sort: Option<&'a Sort>,
    limit: usize,
    offset: usize,
    facets: Option<&'a [FacetRequest]>,
) -> SearchOptions<'a> {
    SearchOptions {
        filter,
        sort,
        limit,
        offset,
        facets,
        ..Default::default()
    }
}
