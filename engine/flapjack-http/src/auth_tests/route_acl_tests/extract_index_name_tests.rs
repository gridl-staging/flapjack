use super::super::*;

// ── extract_index_name ──

#[test]
fn extract_index_name_valid() {
    assert_eq!(
        extract_index_name("/1/indexes/products/query"),
        Some("products".to_string())
    );
}

#[test]
fn extract_index_name_just_index() {
    assert_eq!(
        extract_index_name("/1/indexes/myindex"),
        Some("myindex".to_string())
    );
}

#[test]
fn extract_index_name_queries_excluded() {
    assert_eq!(extract_index_name("/1/indexes/queries"), None);
}

#[test]
fn extract_index_name_objects_excluded() {
    assert_eq!(extract_index_name("/1/indexes/objects"), None);
}

#[test]
fn extract_index_name_wildcard_excluded() {
    // "*" is the Algolia multi-index path marker (e.g., /1/indexes/*/queries),
    // not a real index name. The middleware must skip it so batch search
    // can enforce per-query index restrictions in the handler.
    assert_eq!(extract_index_name("/1/indexes/*/queries"), None);
    assert_eq!(extract_index_name("/1/indexes/*"), None);
}

#[test]
fn extract_index_name_too_short() {
    assert_eq!(extract_index_name("/1/indexes"), None);
}

#[test]
fn extract_index_name_wrong_prefix() {
    assert_eq!(extract_index_name("/2/indexes/foo"), None);
}
