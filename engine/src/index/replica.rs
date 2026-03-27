//! Parse, validate, and classify replica entries (standard vs. virtual) from an index's `replicas` setting.
// Replica Design (§10)
//
// Two types of replicas:
//
// **Standard replica**: A physically separate index in IndexManager. Same records
// as the primary, but with independent settings (different ranking, customRanking,
// etc.). When the primary receives a write (add, update, delete, clear), the same
// operation is automatically applied to each standard replica. Storage-heavy but
// fast reads.
//
// **Virtual replica** (Stage 2): No separate data storage. At query time, the
// primary's records are searched using the virtual replica's ranking settings.
// Represented in the `replicas` setting with `virtual(name)` syntax. Light on
// storage.
//
// Virtual-resolution strategy:
//   1. Virtual replicas are settings-only entries on disk (settings.json only),
//      with no Tantivy physical index data.
//   2. Search handler loads the target index settings. If `primary` is set and
//      the replica has no physical index files, treat it as virtual.
//   3. Search executes against primary index data while applying the virtual
//      replica settings as query-time overrides.
//
// NOTE: Algolia virtual replicas use "relevant sort" (blend relevance + sort
// attribute), while standard replicas use "exhaustive sort". Our simplified
// implementation applies the virtual replica's ranking/customRanking settings
// to primary data at query time. Virtual replicas also use their stored
// `relevancyStrictness` setting (defaulting to 100 in ranking) unless a query
// overrides it; this is wired in `search/single.rs` and consumed by the
// three-branch strictness logic in `index/manager/ranking.rs`.
//
// The `replicas` setting on the primary index lists all replicas:
//   ["replica_price_asc", "virtual(replica_relevance)"]
//
// Each replica (standard or virtual) stores a `primary` field in its settings
// pointing back to the parent index. This field is read-only from the API.
//
// Write-sync flow (standard replicas only):
//   1. Handler writes to primary index
//   2. Handler reads primary's settings to get `replicas` list
//   3. For each standard replica name, apply the same write operation
//   4. If replica index doesn't exist yet, create it in IndexManager
//   5. Replica settings are never overwritten by write-sync

use crate::error::{FlapjackError, Result};
use crate::index::manager::validate_index_name;

/// Parsed replica entry from the `replicas` setting.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaEntry {
    /// Standard replica: a real index with auto-synced writes.
    Standard(String),
    /// Virtual replica: settings-only, resolved at query time against primary data.
    Virtual(String),
}

impl ReplicaEntry {
    /// The index name this replica entry refers to.
    pub fn name(&self) -> &str {
        match self {
            ReplicaEntry::Standard(name) => name,
            ReplicaEntry::Virtual(name) => name,
        }
    }

    /// Whether this is a standard (non-virtual) replica.
    pub fn is_standard(&self) -> bool {
        matches!(self, ReplicaEntry::Standard(_))
    }
}

/// Parse a single replica entry string into a `ReplicaEntry`.
///
/// Standard replicas are plain index names: `"products_price_asc"`
/// Virtual replicas use `virtual()` wrapper: `"virtual(products_relevance)"`
pub fn parse_replica_entry(entry: &str) -> Result<ReplicaEntry> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return Err(FlapjackError::InvalidQuery(
            "Replica entry must not be empty".to_string(),
        ));
    }

    if let Some(rest) = trimmed.strip_prefix("virtual(") {
        let name = rest.strip_suffix(')').ok_or_else(|| {
            FlapjackError::InvalidQuery(format!(
                "Invalid virtual replica syntax: '{}'. Expected 'virtual(name)'",
                trimmed
            ))
        })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(FlapjackError::InvalidQuery(
                "Virtual replica name must not be empty".to_string(),
            ));
        }
        super::manager::validate_index_name(name)?;
        Ok(ReplicaEntry::Virtual(name.to_string()))
    } else {
        super::manager::validate_index_name(trimmed)?;
        Ok(ReplicaEntry::Standard(trimmed.to_string()))
    }
}

/// Validate a list of replica entries for an index.
///
/// Checks:
/// - Each entry parses as standard or virtual
/// - No duplicate replica names
/// - No self-reference (replica name == primary index name)
pub fn validate_replicas(
    primary_index_name: &str,
    replicas: &[String],
) -> Result<Vec<ReplicaEntry>> {
    let mut entries = Vec::with_capacity(replicas.len());
    let mut seen_names = std::collections::HashSet::new();

    for raw in replicas {
        let entry = parse_replica_entry(raw)?;
        let name = entry.name().to_string();

        if name == primary_index_name {
            return Err(FlapjackError::InvalidQuery(format!(
                "Replica '{}' cannot reference itself",
                name
            )));
        }

        validate_index_name(&name)?;

        if !seen_names.insert(name.clone()) {
            return Err(FlapjackError::InvalidQuery(format!(
                "Duplicate replica name: '{}'",
                name
            )));
        }

        entries.push(entry);
    }

    Ok(entries)
}

/// Extract only standard replica names from a replicas list.
pub fn standard_replica_names(replicas: &[String]) -> Vec<String> {
    replicas
        .iter()
        .filter_map(|entry| {
            parse_replica_entry(entry)
                .ok()
                .filter(|e| e.is_standard())
                .map(|e| e.name().to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_standard_replica() {
        let entry = parse_replica_entry("products_price_asc").unwrap();
        assert_eq!(
            entry,
            ReplicaEntry::Standard("products_price_asc".to_string())
        );
        assert!(entry.is_standard());
    }

    #[test]
    fn parse_virtual_replica() {
        let entry = parse_replica_entry("virtual(products_relevance)").unwrap();
        assert_eq!(
            entry,
            ReplicaEntry::Virtual("products_relevance".to_string())
        );
        assert!(!entry.is_standard());
    }

    #[test]
    fn parse_empty_rejected() {
        assert!(parse_replica_entry("").is_err());
        assert!(parse_replica_entry("  ").is_err());
    }

    #[test]
    fn parse_virtual_empty_name_rejected() {
        assert!(parse_replica_entry("virtual()").is_err());
        assert!(parse_replica_entry("virtual(  )").is_err());
    }

    #[test]
    fn parse_virtual_missing_close_paren() {
        assert!(parse_replica_entry("virtual(foo").is_err());
    }

    #[test]
    fn validate_rejects_self_reference() {
        let result = validate_replicas("products", &["products".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot reference itself"), "got: {}", err);
    }

    #[test]
    fn validate_rejects_duplicates() {
        let result = validate_replicas(
            "products",
            &["replica_a".to_string(), "replica_a".to_string()],
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Duplicate"), "got: {}", err);
    }

    #[test]
    fn validate_rejects_virtual_self_reference() {
        let result = validate_replicas("products", &["virtual(products)".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn validate_rejects_invalid_index_name() {
        let result = validate_replicas("products", &["../replica".to_string()]);
        assert!(result.is_err());
    }

    /// Verify that a replica list containing both standard and virtual entries validates successfully and preserves entry order and types.
    #[test]
    fn validate_mixed_replicas_ok() {
        let entries = validate_replicas(
            "products",
            &[
                "products_price_asc".to_string(),
                "virtual(products_relevance)".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0],
            ReplicaEntry::Standard("products_price_asc".to_string())
        );
        assert_eq!(
            entries[1],
            ReplicaEntry::Virtual("products_relevance".to_string())
        );
    }

    #[test]
    fn standard_replica_names_filters_virtual() {
        let names = standard_replica_names(&[
            "price_asc".to_string(),
            "virtual(relevance)".to_string(),
            "date_desc".to_string(),
        ]);
        assert_eq!(names, vec!["price_asc", "date_desc"]);
    }
}
