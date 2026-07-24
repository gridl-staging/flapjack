const OPERATIONS_CONSUMER_CONTRACT: &str =
    include_str!("../../docs2/operations_consumer_contract.md");
const FEATURES: &str = include_str!("../../docs2/FEATURES.md");

#[test]
fn operations_consumer_contract_names_runtime_and_screen_spec_owners() {
    assert_contains_all(
        OPERATIONS_CONSUMER_CONTRACT,
        &[
            "docs/screen_specs/system.md",
            "docs/screen_specs/cluster.md",
            "docs/screen_specs/snapshots.md",
            "engine/flapjack-http/src/handlers/health.rs::health",
            "engine/flapjack-http/src/handlers/internal.rs::replication_status",
            "engine/flapjack-http/src/handlers/internal.rs::cluster_status",
            "engine/flapjack-http/src/handlers/snapshot.rs::snapshot_capability",
            "engine/flapjack-http/src/openapi.rs",
            "engine/docs2/openapi.json",
        ],
    );
}

#[test]
fn operations_consumer_contract_publishes_r5_wire_contract() {
    assert_contains_all(
        OPERATIONS_CONSUMER_CONTRACT,
        &[
            "/health",
            "14 top-level keys",
            "5 seconds",
            "/internal/status",
            "10 seconds",
            "\"unknown\"",
            "/internal/cluster/status",
            "replication_enabled: false",
            "replication_enabled: true",
            "peers_total",
            "peers_healthy",
            "/internal/snapshots/capability",
            "not_configured",
            "configured_unverified",
        ],
    );
}

#[test]
fn operations_consumer_contract_captures_known_bounds() {
    assert_contains_all(
        OPERATIONS_CONSUMER_CONTRACT,
        &[
            "build_profile",
            "not a top-level `/health` field",
            "schemaVersion",
            "profile",
            "intentionally omits revision, dirty state, workspace digest, target triple, and feature list",
            "S3 config is present",
            "credentials, bucket existence, or reachability have not been checked",
            "`bucket` field is always present",
            "`null` when no bucket is configured",
            "`peers: []`",
            "request or parse failure",
            "consumer error state",
        ],
    );
}

#[test]
fn features_mentions_published_operations_contracts() {
    assert_contains_all(
        FEATURES,
        &[
            "Published operations APIs",
            "operations_consumer_contract.md",
            "configured_unverified",
            "engine/flapjack-http/src/handlers/experiments/mod.rs::list_experiments",
            "exact `indexName`",
            "`indexPrefix`",
        ],
    );
}

fn assert_contains_all(document: &str, expected_fragments: &[&str]) {
    let normalized_document = normalize_whitespace(document);
    for fragment in expected_fragments {
        let normalized_fragment = normalize_whitespace(fragment);
        assert!(
            normalized_document.contains(&normalized_fragment),
            "document should contain `{fragment}`"
        );
    }
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
