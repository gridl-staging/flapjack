//! Structural validation for HA soak harness files.
//!
//! These tests verify that key harness components (scripts, compose topology,
//! k6 scenarios, shared helpers) exist and contain expected content.
//! Pure filesystem checks — no Docker, no network, no async.

use std::fs;
use std::path::PathBuf;

fn engine_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_engine_file(relative_path: &str) -> String {
    let path = engine_dir().join(relative_path);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()))
}

fn assert_contains(content: &str, needle: &str, file_label: &str) {
    assert!(
        content.contains(needle),
        "{file_label} must contain `{needle}`"
    );
}

#[test]
fn test_ha_soak_harness_script_structure() {
    let content = read_engine_file("_dev/s/manual-tests/ha-soak-test.sh");

    // 7 required function definitions
    let required_functions = [
        "start_cluster",
        "wait_lb_and_nodes_healthy",
        "run_k6_soak",
        "restart_next_node",
        "sample_cluster_state",
        "assert_final_convergence",
        "cleanup",
    ];
    for func in &required_functions {
        assert_contains(&content, func, "ha-soak-test.sh");
    }

    // Default env vars
    assert_contains(
        &content,
        "FLAPJACK_LOADTEST_SOAK_DURATION:-2h",
        "ha-soak-test.sh",
    );
    assert_contains(
        &content,
        "FLAPJACK_LOADTEST_BASE_URL:-http://127.0.0.1:7800",
        "ha-soak-test.sh",
    );

    // Source lines for both shared helper libs
    assert_contains(&content, "loadtest_shell_helpers.sh", "ha-soak-test.sh");
    assert_contains(&content, "loadtest_soak_helpers.sh", "ha-soak-test.sh");
}

#[test]
fn test_ha_docker_compose_topology() {
    let content = read_engine_file("examples/ha-cluster/docker-compose.yml");

    // Required services
    for service in &["node-a", "node-b", "node-c", "lb"] {
        assert_contains(&content, service, "docker-compose.yml");
    }

    // Network name
    assert_contains(&content, "fj-net", "docker-compose.yml");

    // LB port mapping
    assert_contains(&content, "7800:80", "docker-compose.yml");
}

#[test]
fn test_k6_mixed_soak_scenario_exists() {
    let content = read_engine_file("loadtest/scenarios/mixed-soak.js");

    assert_contains(&content, "SOAK_WRITE_THRESHOLDS", "mixed-soak.js");
    assert_contains(&content, "sharedLoadtestConfig", "mixed-soak.js");
}

#[test]
fn test_shared_loadtest_helpers_present() {
    let shell_helpers = read_engine_file("loadtest/lib/loadtest_shell_helpers.sh");
    assert_contains(
        &shell_helpers,
        "require_loadtest_command",
        "loadtest_shell_helpers.sh",
    );
    assert_contains(
        &shell_helpers,
        "run_k6_scenario",
        "loadtest_shell_helpers.sh",
    );

    let soak_helpers = read_engine_file("loadtest/lib/loadtest_soak_helpers.sh");
    assert_contains(
        &soak_helpers,
        "create_loadtest_results_dir",
        "loadtest_soak_helpers.sh",
    );
    assert_contains(
        &soak_helpers,
        "run_loadtest_scenario_with_artifacts",
        "loadtest_soak_helpers.sh",
    );
}
