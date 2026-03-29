#!/usr/bin/env bash

# Create a timestamped results directory and print the absolute path.
# Args:
#   1) base results directory (e.g., engine/loadtest/results)
#   2) suffix (e.g., mixed-soak, write-soak, ha-soak)
create_loadtest_results_dir() {
  local results_base_dir="$1"
  local suffix="$2"
  local timestamp
  local results_dir

  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  results_dir="$results_base_dir/${timestamp}-${suffix}"
  mkdir -p "$results_dir"
  printf '%s' "$results_dir"
}

# Run a k6 scenario from engine/loadtest/scenarios/ with artifact capture.
# Args:
#   1) engine/loadtest directory
#   2) scenario name without extension
#   3) k6 API address (empty string disables --address)
#   4) json output path
#   5) stdout capture path
run_loadtest_scenario_with_artifacts() {
  local loadtest_dir="$1"
  local scenario_name="$2"
  local k6_api_addr="$3"
  local k6_json_path="$4"
  local k6_stdout_path="$5"
  local scenario_path="scenarios/${scenario_name}.js"

  (
    cd "$loadtest_dir"
    local -a k6_args=(run)
    if [[ -n "$k6_api_addr" ]]; then
      k6_args+=(--address "$k6_api_addr")
    fi
    # Skip per-request JSON export when path is empty — callers like
    # ha-soak-test.sh only need cluster-level evidence, not per-request
    # metrics, and the JSON file grows ~16 GB/hour under sustained load.
    if [[ -n "$k6_json_path" ]]; then
      k6_args+=(--out "json=${k6_json_path}")
    fi
    k6_args+=("$scenario_path")
    k6 "${k6_args[@]}"
  ) | tee "$k6_stdout_path"
}
