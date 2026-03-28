#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_BASE_DIR="${RESULTS_BASE_DIR:-$SCRIPT_DIR/results}"
BASELINE_COMPILER="${BASELINE_COMPILER:-$SCRIPT_DIR/compile_baseline.mjs}"
BENCHMARKS_FILE="${BENCHMARKS_FILE:-$SCRIPT_DIR/BENCHMARKS.md}"
DASHBOARD_REPORT="${DASHBOARD_REPORT:-$SCRIPT_DIR/../dashboard/playwright-report/results.json}"
BASELINE_BUILD_MODE="${BASELINE_BUILD_MODE:-unspecified}"
BASELINE_IMPORT_COMMAND="${BASELINE_IMPORT_COMMAND:-bash engine/loadtest/import_benchmark.sh}"
BASELINE_SEARCH_COMMAND="${BASELINE_SEARCH_COMMAND:-bash engine/loadtest/search_benchmark.sh}"
BASELINE_K6_COMMAND="${BASELINE_K6_COMMAND:-bash engine/loadtest/run.sh}"
K6_SCENARIOS=(smoke search-throughput write-throughput mixed-workload spike memory-pressure)

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

list_result_dirs_desc() {
  local -a result_paths=()
  local result_path

  shopt -s nullglob
  result_paths=("$RESULTS_BASE_DIR"/*)
  shopt -u nullglob

  for result_path in "${result_paths[@]}"; do
    [[ -d "$result_path" ]] || continue
    basename "$result_path"
  done | sort -r
}

find_latest_artifact() {
  local file_name="$1"
  local result_dir

  while IFS= read -r result_dir; do
    if [[ -f "$RESULTS_BASE_DIR/$result_dir/$file_name" ]]; then
      echo "$RESULTS_BASE_DIR/$result_dir/$file_name"
      return 0
    fi
  done < <(list_result_dirs_desc)
}

# TODO: Document find_latest_complete_k6_dir.
find_latest_complete_k6_dir() {
  local result_dir
  local scenario_name

  while IFS= read -r result_dir; do
    local has_all_scenarios=1
    for scenario_name in "${K6_SCENARIOS[@]}"; do
      if [[ ! -f "$RESULTS_BASE_DIR/$result_dir/${scenario_name}.json" && ! -f "$RESULTS_BASE_DIR/$result_dir/${scenario_name}.stdout.txt" ]]; then
        has_all_scenarios=0
        break
      fi
    done
    if [[ "$has_all_scenarios" -eq 1 ]]; then
      echo "$RESULTS_BASE_DIR/$result_dir"
      return 0
    fi
  done < <(list_result_dirs_desc)
}

discover_search_vus() {
  local scenario_file="$SCRIPT_DIR/scenarios/search-throughput.js"
  if [[ ! -f "$scenario_file" ]]; then
    echo "n/a"
    return 0
  fi

  local max_vus
  max_vus="$(grep -Eo 'target:[[:space:]]*[0-9]+' "$scenario_file" | awk '{print $2}' | sort -nr | head -n 1)"
  if [[ -z "$max_vus" ]]; then
    echo "n/a"
    return 0
  fi
  echo "$max_vus"
}

# TODO: Document main.
main() {
  [[ -f "$BASELINE_COMPILER" ]] || fail "missing $BASELINE_COMPILER"
  [[ -f "$BENCHMARKS_FILE" ]] || fail "missing $BENCHMARKS_FILE"

  local import_artifact search_artifact
  import_artifact="$(find_latest_artifact "import_benchmark.json")"
  search_artifact="$(find_latest_artifact "search_benchmark.json")"
  [[ -n "$import_artifact" ]] || fail "could not find import_benchmark.json under $RESULTS_BASE_DIR/*/"
  [[ -n "$search_artifact" ]] || fail "could not find search_benchmark.json under $RESULTS_BASE_DIR/*/"

  local -a compiler_args
  compiler_args=(
    "--import-artifact" "$import_artifact"
    "--search-artifact" "$search_artifact"
    "--build-mode" "$BASELINE_BUILD_MODE"
    "--import-command" "$BASELINE_IMPORT_COMMAND"
    "--search-command" "$BASELINE_SEARCH_COMMAND"
    "--k6-command" "$BASELINE_K6_COMMAND"
  )

  local k6_search_vus
  k6_search_vus="$(discover_search_vus)"
  if [[ "$k6_search_vus" != "n/a" ]]; then
    compiler_args+=("--k6-search-vus" "$k6_search_vus")
  fi

  if [[ -f "$DASHBOARD_REPORT" ]]; then
    compiler_args+=("--dashboard-report" "$DASHBOARD_REPORT")
  fi

  local k6_results_dir
  k6_results_dir="$(find_latest_complete_k6_dir)"
  [[ -n "$k6_results_dir" ]] || fail "missing complete k6 artifact set for scenarios: ${K6_SCENARIOS[*]}"

  local scenario_name scenario_json scenario_stdout
  for scenario_name in "${K6_SCENARIOS[@]}"; do
    scenario_json=""
    scenario_stdout=""

    if [[ -f "$k6_results_dir/${scenario_name}.json" ]]; then
      scenario_json="$k6_results_dir/${scenario_name}.json"
    fi
    if [[ -f "$k6_results_dir/${scenario_name}.stdout.txt" ]]; then
      scenario_stdout="$k6_results_dir/${scenario_name}.stdout.txt"
    fi
    if [[ -n "$scenario_json" ]]; then
      compiler_args+=("--k6-json" "${scenario_name}=${scenario_json}")
    fi
    if [[ -n "$scenario_stdout" ]]; then
      compiler_args+=("--k6-stdout" "${scenario_name}=${scenario_stdout}")
    fi
  done

  local section_markdown
  section_markdown="$(node "$BASELINE_COMPILER" "${compiler_args[@]}")"

  {
    echo ""
    echo "---"
    echo ""
    echo "$section_markdown"
  } >> "$BENCHMARKS_FILE"

  echo "INFO: appended Large-Dataset Baseline section to $BENCHMARKS_FILE"
}

main "$@"
