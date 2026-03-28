#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADTEST_HELPERS="${LOADTEST_HELPERS:-$SCRIPT_DIR/lib/loadtest_shell_helpers.sh}"
RESULTS_BASE_DIR="$SCRIPT_DIR/results"
MIN_BENCHMARK_DOCS=90000
SCENARIO_FAILURE_COUNT=0

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# TODO: Document print_usage.
print_usage() {
  cat <<'USAGE'
Usage: benchmark_k6.sh [--help]

Runs the k6 normal-pass scenarios against an already-running Flapjack server.
This wrapper does not start or stop the server and expects benchmark_100k to
already exist and be populated.

Behavior:
  - Verifies benchmark index exists and has at least 90000 documents
  - Forces FLAPJACK_LOADTEST_READ_INDEX to FLAPJACK_LOADTEST_BENCHMARK_INDEX
  - Runs scenarios in this order:
      1) smoke (gate)
      2) search-throughput
      3) write-throughput (after write-index reset/settings)
      4) mixed-workload (after write-index reset/settings)
      5) spike
  - Writes artifacts to engine/loadtest/results/<timestamp>/

Environment variables (from lib/config.js):
  FLAPJACK_LOADTEST_BASE_URL
  FLAPJACK_LOADTEST_APP_ID
  FLAPJACK_LOADTEST_API_KEY
  FLAPJACK_LOADTEST_READ_INDEX
  FLAPJACK_LOADTEST_WRITE_INDEX
  FLAPJACK_LOADTEST_BENCHMARK_INDEX
  FLAPJACK_LOADTEST_TASK_MAX_ATTEMPTS
  FLAPJACK_LOADTEST_TASK_POLL_INTERVAL_SECONDS
USAGE
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --help|-h)
        print_usage
        exit 0
        ;;
      *)
        fail "unknown argument: $1 (use --help)"
        ;;
    esac
  done
}

create_results_dir() {
  local timestamp
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  RESULTS_DIR="$RESULTS_BASE_DIR/$timestamp"
  mkdir -p "$RESULTS_DIR"
}

# TODO: Document verify_benchmark_index_ready.
verify_benchmark_index_ready() {
  local index_name="$1"
  if ! loadtest_index_exists "$index_name"; then
    fail "index \"$index_name\" was not found"
  fi

  local doc_count
  doc_count="$(loadtest_get_index_doc_count "$index_name")"
  if [[ "$doc_count" == "0" ]]; then
    fail "index \"$index_name\" contains 0 documents"
  fi
  if ! [[ "$doc_count" =~ ^[0-9]+$ ]]; then
    fail "index \"$index_name\" returned non-numeric document count: $doc_count"
  fi
  if (( doc_count < MIN_BENCHMARK_DOCS )); then
    fail "index \"$index_name\" must contain at least ${MIN_BENCHMARK_DOCS} documents (found $doc_count)"
  fi

  BENCHMARK_DOC_COUNT="$doc_count"
}

# TODO: Document run_normal_pass.
run_normal_pass() {
  if ! run_smoke_gate; then
    echo "FAIL: smoke scenario breached thresholds; aborting remaining scenarios" >&2
    exit 99
  fi
  run_k6_scenario "search-throughput" "scenarios/search-throughput.js"

  reset_loadtest_index "$FLAPJACK_WRITE_INDEX"
  apply_loadtest_index_settings "$FLAPJACK_WRITE_INDEX"
  run_k6_scenario "write-throughput" "scenarios/write-throughput.js"

  reset_loadtest_index "$FLAPJACK_WRITE_INDEX"
  apply_loadtest_index_settings "$FLAPJACK_WRITE_INDEX"
  run_k6_scenario "mixed-workload" "scenarios/mixed-workload.js"

  run_k6_scenario "spike" "scenarios/spike.js"
}

# TODO: Document main.
main() {
  parse_args "$@"
  [[ -f "$LOADTEST_HELPERS" ]] || fail "missing $LOADTEST_HELPERS"

  # shellcheck source=lib/loadtest_shell_helpers.sh
  source "$LOADTEST_HELPERS"

  require_loadtest_commands k6 curl jq
  load_shared_loadtest_config
  initialize_loadtest_auth_headers
  load_dashboard_seed_settings "$SCRIPT_DIR"

  local benchmark_index="$FLAPJACK_BENCHMARK_INDEX"
  verify_benchmark_index_ready "$benchmark_index"
  export FLAPJACK_LOADTEST_READ_INDEX="$benchmark_index"
  echo "INFO: benchmark index '$benchmark_index' has $BENCHMARK_DOC_COUNT documents"
  echo "INFO: forcing FLAPJACK_LOADTEST_READ_INDEX=$FLAPJACK_LOADTEST_READ_INDEX"

  create_results_dir
  run_normal_pass

  echo "INFO: results written to $RESULTS_DIR"
  if [[ $SCENARIO_FAILURE_COUNT -gt 0 ]]; then
    echo "FAIL: ${SCENARIO_FAILURE_COUNT} scenario(s) breached thresholds"
    exit 99
  fi
  echo "PASS: benchmark_k6 run completed"
}

main "$@"
