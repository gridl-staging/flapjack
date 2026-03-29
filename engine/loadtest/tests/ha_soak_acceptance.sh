#!/usr/bin/env bash
set -euo pipefail

LOADTEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$(cd "$LOADTEST_DIR/.." && pwd)"
HARNESS_SCRIPT="$ENGINE_DIR/_dev/s/manual-tests/ha-soak-test.sh"

fail() {
  echo "FAIL: $1"
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing $path"
}

require_text() {
  local pattern="$1"
  local message="$2"
  grep -Eq "$pattern" "$HARNESS_SCRIPT" || fail "$message"
}

require_file "$HARNESS_SCRIPT"
[[ -x "$HARNESS_SCRIPT" ]] || fail "harness must be executable: $HARNESS_SCRIPT"

for helper_name in \
  start_cluster \
  wait_lb_and_nodes_healthy \
  run_k6_soak \
  restart_next_node \
  sample_cluster_state \
  assert_final_convergence \
  cleanup; do
  require_text "^${helper_name}\(\)" "harness must define helper ${helper_name}()"
done

require_text 'FLAPJACK_LOADTEST_SOAK_DURATION:-2h' \
  'harness must default FLAPJACK_LOADTEST_SOAK_DURATION to 2h for local runs'
require_text 'FLAPJACK_LOADTEST_BASE_URL:-http://127\.0\.0\.1:7800' \
  'harness must default FLAPJACK_LOADTEST_BASE_URL to http://127.0.0.1:7800'
require_text 'engine/examples/ha-cluster/docker-compose\.yml' \
  'harness must target engine/examples/ha-cluster/docker-compose.yml'
require_text 'engine/loadtest/lib/loadtest_shell_helpers\.sh' \
  'harness must source engine/loadtest/lib/loadtest_shell_helpers.sh'
require_text 'engine/loadtest/lib/loadtest_soak_helpers\.sh' \
  'harness must source shared loadtest soak helpers from engine/loadtest/lib/'
require_text 'engine/loadtest/scenarios/' \
  'harness must run an existing soak scenario from engine/loadtest/scenarios/'
require_text 'RESTART_NODES=\("node-a" "node-b" "node-c"\)' \
  'harness must rotate restarts across node-a/node-b/node-c'
require_text 'create_loadtest_results_dir.+\"ha-soak\"' \
  'harness must write timestamped artifacts under engine/loadtest/results/<timestamp>-ha-soak/'
require_text '^assert_final_convergence\(\)' \
  'harness must provide a final convergence assertion helper'
require_text 'assert_final_convergence' \
  'harness must invoke assert_final_convergence before successful exit'
require_text 'docker compose .*exec -T node-[abc]' \
  'harness must inspect individual nodes via docker compose exec -T node-{a,b,c}'

echo "PASS: Stage 5 HA soak acceptance checks"
