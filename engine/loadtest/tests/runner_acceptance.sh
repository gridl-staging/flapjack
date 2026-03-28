#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER_FILE="$ROOT_DIR/run.sh"
SEED_FILE="$ROOT_DIR/seed-loadtest-data.sh"
HELPER_FILE="$ROOT_DIR/lib/loadtest_shell_helpers.sh"

fail() {
  echo "FAIL: $1"
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing $path"
}

require_pattern() {
  local path="$1"
  local pattern="$2"
  local message="$3"
  grep -Eq "$pattern" "$path" || fail "$message"
}

extract_function_body() {
  local file="$1"
  local function_name="$2"
  awk -v fn="$function_name" '
    $0 ~ ("^" fn "\\(\\)[[:space:]]*\\{") { in_fn=1; depth=1; next }
    in_fn {
      if ($0 ~ /\{/) depth += gsub(/\{/, "{")
      if ($0 ~ /\}/) depth -= gsub(/\}/, "}")
      if (depth <= 0) exit
      print
    }
  ' "$file"
}

line_number_in_body() {
  local body="$1"
  local pattern="$2"
  awk -v pat="$pattern" '
    $0 ~ pat { print NR; exit }
  ' <<<"$body"
}

line_number_after_in_body() {
  local body="$1"
  local start_line="$2"
  local pattern="$3"
  awk -v start="$start_line" -v pat="$pattern" '
    NR > start && $0 ~ pat { print NR; exit }
  ' <<<"$body"
}

require_file "$RUNNER_FILE"
require_file "$SEED_FILE"
require_file "$HELPER_FILE"

require_pattern "$RUNNER_FILE" "loadtest_shell_helpers\\.sh" \
  "run.sh must source lib/loadtest_shell_helpers.sh"
require_pattern "$SEED_FILE" "loadtest_shell_helpers\\.sh" \
  "seed-loadtest-data.sh must source lib/loadtest_shell_helpers.sh"

if grep -Eq "^recreate_index\\(\\)|^apply_settings\\(\\)" "$RUNNER_FILE"; then
  fail "run.sh must not define local recreate_index/apply_settings reset helpers"
fi
if grep -Eq 'curl[[:space:]].*/1/indexes/|loadtest_http_request[[:space:]]+(DELETE|POST|PUT)[[:space:]]+\"/1/indexes/' "$RUNNER_FILE"; then
  fail "run.sh must not duplicate inline /1/indexes HTTP reset/settings flows; use shared shell helpers and seed script"
fi

require_pattern "$HELPER_FILE" "^reset_loadtest_index\\(\\)" \
  "lib/loadtest_shell_helpers.sh must define reset_loadtest_index()"
require_pattern "$HELPER_FILE" "^apply_loadtest_index_settings\\(\\)" \
  "lib/loadtest_shell_helpers.sh must define apply_loadtest_index_settings()"
require_pattern "$HELPER_FILE" "^load_dashboard_seed_settings\\(\\)" \
  "lib/loadtest_shell_helpers.sh must define load_dashboard_seed_settings()"

if grep -Eq "^recreate_index\\(\\)|^apply_settings\\(\\)" "$SEED_FILE"; then
  fail "seed-loadtest-data.sh must reuse shared reset/settings helpers, not local recreate_index/apply_settings"
fi

require_pattern "$SEED_FILE" 'reset_loadtest_index[[:space:]]+".*FLAPJACK_READ_INDEX"' \
  "seed-loadtest-data.sh must reset the read index through reset_loadtest_index"
require_pattern "$SEED_FILE" 'reset_loadtest_index[[:space:]]+".*FLAPJACK_WRITE_INDEX"' \
  "seed-loadtest-data.sh must reset the write index through reset_loadtest_index"
require_pattern "$SEED_FILE" 'apply_loadtest_index_settings[[:space:]]+".*FLAPJACK_READ_INDEX"' \
  "seed-loadtest-data.sh must apply read settings through apply_loadtest_index_settings"
require_pattern "$SEED_FILE" 'apply_loadtest_index_settings[[:space:]]+".*FLAPJACK_WRITE_INDEX"' \
  "seed-loadtest-data.sh must apply write settings through apply_loadtest_index_settings"

require_pattern "$RUNNER_FILE" "^run_k6_scenario\\(\\)" \
  "run.sh must define run_k6_scenario()"
require_pattern "$RUNNER_FILE" 'k6[[:space:]]+run[[:space:]]+--out[[:space:]]+json=.*\$\{RESULTS_DIR\}/\$\{scenario_name\}\.json' \
  "run_k6_scenario must emit JSON output at engine/loadtest/results/<timestamp>/<scenario>.json"
require_pattern "$RUNNER_FILE" '\$\{RESULTS_DIR\}/\$\{scenario_name\}\.stdout\.txt' \
  "run_k6_scenario must write a stdout summary artifact per scenario"

start_server_body="$(extract_function_body "$RUNNER_FILE" "start_server")"
[[ -n "$start_server_body" ]] || fail "run.sh must define start_server()"
if ! grep -Eq 'auth-required' <<<"$start_server_body"; then
  fail "start_server must support an auth-required mode for pressure-pass internal routes"
fi
if ! grep -Eq -- '--no-auth' <<<"$start_server_body"; then
  fail "start_server must retain explicit --no-auth launch support for the normal pass"
fi
if ! grep -Eq 'FLAPJACK_ADMIN_KEY=' <<<"$start_server_body"; then
  fail "start_server auth-required mode must launch with FLAPJACK_ADMIN_KEY so internal routes stay available"
fi

wait_for_health_body="$(extract_function_body "$RUNNER_FILE" "wait_for_health")"
[[ -n "$wait_for_health_body" ]] || fail "run.sh must define wait_for_health()"
if ! grep -Eq '%\{http_code\}' <<<"$wait_for_health_body"; then
  fail "wait_for_health must read the /health HTTP status code instead of relying on curl transport success"
fi
if ! grep -Eq '==[[:space:]]*"200"' <<<"$wait_for_health_body"; then
  fail "wait_for_health must require an HTTP 200 response from /health"
fi

run_smoke_gate_body="$(extract_function_body "$RUNNER_FILE" "run_smoke_gate")"
[[ -n "$run_smoke_gate_body" ]] || fail "run.sh must define run_smoke_gate()"
if ! grep -Eq 'run_k6_scenario[[:space:]]+"smoke"[[:space:]]+"scenarios/smoke\.js"' <<<"$run_smoke_gate_body"; then
  fail "run_smoke_gate must invoke run_k6_scenario for scenarios/smoke.js"
fi

run_normal_pass_body="$(extract_function_body "$RUNNER_FILE" "run_normal_pass")"
[[ -n "$run_normal_pass_body" ]] || fail "run.sh must define run_normal_pass()"
if grep -Eq 'k6[[:space:]]+run' <<<"$run_normal_pass_body"; then
  fail "run_normal_pass must use run_k6_scenario, not direct k6 run calls"
fi
require_pattern "$RUNNER_FILE" "run_smoke_gate[[:space:]]*\\|\\|[[:space:]]*return[[:space:]]+1" \
  "run_normal_pass must abort the suite if smoke fails"

smoke_line="$(line_number_in_body "$run_normal_pass_body" 'run_smoke_gate')"
search_line="$(line_number_in_body "$run_normal_pass_body" 'run_k6_scenario[[:space:]]+"search-throughput"[[:space:]]+"scenarios/search-throughput\.js"')"
write_line="$(line_number_in_body "$run_normal_pass_body" 'run_k6_scenario[[:space:]]+"write-throughput"[[:space:]]+"scenarios/write-throughput\.js"')"
mixed_line="$(line_number_in_body "$run_normal_pass_body" 'run_k6_scenario[[:space:]]+"mixed-workload"[[:space:]]+"scenarios/mixed-workload\.js"')"
spike_line="$(line_number_in_body "$run_normal_pass_body" 'run_k6_scenario[[:space:]]+"spike"[[:space:]]+"scenarios/spike\.js"')"

[[ -n "$smoke_line" && -n "$search_line" && -n "$write_line" && -n "$mixed_line" && -n "$spike_line" ]] || \
  fail "run_normal_pass must execute smoke, search-throughput, write-throughput, mixed-workload, then spike"
if ! (( smoke_line < search_line && search_line < write_line && write_line < mixed_line && mixed_line < spike_line )); then
  fail "run_normal_pass scenario order must be smoke -> search-throughput -> write-throughput -> mixed-workload -> spike"
fi

write_reset_before_write_line="$(line_number_in_body "$run_normal_pass_body" 'reset_loadtest_index[[:space:]]+".*FLAPJACK_WRITE_INDEX"')"
write_settings_before_write_line="$(line_number_in_body "$run_normal_pass_body" 'apply_loadtest_index_settings[[:space:]]+".*FLAPJACK_WRITE_INDEX"')"
if [[ -z "$write_reset_before_write_line" || -z "$write_settings_before_write_line" ]]; then
  fail "run_normal_pass must use shared reset + settings helpers before write-throughput"
fi

if ! (( search_line < write_reset_before_write_line &&
  write_reset_before_write_line < write_settings_before_write_line &&
  write_settings_before_write_line < write_line )); then
  fail "run_normal_pass must reset and reapply write settings immediately before write-throughput"
fi

mixed_reset_before_line="$(
  awk -v write_line="$write_line" '
    NR > write_line && $0 ~ /reset_loadtest_index[[:space:]]+".*FLAPJACK_WRITE_INDEX"/ { print NR; exit }
  ' <<<"$run_normal_pass_body"
)"
mixed_settings_before_line="$(
  awk -v reset_line="${mixed_reset_before_line:-0}" '
    NR > reset_line && $0 ~ /apply_loadtest_index_settings[[:space:]]+".*FLAPJACK_WRITE_INDEX"/ { print NR; exit }
  ' <<<"$run_normal_pass_body"
)"
if [[ -z "$mixed_reset_before_line" || -z "$mixed_settings_before_line" ]]; then
  fail "run_normal_pass must use shared reset + settings helpers before mixed-workload"
fi

if ! (( write_line < mixed_reset_before_line &&
  mixed_reset_before_line < mixed_settings_before_line &&
  mixed_settings_before_line < mixed_line )); then
  fail "run_normal_pass must reset and reapply write settings immediately before mixed-workload"
fi

require_pattern "$RUNNER_FILE" "^configure_memory_pressure_env\\(\\)" \
  "run.sh must define one dedicated configure_memory_pressure_env() helper"
pressure_helper_body="$(extract_function_body "$RUNNER_FILE" "configure_memory_pressure_env")"
for required_var in FLAPJACK_MEMORY_LIMIT_MB FLAPJACK_MEMORY_HIGH_WATERMARK FLAPJACK_MEMORY_CRITICAL; do
  if ! grep -Eq "${required_var}=" <<<"$pressure_helper_body"; then
    fail "configure_memory_pressure_env must assign ${required_var}"
  fi
done

run_memory_pressure_body="$(extract_function_body "$RUNNER_FILE" "run_memory_pressure_pass")"
[[ -n "$run_memory_pressure_body" ]] || fail "run.sh must define run_memory_pressure_pass()"
if grep -Eq 'search-throughput\.js|write-throughput\.js|mixed-workload\.js|spike\.js' <<<"$run_memory_pressure_body"; then
  fail "run_memory_pressure_pass must only run scenarios/memory-pressure.js"
fi
if ! grep -Eq 'run_k6_scenario[[:space:]]+"memory-pressure"[[:space:]]+"scenarios/memory-pressure\.js"' <<<"$run_memory_pressure_body"; then
  fail "run_memory_pressure_pass must execute memory-pressure.js via run_k6_scenario"
fi
if ! grep -Eq 'start_server[[:space:]]+"auth-required"' <<<"$run_memory_pressure_body"; then
  fail "run_memory_pressure_pass must restart the server in auth-required mode before memory-pressure.js"
fi

for required_call in stop_server configure_memory_pressure_env start_server wait_for_health seed-loadtest-data.sh; do
  grep -Eq "$required_call" <<<"$run_memory_pressure_body" || \
    fail "run_memory_pressure_pass must include ${required_call}"
done

stop_server_line="$(line_number_in_body "$run_memory_pressure_body" 'stop_server')"
configure_memory_line="$(line_number_in_body "$run_memory_pressure_body" 'configure_memory_pressure_env')"
start_server_line="$(line_number_in_body "$run_memory_pressure_body" 'start_server')"
pre_seed_wait_line="$(line_number_after_in_body "$run_memory_pressure_body" "$start_server_line" 'wait_for_health')"
seed_line="$(line_number_after_in_body "$run_memory_pressure_body" "${pre_seed_wait_line:-0}" 'seed-loadtest-data\.sh')"
post_seed_wait_line="$(line_number_after_in_body "$run_memory_pressure_body" "${seed_line:-0}" 'wait_for_health')"
run_memory_scenario_line="$(line_number_in_body "$run_memory_pressure_body" 'run_k6_scenario[[:space:]]+"memory-pressure"[[:space:]]+"scenarios/memory-pressure\.js"')"

if [[ -z "$stop_server_line" || -z "$configure_memory_line" || -z "$start_server_line" || -z "$pre_seed_wait_line" || -z "$seed_line" || -z "$post_seed_wait_line" || -z "$run_memory_scenario_line" ]]; then
  fail "run_memory_pressure_pass must include stop, configure, restart, pre-seed health wait, reseed, post-seed health wait, and memory-pressure execution steps"
fi

if ! (( stop_server_line < configure_memory_line &&
  configure_memory_line < start_server_line &&
  start_server_line < pre_seed_wait_line &&
  pre_seed_wait_line < seed_line &&
  seed_line < post_seed_wait_line &&
  post_seed_wait_line < run_memory_scenario_line )); then
  fail "run_memory_pressure_pass must enforce stop -> configure env -> start -> wait_for_health -> reseed -> wait_for_health -> memory-pressure ordering"
fi

if grep -Eq 'FLAPJACK_MEMORY_LIMIT_MB=|FLAPJACK_MEMORY_HIGH_WATERMARK=|FLAPJACK_MEMORY_CRITICAL=' <<<"$run_memory_pressure_body"; then
  fail "run_memory_pressure_pass must not inline memory thresholds; use configure_memory_pressure_env"
fi

echo "PASS: Stage 4 runner acceptance checks"
