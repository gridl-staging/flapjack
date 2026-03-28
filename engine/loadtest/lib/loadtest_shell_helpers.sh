#!/usr/bin/env bash

LOADTEST_HELPER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADTEST_AUTH_HEADERS=()

require_loadtest_command() {
  local binary="$1"
  if ! command -v "$binary" >/dev/null 2>&1; then
    echo "FAIL: required command not found: $binary"
    exit 1
  fi
}

require_loadtest_commands() {
  local binary
  for binary in "$@"; do
    require_loadtest_command "$binary"
  done
}

load_shared_loadtest_config() {
  local config_json
  config_json="$(
    cd "$LOADTEST_HELPER_DIR"
    node -e 'import("./config.js").then(({ sharedLoadtestConfig }) => { console.log(JSON.stringify(sharedLoadtestConfig)); }).catch((error) => { console.error(error); process.exit(1); });'
  )"

  FLAPJACK_BASE_URL="$(jq -r '.baseUrl' <<<"$config_json")"
  FLAPJACK_READ_INDEX="$(jq -r '.readIndexName' <<<"$config_json")"
  FLAPJACK_WRITE_INDEX="$(jq -r '.writeIndexName' <<<"$config_json")"
  FLAPJACK_APP_ID="$(jq -r '.appId' <<<"$config_json")"
  FLAPJACK_API_KEY="$(jq -r '.apiKey' <<<"$config_json")"
  FLAPJACK_BENCHMARK_INDEX="$(jq -r '.benchmarkIndexName' <<<"$config_json")"
  FLAPJACK_SOAK_DURATION="$(jq -r '.soakDuration' <<<"$config_json")"
  FLAPJACK_TASK_MAX_ATTEMPTS="$(jq -r '.taskPollMaxAttempts' <<<"$config_json")"
  FLAPJACK_TASK_POLL_INTERVAL_SECONDS="$(jq -r '.taskPollIntervalSeconds' <<<"$config_json")"
}

initialize_loadtest_auth_headers() {
  LOADTEST_AUTH_HEADERS=()
  if [[ -n "${FLAPJACK_API_KEY:-}" ]]; then
    LOADTEST_AUTH_HEADERS=(
      -H "x-algolia-api-key: $FLAPJACK_API_KEY"
      -H "x-algolia-application-id: $FLAPJACK_APP_ID"
    )
  fi
}

loadtest_encode_path_component() {
  local raw_component="$1"
  if [[ -z "$raw_component" ]]; then
    echo "FAIL: URL path component must not be empty."
    exit 1
  fi

  jq -rn --arg component "$raw_component" '$component | @uri'
}

# TODO: Document loadtest_http_request.
loadtest_http_request() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  local expected_statuses_csv="${4:-200}"
  local response_file
  local status_code
  local response_body
  local -a curl_args

  response_file="$(mktemp)"
  curl_args=(curl -sS -o "$response_file" -w '%{http_code}' -X "$method")
  if [[ ${#LOADTEST_AUTH_HEADERS[@]} -gt 0 ]]; then
    curl_args+=("${LOADTEST_AUTH_HEADERS[@]}")
  fi
  if [[ -n "$payload" ]]; then
    curl_args+=(-H "Content-Type: application/json" --data "$payload")
  fi
  curl_args+=("$FLAPJACK_BASE_URL$path")

  status_code="$("${curl_args[@]}")"

  response_body="$(cat "$response_file")"
  rm -f "$response_file"

  if [[ ",${expected_statuses_csv}," != *",${status_code},"* ]]; then
    echo "FAIL: ${method} ${path} returned HTTP ${status_code}."
    echo "$response_body"
    exit 1
  fi

  printf '%s' "$response_body"
}

loadtest_list_indexes_response() {
  loadtest_http_request GET "/1/indexes" "" "200"
}

loadtest_get_index_item_json() {
  local index_name="$1"
  local response
  local item

  response="$(loadtest_list_indexes_response)"
  item="$(jq -cer --arg name "$index_name" '(.items // []) | map(select(.name == $name)) | .[0]' <<<"$response")" || {
    echo "FAIL: index ${index_name} not found in /1/indexes response."
    exit 1
  }

  printf '%s' "$item"
}

loadtest_get_index_doc_count() {
  local index_name="$1"
  loadtest_get_index_item_json "$index_name" | jq -r '.entries // 0'
}

loadtest_get_index_pending_task_count() {
  local index_name="$1"
  loadtest_get_index_item_json "$index_name" | jq -r '.numberOfPendingTasks // 0'
}

loadtest_index_exists() {
  local index_name="$1"
  local response

  response="$(loadtest_list_indexes_response)"
  jq -e --arg name "$index_name" \
    '(.items // []) | any(.name == $name)' \
    <<<"$response" >/dev/null
}

extract_loadtest_numeric_task_id() {
  local response_json="$1"
  jq -er '.taskID | select(type == "number")' <<<"$response_json"
}

# TODO: Document wait_for_loadtest_task_published.
wait_for_loadtest_task_published() {
  local task_id="$1"
  local attempt

  for ((attempt = 1; attempt <= FLAPJACK_TASK_MAX_ATTEMPTS; attempt += 1)); do
    local task_response
    local task_status
    local pending_task

    task_response="$(loadtest_http_request GET "/1/tasks/${task_id}" "" "200")"
    task_status="$(jq -r '.status // ""' <<<"$task_response")"
    pending_task="$(jq -r 'if has("pendingTask") then (.pendingTask | tostring) else "" end' <<<"$task_response")"

    if [[ "$task_status" == "published" && "$pending_task" == "false" ]]; then
      return 0
    fi

    sleep "$FLAPJACK_TASK_POLL_INTERVAL_SECONDS"
  done

  echo "FAIL: task ${task_id} did not settle to published within ${FLAPJACK_TASK_MAX_ATTEMPTS} polls."
  exit 1
}

load_dashboard_seed_settings() {
  local loadtest_root="${1:-$LOADTEST_HELPER_DIR/..}"

  LOADTEST_SETTINGS_JSON="$(
    cd "$loadtest_root"
    node -e 'import("../dashboard/tour/product-seed-data.mjs").then(({ seedSettings }) => { process.stdout.write(JSON.stringify(seedSettings)); }).catch((error) => { console.error(error); process.exit(1); });'
  )"
}

reset_loadtest_index() {
  local index_name="$1"
  local encoded_index_name
  local create_index_payload
  local delete_response

  encoded_index_name="$(loadtest_encode_path_component "$index_name")"
  create_index_payload="$(jq -cn --arg uid "$index_name" '{ uid: $uid }')"

  delete_response="$(loadtest_http_request DELETE "/1/indexes/${encoded_index_name}" "" "200,404")"
  if jq -e '.taskID | type == "number"' >/dev/null 2>&1 <<<"$delete_response"; then
    wait_for_loadtest_task_published "$(extract_loadtest_numeric_task_id "$delete_response")"
  fi

  loadtest_http_request POST "/1/indexes" "$create_index_payload" "200" >/dev/null
}

# Run a single k6 scenario with JSON + stdout artifact capture.
# Requires globals: SCRIPT_DIR, RESULTS_DIR, SCENARIO_FAILURE_COUNT.
# Requires function: fail.
# k6 exit 0 = pass, exit 99 = threshold breach (counted, non-fatal),
# any other exit = hard failure (abort).
run_k6_scenario() {
  local scenario_name="$1"
  local scenario_path="$2"
  local stdout_path="${RESULTS_DIR}/${scenario_name}.stdout.txt"
  local scenario_exit_code=0

  echo "INFO: running scenario ${scenario_name}"
  (
    cd "$SCRIPT_DIR"
    k6 run --out json="${RESULTS_DIR}/${scenario_name}.json" "$scenario_path"
  ) | tee "$stdout_path" || scenario_exit_code=$?

  if [[ $scenario_exit_code -eq 0 ]]; then
    return 0
  fi
  if [[ $scenario_exit_code -eq 99 ]]; then
    echo "WARN: scenario ${scenario_name} breached thresholds (exit code 99)"
    SCENARIO_FAILURE_COUNT=$((SCENARIO_FAILURE_COUNT + 1))
    return 0
  fi

  fail "scenario ${scenario_name} failed with a hard error (k6 exit code ${scenario_exit_code})"
}

# Smoke is a hard gate — if it breaches thresholds, return 1 so the
# caller can decide how to handle it (abort, return, etc.).
run_smoke_gate() {
  local pre_count="$SCENARIO_FAILURE_COUNT"
  run_k6_scenario "smoke" "scenarios/smoke.js"
  if [[ $SCENARIO_FAILURE_COUNT -gt $pre_count ]]; then
    return 1
  fi
}

apply_loadtest_index_settings() {
  local index_name="$1"
  local encoded_index_name
  local settings_response
  local task_id

  if [[ -z "${LOADTEST_SETTINGS_JSON:-}" ]]; then
    echo "FAIL: LOADTEST_SETTINGS_JSON must be populated before apply_loadtest_index_settings."
    exit 1
  fi

  encoded_index_name="$(loadtest_encode_path_component "$index_name")"
  settings_response="$(loadtest_http_request PUT "/1/indexes/${encoded_index_name}/settings" "$LOADTEST_SETTINGS_JSON" "200,207")"
  task_id="$(extract_loadtest_numeric_task_id "$settings_response")"
  wait_for_loadtest_task_published "$task_id"
}
