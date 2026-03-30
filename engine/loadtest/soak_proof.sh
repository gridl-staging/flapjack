#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOADTEST_HELPERS="$SCRIPT_DIR/lib/loadtest_shell_helpers.sh"
SOAK_HELPERS="$SCRIPT_DIR/lib/loadtest_soak_helpers.sh"
SEED_SCRIPT="$SCRIPT_DIR/seed-loadtest-data.sh"
SERVER_BINARY="${FLAPJACK_LOADTEST_SERVER_BINARY:-$ENGINE_DIR/target/release/flapjack}"
RESULTS_BASE_DIR="$SCRIPT_DIR/results"

RESULTS_DIR=""
RUNNER_TMP_DIR=""
SERVER_DATA_DIR=""
SERVER_LOG_PATH=""
SERVER_PID=""
SAMPLER_PID=""
SAMPLE_PATH=""
SUMMARY_PATH=""
K6_STDOUT_PATH=""
K6_JSON_PATH=""
K6_API_ADDR=""
SCENARIO_NAME=""
SCENARIO_EXIT_CODE=0
FLAPJACK_BIND_ADDR=""

usage() {
  cat <<'EOF'
Usage:
  bash engine/loadtest/soak_proof.sh --scenario <mixed-soak|write-soak>

Environment overrides come from engine/loadtest/lib/config.js via
sharedLoadtestConfig. The most relevant override for manual proof runs is:

  FLAPJACK_LOADTEST_SOAK_DURATION=2h
EOF
}

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

create_results_dir() {
  RESULTS_DIR="$(create_loadtest_results_dir "$RESULTS_BASE_DIR" "$SCENARIO_NAME")"
  mkdir -p "$RESULTS_DIR"
  SAMPLE_PATH="$RESULTS_DIR/memory_samples.csv"
  SUMMARY_PATH="$RESULTS_DIR/summary.md"
  K6_STDOUT_PATH="$RESULTS_DIR/${SCENARIO_NAME}.stdout.txt"
  K6_JSON_PATH="$RESULTS_DIR/${SCENARIO_NAME}.json.gz"
}

derive_bind_addr_from_base_url() {
  local parsed_bind_addr

  parsed_bind_addr="$(
    node -e '
const input = process.argv[1];
try {
  const url = new URL(input);
  const fallbackPort = url.protocol === "https:" ? "443" : "80";
  const port = url.port || fallbackPort;
  process.stdout.write(`${url.hostname}:${port}`);
} catch (error) {
  console.error(error.message);
  process.exit(1);
}
' "$FLAPJACK_BASE_URL"
  )" || fail "unable to parse FLAPJACK_LOADTEST_BASE_URL: $FLAPJACK_BASE_URL"

  FLAPJACK_BIND_ADDR="$parsed_bind_addr"
}

derive_k6_api_addr() {
  local host="${FLAPJACK_BIND_ADDR%:*}"
  local port="${FLAPJACK_BIND_ADDR##*:}"
  local k6_port=$((port + 10000))

  if (( k6_port > 65535 )); then
    fail "derived k6 API port ${k6_port} from bind addr ${FLAPJACK_BIND_ADDR} exceeds 65535"
  fi

  K6_API_ADDR="${host}:${k6_port}"
}

build_or_reuse_binary() {
  if [[ -x "$SERVER_BINARY" ]]; then
    echo "INFO: reusing existing release binary at $SERVER_BINARY"
    return 0
  fi

  echo "INFO: building release flapjack server binary"
  (
    cd "$ENGINE_DIR"
    cargo build --release -p flapjack-server
  )

  [[ -x "$SERVER_BINARY" ]] || fail "build completed without producing $SERVER_BINARY"
}

start_server() {
  mkdir -p "$SERVER_DATA_DIR"
  SERVER_LOG_PATH="$RESULTS_DIR/server.log"

  "$SERVER_BINARY" --no-auth --bind-addr "$FLAPJACK_BIND_ADDR" --data-dir "$SERVER_DATA_DIR" \
    >"$SERVER_LOG_PATH" 2>&1 &
  SERVER_PID=$!

  sleep 0.1
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    fail "server exited during startup; see $SERVER_LOG_PATH"
  fi
}

stop_server() {
  if [[ -z "$SERVER_PID" ]]; then
    return 0
  fi

  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true

    for _ in $(seq 1 50); do
      if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done

    if kill -0 "$SERVER_PID" 2>/dev/null; then
      kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
  fi

  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

wait_for_health() {
  local health_url="$FLAPJACK_BASE_URL/health"
  local max_attempts=300
  local attempt
  local health_status_code

  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
      wait "$SERVER_PID" 2>/dev/null || true
      fail "server exited while waiting for /health"
    fi

    health_status_code="$(
      curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "$health_url" 2>/dev/null || true
    )"
    if [[ "$health_status_code" == "200" ]]; then
      return 0
    fi

    sleep 0.1
  done

  fail "timed out waiting for $health_url"
}

metric_value() {
  local metric_name="$1"
  local label_fragment="${2:-}"
  local metrics

  metrics="$(loadtest_http_request GET "/metrics" "" "200")"
  if [[ -n "$label_fragment" ]]; then
    awk -v metric_name="$metric_name" -v label_fragment="$label_fragment" '
      index($0, metric_name "{") == 1 && index($0, label_fragment) > 0 {
        print $NF
        exit
      }
    ' <<<"$metrics"
  else
    awk -v metric_name="$metric_name" '
      $1 == metric_name {
        print $2
        exit
      }
    ' <<<"$metrics"
  fi
}

start_sampler() {
  local sample_interval_seconds="${FLAPJACK_LOADTEST_SAMPLE_INTERVAL_SECONDS:-30}"

  {
    echo "timestamp_utc,rss_kb,heap_bytes,pressure_level"
    while [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; do
      local timestamp_utc
      local rss_kb
      local heap_bytes
      local pressure_level

      timestamp_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      rss_kb="$(ps -o rss= -p "$SERVER_PID" | tr -d ' ' || true)"
      heap_bytes="$(metric_value "flapjack_memory_heap_bytes" || true)"
      pressure_level="$(metric_value "flapjack_memory_pressure_level" || true)"

      echo "${timestamp_utc},${rss_kb:-},${heap_bytes:-},${pressure_level:-}"
      sleep "$sample_interval_seconds"
    done
  } >"$SAMPLE_PATH" &

  SAMPLER_PID=$!
}

stop_sampler() {
  if [[ -z "$SAMPLER_PID" ]]; then
    return 0
  fi

  if kill -0 "$SAMPLER_PID" 2>/dev/null; then
    kill "$SAMPLER_PID" 2>/dev/null || true
  fi
  wait "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
}

run_soak_scenario() {
  echo "INFO: running ${SCENARIO_NAME} for ${FLAPJACK_SOAK_DURATION}"
  run_loadtest_scenario_with_artifacts \
    "$SCRIPT_DIR" \
    "$SCENARIO_NAME" \
    "$K6_API_ADDR" \
    "$K6_JSON_PATH" \
    "$K6_STDOUT_PATH" || SCENARIO_EXIT_CODE=$?

  case "$SCENARIO_EXIT_CODE" in
    0)
      return 0
      ;;
    99)
      echo "WARN: ${SCENARIO_NAME} breached thresholds (exit code 99)"
      return 0
      ;;
    *)
      fail "${SCENARIO_NAME} failed with hard error (k6 exit code ${SCENARIO_EXIT_CODE})"
      ;;
  esac
}

index_doc_count() {
  local index_name="$1"
  loadtest_get_index_doc_count "$index_name"
}

search_hit_count() {
  local index_name="$1"
  local payload="$2"
  local encoded_index_name
  local response

  encoded_index_name="$(loadtest_encode_path_component "$index_name")"
  response="$(loadtest_http_request POST "/1/indexes/${encoded_index_name}/query" "$payload" "200")"
  jq -r '.nbHits' <<<"$response"
}

capture_stable_index_snapshot() {
  local index_name="$1"
  local payload="$2"
  local min_doc_count="$3"
  local stable_polls_required="${4:-2}"
  local max_attempts="${5:-180}"
  local poll_interval_seconds="${6:-1}"
  local previous_signature=""
  local stable_polls=0
  local attempt
  local last_pending_tasks="unknown"
  local last_doc_count="unknown"
  local last_hit_count="unknown"

  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    local index_item
    local pending_tasks
    local doc_count
    local hit_count
    local signature

    index_item="$(loadtest_get_index_item_json "$index_name")"
    pending_tasks="$(jq -r '.numberOfPendingTasks // 0' <<<"$index_item")"
    doc_count="$(jq -r '.entries // 0' <<<"$index_item")"
    hit_count="$(search_hit_count "$index_name" "$payload")"
    signature="${doc_count}:${hit_count}"
    last_pending_tasks="$pending_tasks"
    last_doc_count="$doc_count"
    last_hit_count="$hit_count"

    if [[ "$pending_tasks" == "0" && "$doc_count" -ge "$min_doc_count" && "$hit_count" -ge "$min_doc_count" ]]; then
      if [[ "$signature" == "$previous_signature" ]]; then
        stable_polls=$((stable_polls + 1))
      else
        stable_polls=1
        previous_signature="$signature"
      fi

      if [[ "$stable_polls" -ge "$stable_polls_required" ]]; then
        printf '%s,%s' "$doc_count" "$hit_count"
        return 0
      fi
    else
      previous_signature=""
      stable_polls=0
    fi

    sleep "$poll_interval_seconds"
  done

  fail "index ${index_name} did not reach a stable snapshot after ${max_attempts} polls (pending=${last_pending_tasks} doc_count=${last_doc_count} hit_count=${last_hit_count})"
}

capture_consistency_snapshot() {
  local phase="$1"
  local read_doc_count
  local write_doc_count
  local macbook_hits
  local write_index_hits
  local read_snapshot
  local write_snapshot

  read_snapshot="$(capture_stable_index_snapshot "$FLAPJACK_READ_INDEX" '{"query":"MacBook","hitsPerPage":5}' 1)"
  write_snapshot="$(capture_stable_index_snapshot "$FLAPJACK_WRITE_INDEX" '{"query":"","hitsPerPage":1}' 1)"
  IFS=, read -r read_doc_count macbook_hits <<<"$read_snapshot"
  IFS=, read -r write_doc_count write_index_hits <<<"$write_snapshot"

  printf '%s_read_doc_count=%s\n' "$phase" "$read_doc_count" >>"$RESULTS_DIR/consistency.env"
  printf '%s_write_doc_count=%s\n' "$phase" "$write_doc_count" >>"$RESULTS_DIR/consistency.env"
  printf '%s_macbook_hits=%s\n' "$phase" "$macbook_hits" >>"$RESULTS_DIR/consistency.env"
  printf '%s_write_index_hits=%s\n' "$phase" "$write_index_hits" >>"$RESULTS_DIR/consistency.env"

  [[ "$read_doc_count" == "1000" ]] || fail "expected read index doc count 1000 during ${phase}, got ${read_doc_count}"
  [[ "$macbook_hits" -ge 1 ]] || fail "expected seeded MacBook hits during ${phase}, got ${macbook_hits}"
  [[ "$write_doc_count" -ge 1 ]] || fail "expected write index forward progress during ${phase}, got ${write_doc_count}"
  [[ "$write_index_hits" -ge 1 ]] || \
    fail "expected write index query hits during ${phase}, got ${write_index_hits}"
}

read_consistency_value() {
  local key="$1"

  awk -F= -v key="$key" '
    $1 == key {
      print substr($0, index($0, "=") + 1)
      found = 1
      exit
    }
    END {
      if (!found) {
        exit 1
      }
    }
  ' "$RESULTS_DIR/consistency.env"
}

write_summary() {
  local sample_count
  local rss_start
  local rss_end
  local rss_min
  local rss_max
  local heap_start
  local heap_end
  local heap_min
  local heap_max
  local max_pressure_level
  local post_soak_read_doc_count
  local post_soak_write_doc_count
  local post_soak_macbook_hits
  local post_soak_write_index_hits
  local post_restart_read_doc_count
  local post_restart_write_doc_count
  local post_restart_macbook_hits
  local post_restart_write_index_hits

  post_soak_read_doc_count="$(read_consistency_value "post_soak_read_doc_count")"
  post_soak_write_doc_count="$(read_consistency_value "post_soak_write_doc_count")"
  post_soak_macbook_hits="$(read_consistency_value "post_soak_macbook_hits")"
  post_soak_write_index_hits="$(read_consistency_value "post_soak_write_index_hits")"
  post_restart_read_doc_count="$(read_consistency_value "post_restart_read_doc_count")"
  post_restart_write_doc_count="$(read_consistency_value "post_restart_write_doc_count")"
  post_restart_macbook_hits="$(read_consistency_value "post_restart_macbook_hits")"
  post_restart_write_index_hits="$(read_consistency_value "post_restart_write_index_hits")"
  sample_count="$(awk 'NR > 1 { count += 1 } END { print count + 0 }' "$SAMPLE_PATH")"
  rss_start="$(awk -F, 'NR == 2 { print $2 }' "$SAMPLE_PATH")"
  rss_end="$(awk -F, 'END { print $2 }' "$SAMPLE_PATH")"
  rss_min="$(awk -F, 'NR == 2 { min = $2 } NR > 2 && $2 < min { min = $2 } END { print min }' "$SAMPLE_PATH")"
  rss_max="$(awk -F, 'NR == 2 { max = $2 } NR > 2 && $2 > max { max = $2 } END { print max }' "$SAMPLE_PATH")"
  heap_start="$(awk -F, 'NR == 2 { print $3 }' "$SAMPLE_PATH")"
  heap_end="$(awk -F, 'END { print $3 }' "$SAMPLE_PATH")"
  heap_min="$(awk -F, 'NR == 2 { min = $3 } NR > 2 && $3 < min { min = $3 } END { print min }' "$SAMPLE_PATH")"
  heap_max="$(awk -F, 'NR == 2 { max = $3 } NR > 2 && $3 > max { max = $3 } END { print max }' "$SAMPLE_PATH")"
  max_pressure_level="$(awk -F, 'NR == 2 { max = $4 } NR > 2 && $4 > max { max = $4 } END { print max }' "$SAMPLE_PATH")"

  cat >"$SUMMARY_PATH" <<EOF
# Soak Proof Summary

- Scenario: \`${SCENARIO_NAME}\`
- Soak duration: \`${FLAPJACK_SOAK_DURATION}\`
- Base URL: \`${FLAPJACK_BASE_URL}\`
- Bind address: \`${FLAPJACK_BIND_ADDR}\`
- k6 REST API address: \`${K6_API_ADDR}\`
- Server binary: \`${SERVER_BINARY}\`
- k6 exit code: \`${SCENARIO_EXIT_CODE}\`
- Threshold interpretation:
  - \`0\` means the soak stayed inside the current k6 threshold contract
  - \`99\` means thresholds were breached but the soak still completed and post-soak checks ran
- Artifacts:
  - k6 stdout: \`${K6_STDOUT_PATH}\`
  - k6 JSON: \`${K6_JSON_PATH}\`
  - memory samples: \`${SAMPLE_PATH}\`
  - server log: \`${SERVER_LOG_PATH}\`

## Memory Samples

- sample count: \`${sample_count}\`
- RSS KB: start=\`${rss_start}\`, end=\`${rss_end}\`, min=\`${rss_min}\`, max=\`${rss_max}\`
- Heap bytes: start=\`${heap_start}\`, end=\`${heap_end}\`, min=\`${heap_min}\`, max=\`${heap_max}\`
- max pressure level: \`${max_pressure_level}\`
- latency drift over time lives in the k6 JSON artifact; this summary keeps the canonical file path instead of duplicating k6 math in shell

## Consistency Checks

- post-soak read doc count: \`${post_soak_read_doc_count}\`
- post-soak write doc count: \`${post_soak_write_doc_count}\`
- post-soak seeded MacBook hits: \`${post_soak_macbook_hits}\`
- post-soak write-index hits: \`${post_soak_write_index_hits}\`
- post-restart read doc count: \`${post_restart_read_doc_count}\`
- post-restart write doc count: \`${post_restart_write_doc_count}\`
- post-restart seeded MacBook hits: \`${post_restart_macbook_hits}\`
- post-restart write-index hits: \`${post_restart_write_index_hits}\`
EOF
}

verify_restart_preserved_counts() {
  local post_restart_read_doc_count
  local post_soak_read_doc_count
  local post_restart_write_doc_count
  local post_soak_write_doc_count
  local post_restart_macbook_hits
  local post_soak_macbook_hits
  local post_restart_write_index_hits
  local post_soak_write_index_hits

  post_restart_read_doc_count="$(read_consistency_value "post_restart_read_doc_count")"
  post_soak_read_doc_count="$(read_consistency_value "post_soak_read_doc_count")"
  post_restart_write_doc_count="$(read_consistency_value "post_restart_write_doc_count")"
  post_soak_write_doc_count="$(read_consistency_value "post_soak_write_doc_count")"
  post_restart_macbook_hits="$(read_consistency_value "post_restart_macbook_hits")"
  post_soak_macbook_hits="$(read_consistency_value "post_soak_macbook_hits")"
  post_restart_write_index_hits="$(read_consistency_value "post_restart_write_index_hits")"
  post_soak_write_index_hits="$(read_consistency_value "post_soak_write_index_hits")"

  [[ "$post_restart_read_doc_count" == "$post_soak_read_doc_count" ]] || \
    fail "read index doc count changed across restart: pre=${post_soak_read_doc_count} post=${post_restart_read_doc_count}"
  [[ "$post_restart_write_doc_count" == "$post_soak_write_doc_count" ]] || \
    fail "write index doc count changed across restart: pre=${post_soak_write_doc_count} post=${post_restart_write_doc_count}"
  [[ "$post_restart_macbook_hits" == "$post_soak_macbook_hits" ]] || \
    fail "read index hit count changed across restart: pre=${post_soak_macbook_hits} post=${post_restart_macbook_hits}"
  [[ "$post_restart_write_index_hits" == "$post_soak_write_index_hits" ]] || \
    fail "write index hit count changed across restart: pre=${post_soak_write_index_hits} post=${post_restart_write_index_hits}"
}

cleanup() {
  stop_sampler
  stop_server

  if [[ -n "$RUNNER_TMP_DIR" && -d "$RUNNER_TMP_DIR" ]]; then
    rm -rf "$RUNNER_TMP_DIR"
    RUNNER_TMP_DIR=""
  fi
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --scenario)
        SCENARIO_NAME="${2:-}"
        shift 2
        ;;
      --help|-h)
        usage
        return 0
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage >&2
        return 1
        ;;
    esac
  done

  [[ "$SCENARIO_NAME" == "mixed-soak" || "$SCENARIO_NAME" == "write-soak" ]] || \
    fail "--scenario must be one of: mixed-soak, write-soak"
  [[ -f "$LOADTEST_HELPERS" ]] || fail "missing $LOADTEST_HELPERS"
  [[ -f "$SOAK_HELPERS" ]] || fail "missing $SOAK_HELPERS"
  [[ -x "$SEED_SCRIPT" ]] || fail "missing executable $SEED_SCRIPT"

  # shellcheck source=lib/loadtest_shell_helpers.sh
  source "$LOADTEST_HELPERS"
  # shellcheck source=lib/loadtest_soak_helpers.sh
  source "$SOAK_HELPERS"

  require_loadtest_commands cargo curl jq k6 node ps
  load_shared_loadtest_config
  initialize_loadtest_auth_headers
  load_dashboard_seed_settings "$SCRIPT_DIR"

  create_results_dir
  derive_bind_addr_from_base_url
  derive_k6_api_addr
  build_or_reuse_binary

  RUNNER_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flapjack-loadtest-soak.XXXXXX")"
  SERVER_DATA_DIR="$RUNNER_TMP_DIR/data"

  start_server
  wait_for_health
  "$SEED_SCRIPT"

  start_sampler
  run_soak_scenario
  stop_sampler

  : >"$RESULTS_DIR/consistency.env"
  capture_consistency_snapshot "post_soak"

  stop_server
  start_server
  wait_for_health
  capture_consistency_snapshot "post_restart"
  verify_restart_preserved_counts
  write_summary

  echo "INFO: soak proof artifacts written to $RESULTS_DIR"

  if [[ "$SCENARIO_EXIT_CODE" -eq 99 ]]; then
    exit 99
  fi

  echo "PASS: soak proof completed for ${SCENARIO_NAME}"
}

trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

main "$@"
