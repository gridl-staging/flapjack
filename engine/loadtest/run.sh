#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOADTEST_HELPERS="$SCRIPT_DIR/lib/loadtest_shell_helpers.sh"
SEED_SCRIPT="$SCRIPT_DIR/seed-loadtest-data.sh"
SERVER_BINARY="$ENGINE_DIR/target/release/flapjack"
RESULTS_BASE_DIR="$SCRIPT_DIR/results"

RESULTS_DIR=""
RUNNER_TMP_DIR=""
SERVER_DATA_DIR=""
SERVER_PID=""
FLAPJACK_BIND_ADDR=""
SCENARIO_FAILURE_COUNT=0

fail() {
  echo "FAIL: $1"
  exit 1
}

create_results_dir() {
  local timestamp

  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  RESULTS_DIR="$RESULTS_BASE_DIR/$timestamp"
  mkdir -p "$RESULTS_DIR"
}

# TODO: Document derive_bind_addr_from_base_url.
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

generate_ephemeral_admin_key() {
  node -e '
const crypto = require("node:crypto");
process.stdout.write(crypto.randomBytes(32).toString("hex"));
'
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

# TODO: Document start_server.
start_server() {
  local auth_mode="${1:-no-auth}"
  local server_log_path="$RESULTS_DIR/server.log"

  mkdir -p "$SERVER_DATA_DIR"

  case "$auth_mode" in
    no-auth)
      "$SERVER_BINARY" --no-auth --bind-addr "$FLAPJACK_BIND_ADDR" --data-dir "$SERVER_DATA_DIR" \
        >"$server_log_path" 2>&1 &
      ;;
    auth-required)
      [[ -n "${FLAPJACK_API_KEY:-}" ]] || fail "start_server auth-required mode needs FLAPJACK_API_KEY"
      FLAPJACK_ADMIN_KEY="$FLAPJACK_API_KEY" \
        "$SERVER_BINARY" --bind-addr "$FLAPJACK_BIND_ADDR" --data-dir "$SERVER_DATA_DIR" \
        >"$server_log_path" 2>&1 &
      ;;
    *)
      fail "unknown server auth mode: $auth_mode"
      ;;
  esac

  SERVER_PID=$!

  sleep 0.1
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    fail "server exited during startup; see $server_log_path"
  fi
}

# TODO: Document wait_for_health.
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

run_normal_pass() {
  run_smoke_gate || return 1

  run_k6_scenario "search-throughput" "scenarios/search-throughput.js"

  reset_loadtest_index "$FLAPJACK_WRITE_INDEX"
  apply_loadtest_index_settings "$FLAPJACK_WRITE_INDEX"
  run_k6_scenario "write-throughput" "scenarios/write-throughput.js"

  reset_loadtest_index "$FLAPJACK_WRITE_INDEX"
  apply_loadtest_index_settings "$FLAPJACK_WRITE_INDEX"
  run_k6_scenario "mixed-workload" "scenarios/mixed-workload.js"

  run_k6_scenario "spike" "scenarios/spike.js"
}

configure_memory_pressure_env() {
  FLAPJACK_MEMORY_LIMIT_MB=128
  FLAPJACK_MEMORY_HIGH_WATERMARK=75
  FLAPJACK_MEMORY_CRITICAL=90
  export FLAPJACK_MEMORY_LIMIT_MB
  export FLAPJACK_MEMORY_HIGH_WATERMARK
  export FLAPJACK_MEMORY_CRITICAL
}

# TODO: Document stop_server.
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

cleanup() {
  stop_server

  if [[ -n "$RUNNER_TMP_DIR" && -d "$RUNNER_TMP_DIR" ]]; then
    rm -rf "$RUNNER_TMP_DIR"
    RUNNER_TMP_DIR=""
  fi
}

run_memory_pressure_pass() {
  stop_server
  configure_memory_pressure_env
  if [[ -z "${FLAPJACK_API_KEY:-}" ]]; then
    FLAPJACK_API_KEY="$(generate_ephemeral_admin_key)" || fail "unable to generate pressure-pass admin key"
  fi
  export FLAPJACK_LOADTEST_API_KEY="$FLAPJACK_API_KEY"
  initialize_loadtest_auth_headers

  start_server "auth-required"
  wait_for_health
  "$SCRIPT_DIR/seed-loadtest-data.sh"
  wait_for_health
  run_k6_scenario "memory-pressure" "scenarios/memory-pressure.js"
}

# TODO: Document main.
main() {
  [[ -f "$LOADTEST_HELPERS" ]] || fail "missing $LOADTEST_HELPERS"
  [[ -x "$SEED_SCRIPT" ]] || fail "missing executable $SEED_SCRIPT"

  # shellcheck source=lib/loadtest_shell_helpers.sh
  source "$LOADTEST_HELPERS"

  require_loadtest_commands cargo curl jq k6 node
  load_shared_loadtest_config
  initialize_loadtest_auth_headers
  load_dashboard_seed_settings "$SCRIPT_DIR"

  create_results_dir
  derive_bind_addr_from_base_url
  build_or_reuse_binary

  RUNNER_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flapjack-loadtest-run.XXXXXX")"
  SERVER_DATA_DIR="$RUNNER_TMP_DIR/data"

  start_server "no-auth"
  wait_for_health
  "$SCRIPT_DIR/seed-loadtest-data.sh"

  run_normal_pass
  run_memory_pressure_pass

  echo "INFO: results written to $RESULTS_DIR"

  if [[ $SCENARIO_FAILURE_COUNT -gt 0 ]]; then
    echo "FAIL: ${SCENARIO_FAILURE_COUNT} scenario(s) breached thresholds"
    exit 99
  fi

  echo "PASS: loadtest run completed"
}

trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

main "$@"
