#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./engine/tests/upgrade_smoke.sh --old-bin <path> --new-bin <path>

Runs a minimal upgrade smoke by:
1. starting the old binary on a temp data dir
2. seeding data and verifying search
3. stopping the old binary
4. starting the new binary on the same data dir
5. verifying /health, /health/ready, /dashboard, search, and writes
EOF
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
WAIT_HELPER="$SCRIPT_DIR/common/wait_for_flapjack.sh"
ADMIN_KEY="upgrade-smoke-admin-key-20260328"
INDEX_NAME="upgrade_smoke"
QUERY_TOKEN="upgrade-smoke-token"

OLD_BIN=""
NEW_BIN=""
TMP_DIR=""
DATA_DIR=""
OLD_LOG=""
NEW_LOG=""
OLD_PID=""
NEW_PID=""

pass() {
  printf 'PASS: %s\n' "$1"
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  if [ -n "${OLD_LOG:-}" ] && [ -f "$OLD_LOG" ]; then
    printf '\n== old log ==\n' >&2
    cat "$OLD_LOG" >&2 || true
  fi
  if [ -n "${NEW_LOG:-}" ] && [ -f "$NEW_LOG" ]; then
    printf '\n== new log ==\n' >&2
    cat "$NEW_LOG" >&2 || true
  fi
  exit 1
}

cleanup() {
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" 2>/dev/null || true
    wait "$OLD_PID" 2>/dev/null || true
  fi
  if [ -n "$NEW_PID" ] && kill -0 "$NEW_PID" 2>/dev/null; then
    kill "$NEW_PID" 2>/dev/null || true
    wait "$NEW_PID" 2>/dev/null || true
  fi
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}

extract_port_from_log() {
  local log_path="$1"
  sed -n 's/.*Local:.*http:\/\/127\.0\.0\.1:\([0-9]*\).*/\1/p' "$log_path" | head -1
}

http_json() {
  local method="$1"
  local url="$2"
  local body="${3:-}"

  if [ -n "$body" ]; then
    curl -fsS -X "$method" "$url" \
      -H "content-type: application/json" \
      -H "x-algolia-application-id: flapjack" \
      -H "x-algolia-api-key: $ADMIN_KEY" \
      -d "$body"
  else
    curl -fsS -X "$method" "$url" \
      -H "content-type: application/json" \
      -H "x-algolia-application-id: flapjack" \
      -H "x-algolia-api-key: $ADMIN_KEY"
  fi
}

wait_for_task_published() {
  local base_url="$1"
  local task_id="$2"

  for _ in $(seq 1 120); do
    local response
    response="$(http_json GET "$base_url/1/indexes/$INDEX_NAME/task/$task_id")" || true
    if [ -n "$response" ] && [ "$(printf '%s' "$response" | jq -r '.status // empty')" = "published" ]; then
      return 0
    fi
    sleep 0.25
  done

  fail "task $task_id did not reach published state"
}

verify_search_hits() {
  local base_url="$1"
  local query="$2"
  local expected_hits="$3"
  local response
  local hits

  response="$(http_json POST "$base_url/1/indexes/$INDEX_NAME/query" "{\"query\":\"$query\"}")"
  hits="$(printf '%s' "$response" | jq -r '.nbHits')"
  [ "$hits" = "$expected_hits" ] || fail "expected $expected_hits hits for query '$query', got $hits"
}

start_server() {
  local bin_path="$1"
  local log_path="$2"

  FLAPJACK_ENV=production \
  FLAPJACK_ADMIN_KEY="$ADMIN_KEY" \
  FLAPJACK_BIND_ADDR="127.0.0.1:0" \
  FLAPJACK_DATA_DIR="$DATA_DIR" \
  "$bin_path" >"$log_path" 2>&1 &

  printf '%s' "$!"
}

main() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --old-bin)
        OLD_BIN="${2:-}"
        shift 2
        ;;
      --new-bin)
        NEW_BIN="${2:-}"
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

  [ -n "$OLD_BIN" ] || { echo "ERROR: --old-bin is required" >&2; usage >&2; return 1; }
  [ -n "$NEW_BIN" ] || { echo "ERROR: --new-bin is required" >&2; usage >&2; return 1; }
  [ -x "$OLD_BIN" ] || fail "old binary is not executable: $OLD_BIN"
  [ -x "$NEW_BIN" ] || fail "new binary is not executable: $NEW_BIN"
  [ -x "$WAIT_HELPER" ] || fail "missing wait helper: $WAIT_HELPER"

  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flapjack-upgrade-smoke.XXXXXX")"
  DATA_DIR="$TMP_DIR/data"
  OLD_LOG="$TMP_DIR/old.log"
  NEW_LOG="$TMP_DIR/new.log"
  mkdir -p "$DATA_DIR"

  OLD_PID="$(start_server "$OLD_BIN" "$OLD_LOG")"
  "$WAIT_HELPER" --pid "$OLD_PID" --port auto --log-path "$OLD_LOG" >/dev/null
  local old_port
  old_port="$(extract_port_from_log "$OLD_LOG")"
  [ -n "$old_port" ] || fail "could not detect old server port"
  local old_base="http://127.0.0.1:$old_port"

  local batch_response
  local task_id
  batch_response="$(http_json POST "$old_base/1/indexes/$INDEX_NAME/batch" '{
    "requests": [
      {
        "action": "addObject",
        "body": {
          "objectID": "old-doc-1",
          "title": "Upgrade smoke old doc",
          "token": "'"$QUERY_TOKEN"'"
        }
      },
      {
        "action": "addObject",
        "body": {
          "objectID": "old-doc-2",
          "title": "Upgrade smoke second doc",
          "token": "'"$QUERY_TOKEN"'"
        }
      }
    ]
  }')"
  task_id="$(printf '%s' "$batch_response" | jq -r '.taskID')"
  wait_for_task_published "$old_base" "$task_id"
  verify_search_hits "$old_base" "$QUERY_TOKEN" "2"
  pass "old binary seeded and searchable"

  kill "$OLD_PID" 2>/dev/null || true
  wait "$OLD_PID" 2>/dev/null || true
  OLD_PID=""

  NEW_PID="$(start_server "$NEW_BIN" "$NEW_LOG")"
  "$WAIT_HELPER" --pid "$NEW_PID" --port auto --log-path "$NEW_LOG" >/dev/null
  local new_port
  new_port="$(extract_port_from_log "$NEW_LOG")"
  [ -n "$new_port" ] || fail "could not detect new server port"
  local new_base="http://127.0.0.1:$new_port"

  curl -fsS "$new_base/health" >/dev/null || fail "new binary health check failed"
  curl -fsS "$new_base/health/ready" >/dev/null || fail "new binary readiness check failed"
  curl -fsS "$new_base/dashboard" >/dev/null || fail "new binary dashboard load failed"
  verify_search_hits "$new_base" "$QUERY_TOKEN" "2"
  pass "new binary preserved pre-upgrade search state"

  local upgrade_write_response
  local upgrade_task_id
  upgrade_write_response="$(http_json POST "$new_base/1/indexes/$INDEX_NAME/batch" '{
    "requests": [
      {
        "action": "addObject",
        "body": {
          "objectID": "new-doc-1",
          "title": "Upgrade smoke new doc",
          "token": "post-upgrade-token"
        }
      }
    ]
  }')"
  upgrade_task_id="$(printf '%s' "$upgrade_write_response" | jq -r '.taskID')"
  wait_for_task_published "$new_base" "$upgrade_task_id"
  verify_search_hits "$new_base" "post-upgrade-token" "1"
  pass "new binary accepted writes on the upgraded data dir"
}

trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

main "$@"
