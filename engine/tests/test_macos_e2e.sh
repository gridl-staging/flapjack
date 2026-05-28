#!/bin/bash
# test_macos_e2e.sh — Host macOS e2e validation for Flapjack installer/runtime.
#
# Exercises the customer install path under isolated HOME/install/data roots,
# then validates server lifecycle, batch ingest, search retrieval, auth contract,
# and stage-owned cleanup behavior.

set -uo pipefail

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FIRST_FAILURE=""

SERVER_PID=""
TMP_ROOT=""
TMP_HOME=""
TMP_INSTALL_ROOT=""
TMP_DATA_ROOT=""
CLEANUP_RAN="false"

BIND_ADDR="127.0.0.1:7801"
BASE_URL="http://${BIND_ADDR}"

generate_admin_key() {
  local random_hex=""
  random_hex="$(od -An -N16 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n')"
  if [ -z "$random_hex" ]; then
    printf 'ERROR: failed to generate a random admin key from /dev/urandom\n' >&2
    exit 1
  fi
  printf 'fj_macos_e2e_%s\n' "$random_hex"
}

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

pass() {
  TESTS_PASSED=$((TESTS_PASSED + 1))
  TESTS_RUN=$((TESTS_RUN + 1))
  printf "[%s] PASS: %s\n" "$(timestamp)" "$1"
}

fail() {
  TESTS_FAILED=$((TESTS_FAILED + 1))
  TESTS_RUN=$((TESTS_RUN + 1))
  printf "[%s] FAIL: %s\n" "$(timestamp)" "$1"
  if [ -n "${2:-}" ]; then
    printf "  expected: %s\n" "$2"
  fi
  if [ -n "${3:-}" ]; then
    printf "  actual:   %s\n" "$3"
  fi
  if [ -z "$FIRST_FAILURE" ]; then
    FIRST_FAILURE="$1"
  fi
}

cleanup() {
  local script_exit_code=$?
  if [ "$CLEANUP_RAN" = "true" ]; then
    return
  fi
  CLEANUP_RAN="true"

  printf "\n%s\n" "--- Step 10: Cleanup ---"

  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null && KILL_EXIT=0 || KILL_EXIT=$?
    wait "$SERVER_PID" 2>/dev/null && WAIT_EXIT=0 || WAIT_EXIT=$?
    if [ "$KILL_EXIT" -eq 0 ] || [ "$WAIT_EXIT" -eq 0 ]; then
      pass "cleanup-server-pid"
    else
      fail "cleanup-server-pid" "server process stopped" "kill=$KILL_EXIT wait=$WAIT_EXIT pid=$SERVER_PID"
    fi
  else
    pass "cleanup-server-pid"
  fi

  if [ "$TESTS_FAILED" -gt 0 ] || [ "$script_exit_code" -ne 0 ]; then
    if [ -n "$TMP_DATA_ROOT" ] && [ -d "$TMP_DATA_ROOT" ]; then
      local failure_snapshot="/tmp/flapjack_macos_e2e_failure_${$}_$(date +%s)"
      cp -R "$TMP_DATA_ROOT" "$failure_snapshot"
      printf "INFO: preserved macOS e2e data at %s\n" "$failure_snapshot"
    fi
    return
  fi

  for owned_path in "$TMP_HOME" "$TMP_INSTALL_ROOT" "$TMP_DATA_ROOT" "$TMP_ROOT"; do
    if [ -z "$owned_path" ]; then
      continue
    fi

    if [ -e "$owned_path" ]; then
      rm -rf "$owned_path" && RM_EXIT=0 || RM_EXIT=$?
      if [ "$RM_EXIT" -eq 0 ] && [ ! -e "$owned_path" ]; then
        pass "cleanup-remove-$owned_path"
      else
        fail "cleanup-remove-$owned_path" "remove $owned_path" "rm exit=$RM_EXIT"
      fi
    else
      pass "cleanup-remove-$owned_path"
    fi
  done

  if [ ! -e "$TMP_HOME" ] && [ ! -e "$TMP_INSTALL_ROOT" ] && [ ! -e "$TMP_DATA_ROOT" ] && [ ! -e "$TMP_ROOT" ]; then
    pass "cleanup-complete"
  else
    fail "cleanup-complete" "all stage-owned paths removed" "one or more paths still exist"
  fi
}
trap cleanup EXIT

print_summary() {
  printf "\n=== Summary ===\n"
  printf "Finished: %s\n" "$(timestamp)"
  printf "Total: %d  Passed: %d  Failed: %d\n" "$TESTS_RUN" "$TESTS_PASSED" "$TESTS_FAILED"

  if [ "$TESTS_FAILED" -gt 0 ]; then
    printf "VERDICT: FAIL (first failure: %s)\n" "$FIRST_FAILURE"
  else
    printf "VERDICT: PASS\n"
  fi
}

printf "=== Flapjack macOS E2E Validation ===\n"
printf "Started: %s\n\n" "$(timestamp)"
ADMIN_KEY="$(generate_admin_key)"

# Step 1: Install via isolated curl | sh path
printf '%s\n' "--- Step 1: Install via curl | sh ---"
TMP_ROOT=$(mktemp -d)
TMP_HOME="$TMP_ROOT/home"
TMP_INSTALL_ROOT="$TMP_ROOT/install"
TMP_DATA_ROOT="$TMP_ROOT/data"
mkdir -p "$TMP_HOME" "$TMP_INSTALL_ROOT" "$TMP_DATA_ROOT"
printf "TMP_ROOT: %s\n" "$TMP_ROOT"
printf "TMP_HOME: %s\n" "$TMP_HOME"
printf "TMP_INSTALL_ROOT: %s\n" "$TMP_INSTALL_ROOT"
printf "TMP_DATA_ROOT: %s\n" "$TMP_DATA_ROOT"

INSTALL_OUTPUT=$(curl -fsSL https://install.flapjack.foo | HOME="$TMP_HOME" FLAPJACK_INSTALL="$TMP_INSTALL_ROOT" FLAPJACK_REPO=flapjackhq/flapjack NO_MODIFY_PATH=1 sh 2>&1) && INSTALL_EXIT=0 || INSTALL_EXIT=$?
printf "%s\n" "$INSTALL_OUTPUT"
printf "Install exit code: %d\n" "$INSTALL_EXIT"

DETECTED_TARGET=$(printf '%s\n' "$INSTALL_OUTPUT" | sed -n 's/.*Detected platform: .* → //p' | head -1)
if [ -n "$DETECTED_TARGET" ]; then
  pass "detected-target ($DETECTED_TARGET)"
else
  fail "detected-target" "aarch64-apple-darwin or x86_64-apple-darwin" "target not found in installer output"
fi

FLAPJACK_BIN="$TMP_INSTALL_ROOT/bin/flapjack"

# Step 2: Binary exists
printf '\n%s\n' "--- Step 2: Binary existence ---"
if [ -x "$FLAPJACK_BIN" ]; then
  pass "binary-exists"
else
  fail "binary-exists" "executable at $FLAPJACK_BIN" "not found or not executable"
  cleanup
  trap - EXIT
  print_summary
  exit 1
fi

# Step 3: Binary runs --help
printf '\n%s\n' "--- Step 3: Binary runs ---"
HELP_OUTPUT=$("$FLAPJACK_BIN" --help 2>&1) && HELP_EXIT=0 || HELP_EXIT=$?
if [ "$HELP_EXIT" -eq 0 ]; then
  pass "binary-runs"
else
  fail "binary-runs" "exit 0" "exit $HELP_EXIT"
  printf "  --help output: %s\n" "$HELP_OUTPUT"
fi

# Step 4: Start server (no subcommand required)
printf '\n%s\n' "--- Step 4: Start server ---"
export FLAPJACK_ADMIN_KEY="$ADMIN_KEY"
export FLAPJACK_BIND_ADDR="$BIND_ADDR"
export FLAPJACK_DATA_DIR="$TMP_DATA_ROOT"

"$FLAPJACK_BIN" > "$TMP_DATA_ROOT/server.log" 2>&1 &
SERVER_PID=$!
printf "Server PID: %d\n" "$SERVER_PID"

# Step 5: Health check
printf '\n%s\n' "--- Step 5: Health check ---"
HEALTH_OK="false"
for _i in $(seq 1 60); do
  curl -sf "${BASE_URL}/health" >/dev/null 2>&1 && CURL_HEALTH_EXIT=0 || CURL_HEALTH_EXIT=$?
  if [ "$CURL_HEALTH_EXIT" -eq 0 ]; then
    HEALTH_OK="true"
    break
  fi
  sleep 0.5
done

if [ "$HEALTH_OK" = "true" ]; then
  pass "server-starts"
  HEALTH_BODY=$(curl -sf "${BASE_URL}/health" 2>&1) && HEALTH_BODY_EXIT=0 || HEALTH_BODY_EXIT=$?
  printf "  /health response: %s\n" "$HEALTH_BODY"
  if [ "$HEALTH_BODY_EXIT" -eq 0 ]; then
    pass "health-ok"
  else
    fail "health-ok" "HTTP 200" "exit $HEALTH_BODY_EXIT"
  fi
else
  fail "server-starts" "server healthy within 30s" "timeout"
  fail "health-ok" "HTTP 200" "timeout"
  printf "Server log:\n"
  cat "$TMP_DATA_ROOT/server.log" 2>/dev/null
  cleanup
  trap - EXIT
  print_summary
  exit 1
fi

# Step 6: Batch ingest two documents
printf '\n%s\n' "--- Step 6: Batch ingest ---"
BATCH_BODY='{"requests":[{"action":"addObject","body":{"objectID":"1","title":"The Matrix","year":1999}},{"action":"addObject","body":{"objectID":"2","title":"Inception","year":2010}}]}'

BATCH_HTTP_CODE=$(curl -s -o "$TMP_DATA_ROOT/batch_resp.json" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/batch" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-Application-Id: e2e-test" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d "$BATCH_BODY") && BATCH_EXIT=0 || BATCH_EXIT=$?

BATCH_RESP=$(cat "$TMP_DATA_ROOT/batch_resp.json" 2>/dev/null)
printf "Batch curl exit: %s\n" "$BATCH_EXIT"
printf "Batch HTTP code: %s\n" "$BATCH_HTTP_CODE"
printf "Batch response: %s\n" "$BATCH_RESP"

if [ "$BATCH_EXIT" -eq 0 ] && { [ "$BATCH_HTTP_CODE" = "200" ] || [ "$BATCH_HTTP_CODE" = "201" ]; }; then
  pass "batch-accepted"
else
  fail "batch-accepted" "curl exit 0 and HTTP 200 or 201" "exit=$BATCH_EXIT http=$BATCH_HTTP_CODE"
fi

# Step 7-8: Search validation
printf '\n%s\n' "--- Step 7-8: Search validation ---"
SEARCH_OK="false"
SEARCH_RESP=""
for _i in $(seq 1 20); do
  SEARCH_RESP=$(curl -s -X POST "${BASE_URL}/1/indexes/test_movies/query" \
    -H "Content-Type: application/json" \
    -H "X-Algolia-Application-Id: e2e-test" \
    -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
    -d '{"query":"matrix"}' 2>&1) && SEARCH_EXIT=0 || SEARCH_EXIT=$?

  NBHITS=$(printf '%s' "$SEARCH_RESP" | grep -o '"nbHits":[0-9]*' | head -1 | cut -d: -f2)
  if [ "$SEARCH_EXIT" -eq 0 ] && [ -n "$NBHITS" ] && [ "$NBHITS" -ge 1 ] 2>/dev/null; then
    SEARCH_OK="true"
    break
  fi
  sleep 0.5
done

printf "Final search response: %s\n" "$SEARCH_RESP"

if [ "$SEARCH_OK" = "true" ]; then
  NBHITS=$(printf '%s' "$SEARCH_RESP" | grep -o '"nbHits":[0-9]*' | head -1 | cut -d: -f2)
  if [ -n "$NBHITS" ] && [ "$NBHITS" -ge 1 ] 2>/dev/null; then
    pass "search-nbHits (nbHits=$NBHITS)"
  else
    fail "search-nbHits" "nbHits >= 1" "nbHits=$NBHITS"
  fi

  if printf '%s' "$SEARCH_RESP" | grep -q '"The Matrix"'; then
    pass "search-exact-match"
  else
    fail "search-exact-match" "response contains \"The Matrix\"" "not found in response"
  fi
else
  fail "search-nbHits" "nbHits >= 1 within 10s" "timeout or nbHits=0"
  fail "search-exact-match" "response contains \"The Matrix\"" "search timed out"
fi

# Step 9: Auth contract (missing app id => 403)
printf '\n%s\n' "--- Step 9: Auth contract ---"
AUTH_HTTP_CODE=$(curl -s -o "$TMP_DATA_ROOT/auth_resp.json" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/query" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d '{"query":"matrix"}') && AUTH_EXIT=0 || AUTH_EXIT=$?

AUTH_RESP=$(cat "$TMP_DATA_ROOT/auth_resp.json" 2>/dev/null)
printf "Auth curl exit: %s\n" "$AUTH_EXIT"
printf "Auth test (no Application-Id) HTTP code: %s\n" "$AUTH_HTTP_CODE"
printf "Auth test response: %s\n" "$AUTH_RESP"

if [ "$AUTH_EXIT" -eq 0 ] && [ "$AUTH_HTTP_CODE" = "403" ]; then
  pass "auth-403-without-appid"
else
  fail "auth-403-without-appid" "curl exit 0 and HTTP 403" "exit=$AUTH_EXIT http=$AUTH_HTTP_CODE"
fi

cleanup
trap - EXIT
print_summary

if [ "$TESTS_FAILED" -gt 0 ]; then
  exit 1
fi

exit 0
