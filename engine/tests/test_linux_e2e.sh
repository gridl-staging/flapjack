#!/bin/bash
# test_linux_e2e.sh — Containerized Linux e2e validation for Flapjack.
#
# Runs inside a fresh ubuntu:22.04 Docker container. Exercises the full
# customer install path, server lifecycle, batch ingest, search retrieval,
# and auth contract with value-level assertions.
#
# Exit non-zero on any FAIL.

set -uo pipefail

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FIRST_FAILURE=""

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

SERVER_PID=""
DATA_DIR=""

cleanup() {
  local script_exit_code=$?
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR" ]; then
    if [ "$TESTS_FAILED" -gt 0 ] || [ "$script_exit_code" -ne 0 ]; then
      local failure_snapshot="/tmp/flapjack_linux_e2e_failure_${$}_$(date +%s)"
      cp -R "$DATA_DIR" "$failure_snapshot"
      printf "INFO: preserved linux e2e data at %s\n" "$failure_snapshot"
    else
      rm -rf "$DATA_DIR"
    fi
  fi
}
trap cleanup EXIT

BIND_ADDR="127.0.0.1:7799"
BASE_URL="http://${BIND_ADDR}"

generate_admin_key() {
  local random_hex=""
  random_hex="$(od -An -N16 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n')"
  if [ -z "$random_hex" ]; then
    printf 'ERROR: failed to generate a random admin key from /dev/urandom\n' >&2
    exit 1
  fi
  printf 'fj_linux_e2e_%s\n' "$random_hex"
}

printf "=== Flapjack Linux E2E Validation ===\n"
printf "Started: %s\n\n" "$(timestamp)"
ADMIN_KEY="$(generate_admin_key)"

# ── Step 1: Install via public installer ─────────────────────────────────────

printf '%s\n' "--- Step 1: Install via curl | sh ---"
curl -fsSL https://install.flapjack.foo | FLAPJACK_REPO=flapjackhq/flapjack NO_MODIFY_PATH=1 sh && INSTALL_EXIT=0 || INSTALL_EXIT=$?
printf "Install exit code: %d\n" "$INSTALL_EXIT"

FLAPJACK_BIN="$HOME/.flapjack/bin/flapjack"

# ── Step 2: Assert binary exists and is executable ───────────────────────────

printf '\n%s\n' "--- Step 2: Binary existence ---"
if [ -x "$FLAPJACK_BIN" ]; then
  pass "binary-exists"
else
  fail "binary-exists" "executable at $FLAPJACK_BIN" "not found or not executable"
  printf "\nAborting: binary not installed.\n"
  exit 1
fi

# ── Step 3: Assert --help exits 0 ───────────────────────────────────────────

printf '\n%s\n' "--- Step 3: Binary runs ---"
HELP_OUTPUT=$("$FLAPJACK_BIN" --help 2>&1) && HELP_EXIT=0 || HELP_EXIT=$?
if [ "$HELP_EXIT" -eq 0 ]; then
  pass "binary-runs"
else
  fail "binary-runs" "exit 0" "exit $HELP_EXIT"
  printf "  --help output: %s\n" "$HELP_OUTPUT"
fi

# ── Step 4: Start server ────────────────────────────────────────────────────

printf '\n%s\n' "--- Step 4: Start server ---"
DATA_DIR=$(mktemp -d)
printf "Data dir: %s\n" "$DATA_DIR"

export FLAPJACK_ADMIN_KEY="$ADMIN_KEY"
export FLAPJACK_BIND_ADDR="$BIND_ADDR"
export FLAPJACK_DATA_DIR="$DATA_DIR"

"$FLAPJACK_BIN" > "$DATA_DIR/server.log" 2>&1 &
SERVER_PID=$!
printf "Server PID: %d\n" "$SERVER_PID"

# ── Step 5: Poll /health for readiness ──────────────────────────────────────

printf '\n%s\n' "--- Step 5: Health check ---"
HEALTH_OK=false
for i in $(seq 1 60); do
  if curl -sf "${BASE_URL}/health" >/dev/null 2>&1; then
    HEALTH_OK=true
    break
  fi
  sleep 0.5
done

if [ "$HEALTH_OK" = "true" ]; then
  pass "server-starts"
  HEALTH_BODY=$(curl -sf "${BASE_URL}/health" 2>&1)
  printf "  /health response: %s\n" "$HEALTH_BODY"
  pass "health-ok"
else
  fail "server-starts" "server healthy within 30s" "timeout"
  fail "health-ok" "HTTP 200" "timeout"
  printf "Server log:\n"
  cat "$DATA_DIR/server.log" 2>/dev/null || printf "(no log)\n"
  printf "\nAborting: server did not start.\n"
  exit 1
fi

# ── Step 6-7: Batch ingest two documents ────────────────────────────────────

printf '\n%s\n' "--- Step 6-7: Batch ingest ---"
BATCH_BODY='{"requests":[{"action":"addObject","body":{"objectID":"1","title":"The Matrix","year":1999}},{"action":"addObject","body":{"objectID":"2","title":"Inception","year":2010}}]}'

BATCH_HTTP_CODE=$(curl -s -o "$DATA_DIR/batch_resp.json" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/batch" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-Application-Id: e2e-test" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d "$BATCH_BODY")

BATCH_RESP=$(cat "$DATA_DIR/batch_resp.json")
printf "Batch HTTP code: %s\n" "$BATCH_HTTP_CODE"
printf "Batch response: %s\n" "$BATCH_RESP"

if [ "$BATCH_HTTP_CODE" = "200" ] || [ "$BATCH_HTTP_CODE" = "201" ]; then
  pass "batch-accepted"
else
  fail "batch-accepted" "HTTP 200 or 201" "HTTP $BATCH_HTTP_CODE"
fi

# ── Step 8: Poll search until indexing completes ────────────────────────────

printf '\n%s\n' "--- Step 8-9: Search validation ---"
SEARCH_OK=false
SEARCH_RESP=""
for i in $(seq 1 20); do
  SEARCH_RESP=$(curl -s -X POST "${BASE_URL}/1/indexes/test_movies/query" \
    -H "Content-Type: application/json" \
    -H "X-Algolia-Application-Id: e2e-test" \
    -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
    -d '{"query":"matrix"}' 2>&1)

  NBHITS=$(printf '%s' "$SEARCH_RESP" | grep -o '"nbHits":[0-9]*' | head -1 | cut -d: -f2)
  if [ -n "$NBHITS" ] && [ "$NBHITS" -ge 1 ] 2>/dev/null; then
    SEARCH_OK=true
    break
  fi
  sleep 0.5
done

printf "Final search response: %s\n" "$SEARCH_RESP"

if [ "$SEARCH_OK" = "true" ]; then
  # Assert nbHits >= 1
  NBHITS=$(printf '%s' "$SEARCH_RESP" | grep -o '"nbHits":[0-9]*' | head -1 | cut -d: -f2)
  if [ -n "$NBHITS" ] && [ "$NBHITS" -ge 1 ] 2>/dev/null; then
    pass "search-nbHits (nbHits=$NBHITS)"
  else
    fail "search-nbHits" "nbHits >= 1" "nbHits=$NBHITS"
  fi

  # Assert exact title match
  if printf '%s' "$SEARCH_RESP" | grep -q '"The Matrix"'; then
    pass "search-exact-match"
  else
    fail "search-exact-match" "response contains \"The Matrix\"" "not found in response"
  fi
else
  fail "search-nbHits" "nbHits >= 1 within 10s" "timeout or nbHits=0"
  fail "search-exact-match" "response contains \"The Matrix\"" "search timed out"
fi

# ── Step 10: Auth contract — missing Application-Id returns 403 ─────────────

printf '\n%s\n' "--- Step 10: Auth contract ---"
AUTH_HTTP_CODE=$(curl -s -o "$DATA_DIR/auth_resp.json" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/query" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d '{"query":"matrix"}')

AUTH_RESP=$(cat "$DATA_DIR/auth_resp.json")
printf "Auth test (no Application-Id) HTTP code: %s\n" "$AUTH_HTTP_CODE"
printf "Auth test response: %s\n" "$AUTH_RESP"

if [ "$AUTH_HTTP_CODE" = "403" ]; then
  pass "auth-403-without-appid"
else
  fail "auth-403-without-appid" "HTTP 403" "HTTP $AUTH_HTTP_CODE"
fi

# ── Summary ─────────────────────────────────────────────────────────────────

printf "\n=== Summary ===\n"
printf "Finished: %s\n" "$(timestamp)"
printf "Total: %d  Passed: %d  Failed: %d\n" "$TESTS_RUN" "$TESTS_PASSED" "$TESTS_FAILED"

if [ "$TESTS_FAILED" -gt 0 ]; then
  printf "VERDICT: FAIL (first failure: %s)\n" "$FIRST_FAILURE"
  exit 1
else
  printf "VERDICT: PASS\n"
  exit 0
fi
