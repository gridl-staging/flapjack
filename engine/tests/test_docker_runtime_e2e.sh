#!/bin/bash
# test_docker_runtime_e2e.sh — Published-image Docker runtime e2e.
#
# Validates a published ghcr.io/flapjackhq/flapjack image end to end:
# container start, architecture assertion, /health value-level contract,
# .admin_key persistence, batch ingest, task publication polling (README
# workflow), search retrieval, and missing-Application-Id 403 auth contract.
#
# Accepts env overrides: IMAGE, PLATFORM, EXPECTED_VERSION, CONTAINER_NAME,
# HOST_PORT. Defaults to ghcr.io/flapjackhq/flapjack:1.0.0 on linux/amd64.
#
# Usage:
#   bash engine/tests/test_docker_runtime_e2e.sh
#   IMAGE=ghcr.io/flapjackhq/flapjack:1.0.1 PLATFORM=linux/arm64 \
#     EXPECTED_VERSION=1.0.1 bash engine/tests/test_docker_runtime_e2e.sh

set -uo pipefail

IMAGE="${IMAGE:-ghcr.io/flapjackhq/flapjack:1.0.0}"
CONTAINER_NAME="${CONTAINER_NAME:-flapjack_stage4_e2e}"
HOST_PORT="${HOST_PORT:-17700}"
BASE_URL="http://127.0.0.1:${HOST_PORT}"
PLATFORM="${PLATFORM:-linux/amd64}"
EXPECTED_VERSION="${EXPECTED_VERSION:-1.0.0}"

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

TMP_DATA_DIR=""
CONTAINER_STARTED="false"

cleanup() {
  local script_exit_code=$?
  # Best-effort container teardown. The container was started with --rm so a
  # stop/kill is sufficient to remove it.
  if [ "$CONTAINER_STARTED" = "true" ]; then
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DATA_DIR" ] && [ -d "$TMP_DATA_DIR" ]; then
    if [ "$TESTS_FAILED" -gt 0 ] || [ "$script_exit_code" -ne 0 ]; then
      local failure_snapshot="/tmp/flapjack_docker_runtime_e2e_failure_${$}_$(date +%s)"
      cp -R "$TMP_DATA_DIR" "$failure_snapshot" 2>/dev/null || true
      printf "INFO: preserving docker e2e host data dir for triage: %s\n" "$TMP_DATA_DIR"
    else
      rm -rf "$TMP_DATA_DIR" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

printf "=== Flapjack Docker Runtime E2E ===\n"
printf "Started: %s\n" "$(timestamp)"
printf "Image: %s\n" "$IMAGE"
printf "Platform: %s\n" "$PLATFORM"
printf "Expected version: %s\n" "$EXPECTED_VERSION"
printf "Container: %s\n" "$CONTAINER_NAME"
printf "Host port: %s -> 7700\n\n" "$HOST_PORT"

# ── Pre-flight: docker available and any stale container removed ─────────────

if ! command -v docker >/dev/null 2>&1; then
  fail "docker-available" "docker on PATH" "docker not found"
  printf "\nAborting: docker is required.\n"
  exit 1
fi
pass "docker-available"

# Remove any stale container with our exact name (idempotent run).
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  printf "Removing stale container %s before start\n" "$CONTAINER_NAME"
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# ── Step 1: Start published image ────────────────────────────────────────────

printf '\n%s\n' "--- Step 1: Start published image ---"
TMP_DATA_DIR=$(mktemp -d)
printf "Host data dir: %s\n" "$TMP_DATA_DIR"

docker run -d --rm \
  --platform "$PLATFORM" \
  --name "$CONTAINER_NAME" \
  -p "${HOST_PORT}:7700" \
  -v "${TMP_DATA_DIR}:/data" \
  "$IMAGE" > /tmp/docker_run.out 2>&1 && RUN_EXIT=0 || RUN_EXIT=$?

printf "docker run exit: %d\n" "$RUN_EXIT"
printf "docker run output: "
cat /tmp/docker_run.out 2>/dev/null || true
printf "\n"

if [ "$RUN_EXIT" -eq 0 ]; then
  CONTAINER_STARTED="true"
  pass "container-started"
else
  fail "container-started" "exit 0" "exit $RUN_EXIT"
  printf "\nAborting: container failed to start.\n"
  exit 1
fi

# ── Step 2: Poll /health until HTTP 200 or timeout ───────────────────────────

printf '\n%s\n' "--- Step 2: Health check ---"
HEALTH_OK="false"
HEALTH_BODY=""
for _i in $(seq 1 60); do
  HEALTH_HTTP_CODE=$(curl -s -o /tmp/flapjack_health.out -w "%{http_code}" "${BASE_URL}/health" 2>/dev/null) && HEALTH_EXIT=0 || HEALTH_EXIT=$?; HEALTH_BODY=$(cat /tmp/flapjack_health.out 2>/dev/null)
  if [ "$HEALTH_EXIT" -eq 0 ] && [ "$HEALTH_HTTP_CODE" = "200" ]; then
    HEALTH_OK="true"
    break
  fi
  sleep 0.5
done

if [ "$HEALTH_OK" = "true" ]; then
  pass "server-starts"
  printf "  /health response: %s\n" "$HEALTH_BODY"
else
  fail "server-starts" "/health HTTP 200 within 30s" "timeout"
  printf "Container logs:\n"
  docker logs "$CONTAINER_NAME" 2>&1 | tail -40 || true
  printf "\nAborting: server did not become healthy.\n"
  exit 1
fi

# Value-level /health assertions: status, version, vector-search capability.
# These catch wrong-image / wrong-build regressions, not just "did it boot".
if printf '%s' "$HEALTH_BODY" | grep -q '"status":"ok"'; then
  pass "health-status-ok"
else
  fail "health-status-ok" '"status":"ok" in /health body' "$HEALTH_BODY"
fi

if printf '%s' "$HEALTH_BODY" | grep -q "\"version\":\"${EXPECTED_VERSION}\""; then
  pass "health-version-match (${EXPECTED_VERSION})"
else
  fail "health-version-match" "\"version\":\"${EXPECTED_VERSION}\" in /health body" "$HEALTH_BODY"
fi

# README documents vector search as a Docker-runtime capability; assert the
# image actually carries it (the rust:1 trixie runtime build path enables it,
# unlike the musl release build path).
if printf '%s' "$HEALTH_BODY" | grep -q '"vectorSearch":true'; then
  pass "health-capability-vector-search"
else
  fail "health-capability-vector-search" '"vectorSearch":true in /health.capabilities' "$HEALTH_BODY"
fi

# ── Step 2b: Architecture assertion ──────────────────────────────────────────

printf '\n%s\n' "--- Step 2b: Architecture assertion ---"
ACTUAL_PLATFORM=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$IMAGE" 2>/dev/null) && INSPECT_EXIT=0 || INSPECT_EXIT=$?

if [ "$INSPECT_EXIT" -eq 0 ] && [ "$ACTUAL_PLATFORM" = "$PLATFORM" ]; then
  pass "image-architecture-match ($ACTUAL_PLATFORM)"
else
  fail "image-architecture-match" "$PLATFORM" "${ACTUAL_PLATFORM:-inspect failed (exit $INSPECT_EXIT)}"
fi

# ── Step 3: Admin key discovery via docker exec on /data/.admin_key ──────────

printf '\n%s\n' "--- Step 3: Admin key discovery ---"
ADMIN_KEY=$(docker exec "$CONTAINER_NAME" cat /data/.admin_key 2>/dev/null) && EXEC_EXIT=0 || EXEC_EXIT=$?

if [ "$EXEC_EXIT" -ne 0 ]; then
  fail "admin-key-file-exists" "/data/.admin_key readable via docker exec" "docker exec exit $EXEC_EXIT"
  printf "\nAborting: cannot read admin key from container.\n"
  exit 1
fi
pass "admin-key-file-exists"

if [ -n "$ADMIN_KEY" ]; then
  pass "admin-key-non-empty"
  printf "  admin key length: %d chars\n" "${#ADMIN_KEY}"
else
  fail "admin-key-non-empty" "non-empty admin key value" "empty"
  printf "\nAborting: empty admin key.\n"
  exit 1
fi

# ── Step 4: Batch ingest using Stage 2/3 canonical fixture ───────────────────

printf '\n%s\n' "--- Step 4: Batch ingest ---"
# Verbatim BATCH_BODY from engine/tests/test_linux_e2e.sh:134
# (matches engine/tests/test_macos_e2e.sh:200).
BATCH_BODY='{"requests":[{"action":"addObject","body":{"objectID":"1","title":"The Matrix","year":1999}},{"action":"addObject","body":{"objectID":"2","title":"Inception","year":2010}}]}'

BATCH_RESP_FILE="${TMP_DATA_DIR}/batch_resp.json"
BATCH_HTTP_CODE=$(curl -s -o "$BATCH_RESP_FILE" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/batch" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-Application-Id: e2e-test" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d "$BATCH_BODY") && BATCH_EXIT=0 || BATCH_EXIT=$?

BATCH_RESP=$(cat "$BATCH_RESP_FILE" 2>/dev/null)
printf "Batch HTTP code: %s\n" "$BATCH_HTTP_CODE"
printf "Batch response: %s\n" "$BATCH_RESP"

if [ "$BATCH_EXIT" -eq 0 ] && { [ "$BATCH_HTTP_CODE" = "200" ] || [ "$BATCH_HTTP_CODE" = "201" ]; }; then
  pass "batch-accepted"
else
  fail "batch-accepted" "curl exit 0 and HTTP 200 or 201" "exit=$BATCH_EXIT http=$BATCH_HTTP_CODE"
fi

# Assert a numeric taskID is present (README task workflow contract).
TASK_ID=$(printf '%s' "$BATCH_RESP" | sed -n 's/.*"taskID":\([0-9]*\).*/\1/p' | head -1)
if [ -n "$TASK_ID" ]; then
  pass "batch-taskID-present (taskID=$TASK_ID)"
else
  fail "batch-taskID-present" '"taskID":<integer> in batch response' "$BATCH_RESP"
fi

# ── Step 5: Poll /1/tasks/$TASK_ID until "published" (README workflow) ───────

printf '\n%s\n' "--- Step 5: Task publication ---"
TASK_PUBLISHED="false"
TASK_RESP=""
if [ -n "$TASK_ID" ]; then
  for _i in $(seq 1 40); do
    TASK_RESP=$(curl -s "${BASE_URL}/1/tasks/${TASK_ID}" \
      -H "X-Algolia-Application-Id: e2e-test" \
      -H "X-Algolia-API-Key: ${ADMIN_KEY}" 2>&1) || true
    if printf '%s' "$TASK_RESP" | grep -q '"status":"published"'; then
      TASK_PUBLISHED="true"
      break
    fi
    sleep 0.5
  done
fi

printf "Final task response: %s\n" "$TASK_RESP"

if [ "$TASK_PUBLISHED" = "true" ]; then
  pass "task-published"
else
  fail "task-published" '"status":"published" within 20s' "$TASK_RESP"
fi

# ── Step 6: Query and assert nbHits>=1 and "The Matrix" present ──────────────

printf '\n%s\n' "--- Step 6: Search validation ---"
SEARCH_RESP=$(curl -s -X POST "${BASE_URL}/1/indexes/test_movies/query" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-Application-Id: e2e-test" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d '{"query":"matrix"}' 2>&1)
printf "Search response: %s\n" "$SEARCH_RESP"

NBHITS=$(printf '%s' "$SEARCH_RESP" | grep -o '"nbHits":[0-9]*' | head -1 | cut -d: -f2)
if [ -n "$NBHITS" ] && [ "$NBHITS" -ge 1 ] 2>/dev/null; then
  pass "search-nbHits (nbHits=$NBHITS)"
else
  fail "search-nbHits" "nbHits >= 1" "nbHits=${NBHITS:-<missing>}"
fi

if printf '%s' "$SEARCH_RESP" | grep -q '"The Matrix"'; then
  pass "search-exact-match"
else
  fail "search-exact-match" 'response contains "The Matrix"' "not found in response"
fi

# ── Step 7: Auth contract — missing X-Algolia-Application-Id => 403 ──────────

printf '\n%s\n' "--- Step 7: Auth contract ---"
AUTH_RESP_FILE="${TMP_DATA_DIR}/auth_resp.json"
AUTH_HTTP_CODE=$(curl -s -o "$AUTH_RESP_FILE" -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/query" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d '{"query":"matrix"}')

AUTH_RESP=$(cat "$AUTH_RESP_FILE" 2>/dev/null)
printf "Auth test (no Application-Id) HTTP code: %s\n" "$AUTH_HTTP_CODE"
printf "Auth test response: %s\n" "$AUTH_RESP"

if [ "$AUTH_HTTP_CODE" = "403" ]; then
  pass "auth-403-without-appid"
else
  fail "auth-403-without-appid" "HTTP 403" "HTTP $AUTH_HTTP_CODE"
fi

# Positive control: with both headers, the same query must succeed (proves the
# 403 above is specifically about the missing Application-Id header, not a
# coincidental failure).
AUTH_OK_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/1/indexes/test_movies/query" \
  -H "Content-Type: application/json" \
  -H "X-Algolia-Application-Id: e2e-test" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -d '{"query":"matrix"}')

if [ "$AUTH_OK_HTTP_CODE" = "200" ]; then
  pass "auth-200-with-both-headers"
else
  fail "auth-200-with-both-headers" "HTTP 200" "HTTP $AUTH_OK_HTTP_CODE"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

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
