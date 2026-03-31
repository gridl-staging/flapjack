#!/bin/sh
# readme_api_smoke.sh — Validate README curl examples against a local build.
#
# Builds the flapjack binary (or uses $FLAPJACK_BIN), starts it on an
# ephemeral port with auth enabled, and exercises the README's local API curl
# examples. The migrate-from-Algolia example is verified as present but skipped
# at runtime because it requires live external credentials.
#
# Usage:
#   ./engine/tests/readme_api_smoke.sh             # build + test
#   FLAPJACK_BIN=./target/release/flapjack \
#     ./engine/tests/readme_api_smoke.sh           # use pre-built binary

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENGINE_DIR="$REPO_DIR/engine"
README_PATH="$REPO_DIR/README.md"

# ── Test helpers ─────────────────────────────────────────────────────────────

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0
SERVER_PID=""
TMP_DATA=""
BUILD_LOG=""

pass() {
  TESTS_PASSED=$((TESTS_PASSED + 1))
  TESTS_RUN=$((TESTS_RUN + 1))
  printf "  \033[0;32m✓\033[0m %s\n" "$1"
}

fail() {
  TESTS_FAILED=$((TESTS_FAILED + 1))
  TESTS_RUN=$((TESTS_RUN + 1))
  printf "  \033[0;31m✗\033[0m %s\n" "$1"
  if [ -n "${2:-}" ]; then
    printf "    %s\n" "$2"
  fi
}

skip() {
  TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
  printf "  \033[1;33m-\033[0m %s\n" "$1"
}

# TODO: Document extract_readme_curl_block.
extract_readme_curl_block() {
  path="$1"

  # Pull the exact multi-line curl block from README so CI fails when docs drift.
  awk -v path="$path" '
    $0 ~ /^curl / && index($0, path) {
      capture = 1
    }
    capture {
      print
      line = $0
      quote_count += gsub(/\047/, "", line)
      if ($0 !~ /\\$/ && quote_count % 2 == 0) {
        found = 1
        exit
      }
    }
    END {
      if (!found) {
        exit 1
      }
    }
  ' "$README_PATH"
}

# TODO: Document run_readme_curl.
run_readme_curl() {
  path="$1"

  command_block=$(extract_readme_curl_block "$path")
  # Treat README curl blocks as data, not executable shell, so docs drift checks
  # cannot smuggle extra shell commands into CI via command substitution, pipes,
  # or separators.
  COMMAND_BLOCK="$command_block" API_KEY="$ADMIN_KEY" BASE="$BASE" python3 - <<'PY'
import os
import shlex
import subprocess
import sys

command_block = os.environ["COMMAND_BLOCK"]
base = os.environ["BASE"]
api_key = os.environ["API_KEY"]

expanded = (
    command_block
    .replace("${API_KEY}", api_key)
    .replace("$API_KEY", api_key)
    .replace("http://localhost:7700", base)
)
expanded = expanded.replace("\\\n", " ")

for forbidden in ("`", "$(", ";", "&&", "||", "|", "<", ">"):
    if forbidden in expanded:
        raise SystemExit(f"Unsupported shell control token in README curl block: {forbidden}")

try:
    args = shlex.split(expanded, posix=True)
except ValueError as exc:
    raise SystemExit(f"Could not parse README curl block safely: {exc}") from exc

if not args or args[0] != "curl":
    raise SystemExit("README curl block must start with curl")

result = subprocess.run(
    ["curl", "-sS", "-w", r"\n%{http_code}", *args[1:]],
    check=False,
    capture_output=True,
    text=True,
)
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
raise SystemExit(result.returncode)
PY
}

extract_task_id() {
  response_body="$1"
  printf '%s\n' "$response_body" | sed -n 's/.*"taskID":\([0-9]*\).*/\1/p' | head -1
}

# TODO: Document wait_for_task_published.
wait_for_task_published() {
  task_id="$1"

  for _i in $(seq 1 20); do
    task_resp=$(curl -s "${BASE}/1/tasks/${task_id}" \
      -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
      -H "X-Algolia-Application-Id: flapjack" 2>&1) || true
    if echo "$task_resp" | grep -q '"published"'; then
      pass "Task ${task_id} reached published status"
      return 0
    fi
    sleep 0.5
  done

  fail "Task ${task_id} did not reach published within 10s" "$task_resp"
  return 1
}

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$TMP_DATA" ] && [ -d "$TMP_DATA" ]; then
    rm -rf "$TMP_DATA"
  fi
  if [ -n "$BUILD_LOG" ] && [ -f "$BUILD_LOG" ]; then
    rm -f "$BUILD_LOG"
  fi
}
trap cleanup EXIT

# ── Build or locate binary ───────────────────────────────────────────────────

printf "\033[1mREADME API Smoke Tests\033[0m\n"

if [ -n "${FLAPJACK_BIN:-}" ]; then
  if [ ! -x "$FLAPJACK_BIN" ]; then
    echo "ERROR: FLAPJACK_BIN=$FLAPJACK_BIN is not executable"
    exit 1
  fi
  BIN="$FLAPJACK_BIN"
  printf "  Using pre-built binary: %s\n" "$BIN"
else
  printf "  Building flapjack-server package (release)...\n"
  BUILD_LOG=$(mktemp)
  if (cd "$ENGINE_DIR" && cargo build -p flapjack-server --release >"$BUILD_LOG" 2>&1); then
    tail -5 "$BUILD_LOG"
  else
    tail -20 "$BUILD_LOG" >&2 || true
    echo "ERROR: cargo build -p flapjack-server --release failed" >&2
    exit 1
  fi
  BIN="$ENGINE_DIR/target/release/flapjack"
  if [ ! -x "$BIN" ]; then
    echo "ERROR: build failed — $BIN not found"
    exit 1
  fi
fi

# ── Start server on ephemeral port ───────────────────────────────────────────

TMP_DATA=$(mktemp -d)
ADMIN_KEY="fj_smoketest_$(date +%s)"

FLAPJACK_ADMIN_KEY="$ADMIN_KEY" \
  FLAPJACK_DATA_DIR="$TMP_DATA" \
  "$BIN" --auto-port > "$TMP_DATA/server.log" 2>&1 &
SERVER_PID=$!

# Poll for server readiness via /health (up to 30s)
PORT=""
for _i in $(seq 1 60); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: server exited unexpectedly"
    cat "$TMP_DATA/server.log" 2>/dev/null || true
    exit 1
  fi
  # Extract port from server log: "Local:      http://127.0.0.1:PORT"
  if [ -z "$PORT" ]; then
    PORT=$(sed -n 's/.*Local:.*http:\/\/127\.0\.0\.1:\([0-9]*\).*/\1/p' "$TMP_DATA/server.log" | head -1) || true
  fi
  if [ -n "$PORT" ] && curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

if [ -z "$PORT" ]; then
  echo "ERROR: could not determine server port within 30s"
  cat "$TMP_DATA/server.log" 2>/dev/null || true
  exit 1
fi

BASE="http://127.0.0.1:${PORT}"
printf "  Server ready on port %s (pid %s)\n\n" "$PORT" "$SERVER_PID"

# ── Health endpoint ──────────────────────────────────────────────────────────

health_resp=$(curl -s -w "\n%{http_code}" "${BASE}/health" 2>&1)
health_http=$(echo "$health_resp" | tail -1)
health_body=$(echo "$health_resp" | sed '$d')

if [ "$health_http" = "200" ] && echo "$health_body" | grep -q '{'; then
  pass "GET /health returns HTTP 200 with valid JSON"
else
  fail "GET /health expected HTTP 200 with JSON body" "HTTP $health_http — $health_body"
fi

# ── README batch-add (execute README curl block verbatim) ────────────────────

if batch_resp=$(run_readme_curl "/1/indexes/movies/batch" 2>&1); then
  :
else
  fail "README batch curl command failed to execute" "$batch_resp"
  batch_resp=""
fi

if [ -n "$batch_resp" ]; then
  batch_http=$(echo "$batch_resp" | tail -1)
  batch_body=$(echo "$batch_resp" | sed '$d')
else
  batch_http=""
  batch_body=""
fi

if [ "$batch_http" = "200" ] && echo "$batch_body" | grep -q '"taskID"'; then
  pass "POST /1/indexes/movies/batch returns taskID (HTTP $batch_http)"
else
  fail "POST /1/indexes/movies/batch missing taskID" "HTTP $batch_http — $batch_body"
fi

# Poll task until published (up to 10s)
task_id=$(extract_task_id "$batch_body") || true
if [ -n "$task_id" ]; then
  wait_for_task_published "$task_id" || true
else
  fail "Could not extract taskID from batch response"
fi

# ── README typo-search (execute README curl block verbatim) ──────────────────

if search_resp=$(run_readme_curl "/1/indexes/movies/query" 2>&1); then
  search_http=$(echo "$search_resp" | tail -1)
  search_body=$(echo "$search_resp" | sed '$d')
else
  search_http=""
  search_body="$search_resp"
fi

if [ "$search_http" = "200" ] && echo "$search_body" | grep -q '"The Matrix"'; then
  pass "POST /1/indexes/movies/query README typo-search returns The Matrix"
else
  fail "README typo-search did not return HTTP 200 with The Matrix" "HTTP $search_http — $search_body"
fi

# ── README migrated-index search (execute README curl block verbatim) ─────────

# The README's post-migration search example assumes /1/indexes/products already
# contains migrated documents. Seed equivalent local data here so the exact curl
# example can be validated without depending on external Algolia credentials.
products_seed_resp=$(curl -sS -w "\n%{http_code}" -X POST "${BASE}/1/indexes/products/batch" \
  -H "X-Algolia-API-Key: ${ADMIN_KEY}" \
  -H "X-Algolia-Application-Id: flapjack" \
  -H "Content-Type: application/json" \
  -d '{"requests":[{"action":"addObject","body":{"objectID":"product-1","name":"widget alpha"}}]}' 2>&1) || true
products_seed_http=$(echo "$products_seed_resp" | tail -1)
products_seed_body=$(echo "$products_seed_resp" | sed '$d')

if [ "$products_seed_http" = "200" ] && echo "$products_seed_body" | grep -q '"taskID"'; then
  products_task_id=$(extract_task_id "$products_seed_body") || true
  if [ -n "$products_task_id" ]; then
    wait_for_task_published "$products_task_id" || true
  else
    fail "Could not extract taskID from local products seed response"
  fi
else
  fail "Local products seed failed before README migrated-index query check" \
    "HTTP $products_seed_http — $products_seed_body"
fi

if products_search_resp=$(run_readme_curl "/1/indexes/products/query" 2>&1); then
  products_search_http=$(echo "$products_search_resp" | tail -1)
  products_search_body=$(echo "$products_search_resp" | sed '$d')
else
  products_search_http=""
  products_search_body="$products_search_resp"
fi

if [ "$products_search_http" = "200" ] && echo "$products_search_body" | grep -q '"widget alpha"'; then
  pass "POST /1/indexes/products/query README migrated-index search returns seeded data"
else
  fail "README migrated-index search did not return HTTP 200 with seeded data" \
    "HTTP $products_search_http — $products_search_body"
fi

# ── README migrate-from-Algolia example presence ─────────────────────────────

if extract_readme_curl_block "/1/migrate-from-algolia" >/dev/null 2>&1; then
  skip "POST /1/migrate-from-algolia example present; runtime execution skipped because it requires live Algolia credentials"
else
  fail "README migrate-from-Algolia curl block could not be extracted"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

printf "\n\033[1mResults: %d/%d passed\033[0m" "$TESTS_PASSED" "$TESTS_RUN"
if [ "$TESTS_SKIPPED" -gt 0 ]; then
  printf " \033[1;33m(%d skipped)\033[0m" "$TESTS_SKIPPED"
fi
printf "\n"
if [ "$TESTS_FAILED" -gt 0 ]; then
  printf "\033[0;31m%d test(s) failed\033[0m\n" "$TESTS_FAILED"
  exit 1
fi
printf "\033[0;32mAll tests passed\033[0m\n"
exit 0
