#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENGINE_DIR="$(cd "$SDK_TEST_DIR/.." && pwd)"
RUNNER="$ENGINE_DIR/s/test"

TMP_DIR="$(mktemp -d)"
BIN_DIR="$TMP_DIR/bin"
OUTPUT_FILE="$TMP_DIR/output.log"
CALL_LOG="$TMP_DIR/calls.log"

DASHBOARD_NODE_MODULES="$ENGINE_DIR/dashboard/node_modules"
DASHBOARD_NODE_MODULES_BACKUP="$TMP_DIR/dashboard_node_modules.backup"
DASHBOARD_NODE_MODULES_WAS_PRESENT=false

fail_with_logs() {
  local message="$1"
  shift
  echo "$message"
  for log_file in "$@"; do
    [ -f "$log_file" ] && cat "$log_file"
  done
  exit 1
}

assert_file_contains() {
  local file_path="$1"
  local expected="$2"
  local failure_message="$3"
  shift 3
  if ! grep -Fq "$expected" "$file_path"; then
    fail_with_logs "$failure_message" "$file_path" "$@"
  fi
}

cleanup() {
  rm -rf "$DASHBOARD_NODE_MODULES"

  if [ "$DASHBOARD_NODE_MODULES_WAS_PRESENT" = "true" ]; then
    mv "$DASHBOARD_NODE_MODULES_BACKUP" "$DASHBOARD_NODE_MODULES"
  fi

  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$BIN_DIR"

if [ -e "$DASHBOARD_NODE_MODULES" ]; then
  DASHBOARD_NODE_MODULES_WAS_PRESENT=true
  mv "$DASHBOARD_NODE_MODULES" "$DASHBOARD_NODE_MODULES_BACKUP"
fi

cat > "$BIN_DIR/curl" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"/health"* ]]; then
  exit 0
fi
if [[ "$*" == *"/1/keys"* ]]; then
  exit 0
fi
exit 0
WRAP
chmod +x "$BIN_DIR/curl"

cat > "$BIN_DIR/npm" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail
printf 'npm cwd=%s args=%s\n' "$PWD" "$*" >> "$STUB_CALL_LOG"
if [[ "${1:-}" == "ci" ]]; then
  mkdir -p "$PWD/node_modules/vitest" "$PWD/node_modules/@playwright/test"
  exit 0
fi
if [[ "${1:-}" == "run" && "${2:-}" == "test:unit:run" ]]; then
  exit 0
fi
if [[ "${1:-}" == "run" && "${2:-}" == "test:e2e-ui:smoke" ]]; then
  exit 0
fi
if [[ "${1:-}" == "run" && "${2:-}" == "test:e2e-ui:full" ]]; then
  exit 99
fi
exit 0
WRAP
chmod +x "$BIN_DIR/npm"

cat > "$BIN_DIR/node" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail
printf 'node cwd=%s args=%s\n' "$PWD" "$*" >> "$STUB_CALL_LOG"
if [[ "${1:-}" == "--version" ]]; then
  echo "v20.10.0"
  exit 0
fi
if [[ "${1:-}" == "-e" ]]; then
  if [[ -d "$PWD/node_modules/vitest" && -d "$PWD/node_modules/@playwright/test" ]]; then
    exit 0
  fi
  exit 42
fi
if [[ "${1:-}" == "scripts/playwright-webserver.mjs" && "${2:-}" == "--wait-port-free" ]]; then
  exit 0
fi
exit 0
WRAP
chmod +x "$BIN_DIR/node"

set +e
PATH="$BIN_DIR:$PATH" \
STUB_CALL_LOG="$CALL_LOG" \
"$RUNNER" --dashboard-full >"$OUTPUT_FILE" 2>&1
status=$?
set -e

if [ "$status" -ne 99 ]; then
  fail_with_logs \
    "Expected s/test --dashboard-full to stop at the stubbed full Playwright command" \
    "$OUTPUT_FILE" \
    "$CALL_LOG"
fi

assert_file_contains \
  "$CALL_LOG" \
  "npm cwd=$ENGINE_DIR/dashboard args=run test:unit:run" \
  "Expected --dashboard-full execution to run dashboard unit tests" \
  "$OUTPUT_FILE"

assert_file_contains \
  "$CALL_LOG" \
  "npm cwd=$ENGINE_DIR/dashboard args=run test:e2e-ui:smoke" \
  "Expected --dashboard-full execution to run Playwright smoke before the port wait" \
  "$OUTPUT_FILE"

assert_file_contains \
  "$CALL_LOG" \
  "node cwd=$ENGINE_DIR/dashboard args=scripts/playwright-webserver.mjs --wait-port-free" \
  "Expected --dashboard-full execution to wait for the Playwright webserver port to become free" \
  "$OUTPUT_FILE"

assert_file_contains \
  "$CALL_LOG" \
  "npm cwd=$ENGINE_DIR/dashboard args=run test:e2e-ui:full" \
  "Expected --dashboard-full execution to continue into the full Playwright suite after the port wait" \
  "$OUTPUT_FILE"

unit_line="$(grep -nF "npm cwd=$ENGINE_DIR/dashboard args=run test:unit:run" "$CALL_LOG" | head -n1 | cut -d: -f1)"
smoke_line="$(grep -nF "npm cwd=$ENGINE_DIR/dashboard args=run test:e2e-ui:smoke" "$CALL_LOG" | head -n1 | cut -d: -f1)"
wait_line="$(grep -nF "node cwd=$ENGINE_DIR/dashboard args=scripts/playwright-webserver.mjs --wait-port-free" "$CALL_LOG" | head -n1 | cut -d: -f1)"
full_line="$(grep -nF "npm cwd=$ENGINE_DIR/dashboard args=run test:e2e-ui:full" "$CALL_LOG" | head -n1 | cut -d: -f1)"

if [ "$unit_line" -ge "$smoke_line" ] || [ "$smoke_line" -ge "$wait_line" ] || [ "$wait_line" -ge "$full_line" ]; then
  fail_with_logs \
    "Expected dashboard-full execution order to be unit -> smoke -> wait-port-free -> full" \
    "$CALL_LOG" \
    "$OUTPUT_FILE"
fi

echo "PASS: s/test --dashboard-full waits for the Playwright webserver port between smoke and full"
