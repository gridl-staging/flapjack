#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENGINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STAGE5_WRAPPER="$ENGINE_DIR/tests/test_stage5_algolia_compat.sh"
BROADER_WRAPPER="$ENGINE_DIR/tests/test_algolia_compat_broader.sh"
HELPER="$ENGINE_DIR/tests/algolia_compat_bootstrap.sh"
SOURCE_LINE="source \"\$SCRIPT_DIR/algolia_compat_bootstrap.sh\""
HARNESS_FUNCTION_MAX_LINES=100
WRAPPER_FUNCTION_CALL="run_algolia_compat_harness"

if [ ! -f "$HELPER" ]; then
  echo "Expected shared bootstrap helper at $HELPER"
  exit 1
fi

source "$HELPER"

assert_wrapper_delegates_to_shared_owner() {
  local wrapper="$1"

  if ! grep -Fq "$SOURCE_LINE" "$wrapper"; then
    echo "Expected wrapper to source shared helper: $wrapper"
    return 1
  fi

  if [ "$(grep -Ec "^${WRAPPER_FUNCTION_CALL}[[:space:]\\\\]*$" "$wrapper")" -ne 1 ]; then
    echo "Expected exactly one ${WRAPPER_FUNCTION_CALL} entrypoint in $wrapper"
    return 1
  fi

  if grep -Eq "for[[:space:]]|while[[:space:]]|curl[[:space:]]|-sf[[:space:]]|npm[[:space:]]+ci|lsof[[:space:]]|cargo[[:space:]]+build|mktemp[[:space:]]+-d|node_modules|/health|trap[[:space:]]+cleanup" "$wrapper"; then
    echo "Expected wrappers to delegate bootstrap flow (build/health/deps), but found bootstrap logic in $wrapper"
    return 1
  fi
}

for wrapper in "$STAGE5_WRAPPER" "$BROADER_WRAPPER"; do
  assert_wrapper_delegates_to_shared_owner "$wrapper"
done

assert_shared_bootstrap_returns_server_log_path() {
  local fixture_data_dir="" fixture_bin="" server_log=""

  fixture_data_dir="$(mktemp -d)"
  fixture_bin="$(mktemp)"

  cat >"$fixture_bin" <<EOF
#!/usr/bin/env bash
while true; do
  sleep 5
done
EOF
  chmod +x "$fixture_bin"

  ALGOLIA_COMPAT_DATA_DIR="$fixture_data_dir"
  start_algolia_compat_server "$fixture_bin" "fixture_server.log" server_log

  if [ -z "$server_log" ]; then
    echo "Expected start_algolia_compat_server() to populate caller server_log reference"
    kill "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
    wait "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
    rm -f "$fixture_bin"
    rm -rf "$fixture_data_dir"
    return 1
  fi

  if [ ! -f "$server_log" ]; then
    echo "Expected server log path to exist after startup: $server_log"
    kill "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
    wait "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
    rm -f "$fixture_bin"
    rm -rf "$fixture_data_dir"
    return 1
  fi

  kill "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
  wait "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
  rm -f "$fixture_bin"
  rm -rf "$fixture_data_dir"
}

assert_shared_bootstrap_redacts_admin_key_in_logs() {
  local fixture_data_dir="" fixture_log="" output=""

  fixture_data_dir="$(mktemp -d)"
  fixture_log="$(mktemp)"
  FLAPJACK_URL="http://127.0.0.1:19999"
  FLAPJACK_ADMIN_KEY="super-secret-admin-key"

  output="$(
    run_algolia_compat_npm_script "$ENGINE_DIR/sdk_test" "definitely_missing_script_for_redaction_test" "$fixture_log" 2>&1 || true
  )"

  rm -f "$fixture_log"
  rm -rf "$fixture_data_dir"

  if printf "%s\n" "$output" | grep -F "super-secret-admin-key" >/dev/null; then
    echo "Expected run_algolia_compat_npm_script() to redact FLAPJACK_ADMIN_KEY from logs"
    return 1
  fi

  if ! printf "%s\n" "$output" | grep -F "FLAPJACK_ADMIN_KEY=<redacted>" >/dev/null; then
    echo "Expected redacted FLAPJACK_ADMIN_KEY marker in bootstrap logs"
    return 1
  fi
}

assert_shared_bootstrap_rejects_invalid_server_log_ref() {
  local fixture_data_dir="" fixture_bin="" invalid_ref="" failure_output=""

  fixture_data_dir="$(mktemp -d)"
  fixture_bin="$(mktemp)"
  invalid_ref="server-log"

  cat >"$fixture_bin" <<EOF
#!/usr/bin/env bash
while true; do
  sleep 5
done
EOF
  chmod +x "$fixture_bin"

  ALGOLIA_COMPAT_DATA_DIR="$fixture_data_dir"
  set +e
  failure_output="$(start_algolia_compat_server "$fixture_bin" "fixture_server.log" "$invalid_ref" 2>&1)"
  status=$?
  set -e

  kill "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
  wait "$ALGOLIA_COMPAT_SERVER_PID" 2>/dev/null || true
  ALGOLIA_COMPAT_SERVER_PID=""
  rm -f "$fixture_bin"
  rm -rf "$fixture_data_dir"

  if [ "$status" -eq 0 ]; then
    echo "Expected start_algolia_compat_server() to reject invalid variable names"
    return 1
  fi

  if ! printf "%s\n" "$failure_output" | grep -F "invalid server-log variable name" >/dev/null; then
    echo "Expected invalid-ref error message from start_algolia_compat_server()"
    return 1
  fi
}

assert_shared_bootstrap_returns_server_log_path
assert_shared_bootstrap_redacts_admin_key_in_logs
assert_shared_bootstrap_rejects_invalid_server_log_ref

negative_fixture="$(mktemp)"
cat >"$negative_fixture" <<'EOF'
#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/algolia_compat_bootstrap.sh"
run_algolia_compat_harness \
  "Fixture wrapper" \
  "127.0.0.1:17899" \
  "fixture_key" \
  "fixture.log" \
  "test:fixture" \
  "false"
for attempt in $(seq 1 3); do
  if curl -sf "http://127.0.0.1:17899/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
EOF

if assert_wrapper_delegates_to_shared_owner "$negative_fixture"; then
  rm -f "$negative_fixture"
  echo "Expected structural guard to reject duplicate wrapper bootstrap logic fixture"
  exit 1
fi
rm -f "$negative_fixture"

function_length="$(awk '
  /^run_algolia_compat_harness\(\) \{/ { in_fn=1; count=1; next }
  in_fn { count++ }
  in_fn && /^\}/ { print count; exit }
' "$HELPER")"

if [ -z "$function_length" ]; then
  echo "Expected run_algolia_compat_harness() in $HELPER"
  exit 1
fi

if [ "$function_length" -gt "$HARNESS_FUNCTION_MAX_LINES" ]; then
  echo "run_algolia_compat_harness() is $function_length lines; must be <= $HARNESS_FUNCTION_MAX_LINES"
  exit 1
fi

echo "PASS: Stage 3 and Stage 5 wrappers share one bootstrap owner seam"
