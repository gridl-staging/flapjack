#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="$ROOT_DIR/lib/config.js"
HTTP_FILE="$ROOT_DIR/lib/http.js"
HELPER_FILE="$ROOT_DIR/lib/loadtest_shell_helpers.sh"
DASHBOARD_SEED_SOURCE="$ROOT_DIR/../dashboard/tour/product-seed-data.mjs"
DASHBOARD_SEED_SCRIPT="$ROOT_DIR/../dashboard/tour/seed-data.ts"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "FAIL: missing $CONFIG_FILE"
  exit 1
fi
if [[ ! -f "$HTTP_FILE" ]]; then
  echo "FAIL: missing $HTTP_FILE"
  exit 1
fi
if [[ ! -f "$HELPER_FILE" ]]; then
  echo "FAIL: missing $HELPER_FILE"
  exit 1
fi
if [[ ! -f "$DASHBOARD_SEED_SOURCE" ]]; then
  echo "FAIL: missing $DASHBOARD_SEED_SOURCE"
  exit 1
fi
if [[ ! -f "$DASHBOARD_SEED_SCRIPT" ]]; then
  echo "FAIL: missing $DASHBOARD_SEED_SCRIPT"
  exit 1
fi

if grep -R --line-number --exclude=config.js FLAPJACK_LOADTEST_ \
  "$ROOT_DIR/lib" \
  "$ROOT_DIR/scenarios" \
  "$ROOT_DIR/seed-loadtest-data.sh" >/tmp/loadtest_env_duplicates.txt 2>/dev/null; then
  echo "FAIL: only lib/config.js may read FLAPJACK_LOADTEST_*"
  cat /tmp/loadtest_env_duplicates.txt
  exit 1
fi

for required_symbol in buildHeaders getHealth getMetrics searchPost searchGet updateSettings batchWrite waitForTaskPublished; do
  if ! grep -q "${required_symbol}" "$HTTP_FILE"; then
    echo "FAIL: missing helper ${required_symbol} in lib/http.js"
    exit 1
  fi
done

if ! grep -q 'import("./config.js")' "$HELPER_FILE"; then
  echo "FAIL: shared shell helper must load shared config from lib/config.js"
  exit 1
fi
for helper_symbol in load_dashboard_seed_settings reset_loadtest_index apply_loadtest_index_settings; do
  if ! grep -Eq "^${helper_symbol}\\(\\)" "$HELPER_FILE"; then
    echo "FAIL: ${HELPER_FILE} must define ${helper_symbol}()"
    exit 1
  fi
done

if grep -Eq '^load_dashboard_seed_settings\(\)|^recreate_index\(\)|^apply_settings\(\)' "$ROOT_DIR/seed-loadtest-data.sh"; then
  echo "FAIL: seed-loadtest-data.sh must use shared dashboard/reset/settings helpers from lib/loadtest_shell_helpers.sh"
  exit 1
fi

for helper_consumer in "$ROOT_DIR/seed-loadtest-data.sh" "$ROOT_DIR/tests/seed_acceptance.sh"; do
  if ! grep -q 'loadtest_shell_helpers.sh' "$helper_consumer"; then
    echo "FAIL: ${helper_consumer} must source lib/loadtest_shell_helpers.sh"
    exit 1
  fi
done
for required_call in \
  'load_dashboard_seed_settings' \
  'reset_loadtest_index[[:space:]]+"\$FLAPJACK_READ_INDEX"' \
  'reset_loadtest_index[[:space:]]+"\$FLAPJACK_WRITE_INDEX"' \
  'apply_loadtest_index_settings[[:space:]]+"\$FLAPJACK_READ_INDEX"' \
  'apply_loadtest_index_settings[[:space:]]+"\$FLAPJACK_WRITE_INDEX"'; do
  if ! grep -Eq "$required_call" "$ROOT_DIR/seed-loadtest-data.sh"; then
    echo "FAIL: seed-loadtest-data.sh must call shared helper pattern: ${required_call}"
    exit 1
  fi
done
if ! grep -q '../dashboard/tour/product-seed-data.mjs' "$ROOT_DIR/seed-loadtest-data.sh"; then
  echo "FAIL: seed-loadtest-data.sh must derive seed data from dashboard/tour/product-seed-data.mjs"
  exit 1
fi
if ! grep -q './product-seed-data.mjs' "$DASHBOARD_SEED_SCRIPT"; then
  echo "FAIL: dashboard/tour/seed-data.ts must import ./product-seed-data.mjs"
  exit 1
fi

for required_contract in \
  "/health" \
  "/metrics" \
  "/query" \
  "/settings" \
  "/batch" \
  "/1/tasks/" \
  "pendingTask" \
  "\"published\""; do
  if ! grep -q "$required_contract" "$HTTP_FILE"; then
    echo "FAIL: missing Stage 1 contract text '$required_contract' in lib/http.js"
    exit 1
  fi
done

echo "PASS: foundation helper acceptance checks"
