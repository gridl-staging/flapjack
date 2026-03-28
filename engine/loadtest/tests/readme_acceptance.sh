#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README_FILE="$ROOT_DIR/README.md"

fail() {
  echo "FAIL: $1"
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing $path"
}

require_text() {
  local pattern="$1"
  local message="$2"
  grep -Eqi "$pattern" "$README_FILE" || fail "$message"
}

require_file "$README_FILE"

for prerequisite in k6 Rust curl jq node; do
  require_text "\\b${prerequisite}\\b" "README must list prerequisite: ${prerequisite}"
done

require_text "(^|[[:space:]])(bash[[:space:]]+)?\\./run\\.sh([[:space:]]|$)" \
  "README must document full-suite runner usage with run.sh"

for scenario in smoke.js search-throughput.js write-throughput.js mixed-workload.js spike.js memory-pressure.js; do
  require_text "k6[[:space:]]+(run|inspect)[[:space:]].*scenarios/${scenario}" \
    "README must include a direct k6 command for scenarios/${scenario}"
done

for loadtest_var in \
  FLAPJACK_LOADTEST_BASE_URL \
  FLAPJACK_LOADTEST_APP_ID \
  FLAPJACK_LOADTEST_API_KEY \
  FLAPJACK_LOADTEST_READ_INDEX \
  FLAPJACK_LOADTEST_WRITE_INDEX \
  FLAPJACK_LOADTEST_TASK_MAX_ATTEMPTS \
  FLAPJACK_LOADTEST_TASK_POLL_INTERVAL_SECONDS; do
  require_text "\\b${loadtest_var}\\b" "README must document shared config variable ${loadtest_var}"
done
require_text "sharedLoadtestConfig" \
  "README must reference lib/config.js::sharedLoadtestConfig as the config source"

require_text "engine/loadtest/results/<timestamp>/" \
  "README must describe the timestamped results layout"

require_text "seed-loadtest-data\\.sh" \
  "README must document seed-loadtest-data.sh as the prerequisite seeding/reset step for direct scenario runs"
require_text "running, seeded server" \
  "README must state that direct scenario commands assume a running, seeded server"

require_text "memory-pressure\\.js" \
  "README must describe memory-pressure scenario execution"
require_text "restart" \
  "README must describe the required alternate-memory server restart flow"

if grep -Eq 'FLAPJACK_MEMORY_(LIMIT_MB|HIGH_WATERMARK|CRITICAL)[^[:digit:]]*[0-9]' "$README_FILE"; then
  fail "README must not duplicate pressure-threshold numeric values"
fi

echo "PASS: Stage 4 README acceptance checks"
