#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIO_DIR="$ROOT_DIR/scenarios"
SEARCH_SCENARIO="$SCENARIO_DIR/search-throughput.js"
WRITE_SCENARIO="$SCENARIO_DIR/write-throughput.js"
MIXED_SCENARIO="$SCENARIO_DIR/mixed-workload.js"
THROUGHPUT_HELPER="$ROOT_DIR/lib/throughput.js"
SEED_SCRIPT="$ROOT_DIR/seed-loadtest-data.sh"

fail() {
  echo "FAIL: $1"
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing $path"
}

require_import() {
  local scenario_path="$1"
  local required_module="$2"
  grep -Eq "from ['\"][^'\"]*${required_module}['\"]" "$scenario_path" || \
    fail "${scenario_path} must import ${required_module}"
}

require_absent() {
  local scenario_path="$1"
  local forbidden_pattern="$2"
  local label="$3"
  if grep -En "$forbidden_pattern" "$scenario_path" >/tmp/loadtest_forbidden_matches.txt 2>/dev/null; then
    echo "FAIL: ${label} in ${scenario_path}"
    cat /tmp/loadtest_forbidden_matches.txt
    exit 1
  fi
}

require_file "$SEARCH_SCENARIO"
require_file "$WRITE_SCENARIO"
require_file "$MIXED_SCENARIO"
require_file "$SEED_SCRIPT"

for scenario_path in "$SEARCH_SCENARIO" "$WRITE_SCENARIO" "$MIXED_SCENARIO"; do
  require_import "$scenario_path" "lib/config.js"
  require_import "$scenario_path" "lib/http.js"
  require_import "$scenario_path" "k6/execution"

  require_absent "$scenario_path" "FLAPJACK_LOADTEST_|__ENV|process\\.env" \
    "throughput scenarios must read env only via lib/config.js"
  require_absent "$scenario_path" "from ['\"][^'\"]*k6/http['\"]" \
    "throughput scenarios must use lib/http.js wrappers, not raw k6/http"
  require_absent "$scenario_path" "\\b__ITER\\b" \
    "throughput scenarios must not use per-VU __ITER for deterministic selection"
done

if rg --files "$ROOT_DIR" | grep -E '(thresholds?|throughput-config|loadtest-config)\.(json|ya?ml|toml)$' \
  >/tmp/loadtest_duplicate_artifacts.txt 2>/dev/null; then
  echo "FAIL: duplicate config/threshold artifacts are not allowed outside scenario options or lib/*.js"
  cat /tmp/loadtest_duplicate_artifacts.txt
  exit 1
fi

if grep -R --line-number --include='*.js' -E 'thresholds?\.(json|ya?ml|toml)' "$SCENARIO_DIR" \
  >/tmp/loadtest_threshold_imports.txt 2>/dev/null; then
  echo "FAIL: scenarios must not import external threshold artifacts"
  cat /tmp/loadtest_threshold_imports.txt
  exit 1
fi

grep -q 'sharedLoadtestConfig.readIndexName' "$SEARCH_SCENARIO" || \
  fail "search-throughput.js must target sharedLoadtestConfig.readIndexName"
if grep -q 'sharedLoadtestConfig.writeIndexName' "$SEARCH_SCENARIO"; then
  fail "search-throughput.js must not target sharedLoadtestConfig.writeIndexName"
fi

grep -q 'sharedLoadtestConfig.writeIndexName' "$WRITE_SCENARIO" || \
  fail "write-throughput.js must target sharedLoadtestConfig.writeIndexName"
if grep -q 'sharedLoadtestConfig.readIndexName' "$WRITE_SCENARIO"; then
  fail "write-throughput.js must not target sharedLoadtestConfig.readIndexName"
fi

grep -q 'sharedLoadtestConfig.readIndexName' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must include read traffic on sharedLoadtestConfig.readIndexName"
grep -q 'sharedLoadtestConfig.writeIndexName' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must include write traffic on sharedLoadtestConfig.writeIndexName"

grep -Eq 'scenarios[[:space:]]*:' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must export options.scenarios"
grep -Eq 'read[^[:space:]]*[[:space:]]*:' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must define a read scenario"
grep -Eq 'write[^[:space:]]*[[:space:]]*:' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must define a write scenario"
if ! grep -Eq 'tags[[:space:]]*:[[:space:]]*\{[[:space:]]*type[[:space:]]*:[[:space:]]*"search"' "$MIXED_SCENARIO" || \
  ! grep -Eq 'tags[[:space:]]*:[[:space:]]*\{[[:space:]]*type[[:space:]]*:[[:space:]]*"write"' "$MIXED_SCENARIO"; then
  fail "mixed-workload.js must declare tagged read/write scenarios"
fi

grep -q 'exec.scenario.iterationInTest' "$SEARCH_SCENARIO" || \
  fail "search-throughput.js must use exec.scenario.iterationInTest for deterministic selection"
grep -q 'exec.scenario.iterationInTest' "$WRITE_SCENARIO" || \
  fail "write-throughput.js must use exec.scenario.iterationInTest for deterministic payload selection"
grep -q 'exec.scenario.iterationInTest' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must use exec.scenario.iterationInTest for deterministic selection"

grep -Fq 'recordWriteHttpStatusCode(' "$WRITE_SCENARIO" || \
  fail "write-throughput.js must record write HTTP status codes for 4xx/5xx thresholds"
grep -Fq 'recordWriteHttpStatusCode(' "$MIXED_SCENARIO" || \
  fail "mixed-workload.js must record write HTTP status codes for 4xx/5xx thresholds"

for seeded_field in brand category subcategory color tags price inStock releaseYear; do
  grep -q "$seeded_field" "$SEED_SCRIPT" || \
    fail "seed-loadtest-data.sh must continue to seed field ${seeded_field}"

  if ! grep -F --line-number "$seeded_field" "$SEARCH_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
    >/tmp/loadtest_seeded_field_match.txt 2>/dev/null; then
    fail "search throughput contracts must cover seeded field ${seeded_field}"
  fi
done

if ! grep -F --line-number 'http_reqs{type:search}' \
  "$SEARCH_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
  >/tmp/loadtest_search_request_rate_threshold.txt 2>/dev/null; then
  fail "search throughput must define a request-rate threshold"
fi

if ! grep -F --line-number 'checks{check:search returns hits array,type:search}' \
  "$SEARCH_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
  >/tmp/loadtest_search_assertion_threshold.txt 2>/dev/null; then
  fail "search throughput must fail the run when the hits-array assertion fails"
fi

if ! grep -F --line-number 'write_http_4xx_rate' \
  "$WRITE_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
  >/tmp/loadtest_write_4xx_threshold.txt 2>/dev/null; then
  fail "write throughput must define an explicit 4xx error-rate threshold"
fi

if ! grep -F --line-number 'write_http_5xx_rate' \
  "$WRITE_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
  >/tmp/loadtest_write_5xx_threshold.txt 2>/dev/null; then
  fail "write throughput must define an explicit 5xx error-rate threshold"
fi

if ! grep -F --line-number 'checks{check:write returns numeric taskID,type:write}' \
  "$WRITE_SCENARIO" "$MIXED_SCENARIO" "$THROUGHPUT_HELPER" \
  >/tmp/loadtest_write_assertion_threshold.txt 2>/dev/null; then
  fail "write throughput must fail the run when the numeric taskID assertion fails"
fi

echo "PASS: Stage 2 throughput acceptance checks"
