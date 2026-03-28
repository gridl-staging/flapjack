#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIO_DIR="$ROOT_DIR/scenarios"
SPIKE_SCENARIO="$SCENARIO_DIR/spike.js"
MEMORY_PRESSURE_SCENARIO="$SCENARIO_DIR/memory-pressure.js"
HTTP_HELPER="$ROOT_DIR/lib/http.js"

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
  if grep -En "$forbidden_pattern" "$scenario_path" >/tmp/loadtest_stress_forbidden_matches.txt 2>/dev/null; then
    echo "FAIL: ${label} in ${scenario_path}"
    cat /tmp/loadtest_stress_forbidden_matches.txt
    exit 1
  fi
}

require_file "$SPIKE_SCENARIO"
require_file "$MEMORY_PRESSURE_SCENARIO"
require_file "$HTTP_HELPER"

for scenario_path in "$SPIKE_SCENARIO" "$MEMORY_PRESSURE_SCENARIO"; do
  require_import "$scenario_path" "lib/config.js"
  require_import "$scenario_path" "lib/http.js"
  require_import "$scenario_path" "k6/execution"

  require_absent "$scenario_path" "FLAPJACK_LOADTEST_|FLAPJACK_MEMORY_|__ENV|process\\.env" \
    "stress scenarios must read env only via lib/config.js"
  require_absent "$scenario_path" "from ['\"][^'\"]*k6/http['\"]" \
    "stress scenarios must use lib/http.js wrappers, not raw k6/http"
  require_absent "$scenario_path" "pressure_override|set_pressure_override" \
    "stress scenarios must not mutate server memory pressure state"
done

grep -q 'export function getInternalStatus' "$HTTP_HELPER" || \
  fail "lib/http.js must export getInternalStatus for stress probes"
grep -q '"/internal/status"' "$HTTP_HELPER" || \
  fail "lib/http.js getInternalStatus must hit /internal/status"

require_import "$SPIKE_SCENARIO" "lib/throughput.js"
grep -q 'searchPost(' "$SPIKE_SCENARIO" || \
  fail "spike.js must issue search traffic through searchPost"
grep -q 'buildSearchRequest' "$SPIKE_SCENARIO" || \
  fail "spike.js must build requests via buildSearchRequest"
grep -q 'SEARCH_THRESHOLDS' "$SPIKE_SCENARIO" || \
  fail "spike.js must reuse shared Stage 2 search threshold surface"
grep -q 'exec.scenario.iterationInTest' "$SPIKE_SCENARIO" || \
  fail "spike.js must use deterministic iteration-driven request selection"
grep -q 'sharedLoadtestConfig.readIndexName' "$SPIKE_SCENARIO" || \
  fail "spike.js must target sharedLoadtestConfig.readIndexName"
if grep -q 'sharedLoadtestConfig.writeIndexName' "$SPIKE_SCENARIO"; then
  fail "spike.js must stay read-only and avoid writeIndexName"
fi
for stage_symbol in WARMUP_STAGE SPIKE_STAGE HOLD_STAGE RECOVERY_STAGE; do
  grep -q "const ${stage_symbol}" "$SPIKE_SCENARIO" || \
    fail "spike.js must define ${stage_symbol}"
done
if ! awk '/stages:/,/\]/ {print}' "$SPIKE_SCENARIO" | tr '\n' ' ' | \
  grep -Eq 'WARMUP_STAGE[^]]*SPIKE_STAGE[^]]*HOLD_STAGE[^]]*RECOVERY_STAGE'; then
  fail "spike.js must define warmup -> spike -> hold -> recovery in one scenario"
fi
grep -q 'target: 40' "$SPIKE_SCENARIO" || \
  fail "spike.js must include a high-VU spike target"

require_import "$MEMORY_PRESSURE_SCENARIO" "lib/throughput.js"
grep -q 'const pressureLevel = healthResponse.json("pressure_level")' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must read pressure_level from /health before branch assertions"
grep -q 'getHealth()' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must probe /health via shared helper"
grep -q 'getInternalStatus()' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must probe /internal/status via shared helper"
grep -q 'searchGet(' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must cover GET search behavior"
grep -q 'searchPost(' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must cover POST search behavior"
grep -q 'batchWrite(' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must cover write behavior"
grep -q 'Retry-After' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must validate Retry-After for 503 memory-pressure responses"
grep -q '=== "5"' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must require Retry-After: 5"
grep -q 'pressureLevel === "normal"' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must include normal-pressure assertions"
grep -q 'pressureLevel === "elevated"' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must include elevated-pressure assertions"
grep -q 'pressureLevel === "critical"' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must include critical-pressure assertions"
grep -q 'normal pressure get search returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert normal pressure GET search success"
grep -q 'normal pressure post search returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert normal pressure POST search success"
grep -q 'normal pressure write returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert normal pressure write success"
grep -q 'normal pressure internal status returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert normal pressure /internal/status success"
grep -q 'elevated pressure get search returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert elevated pressure GET search success"
grep -q 'elevated pressure internal status returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert elevated pressure /internal/status success"
grep -q 'elevated pressure post search returns 503' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert elevated pressure POST search rejection"
grep -q 'elevated pressure write returns 503' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert elevated pressure write rejection"
grep -q 'critical pressure internal status returns 200' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert critical pressure /internal/status success"
grep -q 'critical pressure get search returns 503' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert critical pressure GET search rejection"
grep -q 'critical pressure post search returns 503' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert critical pressure POST search rejection"
grep -q 'critical pressure write returns 503' "$MEMORY_PRESSURE_SCENARIO" || \
  fail "memory-pressure.js must assert critical pressure write rejection"

echo "PASS: Stage 3 stress acceptance checks"
