#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SDK_TEST_DIR/../.." && pwd)"
RUN_MATRIX="$SDK_TEST_DIR/run_matrix.sh"

TMP_DIR="$(mktemp -d)"
REPORT_FILE="$TMP_DIR/matrix_report.json"
LOG_DIR="$TMP_DIR/matrix_logs"
TRACKED_REPORT_FILE="$SDK_TEST_DIR/matrix_report.json"
TRACKED_REPORT_CHECKSUM="$(shasum -a 256 "$TRACKED_REPORT_FILE" | awk '{print $1}')"
JAVA_GRADLEW="$REPO_ROOT/sdks/java/gradlew"
JAVA_GRADLEW_BACKUP="$TMP_DIR/gradlew.original"
cleanup() {
  if [[ -f "$JAVA_GRADLEW_BACKUP" ]]; then
    cp "$JAVA_GRADLEW_BACKUP" "$JAVA_GRADLEW"
    chmod +x "$JAVA_GRADLEW"
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

BIN_DIR="$TMP_DIR/bin"
mkdir -p "$BIN_DIR"

cp "$JAVA_GRADLEW" "$JAVA_GRADLEW_BACKUP"
cat > "$JAVA_GRADLEW" <<'WRAP'
#!/usr/bin/env bash
exit 2
WRAP
chmod +x "$JAVA_GRADLEW"

cat > "$BIN_DIR/curl" <<'WRAP'
#!/usr/bin/env bash
if [[ "$*" == *"/health"* ]]; then
  echo '{"status":"ok"}'
  exit 0
fi
exit 0
WRAP
chmod +x "$BIN_DIR/curl"

cat > "$BIN_DIR/go" <<'WRAP'
#!/usr/bin/env bash
if [[ "${1:-}" == "version" ]]; then
  echo "go version go1.23.0 linux/amd64"
  exit 0
fi
exit 2
WRAP
chmod +x "$BIN_DIR/go"

cat > "$BIN_DIR/ruby" <<'WRAP'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo "ruby 3.2.2"
  exit 0
fi
exit 2
WRAP
chmod +x "$BIN_DIR/ruby"

cat > "$BIN_DIR/php" <<'WRAP'
#!/usr/bin/env bash
echo "PHP 8.3.0"
exit 0
WRAP
chmod +x "$BIN_DIR/php"

cat > "$BIN_DIR/java" <<'WRAP'
#!/usr/bin/env bash
echo "openjdk 21"
exit 0
WRAP
chmod +x "$BIN_DIR/java"

cat > "$BIN_DIR/swift" <<'WRAP'
#!/usr/bin/env bash
echo "Swift version 6.0"
exit 0
WRAP
chmod +x "$BIN_DIR/swift"

# Prevent real external SDK toolchains from running.
cat > "$BIN_DIR/bundle" <<'WRAP'
#!/usr/bin/env bash
exit 2
WRAP
chmod +x "$BIN_DIR/bundle"

cat > "$BIN_DIR/gradle" <<'WRAP'
#!/usr/bin/env bash
exit 2
WRAP
chmod +x "$BIN_DIR/gradle"

cat > "$BIN_DIR/phpunit" <<'WRAP'
#!/usr/bin/env bash
exit 2
WRAP
chmod +x "$BIN_DIR/phpunit"

run_case() {
  local case_name="$1"
  local expected_status="$2"
  local node_log="$3"

  cat > "$BIN_DIR/node" <<'WRAP'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then
  echo "v20.10.0"
  exit 0
fi
if [[ "${1:-}" == "contract_tests.js" ]]; then
  cat "$RUN_MATRIX_NODE_LOG"
  exit 1
fi
exit 0
WRAP
  chmod +x "$BIN_DIR/node"

  set +e
  PATH="$BIN_DIR:$PATH" REPORT_FILE="$REPORT_FILE" LOG_DIR="$LOG_DIR" MATRIX_SDKS="js" RUN_MATRIX_NODE_LOG="$node_log" bash "$RUN_MATRIX" >/dev/null 2>&1
  runner_exit=$?
  set -e

  if [[ "$runner_exit" -eq 0 ]]; then
    echo "[$case_name] Expected run_matrix.sh to exit nonzero for simulated JS failure"
    exit 1
  fi

  if [[ ! -f "$REPORT_FILE" ]]; then
    echo "[$case_name] run_matrix.sh exited before initializing REPORT_FILE (exit=$runner_exit)"
    exit 1
  fi

  api_key_status=$(python3 - <<'PY' "$REPORT_FILE"
import json, sys
with open(sys.argv[1]) as f:
    report = json.load(f)
print(report["results"]["js"]["operations"]["api_key_crud"]["status"])
PY
)

  if [[ "$api_key_status" != "$expected_status" ]]; then
    echo "[$case_name] Expected js.api_key_crud=$expected_status, got: $api_key_status"
    exit 1
  fi

  results_sdk_count=$(python3 - <<'PY' "$REPORT_FILE"
import json, sys
with open(sys.argv[1]) as f:
    report = json.load(f)
print(len(report["results"]))
PY
)

  if [[ "$results_sdk_count" != "1" ]]; then
    echo "[$case_name] Expected MATRIX_SDKS=js to only run the JS matrix path"
    exit 1
  fi

  current_checksum="$(shasum -a 256 "$TRACKED_REPORT_FILE" | awk '{print $1}')"
  if [[ "$current_checksum" != "$TRACKED_REPORT_CHECKSUM" ]]; then
    echo "[$case_name] Expected tracked matrix_report.json to remain unchanged"
    exit 1
  fi
}

case1_log="$TMP_DIR/case1.log"
cat > "$case1_log" <<'LOG'
=== Running SDK Contract API Tests (40) ===

✓ POST /1/indexes/{indexName}/batch - addObject
✗ POST /1/indexes/{indexName}/query - filters
  Error: fake filter failure

=== Results: 1 passed, 1 failed ===
LOG

run_case "unverified_api_key_bucket" "fail" "$case1_log"

case2_log="$TMP_DIR/case2.log"
cat > "$case2_log" <<'LOG'
=== Running SDK Contract API Tests (40) ===

✓ POST /1/keys - create API key
✓ GET /1/keys - list API keys
✓ GET /1/keys/:key - get specific API key
✓ DELETE /1/keys/:key - delete API key
✗ POST /1/indexes/{indexName}/query - filters
  Error: fake filter failure

=== Results: 4 passed, 1 failed ===
LOG

run_case "fully_passed_api_key_bucket" "pass" "$case2_log"

echo "PASS: js.api_key_crud classification is correct for unverified and passed scenarios"
