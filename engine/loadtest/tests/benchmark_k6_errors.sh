#!/usr/bin/env bash
# Shell-level error-path tests for benchmark_k6.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADTEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK_SCRIPT="$LOADTEST_DIR/benchmark_k6.sh"
TEST_PASS=0
TEST_FAIL=0

assert_exit_nonzero() {
  local test_name="$1"
  shift
  local output
  if output=$("$@" 2>&1); then
    echo "FAIL: $test_name - expected non-zero exit, got 0"
    echo "  output: $output"
    TEST_FAIL=$((TEST_FAIL + 1))
    return
  fi
  echo "PASS: $test_name"
  TEST_PASS=$((TEST_PASS + 1))
}

assert_output_contains() {
  local test_name="$1"
  local expected="$2"
  shift 2
  local output
  output=$("$@" 2>&1) || true
  if [[ "$output" == *"$expected"* ]]; then
    echo "PASS: $test_name"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: $test_name - expected output to contain '$expected'"
    echo "  got: $output"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi
}

make_sourceable_script() {
  local dest="$1"
  sed \
    -e "s|^SCRIPT_DIR=.*|SCRIPT_DIR=\"$LOADTEST_DIR\"|" \
    -e 's|^main "\$@"$|:|' \
    "$BENCHMARK_SCRIPT" > "$dest"
  chmod +x "$dest"
}

# TODO: Document write_fixture_helpers.
write_fixture_helpers() {
  local dest="$1"
  cat > "$dest" <<'HELPERS'
#!/usr/bin/env bash
set -euo pipefail

require_loadtest_commands() { :; }
load_shared_loadtest_config() {
  FLAPJACK_BASE_URL="http://fixture.local"
  FLAPJACK_BENCHMARK_INDEX="benchmark_100k"
  FLAPJACK_WRITE_INDEX="loadtest_write"
}
initialize_loadtest_auth_headers() { LOADTEST_AUTH_HEADERS=(); }
load_dashboard_seed_settings() { LOADTEST_SETTINGS_JSON='{}'; }
reset_loadtest_index() { :; }
apply_loadtest_index_settings() { :; }
loadtest_index_exists() {
  if [[ "${BENCHMARK_FIXTURE_INDEX_EXISTS:-1}" == "1" ]]; then
    return 0
  fi
  return 1
}
loadtest_get_index_doc_count() {
  printf '%s' "${BENCHMARK_FIXTURE_DOC_COUNT:-100000}"
}
HELPERS
  chmod +x "$dest"
}

# TODO: Document run_main_with_fixture.
run_main_with_fixture() {
  local sourceable_script="$1"
  local helper_script="$2"
  local index_exists="$3"
  local doc_count="$4"

  BENCHMARK_FIXTURE_INDEX_EXISTS="$index_exists" \
  BENCHMARK_FIXTURE_DOC_COUNT="$doc_count" \
  bash -lc '
    source "$1"
    LOADTEST_HELPERS="$2"

    run_k6_scenario() { :; }
    run_smoke_gate() { :; }
    run_normal_pass() { :; }

    main
  ' bash "$sourceable_script" "$helper_script"
}

# TODO: Document test_missing_index_fails.
test_missing_index_fails() {
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local sourceable_script="$tmp_dir/sourceable_benchmark_k6.sh"
  local fixture_helpers="$tmp_dir/fixture_helpers.sh"

  make_sourceable_script "$sourceable_script"
  write_fixture_helpers "$fixture_helpers"

  assert_exit_nonzero \
    "fails when benchmark_100k index is missing" \
    run_main_with_fixture "$sourceable_script" "$fixture_helpers" "0" "100000"

  assert_output_contains \
    "missing index error message is clear" \
    "index \"benchmark_100k\" was not found" \
    run_main_with_fixture "$sourceable_script" "$fixture_helpers" "0" "100000"

  rm -rf "$tmp_dir"
}

# TODO: Document test_zero_doc_index_fails.
test_zero_doc_index_fails() {
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local sourceable_script="$tmp_dir/sourceable_benchmark_k6.sh"
  local fixture_helpers="$tmp_dir/fixture_helpers.sh"

  make_sourceable_script "$sourceable_script"
  write_fixture_helpers "$fixture_helpers"

  assert_exit_nonzero \
    "fails when benchmark_100k has zero documents" \
    run_main_with_fixture "$sourceable_script" "$fixture_helpers" "1" "0"

  assert_output_contains \
    "zero-doc error message is clear" \
    "contains 0 documents" \
    run_main_with_fixture "$sourceable_script" "$fixture_helpers" "1" "0"

  rm -rf "$tmp_dir"
}

assert_exit_code() {
  local test_name="$1"
  local expected_code="$2"
  shift 2
  local actual_code=0
  "$@" >/dev/null 2>&1 || actual_code=$?
  if [[ "$actual_code" -eq "$expected_code" ]]; then
    echo "PASS: $test_name"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: $test_name - expected exit code $expected_code, got $actual_code"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi
}

# TODO: Document test_smoke_threshold_breach_exits_99.
test_smoke_threshold_breach_exits_99() {
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local sourceable_script="$tmp_dir/sourceable_benchmark_k6.sh"
  local fixture_helpers="$tmp_dir/fixture_helpers.sh"

  make_sourceable_script "$sourceable_script"
  write_fixture_helpers "$fixture_helpers"

  # Override run_smoke_gate to simulate a threshold breach (return 1)
  # and stub run_k6_scenario to do nothing. The wrapper should exit 99.
  assert_exit_code \
    "smoke threshold breach exits 99 (not 1)" \
    99 \
    bash -lc '
      source "$1"
      LOADTEST_HELPERS="$2"
      run_k6_scenario() { :; }
      run_smoke_gate() { return 1; }
      main
    ' bash "$sourceable_script" "$fixture_helpers"

  rm -rf "$tmp_dir"
}

echo "=== benchmark_k6.sh error-path tests ==="
test_missing_index_fails
test_zero_doc_index_fails
test_smoke_threshold_breach_exits_99

echo ""
echo "Results: $TEST_PASS passed, $TEST_FAIL failed"
if [[ $TEST_FAIL -gt 0 ]]; then
  exit 1
fi
echo "All error-path tests passed."
