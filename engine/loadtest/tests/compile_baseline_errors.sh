#!/usr/bin/env bash
# Shell-level tests for compile_baseline.sh artifact discovery and fallback paths.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADTEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPILE_SCRIPT="$LOADTEST_DIR/compile_baseline.sh"
TEST_PASS=0
TEST_FAIL=0
SCENARIOS=(smoke search-throughput write-throughput mixed-workload spike memory-pressure)

TEST_TMP_DIR=""
TEST_MOCK_COMPILER=""
TEST_BENCHMARKS_FILE=""
TEST_RESULTS_DIR=""

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

assert_exit_zero() {
  local test_name="$1"
  shift
  local output exit_code=0
  output=$("$@" 2>&1) || exit_code=$?
  if [[ "$exit_code" -eq 0 ]]; then
    echo "PASS: $test_name"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: $test_name - expected exit 0, got $exit_code"
    echo "  output: $output"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi
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

write_mock_compiler() {
  local dest="$1"
  cat > "$dest" <<'MOCK'
#!/usr/bin/env node
// Mock compile_baseline.mjs — prints args as markdown placeholder.
process.stdout.write("# Large-Dataset Baseline\n\nMock output for testing.\n");
MOCK
  chmod +x "$dest"
}

write_arg_dump_compiler() {
  local dest="$1"
  cat > "$dest" <<'MOCK'
#!/usr/bin/env node
process.stdout.write(process.argv.slice(2).join("\n"));
MOCK
  chmod +x "$dest"
}

setup_test_workspace() {
  local compiler_writer="$1"

  TEST_TMP_DIR="$(mktemp -d)"
  TEST_MOCK_COMPILER="$TEST_TMP_DIR/compile_baseline.mjs"
  TEST_BENCHMARKS_FILE="$TEST_TMP_DIR/BENCHMARKS.md"
  TEST_RESULTS_DIR="$TEST_TMP_DIR/results"

  "$compiler_writer" "$TEST_MOCK_COMPILER"
  echo "# Existing baseline" > "$TEST_BENCHMARKS_FILE"
}

teardown_test_workspace() {
  if [[ -n "$TEST_TMP_DIR" ]]; then
    rm -rf "$TEST_TMP_DIR"
  fi
  TEST_TMP_DIR=""
  TEST_MOCK_COMPILER=""
  TEST_BENCHMARKS_FILE=""
  TEST_RESULTS_DIR=""
}

run_compile_script() {
  env RESULTS_BASE_DIR="$TEST_RESULTS_DIR" \
      BASELINE_COMPILER="$TEST_MOCK_COMPILER" \
      BENCHMARKS_FILE="$TEST_BENCHMARKS_FILE" \
      DASHBOARD_REPORT="$TEST_TMP_DIR/nonexistent/results.json" \
      "$@" \
  bash "$COMPILE_SCRIPT"
}

benchmarks_output() {
  cat "$TEST_BENCHMARKS_FILE"
}

# TODO: Document write_fixture_results.
write_fixture_results() {
  local results_dir="$1"
  local include_k6_json="${2:-yes}"

  mkdir -p "$results_dir/20260322T180000Z"
  echo '{"timestamp":"2026-03-22T16:02:00Z","indexName":"benchmark_100k","totalDocs":100000,"batchCount":100,"wallClockMs":54429}' > "$results_dir/20260322T180000Z/import_benchmark.json"
  echo '{"timestamp":"2026-03-22T16:30:00Z","indexName":"benchmark_100k","docCount":100000,"wallClockMs":1180}' > "$results_dir/20260322T180000Z/search_benchmark.json"

  local scenario
  for scenario in "${SCENARIOS[@]}"; do
    echo "k6 stdout output for $scenario" > "$results_dir/20260322T180000Z/${scenario}.stdout.txt"
    if [[ "$include_k6_json" == "yes" ]]; then
      echo "{}" > "$results_dir/20260322T180000Z/${scenario}.json"
    fi
  done
}

# Test: compile_baseline.sh succeeds when only stdout artifacts exist (no k6 JSON)
test_stdout_only_fallback_succeeds() {
  setup_test_workspace write_mock_compiler
  write_fixture_results "$TEST_RESULTS_DIR" "no"

  assert_exit_zero \
    "compile_baseline.sh succeeds with stdout-only k6 artifacts (no JSON)" \
    run_compile_script

  teardown_test_workspace
}

# TODO: Document test_missing_both_artifacts_fails.
test_missing_both_artifacts_fails() {
  setup_test_workspace write_mock_compiler

  mkdir -p "$TEST_RESULTS_DIR/20260322T180000Z"
  echo '{}' > "$TEST_RESULTS_DIR/20260322T180000Z/import_benchmark.json"
  echo '{}' > "$TEST_RESULTS_DIR/20260322T180000Z/search_benchmark.json"
  # Only write smoke artifacts — other scenarios will be missing entirely
  echo "smoke output" > "$TEST_RESULTS_DIR/20260322T180000Z/smoke.stdout.txt"

  assert_exit_nonzero \
    "compile_baseline.sh fails when scenario artifacts are fully missing" \
    run_compile_script

  assert_output_contains \
    "missing complete k6 run error message is clear" \
    "missing complete k6 artifact set for scenarios:" \
    run_compile_script

  teardown_test_workspace
}

# TODO: Document test_newer_partial_k6_run_is_ignored.
test_newer_partial_k6_run_is_ignored() {
  setup_test_workspace write_arg_dump_compiler
  local appended_output

  write_fixture_results "$TEST_RESULTS_DIR" "yes"
  mkdir -p "$TEST_RESULTS_DIR/20260322T190000Z"
  echo "{}" > "$TEST_RESULTS_DIR/20260322T190000Z/smoke.json"
  echo "new smoke stdout" > "$TEST_RESULTS_DIR/20260322T190000Z/smoke.stdout.txt"

  assert_exit_zero \
    "compile_baseline.sh ignores newer partial k6 directories" \
    run_compile_script

  appended_output="$(benchmarks_output)"
  if [[ "$appended_output" == *"smoke=$TEST_RESULTS_DIR/20260322T180000Z/smoke.json"* ]] && \
     [[ "$appended_output" != *"smoke=$TEST_RESULTS_DIR/20260322T190000Z/smoke.json"* ]] && \
     [[ "$appended_output" == *"memory-pressure=$TEST_RESULTS_DIR/20260322T180000Z/memory-pressure.stdout.txt"* ]]; then
    echo "PASS: newest partial k6 directory was ignored in favor of latest complete run"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: compile_baseline.sh mixed partial k6 results into the baseline"
    echo "  output: $appended_output"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi

  teardown_test_workspace
}

# TODO: Document test_split_timestamp_artifacts_are_discovered.
test_split_timestamp_artifacts_are_discovered() {
  setup_test_workspace write_arg_dump_compiler
  local appended_output

  mkdir -p "$TEST_RESULTS_DIR/20260323T155255Z" "$TEST_RESULTS_DIR/20260323T155318Z" "$TEST_RESULTS_DIR/20260323T164412Z"
  echo '{"timestamp":"2026-03-23T15:52:55Z","indexName":"benchmark_100k","totalDocs":100000,"batchCount":100,"wallClockMs":54429}' > "$TEST_RESULTS_DIR/20260323T155255Z/import_benchmark.json"
  echo '{"timestamp":"2026-03-23T15:53:18Z","indexName":"benchmark_100k","docCount":100000,"wallClockMs":1180}' > "$TEST_RESULTS_DIR/20260323T155318Z/search_benchmark.json"

  local scenario
  for scenario in "${SCENARIOS[@]}"; do
    echo "k6 stdout output for $scenario" > "$TEST_RESULTS_DIR/20260323T164412Z/${scenario}.stdout.txt"
    echo "{}" > "$TEST_RESULTS_DIR/20260323T164412Z/${scenario}.json"
  done

  assert_exit_zero \
    "compile_baseline.sh discovers import/search/k6 artifacts across separate timestamps" \
    run_compile_script

  appended_output="$(benchmarks_output)"
  if [[ "$appended_output" == *"--import-artifact"*"$TEST_RESULTS_DIR/20260323T155255Z/import_benchmark.json"* ]] && \
     [[ "$appended_output" == *"--search-artifact"*"$TEST_RESULTS_DIR/20260323T155318Z/search_benchmark.json"* ]] && \
     [[ "$appended_output" == *"memory-pressure=$TEST_RESULTS_DIR/20260323T164412Z/memory-pressure.stdout.txt"* ]]; then
    echo "PASS: split timestamp artifacts were forwarded from expected directories"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: split timestamp artifacts were not forwarded from expected directories"
    echo "  output: $appended_output"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi

  teardown_test_workspace
}

# TODO: Document test_newest_timestamp_wins_over_newer_mtime.
test_newest_timestamp_wins_over_newer_mtime() {
  setup_test_workspace write_arg_dump_compiler
  local appended_output

  mkdir -p "$TEST_RESULTS_DIR/20260323T155255Z" "$TEST_RESULTS_DIR/20260324T010000Z" "$TEST_RESULTS_DIR/20260324T164412Z"
  echo '{"timestamp":"2026-03-24T01:00:00Z","indexName":"benchmark_100k","totalDocs":100000,"batchCount":100,"wallClockMs":54429}' > "$TEST_RESULTS_DIR/20260324T010000Z/import_benchmark.json"
  echo '{"timestamp":"2026-03-24T01:00:30Z","indexName":"benchmark_100k","docCount":100000,"wallClockMs":1180}' > "$TEST_RESULTS_DIR/20260324T010000Z/search_benchmark.json"
  sleep 1
  echo '{"timestamp":"2026-03-23T15:52:55Z","indexName":"benchmark_100k","totalDocs":100000,"batchCount":100,"wallClockMs":54429}' > "$TEST_RESULTS_DIR/20260323T155255Z/import_benchmark.json"

  local scenario
  for scenario in "${SCENARIOS[@]}"; do
    echo "k6 stdout output for $scenario" > "$TEST_RESULTS_DIR/20260324T164412Z/${scenario}.stdout.txt"
    echo "{}" > "$TEST_RESULTS_DIR/20260324T164412Z/${scenario}.json"
  done

  assert_exit_zero \
    "compile_baseline.sh prefers newest timestamped import artifact over newer mtime" \
    run_compile_script

  appended_output="$(benchmarks_output)"
  if [[ "$appended_output" == *"--import-artifact"*"$TEST_RESULTS_DIR/20260324T010000Z/import_benchmark.json"* ]] && \
     [[ "$appended_output" != *"--import-artifact"*"$TEST_RESULTS_DIR/20260323T155255Z/import_benchmark.json"* ]]; then
    echo "PASS: newest timestamped import artifact was selected despite older file mtime"
    TEST_PASS=$((TEST_PASS + 1))
  else
    echo "FAIL: import artifact selection depended on filesystem mtime instead of timestamped directory"
    echo "  output: $appended_output"
    TEST_FAIL=$((TEST_FAIL + 1))
  fi

  teardown_test_workspace
}

# TODO: Document test_metadata_overrides_are_forwarded.
test_metadata_overrides_are_forwarded() {
  setup_test_workspace write_arg_dump_compiler
  write_fixture_results "$TEST_RESULTS_DIR" "yes"

  assert_exit_zero \
    "compile_baseline.sh forwards explicit build mode and named commands" \
    run_compile_script \
        BASELINE_BUILD_MODE="debug" \
        BASELINE_IMPORT_COMMAND="bash engine/loadtest/import_benchmark.sh --fixture" \
        BASELINE_SEARCH_COMMAND="bash engine/loadtest/search_benchmark.sh --fixture" \
        BASELINE_K6_COMMAND="bash engine/loadtest/run.sh --fixture"

  assert_output_contains \
    "override build mode is preserved" \
    "--build-mode" \
    benchmarks_output
  assert_output_contains \
    "override build mode value is forwarded" \
    "debug" \
    benchmarks_output
  assert_output_contains \
    "override import command flag is forwarded" \
    "--import-command" \
    benchmarks_output
  assert_output_contains \
    "override import command is forwarded" \
    "bash engine/loadtest/import_benchmark.sh --fixture" \
    benchmarks_output
  assert_output_contains \
    "override search command flag is forwarded" \
    "--search-command" \
    benchmarks_output
  assert_output_contains \
    "override k6 command flag is forwarded" \
    "--k6-command" \
    benchmarks_output
  assert_output_contains \
    "override k6 command is forwarded" \
    "bash engine/loadtest/run.sh --fixture" \
    benchmarks_output

  teardown_test_workspace
}

echo "=== compile_baseline.sh artifact discovery tests ==="
test_stdout_only_fallback_succeeds
test_missing_both_artifacts_fails
test_newer_partial_k6_run_is_ignored
test_split_timestamp_artifacts_are_discovered
test_newest_timestamp_wins_over_newer_mtime
test_metadata_overrides_are_forwarded

echo ""
echo "Results: $TEST_PASS passed, $TEST_FAIL failed"
if [[ $TEST_FAIL -gt 0 ]]; then
  exit 1
fi
echo "All artifact discovery tests passed."
