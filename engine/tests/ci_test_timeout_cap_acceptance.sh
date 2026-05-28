#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CI_WORKFLOW="$REPO_DIR/.github/workflows/ci.yml"
NIGHTLY_WORKFLOW="$REPO_DIR/.github/workflows/nightly.yml"

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() {
  TESTS_RUN=$((TESTS_RUN + 1))
  TESTS_PASSED=$((TESTS_PASSED + 1))
  printf '  [PASS] %s\n' "$1"
}

fail() {
  TESTS_RUN=$((TESTS_RUN + 1))
  TESTS_FAILED=$((TESTS_FAILED + 1))
  printf '  [FAIL] %s\n' "$1"
}

section() {
  printf '\n\033[1m%s\033[0m\n' "$1"
}

assert_contains() {
  local file_path="$1"
  local pattern="$2"
  local description="$3"
  if grep -Eq "$pattern" "$file_path"; then
    pass "$description"
  else
    fail "$description"
  fi
}

extract_matching_step_block() {
  local file_path="$1"
  local step_name="$2"
  local command_signature="$3"
  awk -v step="$step_name" -v command="$command_signature" '
    function flush_candidate(   normalized) {
      if (have_candidate != 1) {
        return
      }
      normalized = candidate_run
      gsub(/[[:space:]]+/, " ", normalized)
      sub(/^ /, "", normalized)
      sub(/ $/, "", normalized)
      if (candidate_name == step && normalized == command) {
        if (match_count == 0) {
          printf "%s", candidate_block
        }
        match_count++
      }
      have_candidate = 0
      candidate_name = ""
      candidate_block = ""
      candidate_run = ""
      in_multiline_run = 0
      multiline_indent = -1
    }

    {
      line = $0
      line_indent = match(line, /[^ ]/) - 1
      if (line ~ /^[[:space:]]*$/) {
        line_indent = -1
      }

      if (have_candidate == 1 && in_multiline_run == 1) {
        if (line ~ /^[[:space:]]*$/) {
          # Ignore blank lines inside multiline run values.
        } else if (line_indent <= multiline_indent && line ~ /^[[:space:]]*[^#-]/) {
          in_multiline_run = 0
        } else if (line_indent > multiline_indent) {
          run_line = line
          sub(/^[[:space:]]+/, "", run_line)
          if (run_line ~ /^#/) {
            next
          }
          if (candidate_run != "") {
            candidate_run = candidate_run " " run_line
          } else {
            candidate_run = run_line
          }
          next
        }
      }

      if (line ~ /^[[:space:]]*-[[:space:]]*name:[[:space:]]*/) {
        flush_candidate()
        have_candidate = 1
        candidate_block = line "\n"
        candidate_name = line
        sub(/^[[:space:]]*-[[:space:]]*name:[[:space:]]*/, "", candidate_name)
        next
      }

      if (have_candidate == 1) {
        if (line ~ /^[[:space:]]*-[[:space:]]*[^ ]/) {
          flush_candidate()
        }
      }

      if (have_candidate == 1) {
        candidate_block = candidate_block line "\n"

        if (line ~ /^[[:space:]]*run:[[:space:]]*[^>|].*$/) {
          run_line = line
          sub(/^[[:space:]]*run:[[:space:]]*/, "", run_line)
          candidate_run = run_line
          next
        }

        if (line ~ /^[[:space:]]*run:[[:space:]]*[>|][[:space:]]*$/) {
          in_multiline_run = 1
          multiline_indent = line_indent
          next
        }
      }
    }

    END {
      flush_candidate()
      printf "\n__MATCH_COUNT__=%d\n", match_count
    }
  ' "$file_path"
}

assert_step_contract() {
  local workflow_file="$1"
  local workflow_label="$2"
  local step_name="$3"
  local command_signature="$4"

  local block_file
  block_file="$(mktemp)"
  extract_matching_step_block "$workflow_file" "$step_name" "$command_signature" > "$block_file"

  local match_count
  match_count="$(grep -Eo '__MATCH_COUNT__=[0-9]+' "$block_file" | head -n1 | cut -d= -f2)"
  match_count="${match_count:-0}"

  local sanitized_block
  sanitized_block="$(mktemp)"
  grep -v '^__MATCH_COUNT__=' "$block_file" > "$sanitized_block"
  local normalized_run_command
  normalized_run_command="$(awk '
    function flush_multiline() {
      if (in_multiline != 1) {
        return
      }
      gsub(/[[:space:]]+/, " ", run_accum)
      sub(/^ /, "", run_accum)
      sub(/ $/, "", run_accum)
      print run_accum
      in_multiline = 0
      run_indent = -1
      run_accum = ""
    }

    {
      line = $0
      line_indent = match(line, /[^ ]/) - 1
      if (line ~ /^[[:space:]]*$/) {
        line_indent = -1
      }

      if (in_multiline == 1) {
        if (line ~ /^[[:space:]]*$/) {
          next
        }
        if (line_indent > run_indent) {
          run_line = line
          sub(/^[[:space:]]+/, "", run_line)
          if (run_line ~ /^#/) {
            next
          }
          if (run_accum != "") {
            run_accum = run_accum " " run_line
          } else {
            run_accum = run_line
          }
          next
        }
        flush_multiline()
      }

      if (line ~ /^[[:space:]]*run:[[:space:]]*[^>|].*$/) {
        run_line = line
        sub(/^[[:space:]]*run:[[:space:]]*/, "", run_line)
        gsub(/[[:space:]]+/, " ", run_line)
        sub(/^ /, "", run_line)
        sub(/ $/, "", run_line)
        print run_line
        next
      }

      if (line ~ /^[[:space:]]*run:[[:space:]]*[>|][[:space:]]*$/) {
        in_multiline = 1
        run_indent = line_indent
        run_accum = ""
        next
      }
    }

    END {
      flush_multiline()
    }
  ' "$sanitized_block" | head -n1)"

  if [ "$match_count" -eq 1 ]; then
    pass "$workflow_label resolves '$step_name' to exactly one step using canonical command"
  else
    fail "$workflow_label resolves '$step_name' to exactly one step using canonical command"
  fi

  if [ "$match_count" -eq 1 ] && [ -n "${normalized_run_command//[[:space:]]/}" ]; then
    pass "$workflow_label defines target step '$step_name' with canonical nextest command"
  else
    fail "$workflow_label defines target step '$step_name' with canonical nextest command"
    rm -f "$block_file" "$sanitized_block"
    return
  fi

  assert_contains "$sanitized_block" '^[[:space:]]*run:[[:space:]]*([^>|].*|[>|][[:space:]]*)$' "$workflow_label '$step_name' still defines a run command"
  if printf '%s\n' "$normalized_run_command" | grep -Eq '(^|[[:space:]])cargo[[:space:]]+nextest[[:space:]]+run($|[[:space:]])'; then
    pass "$workflow_label '$step_name' run command still invokes cargo nextest run"
  else
    fail "$workflow_label '$step_name' run command still invokes cargo nextest run"
  fi
  assert_contains "$sanitized_block" '^[[:space:]]*timeout-minutes:[[:space:]]*10$' "$workflow_label '$step_name' enforces timeout-minutes: 10"

  rm -f "$block_file" "$sanitized_block"
}

section "Rust test timeout acceptance contract"
assert_step_contract "$CI_WORKFLOW" "ci.yml" "Fast tests (vector-search)" "cargo nextest run -p flapjack -p flapjack-http --features vector-search"
assert_step_contract "$CI_WORKFLOW" "ci.yml" "Fast tests (remaining)" "cargo nextest run -p flapjack-server -p flapjack-ssl -p flapjack-replication"
assert_step_contract "$CI_WORKFLOW" "ci.yml" "All tests (vector-search)" "cargo nextest run -p flapjack -p flapjack-http --features vector-search -P ci"
assert_step_contract "$CI_WORKFLOW" "ci.yml" "All tests (remaining)" "cargo nextest run -p flapjack-server -p flapjack-ssl -p flapjack-replication -P ci"
assert_step_contract "$NIGHTLY_WORKFLOW" "nightly.yml" "Run all tests" "cargo nextest run -P ci"

printf '\n\033[1mResults: %d/%d passed\033[0m\n' "$TESTS_PASSED" "$TESTS_RUN"
if [ "$TESTS_FAILED" -gt 0 ]; then
  printf '\033[0;31m%d test(s) failed\033[0m\n' "$TESTS_FAILED"
  exit 1
fi
printf '\033[0;32mAll tests passed\033[0m\n'
