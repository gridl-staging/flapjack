#!/usr/bin/env bash
set -euo pipefail

set +e
output="$(bash engine/tests/test_algolia_compat_broader_first_error.sh 2>&1)"
status=$?
set -e

printf "%s\n" "$output"

if [ "$status" -ne 0 ]; then
  echo "Expected delegated intentional red check to pass but it failed"
  exit 1
fi

printf "%s\n" "$output" | rg -n "PASS: verdict recorder preserves first failing error" >/dev/null

echo "PASS: intentional broader-harness first-error red path remains wired"
