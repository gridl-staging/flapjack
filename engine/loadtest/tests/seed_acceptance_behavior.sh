#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEED_ACCEPTANCE_SCRIPT="$ROOT_DIR/tests/seed_acceptance.sh"

if [[ ! -x "$SEED_ACCEPTANCE_SCRIPT" ]]; then
  echo "FAIL: missing executable $SEED_ACCEPTANCE_SCRIPT"
  exit 1
fi

# The acceptance script must perform live HTTP checks. Pointing at an unreachable
# server should fail quickly; if it passes, the check is static-only.
if FLAPJACK_LOADTEST_BASE_URL="http://127.0.0.1:9" \
  "$SEED_ACCEPTANCE_SCRIPT" >/tmp/loadtest_seed_acceptance_behavior.out 2>&1; then
  echo "FAIL: seed acceptance passed with unreachable FLAPJACK_LOADTEST_BASE_URL"
  cat /tmp/loadtest_seed_acceptance_behavior.out
  exit 1
fi

echo "PASS: seed acceptance exercises runtime behavior"
