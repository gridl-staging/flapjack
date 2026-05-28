#!/bin/bash
# test_stage5_algolia_compat.sh — Stage 5 focused Algolia JS compatibility harness.
#
# Boots a fresh local Flapjack server, runs the dedicated six-row JS matrix under
# engine/sdk_test, and relays the JS PASS/FAIL rows as the stage verdict surface.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/algolia_compat_bootstrap.sh"

run_algolia_compat_harness \
  "Flapjack Stage 5 Algolia Compatibility" \
  "127.0.0.1:17881" \
  "fj_stage5_algolia_compat" \
  "stage5_server.log" \
  "test:stage5" \
  "false"
