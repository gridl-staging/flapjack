#!/bin/bash
# test_algolia_compat_broader.sh — Stage 3 manifest-driven Algolia broader harness.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/algolia_compat_bootstrap.sh"

run_algolia_compat_harness \
  "Flapjack Stage 3 Broader Algolia Compatibility" \
  "127.0.0.1:17882" \
  "fj_stage3_algolia_broader" \
  "stage3_broader_server.log" \
  "test:algolia_compat_broader" \
  "true"
