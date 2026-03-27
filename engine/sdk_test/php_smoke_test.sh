#!/bin/bash
# PHP SDK Protocol Smoke Test
# Tests wire protocol compatibility using curl with PHP-style headers/paths.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/smoke_http.sh"

FLAPJACK_URL="${FLAPJACK_URL:-http://localhost:8080}"
FLAPJACK_ADMIN_KEY="${FLAPJACK_ADMIN_KEY:-admin-key}"
APP_ID="php-smoke-app"
INDEX_NAME="php_smoke_test_$(date +%s)_${$}_${RANDOM}"
smoke_http_setup
trap smoke_http_cleanup EXIT

run_standard_sdk_smoke_test "PHP" "$INDEX_NAME" "/1/task/%s"
