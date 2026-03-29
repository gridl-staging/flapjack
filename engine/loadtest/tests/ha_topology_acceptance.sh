#!/usr/bin/env bash
set -euo pipefail

LOADTEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_DIR="$(cd "$LOADTEST_DIR/.." && pwd)"
HA_TEST_SCRIPT="$ENGINE_DIR/examples/ha-cluster/test_ha.sh"

fail() {
  echo "FAIL: $1"
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing $path"
}

require_text() {
  local pattern="$1"
  local message="$2"
  grep -Eq "$pattern" "$HA_TEST_SCRIPT" || fail "$message"
}

require_file "$HA_TEST_SCRIPT"
[[ -x "$HA_TEST_SCRIPT" ]] || fail "script must be executable: $HA_TEST_SCRIPT"

require_text 'SCRIPT_DIR="\$\(cd "\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)".*pwd\)' \
  'test_ha.sh must derive SCRIPT_DIR from BASH_SOURCE[0]'
require_text 'COMPOSE_FILE="\$SCRIPT_DIR/docker-compose\.yml"' \
  'test_ha.sh must bind COMPOSE_FILE to script-local docker-compose.yml'
require_text 'docker compose -f "\$COMPOSE_FILE"' \
  'test_ha.sh must use docker compose -f "$COMPOSE_FILE" for cwd-safe execution'
require_text '^wait_for_peer_mesh_ready\(\)' \
  'test_ha.sh must define wait_for_peer_mesh_ready() before replication checks'
require_text 'wait_for_peer_mesh_ready' \
  'test_ha.sh must call wait_for_peer_mesh_ready before replication assertions'
require_text 'assert_eq "node-a sees 2 peers"' \
  'test_ha.sh must assert node-a peer visibility directly'
require_text 'assert_eq "node-b sees 2 peers"' \
  'test_ha.sh must assert node-b peer visibility directly'
require_text 'assert_eq "node-c sees 2 peers"' \
  'test_ha.sh must assert node-c peer visibility directly'

echo "PASS: HA topology acceptance checks"
