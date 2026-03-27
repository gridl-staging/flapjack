#!/bin/bash
# Start the stable flapjack binary for dashboard development.
# This binary is decoupled from ongoing server code changes.
# Uses port 7700 by default (override with FLAPJACK_BIND_ADDR).
#
# Usage: ./scripts/start-stable-server.sh
# Rebuild (text search only):
#   cargo build -p flapjack-server --release && mkdir -p bin && cp ../target/release/flapjack bin/flapjack-stable
# Rebuild (with vector search):
#   cargo build -p flapjack-server --release --features vector-search && mkdir -p bin && cp ../target/release/flapjack bin/flapjack-stable

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN="$SCRIPT_DIR/../bin/flapjack-stable"

if [ ! -f "$BIN" ]; then
  echo "Stable binary not found at $BIN"
  echo "Build it with: cargo build -p flapjack-server --release && mkdir -p bin && cp ../target/release/flapjack bin/flapjack-stable"
  echo "For vector search: cargo build -p flapjack-server --release --features vector-search && mkdir -p bin && cp ../target/release/flapjack bin/flapjack-stable"
  exit 1
fi

export FLAPJACK_DATA_DIR="${FLAPJACK_DATA_DIR:-/tmp/flapjack-dashboard-clean}"
export FLAPJACK_BIND_ADDR="${FLAPJACK_BIND_ADDR:-127.0.0.1:7700}"

umask 077
mkdir -p "$FLAPJACK_DATA_DIR"
chmod 700 "$FLAPJACK_DATA_DIR"

if [ -z "${FLAPJACK_ADMIN_KEY:-}" ]; then
  if command -v openssl >/dev/null 2>&1; then
    export FLAPJACK_ADMIN_KEY="fj_dev_$(openssl rand -hex 16)"
  else
    export FLAPJACK_ADMIN_KEY="fj_dev_${RANDOM}${RANDOM}_$(date +%s)"
  fi
  GENERATED_ADMIN_KEY=1
else
  GENERATED_ADMIN_KEY=0
fi

ADMIN_KEY_FILE="$FLAPJACK_DATA_DIR/.admin-key"
if [ "$GENERATED_ADMIN_KEY" -eq 1 ]; then
  printf '%s\n' "$FLAPJACK_ADMIN_KEY" > "$ADMIN_KEY_FILE"
  chmod 600 "$ADMIN_KEY_FILE"
fi

echo "Starting stable flapjack on $FLAPJACK_BIND_ADDR"
if [ "$GENERATED_ADMIN_KEY" -eq 1 ]; then
  echo "  Admin key file: $ADMIN_KEY_FILE"
  echo "  (Generated ephemeral admin key for this run)"
else
  echo "  Admin key: [hidden; provided via FLAPJACK_ADMIN_KEY]"
fi
echo "  Data dir:  $FLAPJACK_DATA_DIR"
echo ""

exec "$BIN"
