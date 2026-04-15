#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENGINE_DIR="$(cd "$SDK_TEST_DIR/.." && pwd)"
RUNNER="$ENGINE_DIR/s/test"

TMP_DIR="$(mktemp -d)"
BIN_DIR="$TMP_DIR/bin"
OUTPUT_FILE="$TMP_DIR/output.log"
CURL_LOG="$TMP_DIR/curl.log"
NODE_LOG="$TMP_DIR/node.log"
SDK_NODE_MODULES="$ENGINE_DIR/sdk_test/node_modules"
SDK_NODE_MODULES_CREATED=false

cleanup() {
  if [ "$SDK_NODE_MODULES_CREATED" = "true" ]; then
    rm -rf "$SDK_NODE_MODULES"
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$BIN_DIR"

if [ ! -e "$SDK_NODE_MODULES" ]; then
  mkdir -p "$SDK_NODE_MODULES"
  SDK_NODE_MODULES_CREATED=true
fi

cat > "$BIN_DIR/curl" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$STUB_CURL_LOG"
if [[ "$*" == *"/health"* ]]; then
  exit 0
fi
if [[ "$*" == *"/1/keys"* ]]; then
  exit 22
fi
exit 1
WRAP
chmod +x "$BIN_DIR/curl"

cat > "$BIN_DIR/node" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$STUB_NODE_LOG"
exit 99
WRAP
chmod +x "$BIN_DIR/node"

set +e
PATH="$BIN_DIR:$PATH" \
STUB_CURL_LOG="$CURL_LOG" \
STUB_NODE_LOG="$NODE_LOG" \
"$RUNNER" --sdk >"$OUTPUT_FILE" 2>&1
status=$?
set -e

if [ "$status" -eq 0 ]; then
  echo "Expected s/test --sdk to fail when reused server rejects the configured admin key"
  exit 1
fi

if ! grep -q "rejected FLAPJACK_ADMIN_KEY" "$OUTPUT_FILE"; then
  echo "Expected compatibility failure message in runner output"
  cat "$OUTPUT_FILE"
  exit 1
fi

if ! grep -q "/1/keys" "$CURL_LOG"; then
  echo "Expected runner to verify key compatibility via /1/keys"
  cat "$CURL_LOG"
  exit 1
fi

if [ -s "$NODE_LOG" ]; then
  echo "Expected runner to fail before invoking node-based SDK tests"
  cat "$NODE_LOG"
  exit 1
fi

echo "PASS: s/test refuses to reuse a healthy server with an incompatible admin key"
