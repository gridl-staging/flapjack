#!/bin/bash
# test_stage6_sdk_publish.sh — Stage 6 SDK publish-state and live-connectivity harness.
#
# Boots a fresh local Flapjack server, seeds a test index, and runs three SDK probes
# (npm, PyPI, Go) with deterministic PASS/FAIL rows and a summary matrix.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENGINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

SERVER_PID=""
DATA_DIR=""
BUILD_LOG=""
SERVER_LOG=""
WORK_DIR=""

EXPECTED_NPM_VERSION="${EXPECTED_NPM_VERSION:-${EXPECTED_RELEASE_VERSION_NPM:-0.1.0-beta.1}}"
EXPECTED_PYPI_VERSION="${EXPECTED_PYPI_VERSION:-${EXPECTED_RELEASE_VERSION_PYPI:-1.0.0}}"
EXPECTED_GO_VERSION="${EXPECTED_GO_VERSION:-${EXPECTED_RELEASE_VERSION_GO:-4.0.0}}"
APP_ID="${FLAPJACK_APP_ID:-flapjack}"
INDEX_NAME="sdk_stage6_test"
LOCAL_GO_SDK_PATH="${LOCAL_GO_SDK_PATH:-$ENGINE_DIR/../sdks/go}"

PASS_COUNT=0
FAIL_COUNT=0

SDKS=(npm pypi go)

NPM_REGISTRY_STATUS="PASS"
NPM_IMPORT_STATUS="PASS"
NPM_LIVE_STATUS="PASS"

PYPI_REGISTRY_STATUS="PASS"
PYPI_IMPORT_STATUS="PASS"
PYPI_LIVE_STATUS="PASS"

GO_REGISTRY_STATUS="PASS"
GO_IMPORT_STATUS="PASS"
GO_LIVE_STATUS="PASS"

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "$BUILD_LOG" ] && [ -f "$BUILD_LOG" ]; then
    rm -f "$BUILD_LOG"
  fi
  if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR" ]; then
    rm -rf "$DATA_DIR"
  fi
  if [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

mark_fail() {
  local sdk="$1"
  local column="$2"
  case "${sdk}:${column}" in
    npm:registry) NPM_REGISTRY_STATUS="FAIL" ;;
    npm:import) NPM_IMPORT_STATUS="FAIL" ;;
    npm:live) NPM_LIVE_STATUS="FAIL" ;;
    pypi:registry) PYPI_REGISTRY_STATUS="FAIL" ;;
    pypi:import) PYPI_IMPORT_STATUS="FAIL" ;;
    pypi:live) PYPI_LIVE_STATUS="FAIL" ;;
    go:registry) GO_REGISTRY_STATUS="FAIL" ;;
    go:import) GO_IMPORT_STATUS="FAIL" ;;
    go:live) GO_LIVE_STATUS="FAIL" ;;
    *) ;;
  esac
}

matrix_status() {
  local sdk="$1"
  local column="$2"
  case "${sdk}:${column}" in
    npm:registry) echo "$NPM_REGISTRY_STATUS" ;;
    npm:import) echo "$NPM_IMPORT_STATUS" ;;
    npm:live) echo "$NPM_LIVE_STATUS" ;;
    pypi:registry) echo "$PYPI_REGISTRY_STATUS" ;;
    pypi:import) echo "$PYPI_IMPORT_STATUS" ;;
    pypi:live) echo "$PYPI_LIVE_STATUS" ;;
    go:registry) echo "$GO_REGISTRY_STATUS" ;;
    go:import) echo "$GO_IMPORT_STATUS" ;;
    go:live) echo "$GO_LIVE_STATUS" ;;
    *) echo "FAIL" ;;
  esac
}

record_row() {
  local sdk="$1"
  local check="$2"
  local status="$3"
  local detail="$4"
  local column="${5:-}"

  printf '[%s] %-4s | %-28s | %s\n' "$status" "$sdk" "$check" "$detail"

  if [ "$status" = "PASS" ]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    if [ -n "$column" ]; then
      mark_fail "$sdk" "$column"
    fi
  fi
}

set +e
run_cmd() {
  local out_file="$1"
  shift
  "$@" >"$out_file" 2>&1
  return $?
}
set -e

json_field() {
  local payload="$1"
  local expr="$2"
  PAYLOAD="$payload" EXPR="$expr" python3 - <<'PY'
import json
import os

payload = os.environ.get("PAYLOAD", "")
expr = os.environ.get("EXPR", "")
if not payload:
    raise SystemExit(1)
obj = json.loads(payload)
parts = [p for p in expr.split('.') if p]
cur = obj
for part in parts:
    if isinstance(cur, dict):
        cur = cur.get(part)
    else:
        cur = None
        break
if cur is None:
    print("")
elif isinstance(cur, (dict, list)):
    print(json.dumps(cur))
else:
    print(cur)
PY
}

echo "=== Flapjack Stage 6 SDK Publish Validation ==="
echo "Started: $(timestamp)"
echo "Expected npm version: $EXPECTED_NPM_VERSION"
echo "Expected pypi version: $EXPECTED_PYPI_VERSION"
echo "Expected go version: $EXPECTED_GO_VERSION"

if [ -n "${FLAPJACK_BIN:-}" ]; then
  if [ ! -x "$FLAPJACK_BIN" ]; then
    echo "ERROR: FLAPJACK_BIN=$FLAPJACK_BIN is not executable" >&2
    exit 1
  fi
  BIN="$FLAPJACK_BIN"
  echo "Using pre-built binary: $BIN"
else
  echo "Building flapjack-server release binary..."
  BUILD_LOG="$(mktemp)"
  if (cd "$ENGINE_DIR" && cargo build -p flapjack-server --release >"$BUILD_LOG" 2>&1); then
    tail -5 "$BUILD_LOG"
  else
    tail -40 "$BUILD_LOG" >&2 || true
    echo "ERROR: cargo build -p flapjack-server --release failed" >&2
    exit 1
  fi
  BIN="$ENGINE_DIR/target/release/flapjack"
  if [ ! -x "$BIN" ]; then
    echo "ERROR: build succeeded but binary missing at $BIN" >&2
    exit 1
  fi
fi

BIND_ADDR="${FLAPJACK_BIND_ADDR:-127.0.0.1:17882}"
BASE_URL="http://${BIND_ADDR}"
PORT="${BIND_ADDR##*:}"

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "ERROR: port $PORT already has a listening process" >&2
  exit 1
fi

DATA_DIR="$(mktemp -d)"
WORK_DIR="$(mktemp -d)"
SERVER_LOG="$DATA_DIR/stage6_server.log"
ADMIN_KEY="fj_stage6_sdk_publish_$(date +%s)"
SEED_PUBLISHED="false"

export FLAPJACK_ADMIN_KEY="$ADMIN_KEY"
export FLAPJACK_BIND_ADDR="$BIND_ADDR"
export FLAPJACK_DATA_DIR="$DATA_DIR"

"$BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "Server PID: $SERVER_PID"
echo "Server log: $SERVER_LOG"

HEALTH_OK="false"
for _i in $(seq 1 60); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: server exited before readiness" >&2
    cat "$SERVER_LOG" 2>/dev/null || true
    exit 1
  fi
  if curl -sf "$BASE_URL/health" >/dev/null 2>&1; then
    HEALTH_OK="true"
    break
  fi
  sleep 0.5
done

if [ "$HEALTH_OK" != "true" ]; then
  echo "ERROR: /health was not ready within 30s at $BASE_URL/health" >&2
  cat "$SERVER_LOG" 2>/dev/null || true
  exit 1
fi

echo "Health ready: $BASE_URL/health"

SEED_PAYLOAD='{"requests":[{"action":"addObject","body":{"objectID":"doc1","name":"Stage6 Alpha"}},{"action":"addObject","body":{"objectID":"doc2","name":"Stage6 Beta"}},{"action":"addObject","body":{"objectID":"doc3","name":"Stage6 Gamma"}}]}'

set +e
SEED_RESPONSE=$(curl -sS -f -X POST "$BASE_URL/1/indexes/$INDEX_NAME/batch" \
  -H "X-Algolia-API-Key: $ADMIN_KEY" \
  -H "X-Algolia-Application-Id: $APP_ID" \
  -H "Content-Type: application/json" \
  --data "$SEED_PAYLOAD")
SEED_EXIT=$?
set -e

if [ "$SEED_EXIT" -ne 0 ]; then
  echo "ERROR: failed to seed $INDEX_NAME via /batch" >&2
  exit 1
fi

TASK_ID="$(json_field "$SEED_RESPONSE" "taskID" || true)"
if ! [[ "$TASK_ID" =~ ^[0-9]+$ ]]; then
  echo "ERROR: seed response missing numeric taskID: $SEED_RESPONSE" >&2
  exit 1
fi

echo "Seed taskID: $TASK_ID"

TASK_OK="false"
for _i in $(seq 1 60); do
  set +e
  TASK_RESPONSE=$(curl -sS -f "$BASE_URL/1/indexes/$INDEX_NAME/task/$TASK_ID" \
    -H "X-Algolia-API-Key: $ADMIN_KEY" \
    -H "X-Algolia-Application-Id: $APP_ID")
  TASK_EXIT=$?
  set -e

  if [ "$TASK_EXIT" -eq 0 ]; then
    TASK_STATUS="$(json_field "$TASK_RESPONSE" "status" || true)"
    if [ "$TASK_STATUS" = "published" ]; then
      TASK_OK="true"
      break
    fi
  fi
  sleep 0.5
done

if [ "$TASK_OK" != "true" ]; then
  echo "ERROR: seed task did not reach published status" >&2
  echo "Last task response: ${TASK_RESPONSE:-<none>}" >&2
  exit 1
fi

echo "Seed index ready: $INDEX_NAME"
SEED_PUBLISHED="true"

echo
echo "=== npm probe ==="

NPM_VERSION_OUT="$WORK_DIR/npm_version.out"
if run_cmd "$NPM_VERSION_OUT" npm view flapjack-search version --json; then
  NPM_VERSION_RAW="$(cat "$NPM_VERSION_OUT")"
  NPM_VERSION="$(printf '%s' "$NPM_VERSION_RAW" | python3 -c 'import json,sys; v=json.load(sys.stdin); print(v if isinstance(v,str) else "")' 2>/dev/null || true)"
  if [ -n "$NPM_VERSION" ]; then
    if [ "$NPM_VERSION" = "$EXPECTED_NPM_VERSION" ]; then
      record_row npm "registry-version" PASS "latest=$NPM_VERSION matches expected=$EXPECTED_NPM_VERSION" registry
    else
      record_row npm "registry-version" FAIL "latest=$NPM_VERSION expected=$EXPECTED_NPM_VERSION" registry
    fi
  else
    record_row npm "registry-version" FAIL "could not parse npm version JSON" registry
  fi
else
  record_row npm "registry-version" FAIL "npm view version failed: $(head -n 2 "$NPM_VERSION_OUT" | tr '\n' ' ')" registry
fi

NPM_MAINTAINERS_OUT="$WORK_DIR/npm_maintainers.out"
EXPECTED_NPM_OWNER="${EXPECTED_NPM_OWNER_SUBSTRING:-stuartcrobinsonnpm}"
if run_cmd "$NPM_MAINTAINERS_OUT" npm view flapjack-search maintainers --json; then
  NPM_MAINTAINERS_RAW="$(cat "$NPM_MAINTAINERS_OUT")"
  if printf '%s' "$NPM_MAINTAINERS_RAW" | grep -Eiq "\"name\"[[:space:]]*:[[:space:]]*\"${EXPECTED_NPM_OWNER}\"|${EXPECTED_NPM_OWNER}[[:space:]]*<"; then
    record_row npm "registry-maintainers" PASS "maintainers include '$EXPECTED_NPM_OWNER'" registry
  else
    ONE_LINE="$(printf '%s' "$NPM_MAINTAINERS_RAW" | tr '\n' ' ' | cut -c1-200)"
    record_row npm "registry-maintainers" FAIL "expected maintainer substring '$EXPECTED_NPM_OWNER' not found: $ONE_LINE" registry
  fi
else
  record_row npm "registry-maintainers" FAIL "npm view maintainers failed: $(head -n 2 "$NPM_MAINTAINERS_OUT" | tr '\n' ' ')" registry
fi

NPM_DIR="$WORK_DIR/npm_probe"
mkdir -p "$NPM_DIR"
set +e
(
  cd "$NPM_DIR" && npm init -y >/dev/null 2>&1 && npm install --no-save flapjack-search
) >"$WORK_DIR/npm_install.out" 2>&1
NPM_INSTALL_EXIT=$?
set -e

if [ "$NPM_INSTALL_EXIT" -eq 0 ]; then
  record_row npm "install-package" PASS "npm install --no-save flapjack-search succeeded" import
else
  record_row npm "install-package" FAIL "npm install failed: $(tail -n 3 "$WORK_DIR/npm_install.out" | tr '\n' ' ')" import
fi

if [ "$NPM_INSTALL_EXIT" -eq 0 ]; then
  cat >"$NPM_DIR/probe.js" <<'JS'
const { flapjackSearch } = require('flapjack-search');

(async () => {
  const appId = process.env.FLAPJACK_APP_ID || 'flapjack';
  const apiKey = process.env.FLAPJACK_ADMIN_KEY;
  const baseUrl = process.env.FLAPJACK_BASE_URL;

  if (!apiKey || !baseUrl) {
    throw new Error('Missing FLAPJACK_ADMIN_KEY or FLAPJACK_BASE_URL');
  }

  const target = new URL(baseUrl);
  const requester = {
    async send(request) {
      const url = new URL(request.url);
      url.protocol = target.protocol;
      url.host = target.host;
      const response = await fetch(url.toString(), {
        method: request.method,
        headers: request.headers,
        body: request.data,
      });
      const content = await response.text();
      return { status: response.status, content, isTimedOut: false };
    },
  };

  const client = flapjackSearch(appId, apiKey, { requester });
  if (!client || !client.transporter || !client.transporter.requester) {
    throw new Error('Client construction failed');
  }

  const health = await client.transporter.requester.send({
    url: `${baseUrl}/health`,
    method: 'GET',
    headers: {},
    data: undefined,
    connectTimeout: 2000,
    responseTimeout: 2000,
  });

  if (health.status !== 200) {
    throw new Error(`Health status ${health.status}`);
  }

  console.log('OK npm client health status=200');
})();
JS

  set +e
  (
    cd "$NPM_DIR" && FLAPJACK_APP_ID="$APP_ID" FLAPJACK_ADMIN_KEY="$ADMIN_KEY" FLAPJACK_BASE_URL="$BASE_URL" node probe.js
  ) >"$WORK_DIR/npm_probe.out" 2>&1
  NPM_PROBE_EXIT=$?
  set -e

  if [ "$NPM_PROBE_EXIT" -eq 0 ]; then
    record_row npm "export-and-live-health" PASS "flapjackSearch require + constructed requester health GET=200" live
  else
    record_row npm "export-and-live-health" FAIL "node probe failed: $(tail -n 3 "$WORK_DIR/npm_probe.out" | tr '\n' ' ')" import
    mark_fail npm live
  fi
else
  record_row npm "export-and-live-health" FAIL "skipped after npm install failure" import
  mark_fail npm live
fi

echo
echo "=== PyPI probe ==="

PIP_INDEX_OUT="$WORK_DIR/pypi_versions.out"
PYPI_VERSIONS=""
set +e
pip index versions --pre flapjack-search >"$PIP_INDEX_OUT" 2>&1
PIP_INDEX_EXIT=$?
set -e

if [ "$PIP_INDEX_EXIT" -eq 0 ]; then
  PYPI_VERSIONS="$(cat "$PIP_INDEX_OUT")"
  if printf '%s' "$PYPI_VERSIONS" | grep -Eq "(^|[^0-9])${EXPECTED_PYPI_VERSION//./\.}([^0-9]|$)"; then
    record_row pypi "registry-version" PASS "pip index includes expected version $EXPECTED_PYPI_VERSION" registry
  else
    ONE_LINE="$(printf '%s' "$PYPI_VERSIONS" | tr '\n' ' ' | cut -c1-200)"
    record_row pypi "registry-version" FAIL "expected $EXPECTED_PYPI_VERSION not in pip index output: $ONE_LINE" registry
  fi
else
  set +e
  curl -sf https://pypi.org/pypi/flapjack-search/json >"$WORK_DIR/pypi_releases.json"
  PYPI_JSON_EXIT=$?
  set -e

  if [ "$PYPI_JSON_EXIT" -eq 0 ]; then
    PYPI_RELEASES="$(python3 - "$WORK_DIR/pypi_releases.json" <<'PY'
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    data = json.load(fh)
releases = sorted(data.get('releases', {}).keys())
print(' '.join(releases))
PY
)"
    if printf '%s' "$PYPI_RELEASES" | grep -Eq "(^| )${EXPECTED_PYPI_VERSION//./\.}( |$)"; then
      record_row pypi "registry-version" PASS "PyPI JSON releases include $EXPECTED_PYPI_VERSION" registry
    else
      record_row pypi "registry-version" FAIL "expected $EXPECTED_PYPI_VERSION not in PyPI releases: $PYPI_RELEASES" registry
    fi
  else
    record_row pypi "registry-version" FAIL "pip index unavailable and PyPI JSON fetch failed" registry
  fi
fi

PY_VENV_DIR="$WORK_DIR/pypi_venv"
set +e
python3 -m venv "$PY_VENV_DIR" >"$WORK_DIR/pypi_venv_create.out" 2>&1
VENV_EXIT=$?
set -e

if [ "$VENV_EXIT" -ne 0 ]; then
  record_row pypi "install-and-import" FAIL "python3 -m venv failed" import
  record_row pypi "live-search" FAIL "skipped after venv creation failure" live
else
  set +e
  "$PY_VENV_DIR/bin/pip" install --no-cache-dir flapjack-search >"$WORK_DIR/pypi_install.out" 2>&1
  PYPI_INSTALL_EXIT=$?
  set -e

  if [ "$PYPI_INSTALL_EXIT" -eq 0 ]; then
    set +e
    "$PY_VENV_DIR/bin/python" -c "from flapjacksearch.search.client import SearchClientSync; print(SearchClientSync)" >"$WORK_DIR/pypi_import.out" 2>&1
    PYPI_IMPORT_EXIT=$?
    set -e

    if [ "$PYPI_IMPORT_EXIT" -eq 0 ]; then
      record_row pypi "install-and-import" PASS "pip install succeeded and SearchClientSync import resolved" import

      cat >"$WORK_DIR/pypi_probe.py" <<'PY'
import os
import sys

from flapjacksearch.http.hosts import Host, HostsCollection
from flapjacksearch.search.client import SearchClientSync
from flapjacksearch.search.config import SearchConfig

app_id = os.environ["FLAPJACK_APP_ID"]
api_key = os.environ["FLAPJACK_ADMIN_KEY"]
port = int(os.environ["FLAPJACK_PORT"])
index_name = os.environ["FLAPJACK_INDEX_NAME"]

config = SearchConfig(app_id=app_id, api_key=api_key)
config.hosts = HostsCollection([Host(url="localhost", scheme="http", port=port)])

client = SearchClientSync(config=config)
try:
    resp = client.search_single_index(index_name=index_name, search_params={"query": "Stage6"})
finally:
    client.close()

nb_hits = resp.nb_hits if resp.nb_hits is not None else -1
if nb_hits < 1:
    print(f"Invalid nb_hits={nb_hits}", file=sys.stderr)
    sys.exit(1)

print(f"OK pypi search nb_hits={nb_hits}")
PY

      set +e
      FLAPJACK_APP_ID="$APP_ID" FLAPJACK_ADMIN_KEY="$ADMIN_KEY" FLAPJACK_PORT="$PORT" FLAPJACK_INDEX_NAME="$INDEX_NAME" \
        "$PY_VENV_DIR/bin/python" "$WORK_DIR/pypi_probe.py" >"$WORK_DIR/pypi_probe.out" 2>&1
      PYPI_PROBE_EXIT=$?
      set -e

      if [ "$PYPI_PROBE_EXIT" -eq 0 ]; then
        record_row pypi "live-search" PASS "SearchClientSync search_single_index returned nbHits >= 0" live
      else
        record_row pypi "live-search" FAIL "python live search failed: $(tail -n 3 "$WORK_DIR/pypi_probe.out" | tr '\n' ' ')" live
      fi
    else
      record_row pypi "install-and-import" FAIL "import failed: $(tail -n 3 "$WORK_DIR/pypi_import.out" | tr '\n' ' ')" import
      record_row pypi "live-search" FAIL "skipped after import failure" live
    fi
  else
    record_row pypi "install-and-import" FAIL "pip install failed: $(tail -n 3 "$WORK_DIR/pypi_install.out" | tr '\n' ' ')" import
    record_row pypi "live-search" FAIL "skipped after install failure" live
  fi
fi

echo
echo "=== Go probe ==="

echo "Stage2 owner reuse: using existing server/bootstrap state from this harness and admin-key export pattern from integration_smoke.sh"

if [ "$SEED_PUBLISHED" != "true" ]; then
  record_row go "seed-prerequisite" FAIL "seed task was not published before Stage2 probe" live
  echo "VERDICT: FAIL"
  exit 1
fi

GO_LIST_OUT="$WORK_DIR/go_list.out"
set +e
GOPROXY="https://proxy.golang.org,direct" go list -m -versions github.com/flapjackhq/flapjack-search-go/v4 >"$GO_LIST_OUT" 2>&1
GO_LIST_EXIT=$?
set -e

if [ "$GO_LIST_EXIT" -eq 0 ]; then
  GO_LIST_CONTENT="$(cat "$GO_LIST_OUT")"
  GO_VERSIONS_COUNT="$(echo "$GO_LIST_CONTENT" | awk '{print NF-1}')"
  if [ "${GO_VERSIONS_COUNT:-0}" -gt 0 ]; then
    if echo "$GO_LIST_CONTENT" | grep -Eq "(^| )v${EXPECTED_GO_VERSION//./\.}( |$)"; then
      record_row go "registry-version" PASS "go list returned versions including v$EXPECTED_GO_VERSION" registry
    else
      record_row go "registry-version" FAIL "go list returned versions but missing v$EXPECTED_GO_VERSION: $GO_LIST_CONTENT" registry
    fi
  else
    record_row go "registry-version" FAIL "go list resolved module path but no versions published: $GO_LIST_CONTENT" registry
  fi
else
  record_row go "registry-version" FAIL "go list failed: $(tail -n 3 "$GO_LIST_OUT" | tr '\n' ' ')" registry
fi

GO_DIR="$WORK_DIR/go_probe"
GO_PUBLISHED_DIR="$WORK_DIR/go_published_probe"
mkdir -p "$GO_DIR"
mkdir -p "$GO_PUBLISHED_DIR"

write_go_list_indices_probe() {
  local probe_path="$1"
  local check_default_constructor="$2"

  cat >"$probe_path" <<'GO'
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/flapjackhq/flapjack-search-go/v4/flapjack/call"
	"github.com/flapjackhq/flapjack-search-go/v4/flapjack/search"
	"github.com/flapjackhq/flapjack-search-go/v4/flapjack/transport"
)

func main() {
	appID := os.Getenv("FLAPJACK_APP_ID")
	apiKey := os.Getenv("FLAPJACK_ADMIN_KEY")
	host := os.Getenv("FLAPJACK_HOST")
	indexName := os.Getenv("FLAPJACK_INDEX_NAME")

	if appID == "" || apiKey == "" || host == "" || indexName == "" {
		panic("missing env vars")
	}
GO

  if [ "$check_default_constructor" = "true" ]; then
    cat >>"$probe_path" <<'GO'

	defaultClient, err := search.NewClient(appID, apiKey)
	if err != nil {
		panic(err)
	}
	if defaultClient == nil {
		panic("default constructor returned nil client")
	}
GO
  fi

  cat >>"$probe_path" <<'GO'

	cfg := search.SearchConfiguration{
		Configuration: transport.Configuration{
			AppID:  appID,
			ApiKey: apiKey,
			Hosts: []transport.StatefulHost{
				transport.NewStatefulHost("http", host, call.IsReadWrite),
			},
			DefaultHeader:  map[string]string{},
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   15 * time.Second,
			ConnectTimeout: 2 * time.Second,
		},
	}

	client, err := search.NewClientWithConfig(cfg)
	if err != nil {
		panic(err)
	}

	request := client.NewApiListIndicesRequest()
	response, err := client.ListIndices(request)
	if err != nil {
		panic(err)
	}

	if response.Items == nil {
		panic("list indices returned nil items")
	}

	found := false
	for _, idx := range response.Items {
		if idx.Name == indexName {
			found = true
			break
		}
	}

	if !found {
		panic("expected index not found")
	}

	fmt.Printf("OK go list-indices found index=%s item_count=%d host=%s\n", indexName, len(response.Items), host)
}
GO
}

write_go_list_indices_probe "$GO_PUBLISHED_DIR/main.go" "true"

set +e
  (
    export GOWORK=off
    cd "$GO_PUBLISHED_DIR" && \
      go mod init stage6publishedprobe >/dev/null 2>&1 && \
      GOPROXY="https://proxy.golang.org,direct" go get github.com/flapjackhq/flapjack-search-go/v4@v"$EXPECTED_GO_VERSION" && \
      go mod tidy
  ) >"$WORK_DIR/go_published_get.out" 2>&1
  GO_PUBLISHED_GET_EXIT=$?
  set -e

if [ "$GO_PUBLISHED_GET_EXIT" -eq 0 ]; then
  set +e
  (
    export GOWORK=off
    cd "$GO_PUBLISHED_DIR" && go list -m -json github.com/flapjackhq/flapjack-search-go/v4
  ) >"$WORK_DIR/go_published_module_json.out" 2>&1
  GO_PUBLISHED_MODULE_JSON_EXIT=$?
  set -e

    GO_PUBLISHED_REQUIRE_OK=1
    GO_PUBLISHED_NO_REPLACE_OK=1
    GO_PUBLISHED_VERSION_JSON_OK=1
    GO_PUBLISHED_REQUIRE_VERSION="$(awk '$1=="require" && $2=="github.com/flapjackhq/flapjack-search-go/v4" {print $3; exit}' "$GO_PUBLISHED_DIR/go.mod")"
    GO_PUBLISHED_RESOLVED_VERSION="$(sed -n 's/.*"Version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$WORK_DIR/go_published_module_json.out" | head -n 1)"

    if [ "$GO_PUBLISHED_REQUIRE_VERSION" != "v$EXPECTED_GO_VERSION" ]; then
      GO_PUBLISHED_REQUIRE_OK=0
    fi

    if grep -Eq '^[[:space:]]*replace[[:space:]]+github\.com/flapjackhq/flapjack-search-go/v4([[:space:]]|$)' "$GO_PUBLISHED_DIR/go.mod"; then
      GO_PUBLISHED_NO_REPLACE_OK=0
    fi

    if [ "$GO_PUBLISHED_MODULE_JSON_EXIT" -ne 0 ] || [ "$GO_PUBLISHED_RESOLVED_VERSION" != "v$EXPECTED_GO_VERSION" ]; then
      GO_PUBLISHED_VERSION_JSON_OK=0
    fi

  set +e
  (
    export GOWORK=off
    cd "$GO_PUBLISHED_DIR" && go build -o stage6_go_published_probe main.go
  ) >"$WORK_DIR/go_published_build.out" 2>&1
  GO_PUBLISHED_BUILD_EXIT=$?
  set -e

  if [ "$GO_PUBLISHED_REQUIRE_OK" -ne 1 ] || [ "$GO_PUBLISHED_NO_REPLACE_OK" -ne 1 ] || [ "$GO_PUBLISHED_VERSION_JSON_OK" -ne 1 ]; then
      record_row go "published-module-get-and-live" FAIL "published module assertions failed (require_v$EXPECTED_GO_VERSION=$GO_PUBLISHED_REQUIRE_OK no_replace=$GO_PUBLISHED_NO_REPLACE_OK json_version_v$EXPECTED_GO_VERSION=$GO_PUBLISHED_VERSION_JSON_OK) go.mod_tail=$(tail -n 6 "$GO_PUBLISHED_DIR/go.mod" | tr '\n' ' ') module_json_tail=$(tail -n 6 "$WORK_DIR/go_published_module_json.out" | tr '\n' ' ')" import
      mark_fail go live
    elif [ "$GO_PUBLISHED_BUILD_EXIT" -ne 0 ]; then
      record_row go "published-module-get-and-live" FAIL "go build after exact-version module assertions failed: $(tail -n 3 "$WORK_DIR/go_published_build.out" | tr '\n' ' ')" import
      mark_fail go live
  else
  set +e
  (
    cd "$GO_PUBLISHED_DIR" && FLAPJACK_APP_ID="$APP_ID" FLAPJACK_ADMIN_KEY="$ADMIN_KEY" FLAPJACK_HOST="127.0.0.1:$PORT" FLAPJACK_INDEX_NAME="$INDEX_NAME" ./stage6_go_published_probe
  ) >"$WORK_DIR/go_published_probe.out" 2>&1
  GO_PUBLISHED_PROBE_EXIT=$?
  set -e

    if [ "$GO_PUBLISHED_PROBE_EXIT" -eq 0 ] && grep -Eq "OK go list-indices found index=$INDEX_NAME" "$WORK_DIR/go_published_probe.out"; then
      PUBLISHED_OK_LINE="$(grep -E "OK go list-indices found index=$INDEX_NAME" "$WORK_DIR/go_published_probe.out" | tail -n 1 | tr '\n' ' ')"
      record_row go "published-module-get-and-live" PASS "GOWORK=off go get @v$EXPECTED_GO_VERSION + go.mod(require/no-replace) + go list -m Version v$EXPECTED_GO_VERSION + live ListIndices succeeded marker='$PUBLISHED_OK_LINE'" import
    elif [ "$GO_PUBLISHED_PROBE_EXIT" -eq 0 ]; then
      record_row go "published-module-get-and-live" FAIL "published-module probe output missing success marker: $(tail -n 2 "$WORK_DIR/go_published_probe.out" | tr '\n' ' ')" import
      mark_fail go live
  else
    record_row go "published-module-get-and-live" FAIL "published-module live probe failed: $(tail -n 3 "$WORK_DIR/go_published_probe.out" | tr '\n' ' ')" import
    mark_fail go live
  fi
  fi
  else
    record_row go "published-module-get-and-live" FAIL "go get @v$EXPECTED_GO_VERSION failed: $(tail -n 3 "$WORK_DIR/go_published_get.out" | tr '\n' ' ')" import
    mark_fail go live
  fi

if [ -z "$LOCAL_GO_SDK_PATH" ]; then
  record_row go "local-module-init" FAIL "LOCAL_GO_SDK_PATH is required for Stage2 local replace probe" import
  record_row go "live-list-indices" FAIL "skipped after missing LOCAL_GO_SDK_PATH" live
  GO_GET_EXIT=1
elif [ ! -d "$LOCAL_GO_SDK_PATH" ] || [ ! -f "$LOCAL_GO_SDK_PATH/go.mod" ]; then
  record_row go "local-module-init" FAIL "local SDK path missing or invalid: $LOCAL_GO_SDK_PATH" import
  record_row go "live-list-indices" FAIL "skipped after local SDK path validation failure" live
  GO_GET_EXIT=1
else
  set +e
  (
    cd "$GO_DIR" && go mod init stage2probe >/dev/null 2>&1 && go mod edit -replace=github.com/flapjackhq/flapjack-search-go/v4="$LOCAL_GO_SDK_PATH"
  ) >"$WORK_DIR/go_get.out" 2>&1
  GO_GET_EXIT=$?
  set -e
fi

if [ "$GO_GET_EXIT" -ne 0 ]; then
  if [ -f "$WORK_DIR/go_get.out" ]; then
    record_row go "local-module-init" FAIL "go mod init/edit failed: $(tail -n 3 "$WORK_DIR/go_get.out" | tr '\n' ' ')" import
    record_row go "live-list-indices" FAIL "skipped after local module init failure" live
  fi
else
  write_go_list_indices_probe "$GO_DIR/main.go" "false"

  set +e
  (
    cd "$GO_DIR" && go mod tidy && go build -o stage2_go_probe main.go
  ) >"$WORK_DIR/go_build.out" 2>&1
  GO_BUILD_EXIT=$?
  set -e

  if [ "$GO_BUILD_EXIT" -eq 0 ]; then
    record_row go "local-module-init" PASS "go mod init/edit + local replace + build succeeded" import

    set +e
    (
      cd "$GO_DIR" && FLAPJACK_APP_ID="$APP_ID" FLAPJACK_ADMIN_KEY="$ADMIN_KEY" FLAPJACK_HOST="127.0.0.1:$PORT" FLAPJACK_INDEX_NAME="$INDEX_NAME" ./stage2_go_probe
    ) >"$WORK_DIR/go_probe.out" 2>&1
    GO_PROBE_EXIT=$?
    set -e

    if [ "$GO_PROBE_EXIT" -eq 0 ]; then
      if grep -Eq "OK go list-indices found index=$INDEX_NAME" "$WORK_DIR/go_probe.out"; then
        record_row go "live-list-indices" PASS "local replace ListIndices succeeded and contained $INDEX_NAME" live
      else
        record_row go "live-list-indices" FAIL "Go probe output missing success marker: $(tail -n 2 "$WORK_DIR/go_probe.out" | tr '\n' ' ')" live
      fi
    else
      record_row go "live-list-indices" FAIL "Go ListIndices probe failed: $(tail -n 3 "$WORK_DIR/go_probe.out" | tr '\n' ' ')" live
    fi
  else
    record_row go "local-module-init" FAIL "go build failed: $(tail -n 3 "$WORK_DIR/go_build.out" | tr '\n' ' ')" import
    record_row go "live-list-indices" FAIL "skipped after go build failure" live
  fi
fi

echo
echo "=== Stage 6 Summary Matrix ==="
printf '%-8s | %-19s | %-13s | %-17s\n' "SDK" "registry-resolution" "import-export" "live-connectivity"
printf '%-8s-+-%-19s-+-%-13s-+-%-17s\n' "--------" "-------------------" "-------------" "-----------------"
for sdk in "${SDKS[@]}"; do
  printf '%-8s | %-19s | %-13s | %-17s\n' "$sdk" "$(matrix_status "$sdk" registry)" "$(matrix_status "$sdk" import)" "$(matrix_status "$sdk" live)"
done

echo ""
echo "PASS rows: $PASS_COUNT"
echo "FAIL rows: $FAIL_COUNT"

if [ "$FAIL_COUNT" -eq 0 ]; then
  echo "VERDICT: PASS"
  exit 0
fi

echo "VERDICT: FAIL"
exit 1
