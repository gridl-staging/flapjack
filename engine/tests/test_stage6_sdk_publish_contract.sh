#!/bin/bash
set -euo pipefail

SCRIPT="engine/tests/test_stage6_sdk_publish.sh"

grep -Fqx "EXPECTED_NPM_VERSION=\"\${EXPECTED_NPM_VERSION:-\${EXPECTED_RELEASE_VERSION_NPM:-0.1.0-beta.1}}\"" "$SCRIPT" || { echo "missing expected npm default"; exit 1; }
grep -Fqx "EXPECTED_PYPI_VERSION=\"\${EXPECTED_PYPI_VERSION:-\${EXPECTED_RELEASE_VERSION_PYPI:-1.0.0}}\"" "$SCRIPT" || { echo "missing expected pypi default"; exit 1; }
grep -Fqx "EXPECTED_GO_VERSION=\"\${EXPECTED_GO_VERSION:-\${EXPECTED_RELEASE_VERSION_GO:-4.0.0}}\"" "$SCRIPT" || { echo "missing expected go default"; exit 1; }
grep -Fqx "EXPECTED_NPM_OWNER=\"\${EXPECTED_NPM_OWNER_SUBSTRING:-stuartcrobinsonnpm}\"" "$SCRIPT" || { echo "missing expected npm owner default"; exit 1; }
grep -Fqx "LOCAL_GO_SDK_PATH=\"\${LOCAL_GO_SDK_PATH:-\$ENGINE_DIR/../sdks/go}\"" "$SCRIPT" || { echo "missing default local go sdk path"; exit 1; }
grep -Fqx "      GOPROXY=\"https://proxy.golang.org,direct\" go get github.com/flapjackhq/flapjack-search-go/v4@v\"\$EXPECTED_GO_VERSION\" && \\" "$SCRIPT" || { echo "missing expected go published-module version parameterization"; exit 1; }
grep -Fqx "    if [ \"\$GO_PUBLISHED_REQUIRE_VERSION\" != \"v\$EXPECTED_GO_VERSION\" ]; then" "$SCRIPT" || { echo "missing go.mod published-module version assertion"; exit 1; }
grep -Fqx "    if [ \"\$GO_PUBLISHED_MODULE_JSON_EXIT\" -ne 0 ] || [ \"\$GO_PUBLISHED_RESOLVED_VERSION\" != \"v\$EXPECTED_GO_VERSION\" ]; then" "$SCRIPT" || { echo "missing go list resolved-version assertion"; exit 1; }

echo "PASS stage6 registry defaults are lane-aligned"
