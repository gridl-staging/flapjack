#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTL="$SCRIPT_DIR/source_provider_fixture_ctl.sh"
TMP="$(mktemp -d /tmp/fj_source_provider_fixture_ctl_test_XXXXXX)"
FAKE_BIN="$TMP/bin"
DOCKER_LOG="$TMP/docker.log"

cleanup() {
  rm -rf -- "$TMP"
}
trap cleanup EXIT

mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/docker" <<'FAKE_DOCKER'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${SOURCE_PROVIDER_CTL_TEST_DOCKER_LOG:?}"
exit 0
FAKE_DOCKER
chmod +x "$FAKE_BIN/docker"

run_expect_failure() {
  local output status
  set +e
  output="$(PATH="$FAKE_BIN:$PATH" SOURCE_PROVIDER_CTL_TEST_DOCKER_LOG="$DOCKER_LOG" "$@" 2>&1)"
  status=$?
  set -e
  [ "$status" -ne 0 ] || {
    printf 'expected failure, got success: %s\noutput=%s\n' "$*" "$output" >&2
    exit 1
  }
  printf '%s\n' "$output"
}

assert_docker_not_called() {
  [ ! -s "$DOCKER_LOG" ] || {
    printf 'docker should not have been called, saw:\n' >&2
    cat "$DOCKER_LOG" >&2
    exit 1
  }
}

count_fixture_directories() {
  local pattern="$1" matches
  matches="$(compgen -G "$pattern" || true)"
  if [ -z "$matches" ]; then
    printf '0\n'
  else
    printf '%s\n' "$matches" | wc -l | tr -d ' '
  fi
}

run_expect_failure env SOURCE_PROVIDER_CONTAINER=unrelated SOURCE_PROVIDER_FIXTURE_DIR=/tmp/fj_source_provider_fixture_meilisearch_abc bash "$CTL" down meilisearch \
  | grep -F 'SOURCE_PROVIDER_CONTAINER is not an owned meilisearch fixture container' >/dev/null
assert_docker_not_called

unsupported_provider="unknown_$$"
unsupported_fixture_pattern="/tmp/fj_source_provider_fixture_${unsupported_provider}_*"
before_count="$(count_fixture_directories "$unsupported_fixture_pattern")"
[ "$before_count" = 0 ] || {
  printf 'unsupported-provider proof requires a clean baseline: count=%s\n' "$before_count" >&2
  exit 1
}
run_expect_failure bash "$CTL" up "$unsupported_provider" | grep -F 'usage:' >/dev/null
after_count="$(count_fixture_directories "$unsupported_fixture_pattern")"
[ "$before_count" = "$after_count" ] || {
  printf 'unsupported provider leaked a temp fixture directory: before=%s after=%s\n' "$before_count" "$after_count" >&2
  exit 1
}

printf 'source_provider_fixture_ctl_test=PASS\n'
