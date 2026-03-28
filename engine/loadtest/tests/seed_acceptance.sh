#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEED_SCRIPT="$ROOT_DIR/seed-loadtest-data.sh"
LOADTEST_HELPERS="$ROOT_DIR/lib/loadtest_shell_helpers.sh"

if [[ ! -x "$SEED_SCRIPT" ]]; then
  echo "FAIL: missing executable $SEED_SCRIPT"
  exit 1
fi

if [[ ! -f "$LOADTEST_HELPERS" ]]; then
  echo "FAIL: missing $LOADTEST_HELPERS"
  exit 1
fi

# shellcheck source=../lib/loadtest_shell_helpers.sh
source "$LOADTEST_HELPERS"

require_loadtest_commands curl jq node
load_shared_loadtest_config
initialize_loadtest_auth_headers

if ! "$SEED_SCRIPT" >/tmp/loadtest_seed_acceptance_seed.out 2>&1; then
  echo "FAIL: seed script failed during seed acceptance."
  cat /tmp/loadtest_seed_acceptance_seed.out
  exit 1
fi

settings_response="$(loadtest_http_request PUT "/1/indexes/${FLAPJACK_WRITE_INDEX}/settings" '{"searchableAttributes":["name","description"]}' "200,207")"
settings_task_id="$(extract_loadtest_numeric_task_id "$settings_response")"
wait_for_loadtest_task_published "$settings_task_id"

batch_response="$(
  loadtest_http_request POST "/1/indexes/${FLAPJACK_WRITE_INDEX}/batch" \
    '{"requests":[{"action":"addObject","body":{"objectID":"seed-acceptance-write-0001","name":"Seed Acceptance Write Product","description":"Runtime acceptance write document.","brand":"Acceptance","category":"Accessories","subcategory":"Input","price":15.5,"rating":4.1,"reviewCount":3,"inStock":true,"tags":["acceptance","write"],"color":"Black","releaseYear":2026,"_geo":{"lat":40.7128,"lng":-74.0060}}}]}' \
    "200"
)"
batch_task_id="$(extract_loadtest_numeric_task_id "$batch_response")"
wait_for_loadtest_task_published "$batch_task_id"

post_search_response="$(loadtest_http_request POST "/1/indexes/${FLAPJACK_READ_INDEX}/query" '{"query":"MacBook","hitsPerPage":5}' "200")"
post_hits="$(jq -r '(.hits // []) | length' <<<"$post_search_response")"
if (( post_hits < 1 )); then
  echo "FAIL: POST representative query returned no hits."
  exit 1
fi

get_search_response="$(loadtest_http_request GET "/1/indexes/${FLAPJACK_READ_INDEX}/query?query=MacBook&hitsPerPage=5" "" "200")"
get_hits="$(jq -r '(.hits // []) | length' <<<"$get_search_response")"
if (( get_hits < 1 )); then
  echo "FAIL: GET representative query returned no hits."
  exit 1
fi

echo "PASS: seed acceptance runtime contract checks"
