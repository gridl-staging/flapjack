#!/usr/bin/env bash

# Shared digest-pinned Meilisearch and Typesense source fixtures.
# Callers own TMP, container names, error reporting, and final cleanup policy.

readonly MEILI_IMAGE="getmeili/meilisearch@sha256:9694a59df43ee3f54b3fda9c5de381a3ee9852678e3e31cadf37d6bddea7fc1b"
readonly TYPESENSE_IMAGE_REF="typesense/typesense:30.2"
readonly TYPESENSE_IMAGE_DIGEST="sha256:610f2d34b1f93d00762869da2c67736775e5798d19a2c8b91b014b8a0cc1e110"
readonly TYPESENSE_IMAGE="${TYPESENSE_IMAGE_REF}@${TYPESENSE_IMAGE_DIGEST}"
readonly MEILI_KEY="source-migration-provider-parity-meili-key"
readonly TYPESENSE_KEY="source-migration-provider-parity-typesense-key"
readonly TYPESENSE_PRODUCTS="fj_ts_migration_products"
readonly TYPESENSE_CATEGORIES="fj_ts_migration_categories"
readonly TYPESENSE_SYNONYM_SET="fj_ts_migration_synonyms"
readonly TYPESENSE_CURATION_SET="fj_ts_migration_curations"
readonly SOURCE_PROVIDER_FIXTURE_LABEL="flapjack.source_provider_fixture"
readonly SOURCE_PROVIDER_FIXTURE_PROVIDER_LABEL="flapjack.source_provider_fixture.provider"
readonly SOURCE_PROVIDER_FIXTURE_TOKEN_LABEL="flapjack.source_provider_fixture.token"

source_provider_container_name_matches() {
  local provider="$1" name="$2"
  case "$provider" in
    meilisearch) [[ "$name" =~ ^fj_source_migration_provider_parity_meili_[0-9]+$ ]] ;;
    typesense) [[ "$name" =~ ^fj_source_migration_provider_parity_typesense_[0-9]+$ ]] ;;
    *) return 1 ;;
  esac
}

container_exists() {
  local name="$1"
  docker ps -a --filter "name=^/${name}$" --format '{{.Names}}' 2>/dev/null | grep -Fxq "$name"
}

container_label_value() {
  local name="$1" label="$2"
  docker inspect --format "{{ index .Config.Labels \"$label\" }}" "$name" 2>/dev/null || true
}

container_has_fixture_ownership() {
  local provider="$1" name="$2" token="${SOURCE_PROVIDER_OWNER_TOKEN:-}"
  [ "$(container_label_value "$name" "$SOURCE_PROVIDER_FIXTURE_LABEL")" = "1" ] || return 1
  [ "$(container_label_value "$name" "$SOURCE_PROVIDER_FIXTURE_PROVIDER_LABEL")" = "$provider" ] || return 1
  if [ -n "$token" ]; then
    [ "$(container_label_value "$name" "$SOURCE_PROVIDER_FIXTURE_TOKEN_LABEL")" = "$token" ] || return 1
  fi
}

source_provider_docker_labels() {
  local provider="$1" token="${SOURCE_PROVIDER_OWNER_TOKEN:-}"
  SOURCE_PROVIDER_DOCKER_LABEL_ARGS=(
    --label "${SOURCE_PROVIDER_FIXTURE_LABEL}=1"
    --label "${SOURCE_PROVIDER_FIXTURE_PROVIDER_LABEL}=${provider}"
  )
  if [ -n "$token" ]; then
    SOURCE_PROVIDER_DOCKER_LABEL_ARGS+=(--label "${SOURCE_PROVIDER_FIXTURE_TOKEN_LABEL}=${token}")
  fi
}

remove_owned_container() {
  local label="$1" name="$2"
  [ -n "$name" ] || return 0
  if ! source_provider_container_name_matches "$label" "$name"; then
    mark_cleanup_failure "${label}_container_unowned_name name=${name}"
    return 0
  fi
  if container_exists "$name"; then
    if ! container_has_fixture_ownership "$label" "$name"; then
      mark_cleanup_failure "${label}_container_unowned_label name=${name}"
      return 0
    fi
    if ! docker rm -f "$name" >/dev/null 2>&1; then
      mark_cleanup_failure "${label}_container_rm_failed name=${name}"
      return 0
    fi
  fi
  if container_exists "$name"; then
    mark_cleanup_failure "${label}_container_residue name=${name}"
  fi
  return 0
}

wait_for_json() {
  local url="$1" predicate="$2" header_name="${3:-}" header_value="${4:-}" out="$5"
  local attempt
  for attempt in $(seq 1 120); do
    if [ -n "$header_name" ]; then
      curl -sS --connect-timeout 1 --max-time 2 -H "$header_name: $header_value" "$url" >"$out" 2>/dev/null || true
    else
      curl -sS --connect-timeout 1 --max-time 2 "$url" >"$out" 2>/dev/null || true
    fi
    jq -e "$predicate" "$out" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  die_indeterminate "upstream_readiness_failed url=${url}"
}

poll_meili_task() {
  local task_uid="$1" attempt
  local out="$TMP/meili_task_${task_uid}.json"
  for attempt in $(seq 1 120); do
    curl -sS --connect-timeout 1 --max-time 2 -H "Authorization: Bearer $MEILI_KEY" \
      "http://127.0.0.1:${MEILI_PORT}/tasks/${task_uid}" >"$out" 2>/dev/null || true
    jq -e '.status == "succeeded"' "$out" >/dev/null 2>&1 && return 0
    jq -e '.status == "failed" or .status == "canceled"' "$out" >/dev/null 2>&1 \
      && die_indeterminate 'meilisearch_seed_task_failed'
    sleep 0.25
  done
  die_indeterminate 'meilisearch_seed_task_timeout'
}

submit_meilisearch_seed_task() {
  local label="$1" path="$2" body="$3"
  local response="$TMP/meili_${label}.json" status curl_exit task_uid
  set +e
  status="$(curl -sS --connect-timeout 1 --max-time 10 \
    -o "$response" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $MEILI_KEY" -H 'Content-Type: application/json' \
    --data "$body" "http://127.0.0.1:${MEILI_PORT}${path}")"
  curl_exit=$?
  set -e
  [ "$curl_exit" -eq 0 ] \
    || die_indeterminate "meilisearch_${label}_transport_${curl_exit}"
  [ "$status" = 202 ] || die_indeterminate "meilisearch_${label}_status_${status}"
  task_uid="$(jq -er '.taskUid | select(type == "number")' "$response" 2>/dev/null || true)"
  [ -n "$task_uid" ] || die_indeterminate "meilisearch_${label}_task_uid_missing"
  poll_meili_task "$task_uid"
}

start_meilisearch() {
  source_provider_docker_labels meilisearch
  docker run -d --name "$MEILI_CONTAINER" "${SOURCE_PROVIDER_DOCKER_LABEL_ARGS[@]}" --publish 127.0.0.1::7700 \
    -e "MEILI_MASTER_KEY=$MEILI_KEY" -e MEILI_ENV=development "$MEILI_IMAGE" >"$TMP/meili_container_id.txt"
  MEILI_PORT="$(docker port "$MEILI_CONTAINER" 7700/tcp | awk -F: '/127.0.0.1/ {print $NF; exit}')"
  [ -n "$MEILI_PORT" ] || die_indeterminate 'meilisearch_port_missing'
  wait_for_json "http://127.0.0.1:${MEILI_PORT}/health" '.status == "available"' '' '' "$TMP/meili_health.json"
  submit_meilisearch_seed_task create_index /indexes \
    '{"uid":"configured_pk","primaryKey":"sku"}'
  submit_meilisearch_seed_task seed_documents /indexes/configured_pk/documents \
    '[{"sku":"MEILI-001","title":"Espresso Tamper","price":24.5,"stock":7},{"sku":"MEILI-002","title":"Pour Over Kettle","price":39.75,"stock":3}]'
  curl -sS -H "Authorization: Bearer $MEILI_KEY" \
    "http://127.0.0.1:${MEILI_PORT}/indexes" >"$TMP/meili_expected_listing.json"
}

verify_typesense_image_digest() {
  if ! docker image inspect "$TYPESENSE_IMAGE" >"$TMP/typesense_image_inspect.json" 2>/dev/null; then
    docker pull "$TYPESENSE_IMAGE" >"$TMP/typesense_image_pull.txt"
    docker image inspect "$TYPESENSE_IMAGE" >"$TMP/typesense_image_inspect.json"
  fi
  jq -e --arg digest "$TYPESENSE_IMAGE_DIGEST" '
    .[0].RepoDigests // []
    | any(endswith("@" + $digest))
  ' "$TMP/typesense_image_inspect.json" >/dev/null \
    || die_indeterminate 'typesense_image_digest_mismatch'
}

typesense_json() {
  local method="$1" path="$2" body="${3:-}" out="$4" status
  local args=(-sS -o "$out" -w '%{http_code}' -X "$method" -H "X-TYPESENSE-API-KEY: $TYPESENSE_KEY" -H 'Content-Type: application/json')
  [ -z "$body" ] || args+=(--data "$body")
  status="$(curl "${args[@]}" "http://127.0.0.1:${TYPESENSE_PORT}${path}")"
  [ "$status" = 200 ] || [ "$status" = 201 ] || die_indeterminate "typesense_seed_status_${status}"
}

seed_typesense_linked_sets_from_fixture() {
  jq -e --arg name "$TYPESENSE_SYNONYM_SET" '
    .source.synonym_sets[] | select(.name == $name) | {items}
  ' "$TYPESENSE_FIXTURE" >"$TMP/typesense_synonym_set.json"
  typesense_json PUT "/synonym_sets/${TYPESENSE_SYNONYM_SET}" \
    "$(cat "$TMP/typesense_synonym_set.json")" "$TMP/typesense_create_synonym_set.json"

  jq -e --arg name "$TYPESENSE_CURATION_SET" '
    .source.curation_sets[] | select(.name == $name) | {items}
  ' "$TYPESENSE_FIXTURE" >"$TMP/typesense_curation_set.json"
  typesense_json PUT "/curation_sets/${TYPESENSE_CURATION_SET}" \
    "$(cat "$TMP/typesense_curation_set.json")" "$TMP/typesense_create_curation_set.json"
}

seed_typesense_collection_from_fixture() {
  local collection="$1" index=0 document
  jq -e --arg name "$collection" '
    .source.collections[] | select(.name == $name) | del(.documents)
  ' "$TYPESENSE_FIXTURE" >"$TMP/typesense_schema_${collection}.json"
  typesense_json POST /collections \
    "$(cat "$TMP/typesense_schema_${collection}.json")" "$TMP/typesense_create_${collection}.json"

  case "$collection" in
    "$TYPESENSE_CATEGORIES")
      jq -cn '{id:"cat_1",name:"Coffee",priority:1,active:true,labels:["coffee"]}' \
        >"$TMP/typesense_documents_${collection}.jsonl"
      ;;
    "$TYPESENSE_PRODUCTS")
      jq -cn '{id:"prod_1",title:"Espresso",sku:"ESP-001",price:12.5,inventory:8,available:true,tags:["coffee"],category_id:"cat_1"}' \
        >"$TMP/typesense_documents_${collection}.jsonl"
      jq -cn '{id:"prod_2",title:"Latte",sku:"LAT-002",price:9.5,inventory:5,available:true,tags:["coffee","milk"],category_id:"cat_1"}' \
        >>"$TMP/typesense_documents_${collection}.jsonl"
      ;;
    *)
      die_indeterminate "typesense_unknown_fixture_collection_${collection}"
      ;;
  esac
  while IFS= read -r document; do
    index=$((index + 1))
    typesense_json POST "/collections/${collection}/documents" "$document" \
      "$TMP/typesense_seed_${collection}_${index}.json"
  done <"$TMP/typesense_documents_${collection}.jsonl"
}

start_typesense() {
  mkdir -p "$TMP/typesense_data"
  verify_typesense_image_digest
  source_provider_docker_labels typesense
  docker run -d --name "$TYPESENSE_CONTAINER" "${SOURCE_PROVIDER_DOCKER_LABEL_ARGS[@]}" --publish 127.0.0.1::8108 \
    --volume "$TMP/typesense_data:/data" \
    "$TYPESENSE_IMAGE" --data-dir=/data --api-key="$TYPESENSE_KEY" >"$TMP/typesense_container_id.txt"
  TYPESENSE_PORT="$(docker port "$TYPESENSE_CONTAINER" 8108/tcp | awk -F: '/127.0.0.1/ {print $NF; exit}')"
  [ -n "$TYPESENSE_PORT" ] || die_indeterminate 'typesense_port_missing'
  wait_for_json "http://127.0.0.1:${TYPESENSE_PORT}/health" '.ok == true' '' '' "$TMP/typesense_health.json"
  seed_typesense_linked_sets_from_fixture
  seed_typesense_collection_from_fixture "$TYPESENSE_CATEGORIES"
  sleep 1
  seed_typesense_collection_from_fixture "$TYPESENSE_PRODUCTS"
  curl -sS -H "X-TYPESENSE-API-KEY: $TYPESENSE_KEY" \
    "http://127.0.0.1:${TYPESENSE_PORT}/collections?exclude_fields=fields" >"$TMP/typesense_expected_listing.json"
}
