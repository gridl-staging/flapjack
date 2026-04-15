#!/bin/bash

# load_local_instance_config <repo-root>
# Reads optional repo-local `flapjack.local.conf` overrides and exports:
#   FJ_HOST              default: 127.0.0.1
#   FJ_BACKEND_PORT      default: 7700
#   FJ_DASHBOARD_PORT    default: 5177
#   FJ_TEST_ADMIN_KEY    default: fj_devtestadminkey000000
#   FJ_API_BASE          computed: http://${FJ_HOST}:${FJ_BACKEND_PORT}
#   FJ_DASHBOARD_BASE    computed: http://${FJ_HOST}:${FJ_DASHBOARD_PORT}
# Also exports `FLAPJACK_BACKEND_URL` when provided in config.
local_instance_http_origin() {
  local raw="$1"

  if [[ "$raw" =~ ^(https?)://([^/?#]+) ]]; then
    printf '%s://%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
    return 0
  fi

  return 1
}

local_instance_extract_hostname() {
  local host_port="$1"
  local trimmed=""

  if [[ "$host_port" == \[*\]:* ]]; then
    trimmed="${host_port#\[}"
    printf '%s\n' "${trimmed%%]*}"
    return 0
  fi

  if [[ "$host_port" == \[*\] ]]; then
    trimmed="${host_port#\[}"
    printf '%s\n' "${trimmed%%]*}"
    return 0
  fi

  if [[ "$host_port" == *:* ]]; then
    printf '%s\n' "${host_port%%:*}"
    return 0
  fi

  printf '%s\n' "$host_port"
}

local_instance_is_loopback_host() {
  case "$1" in
    127.0.0.1|localhost|0.0.0.0|::1|'[::1]')
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

local_instance_trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "$value"
}

local_instance_strip_inline_comment() {
  local value="$1"
  local index=0
  local previous=""

  for ((index = 1; index < ${#value}; index += 1)); do
    previous="${value:index-1:1}"
    if [ "${value:index:1}" = "#" ] && [[ "$previous" =~ [[:space:]] ]]; then
      printf '%s\n' "$(local_instance_trim "${value:0:index}")"
      return 0
    fi
  done

  printf '%s\n' "$value"
}

local_instance_apply_config_assignment() {
  local key="$1"
  local value="$2"

  case "$key" in
    FJ_HOST)
      FJ_HOST="$value"
      ;;
    FJ_BACKEND_PORT)
      FJ_BACKEND_PORT="$value"
      ;;
    FJ_DASHBOARD_PORT)
      FJ_DASHBOARD_PORT="$value"
      ;;
    FJ_TEST_ADMIN_KEY)
      FJ_TEST_ADMIN_KEY="$value"
      ;;
    FLAPJACK_BACKEND_URL)
      FLAPJACK_BACKEND_URL="$value"
      ;;
  esac
}

local_instance_load_config_file() {
  local config_path="$1"
  local raw_line=""
  local line=""
  local assignment=""
  local key=""
  local value=""

  # Parse simple KEY=value entries without executing the file as shell.
  while IFS= read -r raw_line || [ -n "$raw_line" ]; do
    line="$(local_instance_trim "$raw_line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue

    assignment="$line"
    if [[ "$assignment" == export[[:space:]]* ]]; then
      assignment="$(local_instance_trim "${assignment#export}")"
    fi

    [ "${assignment#*=}" = "$assignment" ] && continue

    key="$(local_instance_trim "${assignment%%=*}")"
    value="$(local_instance_trim "${assignment#*=}")"
    [ -z "$key" ] && continue

    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    else
      value="$(local_instance_strip_inline_comment "$value")"
    fi

    local_instance_apply_config_assignment "$key" "$value"
  done < "$config_path"
}

load_local_instance_config() {
  local repo_root="$1"
  local config_path="$repo_root/flapjack.local.conf"
  local configured_backend_base=""
  local backend_host=""
  local env_host
  local env_backend_port
  local env_dashboard_port
  local env_admin_key
  local env_backend_url

  env_host="$(printenv FJ_HOST 2>/dev/null || true)"
  env_backend_port="$(printenv FJ_BACKEND_PORT 2>/dev/null || true)"
  env_dashboard_port="$(printenv FJ_DASHBOARD_PORT 2>/dev/null || true)"
  env_admin_key="$(printenv FJ_TEST_ADMIN_KEY 2>/dev/null || true)"
  env_backend_url="$(printenv FLAPJACK_BACKEND_URL 2>/dev/null || true)"

  FJ_HOST="127.0.0.1"
  FJ_BACKEND_PORT="7700"
  FJ_DASHBOARD_PORT="5177"
  FJ_TEST_ADMIN_KEY=""

  if [ -f "$config_path" ]; then
    local_instance_load_config_file "$config_path"
  fi

  [ -n "$env_host" ] && FJ_HOST="$env_host"
  [ -n "$env_backend_port" ] && FJ_BACKEND_PORT="$env_backend_port"
  [ -n "$env_dashboard_port" ] && FJ_DASHBOARD_PORT="$env_dashboard_port"
  [ -n "$env_admin_key" ] && FJ_TEST_ADMIN_KEY="$env_admin_key"
  [ -n "$env_backend_url" ] && FLAPJACK_BACKEND_URL="$env_backend_url"

  configured_backend_base="$(local_instance_http_origin "${FLAPJACK_BACKEND_URL:-}" || true)"
  if [ -n "$configured_backend_base" ]; then
    FJ_API_BASE="$configured_backend_base"
    backend_host="$(local_instance_extract_hostname "${configured_backend_base#*://}")"
  else
    FJ_API_BASE="http://${FJ_HOST}:${FJ_BACKEND_PORT}"
    backend_host="$FJ_HOST"
  fi

  if [ -z "${FJ_TEST_ADMIN_KEY:-}" ]; then
    if local_instance_is_loopback_host "$backend_host"; then
      FJ_TEST_ADMIN_KEY="fj_devtestadminkey000000"
    else
      echo "ERROR: FJ_TEST_ADMIN_KEY must be set when using a non-loopback backend URL: $FJ_API_BASE" >&2
      return 1
    fi
  fi

  FJ_DASHBOARD_BASE="http://${FJ_HOST}:${FJ_DASHBOARD_PORT}"
}
