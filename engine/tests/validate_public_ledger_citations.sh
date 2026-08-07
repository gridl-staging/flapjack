#!/usr/bin/env bash
# Validate public-ledger citations against the files .debbie.toml publishes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_DIR="${PUBLIC_LEDGER_REPO_DIR:-$DEFAULT_REPO_DIR}"

# shellcheck source=engine/tests/doc_sync_helpers.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/doc_sync_helpers.sh"

PUBLIC_LEDGER_CANDIDATES=(
  PROJECT_OVERVIEW.md
  ROADMAP.md
  CHANGELOG.md
  engine/docs2/FEATURES.md
)

public_ledger_reference_shape_is_concrete() {
  local target="$1"
  if [[ "$target" =~ ^[A-Za-z][A-Za-z0-9+.-]*: ]]; then
    return 1
  fi
  case "$target" in
    *[[:space:]]*|*'"'*|*"'"*|*'#'*|*'*'*|*'?'*|*'{'*|*'}'*|*'<'*|*'>'*|*'$'*)
      return 1
      ;;
  esac
  return 0
}

public_ledger_strip_markdown_link_title() {
  printf '%s\n' "$1" | sed -E 's/[[:space:]]+("[^"]*"|'\''[^'\'']*'\''|\([^()]*\))[[:space:]]*$//'
}

public_ledger_path_stays_within_repo() {
  local raw_path="${1#/}"
  local depth=0
  local IFS='/'
  local -a parts
  read -r -a parts <<< "$raw_path"

  local part
  for part in "${parts[@]}"; do
    case "$part" in
      ""|.)
        ;;
      ..)
        [ "$depth" -gt 0 ] || return 1
        depth=$((depth - 1))
        ;;
      *)
        depth=$((depth + 1))
        ;;
    esac
  done
}

public_ledger_normalize_or_preserve_above_root() {
  local candidate="$1"
  if ! public_ledger_path_stays_within_repo "$candidate"; then
    printf '%s\n' "$candidate"
    return 0
  fi

  local normalized
  normalized="$(doc_sync_normalize_repo_path "$candidate")"
  [ -n "$normalized" ] || return 1
  printf '%s\n' "$normalized"
}

public_ledger_resolve_markdown_target() {
  local ledger="$1"
  local target="$2"
  local link_path
  link_path="$(public_ledger_strip_markdown_link_title "$target")"

  public_ledger_reference_shape_is_concrete "$link_path" || return 1

  local ledger_dir
  ledger_dir="$(dirname -- "$ledger")"
  [ "$ledger_dir" = '.' ] && ledger_dir=''

  local candidate
  if [[ "$link_path" == /* ]]; then
    candidate="${link_path#/}"
  else
    candidate="${ledger_dir:+$ledger_dir/}$link_path"
  fi

  public_ledger_normalize_or_preserve_above_root "$candidate"
}

public_ledger_extract_prose_references() {
  local ledger="$1"
  local ledger_dir
  ledger_dir="$(dirname -- "$ledger")"
  [ "$ledger_dir" = '.' ] && ledger_dir=''

  local prose_file
  local matches_file
  prose_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-prose-content.XXXXXX")"
  matches_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-prose-references.XXXXXX")"
  local sed_status=0
  sed -E 's/\]\([^)]*\)/]/g' "$REPO_DIR/$ledger" > "$prose_file" || sed_status=$?
  if [ "$sed_status" -ne 0 ]; then
    rm -f "$prose_file" "$matches_file"
    return "$sed_status"
  fi

  local grep_status=0
  grep -oE '(^|[^#[:alnum:]_./-])((engine/docs2/(4_EVIDENCE|evidence)/|docs2/evidence/|4_EVIDENCE/|docs/|chats/|chatting/|implemented/|pp/)[[:alnum:]_./-]*\.md[#?]?)' "$prose_file" > "$matches_file" || grep_status=$?
  if [ "$grep_status" -gt 1 ]; then
    rm -f "$prose_file" "$matches_file"
    return "$grep_status"
  fi

  while IFS= read -r literal; do
    literal="$(printf '%s\n' "$literal" | sed -E 's/^[^[:alnum:]_./-]//')"
    [ -n "$literal" ] || continue
    public_ledger_reference_shape_is_concrete "$literal" || continue

    local candidate
    case "$literal" in
      engine/*|docs/*|chats/*|chatting/*|implemented/*|pp/*)
        candidate="$literal"
        ;;
      docs2/evidence/*)
        candidate="engine/$literal"
        ;;
      4_EVIDENCE/*)
        candidate="${ledger_dir:+$ledger_dir/}$literal"
        ;;
      *)
        continue
        ;;
    esac
    public_ledger_normalize_or_preserve_above_root "$candidate"
  done < "$matches_file"
  rm -f "$prose_file" "$matches_file"
}

public_ledger_extract_markdown_references() {
  local ledger="$1"
  local links_file
  links_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-markdown-references.XXXXXX")"
  local extractor_status=0
  doc_sync_extract_relative_markdown_links "$ledger" > "$links_file" || extractor_status=$?
  if [ "$extractor_status" -ne 0 ]; then
    rm -f "$links_file"
    return "$extractor_status"
  fi

  while IFS=$'\t' read -r _source _line target resolved; do
    [ -n "${resolved:-}" ] || continue
    public_ledger_resolve_markdown_target "$ledger" "$target" || continue
  done < "$links_file"
  rm -f "$links_file"
}

public_ledger_reject_nul_bytes() {
  local ledger="$1"
  local stripped_file
  stripped_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-nul-stripped.XXXXXX")"

  local filter_status=0
  LC_ALL=C tr -d '\000' < "$REPO_DIR/$ledger" > "$stripped_file" || filter_status=$?
  if [ "$filter_status" -ne 0 ]; then
    rm -f "$stripped_file"
    printf 'ERROR: failed to inspect synced ledger for NUL bytes: %s\n' "$ledger" >&2
    return "$filter_status"
  fi

  local compare_status=0
  cmp -s "$REPO_DIR/$ledger" "$stripped_file" || compare_status=$?
  rm -f "$stripped_file"
  case "$compare_status" in
    0)
      return 0
      ;;
    1)
      printf 'ERROR: synced public ledger contains a NUL byte: %s\n' "$ledger" >&2
      return 1
      ;;
    *)
      printf 'ERROR: failed to compare synced ledger while checking NUL bytes: %s\n' "$ledger" >&2
      return "$compare_status"
      ;;
  esac
}

public_ledger_extract_references() {
  local ledger="$1"
  public_ledger_reject_nul_bytes "$ledger" || return 1

  local extracted_file
  extracted_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-extracted-references.XXXXXX")"

  local extractor_status=0
  public_ledger_extract_markdown_references "$ledger" > "$extracted_file" || extractor_status=$?
  if [ "$extractor_status" -ne 0 ]; then
    rm -f "$extracted_file"
    return "$extractor_status"
  fi

  public_ledger_extract_prose_references "$ledger" >> "$extracted_file" || extractor_status=$?
  if [ "$extractor_status" -ne 0 ]; then
    rm -f "$extracted_file"
    return "$extractor_status"
  fi

  sed '/^$/d' "$extracted_file" | sort -u
  extractor_status=$?
  rm -f "$extracted_file"
  return "$extractor_status"
}

public_ledger_collect_synced_ledgers() {
  local output_file="$1"
  : > "$output_file"

  local ledger
  for ledger in "${PUBLIC_LEDGER_CANDIDATES[@]}"; do
    doc_sync_path_is_synced "$ledger" || continue
    if [ ! -f "$REPO_DIR/$ledger" ]; then
      printf 'ERROR: synced public ledger is missing: %s\n' "$ledger" >&2
      return 1
    fi
    printf '%s\n' "$ledger" >> "$output_file"
  done

  if [ ! -s "$output_file" ]; then
    printf 'ERROR: no synced public ledger candidates found\n' >&2
    return 1
  fi
}

public_ledger_validate_one() {
  local ledger="$1"
  local references_file="$2"
  if ! public_ledger_extract_references "$ledger" > "$references_file"; then
    printf 'ERROR: failed to extract references from synced ledger: %s\n' "$ledger" >&2
    return 1
  fi

  local examined=0
  local resolvable=0
  local unresolvable=0
  local reference
  while IFS= read -r reference; do
    [ -n "$reference" ] || continue
    examined=$((examined + 1))
    if ! public_ledger_path_stays_within_repo "$reference"; then
      unresolvable=$((unresolvable + 1))
      printf 'reference ledger=%s result=UNRESOLVABLE reason=above_repository_root path=%s\n' "$ledger" "$reference"
    elif doc_sync_path_is_synced "$reference"; then
      resolvable=$((resolvable + 1))
      printf 'reference ledger=%s result=RESOLVABLE path=%s\n' "$ledger" "$reference"
    else
      unresolvable=$((unresolvable + 1))
      printf 'reference ledger=%s result=UNRESOLVABLE reason=outside_sync_surface path=%s\n' "$ledger" "$reference"
    fi
  done < "$references_file"

  local status=PASS
  [ "$examined" -eq 0 ] && status=VACUOUS
  [ "$unresolvable" -gt 0 ] && status=FAIL
  printf 'ledger=%s references_examined=%d references_resolvable=%d references_unresolvable=%d status=%s\n' \
    "$ledger" "$examined" "$resolvable" "$unresolvable" "$status"
  [ "$unresolvable" -eq 0 ]
}

validate_public_ledger_citations() {
  local ledgers_file
  local references_file
  ledgers_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-ledgers.XXXXXX")"
  references_file="$(mktemp "${TMPDIR:-/tmp}/flapjack-public-references.XXXXXX")"
  trap 'rm -f "$ledgers_file" "$references_file"; doc_sync_cleanup' RETURN

  doc_sync_init "$REPO_DIR"
  doc_sync_collect_sync_surface || return 1

  local sync_entry_count
  sync_entry_count=$(( $(doc_sync_count_log_lines "$DOC_SYNC_FILES_LOG") + $(doc_sync_count_log_lines "$DOC_SYNC_DIRS_LOG") ))
  if [ "$sync_entry_count" -eq 0 ]; then
    printf 'ERROR: parsed sync surface is empty: %s\n' "$DOC_SYNC_CONFIG_FILE" >&2
    return 1
  fi

  public_ledger_collect_synced_ledgers "$ledgers_file" || return 1

  local overall_status=0
  local total_examined=0
  local ledger
  while IFS= read -r ledger; do
    [ -n "$ledger" ] || continue
    public_ledger_validate_one "$ledger" "$references_file" || overall_status=1
    total_examined=$((total_examined + $(doc_sync_count_log_lines "$references_file") ))
  done < "$ledgers_file"

  if [ "$total_examined" -eq 0 ]; then
    printf 'ERROR: zero public ledger references examined across synced ledgers\n' >&2
    overall_status=1
  fi

  return "$overall_status"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  validate_public_ledger_citations
fi
