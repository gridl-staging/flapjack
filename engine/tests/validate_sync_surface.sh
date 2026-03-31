#!/usr/bin/env bash
# validate_sync_surface.sh — Ensure the canonical public doc graph stays within
# the .debbie sync surface declared for public repos.
#
# Uses .debbie.toml as the source of truth for the Stage 2 public doc graph, then
# checks those canonical markdown docs for relative links that resolve outside
# the synced surface.
#
# Usage:
#   bash engine/tests/validate_sync_surface.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck source=engine/tests/doc_sync_helpers.sh
source "$SCRIPT_DIR/doc_sync_helpers.sh"

FAILURE_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-sync-failures.XXXXXX")
COUNT_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-sync-counts.XXXXXX")
DOCS_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-sync-docs.XXXXXX")

cleanup() {
  doc_sync_cleanup
  rm -f "$FAILURE_LOG" "$COUNT_LOG" "$DOCS_LOG"
}
trap cleanup EXIT

doc_sync_init "$REPO_DIR"

# TODO: Document validate_doc_sync_surface.
validate_doc_sync_surface() {
  local doc
  while IFS= read -r doc; do
    [ -n "$doc" ] || continue
    while IFS=$'\t' read -r src_doc line_num target resolved_path; do
      [ -n "$resolved_path" ] || continue

      if [ ! -e "$REPO_DIR/$resolved_path" ]; then
        # Missing targets belong to validate_doc_links.sh; keep this validator
        # focused on sync-surface coverage only.
        continue
      fi

      printf "CHECK\n" >> "$COUNT_LOG"
      if ! doc_sync_path_is_synced "$resolved_path"; then
        printf "  \033[0;31m✗\033[0m %s:%s → %s (resolves to %s, outside sync surface)\n" "$src_doc" "$line_num" "$target" "$resolved_path"
        printf '%s\n' "$resolved_path" >> "$FAILURE_LOG"
      fi
    done < <(doc_sync_extract_relative_markdown_links "$doc")
  done < "$DOCS_LOG"
}

printf "\033[1mDoc Sync Surface Validation\033[0m\n"
doc_sync_collect_sync_surface
doc_sync_collect_validation_docs "$DOCS_LOG"
validate_doc_sync_surface

CHECKED=$(doc_sync_count_log_lines "$COUNT_LOG")
FAILURES=$(doc_sync_count_log_lines "$FAILURE_LOG")
DOCS_CHECKED=$(doc_sync_count_log_lines "$DOCS_LOG")

printf "\n\033[1mChecked %d relative links across %d public doc graph docs\033[0m\n" "$CHECKED" "$DOCS_CHECKED"

if [ "$FAILURES" -gt 0 ]; then
  printf "\033[0;31m%d unsynced link target(s) found\033[0m\n" "$FAILURES"
  printf "\n\033[1mUnsynced targets (unique):\033[0m\n"
  sort -u "$FAILURE_LOG" | sed 's/^/  - /'
  exit 1
fi

printf "\033[0;32mAll checked link targets are within .debbie sync surface\033[0m\n"
exit 0
