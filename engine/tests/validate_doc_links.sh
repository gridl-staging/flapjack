#!/usr/bin/env bash
# validate_doc_links.sh — Check that public doc graph links resolve to real files.
#
# Uses .debbie.toml as the source of truth for the Stage 2 public doc graph, then
# verifies that every relative markdown link in those canonical docs resolves on
# disk.
#
# Usage:
#   bash engine/tests/validate_doc_links.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck source=engine/tests/doc_sync_helpers.sh
source "$SCRIPT_DIR/doc_sync_helpers.sh"

FAILURE_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-link-failures.XXXXXX")
COUNT_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-link-counts.XXXXXX")
DOCS_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-link-docs.XXXXXX")

cleanup() {
  doc_sync_cleanup
  rm -f "$FAILURE_LOG" "$COUNT_LOG" "$DOCS_LOG"
}
trap cleanup EXIT

doc_sync_init "$REPO_DIR"

printf "\033[1mDoc Link Validation\033[0m\n"
doc_sync_collect_sync_surface
doc_sync_collect_validation_docs "$DOCS_LOG" "engine/docs/HIGHEST_LEVEL.md"

while IFS= read -r doc; do
  [ -n "$doc" ] || continue

  while IFS=$'\t' read -r src_doc line_num target resolved_path; do
    [ -n "$resolved_path" ] || continue

    if [ ! -e "$REPO_DIR/$resolved_path" ]; then
      printf "  \033[0;31m✗\033[0m %s:%s → %s (not found)\n" "$src_doc" "$line_num" "$target"
      printf "FAIL\n" >> "$FAILURE_LOG"
    fi
    printf "CHECK\n" >> "$COUNT_LOG"
  done < <(doc_sync_extract_relative_markdown_links "$doc")
done < "$DOCS_LOG"

CHECKED=$(doc_sync_count_log_lines "$COUNT_LOG")
FAILURES=$(doc_sync_count_log_lines "$FAILURE_LOG")
DOCS_CHECKED=$(doc_sync_count_log_lines "$DOCS_LOG")

printf "\n\033[1mChecked %d links across %d public doc graph docs\033[0m\n" "$CHECKED" "$DOCS_CHECKED"

if [ "$FAILURES" -gt 0 ]; then
  printf "\033[0;31m%d broken link(s) found\033[0m\n" "$FAILURES"
  exit 1
fi

printf "\033[0;32mAll links valid\033[0m\n"
exit 0
