#!/bin/sh
# validate_doc_links.sh — Check that internal markdown links resolve to real files.
#
# Scans the public/root routing docs plus the linked strategy docs that act as
# the current source of truth for launch status.
# for relative markdown links (excluding http/https/mailto/anchors) and verifies
# each target exists on disk.
#
# Usage:
#   ./engine/tests/validate_doc_links.sh

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

DOCS="README.md PRIORITIES.md ROADMAP.md engine/README.md engine/docs/HIGHEST_LEVEL.md engine/docs2/FEATURES.md engine/docs2/1_STRATEGY/HIGHEST_PRIORITY.md"
FAILURE_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-link-failures.XXXXXX")
COUNT_LOG=$(mktemp "${TMPDIR:-/tmp}/flapjack-link-counts.XXXXXX")

cleanup() {
  rm -f "$FAILURE_LOG" "$COUNT_LOG"
}
trap cleanup EXIT

printf "\033[1mDoc Link Validation\033[0m\n"

for doc in $DOCS; do
  doc_path="$REPO_DIR/$doc"
  if [ ! -f "$doc_path" ]; then
    printf "  \033[1;33mWARN\033[0m %s not found, skipping\n" "$doc"
    continue
  fi

  doc_dir=$(dirname "$doc_path")
  line_num=0

  # Extract links with line numbers using grep -n for ](target) patterns
  # grep -oE gives us the match but not line numbers in one shot on all platforms,
  # so we use grep -n to get lines containing links, then extract targets per line.
  grep -n '\]([^)]*)' "$doc_path" | while IFS= read -r match_line; do
    line_num=$(printf '%s' "$match_line" | cut -d: -f1)
    line_content=$(printf '%s' "$match_line" | cut -d: -f2-)

    # Extract all ](target) from this line via grep -oE
    printf '%s' "$line_content" | grep -oE '\]\([^)]+\)' | sed 's/^\](//' | sed 's/)$//' | while IFS= read -r target; do
      # Skip external links, anchors, and mailto
      case "$target" in
        http://*|https://*|mailto:*|\#*) continue ;;
      esac

      # Strip anchor fragment from path (e.g., file.md#section -> file.md)
      link_path=$(printf '%s' "$target" | sed 's/#.*//')
      if [ -z "$link_path" ]; then
        continue
      fi

      # Resolve relative to the document's directory
      resolved="$doc_dir/$link_path"

      if [ ! -e "$resolved" ]; then
        printf "  \033[0;31m✗\033[0m %s:%s → %s (not found)\n" "$doc" "$line_num" "$target"
        # Count failures in a separate temp file because the grep loop runs in a subshell.
        printf "FAIL\n" >> "$FAILURE_LOG"
      fi
      printf "CHECK\n" >> "$COUNT_LOG"
    done
  done
done

# Count results from temp files
CHECKED=0
FAILURES=0
if [ -f "$COUNT_LOG" ]; then
  CHECKED=$(wc -l < "$COUNT_LOG" | tr -d ' ')
fi
if [ -f "$FAILURE_LOG" ]; then
  FAILURES=$(wc -l < "$FAILURE_LOG" | tr -d ' ')
fi

printf "\n\033[1mChecked %d links across %d docs\033[0m\n" "$CHECKED" "$(echo $DOCS | wc -w | tr -d ' ')"

if [ "$FAILURES" -gt 0 ]; then
  printf "\033[0;31m%d broken link(s) found\033[0m\n" "$FAILURES"
  exit 1
fi

printf "\033[0;32mAll links valid\033[0m\n"
exit 0
