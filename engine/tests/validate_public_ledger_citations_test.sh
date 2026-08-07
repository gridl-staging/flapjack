#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VALIDATOR="$SCRIPT_DIR/validate_public_ledger_citations.sh"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/flapjack-public-citations-test.XXXXXX")"

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_status() {
  local expected="$1"
  local actual="$2"
  local context="$3"
  [ "$actual" -eq "$expected" ] || fail "$context: expected exit $expected, got $actual"
}

assert_contains() {
  local output_file="$1"
  local expected="$2"
  local context="$3"
  grep -Fq -- "$expected" "$output_file" || {
    printf '%s\n' "--- $context output ---" >&2
    sed -n '1,160p' "$output_file" >&2
    fail "$context: missing expected text: $expected"
  }
}

assert_not_contains() {
  local output_file="$1"
  local unexpected="$2"
  local context="$3"
  if grep -Fq -- "$unexpected" "$output_file"; then
    printf '%s\n' "--- $context output ---" >&2
    sed -n '1,160p' "$output_file" >&2
    fail "$context: found unexpected text: $unexpected"
  fi
}

new_fixture_repo() {
  local name="$1"
  local repo="$WORK_DIR/$name"
  mkdir -p "$repo/engine/docs2/4_EVIDENCE" "$repo/engine/docs2/evidence/history" "$repo/docs" "$repo/chats" "$repo/chatting" "$repo/implemented"
  printf '%s' "$repo"
}

write_sync_files() {
  local repo="$1"
  shift

  {
    printf '[sync]\nfiles = [\n'
    local path
    for path in "$@"; do
      printf '    "%s",\n' "$path"
    done
    printf ']\n'
  } > "$repo/.debbie.toml"
}

run_validator() {
  local repo="$1"
  local output_file="$2"
  local status=0
  PUBLIC_LEDGER_REPO_DIR="$repo" bash "$VALIDATOR" > "$output_file" 2>&1 || status=$?
  printf '%s' "$status"
}

test_worktree_existence_does_not_imply_public_resolvability() {
  local repo
  repo="$(new_fixture_repo omitted_from_sync)"
  local receipt="engine/docs2/4_EVIDENCE/live_receipt.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf 'Proof: [%s](%s)\n' "$receipt" "$receipt" > "$repo/ROADMAP.md"
  write_sync_files "$repo" ROADMAP.md

  local output="$repo/unsynced.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'worktree-only citation'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=0 references_unresolvable=1 status=FAIL' 'worktree-only citation'
  assert_contains "$output" "ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=$receipt" 'worktree-only citation reason'

  write_sync_files "$repo" ROADMAP.md "$receipt"
  output="$repo/synced.log"
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'allowlisted citation'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'allowlisted citation'
}

test_vacuous_ledger_is_distinct_from_invalid_sync_configuration() {
  local repo
  repo="$(new_fixture_repo vacuous)"
  local receipt="engine/docs2/4_EVIDENCE/real_receipt.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf 'No repository citations here.\n' > "$repo/PROJECT_OVERVIEW.md"
  printf '[Receipt](%s)\n' "$receipt" > "$repo/ROADMAP.md"
  write_sync_files "$repo" PROJECT_OVERVIEW.md ROADMAP.md "$receipt"

  local output="$repo/vacuous.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'vacuous synced ledger'
  assert_contains "$output" 'ledger=PROJECT_OVERVIEW.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'vacuous synced ledger'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'non-vacuous sibling ledger'

  write_sync_files "$repo"
  output="$repo/empty_surface.log"
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'empty parsed sync surface'
  assert_contains "$output" 'parsed sync surface is empty' 'empty parsed sync surface'
  assert_not_contains "$output" 'status=VACUOUS' 'empty parsed sync surface'

  rm "$repo/.debbie.toml"
  output="$repo/missing_config.log"
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'missing sync config'
  assert_contains "$output" 'Missing .debbie.toml' 'missing sync config'
  assert_not_contains "$output" 'status=VACUOUS' 'missing sync config'
}

test_prose_paths_anchor_and_private_surfaces_are_checked() {
  local repo
  repo="$(new_fixture_repo anchoring)"
  local root_receipt="engine/docs2/4_EVIDENCE/root_receipt.md"
  local legacy_receipt="engine/docs2/evidence/history/summary.md"
  local feature_receipt="engine/docs2/4_EVIDENCE/feature_receipt.md"

  printf 'root receipt\n' > "$repo/$root_receipt"
  printf 'legacy receipt\n' > "$repo/$legacy_receipt"
  printf 'feature receipt\n' > "$repo/$feature_receipt"
  printf 'Private source cites docs/nested.md but must not be scanned.\n' > "$repo/docs/private_note.md"
  printf 'private chat\n' > "$repo/chats/private_thread.md"
  printf 'private closeout\n' > "$repo/chatting/private_closeout.md"
  printf 'private implementation\n' > "$repo/implemented/history.md"

  printf '%s\n' \
    "Root proof: \`$root_receipt\` and \`$legacy_receipt\`." \
    'Private context: docs/private_note.md, chats/private_thread.md, and chatting/private_closeout.md.' \
    'Skipped shapes: https://example.test/docs/remote.md, #docs/private_note.md, docs/**/*.md, and docs/<name>.md.' \
    > "$repo/ROADMAP.md"
  printf '%s\n' \
    "Feature proof: \`4_EVIDENCE/feature_receipt.md\`." \
    'Private history: implemented/history.md.' \
    > "$repo/engine/docs2/FEATURES.md"

  write_sync_files "$repo" ROADMAP.md engine/docs2/FEATURES.md "$root_receipt" "$legacy_receipt" "$feature_receipt"

  local output="$repo/anchoring.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'anchoring and private surfaces'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=5 references_resolvable=2 references_unresolvable=3 status=FAIL' 'root ledger anchoring'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md references_examined=2 references_resolvable=1 references_unresolvable=1 status=FAIL' 'nested ledger anchoring'
  assert_contains "$output" "ledger=ROADMAP.md result=RESOLVABLE path=$root_receipt" 'root evidence resolution detail'
  assert_contains "$output" "ledger=ROADMAP.md result=RESOLVABLE path=$legacy_receipt" 'legacy evidence resolution detail'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=docs/private_note.md' 'private docs extraction'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=chats/private_thread.md' 'private chats extraction'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=chatting/private_closeout.md' 'private chatting extraction'
  assert_contains "$output" "ledger=engine/docs2/FEATURES.md result=RESOLVABLE path=$feature_receipt" 'feature evidence resolution detail'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md result=UNRESOLVABLE reason=outside_sync_surface path=implemented/history.md' 'private implemented extraction'
  assert_not_contains "$output" 'remote.md' 'URL exclusion'
  assert_not_contains "$output" 'name>.md' 'template exclusion'
  assert_not_contains "$output" 'ledger=docs/private_note.md' 'private source ledger exclusion'
}

test_anchor_suffixed_citations_are_skipped() {
  local repo
  repo="$(new_fixture_repo anchor_suffixes)"
  local receipt="engine/docs2/4_EVIDENCE/anchor_positive.md"
  mkdir -p "$repo/engine/docs2/3_IMPLEMENTATION"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[Receipt](%s)\n' "$receipt" > "$repo/PROJECT_OVERVIEW.md"
  printf 'private note\n' > "$repo/docs/private_note.md"
  printf 'operations\n' > "$repo/engine/docs2/3_IMPLEMENTATION/OPERATIONS.md"
  printf '%s\n' \
    'Prose anchor: docs/private_note.md#details.' \
    'Markdown anchor: [Runbook](engine/docs2/3_IMPLEMENTATION/OPERATIONS.md#migration-jobs).' \
    > "$repo/ROADMAP.md"
  write_sync_files "$repo" PROJECT_OVERVIEW.md ROADMAP.md "$receipt"

  local output="$repo/anchor_suffixes.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'anchor-suffixed citations'
  assert_contains "$output" 'ledger=PROJECT_OVERVIEW.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'anchor-suffixed positive control'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'anchor-suffixed citations'
  assert_not_contains "$output" 'path=docs/private_note.md' 'prose anchor suffix exclusion'
  assert_not_contains "$output" 'path=engine/docs2/3_IMPLEMENTATION/OPERATIONS.md' 'markdown anchor suffix exclusion'
}

test_unreadable_synced_ledger_fails_closed() {
  local repo
  repo="$(new_fixture_repo unreadable_ledger)"
  printf 'This ledger should not be readable.\n' > "$repo/ROADMAP.md"
  chmod 000 "$repo/ROADMAP.md"
  write_sync_files "$repo" ROADMAP.md

  local output="$repo/unreadable.log"
  local status
  status="$(run_validator "$repo" "$output")"
  chmod 644 "$repo/ROADMAP.md"

  assert_status 1 "$status" 'unreadable synced ledger'
  assert_contains "$output" 'ERROR: failed to extract references from synced ledger: ROADMAP.md' 'unreadable synced ledger'
  assert_not_contains "$output" 'status=VACUOUS' 'unreadable synced ledger'
}

test_nul_byte_in_synced_ledger_fails_closed() {
  local repo
  repo="$(new_fixture_repo nul_byte_ledger)"
  local receipt="engine/docs2/4_EVIDENCE/nul_positive.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[Receipt](%s)\n' "$receipt" > "$repo/PROJECT_OVERVIEW.md"
  printf 'Hidden private citation: [Private](docs/private_note.md)\000\n' > "$repo/ROADMAP.md"
  write_sync_files "$repo" PROJECT_OVERVIEW.md ROADMAP.md "$receipt"

  local output="$repo/nul_byte.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'NUL-byte synced ledger'
  assert_contains "$output" 'ERROR: synced public ledger contains a NUL byte: ROADMAP.md' 'NUL-byte synced ledger'
  assert_not_contains "$output" 'ledger=ROADMAP.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'NUL-byte synced ledger'
}

test_scheme_qualified_urls_are_skipped_case_insensitively() {
  local repo
  repo="$(new_fixture_repo scheme_urls)"
  local receipt="engine/docs2/4_EVIDENCE/scheme_positive.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[Receipt](%s)\n' "$receipt" > "$repo/PROJECT_OVERVIEW.md"
  printf '%s\n' \
    'Uppercase URL: [Remote](HTTPS://example.test/docs/private_note.md).' \
    'FTP URL: [Archive](ftp://example.test/docs/archive.md).' \
    > "$repo/ROADMAP.md"
  write_sync_files "$repo" PROJECT_OVERVIEW.md ROADMAP.md "$receipt"

  local output="$repo/scheme_urls.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'scheme-qualified URLs'
  assert_contains "$output" 'ledger=PROJECT_OVERVIEW.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'scheme-qualified positive control'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'scheme-qualified URLs'
  assert_not_contains "$output" 'HTTPS:' 'uppercase URL exclusion'
  assert_not_contains "$output" 'ftp:' 'FTP URL exclusion'
}

test_all_vacuous_public_ledgers_fail_zero_denominator() {
  local repo
  repo="$(new_fixture_repo all_vacuous)"
  printf 'No citations here.\n' > "$repo/PROJECT_OVERVIEW.md"
  printf 'No citations here either.\n' > "$repo/ROADMAP.md"
  printf 'No citations in this change log.\n' > "$repo/CHANGELOG.md"
  printf 'No feature citations.\n' > "$repo/engine/docs2/FEATURES.md"
  write_sync_files "$repo" PROJECT_OVERVIEW.md ROADMAP.md CHANGELOG.md engine/docs2/FEATURES.md

  local output="$repo/all_vacuous.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'all-vacuous public ledgers'
  assert_contains "$output" 'ledger=PROJECT_OVERVIEW.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'all-vacuous overview ledger'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'all-vacuous roadmap ledger'
  assert_contains "$output" 'ledger=CHANGELOG.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'all-vacuous changelog ledger'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md references_examined=0 references_resolvable=0 references_unresolvable=0 status=VACUOUS' 'all-vacuous features ledger'
  assert_contains "$output" 'ERROR: zero public ledger references examined across synced ledgers' 'all-vacuous zero denominator error'
}

test_markdown_link_title_is_stripped_before_resolution() {
  local repo
  repo="$(new_fixture_repo markdown_title)"
  local receipt="engine/docs2/4_EVIDENCE/real.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[T](%s "Receipt")\n' "$receipt" > "$repo/ROADMAP.md"
  write_sync_files "$repo" ROADMAP.md "$receipt"

  local output="$repo/markdown_title.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'markdown title'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'markdown title'
  assert_contains "$output" "ledger=ROADMAP.md result=RESOLVABLE path=$receipt" 'markdown title resolution'
  assert_not_contains "$output" 'Receipt' 'markdown title excluded from path'
  assert_not_contains "$output" 'references_examined=2' 'markdown title not double-counted'
}

test_root_absolute_markdown_links_are_counted() {
  local repo
  repo="$(new_fixture_repo root_absolute)"
  local receipt="engine/docs2/4_EVIDENCE/root_absolute.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[Abs](/%s)\n' "$receipt" > "$repo/ROADMAP.md"
  write_sync_files "$repo" ROADMAP.md

  local output="$repo/root_absolute.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'root-absolute markdown link'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=0 references_unresolvable=1 status=FAIL' 'root-absolute markdown link'
  assert_contains "$output" "ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=$receipt" 'root-absolute markdown link resolution'
}

test_markdown_extractor_failure_propagates() {
  local output="$WORK_DIR/markdown_extractor_failure.log"
  local status=0

  (
    # shellcheck source=engine/tests/validate_public_ledger_citations.sh
    source "$VALIDATOR"
    # These overrides are invoked indirectly by public_ledger_extract_references.
    # shellcheck disable=SC2329
    doc_sync_extract_relative_markdown_links() {
      return 23
    }
    # shellcheck disable=SC2329
    public_ledger_extract_prose_references() {
      printf 'docs/prose_succeeds.md\n'
    }

    public_ledger_extract_references ROADMAP.md
  ) > "$output" 2>&1 || status=$?

  [ "$status" -ne 0 ] || fail 'markdown extractor failure: expected nonzero exit'
}

test_path_shaped_markdown_title_is_not_prose() {
  local repo
  repo="$(new_fixture_repo path_shaped_markdown_title)"
  local receipt="engine/docs2/4_EVIDENCE/real.md"
  printf 'receipt body\n' > "$repo/$receipt"
  printf '[T](%s "docs/private_note.md")\n' "$receipt" > "$repo/ROADMAP.md"
  printf 'private note\n' > "$repo/docs/private_note.md"
  write_sync_files "$repo" ROADMAP.md "$receipt"

  local output="$repo/path_shaped_markdown_title.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 0 "$status" 'path-shaped markdown title'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=1 references_resolvable=1 references_unresolvable=0 status=PASS' 'path-shaped markdown title'
  assert_contains "$output" "ledger=ROADMAP.md result=RESOLVABLE path=$receipt" 'path-shaped markdown title target'
  assert_not_contains "$output" 'path=docs/private_note.md' 'path-shaped markdown title excluded from prose'
}

test_private_planning_pp_prose_citations_are_extracted_and_unresolvable() {
  local repo
  repo="$(new_fixture_repo private_planning_pp)"
  local receipt="engine/docs2/4_EVIDENCE/pp_positive.md"
  printf 'receipt body\n' > "$repo/$receipt"
  mkdir -p "$repo/pp/supervise_notes"
  printf 'private supervise ledger\n' > "$repo/pp/supervise.md"
  printf 'private supervise note\n' > "$repo/pp/supervise_notes/flapjack_dev.md"
  printf '%s\n' \
    "Root proof: \`$receipt\`." \
    'Provenance: `pp/supervise_notes/flapjack_dev.md`, and registered in `pp/supervise.md`.' \
    > "$repo/ROADMAP.md"
  write_sync_files "$repo" ROADMAP.md "$receipt"

  local output="$repo/private_planning_pp.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'private planning pp citations'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=3 references_resolvable=1 references_unresolvable=2 status=FAIL' 'pp denominator'
  assert_contains "$output" "ledger=ROADMAP.md result=RESOLVABLE path=$receipt" 'pp positive control'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=pp/supervise_notes/flapjack_dev.md' 'pp nested note extraction'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=outside_sync_surface path=pp/supervise.md' 'pp root note extraction'
}

test_path_traversal_above_repo_root_is_unresolvable() {
  local repo
  repo="$(new_fixture_repo path_traversal)"
  printf '%s\n' \
    'Within root prose: docs/../ROADMAP.md' \
    'Above root prose: docs/../../ROADMAP.md' \
    > "$repo/ROADMAP.md"
  printf '%s\n' \
    '[Within root](../../ROADMAP.md)' \
    '[Above root](../../../../ROADMAP.md)' \
    > "$repo/engine/docs2/FEATURES.md"
  write_sync_files "$repo" ROADMAP.md engine/docs2/FEATURES.md

  local output="$repo/path_traversal.log"
  local status
  status="$(run_validator "$repo" "$output")"

  assert_status 1 "$status" 'path traversal above repository root'
  assert_contains "$output" 'ledger=ROADMAP.md references_examined=2 references_resolvable=1 references_unresolvable=1 status=FAIL' 'prose traversal denominator'
  assert_contains "$output" 'ledger=ROADMAP.md result=RESOLVABLE path=ROADMAP.md' 'within-root prose traversal'
  assert_contains "$output" 'ledger=ROADMAP.md result=UNRESOLVABLE reason=above_repository_root path=docs/../../ROADMAP.md' 'above-root prose traversal'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md references_examined=2 references_resolvable=1 references_unresolvable=1 status=FAIL' 'markdown traversal denominator'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md result=RESOLVABLE path=ROADMAP.md' 'within-root traversal'
  assert_contains "$output" 'ledger=engine/docs2/FEATURES.md result=UNRESOLVABLE reason=above_repository_root path=engine/docs2/../../../../ROADMAP.md' 'above-root traversal'
}

test_worktree_existence_does_not_imply_public_resolvability
test_vacuous_ledger_is_distinct_from_invalid_sync_configuration
test_prose_paths_anchor_and_private_surfaces_are_checked
test_anchor_suffixed_citations_are_skipped
test_unreadable_synced_ledger_fails_closed
test_nul_byte_in_synced_ledger_fails_closed
test_scheme_qualified_urls_are_skipped_case_insensitively
test_all_vacuous_public_ledgers_fail_zero_denominator
test_markdown_link_title_is_stripped_before_resolution
test_root_absolute_markdown_links_are_counted
test_markdown_extractor_failure_propagates
test_path_shaped_markdown_title_is_not_prose
test_private_planning_pp_prose_citations_are_extracted_and_unresolvable
test_path_traversal_above_repo_root_is_unresolvable

printf 'PASS: public ledger citation validator regression coverage\n'
