#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/ui.sh"
source "$SCRIPT_DIR/lib/version.sh"

GH_REPO="gridl-hq/flapjack"

LATEST_TAG=$(latest_release_version "$GH_REPO")
AUTO_VERSION=$(auto_bump_version "$GH_REPO" "beta" "$LATEST_TAG")
VERSION="${1:-$AUTO_VERSION}"

banner "Trigger Release" "v${VERSION}"

kv "Latest" "v${LATEST_TAG}"
kv "New" "v${VERSION}"
echo ""

spin_start "Triggering release workflow"
gh workflow run release.yml --repo "$GH_REPO" -f version="$VERSION"
spin_stop success "Release workflow triggered for v${VERSION}"
echo ""

next_steps \
  "gh run watch --repo $GH_REPO" \
  "gh run list --repo $GH_REPO --workflow=release.yml --limit=1"
echo ""
