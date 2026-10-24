#!/bin/bash
# -----------------------------------------------------------------------------
# Upload signed & notarized macOS installers to GitHub Release.
#
# Usage:
#   ./deploy_apple.sh                       # upload release/flx-nocode-*-apple-darwin.pkg
#   ./deploy_apple.sh release/a.pkg ...     # upload specific assets
#   RELEASE_TAG=v1.2.3-release ./deploy_apple.sh
#
# Called automatically by build_apple.sh after a successful build
# (set SKIP_UPLOAD=1 on build_apple.sh to skip).
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Resolve version & release tag (same convention as .github/workflows/release.yml)
APP_VERSION="$(grep -m1 -E '^version[[:space:]]*=' Cargo.toml | sed -E 's/.*"([^"]+)".*/\1/')"
if [[ -z "$APP_VERSION" ]]; then
  echo "Failed to read version from Cargo.toml" >&2
  exit 1
fi
RELEASE_TAG="${RELEASE_TAG:-v${APP_VERSION}-release}"

# Collect assets: from args, or default macOS pkgs in release/
assets=()
if [[ $# -gt 0 ]]; then
  assets=("$@")
else
  shopt -s nullglob
  assets=(release/flx-nocode-*-apple-darwin.pkg)
  shopt -u nullglob
fi

if [[ ${#assets[@]} -eq 0 ]]; then
  echo "No macOS assets found to upload. Run ./build_apple.sh first." >&2
  exit 1
fi
for asset in "${assets[@]}"; do
  if [[ ! -f "$asset" ]]; then
    echo "Asset not found: $asset" >&2
    exit 1
  fi
done

# Tooling checks
if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI (gh) not found. Install with: brew install gh" >&2
  exit 1
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "gh is not authenticated. Run: gh auth login" >&2
  exit 1
fi

# Checksums for the uploaded macOS assets
mkdir -p release
checksum_file="release/checksums-macos-${APP_VERSION}.sha256"
: > "$checksum_file"
for asset in "${assets[@]}"; do
  ( cd "$(dirname "$asset")" && shasum -a 256 "$(basename "$asset")" ) >> "$checksum_file"
done
upload_assets=("${assets[@]}" "$checksum_file")

echo "Uploading to GitHub release $RELEASE_TAG:"
printf '  - %s\n' "${upload_assets[@]}"

if gh release view "$RELEASE_TAG" >/dev/null 2>&1; then
  gh release upload "$RELEASE_TAG" "${upload_assets[@]}" --clobber || {
    echo "Upload to existing release $RELEASE_TAG failed" >&2
    exit 1
  }
else
  # Release not found: create it (gh also creates the tag from current HEAD if it doesn't exist on remote)
  gh release create "$RELEASE_TAG" "${upload_assets[@]}" \
    --title "$RELEASE_TAG" \
    --target "$(git rev-parse HEAD)" \
    --generate-notes || {
    echo "Failed to create release $RELEASE_TAG" >&2
    exit 1
  }
fi

echo "✅ Uploaded ${#upload_assets[@]} asset(s) to release $RELEASE_TAG"
gh release view "$RELEASE_TAG" --json url -q .url 2>/dev/null || true
