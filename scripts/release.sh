#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Flexurio Release Automation Script
#
# Usage:
#   ./scripts/release.sh patch               # Bump patch: 0.4.8 -> 0.4.9
#   ./scripts/release.sh minor               # Bump minor: 0.4.8 -> 0.5.0
#   ./scripts/release.sh major               # Bump major: 0.4.8 -> 1.0.0
#   ./scripts/release.sh 0.4.9               # Set explicit version
#   ./scripts/release.sh 0.4.9 --push        # Automatically push commit & tag
#   ./scripts/release.sh patch --dry-run     # Preview actions without executing
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CARGO_TOML="$ROOT_DIR/Cargo.toml"
CARGO_LOCK="$ROOT_DIR/Cargo.lock"

cd "$ROOT_DIR"

# Ensure git is available
if ! command -v git >/dev/null 2>&1; then
  echo "Error: git is required but not installed." >&2
  exit 1
fi

# Ensure Cargo.toml exists
if [ ! -f "$CARGO_TOML" ]; then
  echo "Error: Cargo.toml not found at $CARGO_TOML" >&2
  exit 1
fi

# Extract current version
CURRENT_VERSION=$(grep '^version' "$CARGO_TOML" | head -1 | awk -F '"' '{print $2}')
if [ -z "$CURRENT_VERSION" ]; then
  echo "Error: Unable to detect current version in $CARGO_TOML" >&2
  exit 1
fi

VERSION_ARG="${1:-}"
PUSH_FLAG=false
DRY_RUN=false

for arg in "$@"; do
  case "$arg" in
    --push|-p)
      PUSH_FLAG=true
      ;;
    --dry-run|-n)
      DRY_RUN=true
      ;;
  esac
done

if [ -z "$VERSION_ARG" ] || [ "$VERSION_ARG" = "--push" ] || [ "$VERSION_ARG" = "-p" ] || [ "$VERSION_ARG" = "--dry-run" ] || [ "$VERSION_ARG" = "-n" ]; then
  echo "Flexurio Release Automation"
  echo "Current version: $CURRENT_VERSION"
  echo ""
  echo "Usage:"
  echo "  $0 [patch|minor|major|<version>] [--push] [--dry-run]"
  echo ""
  echo "Examples:"
  echo "  $0 patch              # Bump patch version (e.g. $CURRENT_VERSION -> next patch)"
  echo "  $0 minor              # Bump minor version (e.g. $CURRENT_VERSION -> next minor)"
  echo "  $0 major              # Bump major version (e.g. $CURRENT_VERSION -> next major)"
  echo "  $0 0.4.9              # Set explicit version 0.4.9"
  echo "  $0 patch --push       # Bump and push to origin automatically"
  exit 0
fi

# Calculate new version
IFS="." read -r c_major c_minor c_patch <<< "$CURRENT_VERSION"
# Strip any pre-release suffix from patch for arithmetic
c_patch_num="${c_patch%%-*}"

case "$VERSION_ARG" in
  patch)
    NEW_VERSION="${c_major}.${c_minor}.$((c_patch_num + 1))"
    ;;
  minor)
    NEW_VERSION="${c_major}.$((c_minor + 1)).0"
    ;;
  major)
    NEW_VERSION="$((c_major + 1)).0.0"
    ;;
  *)
    # Strip optional leading 'v'
    NEW_VERSION="${VERSION_ARG#v}"
    ;;
esac

# Validate semver format
if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+.*$ ]]; then
  echo "Error: '$NEW_VERSION' is not a valid semantic version format (X.Y.Z)." >&2
  exit 1
fi

TAG_NAME="v${NEW_VERSION}-release"

echo "=============================================="
echo "Flexurio Release Preparation"
echo "=============================================="
echo "Current Version : $CURRENT_VERSION"
echo "New Version     : $NEW_VERSION"
echo "Git Tag         : $TAG_NAME"
echo "Dry Run         : $DRY_RUN"
echo "Auto Push       : $PUSH_FLAG"
echo "=============================================="

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
  echo ""
  echo "Notice: You have unstaged or uncommitted changes in your working tree:"
  git status --short
  echo ""
  read -r -p "Continue anyway? (y/N) " confirm
  if [[ ! "$confirm" =~ ^[yY]$ ]]; then
    echo "Aborted."
    exit 1
  fi
fi

# Check if git tag already exists
if git rev-parse "$TAG_NAME" >/dev/null 2>&1; then
  echo "Error: Tag '$TAG_NAME' already exists locally." >&2
  exit 1
fi

if git ls-remote --tags origin "$TAG_NAME" 2>/dev/null | grep -q "$TAG_NAME"; then
  echo "Error: Tag '$TAG_NAME' already exists on remote origin." >&2
  exit 1
fi

if [ "$DRY_RUN" = true ]; then
  echo ""
  echo "[Dry Run] Would update Cargo.toml to version = \"$NEW_VERSION\""
  echo "[Dry Run] Would update Cargo.lock"
  echo "[Dry Run] Would commit changes with message: 'chore(release): bump version to v$NEW_VERSION'"
  echo "[Dry Run] Would create annotated tag: '$TAG_NAME'"
  if [ "$PUSH_FLAG" = true ]; then
    echo "[Dry Run] Would push commit and tag '$TAG_NAME' to origin"
  fi
  echo ""
  echo "[Dry Run] Completed successfully."
  exit 0
fi

# 1. Update Cargo.toml
echo "1. Updating Cargo.toml version to $NEW_VERSION..."
perl -pi -e '$done ||= s/^version = ".*"/version = "'"$NEW_VERSION"'"/;' "$CARGO_TOML"

# 2. Update Cargo.lock
echo "2. Updating Cargo.lock..."
cargo check --quiet || true

# 3. Commit version bump
echo "3. Committing version bump..."
git add "$CARGO_TOML" "$CARGO_LOCK"
git commit -m "chore(release): bump version to v${NEW_VERSION}"

# 4. Create annotated tag
echo "4. Creating tag $TAG_NAME..."
git tag -a "$TAG_NAME" -m "Release $TAG_NAME"

echo ""
echo "Version successfully bumped to $NEW_VERSION and tagged with $TAG_NAME!"
echo ""

# 5. Push commit & tag
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$PUSH_FLAG" = true ]; then
  DO_PUSH=true
else
  read -r -p "Push commit and tag '$TAG_NAME' to origin ($CURRENT_BRANCH)? (Y/n) " push_confirm
  if [[ "$push_confirm" =~ ^[nN]$ ]]; then
    DO_PUSH=false
  else
    DO_PUSH=true
  fi
fi

if [ "$DO_PUSH" = true ]; then
  echo "Pushing commit to origin/$CURRENT_BRANCH..."
  git push origin "$CURRENT_BRANCH"
  echo "Pushing tag $TAG_NAME to origin..."
  git push origin "$TAG_NAME"
  echo ""
  echo "🚀 Successfully pushed to origin!"
  echo "GitHub Actions CI/CD has been triggered:"
  echo "  - Release build workflow : .github/workflows/release.yml"
  echo "  - Docker build & push    : .github/workflows/docker-publish.yml"
  echo "Check progress on GitHub Actions: https://github.com/flexurio/flx-nocode-api/actions"
else
  echo ""
  echo "Commit and tag created locally. When you are ready to publish, run:"
  echo "  git push origin $CURRENT_BRANCH"
  echo "  git push origin $TAG_NAME"
fi
