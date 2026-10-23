#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Sinkronkan file VERSION ke Cargo.toml, buat tag rilis, lalu push.
#
# Usage:
#   ./scripts/sync-version.sh             # sync + commit + tag + push
#   ./scripts/sync-version.sh --dry-run   # tampilkan rencana saja
#   ./scripts/sync-version.sh --retag     # pindahkan tag yang sudah ada ke commit
#                                         # terbaru (force push tag ke origin)
#
# Tag yang dibuat: v<VERSION>-release (sama dengan scripts/release.sh), yang
# memicu .github/workflows/release.yml dan docker-publish.yml.
# -----------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DRY_RUN=false
RETAG=false
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=true ;;
    --retag) RETAG=true ;;
    -h|--help) sed -n '4,15p' "$0"; exit 0 ;;
    *) echo "Error: argumen tidak dikenal '$arg'" >&2; exit 1 ;;
  esac
done

run() {
  if [ "$DRY_RUN" = true ]; then
    echo "  [dry-run] $*"
  else
    "$@"
  fi
}

[ -f VERSION ] || { echo "Error: file VERSION tidak ditemukan." >&2; exit 1; }
VERSION="$(tr -d '[:space:]' < VERSION)"
VERSION="${VERSION#v}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Error: VERSION '$VERSION' bukan semver (X.Y.Z)." >&2
  exit 1
fi

PKG_NAME="$(grep -m1 '^name' Cargo.toml | awk -F '"' '{print $2}')"
CARGO_VERSION="$(grep -m1 '^version' Cargo.toml | awk -F '"' '{print $2}')"
TAG="v${VERSION}-release"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"

echo "=============================================="
echo "VERSION       : $VERSION"
echo "Cargo.toml    : $CARGO_VERSION"
echo "Tag           : $TAG"
echo "Branch        : $BRANCH"
echo "Dry run       : $DRY_RUN"
echo "=============================================="

# Hanya file versi yang boleh berubah, supaya commit rilis tidak ikut membawa
# pekerjaan lain yang belum selesai.
OTHER_CHANGES="$(git status --porcelain | grep -vE '^.. (VERSION|Cargo\.toml|Cargo\.lock)$' || true)"
if [ -n "$OTHER_CHANGES" ]; then
  echo "Error: ada perubahan lain di working tree. Commit/stash dulu:" >&2
  echo "$OTHER_CHANGES" >&2
  exit 1
fi

# Cek tag di lokal & remote.
LOCAL_TAG=false; REMOTE_TAG=false
git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && LOCAL_TAG=true
git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1 && REMOTE_TAG=true
if { [ "$LOCAL_TAG" = true ] || [ "$REMOTE_TAG" = true ]; } && [ "$RETAG" = false ]; then
  echo "Error: tag '$TAG' sudah ada (lokal=$LOCAL_TAG, remote=$REMOTE_TAG)." >&2
  echo "Naikkan VERSION, atau jalankan dengan --retag untuk memindahkan tag." >&2
  exit 1
fi

# 1. Sync Cargo.toml (baris version pertama = [package]).
if [ "$CARGO_VERSION" != "$VERSION" ]; then
  echo "1. Cargo.toml: $CARGO_VERSION -> $VERSION"
  run perl -pi -e '$done ||= s/^version = ".*"/version = "'"$VERSION"'"/;' Cargo.toml
else
  echo "1. Cargo.toml sudah $VERSION"
fi

# 2. Sync Cargo.lock tanpa compile.
echo "2. Update Cargo.lock"
run cargo update -p "$PKG_NAME" --offline --quiet

# 3. Commit bila ada perubahan.
echo "3. Commit"
run git add VERSION Cargo.toml Cargo.lock
if [ "$DRY_RUN" = true ] || ! git diff --cached --quiet; then
  run git commit -m "chore(release): bump version to v${VERSION}"
else
  echo "  tidak ada perubahan untuk di-commit"
fi

# 4. Tag.
echo "4. Tag $TAG"
if [ "$LOCAL_TAG" = true ]; then
  run git tag -d "$TAG"
fi
run git tag -a "$TAG" -m "Release $TAG"

# 5. Push branch lalu tag.
echo "5. Push ke origin"
run git push origin "$BRANCH"
if [ "$REMOTE_TAG" = true ]; then
  run git push --force origin "refs/tags/$TAG"
else
  run git push origin "refs/tags/$TAG"
fi

echo ""
echo "Selesai: $TAG dipush. Pantau CI: https://github.com/flexurio/flx-nocode-api/actions"
