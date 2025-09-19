#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Flexurio Multi-Target Builder (with DB feature selection)
# -----------------------------------------------------------------------------
# Usage examples:
#   ./build.sh                          # build all DB variants for all OS targets
#   ./build.sh --db mysql               # only mysql for all OS targets
#   ./build.sh --db mysql,sqlite        # mysql + sqlite for all OS targets
#   ./build.sh --db all --os macos      # all DBs only for macOS (both arch)
#   ./build.sh --os macos,windows --db postgres
#   ./build.sh --help                   # show help
#
# Output artifacts are placed under ./release named as:
#   flx-nocode-<driver>-<target>[.exe]
# For macOS targets, a signed & notarized pkg is produced (if env variables set):
#   flx-nocode-<driver>-<target>.pkg
# -----------------------------------------------------------------------------

# NOTE: For security, DO NOT hardcode Apple credentials here. Export them in your shell or CI environment instead, e.g.:
#   export APPLE_ID="you@example.com"
#   export APPLE_TEAM_ID="TEAMID"
#   export APPLE_APP_SPECIFIC_PASSWORD="xxxx-xxxx-xxxx-xxxx"
#   export APPLE_IDENTITY="Developer ID Application: Company (TEAMID)"
#   export APPLE_IDENTITY_INS="Developer ID Installer: Company (TEAMID)"
#   export PRIMARY_BUNDLE_ID="com.company.app"
#   export KEYCHAIN_PROFILE="notary-profile"

if [ -z "${BASH_VERSION:-}" ]; then
  echo "This script requires bash. Run as ./build.sh or bash build.sh (not sh)." >&2
  exit 1
fi


show_help() {
  cat <<'EOF'
Flexurio build script

Flags:
  --db <list>    Comma separated database drivers (mysql,postgres,sqlite,all). Default: all
  --os <list>    Comma separated OS targets (macos,windows,linux,all). Default: all
  --arch <list>  Comma separated arch list (x86_64,aarch64,all). Filters expanded targets. Default: all
  --help         Show this help

OS Expansions:
  macos   => x86_64-apple-darwin,aarch64-apple-darwin
  windows => x86_64-pc-windows-gnu
  linux   => x86_64-unknown-linux-gnu,aarch64-unknown-linux-gnu

Arch Filter:
  After expansion you can restrict to `--arch x86_64` or `--arch aarch64`.

Examples:
  ./build.sh --db mysql --os macos
  ./build.sh --db mysql --os macos --arch aarch64
  ./build.sh --db postgres,sqlite --os linux --arch aarch64
  ./build.sh --db all --os macos,windows --arch x86_64
  ./build.sh --db mysql,sqlite --os macos,linux --arch x86_64


Environment (macOS signing): APPLE_ID, APPLE_TEAM_ID, PASSWORD/APPLE_APP_SPECIFIC_PASSWORD,
  APPLE_IDENTITY, APPLE_IDENTITY_INS, PRIMARY_BUNDLE_ID, KEYCHAIN_PROFILE
EOF
}

DB_LIST="all"
OS_LIST="all"
ARCH_LIST="all"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      DB_LIST="$2"; shift 2 ;;
    --os)
      OS_LIST="$2"; shift 2 ;;
    --arch)
      ARCH_LIST="$2"; shift 2 ;;
    --help|-h)
      show_help; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; show_help; exit 1 ;;
  esac
done

# Resolve drivers
if [[ "$DB_LIST" == "all" ]]; then
  DRIVERS=(mysql postgres sqlite)
else
  IFS=',' read -r -a DRIVERS <<<"$DB_LIST"
fi
for d in "${DRIVERS[@]}"; do
  case "$d" in
    mysql|postgres|sqlite) ;;
    *) echo "Unknown driver: $d" >&2; exit 1 ;;
  esac
done

# Resolve OS targets
declare -a targets
expand_os() {
  case "$1" in
    macos)   echo "x86_64-apple-darwin aarch64-apple-darwin" ;;
    windows) echo "x86_64-pc-windows-gnu" ;;
    linux)   echo "x86_64-unknown-linux-gnu" ;;
    *) echo "" ;;
  esac
}

if [[ "$OS_LIST" == "all" ]]; then
  targets=(x86_64-pc-windows-gnu x86_64-apple-darwin aarch64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
else
  IFS=',' read -r -a OS_ARR <<<"$OS_LIST"
  for osname in "${OS_ARR[@]}"; do
    case "$osname" in
      macos|windows|linux) ;;
      *) echo "Unknown OS: $osname" >&2; exit 1 ;;
    esac
    for tgt in $(expand_os "$osname"); do
      # Deduplicate manually (portable across old bash)
      already=0
      for existing in "${targets[@]:-}"; do
        if [[ "$existing" == "$tgt" ]]; then
          already=1; break
        fi
      done
      if [[ $already -eq 0 ]]; then
        targets+=("$tgt")
      fi
    done
  done
fi

# Architecture filtering
if [[ "$ARCH_LIST" != "all" ]]; then
  IFS=',' read -r -a ARCH_ARR <<<"$ARCH_LIST"
  # Validate arch names
  for a in "${ARCH_ARR[@]}"; do
    case "$a" in
      x86_64|aarch64) ;;
      *) echo "Unknown arch: $a" >&2; exit 1 ;;
    esac
  done
  filtered=()
  for tgt in "${targets[@]}"; do
    case "$tgt" in
      x86_64-*) arch_prefix="x86_64" ;;
      aarch64-*) arch_prefix="aarch64" ;;
      *) arch_prefix="" ;;
    esac
    keep=0
    for a in "${ARCH_ARR[@]}"; do
      if [[ "$a" == "$arch_prefix" ]]; then
        keep=1; break
      fi
    done
    if [[ $keep -eq 1 ]]; then
      filtered+=("$tgt")
    fi
  done
  targets=(${filtered[@]})
fi

if [[ ${#targets[@]} -eq 0 ]]; then
  echo "No targets resolved" >&2; exit 1
fi

mkdir -p release

build_variant() {
  local driver="$1" target="$2"
  echo "============================="
  echo "Building driver=$driver target=$target"
  echo "============================="

  local features_flag
  case "$driver" in
    mysql) features_flag="--no-default-features --features mysql" ;;
    postgres) features_flag="--no-default-features --features postgres" ;;
    sqlite) features_flag="--no-default-features --features sqlite" ;;
  esac

  echo "cargo build --release $features_flag --target $target"
  cargo build --release $features_flag --target "$target"

  local ext=""; [[ "$target" == *"windows"* ]] && ext=".exe"
  local src="target/$target/release/flx-nocode-api$ext"
  local dst="release/flx-nocode-${driver}-$target$ext"
  if [[ -f "$src" ]]; then
    mv "$src" "$dst"
    echo "Artifact: $dst"
  else
    echo "Build failed for $driver / $target" >&2
    return 1
  fi

  if [[ "$target" == *"apple-darwin"* ]]; then
    sign_and_pkg "$driver" "$target" "$dst"
  fi
}

sign_and_pkg() {
  local driver="$1" target="$2" bin_path="$3"
  echo "-- macOS signing & packaging ($driver / $target) --"

  if ! command -v codesign >/dev/null 2>&1; then
    echo "codesign not found, skipping signing"; return
  fi
  if ! command -v xcrun >/dev/null 2>&1; then
    echo "xcrun not found, skipping notarization"; return
  fi
  if [[ -z "${APPLE_IDENTITY:-}" ]]; then
    echo "APPLE_IDENTITY not set, skipping signing"; return
  fi

  local CODESIGN_ARGS=(--force --sign "$APPLE_IDENTITY" --options runtime --timestamp)
  if [[ -n "${ENTITLEMENTS_PATH:-}" && -f "${ENTITLEMENTS_PATH}" ]]; then
    CODESIGN_ARGS+=(--entitlements "$ENTITLEMENTS_PATH")
    echo "Using entitlements: $ENTITLEMENTS_PATH"
  fi
  echo "codesign ${CODESIGN_ARGS[*]} $bin_path"
  if ! codesign "${CODESIGN_ARGS[@]}" "$bin_path"; then
    echo "codesign failed" >&2; return
  fi
  codesign --verify --verbose=2 "$bin_path" || echo "codesign verify warning"
  spctl --assess --type execute --verbose "$bin_path" || echo "spctl pre‑notary warning"

  if [[ -z "${APPLE_IDENTITY_INS:-}" ]]; then
    echo "APPLE_IDENTITY_INS not set, skip pkg build"; return
  fi

  local pkg_root="release/pkg-${driver}-${target}/root"
  local install_bin_name="flx-nocode-api"
  mkdir -p "$pkg_root/usr/local/bin"
  cp "$bin_path" "$pkg_root/usr/local/bin/$install_bin_name"
  chmod 755 "$pkg_root/usr/local/bin/$install_bin_name"

  local pkg_identifier="${PRIMARY_BUNDLE_ID:-com.flexurio.api}.${driver}"
  local pkg_version="${PKG_VERSION:-1.0.0}"
  local pkg_output="release/flx-nocode-${driver}-${target}.pkg"
  echo "Creating pkg: $pkg_output"
  if ! pkgbuild --root "$pkg_root" \
      --install-location "/" \
      --identifier "$pkg_identifier" \
      --version "$pkg_version" \
      --sign "$APPLE_IDENTITY_INS" \
      "$pkg_output"; then
    echo "pkgbuild failed" >&2
    rm -rf "release/pkg-${driver}-${target}"
    return
  fi

  echo "Submitting for notarization..."
  local BASE_ARGS=(submit "$pkg_output" --wait)
  if xcrun notarytool submit --help 2>&1 | grep -q -- "--primary-bundle-id" && [[ -n "${PRIMARY_BUNDLE_ID:-}" ]]; then
    BASE_ARGS+=(--primary-bundle-id "$PRIMARY_BUNDLE_ID")
  fi

  local notar_ok=0
  if [[ -n "${KEYCHAIN_PROFILE:-}" ]]; then
    if xcrun notarytool "${BASE_ARGS[@]}" --keychain-profile "$KEYCHAIN_PROFILE"; then
      notar_ok=1
    fi
  fi
  if [[ $notar_ok -eq 0 ]]; then
    local APP_PW="${APPLE_APP_SPECIFIC_PASSWORD:-${PASSWORD:-}}"
    if [[ -n "${APPLE_ID:-}" && -n "${APPLE_TEAM_ID:-}" && -n "$APP_PW" ]]; then
      if xcrun notarytool "${BASE_ARGS[@]}" --apple-id "$APPLE_ID" --team-id "$APPLE_TEAM_ID" --password "$APP_PW"; then
        notar_ok=1
      fi
    fi
  fi
  if [[ $notar_ok -ne 1 ]]; then
    echo "Notarization failed (continuing without pkg stapling)" >&2
    return
  fi

  echo "Stapling pkg..."
  if ! xcrun stapler staple "$pkg_output"; then
    echo "Staple failed (non-fatal)" >&2
  fi

  rm -rf "release/pkg-${driver}-${target}"
  echo "✅ Completed pkg: $pkg_output"
}

for driver in "${DRIVERS[@]}"; do
  for target in "${targets[@]}"; do
    build_variant "$driver" "$target"
  done
done

echo "All builds finished. Artifacts in ./release"