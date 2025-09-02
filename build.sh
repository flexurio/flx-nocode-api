#!/bin/bash

targets=(
  # "x86_64-unknown-linux-gnu"
  # # "aarch64-unknown-linux-gnu"
  "x86_64-pc-windows-gnu"
  # "aarch64-pc-windows-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
)

export APPLE_ID='YOURIDBOSS'
export APPLE_TEAM_ID='YOURIDBOSS'
export PASSWORD='YOURIDBOSS'
export PRIMARY_BUNDLE_ID='YOURIDBOSS'
export APPLE_IDENTITY='YOURIDBOSS'
export APPLE_IDENTITY_INS='YOURIDBOSS'
export KEYCHAIN_PROFILE="YOURIDBOSS"


mkdir -p release

for target in "${targets[@]}"; do
  # check if target is not for apple
  echo "Building for $target..."
  echo "cargo build --release --target $target"
  cargo build --release --target "$target"
  
  # Tentukan ekstensi
  if [[ "$target" == *"windows"* ]]; then
    ext=".exe"
  else
    ext=""
  fi
  
  # Lokasi file output (asumsi nama default dari Cargo.toml adalah 'flx-nocode')
  default_output="target/$target/release/flx-nocode-api$ext"
  new_output="release/flx-nocode-$target$ext"
  
  # Rename
  if [ -f "$default_output" ]; then
    mv "$default_output" "$new_output"
    echo "Renamed to $new_output"
  else
    echo "Build failed for $target"
    continue
  fi

  # macOS signing and pkg creation
  if [[ "$target" == *"apple-darwin"* ]]; then
    echo "Signing and packaging $new_output for macOS..."

    # Ensure required tools are available
    if ! command -v codesign >/dev/null 2>&1; then
      echo "codesign not found. Xcode Command Line Tools are required. Skipping signing/packaging." >&2
      continue
    fi
    if ! command -v xcrun >/dev/null 2>&1; then
      echo "xcrun not found. Xcode is required. Skipping signing/packaging." >&2
      continue
    fi

    # codesign
    if [[ -z "${APPLE_IDENTITY:-}" ]]; then
      echo "APPLE_IDENTITY is not set. Skipping signing/packaging." >&2
      continue
    fi

    CODESIGN_ARGS=(--force --sign "$APPLE_IDENTITY" --options runtime --timestamp)
    if [[ -n "${ENTITLEMENTS_PATH:-}" ]] && [[ -f "${ENTITLEMENTS_PATH}" ]]; then
      CODESIGN_ARGS+=(--entitlements "$ENTITLEMENTS_PATH")
      echo "Using entitlements at ${ENTITLEMENTS_PATH}"
    fi

    echo "codesign ${CODESIGN_ARGS[*]} $new_output"
    if ! codesign "${CODESIGN_ARGS[@]}" "$new_output"; then
      echo "codesign failed for $new_output" >&2
      continue
    fi

    echo "Verifying signature..."
    codesign --verify --verbose=2 "$new_output" || { echo "codesign verify failed" >&2; continue; }
    spctl --assess --type execute --verbose "$new_output" || echo "spctl assessment non-fatal: may fail before notarization"

    # Build a signed .pkg installer for macOS targets
    echo "Building signed .pkg installer for $target..."
    if [[ -z "${APPLE_IDENTITY_INS:-}" ]]; then
      echo "APPLE_IDENTITY_INS is not set (Developer ID Installer). Skipping pkg build for $target." >&2
      continue
    fi

    pkg_staging_dir="release/pkg-$target"
    pkg_root="$pkg_staging_dir/root"
    install_bin_name="flx-nocode-api"
    mkdir -p "$pkg_root/usr/local/bin"
    cp "$new_output" "$pkg_root/usr/local/bin/$install_bin_name"
    chmod 755 "$pkg_root/usr/local/bin/$install_bin_name"

    pkg_identifier="${PRIMARY_BUNDLE_ID:-com.flexurio.api}"
    pkg_version="${PKG_VERSION:-1.0.0}"
    pkg_output="release/flx-nocode-$target.pkg"

    echo "Creating pkg: $pkg_output (id=$pkg_identifier, version=$pkg_version)"
    if ! pkgbuild --root "$pkg_root" \
        --install-location "/" \
        --identifier "$pkg_identifier" \
        --version "$pkg_version" \
        --sign "$APPLE_IDENTITY_INS" \
        "$pkg_output"; then
      echo "pkgbuild failed for $target" >&2
      rm -rf "$pkg_staging_dir"
      continue
    fi

    echo "Submitting PKG to Apple Notary Service..."
    PKG_NOTARY_ARGS_BASE=(submit "$pkg_output" --wait)
    if [[ -n "${PRIMARY_BUNDLE_ID:-}" ]]; then
      if xcrun notarytool submit --help 2>&1 | grep -q -- "--primary-bundle-id"; then
        PKG_NOTARY_ARGS_BASE+=(--primary-bundle-id "$PRIMARY_BUNDLE_ID")
      else
        echo "notarytool: --primary-bundle-id not supported on this Xcode version; skipping that flag."
      fi
    fi

    pkg_notar_success=0
    if [[ -n "${KEYCHAIN_PROFILE:-}" ]]; then
      PKG_FIRST_ARGS=("${PKG_NOTARY_ARGS_BASE[@]}" --keychain-profile "$KEYCHAIN_PROFILE")
      if xcrun notarytool "${PKG_FIRST_ARGS[@]}"; then
        pkg_notar_success=1
      else
        echo "Profile '$KEYCHAIN_PROFILE' failed; attempting direct Apple ID credentials if available..."
      fi
    fi

    if [[ $pkg_notar_success -eq 0 ]]; then
      APP_PW="${APPLE_APP_SPECIFIC_PASSWORD:-${PASSWORD:-}}"
      if [[ -n "${APPLE_ID:-}" && -n "${APPLE_TEAM_ID:-}" && -n "${APP_PW}" ]]; then
        PKG_SECOND_ARGS=("${PKG_NOTARY_ARGS_BASE[@]}" --apple-id "$APPLE_ID" --team-id "$APPLE_TEAM_ID" --password "$APP_PW")
        if xcrun notarytool "${PKG_SECOND_ARGS[@]}"; then
          pkg_notar_success=1
        fi
      fi
    fi

    if [[ $pkg_notar_success -ne 1 ]]; then
      echo "Notarization failed for $pkg_output" >&2
      rm -rf "$pkg_staging_dir"
      continue
    fi

    echo "Stapling notarization ticket to PKG..."
    pkg_staple_ok=0
    for i in 1 2 3; do
      if xcrun stapler staple "$pkg_output"; then
        pkg_staple_ok=1
        break
      fi
      echo "PKG staple attempt $i failed; retrying in 10s..."
      sleep 10
    done

    if [[ $pkg_staple_ok -ne 1 ]]; then
      echo "Stapling PKG failed (non-fatal). The PKG is notarized but may not carry a local ticket." >&2
    else
      echo "Stapling completed for $pkg_output"
    fi

    # Cleanup staging
    rm -rf "$pkg_staging_dir"
    echo "Cleaned staging: $pkg_staging_dir"

    echo "✅ PKG creation completed: $pkg_output"
  fi
done