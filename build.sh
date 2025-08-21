#!/bin/bash
targets=(
  # "x86_64-unknown-linux-gnu"
  # # "aarch64-unknown-linux-gnu"
  "x86_64-pc-windows-gnu"
  # "aarch64-pc-windows-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
)

unset APPLE_APP_IDENTITY

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
  default_output="target/$target/release/flexurio-api-nocode-v2$ext"
  new_output="release/flx-nocode-$target$ext"
  
  # Rename
  if [ -f "$default_output" ]; then
    mv "$default_output" "$new_output"
    echo "Renamed to $new_output"
  else
    echo "Build failed for $target"
  fi
done