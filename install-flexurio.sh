#!/usr/bin/env bash

# Flexurio installer: downloads latest release binary, installs to ~/.local/bin,
# creates a wrapper command `flexurio`, and configures zsh environment.
# Tested on macOS (zsh). Should also work on Linux.

set -euo pipefail

REPO_OWNER="flexurio"
REPO_NAME="flx-nocode-api"
GITHUB_BASE="https://github.com/${REPO_OWNER}/${REPO_NAME}"

# Installation targets (override via env if needed)
INSTALL_BIN_DIR="${INSTALL_BIN_DIR:-$HOME/.local/bin}"

mkdir -p "$INSTALL_BIN_DIR"

log() { printf "[flexurio-install] %s\n" "$*"; }
err() { printf "[flexurio-install][ERROR] %s\n" "$*" 1>&2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { err "Command '$1' is required"; exit 1; }
}

need_cmd uname
need_cmd curl

OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Darwin) OS_TAG="apple-darwin" ;;
  Linux)  OS_TAG="unknown-linux-gnu" ;;
  *) err "Unsupported OS: $OS"; exit 1 ;;
 esac

case "$ARCH" in
  x86_64|amd64) ARCH_TAG="x86_64" ;;
  arm64|aarch64) ARCH_TAG="aarch64" ;;
  *) err "Unsupported architecture: $ARCH"; exit 1 ;;
 esac

# Expected asset name mapping based on available releases
if [ "$OS" = "Darwin" ]; then
  # macOS uses .pkg installers
  ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}.pkg"
  IS_PKG=1
else
  # Linux uses direct binaries
  ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}"
  IS_PKG=0
fi

TMPDIR="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "$TMPDIR/flexurio-install.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

if [ "$IS_PKG" = "1" ]; then
  PKG_TEMP="$WORK_DIR/$ASSET_NAME"
else
  BIN_TEMP="$WORK_DIR/$ASSET_NAME"
fi

log "Detecting platform: OS=$OS, ARCH=$ARCH -> asset=$ASSET_NAME"

# Detect user's shell and pick appropriate rc file(s)
USER_SHELL="${SHELL:-}"
SHELL_NAME="$(basename "$USER_SHELL" 2>/dev/null || echo unknown)"
RC_PRIMARY=""
RC_SECONDARY=""
case "$SHELL_NAME" in
  zsh)
    RC_PRIMARY="${RC_PRIMARY:-$HOME/.zshrc}"
    SHELL_DISPLAY="zsh"
    ;;
  bash)
    if [ "$OS" = "Darwin" ]; then
      # macOS Terminal often uses login shells for bash
      RC_PRIMARY="${RC_PRIMARY:-$HOME/.bash_profile}"
      RC_SECONDARY="${RC_SECONDARY:-$HOME/.bashrc}"
    else
      RC_PRIMARY="${RC_PRIMARY:-$HOME/.bashrc}"
    fi
    SHELL_DISPLAY="bash"
    ;;
  *)
    # Default to zshrc if unknown; user can customize via RC_PRIMARY env
    RC_PRIMARY="${RC_PRIMARY:-$HOME/.zshrc}"
    SHELL_DISPLAY="$SHELL_NAME"
    ;;
esac
log "Detected shell: ${SHELL_DISPLAY} -> rc: ${RC_PRIMARY}${RC_SECONDARY:+, $RC_SECONDARY}"

DOWNLOAD_URL_1="$GITHUB_BASE/releases/latest/download/${ASSET_NAME}"

if [ "$IS_PKG" = "1" ]; then
  log "Downloading latest macOS installer from: $DOWNLOAD_URL_1"
  DOWNLOAD_TARGET="$PKG_TEMP"
else
  log "Downloading latest binary from: $DOWNLOAD_URL_1"
  DOWNLOAD_TARGET="$BIN_TEMP"
fi

if ! curl -fSL "$DOWNLOAD_URL_1" -o "$DOWNLOAD_TARGET"; then
  log "Direct download failed. Trying to discover assets via GitHub API..."
  API_URL="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/latest"
  # Try to locate a matching asset name
  JSON="$WORK_DIR/release.json"
  if curl -fSL -H 'User-Agent: flexurio-installer' "$API_URL" -o "$JSON"; then
    # Very small POSIX JSON scrape to avoid jq dependency
    ASSET_URL=$(awk -v os="$OS_TAG" -v arch="$ARCH_TAG" '
      BEGIN{lc=0}
      /"browser_download_url"/ {
        match($0, /"browser_download_url" *: *"([^"]+)"/, m);
        if (m[1] ~ arch && m[1] ~ os) { print m[1]; lc=1; exit }
      }
      END{ if (lc==0) exit 1 }
    ' "$JSON" || true)
    if [ -n "$ASSET_URL" ]; then
      log "Found asset: $ASSET_URL"
      curl -fSL "$ASSET_URL" -o "$DOWNLOAD_TARGET"
    else
      err "Could not find a matching asset in the latest release."
      err "Please check the releases page: $GITHUB_BASE/releases"
      exit 1
    fi
  else
    err "Failed to query GitHub API for latest release."
    err "Please ensure network access and try again."
    exit 1
  fi
fi

if [ "$IS_PKG" = "1" ]; then
  # macOS: Install via .pkg
  log "Installing macOS package..."
  if ! sudo installer -pkg "$PKG_TEMP" -target /; then
    err "Package installation failed. Please check permissions and try again."
    exit 1
  fi
  log "Package installed successfully. flx-nocode-api is now available at /usr/local/bin/flx-nocode-api"
  
  # Create wrapper command
  WRAPPER_PATH="$INSTALL_BIN_DIR/flexurio"
  cat > "$WRAPPER_PATH" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail

log() { printf "[flexurio] %s\n" "$*"; }
err() { printf "[flexurio][ERROR] %s\n" "$*" 1>&2; }

# Load environment from current directory if .env exists (safe parser)
load_dotenv() {
  local line raw key val
  while IFS= read -r line || [ -n "$line" ]; do
    # Trim leading/trailing spaces
    raw="${line%%[[:space:]]*}"
    # Skip comments and empty lines
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    # Remove optional 'export '
    line=${line#export }
    # Only accept KEY=VALUE on a single line
    if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      key=${line%%=*}
      val=${line#*=}
      # Trim surrounding whitespace in val
      val="${val%%[[:space:]]*}"
      # Strip optional matching quotes
      if [[ "$val" =~ ^\".*\"$ ]]; then
        val=${val:1:${#val}-2}
      elif [[ "$val" =~ ^\'.*\'$ ]]; then
        val=${val:1:${#val}-2}
      fi
      export "$key=$val"
    fi
  done < .env
}

if [ -f ".env" ]; then
  load_dotenv
fi

# Sensible defaults if not set in .env
: "${LOC_LOGGING:=logs}"
: "${LOC_STATIC:=static}"

EXEC="/usr/local/bin/flx-nocode-api"

# Handle version command
if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ] || [ "${1:-}" = "version" ]; then
  if [ ! -x "$EXEC" ]; then
    err "flexurio wrapper: flx-nocode-api binary not found at $EXEC"
    exit 1
  fi
  exec "$EXEC" --version
fi

# Handle update command for macOS
do_update() {
  log "Checking for updates..."
  OS=$(uname -s)
  ARCH=$(uname -m)
  
  case "$ARCH" in
    x86_64|amd64) ARCH_TAG="x86_64" ;;
    arm64|aarch64) ARCH_TAG="aarch64" ;;
    *) err "Unsupported architecture: $ARCH"; exit 1 ;;
  esac
  
  ASSET_NAME="flx-nocode-${ARCH_TAG}-apple-darwin.pkg"
  REPO_OWNER="flexurio"; REPO_NAME="flx-nocode-api"
  DOWNLOAD_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/latest/download/${ASSET_NAME}"
  
  TMPDIR="${TMPDIR:-/tmp}"
  WORK_DIR="$(mktemp -d "$TMPDIR/flexurio-update.XXXXXX")"
  trap 'rm -rf "$WORK_DIR"' EXIT
  PKG_TEMP="$WORK_DIR/$ASSET_NAME"
  
  log "Downloading latest macOS installer: $ASSET_NAME"
  if ! curl -fSL "$DOWNLOAD_URL" -o "$PKG_TEMP"; then
    log "Direct download failed, trying GitHub API..."
    API_URL="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/latest"
    JSON="$WORK_DIR/release.json"
    if curl -fSL -H 'User-Agent: flexurio-updater' "$API_URL" -o "$JSON"; then
      ASSET_URL=$(awk -v pkg="$ASSET_NAME" '
        /"browser_download_url"/ {
          match($0, /"browser_download_url" *: *"([^"]+)"/, m);
          if (m[1] ~ pkg) { print m[1]; exit }
        }
      ' "$JSON" || true)
      [ -n "$ASSET_URL" ] || { err "No matching .pkg asset found"; exit 1; }
      curl -fSL "$ASSET_URL" -o "$PKG_TEMP"
    else
      err "Failed to fetch latest release metadata"; exit 1
    fi
  fi
  
  log "Installing update via package manager..."
  if ! sudo installer -pkg "$PKG_TEMP" -target /; then
    err "Package installation failed. Please check permissions and try again."
    exit 1
  fi
  
  log "Update completed successfully!"
  rm -rf "$WORK_DIR"
}

# Handle update command
if [ "${1:-}" = "--update" ] || [ "${1:-}" = "update" ] || [ "${1:-}" = "-U" ]; then
  do_update
  exit 0
fi

if [ ! -x "$EXEC" ]; then
  err "flexurio wrapper: flx-nocode-api binary not found at $EXEC"
  exit 1
fi

exec "$EXEC" "$@"
WRAP

  chmod +x "$WRAPPER_PATH"
  log "Installed wrapper command to: $WRAPPER_PATH"
  
else
  # Linux: Install binary directly
  chmod +x "$BIN_TEMP"

  # On macOS, remove quarantine attribute if present
  if [ "$OS" = "Darwin" ] && command -v xattr >/dev/null 2>&1; then
    xattr -dr com.apple.quarantine "$BIN_TEMP" || true
  fi

  # Install the core binary as `flx-nocode`
  TARGET_BIN_PATH="$INSTALL_BIN_DIR/flx-nocode"
  mv "$BIN_TEMP" "$TARGET_BIN_PATH"
  chmod +x "$TARGET_BIN_PATH"
  log "Installed core binary to: $TARGET_BIN_PATH"

  # Create wrapper command `flexurio` to ensure consistent env from current dir
  WRAPPER_PATH="$INSTALL_BIN_DIR/flexurio"
  cat > "$WRAPPER_PATH" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail

log() { printf "[flexurio] %s\n" "$*"; }
err() { printf "[flexurio][ERROR] %s\n" "$*" 1>&2; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || { err "Command '$1' is required"; exit 1; }; }

# Load environment from current directory if .env exists (safe parser)
load_dotenv() {
  local line raw key val
  while IFS= read -r line || [ -n "$line" ]; do
    # Trim leading/trailing spaces
    raw="${line%%[[:space:]]*}"
    # Skip comments and empty lines
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    # Remove optional 'export '
    line=${line#export }
    # Only accept KEY=VALUE on a single line
    if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
      key=${line%%=*}
      val=${line#*=}
      # Trim surrounding whitespace in val
      val="${val%%[[:space:]]*}"
      # Strip optional matching quotes
      if [[ "$val" =~ ^\".*\"$ ]]; then
        val=${val:1:${#val}-2}
      elif [[ "$val" =~ ^\'.*\'$ ]]; then
        val=${val:1:${#val}-2}
      fi
      export "$key=$val"
    fi
  done < .env
}

if [ -f ".env" ]; then
  load_dotenv
fi

# Sensible defaults if not set in .env
: "${LOC_LOGGING:=logs}"
: "${LOC_STATIC:=static}"

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXEC="$BIN_DIR/flx-nocode"
if [ ! -x "$EXEC" ]; then
  EXEC="$(command -v flx-nocode || true)"
fi

# Handle version command
if [ "${1:-}" = "--version" ] || [ "${1:-}" = "-V" ] || [ "${1:-}" = "version" ]; then
  if [ -z "$EXEC" ] || [ ! -x "$EXEC" ]; then
    err "flexurio wrapper: flx-nocode binary not found in PATH"
    exit 1
  fi
  exec "$EXEC" --version
fi

do_update() {
  need_cmd uname; need_cmd curl
  OS=$(uname -s)
  ARCH=$(uname -m)
  case "$OS" in
    Darwin) OS_TAG="apple-darwin" ;;
    Linux)  OS_TAG="unknown-linux-gnu" ;;
    *) err "Unsupported OS: $OS"; exit 1 ;;
  esac
  case "$ARCH" in
    x86_64|amd64) ARCH_TAG="x86_64" ;;
    arm64|aarch64) ARCH_TAG="aarch64" ;;
    *) err "Unsupported architecture: $ARCH"; exit 1 ;;
  esac
  
  if [ "$OS" = "Darwin" ]; then
    ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}.pkg"
    log "macOS update requires re-running the installer. Please download and install:"
    log "https://github.com/flexurio/flx-nocode-api/releases/latest/download/${ASSET_NAME}"
    exit 0
  else
    ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}"
  fi
  
  REPO_OWNER="flexurio"; REPO_NAME="flx-nocode-api"
  BASE_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}"

  TMPDIR="${TMPDIR:-/tmp}"
  WORK_DIR="$(mktemp -d "$TMPDIR/flexurio-update.XXXXXX")"
  trap 'rm -rf "$WORK_DIR"' EXIT
  BIN_TEMP="$WORK_DIR/$ASSET_NAME"

  URL1="$BASE_URL/releases/latest/download/${ASSET_NAME}"
  log "Updating binary: $ASSET_NAME"
  if ! curl -fSL "$URL1" -o "$BIN_TEMP"; then
    log "Direct download failed, trying GitHub API"
    API_URL="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/latest"
    JSON="$WORK_DIR/release.json"
    if curl -fSL -H 'User-Agent: flexurio-wrapper' "$API_URL" -o "$JSON"; then
      ASSET_URL=$(awk -v os="$OS_TAG" -v arch="$ARCH_TAG" '
        BEGIN{lc=0}
        /"browser_download_url"/ {
          match($0, /"browser_download_url" *: *"([^"]+)"/, m);
          if (m[1] ~ arch && m[1] ~ os && m[1] !~ /\.pkg$/) { print m[1]; lc=1; exit }
        }
        END{ if (lc==0) exit 1 }
      ' "$JSON" || true)
      [ -n "$ASSET_URL" ] || { err "No matching asset found"; exit 1; }
      curl -fSL "$ASSET_URL" -o "$BIN_TEMP"
    else
      err "Failed to fetch latest release metadata"; exit 1
    fi
  fi

  chmod +x "$BIN_TEMP"
  TARGET="${BIN_DIR}/flx-nocode"
  mv "$BIN_TEMP" "$TARGET"
  chmod +x "$TARGET"
  log "Updated: $TARGET"
  log "Done."
}

# Handle update command
if [ "${1:-}" = "--update" ] || [ "${1:-}" = "update" ] || [ "${1:-}" = "-U" ]; then
  do_update
  exit 0
fi

if [ -z "$EXEC" ] || [ ! -x "$EXEC" ]; then
  err "flexurio wrapper: flx-nocode binary not found in PATH"
  exit 1
fi

exec "$EXEC" "$@"
WRAP

  chmod +x "$WRAPPER_PATH"
  log "Installed wrapper command to: $WRAPPER_PATH"
fi

# Ensure PATH configured in the detected shell
SNIPPET_BEGIN="# >>> flexurio init >>>"
SNIPPET_END="# <<< flexurio init <<<"

patch_rc() {
  local rc_file="$1"
  if [ -z "$rc_file" ]; then return; fi
  if [ -f "$rc_file" ]; then
    if ! grep -q "flexurio init" "$rc_file"; then
      log "Patching $rc_file to add PATH and env loader"
      {
        echo "$SNIPPET_BEGIN"
        echo "export PATH=\"$INSTALL_BIN_DIR:\$PATH\""
        echo "$SNIPPET_END"
      } >> "$rc_file"
    else
      log "$rc_file already configured"
    fi
  else
    log "$rc_file not found; creating it with flexurio settings"
    {
      echo "$SNIPPET_BEGIN"
      echo "export PATH=\"$INSTALL_BIN_DIR:\$PATH\""
      echo "$SNIPPET_END"
    } >> "$rc_file"
  fi
}

patch_rc "$RC_PRIMARY"
if [ -n "${RC_SECONDARY}" ]; then
  patch_rc "$RC_SECONDARY"
fi

log "Installation complete!"
log "Next steps:"
if [ -n "$RC_SECONDARY" ]; then
  log "  1) Reload your shell: 'source \"$RC_PRIMARY\"' (and optionally 'source \"$RC_SECONDARY\"')"
else
  log "  1) Reload your shell: 'source \"$RC_PRIMARY\"'"
fi
log "  2) Start the server from anywhere: 'flexurio'"
