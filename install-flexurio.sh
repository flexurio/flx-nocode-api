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
FLEXURIO_HOME="${FLEXURIO_HOME:-$HOME/.flexurio}"

mkdir -p "$INSTALL_BIN_DIR" "$FLEXURIO_HOME"

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
ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}"

TMPDIR="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "$TMPDIR/flexurio-install.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

BIN_TEMP="$WORK_DIR/$ASSET_NAME"

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

log "Downloading latest binary from: $DOWNLOAD_URL_1"
if ! curl -fSL "$DOWNLOAD_URL_1" -o "$BIN_TEMP"; then
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
      curl -fSL "$ASSET_URL" -o "$BIN_TEMP"
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

# Create wrapper command `flexurio` to ensure consistent working dir and env
WRAPPER_PATH="$INSTALL_BIN_DIR/flexurio"
cat > "$WRAPPER_PATH" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail

log() { printf "[flexurio] %s\n" "$*"; }
err() { printf "[flexurio][ERROR] %s\n" "$*" 1>&2; }
need_cmd(){ command -v "$1" >/dev/null 2>&1 || { err "Command '$1' is required"; exit 1; }; }

FLEXURIO_HOME="${FLEXURIO_HOME:-$HOME/.flexurio}"
mkdir -p "$FLEXURIO_HOME" "$FLEXURIO_HOME/logs" "$FLEXURIO_HOME/static"

# Load per-user environment (exports vars)
if [ -f "$FLEXURIO_HOME/.env" ]; then
  set -a
  . "$FLEXURIO_HOME/.env"
  set +a
fi

# Sensible defaults if not set in .env
: "${LOC_LOGGING:=$FLEXURIO_HOME/logs}"
: "${LOC_STATIC:=$FLEXURIO_HOME/static}"

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXEC="$BIN_DIR/flx-nocode"
if [ ! -x "$EXEC" ]; then
  EXEC="$(command -v flx-nocode || true)"
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
  ASSET_NAME="flx-nocode-${ARCH_TAG}-${OS_TAG}"
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
          if (m[1] ~ arch && m[1] ~ os) { print m[1]; lc=1; exit }
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
  if [ "$OS" = "Darwin" ] && command -v xattr >/dev/null 2>&1; then
    xattr -dr com.apple.quarantine "$BIN_TEMP" || true
  fi

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

# Always run the server from FLEXURIO_HOME so its relative paths (db/, config/) are stable
cd "$FLEXURIO_HOME"

if [ -z "$EXEC" ] || [ ! -x "$EXEC" ]; then
  err "flexurio wrapper: flx-nocode binary not found in PATH"
  exit 1
fi

exec "$EXEC" "$@"
WRAP

chmod +x "$WRAPPER_PATH"
log "Installed wrapper command to: $WRAPPER_PATH"

# Create default .env if missing
ENV_FILE="$FLEXURIO_HOME/.env"
if [ ! -f "$ENV_FILE" ]; then
  log "Creating default environment at: $ENV_FILE"
  # Generate random secrets
  if command -v openssl >/dev/null 2>&1; then
    SECRET_KEY=$(openssl rand -hex 32)
    ENCRYPT_KEY=$(openssl rand -hex 32)
  else
    # Fallback: not cryptographically strong
    SECRET_KEY=$(date +%s | md5)
    ENCRYPT_KEY=$(date +%s | shasum | cut -d' ' -f1)
  fi

  cat > "$ENV_FILE" <<EOF
# Flexurio environment
DEBUG=false
LOGGING=true
PORT=8080
UPLOAD_LIMIT_MB=10

# Storage locations
LOC_LOGGING="$FLEXURIO_HOME/logs"
LOC_STATIC="$FLEXURIO_HOME/static"
LOC_IMAGE=DB

# Database (default: SQLite)
DB_TYPE=sqlite
SQLITE_URL="sqlite://$FLEXURIO_HOME/data.db"
# For MySQL or Postgres, set instead:
# DB_TYPE=mysql
# MYSQL_URL="mysql://user:password@localhost:3306/dbname"
# DB_TYPE=postgres
# POSTGRES_URL="postgres://user:password@localhost:5432/dbname"

# API
CORS_ALLOW_ORIGINS=*

# Security
SECRET_KEY="$SECRET_KEY"
ENCRYPT_KEY="$ENCRYPT_KEY"

# Config location (default example copied by installer below)
LOC_CONFIG="$FLEXURIO_HOME/config/example"
EOF
fi

# Ensure PATH configured and auto-load .env in the detected shell
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
        echo "export FLEXURIO_HOME=\"$FLEXURIO_HOME\""
        echo "export PATH=\"$INSTALL_BIN_DIR:\$PATH\""
        echo "if [ -f \"$FLEXURIO_HOME/.env\" ]; then set -a; . \"$FLEXURIO_HOME/.env\"; set +a; fi"
        echo "$SNIPPET_END"
      } >> "$rc_file"
    else
      log "$rc_file already configured"
    fi
  else
    log "$rc_file not found; creating it with flexurio settings"
    {
      echo "$SNIPPET_BEGIN"
      echo "export FLEXURIO_HOME=\"$FLEXURIO_HOME\""
      echo "export PATH=\"$INSTALL_BIN_DIR:\$PATH\""
      echo "if [ -f \"$FLEXURIO_HOME/.env\" ]; then set -a; . \"$FLEXURIO_HOME/.env\"; set +a; fi"
      echo "$SNIPPET_END"
    } >> "$rc_file"
  fi
}

patch_rc "$RC_PRIMARY"
if [ -n "${RC_SECONDARY}" ]; then
  patch_rc "$RC_SECONDARY"
fi

# Download example config into FLEXURIO_HOME if missing
if [ ! -d "$FLEXURIO_HOME/config/example" ]; then
  log "Fetching example config into $FLEXURIO_HOME/config/example"
  mkdir -p "$FLEXURIO_HOME/config"
  ARCHIVE="$WORK_DIR/repo.zip"
  curl -fSL "$GITHUB_BASE/archive/refs/heads/main.zip" -o "$ARCHIVE"
  # Unzip only the config folder
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "$ARCHIVE" -d "$WORK_DIR"
    # Find extracted folder (name ends with repo name + branch)
    SRC_DIR=$(find "$WORK_DIR" -maxdepth 1 -type d -name "${REPO_NAME}-main" -print -quit)
    if [ -n "$SRC_DIR" ] && [ -d "$SRC_DIR/config/example" ]; then
      cp -R "$SRC_DIR/config/example" "$FLEXURIO_HOME/config/"
    else
      err "Could not locate config/example in repository archive"
    fi
  else
    err "'unzip' not found; skipping example config extraction"
  fi
fi

log "Installation complete!"
log "Next steps:"
if [ -n "$RC_SECONDARY" ]; then
  log "  1) Reload your shell: 'source \"$RC_PRIMARY\"' (and optionally 'source \"$RC_SECONDARY\"')"
else
  log "  1) Reload your shell: 'source \"$RC_PRIMARY\"'"
fi
log "  2) Start the server from anywhere: 'flexurio'"
log "     It will run in $FLEXURIO_HOME and keep its db/config/logs there."
