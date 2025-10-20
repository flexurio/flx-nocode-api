#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--dry-run] [path-to-.env]"
  echo "Example: $0 --dry-run .env"
}

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi

ENV_FILE="${1:-.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found at: $ENV_FILE"
  usage
  exit 1
fi

# Detect cores (prefer physical)
OS="$(uname -s)"
logical=""
physical=""

if [[ "$OS" == "Darwin" ]]; then
  logical="$(sysctl -n hw.ncpu || true)"
  physical="$(sysctl -n hw.physicalcpu || true)"
else
  if command -v nproc >/dev/null 2>&1; then
    logical="$(nproc --all || true)"
  else
    logical="$(getconf _NPROCESSORS_ONLN || true)"
  fi
  # Try to compute physical cores on Linux
  if command -v lscpu >/dev/null 2>&1; then
    physical="$(lscpu | awk '
      /^Core\(s\) per socket:/ {c=$4}
      /^Socket\(s\):/ {s=$2}
      END{ if(c && s) print c*s; }'
    )"
  fi
fi

# Fallbacks
if [[ -z "${physical:-}" || "$physical" -lt 1 ]]; then
  physical="$logical"
fi
if [[ -z "${logical:-}" || "$logical" -lt 1 ]]; then
  echo "Error: unable to detect CPU cores."
  exit 1
fi

# Clamp helpers
clamp() {
  local val="$1" min="$2" max="$3"
  if (( val < min )); then echo "$min"
  elif (( val > max )); then echo "$max"
  else echo "$val"
  fi
}

# Heuristik rekomendasi (IO/DB-bound services):
# - workers: ~physical cores (min 2, maks 8 untuk dev/host single)
# - write_conc: ~workers (min 2, maks 8)
# - db pool: workers * 32 (min 64, maks 256)
# - http conn rate: workers * 64 (min 128, maks 512)
# workers = workers_raw - 2
workers=$(( physical - 2 ))
db_pool_raw=$(( workers * 32 ))
db_pool="$(clamp "$db_pool_raw" 64 256 )"
http_rate_raw=$(( workers * 64 ))
http_rate="$(clamp "$http_rate_raw" 128 512 )"

# Constants we keep
keepalive="30"
connect_timeout="15"

echo "Detected cores: physical=$physical logical=$logical"
echo "Proposed values:"
echo "  physical=$physical"
echo "  ACTIX_WORKERS=$workers"
echo "  WRITE_CONCURRENCY=$workers"
echo "  MAX_POOL=$db_pool"
echo "  HTTP_MAX_CONN_RATE=$http_rate"
echo "  HTTP_KEEPALIVE_SECS=$keepalive"
echo "  CONNECT_TIMEOUT=$connect_timeout"
