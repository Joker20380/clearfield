#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

BASE="$CLEARFIELD_PROJECT_DIR"
LOG="$BASE/logs/dzagurov_news_sync.panel.log"

cd "$BASE"

{
  echo
  echo "[$(date -Is)] === PANEL DZAGUROV NEWS SYNC START ==="

  /bin/bash "$BASE/bin/sync_medical_news_to_dzagurov.sh"

  echo "[$(date -Is)] === PANEL DZAGUROV NEWS SYNC DONE ==="
} >> "$LOG" 2>&1
