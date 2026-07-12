#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" \
    && pwd
)"

PROJECT_DIR="$(
  cd -- "$SCRIPT_DIR/.." \
    && pwd
)"

CONFIG_FILE="${AUTOMOTIVE_SYNC_CONFIG:-$PROJECT_DIR/var/automotive-sync.env}"

if [ -f "$CONFIG_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
fi

LOG_DIR="$PROJECT_DIR/logs/automotive-news"
SYNC_ENABLED="${AUTOMOTIVE_SYNC_ENABLED:-0}"

mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/cron-$(date +%F).log"

exec >> "$LOG_FILE" 2>&1

echo
echo "================================================================"
echo "Automotive cron started: $(date --iso-8601=seconds)"
echo "Sync enabled: $SYNC_ENABLED"

"$SCRIPT_DIR/automotive_news_pipeline.sh"

if [ "$SYNC_ENABLED" = "1" ]; then
  "$SCRIPT_DIR/sync_automotive_news_to_diagnost.sh"
else
  echo "Remote sync skipped: AUTOMOTIVE_SYNC_ENABLED is not 1."
fi

echo "Automotive cron finished: $(date --iso-8601=seconds)"
