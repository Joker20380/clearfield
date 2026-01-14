#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
PROJ="$BASE/clearfield"
PY="$BASE/venv/bin/python"
LOG="$BASE/logs/cron_ingest.log"

mkdir -p "$BASE/logs"
cd "$PROJ"

echo "=== $(date -Is) ingest_feeds start ===" >> "$LOG"
"$PY" manage.py ingest_feeds --limit 5000 >> "$LOG" 2>&1
echo "=== $(date -Is) ingest_feeds end ===" >> "$LOG"
