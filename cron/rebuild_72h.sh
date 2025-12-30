#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
PROJ="$BASE/clearfield"
PY="$BASE/venv/bin/python"
LOG="$BASE/logs/cron_rebuild.log"

mkdir -p "$BASE/logs"

cd "$PROJ"
echo "=== $(date -Is) rebuild_event_summaries start ===" >> "$LOG"
"$PY" manage.py rebuild_event_summaries --hours 72 >> "$LOG" 2>&1
echo "=== $(date -Is) rebuild_event_summaries end ===" >> "$LOG"
