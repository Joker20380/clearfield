#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
PROJ="$BASE/clearfield"
PY="$BASE/venv/bin/python"
LOG="$BASE/logs/cron_extract.log"

mkdir -p "$BASE/logs"
cd "$PROJ"

echo "=== $(date -Is) extract_articles start ===" >> "$LOG"
"$PY" manage.py extract_articles --limit 2000 --concurrency 4 --retries 1 --timeout 20 >> "$LOG" 2>&1
echo "=== $(date -Is) extract_articles end ===" >> "$LOG"
