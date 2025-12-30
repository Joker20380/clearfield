#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
PROJ="$BASE/clearfield"
PY="$BASE/venv/bin/python"
OUT="$BASE/static/brief.md"
LOG="$BASE/logs/cron_brief.log"

mkdir -p "$BASE/logs"
mkdir -p "$(dirname "$OUT")"

cd "$PROJ"
echo "=== $(date -Is) render_brief start ===" >> "$LOG"
"$PY" manage.py daily_brief --hours 72 --min-evidence 1 > "$OUT"
echo "=== $(date -Is) render_brief end ===" >> "$LOG"
