#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
LOG="$BASE/logs/cron_pipeline.log"
mkdir -p "$BASE/logs"

echo "=== $(date -Is) PIPELINE start ===" >> "$LOG"

/bin/bash "$BASE/cron/ingest_24h.sh" >> "$LOG" 2>&1
/bin/bash "$BASE/cron/extract_24h.sh" >> "$LOG" 2>&1
/bin/bash "$BASE/cron/cluster_24h.sh" >> "$LOG" 2>&1
/bin/bash "$BASE/cron/rebuild_72h.sh" >> "$LOG" 2>&1
/bin/bash "$BASE/cron/render_brief.sh" >> "$LOG" 2>&1

echo "=== $(date -Is) PIPELINE end ===" >> "$LOG"
