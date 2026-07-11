#!/usr/bin/env bash

set -uo pipefail

export HOME="/home/j/joker2038"
export PATH="/home/j/joker2038/clearfield/public_html/venv/bin:/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="/home/j/joker2038/clearfield/public_html/clearfield"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/medical-news-cron-$(date +%F).log"

mkdir -p "$LOG_DIR"

exec >>"$LOG_FILE" 2>&1

echo
echo "================================================================"
echo "[$(date --iso-8601=seconds)] MEDICAL NEWS CRON START"
echo "================================================================"

if ! cd "$PROJECT_DIR"; then
    echo "ERROR: cannot enter project directory"
    exit 1
fi

echo
echo "=== 1. GENERATE AND EXPORT MEDICAL NEWS ==="

set +e
bash ./bin/medical_news_pipeline.sh
PIPELINE_STATUS=$?
set -e

echo
echo "Pipeline status: $PIPELINE_STATUS"

if [ "$PIPELINE_STATUS" -ne 0 ]; then
    echo "ERROR: medical news pipeline failed"
    echo "Dzagurov synchronization was not started."
    exit "$PIPELINE_STATUS"
fi

echo
echo "=== 2. SYNCHRONIZE NEWS TO DZAGUROV ==="

set +e
bash ./bin/sync_medical_news_to_dzagurov.sh
SYNC_STATUS=$?
set -e

echo
echo "Synchronization status: $SYNC_STATUS"

if [ "$SYNC_STATUS" -ne 0 ]; then
    echo "ERROR: Dzagurov synchronization failed"
    exit "$SYNC_STATUS"
fi

echo
echo "================================================================"
echo "[$(date --iso-8601=seconds)] MEDICAL NEWS CRON COMPLETED"
echo "================================================================"

exit 0
