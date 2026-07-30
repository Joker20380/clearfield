#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

export PATH="$CLEARFIELD_VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$CLEARFIELD_PROJECT_DIR"
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
echo "=== 1. SYNCHRONIZE SEMANTIC CATALOG FROM DZAGUROV ==="

set +e
bash ./bin/sync_medical_semantic_feed_from_dzagurov.sh
SEMANTIC_SYNC_STATUS=$?
set -e

echo
echo "Semantic catalog synchronization status: $SEMANTIC_SYNC_STATUS"

if [ "$SEMANTIC_SYNC_STATUS" -ne 0 ]; then
    echo "WARNING: semantic catalog synchronization failed."
    echo "Checking whether the last validated local feed can be used."

    if python - <<'PY'
import hashlib
import json
from pathlib import Path

path = Path("var/medical-semantic-feed.json")

if not path.is_file():
    raise SystemExit(1)

payload = json.loads(
    path.read_text(encoding="utf-8")
)

items = payload.get("items")

if payload.get("version") != 1:
    raise SystemExit(1)

if not isinstance(items, list) or not items:
    raise SystemExit(1)

required = {
    "panel_id",
    "code",
    "title",
    "url",
}

for item in items:
    if not isinstance(item, dict):
        raise SystemExit(1)

    if not required.issubset(item):
        raise SystemExit(1)

print("Fallback semantic feed:", path)
print("Items:", len(items))
print(
    "SHA256:",
    hashlib.sha256(
        path.read_bytes()
    ).hexdigest(),
)
PY
    then
        echo "WARNING: using the last valid local semantic feed."
        export MEDICAL_SEMANTIC_ASSIGN_ENABLED=1
    else
        echo "WARNING: no valid semantic feed is available."
        echo "Semantic assignment will be skipped for this run."
        export MEDICAL_SEMANTIC_ASSIGN_ENABLED=0
    fi
else
    export MEDICAL_SEMANTIC_ASSIGN_ENABLED=1
fi

echo
echo "Semantic assignment enabled: $MEDICAL_SEMANTIC_ASSIGN_ENABLED"

echo
echo "=== 2. GENERATE AND EXPORT MEDICAL NEWS ==="

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
echo "=== 3. SYNCHRONIZE NEWS TO DZAGUROV ==="

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
