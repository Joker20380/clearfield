#!/usr/bin/env bash

set -Eeuo pipefail

BASE="$HOME/clearfield/public_html/clearfield"
PYTHON="$HOME/clearfield/public_html/venv/bin/python"
FEED_DIR="$HOME/clearfield/public_html/generated-news"

REMOTE_USER="appuser"
REMOTE_HOST="186.246.51.201"
REMOTE_DIR="/opt/apps/dzagurov/dzagurov"
REMOTE_PYTHON="/opt/apps/dzagurov/venv/bin/python"

SSH_KEY="$HOME/.ssh/id_ed25519_dzagurov_news"

LOG_DIR="$BASE/logs"
LOCK_FILE="$LOG_DIR/dzagurov_regional_digest_sync.lock"

mkdir -p "$LOG_DIR" "$FEED_DIR"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
    echo "[$(date --iso-8601=seconds)] Regional sync already running."
    exit 0
fi

cd "$BASE"

echo "[$(date --iso-8601=seconds)] === EXPORT REGIONAL DIGEST FEED ==="

"$PYTHON" manage.py export_regional_digest_feed \
    --status published \
    --limit 365 \
    --public-dir "$FEED_DIR" \
    --show-content-size

LATEST_FEED="$(
    find "$FEED_DIR" \
        -maxdepth 1 \
        -type f \
        -name 'regional-digest-feed-*.json' \
        -printf '%T@ %p\n' \
        2>/dev/null \
        | sort -nr \
        | head -n 1 \
        | cut -d' ' -f2-
)"

if [ -z "${LATEST_FEED:-}" ] || [ ! -s "$LATEST_FEED" ]; then
    echo "ERROR: regional digest feed was not found."
    exit 1
fi

ITEMS="$(
    "$PYTHON" - "$LATEST_FEED" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])

payload = json.loads(
    path.read_text(encoding="utf-8")
)

print(len(payload.get("items", [])))
PY
)"

echo "Feed: $LATEST_FEED"
echo "Items: $ITEMS"
sha256sum "$LATEST_FEED"

if [ "$ITEMS" = "0" ]; then
    echo "No regional digest items to synchronize."
    exit 0
fi

SSH_OPTS=(
    -i "$SSH_KEY"
    -o BatchMode=yes
    -o StrictHostKeyChecking=accept-new
    -o UserKnownHostsFile="$HOME/.ssh/known_hosts"
)

REMOTE_FEED="$REMOTE_DIR/generated_regional_digest_feed.json"

echo "[$(date --iso-8601=seconds)] === COPY TO DZAGUROV ==="

scp "${SSH_OPTS[@]}" \
    "$LATEST_FEED" \
    "$REMOTE_USER@$REMOTE_HOST:${REMOTE_FEED}.tmp"

echo "[$(date --iso-8601=seconds)] === IMPORT ON DZAGUROV ==="

ssh "${SSH_OPTS[@]}" \
    "$REMOTE_USER@$REMOTE_HOST" "
        set -Eeuo pipefail

        cd '$REMOTE_DIR'

        mv \
          '${REMOTE_FEED}.tmp' \
          '$REMOTE_FEED'

        '$REMOTE_PYTHON' manage.py \
          import_generated_medical_news \
          --file '$REMOTE_FEED' \
          --sync-existing \
          --preserve-existing-photos
    "

echo "[$(date --iso-8601=seconds)] === REGIONAL SYNC COMPLETED ==="
