#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

BASE="$CLEARFIELD_PROJECT_DIR"
PYTHON="$CLEARFIELD_PYTHON"
FEED_DIR="$CLEARFIELD_FEED_DIR"

REMOTE_USER="${DZAGUROV_REMOTE_USER:-appuser}"
REMOTE_HOST="${DZAGUROV_REMOTE_HOST:-186.246.51.201}"
REMOTE_DIR="${DZAGUROV_REMOTE_DIR:-/opt/apps/dzagurov/dzagurov}"
REMOTE_PYTHON="${DZAGUROV_REMOTE_PYTHON:-/opt/apps/dzagurov/venv/bin/python}"

SSH_KEY="${DZAGUROV_SSH_KEY:-$HOME/.ssh/id_ed25519_dzagurov_news}"
SSH_KNOWN_HOSTS="${DZAGUROV_KNOWN_HOSTS:-$HOME/.ssh/known_hosts}"

LOG_DIR="$BASE/logs"
LOCK_FILE="$LOG_DIR/dzagurov_news_sync.lock"

mkdir -p "$LOG_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] Sync already running, exit."
  exit 0
fi

cd "$BASE"

echo "[$(date -Is)] === EXPORT MEDICAL NEWS FEED ==="
"$PYTHON" manage.py export_medical_news_feed

LATEST_FEED=$(
  find "$FEED_DIR" -maxdepth 1 -type f -name "medical-news-feed-*.json" -printf "%T@ %p\n" 2>/dev/null \
    | sort -nr \
    | head -1 \
    | cut -d' ' -f2-
)

if [ -z "${LATEST_FEED:-}" ] || [ ! -s "$LATEST_FEED" ]; then
  echo "[$(date -Is)] ERROR: latest feed not found in $FEED_DIR"
  exit 1
fi

ITEMS=$("$PYTHON" - "$LATEST_FEED" <<'PY'
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
data = json.loads(p.read_text(encoding="utf-8"))
print(len(data.get("items", [])))
PY
)

echo "[$(date -Is)] Feed: $LATEST_FEED"
echo "[$(date -Is)] Items: $ITEMS"

if [ "$ITEMS" = "0" ]; then
  echo "[$(date -Is)] No items to sync."
  exit 0
fi

SSH_OPTS=(
  -i "$SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$SSH_KNOWN_HOSTS"
)

echo "[$(date -Is)] === COPY FEED TO DZAGUROV ==="
scp "${SSH_OPTS[@]}" \
  "$LATEST_FEED" \
  "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/generated_medical_news_feed.json.tmp"

echo "[$(date -Is)] === IMPORT ON DZAGUROV ==="
ssh "${SSH_OPTS[@]}" "$REMOTE_USER@$REMOTE_HOST" "
  set -Eeuo pipefail
  cd '$REMOTE_DIR'
  mv generated_medical_news_feed.json.tmp generated_medical_news_feed.json
  '$REMOTE_PYTHON' manage.py import_generated_medical_news --file generated_medical_news_feed.json --sync-existing --preserve-existing-photos
"

echo "[$(date -Is)] === DONE ==="
