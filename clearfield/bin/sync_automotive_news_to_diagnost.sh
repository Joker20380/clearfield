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

VENV_ACTIVATE="${CLEARFIELD_VENV_ACTIVATE:-$PROJECT_DIR/../venv/bin/activate}"

CONFIG_FILE="${AUTOMOTIVE_SYNC_CONFIG:-$PROJECT_DIR/var/automotive-sync.env}"

if [ -f "$CONFIG_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
fi

FEED_PATH="${AUTOMOTIVE_FEED_PATH:-${CLEARFIELD_FEED_DIR:-$PROJECT_DIR/../generated-news}/generated_automotive_news_feed.json}"

LOCK_DIR="$PROJECT_DIR/var"
LOCK_FILE="$LOCK_DIR/automotive-news-sync.lock"
LOG_DIR="$PROJECT_DIR/logs/automotive-news"

DIAGNOST_SSH_TARGET="${DIAGNOST_SSH_TARGET:-appuser@diagnost-rso.ru}"
DIAGNOST_SSH_PORT="${DIAGNOST_SSH_PORT:-22}"
DIAGNOST_SSH_IDENTITY_FILE="${DIAGNOST_SSH_IDENTITY_FILE:-}"
DIAGNOST_SSH_KNOWN_HOSTS_FILE="${DIAGNOST_SSH_KNOWN_HOSTS_FILE:-$PROJECT_DIR/var/diagnost-known-hosts}"

DIAGNOST_ROOT="${DIAGNOST_ROOT:-/opt/apps/diagnost}"
DIAGNOST_COMPOSE_FILE="${DIAGNOST_COMPOSE_FILE:-docker-compose.prod.yml}"
DIAGNOST_WEB_SERVICE="${DIAGNOST_WEB_SERVICE:-web}"

REMOTE_FEED_PATH="${DIAGNOST_REMOTE_FEED_PATH:-/tmp/clearfield-generated-automotive-news-feed.json}"
CONTAINER_FEED_PATH="${DIAGNOST_CONTAINER_FEED_PATH:-/tmp/generated_automotive_news_feed.json}"

EXPORT_LIMIT="${AUTOMOTIVE_EXPORT_LIMIT:-100}"

DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  bin/sync_automotive_news_to_diagnost.sh
  bin/sync_automotive_news_to_diagnost.sh --dry-run

Environment:
  DIAGNOST_SSH_TARGET
  DIAGNOST_SSH_PORT
  DIAGNOST_SSH_IDENTITY_FILE
  DIAGNOST_SSH_KNOWN_HOSTS_FILE
  DIAGNOST_ROOT
  DIAGNOST_COMPOSE_FILE
  DIAGNOST_WEB_SERVICE
  DIAGNOST_REMOTE_FEED_PATH
  DIAGNOST_CONTAINER_FEED_PATH
  AUTOMOTIVE_FEED_PATH
  AUTOMOTIVE_EXPORT_LIMIT
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;

    -h|--help)
      usage
      exit 0
      ;;

    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac

  shift
done

test -s "$VENV_ACTIVATE"

mkdir -p \
  "$LOCK_DIR" \
  "$LOG_DIR"

exec 8>"$LOCK_FILE"

if ! flock -n 8; then
  echo "Automotive sync is already running."
  exit 0
fi

source "$VENV_ACTIVATE"
cd "$PROJECT_DIR"

ITEM_COUNT="$(
  python - "$FEED_PATH" <<'PY'
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

path = Path(sys.argv[1])

if not path.is_file():
    raise RuntimeError(
        f"Automotive feed not found: {path}"
    )

payload = json.loads(
    path.read_text(
        encoding="utf-8",
    )
)

if (
    payload.get("source")
    != "clearfield_generated_automotive_news"
):
    raise RuntimeError(
        "Unexpected feed source"
    )

items = payload.get("items")

if not isinstance(items, list):
    raise RuntimeError(
        "Feed items must be a list"
    )

for number, item in enumerate(
    items,
    start=1,
):
    if not isinstance(item, dict):
        raise RuntimeError(
            f"Item {number} is not an object"
        )

    required = {
        "source_id",
        "title",
        "body_markdown",
        "source_urls",
        "image_topic",
    }

    missing = sorted(
        required - set(item)
    )

    if missing:
        raise RuntimeError(
            f"Item {number} missing: {missing}"
        )

    for url in item["source_urls"]:
        parsed = urlsplit(str(url))

        if (
            parsed.scheme
            not in {"http", "https"}
            or not parsed.netloc
        ):
            raise RuntimeError(
                f"Invalid source URL: {url}"
            )

print(len(items))
PY
)"

if [[ ! "$ITEM_COUNT" =~ ^[0-9]+$ ]]; then
  echo "Could not resolve automotive feed item count." >&2
  exit 1
fi

echo "=== AUTOMOTIVE NEWS SYNC ==="
echo "Feed: $FEED_PATH"
echo "Items: $ITEM_COUNT"
echo "Target: $DIAGNOST_SSH_TARGET"
echo "Target root: $DIAGNOST_ROOT"
echo "Identity: ${DIAGNOST_SSH_IDENTITY_FILE:-SSH default}"
echo "Known hosts: $DIAGNOST_SSH_KNOWN_HOSTS_FILE"
echo "Dry-run: $DRY_RUN"

if [ "$ITEM_COUNT" -eq 0 ]; then
  echo "Feed is empty; remote synchronization skipped."
  exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo
  echo "Dry-run validation passed."
  echo "No SSH connection was made."
  exit 0
fi

if [ ! -s "$DIAGNOST_SSH_KNOWN_HOSTS_FILE" ]; then
  echo "SSH known_hosts file not found or empty: $DIAGNOST_SSH_KNOWN_HOSTS_FILE" >&2
  exit 1
fi

SSH_OPTIONS=(
  -o BatchMode=yes
  -o ConnectTimeout=20
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$DIAGNOST_SSH_KNOWN_HOSTS_FILE"
  -o GlobalKnownHostsFile=/dev/null
)

SCP_OPTIONS=(
  -o BatchMode=yes
  -o ConnectTimeout=20
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$DIAGNOST_SSH_KNOWN_HOSTS_FILE"
  -o GlobalKnownHostsFile=/dev/null
)

if [ -n "$DIAGNOST_SSH_PORT" ]; then
  SSH_OPTIONS+=(
    -p "$DIAGNOST_SSH_PORT"
  )

  SCP_OPTIONS+=(
    -P "$DIAGNOST_SSH_PORT"
  )
fi

if [ -n "$DIAGNOST_SSH_IDENTITY_FILE" ]; then
  if [ ! -f "$DIAGNOST_SSH_IDENTITY_FILE" ]; then
    echo "SSH identity file not found: $DIAGNOST_SSH_IDENTITY_FILE" >&2
    exit 1
  fi

  SSH_OPTIONS+=(
    -i "$DIAGNOST_SSH_IDENTITY_FILE"
    -o IdentitiesOnly=yes
  )

  SCP_OPTIONS+=(
    -i "$DIAGNOST_SSH_IDENTITY_FILE"
    -o IdentitiesOnly=yes
  )
fi

REMOTE_UPLOAD_PATH="$REMOTE_FEED_PATH.uploading.$$"

echo
echo "=== 1. VERIFY SSH CONNECTION ==="

ssh \
  "${SSH_OPTIONS[@]}" \
  "$DIAGNOST_SSH_TARGET" \
  "test -d '$DIAGNOST_ROOT'"

echo
echo "=== 2. UPLOAD FEED ==="

scp \
  "${SCP_OPTIONS[@]}" \
  "$FEED_PATH" \
  "$DIAGNOST_SSH_TARGET:$REMOTE_UPLOAD_PATH"

echo
echo "=== 3. VALIDATE AND IMPORT REMOTELY ==="

ssh \
  "${SSH_OPTIONS[@]}" \
  "$DIAGNOST_SSH_TARGET" \
  bash -s -- \
    "$REMOTE_UPLOAD_PATH" \
    "$REMOTE_FEED_PATH" \
    "$DIAGNOST_ROOT" \
    "$DIAGNOST_COMPOSE_FILE" \
    "$DIAGNOST_WEB_SERVICE" \
    "$CONTAINER_FEED_PATH" <<'REMOTE'
set -Eeuo pipefail

UPLOAD_PATH="$1"
FEED_PATH="$2"
PROJECT_ROOT="$3"
COMPOSE_FILE="$4"
WEB_SERVICE="$5"
CONTAINER_FEED="$6"

test -s "$UPLOAD_PATH"
test -d "$PROJECT_ROOT"
test -f "$PROJECT_ROOT/$COMPOSE_FILE"

mv -f \
  "$UPLOAD_PATH" \
  "$FEED_PATH"

chmod 600 \
  "$FEED_PATH"

python3 - "$FEED_PATH" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])

payload = json.loads(
    path.read_text(
        encoding="utf-8",
    )
)

assert (
    payload.get("source")
    == "clearfield_generated_automotive_news"
)

assert isinstance(
    payload.get("items"),
    list,
)

print(
    "Remote feed items:",
    len(payload["items"]),
)
PY

cd "$PROJECT_ROOT"

docker compose \
  -f "$COMPOSE_FILE" \
  ps "$WEB_SERVICE"

docker compose \
  -f "$COMPOSE_FILE" \
  cp \
  "$FEED_PATH" \
  "$WEB_SERVICE:$CONTAINER_FEED"

docker compose \
  -f "$COMPOSE_FILE" \
  exec \
  -T \
  "$WEB_SERVICE" \
  python manage.py \
    import_generated_automotive_news \
      --publish \
    "$CONTAINER_FEED"

docker compose \
  -f "$COMPOSE_FILE" \
  exec \
  -T \
  "$WEB_SERVICE" \
  rm -f "$CONTAINER_FEED"

echo "Remote automotive import completed."
REMOTE

echo
echo "=== 4. MARK SUCCESSFULLY EXPORTED ITEMS AS PUBLISHED ==="

python manage.py export_automotive_news_feed \
  --output "$FEED_PATH" \
  --statuses approved,published \
  --limit "$EXPORT_LIMIT" \
  --mark-published

echo
echo "Automotive synchronization completed."
