#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

BASE="$CLEARFIELD_PROJECT_DIR"
PYTHON="$CLEARFIELD_PYTHON"

LOCAL_DIR="$BASE/var"
LOCAL_FEED="$LOCAL_DIR/medical-semantic-feed.json"

REMOTE_USER="${DZAGUROV_REMOTE_USER:-appuser}"
REMOTE_HOST="${DZAGUROV_REMOTE_HOST:-186.246.51.201}"
REMOTE_DIR="${DZAGUROV_REMOTE_DIR:-/opt/apps/dzagurov/dzagurov}"
REMOTE_PYTHON="${DZAGUROV_REMOTE_PYTHON:-/opt/apps/dzagurov/venv/bin/python}"
REMOTE_FEED="${DZAGUROV_SEMANTIC_FEED:-var/medical-semantic-feed.json}"

SSH_KEY="${DZAGUROV_SSH_KEY:-$HOME/.ssh/id_ed25519_dzagurov_news}"
SSH_KNOWN_HOSTS="${DZAGUROV_KNOWN_HOSTS:-$HOME/.ssh/known_hosts}"

LOG_DIR="$BASE/logs"
LOCK_FILE="$LOG_DIR/dzagurov_semantic_feed_sync.lock"

mkdir -p "$LOCAL_DIR" "$LOG_DIR"

if [ ! -r "$SSH_KEY" ]; then
  echo "ERROR: SSH key is not readable: $SSH_KEY" >&2
  exit 1
fi

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  echo "[$(date -Is)] Semantic feed sync is already running."
  exit 0
fi

SSH_OPTS=(
  -i "$SSH_KEY"
  -o BatchMode=yes
  -o ConnectTimeout=20
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
  -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$SSH_KNOWN_HOSTS"
)

TMP_FEED="$(
  mktemp \
    "$LOCAL_DIR/.medical-semantic-feed.XXXXXX.tmp"
)"

cleanup() {
  rm -f "$TMP_FEED"
}

trap cleanup EXIT

echo "[$(date -Is)] === REMOTE SEMANTIC EXPORT ==="

ssh \
  "${SSH_OPTS[@]}" \
  "$REMOTE_USER@$REMOTE_HOST" \
  /bin/bash -s -- \
  "$REMOTE_DIR" \
  "$REMOTE_PYTHON" \
  "$REMOTE_FEED" <<'REMOTE'
set -Eeuo pipefail

REMOTE_DIR="$1"
REMOTE_PYTHON="$2"
REMOTE_FEED="$3"

cd "$REMOTE_DIR"

"$REMOTE_PYTHON" \
  manage.py \
  export_medical_semantic_feed \
  --output "$REMOTE_FEED" \
  --base-url "https://kdl-dzagurov.ru"

test -s "$REMOTE_FEED"

stat \
  --printf='Remote feed: %n, %s bytes\n' \
  "$REMOTE_FEED"
REMOTE

echo
echo "[$(date -Is)] === DOWNLOAD SEMANTIC FEED ==="

scp \
  "${SSH_OPTS[@]}" \
  "$REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/$REMOTE_FEED" \
  "$TMP_FEED"

test -s "$TMP_FEED"

echo
echo "[$(date -Is)] === VALIDATE SEMANTIC FEED ==="

"$PYTHON" - "$TMP_FEED" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])

try:
    payload = json.loads(
        path.read_text(encoding="utf-8")
    )
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(
        f"Invalid semantic JSON: {exc}"
    ) from exc

if not isinstance(payload, dict):
    raise SystemExit(
        "Semantic feed root must be an object."
    )

required_top_fields = {
    "version",
    "generated_at",
    "source",
    "base_url",
    "item_count",
    "category_count",
    "content_sha256",
    "categories",
    "items",
}

missing_top_fields = sorted(
    required_top_fields - payload.keys()
)

if missing_top_fields:
    raise SystemExit(
        "Missing top-level fields: "
        + ", ".join(missing_top_fields)
    )

if payload["version"] != 1:
    raise SystemExit(
        f"Unsupported semantic feed version: "
        f"{payload['version']!r}"
    )

if payload["source"] != "kdl-dzagurov.ru":
    raise SystemExit(
        f"Unexpected source: "
        f"{payload['source']!r}"
    )

if payload["base_url"] != "https://kdl-dzagurov.ru":
    raise SystemExit(
        f"Unexpected base URL: "
        f"{payload['base_url']!r}"
    )

items = payload["items"]
categories = payload["categories"]

if not isinstance(items, list):
    raise SystemExit(
        "Semantic feed items must be a list."
    )

if not isinstance(categories, list):
    raise SystemExit(
        "Semantic feed categories must be a list."
    )

if payload["item_count"] != len(items):
    raise SystemExit(
        "item_count does not match items length."
    )

if payload["category_count"] != len(categories):
    raise SystemExit(
        "category_count does not match categories length."
    )

if len(items) < 1600:
    raise SystemExit(
        f"Too few semantic items: {len(items)}"
    )

required_item_fields = {
    "panel_id",
    "code",
    "title",
    "canonical_anchor",
    "relative_url",
    "url",
    "category",
    "price",
    "currency",
    "duration",
    "tests",
    "biomaterials",
    "semantic_terms",
    "search_text",
    "boost",
}

panel_ids = []
codes = []
urls = []

for position, item in enumerate(items, start=1):
    if not isinstance(item, dict):
        raise SystemExit(
            f"Item #{position} is not an object."
        )

    missing_item_fields = sorted(
        required_item_fields - item.keys()
    )

    if missing_item_fields:
        raise SystemExit(
            f"Item #{position} missing fields: "
            + ", ".join(missing_item_fields)
        )

    relative_url = str(
        item["relative_url"]
    )
    url = str(item["url"])

    if not relative_url.startswith(
        "/analysis/"
    ):
        raise SystemExit(
            f"Item #{position} has invalid "
            f"relative URL: {relative_url!r}"
        )

    if not url.startswith(
        "https://kdl-dzagurov.ru/analysis/"
    ):
        raise SystemExit(
            f"Item #{position} has invalid "
            f"absolute URL: {url!r}"
        )

    panel_ids.append(item["panel_id"])
    codes.append(item["code"])
    urls.append(url)

if len(panel_ids) != len(set(panel_ids)):
    raise SystemExit(
        "Duplicate panel_id values detected."
    )

if len(codes) != len(set(codes)):
    raise SystemExit(
        "Duplicate panel codes detected."
    )

if len(urls) != len(set(urls)):
    raise SystemExit(
        "Duplicate panel URLs detected."
    )

canonical_items = json.dumps(
    items,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")

actual_hash = hashlib.sha256(
    canonical_items
).hexdigest()

expected_hash = str(
    payload["content_sha256"]
)

if actual_hash != expected_hash:
    raise SystemExit(
        "Semantic content SHA256 mismatch: "
        f"expected={expected_hash}, "
        f"actual={actual_hash}"
    )

print("Version:", payload["version"])
print("Generated at:", payload["generated_at"])
print("Items:", len(items))
print("Categories:", len(categories))
print("SHA256:", actual_hash)
print("Semantic feed validation: OK")
PY

echo
echo "[$(date -Is)] === INSTALL LOCAL FEED ==="

chmod 600 "$TMP_FEED"
mv -f "$TMP_FEED" "$LOCAL_FEED"

trap - EXIT

echo "Local feed: $LOCAL_FEED"

stat \
  --printf='Size: %s bytes\nPermissions: %A\nModified: %y\n' \
  "$LOCAL_FEED"

echo
echo "[$(date -Is)] === SEMANTIC FEED SYNC DONE ==="
