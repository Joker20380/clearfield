#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

failures=0

check() {
  local label="$1"
  shift

  printf '%-38s' "$label"

  if "$@" >/dev/null 2>&1; then
    echo "OK"
  else
    echo "FAILED"
    failures=$((failures + 1))
  fi
}

validate_json_feed() {
  "$CLEARFIELD_PYTHON" - "$1" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))

if not isinstance(payload, dict):
    raise SystemExit("feed root is not an object")

items = payload.get("items")
if not isinstance(items, list):
    raise SystemExit("feed items is not a list")
PY
}

echo "Clearfield health check"
echo "Project: $CLEARFIELD_PROJECT_DIR"
echo "Python:  $CLEARFIELD_PYTHON"
echo "Feeds:   $CLEARFIELD_FEED_DIR"
echo

check "Python executable" test -x "$CLEARFIELD_PYTHON"
check "Environment file" test -r "$CLEARFIELD_PROJECT_DIR/.env"
check "Django system check" \
  "$CLEARFIELD_PYTHON" "$CLEARFIELD_PROJECT_DIR/manage.py" check
check "Migration consistency" \
  "$CLEARFIELD_PYTHON" "$CLEARFIELD_PROJECT_DIR/manage.py" \
  makemigrations --check --dry-run
check "Database connectivity" \
  "$CLEARFIELD_PYTHON" "$CLEARFIELD_PROJECT_DIR/manage.py" \
  shell -c "from django.db import connection; connection.ensure_connection()"

feed_count=0

if [ -d "$CLEARFIELD_FEED_DIR" ]; then
  while IFS= read -r -d '' feed; do
    feed_count=$((feed_count + 1))
    check "JSON feed: $(basename "$feed")" validate_json_feed "$feed"
  done < <(
    find "$CLEARFIELD_FEED_DIR" \
      -maxdepth 1 \
      -type f \
      -name '*.json' \
      -print0
  )
fi

if [ "$feed_count" -eq 0 ]; then
  echo "JSON feeds                            FAILED (none found)"
  failures=$((failures + 1))
fi

if [ "${CLEARFIELD_CHECK_LLM:-0}" = "1" ]; then
  check "LLM endpoint" \
    curl -fsS --max-time 10 \
    "${OLLAMA_BASE_URL:-http://127.0.0.1:18081}/health"
fi

echo

if [ "$failures" -ne 0 ]; then
  echo "Health check failed: $failures check(s)."
  exit 1
fi

echo "Health check passed."
