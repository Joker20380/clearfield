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

FEED_DIR="${CLEARFIELD_FEED_DIR:-$PROJECT_DIR/../generated-news}"
FEED_PATH="$FEED_DIR/generated_automotive_news_feed.json"

LOG_DIR="$PROJECT_DIR/logs/automotive-news"
LOCK_DIR="$PROJECT_DIR/var"
LOCK_FILE="$LOCK_DIR/automotive-news-pipeline.lock"

INGEST_LIMIT="${AUTOMOTIVE_INGEST_LIMIT:-20}"
INGEST_SINCE_HOURS="${AUTOMOTIVE_INGEST_SINCE_HOURS:-168}"
INGEST_CONCURRENCY="${AUTOMOTIVE_INGEST_CONCURRENCY:-2}"
INGEST_MAX_ITEMS_PER_SOURCE="${AUTOMOTIVE_INGEST_MAX_ITEMS_PER_SOURCE:-40}"

CLUSTER_LIMIT="${AUTOMOTIVE_CLUSTER_LIMIT:-2000}"

CREATE_HOURS="${AUTOMOTIVE_CREATE_HOURS:-720}"
CREATE_LIMIT="${AUTOMOTIVE_CREATE_LIMIT:-30}"
CREATE_MIN_EVIDENCE="${AUTOMOTIVE_CREATE_MIN_EVIDENCE:-1}"
CREATE_MIN_SCORE="${AUTOMOTIVE_CREATE_MIN_SCORE:-8}"

AUDIT_LIMIT="${AUTOMOTIVE_AUDIT_LIMIT:-100}"
GENERATE_LIMIT="${AUTOMOTIVE_GENERATE_LIMIT:-3}"

APPROVE_LIMIT="${AUTOMOTIVE_APPROVE_LIMIT:-100}"
APPROVE_MIN_SCORE="${AUTOMOTIVE_APPROVE_MIN_SCORE:-70}"
APPROVE_MIN_BODY_CHARS="${AUTOMOTIVE_APPROVE_MIN_BODY_CHARS:-0}"

EXPORT_LIMIT="${AUTOMOTIVE_EXPORT_LIMIT:-100}"

DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  bin/automotive_news_pipeline.sh
  bin/automotive_news_pipeline.sh --dry-run

Environment:
  AUTOMOTIVE_INGEST_LIMIT
  AUTOMOTIVE_INGEST_SINCE_HOURS
  AUTOMOTIVE_INGEST_CONCURRENCY
  AUTOMOTIVE_INGEST_MAX_ITEMS_PER_SOURCE
  AUTOMOTIVE_CLUSTER_LIMIT
  AUTOMOTIVE_CREATE_HOURS
  AUTOMOTIVE_CREATE_LIMIT
  AUTOMOTIVE_CREATE_MIN_EVIDENCE
  AUTOMOTIVE_CREATE_MIN_SCORE
  AUTOMOTIVE_AUDIT_LIMIT
  AUTOMOTIVE_GENERATE_LIMIT
  AUTOMOTIVE_APPROVE_LIMIT
  AUTOMOTIVE_APPROVE_MIN_SCORE
  AUTOMOTIVE_APPROVE_MIN_BODY_CHARS
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
  "$FEED_DIR" \
  "$LOG_DIR" \
  "$LOCK_DIR"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  echo "Automotive pipeline is already running."
  exit 0
fi

RUN_STAMP="$(date +%F-%H%M%S)"
LOG_FILE="$LOG_DIR/pipeline-$RUN_STAMP.log"

exec > >(tee -a "$LOG_FILE") 2>&1

source "$VENV_ACTIVATE"
cd "$PROJECT_DIR"

run_command() {
  printf '\n>>>'

  printf ' %q' "$@"

  printf '\n'

  "$@"
}

django_count() {
  local code="$1"
  local value

  value="$(
    python manage.py shell -c "$code" \
      | sed -nE '/^[0-9]+$/p' \
      | tail -n 1
  )"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Could not resolve Django count." >&2
    exit 1
  fi

  printf '%s\n' "$value"
}

validate_feed() {
  python - "$FEED_PATH" <<'PY'
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

path = Path(sys.argv[1])

if not path.is_file():
    raise RuntimeError(
        f"Feed does not exist: {path}"
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
        "Unexpected automotive feed source"
    )

items = payload.get("items")

if not isinstance(items, list):
    raise RuntimeError(
        "Automotive feed items must be a list"
    )

for number, item in enumerate(
    items,
    start=1,
):
    if not isinstance(item, dict):
        raise RuntimeError(
            f"Item {number} is not an object"
        )

    source_id = str(
        item.get("source_id") or ""
    )

    if not source_id.startswith(
        "automotive-news-"
    ):
        raise RuntimeError(
            f"Invalid source_id in item {number}"
        )

    urls = item.get("source_urls")

    if not isinstance(urls, list):
        raise RuntimeError(
            f"source_urls is not a list "
            f"in item {number}"
        )

    for url in urls:
        parsed = urlsplit(str(url))

        if (
            parsed.scheme
            not in {"http", "https"}
            or not parsed.netloc
        ):
            raise RuntimeError(
                f"Invalid source URL: {url}"
            )

print("Feed:", path)
print("Items:", len(items))
print("Feed validation passed.")
PY
}

echo "=== AUTOMOTIVE NEWS PIPELINE ==="
echo "Started: $(date --iso-8601=seconds)"
echo "Project: $PROJECT_DIR"
echo "Feed: $FEED_PATH"
echo "Dry-run: $DRY_RUN"
echo "Log: $LOG_FILE"

if [ "$DRY_RUN" -eq 1 ]; then
  run_command \
    python manage.py cluster_events \
      --topic auto \
      --limit "$CLUSTER_LIMIT" \
      --dry-run

  run_command \
    python manage.py create_automotive_briefs \
      --hours "$CREATE_HOURS" \
      --min-evidence "$CREATE_MIN_EVIDENCE" \
      --min-score "$CREATE_MIN_SCORE" \
      --limit "$CREATE_LIMIT" \
      --dry-run \
      --show-rejected

  run_command \
    python manage.py audit_automotive_briefs \
      --status ready \
      --limit "$AUDIT_LIMIT" \
      --dry-run \
      --show-accepted \
      --show-rejected

  run_command \
    python manage.py generate_automotive_news \
      --status ready \
      --limit "$GENERATE_LIMIT" \
      --dry-run

  run_command \
    python manage.py auto_approve_automotive_news \
      --limit "$APPROVE_LIMIT" \
      --min-score "$APPROVE_MIN_SCORE" \
      --min-body-chars "$APPROVE_MIN_BODY_CHARS" \
      --dry-run \
      --show-approved \
      --show-skipped

  run_command \
    python manage.py export_automotive_news_feed \
      --statuses approved,published \
      --limit "$EXPORT_LIMIT" \
      --dry-run

  echo
  echo "Dry-run completed."
  echo "Finished: $(date --iso-8601=seconds)"
  exit 0
fi

run_command \
  python manage.py ingest_feeds \
    --topic auto \
    --limit "$INGEST_LIMIT" \
    --since-hours "$INGEST_SINCE_HOURS" \
    --concurrency "$INGEST_CONCURRENCY" \
    --max-items-per-source "$INGEST_MAX_ITEMS_PER_SOURCE"

run_command \
  python manage.py cluster_events \
    --topic auto \
    --limit "$CLUSTER_LIMIT"

run_command \
  python manage.py create_automotive_briefs \
    --hours "$CREATE_HOURS" \
    --min-evidence "$CREATE_MIN_EVIDENCE" \
    --min-score "$CREATE_MIN_SCORE" \
    --limit "$CREATE_LIMIT" \
    --show-rejected

run_command \
  python manage.py audit_automotive_briefs \
    --status ready \
    --limit "$AUDIT_LIMIT" \
    --show-accepted \
    --show-rejected

READY_COUNT="$(
  django_count '
from intel.models import AutomotiveBrief

print(
    AutomotiveBrief.objects.filter(
        status="ready",
    ).count()
)
'
)"

echo
echo "Ready briefs: $READY_COUNT"

if [ "$READY_COUNT" -gt 0 ]; then
  ERROR_COUNT_BEFORE="$(
    django_count '
from intel.models import GeneratedAutomotiveNews

print(
    GeneratedAutomotiveNews.objects.filter(
        status="error",
    ).count()
)
'
  )"

  run_command \
    bash "$SCRIPT_DIR/ensure_llm_tunnel.sh"

  run_command \
    python manage.py generate_automotive_news \
      --status ready \
      --limit "$GENERATE_LIMIT"

  ERROR_COUNT_AFTER="$(
    django_count '
from intel.models import GeneratedAutomotiveNews

print(
    GeneratedAutomotiveNews.objects.filter(
        status="error",
    ).count()
)
'
  )"

  echo "Generation errors before: $ERROR_COUNT_BEFORE"
  echo "Generation errors after:  $ERROR_COUNT_AFTER"

  if [ "$ERROR_COUNT_AFTER" -gt "$ERROR_COUNT_BEFORE" ]; then
    echo "WARNING: new automotive generation errors detected; continuing with successful news."
    echo "Successful generated news will continue to editorial approval and export."
  fi
else
  echo "No ready automotive briefs; LLM generation skipped."
fi

run_command \
  python manage.py auto_approve_automotive_news \
    --limit "$APPROVE_LIMIT" \
    --min-score "$APPROVE_MIN_SCORE" \
    --min-body-chars "$APPROVE_MIN_BODY_CHARS" \
    --show-approved \
    --show-skipped

run_command \
  python manage.py export_automotive_news_feed \
    --output "$FEED_PATH" \
    --statuses approved,published \
    --limit "$EXPORT_LIMIT"

validate_feed

echo
echo "Automotive pipeline completed."
echo "Finished: $(date --iso-8601=seconds)"
