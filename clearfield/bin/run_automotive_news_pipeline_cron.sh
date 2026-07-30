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

CONFIG_FILE="${AUTOMOTIVE_SYNC_CONFIG:-$PROJECT_DIR/var/automotive-sync.env}"

if [ -f "$CONFIG_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
fi

LOG_DIR="$PROJECT_DIR/logs/automotive-news"
SYNC_ENABLED="${AUTOMOTIVE_SYNC_ENABLED:-0}"

mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/cron-$(date +%F).log"

exec >> "$LOG_FILE" 2>&1

SUCCESS_DIR="$PROJECT_DIR/var/automotive-news-success"
SUCCESS_FILE="$SUCCESS_DIR/$(date +%F).ok"

FORCE_RUN="${AUTOMOTIVE_FORCE_RUN:-0}"

PIPELINE_ATTEMPTS="${AUTOMOTIVE_PIPELINE_ATTEMPTS:-4}"
PIPELINE_RETRY_DELAY_SECONDS="${AUTOMOTIVE_PIPELINE_RETRY_DELAY_SECONDS:-900}"

SYNC_ATTEMPTS="${AUTOMOTIVE_SYNC_ATTEMPTS:-3}"
SYNC_RETRY_DELAY_SECONDS="${AUTOMOTIVE_SYNC_RETRY_DELAY_SECONDS:-300}"

mkdir -p "$SUCCESS_DIR"

validate_positive_integer() {
  local name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer; got: $value" >&2
    exit 2
  fi
}

validate_non_negative_integer() {
  local name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a non-negative integer; got: $value" >&2
    exit 2
  fi
}

validate_positive_integer   "AUTOMOTIVE_PIPELINE_ATTEMPTS"   "$PIPELINE_ATTEMPTS"

validate_non_negative_integer   "AUTOMOTIVE_PIPELINE_RETRY_DELAY_SECONDS"   "$PIPELINE_RETRY_DELAY_SECONDS"

validate_positive_integer   "AUTOMOTIVE_SYNC_ATTEMPTS"   "$SYNC_ATTEMPTS"

validate_non_negative_integer   "AUTOMOTIVE_SYNC_RETRY_DELAY_SECONDS"   "$SYNC_RETRY_DELAY_SECONDS"

run_with_retries() {
  local label="$1"
  local attempts="$2"
  local delay_seconds="$3"

  shift 3

  local attempt=1
  local rc=0

  while [ "$attempt" -le "$attempts" ]; do
    echo
    echo "----------------------------------------------------------------"
    echo "$label attempt $attempt/$attempts"
    echo "Started: $(date --iso-8601=seconds)"
    echo "----------------------------------------------------------------"

    if "$@"; then
      echo "$label attempt $attempt succeeded."
      return 0
    else
      rc=$?
    fi

    echo "$label attempt $attempt failed with exit code $rc."

    if [ "$attempt" -lt "$attempts" ]; then
      echo "Retrying in $delay_seconds seconds."
      sleep "$delay_seconds"
    fi

    attempt=$((attempt + 1))
  done

  echo "$label failed after $attempts attempts." >&2
  return "$rc"
}

echo
echo "================================================================"
echo "Automotive cron started: $(date --iso-8601=seconds)"
echo "Sync enabled: $SYNC_ENABLED"
echo "Force run: $FORCE_RUN"
echo "Pipeline attempts: $PIPELINE_ATTEMPTS"
echo "Pipeline retry delay: $PIPELINE_RETRY_DELAY_SECONDS"
echo "Sync attempts: $SYNC_ATTEMPTS"
echo "Sync retry delay: $SYNC_RETRY_DELAY_SECONDS"
echo "Success marker: $SUCCESS_FILE"

if [ "$FORCE_RUN" != "1" ] && [ -s "$SUCCESS_FILE" ]; then
  echo
  echo "Automotive pipeline already completed successfully today."
  echo "Marker contents:"
  cat "$SUCCESS_FILE"
  echo
  echo "Automotive cron finished without a repeated run:"
  date --iso-8601=seconds
  exit 0
fi

if ! run_with_retries   "Automotive pipeline"   "$PIPELINE_ATTEMPTS"   "$PIPELINE_RETRY_DELAY_SECONDS"   "$SCRIPT_DIR/automotive_news_pipeline.sh"
then
  echo "Automotive pipeline did not complete successfully." >&2
  exit 1
fi

if [ "$SYNC_ENABLED" = "1" ]; then
  if ! run_with_retries     "Automotive synchronization"     "$SYNC_ATTEMPTS"     "$SYNC_RETRY_DELAY_SECONDS"     "$SCRIPT_DIR/sync_automotive_news_to_diagnost.sh"
  then
    echo "Automotive synchronization did not complete successfully." >&2
    exit 1
  fi
else
  echo "Remote sync skipped: AUTOMOTIVE_SYNC_ENABLED is not 1."
fi

SUCCESS_TMP="$SUCCESS_FILE.tmp.$$"

{
  echo "completed_at=$(date --iso-8601=seconds)"
  echo "sync_enabled=$SYNC_ENABLED"
  echo "host=$(hostname)"
} > "$SUCCESS_TMP"

chmod 600 "$SUCCESS_TMP"
mv -f "$SUCCESS_TMP" "$SUCCESS_FILE"

echo
echo "Daily success marker created:"
cat "$SUCCESS_FILE"

echo
echo "Automotive cron finished: $(date --iso-8601=seconds)"
