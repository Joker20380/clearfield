#!/usr/bin/env bash

set -Eeuo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

PROJECT_DIR="$CLEARFIELD_PROJECT_DIR"
VENV_DIR="$CLEARFIELD_VENV_DIR"

LOG_DIR="${PROJECT_DIR}/logs/regional_digest"
RUN_DIR="${PROJECT_DIR}/var/run"

mkdir -p \
  "$LOG_DIR" \
  "$RUN_DIR"

# Cron запускает wrapper каждый час, но реальный pipeline
# выполняется только в 06 часов по московскому времени.
# Ручной запуск не ограничивается этим условием.
if [[ "${REGIONAL_DIGEST_SCHEDULED:-0}" == "1" ]]; then
  MOSCOW_HOUR_RAW="$(
    TZ=Europe/Moscow date '+%H'
  )"

  MOSCOW_HOUR="$((10#${MOSCOW_HOUR_RAW}))"

  # Cron проверяет pipeline каждый час.
  # Первый запуск разрешён после 06:00 МСК.
  # Последующие проверки будут быстро завершены
  # дневной защитой Django-команды.
  if (( MOSCOW_HOUR < 6 )); then
    exit 0
  fi
fi

MOSCOW_DATE="$(
  TZ=Europe/Moscow date '+%F'
)"

SYNC_MARKER="${RUN_DIR}/regional-digest-synced-${MOSCOW_DATE}.ok"
EMPTY_MARKER="${RUN_DIR}/regional-digest-empty-${MOSCOW_DATE}.ok"

if [[ "${REGIONAL_DIGEST_SCHEDULED:-0}" == "1" ]] \
  && [[ -f "$SYNC_MARKER" ]]; then
  exit 0
fi

LOG_FILE="${LOG_DIR}/regional-digest-${MOSCOW_DATE}.log"
LOCK_FILE="${RUN_DIR}/regional-digest.lock"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  {
    echo
    echo "[$(date --iso-8601=seconds)] SKIPPED"
    echo "Another regional digest pipeline is running."
  } >> "$LOG_FILE"

  exit 0
fi

if [[ "${REGIONAL_DIGEST_SCHEDULED:-0}" == "1" ]]; then
  # Cron пишет только в журнал.
  exec >> "$LOG_FILE" 2>&1
else
  # Ручной запуск одновременно виден в терминале
  # и сохраняется в журнале.
  exec > >(tee -a "$LOG_FILE") 2>&1
fi

STARTED_AT="$(
  date --iso-8601=seconds
)"

STARTED_EPOCH="$(
  date '+%s'
)"

on_exit() {
  local exit_code="$?"
  local finished_at
  local finished_epoch
  local elapsed

  finished_at="$(
    date --iso-8601=seconds
  )"

  finished_epoch="$(
    date '+%s'
  )"

  elapsed="$((finished_epoch - STARTED_EPOCH))"

  echo
  echo "=== PIPELINE FINISHED ==="
  echo "Finished: ${finished_at}"
  echo "Exit code: ${exit_code}"
  echo "Elapsed seconds: ${elapsed}"
  echo

  find "$LOG_DIR" \
    -maxdepth 1 \
    -type f \
    -name 'regional-digest-*.log' \
    -mtime +30 \
    -delete \
    2>/dev/null || true

  trap - EXIT
  exit "$exit_code"
}

trap on_exit EXIT

cd "$PROJECT_DIR"

source "${VENV_DIR}/bin/activate"

echo "=== ENSURE LLM TUNNEL ==="

"${PROJECT_DIR}/bin/ensure_llm_tunnel.sh"

echo "LLM tunnel: OK"
echo

echo
echo "=================================================="
echo "REGIONAL DIGEST PIPELINE"
echo "Started: ${STARTED_AT}"
echo "Moscow date: ${MOSCOW_DATE}"
echo "Project: ${PROJECT_DIR}"
echo "Python: $(command -v python)"
echo "=================================================="
echo

rm -f "$EMPTY_MARKER"

python manage.py run_regional_digest_pipeline \
  --region north_ossetia \
  --region-label "Северная Осетия" \
  --region-query \
    "рсо-алания,рсо алания,северная осетия,северной осетии,владикавказ" \
  --topic medicine \
  --days 90 \
  --min-events 1 \
  --max-events 6 \
  --max-candidates 30 \
  --batch-size 10 \
  --fact-retries 2 \
  --compose-retries 3 \
  --empty-marker "$EMPTY_MARKER"

if [[ -s "$EMPTY_MARKER" ]]; then
  echo
  echo "=== NO SUITABLE EVENTS TODAY ==="
  cat "$EMPTY_MARKER"
  echo "No digest was created or published."

  touch "$SYNC_MARKER"
  exit 0
fi

echo
echo "=== PUBLISH VALIDATED REGIONAL DIGEST ==="

python manage.py publish_regional_digest \
  --latest-review \
  --region-label "Северная Осетия" \
  --topic medicine

echo
echo "=== EXPORT PUBLISHED REGIONAL DIGEST FEED ==="

python manage.py export_regional_digest_feed \
  --status published \
  --limit 365 \
  --public-dir "${PROJECT_DIR}/../generated-news" \
  --show-content-size

echo
echo "=== SYNCHRONIZE REGIONAL DIGEST TO DZAGUROV ==="

"${PROJECT_DIR}/bin/sync_regional_digest_to_dzagurov.sh"

touch "$SYNC_MARKER"

find "$RUN_DIR" \
  -maxdepth 1 \
  -type f \
  -name 'regional-digest-synced-*.ok' \
  -mtime +7 \
  -delete \
  2>/dev/null || true
