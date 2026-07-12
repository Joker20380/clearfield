#!/usr/bin/env bash

set -Eeuo pipefail

export HOME="/home/j/joker2038"
export PATH="/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

VENV_DIR="$(
  cd "${PROJECT_DIR}/.."
  pwd
)/venv"

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

LOG_FILE="${LOG_DIR}/russia-medical-digest-${MOSCOW_DATE}.log"
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
    -name 'russia-medical-digest-*.log' \
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
echo "RUSSIA MEDICAL DIGEST PIPELINE"
echo "Started: ${STARTED_AT}"
echo "Moscow date: ${MOSCOW_DATE}"
echo "Project: ${PROJECT_DIR}"
echo "Python: $(command -v python)"
echo "=================================================="
echo

python manage.py run_regional_digest_pipeline \
  --region russia \
  --region-label "Россия" \
  --region-query \
    "рсо-алания,рсо алания,северная осетия,северной осетии,владикавказ" \
  --topic medicine \
  --days 90 \
  --min-events 1 \
  --max-events 5 \
  --max-candidates 50 \
  --batch-size 10 \
  --fact-retries 2 \
  --compose-retries 3

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
