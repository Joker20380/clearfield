#!/usr/bin/env bash
set -Eeuo pipefail

BASE="/home/j/joker2038/clearfield/public_html/clearfield"
VENV="/home/j/joker2038/clearfield/public_html/venv/bin/activate"
LOG_DIR="$BASE/logs"
LOG_FILE="$LOG_DIR/medical_news_pipeline.log"
LOCK_FILE="$LOG_DIR/medical_news_pipeline.lock"

mkdir -p "$LOG_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date '+%F %T') Pipeline already running. Exit." >> "$LOG_FILE"
  exit 0
fi

{
  echo ""
  echo "============================================================"
  echo "$(date '+%F %T') Medical news pipeline started"
  echo "============================================================"

  cd "$BASE"
  source "$VENV"

CERTIFI_BUNDLE="$(python -c 'import certifi; print(certifi.where())' 2>/dev/null || true)"
if [ -n "$CERTIFI_BUNDLE" ]; then
  export SSL_CERT_FILE="$CERTIFI_BUNDLE"
  export REQUESTS_CA_BUNDLE="$CERTIFI_BUNDLE"
  echo "SSL_CERT_FILE: $SSL_CERT_FILE"
fi

  echo "Python: $(which python)"
  python -c "import django; print('Django', django.get_version())"

  echo "[1/8] Ensure Ollama tunnel"
  ./bin/ensure_llm_tunnel.sh

  echo "[2/8] Ingest feeds"
  python manage.py ingest_feeds --topic medicine

  echo "[3/8] Cluster events"
  python manage.py cluster_events --topic medicine

  echo "[4/8] Create medical briefs"
  python manage.py create_medical_briefs \
    --hours 96 \
    --limit 10 \
    --min-evidence 1 \
    --min-score 1 \
    --min-summary-len 60 \
    --show-rejected

  echo "[5/8] Audit ready briefs"
  python manage.py audit_medical_briefs --status ready

  echo "[6/8] Generate medical news"
  python manage.py generate_medical_news --limit 5

  echo "[7/8] Auto approve"
  python manage.py auto_approve_medical_news \
    --status review \
    --limit 5 \
    --min-chars 1400 \
    --show-rejected

  echo "[8/8] Export feed"
  python manage.py export_medical_news_feed \
    --status published \
    --limit 20 \
    --show-content-size

  echo "============================================================"
  echo "$(date '+%F %T') Medical news pipeline finished"
  echo "============================================================"
} >> "$LOG_FILE" 2>&1
