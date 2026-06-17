#!/usr/bin/env bash
set -Eeuo pipefail

BASE="$HOME/clearfield/public_html/clearfield"
VENV="$HOME/clearfield/public_html/venv/bin/activate"
LOG_DIR="$BASE/logs"
FEED_DIR="$HOME/clearfield/public_html/generated-news"

cd "$BASE"
mkdir -p "$LOG_DIR" "$FEED_DIR"

exec 9>"$LOG_DIR/medical_news_pipeline.lock"

if ! flock -n 9; then
  echo "$(date '+%F %T') Pipeline already running. Exit."
  exit 0
fi

source "$VENV"

echo "============================================================"
echo "$(date '+%F %T') Medical news pipeline started"
echo "============================================================"

echo "[1/7] Ensure Ollama tunnel"
./bin/ensure_ollama_tunnel.sh

echo "[2/7] Rebuild event summaries"
python3 manage.py rebuild_event_summaries --hours 720 -v 1

echo "[3/7] Create medical briefs"
python3 manage.py create_medical_briefs \
  --hours 720 \
  --min-evidence 1 \
  --limit 10 \
  --min-score 5

echo "[4/7] Audit medical briefs"
python3 manage.py audit_medical_briefs --status ready

echo "[5/7] Generate medical news via LLM"
python3 manage.py generate_medical_news --limit 3

echo "[6/7] Auto publish generated reviews"
python3 manage.py auto_approve_medical_news \
  --status review \
  --limit 3 \
  --min-chars 1700 \
  --show-rejected

echo "[7/7] Export public JSON feed"
python3 manage.py export_medical_news_feed \
  --status published \
  --limit 20 \
  --show-content-size

chmod 755 "$FEED_DIR"
chmod 644 "$FEED_DIR"/medical-news-feed-*.json 2>/dev/null || true
chmod 600 "$FEED_DIR"/.medical_news_feed_token 2>/dev/null || true

echo "============================================================"
echo "$(date '+%F %T') Medical news pipeline finished"
echo "============================================================"
