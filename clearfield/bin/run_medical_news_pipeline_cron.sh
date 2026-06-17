#!/usr/bin/env bash
set -Eeuo pipefail

BASE="$HOME/clearfield/public_html/clearfield"
LOG="$BASE/logs/medical_news_pipeline.cron.log"

mkdir -p "$BASE/logs"

{
  echo "============================================================"
  echo "$(date '+%F %T') Timeweb cron wrapper started"
  echo "USER=$(whoami)"
  echo "HOME=$HOME"
  echo "PWD before cd=$(pwd)"

  cd "$BASE"

  echo "PWD after cd=$(pwd)"
  echo "Python: $(which python3 || true)"
  echo "Bash: $BASH_VERSION"

  ./bin/medical_news_pipeline.sh

  echo "$(date '+%F %T') Timeweb cron wrapper finished"
  echo "============================================================"
} >> "$LOG" 2>&1
