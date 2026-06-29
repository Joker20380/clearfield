#!/bin/sh

export HOME="/home/j/joker2038"

BASE="/home/j/joker2038/clearfield/public_html/clearfield"
LOG="$BASE/logs/medical_news_pipeline.cron.log"

mkdir -p "$BASE/logs"

{
  echo ""
  echo "============================================================"
  echo "$(date '+%F %T %Z') TIMEWEB SH WRAPPER START"
  echo "USER=$(whoami)"
  echo "HOME=$HOME"
  echo "BASE=$BASE"
  echo "PWD before cd=$(pwd)"

  cd "$BASE" || exit 1

  echo "PWD after cd=$(pwd)"
  echo "Running bash pipeline..."

  /bin/bash "$BASE/bin/medical_news_pipeline.sh"

  echo "$(date '+%F %T %Z') TIMEWEB SH WRAPPER END"
  echo "============================================================"
} >> "$LOG" 2>&1
