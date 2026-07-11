#!/bin/sh

set -u

export HOME="/home/j/joker2038"
export PATH="/usr/local/bin:/usr/bin:/bin"

BASE="/home/j/joker2038/clearfield/public_html/clearfield"
LOG="$BASE/logs/medical_news_pipeline.cron.log"

mkdir -p "$BASE/logs"

{
    echo
    echo "============================================================"
    echo "$(date '+%F %T %Z') TIMEWEB SH WRAPPER START"
    echo "USER=$(whoami)"
    echo "HOME=$HOME"
    echo "BASE=$BASE"
    echo "PWD before cd=$(pwd)"

    if ! cd "$BASE"; then
        echo "$(date '+%F %T %Z') ERROR: cannot cd to $BASE"
        exit 1
    fi

    echo "PWD after cd=$(pwd)"
    echo "Running medical news pipeline..."

    status=0

    /bin/bash "$BASE/bin/medical_news_pipeline.sh" \
        || status=$?

    echo "PIPELINE EXIT STATUS=$status"
    echo "$(date '+%F %T %Z') TIMEWEB SH WRAPPER END"
    echo "============================================================"

    exit "$status"
} >>"$LOG" 2>&1
