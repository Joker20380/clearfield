#!/usr/bin/env bash
set -euo pipefail

BASE="/home/j/joker2038/clearfield/public_html"
PROJ="$BASE/clearfield"
PY="$BASE/venv/bin/python"
LOG="$BASE/logs/cron_cluster.log"

mkdir -p "$BASE/logs"

cd "$PROJ"
echo "=== $(date -Is) cluster_events start ===" >> "$LOG"

# lightweight visibility: do we even have fresh items?
"$PY" manage.py shell -c "from django.utils import timezone; from datetime import timedelta; from intel.models import RawItem; since=timezone.now()-timedelta(hours=24); print('raw24_pub', RawItem.objects.filter(published_at__gte=since).count(), 'raw24_created', RawItem.objects.filter(created_at__gte=since).count())" >> "$LOG" 2>&1

"$PY" manage.py cluster_events --since-hours 24 --limit 2000 >> "$LOG" 2>&1
echo "=== $(date -Is) cluster_events end ===" >> "$LOG"
