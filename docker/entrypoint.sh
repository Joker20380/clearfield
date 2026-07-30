#!/usr/bin/env bash
set -Eeuo pipefail

cd "$CLEARFIELD_PROJECT_DIR"

python - <<'PY'
import os
import time

import psycopg

config = {
    "dbname": os.environ["DATABASE_NAME"],
    "user": os.environ["DATABASE_USER"],
    "password": os.environ["DATABASE_PASSWORD"],
    "host": os.environ.get("DATABASE_HOST", "127.0.0.1"),
    "port": os.environ.get("DATABASE_PORT", "5432"),
}

for attempt in range(1, 31):
    try:
        with psycopg.connect(**config):
            break
    except psycopg.OperationalError:
        if attempt == 30:
            raise
        time.sleep(2)
PY

python manage.py migrate --noinput
python manage.py collectstatic --noinput

exec "$@"
