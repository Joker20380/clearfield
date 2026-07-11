#!/usr/bin/env bash

set -Eeuo pipefail

export HOME="/home/j/joker2038"
export PATH="/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

RUN_DIR="${PROJECT_DIR}/var/run"
LOG_DIR="${PROJECT_DIR}/logs/llm_tunnel"

LOCK_FILE="${RUN_DIR}/llm-tunnel-watchdog.lock"

IDENTITY_FILE="${HOME}/.ssh/clearfield_llm_tunnel_ed25519"

LOCAL_HOST="127.0.0.1"
LOCAL_PORT="18081"

REMOTE_HOST="5.11.60.125"
REMOTE_PORT="2223"
REMOTE_USER="nadmozg"

REMOTE_TARGET_HOST="127.0.0.1"
REMOTE_TARGET_PORT="8082"

HEALTH_URL="http://${LOCAL_HOST}:${LOCAL_PORT}/health"

mkdir -p \
  "$RUN_DIR" \
  "$LOG_DIR"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
  exit 0
fi

timestamp() {
  date --iso-8601=seconds
}

health_ok() {
  curl \
    -fsS \
    --max-time 10 \
    "$HEALTH_URL" \
    >/dev/null 2>&1
}

matching_tunnel_pids() {
  ps -eo pid=,comm=,args= \
    | awk \
        -v forward="${LOCAL_HOST}:${LOCAL_PORT}:${REMOTE_TARGET_HOST}:${REMOTE_TARGET_PORT}" \
        -v destination="${REMOTE_USER}@${REMOTE_HOST}" \
        '$2 == "ssh" && index($0, forward) > 0 && index($0, destination) > 0 { print $1 }'
}

if health_ok; then
  exit 0
fi

if [ ! -r "$IDENTITY_FILE" ]; then
  echo "[$(timestamp)] ERROR: SSH identity is missing: ${IDENTITY_FILE}" >&2
  exit 1
fi

echo "[$(timestamp)] LLM health check failed."

mapfile -t stale_pids < <(
  matching_tunnel_pids
)

if [ "${#stale_pids[@]}" -gt 0 ]; then
  echo "[$(timestamp)] Removing stale tunnel PIDs: ${stale_pids[*]}"

  kill "${stale_pids[@]}" 2>/dev/null || true
  sleep 2

  for pid in "${stale_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
fi

echo "[$(timestamp)] Starting SSH tunnel."

ssh \
  -fNT \
  -i "$IDENTITY_FILE" \
  -p "$REMOTE_PORT" \
  -L "${LOCAL_HOST}:${LOCAL_PORT}:${REMOTE_TARGET_HOST}:${REMOTE_TARGET_PORT}" \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=15 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -o StrictHostKeyChecking=yes \
  "${REMOTE_USER}@${REMOTE_HOST}"

for attempt in $(seq 1 20); do
  if health_ok; then
    echo "[$(timestamp)] SSH tunnel is healthy."
    exit 0
  fi

  sleep 1
done

echo "[$(timestamp)] ERROR: tunnel started but health check failed." >&2

matching_tunnel_pids \
  | xargs -r kill \
  2>/dev/null || true

exit 1
