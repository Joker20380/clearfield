#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE="${CLEARFIELD_OLLAMA_REMOTE:-nadmozg@5.11.60.125}"
SSH_PORT="${CLEARFIELD_OLLAMA_SSH_PORT:-2222}"
LOCAL_PORT="${CLEARFIELD_OLLAMA_LOCAL_PORT:-11434}"
REMOTE_PORT="${CLEARFIELD_OLLAMA_REMOTE_PORT:-11434}"
SSH_KEY="${CLEARFIELD_OLLAMA_SSH_KEY:-$HOME/.ssh/id_ed25519}"

if [ ! -r "$SSH_KEY" ]; then
  echo "Ollama SSH identity is not readable: $SSH_KEY" >&2
  exit 1
fi

if curl -fsS "http://127.0.0.1:${LOCAL_PORT}/api/tags" >/dev/null 2>&1; then
  echo "Ollama tunnel already works."
  exit 0
fi

pkill -f "ssh .*127.0.0.1:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" || true

ssh -fN \
  -p "${SSH_PORT}" \
  -i "$SSH_KEY" \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o PasswordAuthentication=no \
  -o StrictHostKeyChecking=yes \
  -L "127.0.0.1:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  "$REMOTE"

sleep 2

curl -fsS "http://127.0.0.1:${LOCAL_PORT}/api/tags" >/dev/null

echo "Ollama tunnel started."
