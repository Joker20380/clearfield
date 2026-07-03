#!/usr/bin/env bash
set -e

REMOTE="nadmozg@5.11.60.125"
SSH_PORT="2222"
LOCAL_PORT="11434"
REMOTE_PORT="11434"

if curl -fsS "http://127.0.0.1:${LOCAL_PORT}/api/tags" >/dev/null 2>&1; then
  echo "Ollama tunnel already works."
  exit 0
fi

pkill -f "ssh .*127.0.0.1:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" || true

ssh -fN \
  -p "${SSH_PORT}" \
  -i "$HOME/.ssh/id_ed25519" \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o PasswordAuthentication=no \
  -L "127.0.0.1:${LOCAL_PORT}:127.0.0.1:${REMOTE_PORT}" \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  "$REMOTE"

sleep 2

curl -fsS "http://127.0.0.1:${LOCAL_PORT}/api/tags" >/dev/null

echo "Ollama tunnel started."
