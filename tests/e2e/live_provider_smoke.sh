#!/usr/bin/env bash
# Opt-in live provider smoke test (Unix). Never runs by default: it requires
# explicit credentials through environment variables. Drives the native harness
# binary directly so provider variables are visible to the harness process.
#
# Required:
#   AGENTMOD_LIVE_SMOKE=1
#   AGENTMOD_PROVIDER_<ID>_API_KEY (or an api_key_ref option to a file)
#
# Optional:
#   AGENTMOD_LIVE_SMOKE_PROVIDER (default openrouter)
#   AGENTMOD_LIVE_SMOKE_MODEL (defaults to the adapter default)
#   AGENTMOD_PROVIDER_<ID>_BASE_URL (explicit endpoint)
set -euo pipefail

if [[ "${AGENTMOD_LIVE_SMOKE:-}" != "1" ]]; then
  echo "live provider smoke skipped (set AGENTMOD_LIVE_SMOKE=1 to enable)"
  exit 0
fi

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"

cargo build -p agentmod-harness

provider="${AGENTMOD_LIVE_SMOKE_PROVIDER:-openrouter}"
model="${AGENTMOD_LIVE_SMOKE_MODEL:-}"
model_option=""
if [[ -n "$model" ]]; then
  model_option=",\"model\":\"$model\""
fi

export AGENTMOD_HARNESS_AUTH_KEY="$(printf 'ab%.0s' {1..32})"
export AGENTMOD_HARNESS_DEV_MODE=1

coproc HARNESS { target/debug/agentmod-harness; }
trap 'kill "$HARNESS_PID" 2>/dev/null || true' EXIT

send() { printf '%s\n' "$1" >&"${HARNESS[1]}"; }

command="{\"command\":\"execute\",\"value\":{\"session_id\":\"018f6f83-7b80-7000-8000-000000000001\",\"provider\":\"$provider\",\"model\":\"$model\",\"entries\":[{\"kind\":\"user\",\"value\":{\"text\":\"Reply with the single word: pong\"}}],\"options\":{\"max_tokens\":64,\"streaming\":true}$model_option,\"authorization_grant\":\"grant\",\"cancellation_id\":\"018f6f83-7b80-7000-8000-000000000002\"}}"

send "$command"
completed=""
while IFS= read -r line <&"${HARNESS[0]}"; do
  if [[ "$line" == *'"reply":"failed"'* ]]; then
    echo "harness failed: $line" >&2
    exit 1
  fi
  if [[ "$line" == *'"event":"completed"'* ]]; then
    completed="$line"
    break
  fi
done
if [[ -z "$completed" ]]; then
  echo "live provider did not complete" >&2
  exit 1
fi
echo "live provider smoke passed: provider=$provider model=$model"
