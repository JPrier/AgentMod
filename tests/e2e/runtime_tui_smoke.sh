#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""
cleanup() {
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-tui
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_SCHEDULER_POLL_MS=0
"$repository/target/debug/agentmod-runtime" serve >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"

for _ in $(seq 1 50); do
  if "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
"$repository/target/debug/agentmod" doctor --json >/dev/null
created="$("$repository/target/debug/agentmod" session create \
  --workspace "$run_root" --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
test -n "$session_id"
smoke="$("$repository/target/debug/agentmod-tui" --smoke)"
grep -q "ready=true" <<<"$smoke"
grep -q "sessions=1" <<<"$smoke"
turn="$("$repository/target/debug/agentmod-tui" --smoke-turn \
  "verify committed TUI streaming")"
grep -q "deterministic response" <<<"$turn"
grep -q "turn committed" <<<"$turn"
journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c '"event_type":"model.response_completed"' "$journal")" -eq 1
test "$(grep -c '"event_type":"conversation.entry_committed"' "$journal")" -ge 2
echo "runtime TUI smoke E2E passed"
