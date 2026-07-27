#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-scheduler -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-runtime-scheduler-e2e.XXXXXX")"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
scheduler="$repository/target/debug/agentmod-scheduler"
cli="$repository/target/debug/agentmod"
daemon_pid=""
cleanup() {
  if [[ -n "$daemon_pid" ]]; then kill "$daemon_pid" 2>/dev/null || true; fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT

mkdir -p "$run_root/workspace"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"

start_runtime() {
  (cd "$run_root" && "$runtime" serve >/dev/null 2>&1) &
  daemon_pid=$!
  for _ in $(seq 1 50); do
    if "$cli" doctor --json >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  echo "runtime did not become ready" >&2
  exit 1
}

start_runtime
created="$("$cli" session create --workspace "$run_root/workspace" --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
"$cli" schedule add daily-driver --session "$session_id" \
  --prompt "execute scheduled development work" --at-ms 0 --json >/dev/null
journal="$run_root/sessions/$session_id/events.jsonl"
for _ in $(seq 1 50); do
  if [[ -f "$journal" ]] && [[ "$(grep -c '"event_type":"scheduler.fired"' "$journal" || true)" -eq 1 ]]; then
    break
  fi
  sleep 0.1
done
test "$(grep -c '"event_type":"scheduler.fired"' "$journal")" -eq 1
test "$(grep -c '"event_type":"model.response_completed"' "$journal")" -eq 1
execution_id="$(sed -n 's/.*"execution_id":"\([0-9a-f]*\)".*/\1/p' "$journal" | head -n 1)"
test -f "$run_root/scheduler/executions/$execution_id.succeeded"
manual="$("$cli" schedule run --limit 4 --json)"
printf '%s' "$manual" | grep -q '"runs":\[\]'

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=""
start_runtime
after="$("$cli" schedule run --limit 4 --json)"
printf '%s' "$after" | grep -q '"runs":\[\]'
test "$(grep -c '"event_type":"scheduler.fired"' "$journal")" -eq 1
echo "runtime-owned scheduled turn E2E passed"
