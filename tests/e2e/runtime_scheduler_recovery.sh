#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-scheduler -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-runtime-scheduler-recovery.XXXXXX")"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
scheduler="$repository/target/debug/agentmod-scheduler"
cli="$repository/target/debug/agentmod"
daemon_pid=""
launch_count=0
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
export AGENTMOD_SCHEDULER_POLL_MS=0

start_runtime() {
  launch_count=$((launch_count + 1))
  (cd "$run_root" && "$runtime" serve \
    >/dev/null 2>"$run_root/runtime-$launch_count.stderr.log") &
  daemon_pid=$!
}

wait_runtime() {
  for _ in $(seq 1 80); do
    if "$cli" doctor --json >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  cat "$run_root/runtime-$launch_count.stderr.log" >&2 || true
  echo "runtime did not become ready" >&2
  exit 1
}

stop_runtime() {
  if [[ -n "$daemon_pid" ]]; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
    daemon_pid=""
  fi
}

start_runtime
wait_runtime
created="$("$cli" session create --workspace "$run_root/workspace" \
  --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
"$cli" schedule add crash-recovery --session "$session_id" \
  --prompt "recover claimed scheduled work" --at-ms 0 --json >/dev/null
claimed="$("$cli" schedule claim --limit 4 --json)"
execution_id="$(
  printf '%s' "$claimed" |
    grep -o '"execution_id":"[0-9a-f]*"' |
    head -n 1 |
    cut -d'"' -f4
)"
test -n "$execution_id"
test -f "$run_root/scheduler/executions/$execution_id.json"
stop_runtime

export AGENTMOD_SCHEDULER_COMPLETION_DELAY_MS=10000
start_runtime
journal="$run_root/sessions/$session_id/events.jsonl"
for _ in $(seq 1 100); do
  if [[ -f "$journal" ]] &&
    [[ "$(grep -c '"event_type":"model.response_completed"' "$journal" || true)" -eq 1 ]]; then
    break
  fi
  sleep 0.1
done
test "$(grep -c '"event_type":"model.response_completed"' "$journal")" -eq 1
test ! -f "$run_root/scheduler/executions/$execution_id.succeeded"
stop_runtime

unset AGENTMOD_SCHEDULER_COMPLETION_DELAY_MS
start_runtime
wait_runtime
test -f "$run_root/scheduler/executions/$execution_id.succeeded"
test "$(grep -c '"event_type":"scheduler.fired"' "$journal")" -eq 1
test "$(grep -c '"event_type":"model.response_completed"' "$journal")" -eq 1
test "$(grep -c '"event_type":"scheduler.delivery_reconciled"' "$journal")" -eq 1
grep '"event_type":"scheduler.delivery_reconciled"' "$journal" |
  grep -q "\"execution_id\":\"$execution_id\".*\"outcome\":\"succeeded\""
after="$("$cli" schedule claim --limit 4 --json)"
printf '%s' "$after" | grep -q '"executions":\[\]'
echo "runtime scheduler claim and terminal reconciliation E2E passed"
