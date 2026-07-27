#!/usr/bin/env sh
set -eu

repository="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
  -p agentmod-filesystem-host -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-startup-recovery-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace/src"
printf 'pub fn recovered() -> bool { true }' >"$workspace/src/lib.rs"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
filesystem="$repository/target/debug/agentmod-filesystem-host"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN='0123456789abcdef0123456789abcdef0123456789abcdef'
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem"
export AGENTMOD_TOOL_RECEIPT_DELAY_MS=8000
unset AGENTMOD_PERMISSION_MODE || true
daemon_pid=''
turn_pid=''
cleanup() {
  [ -z "$turn_pid" ] || kill "$turn_pid" 2>/dev/null || true
  [ -z "$daemon_pid" ] || kill "$daemon_pid" 2>/dev/null || true
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-startup-recovery-e2e.*) rm -rf -- "$run_root" ;;
  esac
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$runtime" serve >runtime.out 2>runtime.err) &
daemon_pid=$!
attempt=0
until "$cli" doctor --json >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 50 ] || { printf 'runtime did not become ready\n' >&2; exit 1; }
  sleep 0.1
done
created="$("$cli" session create --workspace "$workspace" --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
journal="$run_root/sessions/$session_id/events.jsonl"
receipt_root="$run_root/sessions/$session_id/artifacts/tool-receipts"
(cd "$workspace" && "$cli" run 'read before the daemon crashes' \
  --session "$session_id" --option 'mock_scenario="one_tool_call"' --json \
  >"$run_root/turn.out" 2>"$run_root/turn.err") &
turn_pid=$!
attempt=0
while :; do
  receipt_count="$(find "$receipt_root" -maxdepth 1 -name '*.json' 2>/dev/null | wc -l | tr -d ' ')"
  [ "$receipt_count" = 1 ] && break
  attempt=$((attempt + 1))
  [ "$attempt" -lt 80 ] || { printf 'receipt was not created\n' >&2; exit 1; }
  sleep 0.1
done
test "$(grep -c '"event_type":"tool.execution_dispatched"' "$journal")" -eq 1
test "$(grep -c '"event_type":"tool.execution_completed"' "$journal" || true)" -eq 0
kill -9 "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=''
kill "$turn_pid" 2>/dev/null || true
wait "$turn_pid" 2>/dev/null || true
turn_pid=''

unset AGENTMOD_TOOL_RECEIPT_DELAY_MS
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$run_root/host-must-not-be-spawned"
(cd "$run_root" && "$runtime" serve >runtime-restarted.out 2>runtime-restarted.err) &
daemon_pid=$!
attempt=0
until "$cli" doctor --json >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 50 ] || { cat "$run_root/runtime-restarted.err" >&2; exit 1; }
  sleep 0.1
done
test "$(grep -c '"event_type":"tool.execution_dispatched"' "$journal")" -eq 1
test "$(grep -c '"event_type":"tool.execution_started"' "$journal")" -eq 1
test "$(grep -c '"event_type":"tool.execution_completed"' "$journal")" -eq 1
grep -F '"tool_call_request"' "$journal" >/dev/null
grep -F '"tool_result"' "$journal" >/dev/null
"$cli" run 'continue after startup recovery' --session "$session_id" \
  --option 'mock_scenario="streaming_text"' \
  --option 'mock_text="startup-recovery-ok"' --json >/dev/null
printf 'runtime startup-wide tool receipt recovery E2E passed\n'
