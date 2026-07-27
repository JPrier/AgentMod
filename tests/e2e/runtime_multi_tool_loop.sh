#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-multi-tool-e2e.XXXXXX")
mkdir -p "$run_root/workspace/src"
printf '%s\n' 'pub const NEEDLE: &str = "needle";' > \
    "$run_root/workspace/src/lib.rs"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"
unset AGENTMOD_PERMISSION_MODE || true

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$repository/target/debug/agentmod-runtime" serve) &
daemon_pid=$!

attempt=0
until "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "runtime did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done

created=$("$repository/target/debug/agentmod" session create \
    --workspace "$run_root/workspace" --style persistent-chat --json)
session_id=$(printf '%s' "$created" | sed -n \
    's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"

turn=$("$repository/target/debug/agentmod" run \
    "read two files in one provider response" --session "$session_id" \
    --option 'mock_scenario="multiple_tool_calls"' --json)
printf '%s' "$turn" | grep -F \
    '"text":"continued after approved runtime decision"' >/dev/null
printf '%s' "$turn" | grep -F '"awaiting_continuation":null' >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
for event_type in model.tool_call_proposed tool.call_proposed tool.call_approved \
    tool.execution_started tool.execution_completed; do
    count=$(grep -c "\"event_type\":\"$event_type\"" "$journal")
    test "$count" -eq 2
done
test "$(grep -c '"kind":"tool_result"' "$journal")" -eq 2
test "$(grep -c '"event_type":"model.request_started"' "$journal")" -eq 2

echo "runtime multi-tool batch/join E2E passed"
