#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-tool-e2e.XXXXXX")
mkdir -p "$run_root/workspace/src"
printf '%s\n' 'pub fn fixture() -> bool { true }' > \
    "$run_root/workspace/src/lib.rs"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"

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

turn=$("$repository/target/debug/agentmod" run "read src/lib.rs and continue" \
    --session "$session_id" --option 'mock_scenario="one_tool_call"' --json)
printf '%s' "$turn" | grep -F '"last_committed_sequence":27' >/dev/null
printf '%s' "$turn" | grep -F \
    '"text":"continued after approved runtime decision"' >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
for event_type in model.tool_call_proposed tool.call_proposed tool.call_approved \
    tool.execution_dispatched tool.execution_started tool.execution_completed; do
    grep -F "\"event_type\":\"$event_type\"" "$journal" >/dev/null
done
grep -F '"tool_call_request"' "$journal" >/dev/null
grep -F '"tool_result"' "$journal" >/dev/null

echo "runtime/CLI/harness/filesystem tool-loop E2E passed"
