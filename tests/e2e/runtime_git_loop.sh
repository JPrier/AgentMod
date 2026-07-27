#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-git-host -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-git-e2e.XXXXXX")
mkdir -p "$run_root/workspace"
git -C "$run_root/workspace" init --quiet
git -C "$run_root/workspace" -c user.name=AgentMod \
    -c user.email=agentmod@example.invalid commit --allow-empty --quiet -m initial
printf '%s\n' 'runtime Git host fixture' > "$run_root/workspace/untracked.txt"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_GIT_HOST_PROGRAM="$repository/target/debug/agentmod-git-host"

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

turn=$("$repository/target/debug/agentmod" run "inspect Git status and continue" \
    --session "$session_id" --option 'mock_scenario="git_status"' --json)
printf '%s' "$turn" | grep -F \
    '"text":"continued after approved runtime decision"' >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
for event_type in tool.call_proposed tool.call_approved \
    tool.execution_started tool.execution_completed; do
    test "$(grep -c "\"event_type\":\"$event_type\"" "$journal")" -eq 1
done
grep -F '"tool_result"' "$journal" >/dev/null
grep -F 'untracked.txt' "$journal" >/dev/null

echo "runtime/CLI/harness/Git tool-loop E2E passed"
