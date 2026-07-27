#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-branch-e2e.XXXXXX")
workspace="$run_root/workspace"
mkdir -p "$workspace"
endpoint="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_ENDPOINT="$endpoint"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
runtime="$repository/target/debug/agentmod-runtime"
cli="$repository/target/debug/agentmod"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf "$run_root"
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
i=0
until "$cli" doctor --json >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -lt 50 ] || { echo "runtime did not become ready" >&2; exit 1; }
    sleep 0.1
done

parent=$("$cli" session create --workspace "$workspace" --style persistent-chat --json |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
turn=$("$cli" run "parent prompt" --session "$parent" \
    --option 'mock_scenario="streaming_text"' --option 'mock_text="parent answer"' --json)
fork=$(printf '%s' "$turn" | sed -n 's/.*"last_committed_sequence":\([0-9]*\).*/\1/p')
head=$("$cli" session inspect "$parent" --json)
replayed=$("$cli" session replay "$parent" --at 1 --json)
printf '%s' "$replayed" | grep -q '"inspected_sequence":1'
branched=$("$cli" session branch "$parent" --at "$fork" --style ephemeral-turn --json)
child=$(printf '%s' "$branched" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
child_before=$("$cli" session inspect "$child" --json)
printf '%s' "$child_before" | grep -q "\"parent_session_id\":\"$parent\""
printf '%s' "$child_before" | grep -q '"style":"ephemeral-turn"'
"$cli" run "child prompt" --session "$child" \
    --option 'mock_scenario="streaming_text"' --option 'mock_text="child answer"' --json >/dev/null
parent_after=$("$cli" session inspect "$parent" --json)
child_after=$("$cli" session inspect "$child" --json)
[ "$head" = "$parent_after" ]
printf '%s' "$parent_after" | grep -qv 'child answer'
printf '%s' "$child_after" | grep -q 'child answer'
printf '%s\n' "runtime replay/branch/continue E2E passed"
