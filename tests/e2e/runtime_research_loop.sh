#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-research-e2e.XXXXXX")
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
runtime="$repository/target/debug/agentmod-runtime"
cli="$repository/target/debug/agentmod"

stop_runtime() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=
    fi
}

cleanup() {
    stop_runtime
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-research-e2e.*)
            rm -rf -- "$run_root"
            ;;
        *)
            echo "refusing to remove unexpected E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 80 ]; then
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
}

assert_research_state() {
    inspection=$1
    printf '%s' "$inspection" | grep -F '"id":"research-loop"' >/dev/null
    printf '%s' "$inspection" | grep -F '"version":"1.1.0"' >/dev/null
    printf '%s' "$inspection" | grep -F '"lifecycle":"completed"' >/dev/null
    printf '%s' "$inspection" | grep -F '"termination_reason":"complete_session"' >/dev/null
    test "$(printf '%s' "$inspection" | grep -o '"node_id":"persist"' | wc -l | tr -d ' ')" -eq 3
    test "$(printf '%s' "$inspection" | grep -o '"node_id":"repeat"' | wc -l | tr -d ' ')" -eq 3
}

start_runtime
session=$("$cli" session create --workspace "$repository" \
    --style research-loop@1.1.0 --json)
session_id=$(printf '%s' "$session" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"

result=$("$cli" run "map the repository architecture" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="deterministic finding"' \
    --option 'research_complete_after=3' --json)
inspection=$("$cli" session inspect "$session_id" --json)
assert_research_state "$inspection"

journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c '"artifact.persistence_completed"' "$journal")" -eq 3
test "$(grep -c 'research_fresh_context' "$journal")" -eq 3
journal_lines=$(wc -l < "$journal" | tr -d ' ')
printf '%s' "$result" |
    grep -F "\"last_committed_sequence\":$journal_lines" >/dev/null

stop_runtime
start_runtime
restarted=$("$cli" session inspect "$session_id" --json)
assert_research_state "$restarted"
replayed=$("$cli" session replay "$session_id" --json)
printf '%s' "$replayed" | grep -F '"command":"session_replay"' >/dev/null
assert_research_state "$replayed"

echo "runtime research-loop iteration/artifact/restart/replay E2E passed"
