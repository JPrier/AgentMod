#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-declarative-e2e.XXXXXX")
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"
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
        "${TMPDIR:-/tmp}"/agentmod-declarative-e2e.*)
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

assert_completed_graph() {
    inspection=$1
    tool_count=$2
    printf '%s' "$inspection" | grep -F '"id":"declarative-graph"' >/dev/null
    printf '%s' "$inspection" | grep -F '"version":"1.1.0"' >/dev/null
    printf '%s' "$inspection" | grep -F '"lifecycle":"completed"' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"termination_reason":"complete_session"' >/dev/null
    test "$(printf '%s' "$inspection" |
        grep -o '"state":"terminal"' | wc -l | tr -d ' ')" -eq "$tool_count"
    for node in branch tool repeat done; do
        printf '%s' "$inspection" |
            grep -F "\"node_id\":\"$node\"" >/dev/null
    done
}

start_runtime
direct=$("$cli" session create --workspace "$repository" \
    --style declarative-graph@1.1.0 --json)
direct_id=$(printf '%s' "$direct" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
"$cli" run "read the repository graph fixture" --session "$direct_id" \
    --option 'graph_requires_approval=false' \
    --option 'graph_iterations=3' --json >/dev/null
direct_inspection=$("$cli" session inspect "$direct_id" --json)
assert_completed_graph "$direct_inspection" 3

approval=$("$cli" session create --workspace "$repository" \
    --style declarative-graph@1.1.0 --json)
approval_id=$(printf '%s' "$approval" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
waiting=$("$cli" run "read after an explicit graph approval" \
    --session "$approval_id" \
    --option 'graph_requires_approval=true' \
    --option 'graph_iterations=1' --json)
continuation=$(printf '%s' "$waiting" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$continuation"
waiting_inspection=$("$cli" session inspect "$approval_id" --json)
printf '%s' "$waiting_inspection" |
    grep -F '"active_node":{"node_id":"approval"' >/dev/null

stop_runtime
start_runtime
"$cli" approval resolve "$approval_id" "$continuation" approve --json \
    >/dev/null
approved_inspection=$("$cli" session inspect "$approval_id" --json)
assert_completed_graph "$approved_inspection" 1
printf '%s' "$approved_inspection" |
    grep -F '"node_id":"approval"' >/dev/null

duplicate=$("$cli" approval resolve "$approval_id" "$continuation" approve --json)
printf '%s' "$duplicate" | grep -F '"transitioned":false' >/dev/null
after_duplicate=$("$cli" session inspect "$approval_id" --json)
assert_completed_graph "$after_duplicate" 1

replayed=$("$cli" session replay "$approval_id" --json)
printf '%s' "$replayed" | grep -F '"command":"session_replay"' >/dev/null
assert_completed_graph "$replayed" 1

echo "runtime declarative branch/loop/tool/approval/restart/replay E2E passed"
