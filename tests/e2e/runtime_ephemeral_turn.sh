#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-ephemeral-e2e.XXXXXX")
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
        "${TMPDIR:-/tmp}"/agentmod-ephemeral-e2e.*)
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

assert_empty_projection() {
    inspected=$("$cli" session inspect "$1" --json)
    printf '%s' "$inspected" |
        grep -F '"provider_projection":[]' >/dev/null
}

start_runtime
session=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.1.0 --json)
session_id=$(printf '%s' "$session" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"

first_prompt=turn-one-secret-input
first_output=turn-one-secret-output
"$cli" run "$first_prompt" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$first_output\"" --json >/dev/null
assert_empty_projection "$session_id"
first_inspection=$("$cli" session inspect "$session_id" --json)
printf '%s' "$first_inspection" | grep -F "$first_prompt" >/dev/null
printf '%s' "$first_inspection" | grep -F "$first_output" >/dev/null

stop_runtime
start_runtime
assert_empty_projection "$session_id"
restart_inspection=$("$cli" session inspect "$session_id" --json)
printf '%s' "$restart_inspection" | grep -F "$first_prompt" >/dev/null
printf '%s' "$restart_inspection" | grep -F "$first_output" >/dev/null

second_prompt=turn-two-current-input
second_output=turn-two-output
"$cli" run "$second_prompt" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$second_output\"" --json >/dev/null
assert_empty_projection "$session_id"
final_inspection=$("$cli" session inspect "$session_id" --json)
for expected in "$first_prompt" "$first_output" "$second_prompt" "$second_output"; do
    printf '%s' "$final_inspection" | grep -F "$expected" >/dev/null
done

journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c 'ephemeral_fresh_context' "$journal")" -eq 2
test "$(grep -c 'ephemeral_discard' "$journal")" -eq 2
second_fresh=$(grep 'ephemeral_fresh_context' "$journal" | tail -n 1)
printf '%s' "$second_fresh" | grep -F "$second_prompt" >/dev/null
if printf '%s' "$second_fresh" |
    grep -E "$first_prompt|$first_output" >/dev/null
then
    echo "second fresh projection leaked unselected turn-one state" >&2
    exit 1
fi

echo "runtime ephemeral-turn fresh-context/restart E2E passed"
