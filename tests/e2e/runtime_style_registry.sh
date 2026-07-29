#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-style-e2e.XXXXXX")
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
    rm -rf -- "$run_root"
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

start_runtime
styles=$("$cli" style list --json)
for style in persistent-chat ephemeral-turn research-loop planner-worker declarative-graph; do
    printf '%s' "$styles" | grep -F "\"id\":\"$style\"" >/dev/null
done
test "$(printf '%s' "$styles" | grep -o '"availability":"available"' | wc -l)" -eq 5

persistent=$("$cli" session create --workspace "$repository" \
    --style persistent-chat --json)
persistent_id=$(printf '%s' "$persistent" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
ephemeral=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.1.0 --json)
ephemeral_id=$(printf '%s' "$ephemeral" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$persistent_id"
test -n "$ephemeral_id"

for pair in \
    "$persistent_id:persistent-chat:1.1.0" \
    "$ephemeral_id:ephemeral-turn:1.1.0"
do
    session_id=${pair%%:*}
    remainder=${pair#*:}
    style=${remainder%%:*}
    version=${remainder#*:}
    inspected=$("$cli" session inspect "$session_id" --json)
    printf '%s' "$inspected" | grep -F "\"id\":\"$style\"" >/dev/null
    printf '%s' "$inspected" | grep -F "\"version\":\"$version\"" >/dev/null
    printf '%s' "$inspected" | grep -F '"harness":"native"' >/dev/null
    printf '%s' "$inspected" | grep -F '"style_compatibility":{"status":"compatible"}' >/dev/null
done

persistent_before=$("$cli" run "persistent before restart" --session "$persistent_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="persistent-before"' --json)
persistent_before_sequence=$(printf '%s' "$persistent_before" |
    sed -n 's/.*"last_committed_sequence":\([0-9][0-9]*\).*/\1/p')
persistent_journal="$run_root/sessions/$persistent_id/events.jsonl"
test -n "$persistent_before_sequence"
test "$persistent_before_sequence" -eq "$(wc -l <"$persistent_journal" | tr -d ' ')"
ephemeral_before=$("$cli" run "ephemeral before restart" --session "$ephemeral_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="ephemeral-before"' --json)
ephemeral_before_sequence=$(printf '%s' "$ephemeral_before" |
    sed -n 's/.*"last_committed_sequence":\([0-9][0-9]*\).*/\1/p')
ephemeral_journal="$run_root/sessions/$ephemeral_id/events.jsonl"
test -n "$ephemeral_before_sequence"
test "$ephemeral_before_sequence" -eq \
    "$(wc -l <"$ephemeral_journal" | tr -d ' ')"

stop_runtime
start_runtime
for session_id in "$persistent_id" "$ephemeral_id"; do
    "$cli" session inspect "$session_id" --json |
        grep -F '"style_compatibility":{"status":"compatible"}' >/dev/null
done
persistent_after=$("$cli" run "persistent after restart" --session "$persistent_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="persistent-after"' --json)
persistent_after_sequence=$(printf '%s' "$persistent_after" |
    sed -n 's/.*"last_committed_sequence":\([0-9][0-9]*\).*/\1/p')
test -n "$persistent_after_sequence"
test "$persistent_after_sequence" -gt "$persistent_before_sequence"
test "$persistent_after_sequence" -eq "$(wc -l <"$persistent_journal" | tr -d ' ')"
ephemeral_after=$("$cli" run "ephemeral after restart" --session "$ephemeral_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="ephemeral-after"' --json)
ephemeral_after_sequence=$(printf '%s' "$ephemeral_after" |
    sed -n 's/.*"last_committed_sequence":\([0-9][0-9]*\).*/\1/p')
test -n "$ephemeral_after_sequence"
test "$ephemeral_after_sequence" -gt "$ephemeral_before_sequence"
test "$ephemeral_after_sequence" -eq \
    "$(wc -l <"$ephemeral_journal" | tr -d ' ')"

branch=$("$cli" session branch "$persistent_id" --at "$persistent_before_sequence" \
    --style ephemeral-turn --json)
branch_id=$(printf '%s' "$branch" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
"$cli" session inspect "$branch_id" --json |
    grep -F '"id":"ephemeral-turn"' >/dev/null
"$cli" run "parent continues persistently" --session "$persistent_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="parent-continuation"' --json >/dev/null
"$cli" run "branch continues ephemerally" --session "$branch_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="branch-continuation"' --json >/dev/null
"$cli" session inspect "$persistent_id" --json |
    grep -F '"id":"persistent-chat"' >/dev/null
branch_after=$("$cli" session inspect "$branch_id" --json)
printf '%s' "$branch_after" | grep -F '"id":"ephemeral-turn"' >/dev/null
printf '%s' "$branch_after" | grep -F '"provider_projection":[]' >/dev/null

mkdir -p "$run_root/styles/user"
printf disabled >"$run_root/styles/user/persistent-chat.disabled"
"$cli" session inspect "$persistent_id" --json |
    grep -F '"status":"incompatible"' >/dev/null
if "$cli" run "must not silently substitute" --session "$persistent_id" \
    --option 'mock_scenario="streaming_text"' --json >/dev/null 2>&1; then
    echo "disabled persisted style executed through a fallback" >&2
    exit 1
fi
rm -- "$run_root/styles/user/persistent-chat.disabled"

for session_id in "$persistent_id" "$ephemeral_id"; do
    grep -F '"schema_version":2' \
        "$run_root/sessions/$session_id/metadata.json" >/dev/null
    grep -F '"style_binding":' \
        "$run_root/sessions/$session_id/metadata.json" >/dev/null
    grep -F '"binding":' "$run_root/sessions/$session_id/style.lock" >/dev/null
    grep -F '"compiled":' "$run_root/sessions/$session_id/style.lock" >/dev/null
done

echo "runtime session-style registry/restart/branch E2E passed"
