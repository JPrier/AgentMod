#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-e2e.XXXXXX")
endpoint="$run_root/runtime.sock"
token="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_RUNTIME_ENDPOINT="$endpoint"
export AGENTMOD_RUNTIME_AUTH_TOKEN="$token"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"

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
until doctor=$("$repository/target/debug/agentmod" doctor --json 2>/dev/null); do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "runtime did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done
printf '%s' "$doctor" | grep -F '"state":"ready"' >/dev/null

created=$("$repository/target/debug/agentmod" session create \
    --workspace "$repository" --style persistent-chat --json)
session_id=$(printf '%s' "$created" | sed -n \
    's/.*"session_id":"\([^"]*\)".*/\1/p')
[ -n "$session_id" ]

listed=$("$repository/target/debug/agentmod" session list --limit 10 --json)
printf '%s' "$listed" | grep -F "\"id\":\"$session_id\"" >/dev/null

turn=$("$repository/target/debug/agentmod" run "complete the deterministic turn" \
    --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="daemon-turn-ok"' --json)
printf '%s' "$turn" | grep -F '"last_committed_sequence":19' >/dev/null
printf '%s' "$turn" | grep -F '"text":"daemon-turn-ok"' >/dev/null

for required in metadata.json events.jsonl style.json style.lock workspace.json \
    continuations snapshots artifacts process-logs branches; do
    test -e "$run_root/sessions/$session_id/$required"
done
test "$(wc -l < "$run_root/sessions/$session_id/events.jsonl")" -eq 19
for event_type in session.created conversation.entry_committed \
    model.request_proposed model.request_approved model.request_started \
    model.output_delta_observed model.response_completed \
    style.execution_initialized style.node_entered style.node_completed \
    style.transition_selected; do
    grep -F "\"event_type\":\"$event_type\"" \
        "$run_root/sessions/$session_id/events.jsonl" >/dev/null
done
inspection=$("$repository/target/debug/agentmod" session inspect \
    "$session_id" --json)
python3 - "$inspection" <<'PY'
import json
import sys

execution = json.loads(sys.argv[1])["state"]["style_execution"]
assert execution["active_node"] is None
assert [node["node_id"] for node in execution["completed_nodes"]] == [
    "respond",
    "tool",
    "done",
]
assert len(execution["transitions"]) == 2
PY

echo "runtime/CLI/harness durable-turn E2E passed"
