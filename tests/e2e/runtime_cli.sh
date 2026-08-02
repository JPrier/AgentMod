#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-e2e.XXXXXX")
endpoint="$run_root/runtime.sock"
token="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_RUNTIME_ENDPOINT="$endpoint"
export AGENTMOD_RUNTIME_AUTH_TOKEN="$token"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_SCHEDULER_PROGRAM="$repository/target/debug/agentmod-scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"

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
printf '%s' "$turn" | grep -F '"text":"daemon-turn-ok"' >/dev/null

for required in metadata.json events.jsonl style.json style.lock workspace.json \
    continuations snapshots artifacts process-logs branches; do
    test -e "$run_root/sessions/$session_id/$required"
done
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
python3 - "$turn" "$inspection" \
    "$run_root/sessions/$session_id/events.jsonl" <<'PY'
import json
import sys

turn = json.loads(sys.argv[1])
inspection = json.loads(sys.argv[2])
with open(sys.argv[3], encoding="utf-8") as journal:
    records = [json.loads(line) for line in journal]
assert turn["last_committed_sequence"] == len(records)
event_types = [record["event"]["metadata"]["event_type"] for record in records]
visible = "".join(
    event["text"] for event in turn["events"] if event["event"] == "text"
)
assert visible == "alpha beta daemon-turn-ok"
expected_counts = {
    "session.created": 1,
    "conversation.entry_committed": 2,
    "style.execution_initialized": 1,
    "style.node_entered": 4,
    "style.node_completed": 4,
    "style.transition_selected": 3,
    "model.request_proposed": 1,
    "model.request_approved": 1,
    "model.request_started": 1,
    "model.response_completed": 1,
    "memory.write_proposed": 2,
    "memory.write_approved": 2,
    "memory.write_dispatched": 2,
    "memory.write_completed": 2,
}
for event_type, expected in expected_counts.items():
    assert event_types.count(event_type) == expected
assert event_types.count("model.output_delta_observed") >= 1
provider_order = [
    event_types.index(event_type)
    for event_type in (
        "model.request_proposed",
        "model.request_approved",
        "model.request_started",
        "model.response_completed",
    )
]
assert provider_order == sorted(provider_order)
assert event_types[0] == "session.created"
assert event_types[-8:] == [
    "memory.write_proposed",
    "memory.write_approved",
    "memory.write_dispatched",
    "memory.write_completed",
    "memory.write_proposed",
    "memory.write_approved",
    "memory.write_dispatched",
    "memory.write_completed",
]

state = inspection["state"]
binding = state["style_binding"]
execution = state["style_execution"]
assert binding["id"] == "persistent-chat"
assert binding["version"] == "1.2.0"
assert binding["execution_plan"]["compilation"]["compiler"] == \
    "agentmod-runtime-node-plan@3"
expected_executors = {
    "prepare-context": ("runtime.context-construction", "1.0.0"),
    "respond": ("runtime.model-request", "1.0.0"),
    "tool-batch": ("runtime.tool-gate", "1.1.0"),
    "done": ("runtime.turn-completion", "1.0.0"),
}
resolutions = {
    node["node_id"]: node for node in binding["execution_plan"]["nodes"]
}
assert set(resolutions) == set(expected_executors)
for node_id, (executor_id, executor_version) in expected_executors.items():
    resolution = resolutions[node_id]
    assert resolution["executor_id"] == executor_id
    assert resolution["executor_version"] == executor_version
    assert resolution["source"]["kind"] == "runtime"
    assert resolution["boundary"] == "runtime_logic"
contract = execution["execution_contract"]
assert contract["execution_plan_hash"] == binding["execution_plan_hash"]
assert contract["registry_hash"] == binding["execution_plan"]["registry_hash"]
assert execution.get("active_node") is None
assert [node["node_id"] for node in execution["completed_nodes"]] == [
    "prepare-context",
    "respond",
    "tool-batch",
    "done",
]
assert len(execution["transitions"]) == 3
assert len(state["conversation"]["provider_projection"]) == 2
PY

echo "runtime/CLI/harness persistent-chat 1.2 generic durable-turn E2E passed"
