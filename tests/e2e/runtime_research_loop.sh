#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-research-e2e.XXXXXX")
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_SCHEDULER_PROGRAM="$repository/target/debug/agentmod-scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"
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
    unset AGENTMOD_RUNTIME_ENDPOINT AGENTMOD_RUNTIME_AUTH_TOKEN
    unset AGENTMOD_HARNESS_PROGRAM AGENTMOD_SCHEDULER_PROGRAM
    unset AGENTMOD_SCHEDULER_ROOT
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
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log" &
    daemon_pid=$!
    attempt=0
    until doctor=$("$cli" doctor --json 2>/dev/null); do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 80 ]; then
            cat "$run_root/runtime.stderr.log" >&2 || true
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
    printf '%s' "$doctor" | python3 -c '
import json
import sys
assert json.load(sys.stdin)["state"] == "ready"
'
}

session_id_from() {
    python3 - "$1" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["session_id"])
PY
}

assert_legacy_state() {
    inspection_path=$1
    journal_path=$2
    python3 - "$inspection_path" "$journal_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    inspection = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    journal = [json.loads(line) for line in source]

state = inspection["state"]
binding = state["style_binding"]
execution = state["style_execution"]
introspection = state["style_introspection"]
assert binding["id"] == "research-loop"
assert binding["version"] == "1.1.0"
assert state["lifecycle"] == "completed"
assert execution["termination_reason"] == "complete_session"
assert introspection["style"]["id"] == "research-loop"
assert introspection["graph"].get("active_node") is None
assert introspection["graph"]["loop_count"] == 3
assert introspection["graph"]["retry_count"] == 0
assert introspection["graph"]["next_eligible_transitions"] == []
assert introspection["termination_reason"] == "complete_session"
assert len(introspection["graph"]["completed_nodes"]) >= 15
assert len(introspection["graph"]["previous_transitions"]) >= 14
assert introspection["remaining_budgets"]["steps"] >= 0
assert introspection["remaining_budgets"]["tokens"] >= 0
assert introspection["pipeline"]["blocking_interceptor_order"] is not None
assert introspection["memory"]["retrieved_provenance"] is not None
assert len(introspection["compaction"]["history"]) >= 3
assert introspection["child_agents"]["executions"] is not None
assert introspection["child_agents"]["joins"] is not None
assert introspection["child_agents"]["reviewer_findings"] is not None
assert len(state["artifact_persistences"]) == 3
completed = execution["completed_nodes"]
assert sum(node["node_id"] == "persist" for node in completed) == 3
assert sum(node["node_id"] == "repeat" for node in completed) == 3
event_types = [record["event"]["metadata"]["event_type"] for record in journal]
assert event_types.count("artifact.persistence_completed") == 3
assert sum("research_fresh_context" in json.dumps(record) for record in journal) == 3
PY
}

assert_generic_plan() {
    inspection_path=$1
    python3 - "$inspection_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    inspection = json.load(source)
binding = inspection["state"]["style_binding"]
plan = binding["execution_plan"]
assert binding["id"] == "research-loop"
assert binding["version"] == "1.2.0"
assert plan["compilation"]["compiler"] == "agentmod-runtime-node-plan@3"
assert binding["execution_plan_hash"]
assert plan["registry_hash"]
expected = {
    "fresh-context": ("runtime.context-construction", "1.0.0"),
    "research": ("runtime.model-request", "1.0.0"),
    "tool-batch": ("runtime.tool-gate", "1.1.0"),
    "persist": ("runtime.artifact-persistence", "1.0.0"),
    "repeat": ("runtime.loop", "1.0.0"),
    "done": ("runtime.session-completion", "1.0.0"),
}
resolutions = {node["node_id"]: node for node in plan["nodes"]}
assert set(resolutions) == set(expected)
for node_id, (executor_id, executor_version) in expected.items():
    resolution = resolutions[node_id]
    assert resolution["executor_id"] == executor_id
    assert resolution["executor_version"] == executor_version
    assert resolution["source"]["kind"] == "runtime"
    assert resolution["boundary"] == "runtime_logic"
PY
}

assert_generic_state() {
    inspection_path=$1
    journal_path=$2
    session_root=$3
    expected_text=$4
    python3 - "$inspection_path" "$journal_path" "$session_root" \
        "$expected_text" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    inspection = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    journal = [json.loads(line) for line in source]
session_root = pathlib.Path(sys.argv[3])
expected_text = sys.argv[4]

state = inspection["state"]
binding = state["style_binding"]
plan = binding["execution_plan"]
execution = state["style_execution"]
assert binding["id"] == "research-loop"
assert binding["version"] == "1.2.0"
assert plan["compilation"]["compiler"] == "agentmod-runtime-node-plan@3"
expected_executors = {
    "fresh-context": ("runtime.context-construction", "1.0.0"),
    "research": ("runtime.model-request", "1.0.0"),
    "tool-batch": ("runtime.tool-gate", "1.1.0"),
    "persist": ("runtime.artifact-persistence", "1.0.0"),
    "repeat": ("runtime.loop", "1.0.0"),
    "done": ("runtime.session-completion", "1.0.0"),
}
resolutions = {node["node_id"]: node for node in plan["nodes"]}
assert set(resolutions) == set(expected_executors)
for node_id, (executor_id, executor_version) in expected_executors.items():
    resolution = resolutions[node_id]
    assert resolution["executor_id"] == executor_id
    assert resolution["executor_version"] == executor_version
    assert resolution["source"]["kind"] == "runtime"
    assert resolution["boundary"] == "runtime_logic"

assert state["lifecycle"] == "completed"
assert execution["termination_reason"] == "complete_session"
assert execution.get("active_node") is None
contract = execution["execution_contract"]
assert contract["execution_plan_hash"] == binding["execution_plan_hash"]
assert contract["registry_hash"] == plan["registry_hash"]
assert len(contract["node_executors"]) == 6
expected_order = [
    "fresh-context", "research", "tool-batch", "persist", "repeat",
    "fresh-context", "research", "tool-batch", "persist", "repeat",
    "fresh-context", "research", "tool-batch", "persist", "repeat",
    "done",
]
completed = execution["completed_nodes"]
assert [node["node_id"] for node in completed] == expected_order
assert len(completed) == 16
assert len(execution["transitions"]) == 15
for node_id in ("fresh-context", "research", "tool-batch", "persist", "repeat"):
    assert sum(node["node_id"] == node_id for node in completed) == 3
assert sum(node["node_id"] == "done" for node in completed) == 1
assert len(execution["generic_model_invocations"]) == 3

boundaries = execution["context_boundaries"]
assert len(boundaries) == 6
by_run = {}
for boundary in boundaries:
    by_run.setdefault(boundary["identity"]["run_id"], []).append(boundary)
assert len(by_run) == 3
for pair in by_run.values():
    assert len(pair) == 2
    assert sorted(item["identity"]["boundary"] for item in pair) == [
        "before_model_request", "turn_start"
    ]
    assert sorted(item["identity"]["node_id"] for item in pair) == [
        "fresh-context", "research"
    ]

entries = execution["canonical_variables"]["environment"]["entries"]
assert set(entries) == {
    "iteration",
    "model_disposition",
    "model_result",
    "receipt_artifact",
    "research_receipt",
}
assert entries["model_disposition"]["version"] == 3
assert entries["model_disposition"]["value"] == {
    "kind": "enum", "value": "response_complete"
}
assert entries["model_result"]["version"] == 3
assert entries["model_result"]["value"]["kind"] == "node_result_reference"
assert entries["research_receipt"]["version"] == 3
assert entries["research_receipt"]["value"]["kind"] == "node_result_reference"
assert entries["receipt_artifact"]["version"] == 3
assert entries["receipt_artifact"]["value"]["kind"] == "artifact_reference"
assert entries["iteration"]["version"] == 3
assert entries["iteration"]["value"] == {
    "kind": "map",
    "value": {"remaining": {"kind": "boolean", "value": False}},
}

artifacts = list(state["artifact_persistences"].values())
assert len(artifacts) == 3
for artifact in artifacts:
    content_hash = artifact["identity"]["content_hash"]
    content = (
        session_root
        / "artifacts"
        / "style"
        / "objects"
        / content_hash[:2]
        / content_hash
        / "content"
    )
    assert artifact["state"] == "completed"
    assert artifact["mime_type"] == "text/markdown"
    assert content.read_text(encoding="utf-8") == expected_text

event_types = [record["event"]["metadata"]["event_type"] for record in journal]
expected_counts = {
    "context.boundary_started": 6,
    "context.boundary_completed": 6,
    "graph.model_invocation_bound": 3,
    "model.request_proposed": 3,
    "model.request_approved": 3,
    "model.request_started": 3,
    "model.response_completed": 3,
    "artifact.persistence_proposed": 3,
    "artifact.persistence_approved": 3,
    "artifact.persistence_dispatched": 3,
    "artifact.persistence_completed": 3,
    "graph.variable_declared": 5,
    "graph.variable_assigned": 15,
    "style.node_entered": 16,
    "style.node_completed": 16,
    "style.transition_selected": 15,
}
for event_type, expected in expected_counts.items():
    assert event_types.count(event_type) == expected, (
        event_type,
        event_types.count(event_type),
        expected,
    )
assert sum("generic_fresh_context" in json.dumps(record) for record in journal) == 3
assert not any("research_fresh_context" in json.dumps(record) for record in journal)
PY
}

start_runtime

"$cli" session create --workspace "$repository" \
    --style research-loop@1.1.0 --json >"$run_root/legacy-created.json"
legacy_id=$(session_id_from "$run_root/legacy-created.json")
legacy_root="$run_root/sessions/$legacy_id"
legacy_journal="$legacy_root/events.jsonl"
"$cli" run "map the repository architecture" --session "$legacy_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="deterministic finding"' \
    --option 'research_complete_after=3' --json >"$run_root/legacy-result.json"
"$cli" session inspect "$legacy_id" --json >"$run_root/legacy-inspection.json"
assert_legacy_state "$run_root/legacy-inspection.json" "$legacy_journal"
python3 - "$run_root/legacy-result.json" "$legacy_journal" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    journal = list(source)
assert result["last_committed_sequence"] == len(journal)
PY
cp "$legacy_journal" "$run_root/legacy-journal.before"

"$cli" session create --workspace "$repository" \
    --style research-loop@1.2.0 --json >"$run_root/generic-created.json"
generic_id=$(session_id_from "$run_root/generic-created.json")
generic_root="$run_root/sessions/$generic_id"
generic_journal="$generic_root/events.jsonl"
"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-created-inspection.json"
assert_generic_plan "$run_root/generic-created-inspection.json"
expected_text="alpha beta generic research finding"
"$cli" run "map the repository architecture" --session "$generic_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="generic research finding"' \
    --json >"$run_root/generic-result.json"
"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-inspection.json"
assert_generic_state "$run_root/generic-inspection.json" "$generic_journal" \
    "$generic_root" "$expected_text"
python3 - "$run_root/generic-result.json" "$generic_journal" \
    "$expected_text" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    journal = list(source)
expected = sys.argv[3]
visible = "".join(
    event["text"] for event in result["events"] if event["event"] == "text"
)
assert visible == expected * 3
assert result["last_committed_sequence"] == len(journal)
PY
python3 - "$run_root/generic-inspection.json" \
    "$run_root/generic-binding.before" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    binding = json.load(source)["state"]["style_binding"]
with open(sys.argv[2], "w", encoding="utf-8", newline="\n") as target:
    json.dump(binding, target, sort_keys=True, separators=(",", ":"))
    target.write("\n")
PY
cp "$generic_root/style.json" "$run_root/generic-style.json.before"
cp "$generic_root/style.lock" "$run_root/generic-style.lock.before"
cp "$generic_journal" "$run_root/generic-journal.before"

stop_runtime
start_runtime

"$cli" session inspect "$legacy_id" --json \
    >"$run_root/legacy-restarted.json"
assert_legacy_state "$run_root/legacy-restarted.json" "$legacy_journal"
"$cli" session replay "$legacy_id" --json >"$run_root/legacy-replayed.json"
assert_legacy_state "$run_root/legacy-replayed.json" "$legacy_journal"
python3 - "$run_root/legacy-replayed.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    assert json.load(source)["command"] == "session_replay"
PY
cmp -s "$run_root/legacy-journal.before" "$legacy_journal"

"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-restarted.json"
assert_generic_state "$run_root/generic-restarted.json" "$generic_journal" \
    "$generic_root" "$expected_text"
cmp -s "$run_root/generic-style.json.before" "$generic_root/style.json"
cmp -s "$run_root/generic-style.lock.before" "$generic_root/style.lock"
cmp -s "$run_root/generic-journal.before" "$generic_journal"
python3 - "$run_root/generic-restarted.json" \
    "$run_root/generic-binding.before" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    binding = json.load(source)["state"]["style_binding"]
with open(sys.argv[2], encoding="utf-8") as source:
    expected = json.load(source)
assert binding == expected
PY

"$cli" session replay "$generic_id" --json >"$run_root/generic-replayed.json"
assert_generic_state "$run_root/generic-replayed.json" "$generic_journal" \
    "$generic_root" "$expected_text"
python3 - "$run_root/generic-replayed.json" \
    "$run_root/generic-binding.before" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    replay = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    expected_binding = json.load(source)
assert replay["command"] == "session_replay"
assert replay["state"]["style_binding"] == expected_binding
PY
cmp -s "$run_root/generic-journal.before" "$generic_journal"

echo "runtime research-loop frozen-1.1/current-1.2 exact-plan/generic-dispatch/typed-variables/artifacts/context-identities/restart/replay E2E passed"
