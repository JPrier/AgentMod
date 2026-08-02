#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli

target_dir=${CARGO_TARGET_DIR:-target}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
scheduler="$target_dir/debug/agentmod-scheduler"
cli="$target_dir/debug/agentmod"
run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-planner-e2e.XXXXXX")
runtime_pid=
succeeded=false

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
0123456789abcdef0123456789abcdef0123456789abcdef
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"

stop_runtime() {
    if [ -n "${runtime_pid:-}" ] && kill -0 "$runtime_pid" 2>/dev/null; then
        kill "$runtime_pid"
        wait "$runtime_pid" 2>/dev/null || true
    fi
    runtime_pid=
}

cleanup() {
    stop_runtime
    unset AGENTMOD_RUNTIME_ENDPOINT AGENTMOD_RUNTIME_AUTH_TOKEN
    unset AGENTMOD_HARNESS_PROGRAM AGENTMOD_SCHEDULER_PROGRAM
    unset AGENTMOD_SCHEDULER_ROOT
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-planner-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed planner-worker E2E root: $run_root" >&2
            fi
            ;;
        *) echo "refusing unexpected planner-worker E2E root: $run_root" >&2 ;;
    esac
}
trap cleanup EXIT HUP INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log" &
    runtime_pid=$!
    attempt=0
    while [ "$attempt" -lt 120 ]; do
        if doctor=$("$cli" doctor --json 2>/dev/null); then
            printf '%s' "$doctor" | python3 -c '
import json
import sys
assert json.load(sys.stdin)["state"] == "ready"
'
            return
        fi
        if ! kill -0 "$runtime_pid" 2>/dev/null; then
            break
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    cat "$run_root/runtime.stderr.log" >&2 || true
    echo "planner-worker runtime did not become ready" >&2
    exit 1
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
    python3 - "$1" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
assert state["style_binding"]["id"] == "planner-worker"
assert state["style_binding"]["version"] == "1.1.0"
assert state["lifecycle"] == "completed"
assert state["style_execution"]["termination_reason"] == "complete_session"
assert len(state["planner_worker"]["tasks"]) == 2
assert [review["approved"] for review in state["planner_worker"]["reviews"]] == [
    False, True
]
assert len(state["child_agents"]) == 3
assert len(state["planner_worker"]["joins"]) == 2
PY
}

assert_generic_plan() {
    python3 - "$1" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
binding = state["style_binding"]
plan = binding["execution_plan"]
expected = {
    "plan": ("runtime.model-request", "1.0.0"),
    "plan-route": ("runtime.conditional", "1.0.0"),
    "spawn-planner": ("runtime.child-spawn", "1.0.0"),
    "spawn-evidence": ("runtime.child-spawn", "1.0.0"),
    "worker-fanout": ("runtime.parallel", "1.0.0"),
    "wait-planner": ("runtime.child-wait", "1.0.0"),
    "wait-evidence": ("runtime.child-wait", "1.0.0"),
    "join-workers": ("runtime.join", "1.0.0"),
    "integrate": ("runtime.model-request", "1.0.0"),
    "integration-route": ("runtime.conditional", "1.0.0"),
    "persist-integration": ("runtime.artifact-persistence", "1.0.0"),
    "review": ("runtime.review", "1.0.0"),
    "revision": ("runtime.loop", "1.0.0"),
    "done": ("runtime.session-completion", "1.0.0"),
    "structured-failure": ("runtime.structured-failure", "1.0.0"),
}
assert binding["id"] == "planner-worker"
assert binding["version"] == "1.3.0"
assert binding["execution_plan_hash"]
assert plan["registry_hash"]
assert plan["compilation"]["compiler"] == "agentmod-runtime-node-plan@3"
assert plan["compilation"]["compiled_style_hash"] == binding["compiled_style_hash"]
assert plan["compilation"]["compiled_cache_key"] == binding["compiled_cache_key"]
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
    inspection=$1
    journal=$2
    session_root=$3
    python3 - "$inspection" "$journal" "$session_root" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
with open(sys.argv[2], encoding="utf-8") as source:
    events = [json.loads(line)["event"] for line in source]
session_root = pathlib.Path(sys.argv[3])

binding = state["style_binding"]
plan = binding["execution_plan"]
execution = state["style_execution"]
assert binding["id"] == "planner-worker"
assert binding["version"] == "1.3.0"
assert state["lifecycle"] == "completed"
assert execution["termination_reason"] == "complete_session"
assert execution.get("active_node") is None
contract = execution["execution_contract"]
assert contract["execution_plan_hash"] == binding["execution_plan_hash"]
assert contract["registry_hash"] == plan["registry_hash"]
assert len(contract["node_executors"]) == 15

completed = [node["node_id"] for node in execution["completed_nodes"]]
expected_counts = {
    "plan": 1,
    "plan-route": 1,
    "worker-fanout": 2,
    "spawn-planner": 0,
    "spawn-evidence": 0,
    "wait-planner": 0,
    "wait-evidence": 0,
    "join-workers": 2,
    "integrate": 2,
    "integration-route": 2,
    "persist-integration": 2,
    "review": 2,
    "revision": 1,
    "done": 1,
    "structured-failure": 0,
}
for node_id, expected in expected_counts.items():
    assert completed.count(node_id) == expected, (
        node_id, completed.count(node_id), expected
    )

entries = execution["canonical_variables"]["environment"]["entries"]
assert set(entries) == {
    "plan_disposition",
    "plan_result",
    "planner_task",
    "evidence_task",
    "planner_child",
    "evidence_child",
    "joined_results",
    "integration_disposition",
    "integration_result",
    "integration_artifact",
    "iteration",
}
assert entries["plan_disposition"]["version"] == 1
assert entries["plan_disposition"]["value"] == {
    "kind": "enum", "value": "response_complete"
}
assert entries["planner_task"]["version"] == 1
assert entries["planner_task"]["value"]["kind"] == "string"
assert entries["evidence_task"]["version"] == 1
assert entries["evidence_task"]["value"]["kind"] == "string"
for child in ("planner_child", "evidence_child"):
    assert entries[child]["version"] == 2
    assert entries[child]["value"]["kind"] == "child_id"
assert entries["joined_results"]["version"] == 2
assert entries["joined_results"]["value"]["kind"] == "node_result_reference"
assert entries["integration_disposition"]["version"] == 2
assert entries["integration_disposition"]["value"] == {
    "kind": "enum", "value": "response_complete"
}
assert entries["integration_result"]["version"] == 2
assert entries["integration_result"]["value"]["kind"] == "node_result_reference"
assert entries["integration_artifact"]["version"] == 2
assert entries["integration_artifact"]["value"]["kind"] == "artifact_reference"
assert entries["iteration"]["version"] == 1
assert entries["iteration"]["value"] == {
    "kind": "map",
    "value": {"remaining": {"kind": "boolean", "value": True}},
}

children = list(state["child_agents"].values())
assert len(children) == 4
assert sum(
    child["identity"]["task_id"] == "planner-task-0" for child in children
) == 2
assert sum(
    child["identity"]["task_id"] == "evidence-task-0" for child in children
) == 2
assert all(child["state"] == "completed" for child in children)
for child in children:
    lease = child["workspace_lease"]
    assert lease["mode"]["mode"] == "shared_read_only"
    assert lease["ownership"] == "borrowed_read_only"
    assert lease["owner"]["parent_session_id"] == state["id"]
    assert lease["owner"]["task_id"] == child["identity"]["task_id"]
    assert lease["lease_id"]
    assert lease["lease_hash"]
    assert (
        lease["source_snapshot_hash"] == lease["materialized_snapshot_hash"]
    )
    child_journal = (
        session_root.parent / child["child_session_id"] / "events.jsonl"
    )
    child_events = [
        json.loads(line)["event"]
        for line in child_journal.read_text(encoding="utf-8").splitlines()
    ]
    assert sum(
        event["metadata"]["event_type"]
        == "child_session.workspace_lease_bound"
        for event in child_events
    ) == 1

artifacts = list(state["artifact_persistences"].values())
assert len(artifacts) == 2
iterations = []
for artifact in artifacts:
    content_hash = artifact["identity"]["content_hash"]
    content_path = (
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
    content = json.loads(content_path.read_text(encoding="utf-8"))
    assert content["integration"] == "combined runtime-owned child handoffs"
    assert content["tests"] == "deterministic fixture passed"
    iterations.append(artifact["identity"]["loop_iteration"])
assert sorted(iterations) == [0, 1]

types = [event["metadata"]["event_type"] for event in events]
event_counts = {
    "style.execution_initialized": 1,
    "graph.variable_declared": 11,
    "child_agent.generic_creation_dispatched": 4,
    "child_agent.generic_created": 4,
    "graph.parallel_initialized": 2,
    "graph.parallel_branch_dispatched": 4,
    "graph.generic_join_ready": 2,
    "artifact.persistence_completed": 2,
    "style.generic_review_routed": 2,
}
for event_type, expected in event_counts.items():
    assert types.count(event_type) == expected, (
        event_type, types.count(event_type), expected
    )
reviews = [
    event["payload"]["payload"]
    for event in events
    if event["metadata"]["event_type"] == "style.generic_review_routed"
]
revision, approved = reviews
revision_evidence = revision["evidence"]
assert revision["approved"] is False
assert revision["rejected_task_ids"] == ["evidence-task-0"]
assert revision["findings"] == [
    "evidence task requires one artifact-bound revision"
]
assert revision_evidence["disposition"] == "revision"
assert revision_evidence["destination_node_id"] == "revision"
assert revision_evidence["current_revision"] == 0
assert revision_evidence["next_revision"] == 1
assert revision_evidence["structured_findings"] == [
    {
        "code": "planner.evidence_revision",
        "message": "evidence task requires one artifact-bound revision",
        "artifact_references": ["integration_artifact"],
    }
]
assert revision_evidence["evidence_hash"]
assert revision_evidence["application_hash"]
approved_evidence = approved["evidence"]
assert approved["approved"] is True
assert approved["rejected_task_ids"] == []
assert approved["findings"] == []
assert approved_evidence["disposition"] == "approved"
assert approved_evidence["destination_node_id"] == "done"
assert approved_evidence["current_revision"] == 1
assert approved_evidence.get("next_revision") is None
assert approved_evidence["structured_findings"] == []
assert approved_evidence["evidence_hash"]
assert approved_evidence["application_hash"]
parallel = [
    event
    for event in events
    if event["metadata"]["event_type"] == "graph.parallel_branch_node_entered"
]
assert len(parallel) == 8
assert all(
    event["payload"]["payload"]["work"]["loop_iteration"]
    == event["payload"]["payload"]["owner"]["loop_iteration"]
    for event in parallel
)
parallel_completions = [
    event["payload"]["payload"]["entered"]["work"]["node_id"]
    for event in events
    if event["metadata"]["event_type"]
    == "graph.parallel_branch_node_completed"
]
assert sorted(parallel_completions) == [
    "spawn-evidence",
    "spawn-evidence",
    "spawn-planner",
    "spawn-planner",
    "wait-evidence",
    "wait-evidence",
    "wait-planner",
    "wait-planner",
]
PY
}

generic_run() {
    "$cli" run "verify generic planner child orchestration" \
        --session "$generic_id" \
        --provider deterministic-mock \
        --model mock-model \
        --cancellation-id "$generic_turn_id" \
        --option 'mock_scenario="planner_worker"' \
        --json
}

continuation_from() {
    printf '%s' "$1" | python3 -c '
import json
import sys
print(json.load(sys.stdin).get("awaiting_continuation") or "")
'
}

resolve_spawn_approvals() {
    result=$1
    expected=${2:-2}
    continuation=$(continuation_from "$result")
    approval=0
    while [ "$approval" -lt "$expected" ]; do
        if [ -z "$continuation" ]; then
            echo "expected $expected child approvals, found $approval" >&2
            return 1
        fi
        result=$("$cli" approval resolve "$generic_id" \
            "$continuation" approve --json)
        approval=$((approval + 1))
        if [ "$approval" -lt "$expected" ]; then
            "$cli" session inspect "$generic_id" --json \
                >"$run_root/pending-approvals.json"
            continuation=$(python3 - "$run_root/pending-approvals.json" \
                "$((expected - approval))" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    approvals = json.load(source)["state"]["approvals"]
pending = sorted(
    continuation
    for continuation, record in approvals.items()
    if record["state"] == "pending"
)
assert len(pending) == int(sys.argv[2])
print(pending[0])
PY
)
        fi
    done
    printf '%s' "$result"
}

collect_children() {
    "$cli" session list --limit 64 --json >"$run_root/sessions.json"
    python3 - "$run_root/sessions.json" >"$run_root/session-ids.txt" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    for session in json.load(source)["sessions"]:
        print(session["id"])
PY
    : >"$run_root/children.tsv"
    while IFS= read -r child_id; do
        [ "$child_id" = "$generic_id" ] && continue
        child_path="$run_root/child-$child_id.json"
        "$cli" session inspect "$child_id" --json >"$child_path"
        python3 - "$child_path" "$generic_id" >>"$run_root/children.tsv" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
origin = state.get("child_origin")
if origin and origin["parent_session_id"] == sys.argv[2]:
    print(
        state["id"],
        origin["task_id"],
        origin["revision"],
        state["lifecycle"],
        sep="\t",
    )
PY
    done <"$run_root/session-ids.txt"
    sort -t "$(printf '\t')" -k3,3n -k2,2 \
        "$run_root/children.tsv" >"$run_root/children.sorted.tsv"
    mv "$run_root/children.sorted.tsv" "$run_root/children.tsv"
}

complete_child() {
    child_id=$1
    text=$2
    child_path="$run_root/child-$child_id.json"
    "$cli" session inspect "$child_id" --json >"$child_path"
    task=$(python3 - "$child_path" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["state"]["child_origin"]["task"])
PY
)
    cancellation_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
    "$cli" run "$task" \
        --session "$child_id" \
        --provider deterministic-mock \
        --model mock-model \
        --option 'mock_scenario="streaming_text"' \
        --option "mock_text=\"$text\"" \
        --cancellation-id "$cancellation_id" \
        --json >/dev/null
    "$cli" session inspect "$child_id" --json |
        python3 -c '
import json
import sys
assert json.load(sys.stdin)["state"]["lifecycle"] == "completed"
'
}

start_runtime

"$cli" session create --workspace "$repository" \
    --style planner-worker@1.1.0 --json >"$run_root/legacy-created.json"
legacy_id=$(session_id_from "$run_root/legacy-created.json")
legacy_root="$run_root/sessions/$legacy_id"
legacy_journal="$legacy_root/events.jsonl"
"$cli" run "verify frozen planner child orchestration" \
    --session "$legacy_id" \
    --provider mock \
    --model mock-model \
    --option 'mock_scenario="planner_worker"' \
    --json >"$run_root/legacy-result.json"
"$cli" session inspect "$legacy_id" --json \
    >"$run_root/legacy-inspection.json"
assert_legacy_state "$run_root/legacy-inspection.json"
python3 - "$run_root/legacy-result.json" "$legacy_journal" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    events = [json.loads(line)["event"] for line in source]
types = [event["metadata"]["event_type"] for event in events]
assert types.count("child_agent.created") == 3
assert types.count("child_agent.join_completed") == 2
assert types.count("style.reviewer_findings_committed") == 2
assert result["last_committed_sequence"] == len(events)
PY
cp "$legacy_journal" "$run_root/legacy-journal.before"

"$cli" session create --workspace "$repository" \
    --style planner-worker@1.3.0 --json >"$run_root/generic-created.json"
generic_id=$(session_id_from "$run_root/generic-created.json")
generic_root="$run_root/sessions/$generic_id"
generic_journal="$generic_root/events.jsonl"
generic_turn_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-created-inspection.json"
assert_generic_plan "$run_root/generic-created-inspection.json"

waiting=$(generic_run)
test -n "$(continuation_from "$waiting")"
stop_runtime
start_runtime
waiting=$(resolve_spawn_approvals "$waiting")
test -n "$(continuation_from "$waiting")"

collect_children
test "$(wc -l <"$run_root/children.tsv" | tr -d ' ')" -eq 2
evidence_child=$(awk -F '\t' '$2 == "evidence-task-0" { print $1; exit }' \
    "$run_root/children.tsv")
planner_child=$(awk -F '\t' '$2 == "planner-task-0" { print $1; exit }' \
    "$run_root/children.tsv")
test -n "$evidence_child"
test -n "$planner_child"
complete_child "$evidence_child" evidence-child-completed-first
stop_runtime
start_runtime
waiting=$(generic_run)
test "$(grep -c '"event_type":"graph.generic_join_ready"' \
    "$generic_journal" || true)" -eq 0

complete_child "$planner_child" planner-child-completed-second
waiting=$(generic_run)
waiting=$(resolve_spawn_approvals "$waiting")
test -n "$(continuation_from "$waiting")"
collect_children
test "$(wc -l <"$run_root/children.tsv" | tr -d ' ')" -eq 4
revision_ids=$(awk -F '\t' '$3 == "1" { print $1 }' \
    "$run_root/children.tsv")
test "$(printf '%s\n' "$revision_ids" | sed '/^$/d' | wc -l | tr -d ' ')" -eq 2
revision_index=0
for child_id in $revision_ids; do
    complete_child "$child_id" "revised-child-$revision_index-completed"
    revision_index=$((revision_index + 1))
done

"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-completed.json"
recovery=0
while [ "$recovery" -lt 4 ]; do
    generic_run >"$run_root/generic-result.json"
    "$cli" session inspect "$generic_id" --json \
        >"$run_root/generic-completed.json"
    if python3 - "$run_root/generic-completed.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    completed = json.load(source)["state"]["lifecycle"] == "completed"
raise SystemExit(0 if completed else 1)
PY
    then
        break
    fi
    test -n "$(python3 - "$run_root/generic-result.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source).get("awaiting_continuation") or "")
PY
)"
    recovery=$((recovery + 1))
done
assert_generic_plan "$run_root/generic-completed.json"
assert_generic_state "$run_root/generic-completed.json" \
    "$generic_journal" "$generic_root"
python3 - "$run_root/generic-result.json" "$generic_journal" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    journal = list(source)
assert result["last_committed_sequence"] == len(journal)
PY
python3 - "$run_root/generic-completed.json" \
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
assert_legacy_state "$run_root/legacy-restarted.json"
"$cli" session replay "$legacy_id" --json \
    >"$run_root/legacy-replayed.json"
assert_legacy_state "$run_root/legacy-replayed.json"
python3 - "$run_root/legacy-replayed.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    assert json.load(source)["command"] == "session_replay"
PY
cmp -s "$run_root/legacy-journal.before" "$legacy_journal"

"$cli" session inspect "$generic_id" --json \
    >"$run_root/generic-restarted.json"
assert_generic_plan "$run_root/generic-restarted.json"
assert_generic_state "$run_root/generic-restarted.json" \
    "$generic_journal" "$generic_root"
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

"$cli" session replay "$generic_id" --json \
    >"$run_root/generic-replayed.json"
assert_generic_plan "$run_root/generic-replayed.json"
assert_generic_state "$run_root/generic-replayed.json" \
    "$generic_journal" "$generic_root"
python3 - "$run_root/generic-replayed.json" \
    "$run_root/generic-binding.before" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    replay = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    expected = json.load(source)
assert replay["command"] == "session_replay"
assert replay["state"]["style_binding"] == expected
PY
cmp -s "$run_root/generic-journal.before" "$generic_journal"

echo "runtime planner-worker frozen-1.1/generic-1.3 exact-plan/children/parallel/join/artifact/review-revision/restart/replay E2E passed"
succeeded=true
