#!/usr/bin/env bash
set -euo pipefail

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"

cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-filesystem-host \
    -p agentmod-process-host -p agentmod-git-host -p agentmod-cli

target_dir=${CARGO_TARGET_DIR:-target}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
scheduler="$target_dir/debug/agentmod-scheduler"
filesystem_host="$target_dir/debug/agentmod-filesystem-host"
process_host="$target_dir/debug/agentmod-process-host"
git_host="$target_dir/debug/agentmod-git-host"
cli="$target_dir/debug/agentmod"

# Keep branch-worktree Cargo outputs and the Unix socket path short.
run_root=$(mktemp -d "${TMPDIR:-/tmp}/am14.XXXXXXXX")
workspace="$run_root/workspace"
mkdir -p "$workspace/src"
printf 'parent-owned' >"$workspace/worker.txt"
cat >"$workspace/Cargo.toml" <<'EOF'
[package]
name = "agentmod-planner-worker-fixture"
version = "0.0.0"
edition = "2024"

[workspace]
EOF
cat >"$workspace/src/lib.rs" <<'EOF'
#[cfg(test)]
mod tests {
    #[test]
    fn child_workspace_contains_the_owned_edit() {
        assert_eq!(std::fs::read_to_string("worker.txt").unwrap(), "child-owned\n");
    }
}
EOF
git -C "$workspace" init >/dev/null
git -C "$workspace" config user.email agentmod@example.invalid
git -C "$workspace" config user.name 'AgentMod Fixture'
git -C "$workspace" add worker.txt Cargo.toml src/lib.rs
git -C "$workspace" commit -m 'planner v1.4 fixture base' >/dev/null

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
0123456789abcdef0123456789abcdef0123456789abcdef
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem_host"
export AGENTMOD_PROCESS_HOST_PROGRAM="$process_host"
export AGENTMOD_GIT_HOST_PROGRAM="$git_host"
export AGENTMOD_PROCESS_ALLOWED_EXECUTABLES=cargo
export AGENTMOD_HARNESS_TEST_GATE_ROOT="$run_root/harness-gates"
export AGENTMOD_PERMISSION_MODE=allow

runtime_pid=
succeeded=false
declare -a child_pids=()
declare -a child_stdout=()
declare -a child_stderr=()
last_child_run=

stop_runtime() {
    if [[ -n ${runtime_pid:-} ]] && kill -0 "$runtime_pid" 2>/dev/null; then
        kill -TERM -- "-$runtime_pid" 2>/dev/null || kill -TERM "$runtime_pid" 2>/dev/null || true
        for _ in {1..50}; do
            kill -0 "$runtime_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL -- "-$runtime_pid" 2>/dev/null || kill -KILL "$runtime_pid" 2>/dev/null || true
        wait "$runtime_pid" 2>/dev/null || true
    fi
    runtime_pid=
}

cleanup() {
    local index pid
    for index in "${!child_pids[@]}"; do
        pid=${child_pids[$index]:-}
        if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            for _ in {1..20}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.1
            done
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    stop_runtime
    unset AGENTMOD_RUNTIME_ENDPOINT AGENTMOD_RUNTIME_AUTH_TOKEN
    unset AGENTMOD_HARNESS_PROGRAM AGENTMOD_SCHEDULER_PROGRAM
    unset AGENTMOD_SCHEDULER_ROOT AGENTMOD_FILESYSTEM_HOST_PROGRAM
    unset AGENTMOD_PROCESS_HOST_PROGRAM AGENTMOD_GIT_HOST_PROGRAM
    unset AGENTMOD_PROCESS_ALLOWED_EXECUTABLES
    unset AGENTMOD_HARNESS_TEST_GATE_ROOT AGENTMOD_PERMISSION_MODE
    if [[ $succeeded != true ]]; then
        cat "$run_root/runtime.stderr.log" >&2 2>/dev/null || true
        while IFS= read -r journal; do
            printf 'journal: %s\n' "$journal" >&2
            tail -n 30 "$journal" >&2 || true
        done < <(find "$run_root/sessions" -name events.jsonl -type f 2>/dev/null || true)
    fi
    case "$run_root" in
        "${TMPDIR:-/tmp}"/am14.*)
            if [[ $succeeded == true ]]; then
                rm -rf -- "$run_root"
            else
                printf 'retained failed planner-worker v1.4 E2E root: %s\n' "$run_root" >&2
            fi
            ;;
        *) printf 'refusing unexpected planner-worker v1.4 E2E root: %s\n' "$run_root" >&2 ;;
    esac
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

start_runtime() {
    (
        cd "$run_root"
        exec setsid "$runtime" serve
    ) >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
    runtime_pid=$!
    local attempt doctor
    for attempt in {1..120}; do
        if doctor=$("$cli" doctor --json 2>/dev/null) && \
            printf '%s' "$doctor" | python3 -c \
                'import json,sys; assert json.load(sys.stdin)["state"] == "ready"' 2>/dev/null
        then
            return
        fi
        kill -0 "$runtime_pid" 2>/dev/null || break
        sleep 0.1
    done
    cat "$run_root/runtime.stderr.log" >&2 || true
    printf 'planner-worker v1.4 runtime did not become ready\n' >&2
    return 1
}

parent_run() {
    local session_id=$1 cancellation_id=$2 output=$3
    "$cli" run 'produce child-owned diff and test evidence' \
        --session "$session_id" \
        --provider deterministic-mock \
        --model mock-model \
        --cancellation-id "$cancellation_id" \
        --option 'mock_scenario="planner_worker"' \
        --json >"$output"
}

resolve_approvals() {
    local parent_id=$1 count=$2 output=$3 index pending
    for ((index = 0; index < count; index++)); do
        "$cli" session inspect "$parent_id" --json >"$run_root/approval-inspection.json"
        pending=$(python3 - "$run_root/approval-inspection.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    approvals = json.load(source)["state"]["approvals"]
pending = sorted(key for key, value in approvals.items() if value["state"] == "pending")
assert pending, "expected a pending child approval"
print(pending[0])
PY
)
        "$cli" approval resolve "$parent_id" "$pending" approve --json >"$output"
    done
}

find_children() {
    local parent_id=$1 output=$2 session_id path
    "$cli" session list --limit 64 --json >"$run_root/sessions.json"
    : >"$run_root/child-inspection-paths.txt"
    while IFS= read -r session_id; do
        [[ $session_id == "$parent_id" ]] && continue
        path="$run_root/inspect-$session_id.json"
        "$cli" session inspect "$session_id" --json >"$path"
        printf '%s\n' "$path" >>"$run_root/child-inspection-paths.txt"
    done < <(python3 - "$run_root/sessions.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    for summary in json.load(source)["sessions"]:
        print(summary["id"])
PY
)
    python3 - "$parent_id" "$run_root/child-inspection-paths.txt" >"$output" <<'PY'
import json
import sys
parent_id, paths_file = sys.argv[1:]
children = []
with open(paths_file, encoding="utf-8") as source:
    for raw_path in source:
        path = raw_path.strip()
        if not path:
            continue
        with open(path, encoding="utf-8") as child_source:
            inspection = json.load(child_source)
        origin = inspection["state"].get("child_origin")
        if origin and origin["parent_session_id"] == parent_id:
            children.append((origin["revision"], origin["task_id"], path))
for _, _, path in sorted(children):
    print(path)
PY
}

start_child_run() {
    local child_path=$1 gate_id=${2:-} id task cancellation stdout stderr index
    read -r id task < <(python3 - "$child_path" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
print(state["id"], state["child_origin"]["task"], sep="\t")
PY
)
    cancellation=$(python3 -c 'import uuid; print(uuid.uuid4())')
    stdout="$run_root/child-$id.stdout.log"
    stderr="$run_root/child-$id.stderr.log"
    local -a arguments=(
        run "$task"
        --session "$id"
        --provider deterministic-mock
        --model mock-model
        --option 'mock_scenario="planner_worker_child"'
        --cancellation-id "$cancellation"
        --json
    )
    if [[ -n $gate_id ]]; then
        arguments=(
            run "$task"
            --session "$id"
            --provider deterministic-mock
            --model mock-model
            --option 'mock_scenario="planner_worker_child"'
            --option "mock_gate_id=\"$gate_id\""
            --option 'mock_gate_timeout_ms="120000"'
            --cancellation-id "$cancellation"
            --json
        )
    fi
    timeout --signal=KILL 125s "$cli" "${arguments[@]}" >"$stdout" 2>"$stderr" &
    index=${#child_pids[@]}
    child_pids[$index]=$!
    child_stdout[$index]=$stdout
    child_stderr[$index]=$stderr
    last_child_run=$index
}

wait_child_run() {
    local index=$1 pid=${child_pids[$1]}
    if wait "$pid"; then
        :
    else
        local status=$?
        printf 'child CLI failed (%s)\n' "$status" >&2
        cat "${child_stdout[$index]}" >&2 || true
        cat "${child_stderr[$index]}" >&2 || true
        return 1
    fi
    child_pids[$index]=
}

assert_child_evidence() {
    local child_path=$1
    local child_id
    child_id=$(python3 - "$child_path" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["state"]["id"])
PY
)
    "$cli" session inspect "$child_id" --json >"$run_root/completed-$child_id.json"
    python3 - "$run_root/completed-$child_id.json" \
        "$run_root/sessions/$child_id/events.jsonl" "$run_root" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    completed = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    events = [json.loads(line)["event"] for line in source]
run_root = pathlib.Path(sys.argv[3])
state = completed["state"]
assert state["lifecycle"] == "completed", "planner-worker v1.4 child did not complete"
expected_calls = {
    "planner-worker-edit": "filesystem.edit",
    "planner-worker-test": "process.run",
    "planner-worker-diff": "git.diff",
}
for call_id, tool in expected_calls.items():
    model_proposals = [event for event in events
        if event["metadata"]["event_type"] == "model.tool_call_proposed"
        and event["payload"]["payload"]["call_id"] == call_id
        and event["payload"]["payload"]["tool"] == tool]
    assert len(model_proposals) == 1, f"child model tool {call_id} was missing or duplicated"
    model_proposal = model_proposals[0]
    model_sequence = model_proposal["metadata"]["sequence"]
    later_model_sequences = sorted(event["metadata"]["sequence"] for event in events
        if event["metadata"]["event_type"] == "model.tool_call_proposed"
        and event["metadata"]["sequence"] > model_sequence)
    next_model_sequence = later_model_sequences[0] if later_model_sequences else None
    expected_arguments = model_proposal["payload"]["payload"]["arguments"]
    proposals = [event for event in events
        if event["metadata"]["event_type"] == "tool.call_proposed"
        and event["metadata"]["sequence"] > model_sequence
        and (next_model_sequence is None or event["metadata"]["sequence"] < next_model_sequence)
        and event["payload"]["payload"]["tool"] == tool
        and event["payload"]["payload"]["arguments"] == expected_arguments]
    assert len(proposals) == 1, f"child tool {call_id} did not map to one canonical proposal"
    canonical_call_id = proposals[0]["payload"]["payload"]["call_id"]
    assert canonical_call_id.startswith("generic-provider-tool:"), canonical_call_id
    dispatches = [event for event in events
        if event["metadata"]["event_type"] == "tool.execution_dispatched"
        and event["payload"]["payload"]["call_id"] == canonical_call_id]
    terminals = [event for event in events
        if event["metadata"]["event_type"] == "tool.execution_completed"
        and event["payload"]["payload"]["call_id"] == canonical_call_id]
    assert len(dispatches) == len(terminals) == 1, f"child tool {call_id} was redispatched"

artifacts = list(state["artifact_persistences"].values())
assert len(artifacts) >= 3, "child terminal state omitted owned evidence artifacts"
contents = []
for artifact in artifacts:
    content_hash = artifact["identity"]["content_hash"]
    path = (run_root / "sessions" / state["id"] / "artifacts" / "style" /
            "objects" / content_hash[:2] / content_hash / "content")
    assert path.is_file(), f"child artifact content is missing: {path}"
    contents.append(path.read_text(encoding="utf-8"))
evidence = "\n".join(contents)
assert "worker.txt" in evidence
assert "parent-owned" in evidence
assert "child-owned" in evidence
assert '"success":true' in evidence or '"success": true' in evidence
PY
}

complete_child() {
    local child_path=$1 index
    start_child_run "$child_path"
    index=$last_child_run
    wait_child_run "$index"
    assert_child_evidence "$child_path"
}

start_runtime
"$cli" session create --workspace "$workspace" \
    --style planner-worker@1.4.0 --json >"$run_root/created.json"
parent_id=$(python3 - "$run_root/created.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["session_id"])
PY
)
turn_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
"$cli" session inspect "$parent_id" --json >"$run_root/created-inspection.json"
python3 - "$run_root/created-inspection.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
assert state["style_binding"]["version"] == "1.4.0"
nodes = {node["node_id"]: node for node in state["style_binding"]["execution_plan"]["nodes"]}
for node_id in ("spawn-planner", "spawn-evidence", "integrate", "review"):
    assert nodes[node_id]["executor_version"] == "1.1.0", node_id
for node_id in ("plan", "persist-integration"):
    assert nodes[node_id]["executor_version"] == "1.0.0", node_id
PY

parent_run "$parent_id" "$turn_id" "$run_root/waiting.json"
python3 - "$run_root/waiting.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    assert json.load(source).get("awaiting_continuation")
PY
resolve_approvals "$parent_id" 1 "$run_root/approval-1.json"
stop_runtime
start_runtime
resolve_approvals "$parent_id" 1 "$run_root/approval-2.json"

find_children "$parent_id" "$run_root/children.txt"
mapfile -t children <"$run_root/children.txt"
[[ ${#children[@]} -eq 2 ]] || {
    printf 'planner-worker v1.4 created %s children, expected 2\n' "${#children[@]}" >&2
    exit 1
}
"$cli" session inspect "$parent_id" --json >"$run_root/parent-with-children.json"
python3 - "$run_root/parent-with-children.json" "${children[@]}" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    parent = json.load(source)["state"]
records = list(parent["child_agents"].values())
for child_path in sys.argv[2:]:
    with open(child_path, encoding="utf-8") as source:
        child_id = json.load(source)["state"]["id"]
    matches = [record for record in records if record["child_session_id"] == child_id]
    assert len(matches) == 1, "parent omitted the exact child ownership record"
    lease = matches[0]["workspace_lease"]
    assert lease["mode"]["mode"] == "branch_workspace"
    assert lease["mode"]["merge_policy"] == "manual_review"
    assert lease["ownership"] == "runtime_owned_branch"
    assert lease["branch_name"]
    assert lease["effective_root"] != lease["source_root"]
PY

gate_ids=(planner-v1-4-first-worker-0 planner-v1-4-first-worker-1)
gates=("$AGENTMOD_HARNESS_TEST_GATE_ROOT/${gate_ids[0]}" \
       "$AGENTMOD_HARNESS_TEST_GATE_ROOT/${gate_ids[1]}")
start_child_run "${children[0]}" "${gate_ids[0]}"
child_0_run=$last_child_run
start_child_run "${children[1]}" "${gate_ids[1]}"
child_1_run=$last_child_run
started=0
for _ in {1..300}; do
    started=0
    compgen -G "${gates[0]}/started-*" >/dev/null && started=$((started + 1))
    compgen -G "${gates[1]}/started-*" >/dev/null && started=$((started + 1))
    [[ $started -ge 2 ]] && break
    kill -0 "${child_pids[$child_0_run]}" 2>/dev/null || break
    kill -0 "${child_pids[$child_1_run]}" 2>/dev/null || break
    sleep 0.1
done
if [[ $started -lt 2 ]] || ! kill -0 "${child_pids[$child_0_run]}" 2>/dev/null || \
    ! kill -0 "${child_pids[$child_1_run]}" 2>/dev/null
then
    printf 'first planner workers did not overlap at the harness gate\n' >&2
    exit 1
fi

# Children are canonically task-sorted. Complete task 1 before task 0 to prove
# integration does not inherit external completion order.
touch "${gates[1]}/release"
wait_child_run "$child_1_run"
assert_child_evidence "${children[1]}"
if ! kill -0 "${child_pids[$child_0_run]}" 2>/dev/null; then
    printf 'task-order-first child completed before its gate was released\n' >&2
    exit 1
fi
touch "${gates[0]}/release"
wait_child_run "$child_0_run"
assert_child_evidence "${children[0]}"

stop_runtime
start_runtime
parent_run "$parent_id" "$turn_id" "$run_root/after-worker-restart.json"
for _ in {1..8}; do
    "$cli" session inspect "$parent_id" --json >"$run_root/revision-approval-inspection.json"
    read -r pending_revision lifecycle < <(python3 - "$run_root/revision-approval-inspection.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    state = json.load(source)["state"]
pending = sum(value["state"] == "pending" for value in state["approvals"].values())
print(pending, state["lifecycle"])
PY
)
    [[ $pending_revision -ge 2 ]] && break
    [[ $lifecycle != completed ]] || {
        printf 'reviewer completed without requesting revision children\n' >&2
        exit 1
    }
    parent_run "$parent_id" "$turn_id" "$run_root/after-worker-restart.json"
done
[[ ${pending_revision:-0} -ge 2 ]] || {
    printf 'reviewer did not request two revision child approvals\n' >&2
    exit 1
}
resolve_approvals "$parent_id" 2 "$run_root/revision-approvals.json"

find_children "$parent_id" "$run_root/all-children.txt"
python3 - "$run_root/all-children.txt" >"$run_root/revision-children.txt" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    paths = [line.strip() for line in source if line.strip()]
for path in paths:
    with open(path, encoding="utf-8") as child_source:
        state = json.load(child_source)["state"]
    if state["child_origin"]["revision"] == 1:
        print(path)
PY
mapfile -t revision_children <"$run_root/revision-children.txt"
[[ ${#revision_children[@]} -eq 2 ]] || {
    printf 'structured reviewer rejection created %s revisions, expected 2\n' \
        "${#revision_children[@]}" >&2
    exit 1
}
for child in "${revision_children[@]}"; do
    complete_child "$child"
done

inspection="$run_root/completed-inspection.json"
for _ in {1..6}; do
    parent_run "$parent_id" "$turn_id" "$run_root/final-result.json"
    "$cli" session inspect "$parent_id" --json >"$inspection"
    lifecycle=$(python3 - "$inspection" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["state"]["lifecycle"])
PY
)
    [[ $lifecycle == completed ]] && break
    python3 - "$run_root/final-result.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    assert json.load(source).get("awaiting_continuation"), \
        "planner-worker v1.4 recovery lost its continuation"
PY
done
[[ ${lifecycle:-} == completed ]] || {
    printf 'planner-worker v1.4 did not complete\n' >&2
    exit 1
}

python3 - "$inspection" "$run_root/sessions/$parent_id/events.jsonl" \
    "$run_root" "$workspace/worker.txt" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    inspection = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    events = [json.loads(line)["event"] for line in source]
run_root = pathlib.Path(sys.argv[3])
workspace_file = pathlib.Path(sys.argv[4])
state = inspection["state"]
parent_id = state["id"]

contents = []
for artifact in state["artifact_persistences"].values():
    content_hash = artifact["identity"]["content_hash"]
    path = (run_root / "sessions" / parent_id / "artifacts" / "style" /
            "objects" / content_hash[:2] / content_hash / "content")
    if path.is_file():
        contents.append(path.read_text(encoding="utf-8"))
integration_evidence = "\n".join(contents)
evidence_at = integration_evidence.find('"member_order":["evidence"')
planner_at = integration_evidence.find('"planner"', evidence_at + 1)
assert evidence_at >= 0 and planner_at > evidence_at, \
    "integration did not preserve canonical evidence-before-planner member order"
assert workspace_file.read_text(encoding="utf-8") == "parent-owned", \
    "branch workspace was implicitly merged into the parent"

expected_counts = {
    "graph.generic_join_ready": 2,
    "style.generic_review_routed": 2,
    "artifact.persistence_completed": 2,
}
for event_type, expected in expected_counts.items():
    count = sum(event["metadata"]["event_type"] == event_type for event in events)
    assert count == expected, f"expected {expected} {event_type}, found {count}"
reviews = [event for event in events
    if event["metadata"]["event_type"] == "style.generic_review_routed"]
first = reviews[0]["payload"]["payload"]["evidence"]
second = reviews[1]["payload"]["payload"]["evidence"]
assert first["disposition"] == "revision"
assert len(first["structured_findings"]) == 1
assert second["disposition"] == "approved"
review_evidence_outputs = [event for event in events
    if event["metadata"]["event_type"] == "model.output_delta_observed"
    and "planner.evidence_revision" in event["payload"]["payload"]["text"]
    and "artifact:blake3:" in event["payload"]["payload"]["text"]]
assert len(review_evidence_outputs) == 1, \
    "integration/reviewer did not consume exact artifact evidence"
PY

printf '%s\n' \
    'planner-worker v1.4 branch workspace/child tool/artifact/join/reviewer/restart Linux E2E passed'
succeeded=true
