#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-style-e2e.XXXXXX")
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
python3 - "$styles" <<'PY'
import json
import sys

actual = {
    (style["id"], style["version"], style["availability"])
    for style in json.loads(sys.argv[1])["styles"]
}
expected = {
    ("persistent-chat", "1.1.0", "available"),
    ("persistent-chat", "1.2.0", "available"),
    ("ephemeral-turn", "1.1.0", "available"),
    ("ephemeral-turn", "1.2.0", "available"),
    ("research-loop", "1.1.0", "available"),
    ("research-loop", "1.2.0", "available"),
    ("research-loop", "1.3.0", "available"),
    ("planner-worker", "1.1.0", "available"),
    ("planner-worker", "1.2.0", "available"),
    ("planner-worker", "1.3.0", "available"),
    ("planner-worker", "1.4.0", "available"),
    ("declarative-graph", "1.1.0", "available"),
    ("declarative-graph", "1.2.0", "available"),
}
assert actual == expected, f"exact built-in catalog mismatch: {sorted(actual)!r}"
PY

persistent=$("$cli" session create --workspace "$repository" \
    --style persistent-chat --json)
persistent_id=$(printf '%s' "$persistent" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
persistent_legacy=$("$cli" session create --workspace "$repository" \
    --style persistent-chat@1.1.0 --json)
persistent_legacy_id=$(printf '%s' "$persistent_legacy" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
ephemeral=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.1.0 --json)
ephemeral_id=$(printf '%s' "$ephemeral" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
planner=$("$cli" session create --workspace "$repository" \
    --style planner-worker --json)
planner_id=$(printf '%s' "$planner" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
planner_legacy=$("$cli" session create --workspace "$repository" \
    --style planner-worker@1.1.0 --json)
planner_legacy_id=$(printf '%s' "$planner_legacy" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
selected=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.1.0 --memory sqlite-fts \
    --compaction sliding_window --max-iterations 3 --max-steps 40 \
    --max-tokens 100000 --max-cost-micros 1000000 --max-duration-ms 60000 --json)
selected_id=$(printf '%s' "$selected" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$persistent_id"
test -n "$persistent_legacy_id"
test -n "$ephemeral_id"
test -n "$planner_id"
test -n "$planner_legacy_id"
test -n "$selected_id"

for pair in \
    "$persistent_id:persistent-chat:1.2.0" \
    "$persistent_legacy_id:persistent-chat:1.1.0" \
    "$ephemeral_id:ephemeral-turn:1.1.0" \
    "$planner_id:planner-worker:1.4.0" \
    "$planner_legacy_id:planner-worker:1.1.0" \
    "$selected_id:ephemeral-turn:1.1.0"
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
selected_inspection=$("$cli" session inspect "$selected_id" --json)
persistent_inspection_before_restart=$(
    "$cli" session inspect "$persistent_id" --json
)
persistent_legacy_inspection_before_restart=$(
    "$cli" session inspect "$persistent_legacy_id" --json
)
planner_inspection_before_restart=$(
    "$cli" session inspect "$planner_id" --json
)
planner_legacy_inspection_before_restart=$(
    "$cli" session inspect "$planner_legacy_id" --json
)
python3 - "$planner_inspection_before_restart" \
    "$planner_legacy_inspection_before_restart" <<'PY'
import json
import sys

current = json.loads(sys.argv[1])["state"]["style_binding"]
current_plan = current["execution_plan"]
current_compiled = json.loads(current["compiled_style_json"])
expected = {
    "plan": ("runtime.model-request", "1.0.0"),
    "plan-route": ("runtime.conditional", "1.0.0"),
    "spawn-planner": ("runtime.child-spawn", "1.1.0"),
    "spawn-evidence": ("runtime.child-spawn", "1.1.0"),
    "worker-fanout": ("runtime.parallel", "1.0.0"),
    "wait-planner": ("runtime.child-wait", "1.0.0"),
    "wait-evidence": ("runtime.child-wait", "1.0.0"),
    "join-workers": ("runtime.join", "1.0.0"),
    "integrate": ("runtime.model-request", "1.1.0"),
    "integration-route": ("runtime.conditional", "1.0.0"),
    "persist-integration": ("runtime.artifact-persistence", "1.0.0"),
    "review": ("runtime.review", "1.1.0"),
    "revision": ("runtime.loop", "1.0.0"),
    "done": ("runtime.session-completion", "1.0.0"),
    "structured-failure": ("runtime.structured-failure", "1.0.0"),
}
assert current["version"] == "1.4.0"
assert current_plan["compilation"]["compiler"] == \
    "agentmod-runtime-node-plan@3"
resolutions = {
    resolution["node_id"]: resolution
    for resolution in current_plan["nodes"]
}
assert set(resolutions) == set(expected)
for node_id, (executor_id, executor_version) in expected.items():
    resolution = resolutions[node_id]
    assert resolution["executor_id"] == executor_id
    assert resolution["executor_version"] == executor_version
    assert resolution["source"]["kind"] == "runtime"
    assert resolution["boundary"] == "runtime_logic"
assert len(current_compiled["graph"]["variables"]) == 11
configured = {
    "plan": "model_request",
    "spawn-planner": "spawn_child_agent",
    "spawn-evidence": "spawn_child_agent",
    "worker-fanout": "parallel_branch",
    "wait-planner": "wait_for_agents",
    "wait-evidence": "wait_for_agents",
    "join-workers": "join_results",
    "integrate": "model_request",
    "persist-integration": "persist_artifact",
    "review": "review",
}
nodes = {node["id"]: node for node in current_compiled["graph"]["nodes"]}
for node_id, configuration_type in configured.items():
    assert nodes[node_id]["configuration"]["type"] == configuration_type

legacy = json.loads(sys.argv[2])["state"]["style_binding"]
legacy_plan = legacy["execution_plan"]
legacy_compiled = json.loads(legacy["compiled_style_json"])
legacy_expected = {
    "plan": ("runtime.model-request", "1.1.0"),
    "spawn-workers": ("runtime.child-spawn", "1.1.0"),
    "wait-workers": ("runtime.child-wait", "1.0.0"),
    "integrate": ("runtime.model-request", "1.1.0"),
    "review": ("runtime.review", "1.1.0"),
    "revision": ("runtime.loop", "1.0.0"),
    "done": ("runtime.session-completion", "1.0.0"),
}
assert legacy["version"] == "1.1.0"
assert legacy_plan["compilation"]["compiler"] == \
    "agentmod-runtime-node-plan@2"
legacy_resolutions = {
    resolution["node_id"]: resolution
    for resolution in legacy_plan["nodes"]
}
assert set(legacy_resolutions) == set(legacy_expected)
for node_id, (executor_id, executor_version) in legacy_expected.items():
    resolution = legacy_resolutions[node_id]
    assert resolution["executor_id"] == executor_id
    assert resolution["executor_version"] == executor_version
    assert resolution["source"]["kind"] == "runtime"
    assert resolution["boundary"] == "runtime_logic"
assert not legacy_compiled["graph"].get("variables")
assert [node["id"] for node in legacy_compiled["graph"]["nodes"]] == [
    "done",
    "integrate",
    "plan",
    "review",
    "revision",
    "spawn-workers",
    "wait-workers",
]
assert all(node.get("configuration") is None for node in legacy_compiled["graph"]["nodes"])
PY
printf '%s' "$selected_inspection" | grep -F '"provider":"sqlite-fts"' >/dev/null
printf '%s' "$selected_inspection" | grep -F '"strategy":"sliding_window"' >/dev/null
printf '%s' "$selected_inspection" | grep -F '"max_iterations":3' >/dev/null
printf '%s' "$selected_inspection" | grep -F '"max_tokens":100000' >/dev/null

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
for session_id in \
    "$persistent_id" "$persistent_legacy_id" "$ephemeral_id" \
    "$planner_id" "$planner_legacy_id" "$selected_id"
do
    "$cli" session inspect "$session_id" --json |
        grep -F '"style_compatibility":{"status":"compatible"}' >/dev/null
done
persistent_inspection_after_restart=$(
    "$cli" session inspect "$persistent_id" --json
)
persistent_legacy_inspection_after_restart=$(
    "$cli" session inspect "$persistent_legacy_id" --json
)
planner_inspection_after_restart=$(
    "$cli" session inspect "$planner_id" --json
)
planner_legacy_inspection_after_restart=$(
    "$cli" session inspect "$planner_legacy_id" --json
)
python3 - "$persistent_inspection_before_restart" \
    "$persistent_inspection_after_restart" \
    "$persistent_legacy_inspection_before_restart" \
    "$persistent_legacy_inspection_after_restart" \
    "$planner_inspection_before_restart" \
    "$planner_inspection_after_restart" \
    "$planner_legacy_inspection_before_restart" \
    "$planner_legacy_inspection_after_restart" <<'PY'
import json
import sys

before = json.loads(sys.argv[1])["state"]["style_binding"]
after = json.loads(sys.argv[2])["state"]["style_binding"]
assert after == before, "persistent exact execution plan was rebound"
legacy_before = json.loads(sys.argv[3])["state"]["style_binding"]
legacy_after = json.loads(sys.argv[4])["state"]["style_binding"]
assert legacy_after == legacy_before, "persistent 1.1 binding was rebound"
planner_before = json.loads(sys.argv[5])["state"]["style_binding"]
planner_after = json.loads(sys.argv[6])["state"]["style_binding"]
assert planner_after == planner_before, "planner-worker 1.4 binding was rebound"
planner_legacy_before = json.loads(sys.argv[7])["state"]["style_binding"]
planner_legacy_after = json.loads(sys.argv[8])["state"]["style_binding"]
assert planner_legacy_after == planner_legacy_before, \
    "planner-worker 1.1 binding was rebound"
PY
selected_after_restart=$("$cli" session inspect "$selected_id" --json)
printf '%s' "$selected_after_restart" | grep -F '"provider":"sqlite-fts"' >/dev/null
printf '%s' "$selected_after_restart" | grep -F '"strategy":"sliding_window"' >/dev/null
printf '%s' "$selected_after_restart" | grep -F '"max_iterations":3' >/dev/null
printf '%s' "$selected_after_restart" | grep -F '"max_tokens":100000' >/dev/null
"$cli" run "selected components after restart" --session "$selected_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="selected-after"' --json >/dev/null
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

for session_id in "$persistent_id" "$persistent_legacy_id" "$ephemeral_id" \
    "$planner_id" "$planner_legacy_id"; do
    python3 - "$run_root/sessions/$session_id/metadata.json" \
        "$run_root/sessions/$session_id/style.lock" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    metadata = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    style_lock = json.load(source)
assert metadata["schema_version"] == 2
assert metadata["style_binding"] is not None
assert style_lock["binding"] is not None
assert style_lock["compiled"] is not None
PY
done

echo "runtime session-style registry/restart/branch E2E passed"
