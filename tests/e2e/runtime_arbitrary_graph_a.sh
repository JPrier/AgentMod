#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-scheduler -p agentmod-cli \
    -p agentmod-tui

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-graph-a-e2e.XXXXXX")
workspace="$run_root/workspace"
mkdir -p "$workspace" "$run_root/styles/user"
cp "$repository/tests/fixtures/styles/arbitrary-graph-a.toml" \
    "$run_root/styles/user/"
cp "$repository/README.md" "$workspace/"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
target_dir=${CARGO_TARGET_DIR:-target}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
export AGENTMOD_HARNESS_PROGRAM="$target_dir/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$target_dir/debug/agentmod-filesystem-host"
export AGENTMOD_SCHEDULER_PROGRAM="$target_dir/debug/agentmod-scheduler"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
runtime="$target_dir/debug/agentmod-runtime"
cli="$target_dir/debug/agentmod"
tui="$target_dir/debug/agentmod-tui"
daemon_pid=
succeeded=false

stop_runtime() {
    if [ -n "$daemon_pid" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=
    fi
}

cleanup() {
    stop_runtime
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-graph-a-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed Graph A E2E root: $run_root" >&2
            fi
            ;;
        *)
            echo "refusing to remove unexpected Graph A E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>>"$run_root/runtime.stderr.log" &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 100 ]; then
            cat "$run_root/runtime.stderr.log" >&2
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
}

journal_path=
event_count() {
    grep -c "\"event_type\":\"$1\"" "$journal_path" || true
}

assert_event_count() {
    actual=$(event_count "$1")
    if [ "$actual" -ne "$2" ]; then
        echo "expected $2 $1 events, found $actual" >&2
        exit 1
    fi
}

assert_exact_executor_set() {
    inspection=$1
    for resolution in \
        '"executor_id":"runtime.context-construction","executor_version":"1.0.0","node_id":"context","node_kind":"context_transform"' \
        '"executor_id":"runtime.model-request","executor_version":"1.1.0","node_id":"model","node_kind":"model_call"' \
        '"executor_id":"runtime.conditional","executor_version":"1.0.0","node_id":"route","node_kind":"conditional_branch"' \
        '"executor_id":"runtime.parallel","executor_version":"1.0.0","node_id":"fanout","node_kind":"parallel_branch"' \
        '"executor_id":"runtime.user-approval","executor_version":"1.0.0","node_id":"approve","node_kind":"user_approval"' \
        '"executor_id":"runtime.tool-gate","executor_version":"1.1.0","node_id":"read-tool","node_kind":"tool_execution_gate"' \
        '"executor_id":"runtime.artifact-persistence","executor_version":"1.0.0","node_id":"save-artifact","node_kind":"persist_artifact"' \
        '"executor_id":"runtime.join","executor_version":"1.0.0","node_id":"join","node_kind":"join_results"' \
        '"executor_id":"runtime.loop","executor_version":"1.0.0","node_id":"bounded-loop","node_kind":"loop"' \
        '"executor_id":"runtime.conditional","executor_version":"1.0.0","node_id":"loop-check","node_kind":"conditional_branch"' \
        '"executor_id":"runtime.event-emission","executor_version":"1.0.0","node_id":"emit-progress","node_kind":"emit_event"' \
        '"executor_id":"runtime.delay","executor_version":"1.0.0","node_id":"durable-delay","node_kind":"delay"' \
        '"executor_id":"runtime.session-completion","executor_version":"1.0.0","node_id":"finish","node_kind":"complete_session"' \
        '"executor_id":"runtime.structured-failure","executor_version":"1.0.0","node_id":"invalid-input","node_kind":"fail"'
    do
        printf '%s' "$inspection" | grep -F "$resolution" >/dev/null
    done
    test "$(printf '%s' "$inspection" |
        grep -o '"source":{"kind":"runtime"}' | wc -l | tr -d ' ')" -eq 14
    test "$(printf '%s' "$inspection" |
        grep -o '"boundary":"runtime_logic"' | wc -l | tr -d ' ')" -eq 14
}

assert_exact_binding_plan() {
    inspection=$1
    binding=$(printf '%s' "$inspection" |
        sed 's/,"style_compatibility":.*$//')
    test -n "$binding"
    printf '%s' "$binding" |
        grep -F '"compiler":"agentmod-runtime-node-plan@3"' >/dev/null
    printf '%s' "$binding" | grep -F '"execution_plan_hash":"' >/dev/null
    printf '%s' "$binding" | grep -F '"registry_hash":"' >/dev/null
    assert_exact_executor_set "$binding"
}

assert_exact_execution_contract() {
    inspection=$1
    plan_hash=$2
    registry_hash=$3
    contract_tail=$(printf '%s' "$inspection" |
        sed 's/.*"execution_contract"://')
    contract=$(printf '%s' "$contract_tail" |
        sed 's/,"failed_nodes":.*$//')
    test -n "$contract"
    printf '%s' "$contract" |
        grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
    printf '%s' "$contract" |
        grep -F "\"registry_hash\":\"$registry_hash\"" >/dev/null
    assert_exact_executor_set "$contract"
}

start_runtime
validation=$("$cli" style validate \
    "$run_root/styles/user/arbitrary-graph-a.toml" --json)
printf '%s' "$validation" | grep -F '"valid":true' >/dev/null
style=$("$cli" style inspect user-graph-a@1.0.0 --json)
printf '%s' "$style" | grep -F '"availability":"available"' >/dev/null
printf '%s' "$style" | grep -F '"source":"user"' >/dev/null

session=$("$cli" session create --workspace "$workspace" \
    --style user-graph-a@1.0.0 --json)
session_id=$(printf '%s' "$session" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"
journal_path="$run_root/sessions/$session_id/events.jsonl"
created=$("$cli" session inspect "$session_id" --json)
assert_exact_binding_plan "$created"
plan_hash=$(printf '%s' "$created" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
registry_hash=$(printf '%s' "$created" |
    sed -n 's/.*"registry_hash":"\([^"]*\)".*/\1/p')

waiting=$("$cli" run "execute arbitrary Graph A" --session "$session_id" \
    --provider deterministic-mock --model mock-model \
    --option 'ready=true' \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="graph-a-model-complete"' --json)
approval_continuation=$(printf '%s' "$waiting" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$approval_continuation"
assert_event_count style.execution_initialized 1
assert_event_count approval.requested 1
assert_event_count artifact.persistence_completed 1

stop_runtime
start_runtime
pending=$("$cli" session inspect "$session_id" --json)
printf '%s' "$pending" | grep -F '"lifecycle":"active"' >/dev/null
assert_exact_binding_plan "$pending"
assert_exact_execution_contract "$pending" "$plan_hash" "$registry_hash"
resolved=$("$cli" approval resolve "$session_id" \
    "$approval_continuation" approve --json)
delay_continuation=$(printf '%s' "$resolved" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$delay_continuation"
delay_state=$("$cli" session inspect "$session_id" --json)
printf '%s' "$delay_state" | grep -F '"state":"stored"' >/dev/null
for variable in ready approval_result tool_result artifact_result iteration; do
    printf '%s' "$delay_state" | grep -F "\"$variable\":" >/dev/null
done
before_duplicate_approval=$(wc -l <"$journal_path" | tr -d ' ')
duplicate=$("$cli" approval resolve "$session_id" \
    "$approval_continuation" approve --json)
printf '%s' "$duplicate" | grep -F '"transitioned":false' >/dev/null
test "$(wc -l <"$journal_path" | tr -d ' ')" -eq \
    "$before_duplicate_approval"
after_duplicate=$("$cli" session inspect "$session_id" --json)
printf '%s' "$after_duplicate" | grep -F '"lifecycle":"active"' >/dev/null
printf '%s' "$after_duplicate" | grep -F '"state":"stored"' >/dev/null

stop_runtime
export AGENTMOD_SCHEDULER_POLL_MS=25
start_runtime
attempt=0
while :; do
    completed=$("$cli" session inspect "$session_id" --json)
    if printf '%s' "$completed" | grep -F '"lifecycle":"completed"' >/dev/null; then
        break
    fi
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 160 ]; then
        cat "$run_root/runtime.stderr.log" >&2
        echo "Graph A delay did not resume" >&2
        exit 1
    fi
    sleep 0.05
done
printf '%s' "$completed" |
    grep -F '"termination_reason":"complete_session"' >/dev/null
for variable in resolved_at resolved_duration delay_result; do
    printf '%s' "$completed" | grep -F "\"$variable\":" >/dev/null
done
resources=$("$tui" --smoke-session-command "$session_id" /resources)
printf '%s' "$resources" | grep -F "selected=$session_id" >/dev/null
printf '%s' "$resources" | grep -F 'resources=1/0/0' >/dev/null

for expectation in \
    "style.execution_initialized 1" \
    "approval.requested 1" \
    "approval.resolved 1" \
    "tool.execution_dispatched 1" \
    "tool.execution_completed 1" \
    "artifact.persistence_dispatched 1" \
    "artifact.persistence_completed 1" \
    "graph.parallel_initialized 1" \
    "graph.schedule_dispatched 1" \
    "graph.schedule_stored 1" \
    "scheduler.fired 1" \
    "graph.node_wait_resolved 1" \
    "graph.user_space_event_emitted 1"
do
    # shellcheck disable=SC2086
    assert_event_count $expectation
done
for node in context model route fanout join bounded-loop loop-check emit-progress durable-delay finish; do
    printf '%s' "$completed" | grep -F "\"node_id\":\"$node\"" >/dev/null
done

before_replay_count=$(wc -l <"$journal_path" | tr -d ' ')
replayed=$("$cli" session replay "$session_id" --json)
printf '%s' "$replayed" | grep -F '"lifecycle":"completed"' >/dev/null
printf '%s' "$replayed" |
    grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
printf '%s' "$replayed" | grep -F '"duration_ms":750' >/dev/null
assert_exact_binding_plan "$replayed"
assert_exact_execution_contract "$replayed" "$plan_hash" "$registry_hash"
test "$(wc -l <"$journal_path" | tr -d ' ')" -eq "$before_replay_count"

succeeded=true
echo "runtime arbitrary Graph A plan/approval/parallel/tool/artifact/loop/event/delay/join/restart/replay E2E passed"
