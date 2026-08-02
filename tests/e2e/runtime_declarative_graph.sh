#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-scheduler -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-declarative-e2e.XXXXXX")
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
runtime="$target_dir/debug/agentmod-runtime"
cli="$target_dir/debug/agentmod"
succeeded=false

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
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed E2E root: $run_root" >&2
            fi
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
        2>>"$run_root/runtime.stderr.log" &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 80 ]; then
            cat "$run_root/runtime.stderr.log" >&2
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
}

assert_event_count() {
    session_id=$1
    event_type=$2
    expected=$3
    journal="$run_root/sessions/$session_id/events.jsonl"
    actual=$(grep -c "\"event_type\":\"$event_type\"" "$journal" || true)
    if [ "$actual" -ne "$expected" ]; then
        echo "expected $expected $event_type events, found $actual" >&2
        exit 1
    fi
}

assert_generic_plan() {
    inspection=$1
    printf '%s' "$inspection" |
        grep -F '"compiler":"agentmod-runtime-node-plan@3"' >/dev/null
    printf '%s' "$inspection" | grep -F '"registry_hash":"' >/dev/null
    printf '%s' "$inspection" | grep -F '"execution_plan_hash":"' >/dev/null
    test "$(printf '%s' "$inspection" |
        grep -o '"boundary":"runtime_logic"' | wc -l | tr -d ' ')" -ge 5
    for resolution in \
        '"executor_id":"runtime.conditional","executor_version":"1.0.0","node_id":"branch","node_kind":"conditional_branch"' \
        '"executor_id":"runtime.user-approval","executor_version":"1.0.0","node_id":"approval","node_kind":"user_approval"' \
        '"executor_id":"runtime.tool-gate","executor_version":"1.1.0","node_id":"tool","node_kind":"tool_execution_gate"' \
        '"executor_id":"runtime.loop","executor_version":"1.0.0","node_id":"repeat","node_kind":"loop"' \
        '"executor_id":"runtime.session-completion","executor_version":"1.0.0","node_id":"done","node_kind":"complete_session"'
    do
        printf '%s' "$inspection" | grep -F "$resolution" >/dev/null
    done
}

assert_no_legacy_evidence() {
    inspection=$1
    session_id=$2
    journal="$run_root/sessions/$session_id/events.jsonl"
    for marker in \
        'declarative-request:approval:' \
        'declarative-approval:' \
        'declarative-tool:'
    do
        if printf '%s' "$inspection" | grep -F "$marker" >/dev/null ||
            grep -F "$marker" "$journal" >/dev/null
        then
            echo "legacy declarative evidence survived generic execution: $marker" >&2
            exit 1
        fi
    done
}

assert_completed_graph() {
    inspection=$1
    session_id=$2
    requires_approval=$3
    execution_state=$(printf '%s' "$inspection" |
        sed 's/,"style_introspection":.*$//')
    printf '%s' "$inspection" | grep -F '"id":"declarative-graph"' >/dev/null
    printf '%s' "$inspection" | grep -F '"version":"1.2.0"' >/dev/null
    assert_generic_plan "$inspection"
    printf '%s' "$inspection" | grep -F '"lifecycle":"completed"' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"termination_reason":"complete_session"' >/dev/null
    test "$(printf '%s' "$execution_state" |
        grep -o '"node_id":"tool","result_reference":"tool:graph:tool:' |
        wc -l | tr -d ' ')" -eq 3
    test "$(printf '%s' "$execution_state" |
        grep -o '"node_id":"repeat","result_reference":' |
        wc -l | tr -d ' ')" -eq 3
    printf '%s' "$execution_state" |
        grep -F '"node_id":"done","result_reference":' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"tool":"filesystem.read"' >/dev/null
    approval_count=$(printf '%s' "$execution_state" |
        grep -o '"node_id":"approval","result_reference":"generic-approval:' |
        wc -l | tr -d ' ')
    if [ "$requires_approval" = true ]; then
        test "$approval_count" -eq 1
    else
        test "$approval_count" -eq 0
    fi
    printf '%s' "$execution_state" |
        grep -F '"request":{"value":{"kind":"map","value":{"requires_approval":{"kind":"boolean","value":'"$requires_approval"'}}},"value_hash":"' >/dev/null
    printf '%s' "$execution_state" |
        grep -F '"tool_arguments":{"value":{"kind":"map","value":{"path":{"kind":"string","value":"README.md"}}},"value_hash":"' >/dev/null
    printf '%s' "$execution_state" |
        grep -F '"iteration":{"value":{"kind":"map","value":{"remaining":{"kind":"boolean","value":false}}},"value_hash":"' >/dev/null
    test "$(printf '%s' "$execution_state" |
        grep -o '"version":1,"writer":{"kind":"runtime"}' |
        wc -l | tr -d ' ')" -eq 2
    test "$(printf '%s' "$execution_state" |
        grep -o '"version":3,"writer":{"kind":"node","node_id":"repeat"}' |
        wc -l | tr -d ' ')" -eq 1
    assert_event_count "$session_id" tool.execution_dispatched 3
    assert_event_count "$session_id" tool.execution_completed 3
    assert_no_legacy_evidence "$inspection" "$session_id"
}

start_runtime
direct=$("$cli" session create --workspace "$repository" \
    --style declarative-graph@1.2.0 --json)
direct_id=$(printf '%s' "$direct" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
direct_created=$("$cli" session inspect "$direct_id" --json)
assert_generic_plan "$direct_created"
"$cli" run "read the repository graph fixture" --session "$direct_id" \
    --option 'request={"requires_approval":false}' \
    --option 'tool_arguments={"path":"README.md"}' --json >/dev/null
direct_inspection=$("$cli" session inspect "$direct_id" --json)
assert_completed_graph "$direct_inspection" "$direct_id" false

approval=$("$cli" session create --workspace "$repository" \
    --style declarative-graph@1.2.0 --json)
approval_id=$(printf '%s' "$approval" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
approval_created=$("$cli" session inspect "$approval_id" --json)
assert_generic_plan "$approval_created"
waiting=$("$cli" run "read after an explicit graph approval" \
    --session "$approval_id" \
    --option 'request={"requires_approval":true}' \
    --option 'tool_arguments={"path":"README.md"}' --json)
continuation=$(printf '%s' "$waiting" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$continuation"
waiting_inspection=$("$cli" session inspect "$approval_id" --json)
printf '%s' "$waiting_inspection" |
    grep -F '"active_node":{"attempt":1,"loop_iteration":0,"node_id":"approval","step":2}' >/dev/null
assert_event_count "$approval_id" tool.execution_dispatched 0
assert_no_legacy_evidence "$waiting_inspection" "$approval_id"

stop_runtime
start_runtime
"$cli" approval resolve "$approval_id" "$continuation" approve --json \
    >/dev/null
approved_inspection=$("$cli" session inspect "$approval_id" --json)
assert_completed_graph "$approved_inspection" "$approval_id" true

approval_journal="$run_root/sessions/$approval_id/events.jsonl"
before_duplicate=$(wc -l <"$approval_journal" | tr -d ' ')
duplicate=$("$cli" approval resolve "$approval_id" "$continuation" approve --json)
printf '%s' "$duplicate" | grep -F '"transitioned":false' >/dev/null
test "$(wc -l <"$approval_journal" | tr -d ' ')" -eq "$before_duplicate"
after_duplicate=$("$cli" session inspect "$approval_id" --json)
assert_completed_graph "$after_duplicate" "$approval_id" true

before_replay=$(wc -l <"$approval_journal" | tr -d ' ')
replayed=$("$cli" session replay "$approval_id" --json)
printf '%s' "$replayed" | grep -F '"command":"session_replay"' >/dev/null
plan_hash=$(printf '%s' "$approval_created" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
printf '%s' "$replayed" |
    grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
test "$(wc -l <"$approval_journal" | tr -d ' ')" -eq "$before_replay"
assert_completed_graph "$replayed" "$approval_id" true

succeeded=true
echo "runtime declarative v1.2 exact-plan/generic-dispatch/branch/loop/three-tool/approval/restart/replay E2E passed"
