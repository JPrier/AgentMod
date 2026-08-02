#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"

cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli -p agentmod-tui

target_dir=${CARGO_TARGET_DIR:-target}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
scheduler="$target_dir/debug/agentmod-scheduler"
cli="$target_dir/debug/agentmod"
tui="$target_dir/debug/agentmod-tui"
run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-graph-b-e2e.XXXXXX")
runtime_pid=
succeeded=false

stop_runtime() {
    if [ -n "${runtime_pid:-}" ] && kill -0 "$runtime_pid" 2>/dev/null; then
        kill "$runtime_pid"
        wait "$runtime_pid" 2>/dev/null || true
    fi
    runtime_pid=
}

cleanup() {
    stop_runtime
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-graph-b-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed Graph B E2E root: $run_root" >&2
            fi
            ;;
        *) echo "refusing unexpected Graph B E2E root: $run_root" >&2 ;;
    esac
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$run_root/styles/user" "$run_root/workspace"
cp tests/fixtures/styles/arbitrary-graph-b.toml \
    "$run_root/styles/user/arbitrary-graph-b.toml"
cp tests/fixtures/styles/graph-b-worker.toml \
    "$run_root/styles/user/graph-b-worker.toml"
if [ "${AGENTMOD_GRAPH_B_CANCELLATION_ONLY:-0}" = 1 ]; then
    sed \
        's/timeout_ms = 60000, cancellation = "cascade"/timeout_ms = 1000, cancellation = "cascade"/g' \
        "$run_root/styles/user/arbitrary-graph-b.toml" \
        >"$run_root/styles/user/arbitrary-graph-b.toml.tmp"
    mv "$run_root/styles/user/arbitrary-graph-b.toml.tmp" \
        "$run_root/styles/user/arbitrary-graph-b.toml"
fi

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
0123456789abcdef0123456789abcdef0123456789abcdef
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_USER_STYLES_DIR="$run_root/styles/user"

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log" &
    runtime_pid=$!
    attempt=0
    while [ "$attempt" -lt 120 ]; do
        if "$cli" doctor --json >/dev/null 2>&1; then
            return
        fi
        if ! kill -0 "$runtime_pid" 2>/dev/null; then
            break
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    cat "$run_root/runtime.stderr.log" >&2
    echo "Graph B runtime did not become ready" >&2
    exit 1
}

event_count() {
    event_type=$1
    grep -c "\"event_type\":\"$event_type\"" "$parent_journal" || true
}

assert_event_count() {
    event_type=$1
    expected=$2
    actual=$(event_count "$event_type")
    if [ "$actual" -ne "$expected" ]; then
        echo "expected $expected $event_type events, found $actual" >&2
        exit 1
    fi
}

assert_generation_three_binding() {
    inspection=$1
    expected_nodes=$2
    binding=$(printf '%s' "$inspection" |
        sed 's/,"style_compatibility":.*$//')
    printf '%s' "$binding" |
        grep -F '"compiler":"agentmod-runtime-node-plan@3"' >/dev/null
    printf '%s' "$binding" | grep -F '"execution_plan_hash":"' >/dev/null
    printf '%s' "$binding" | grep -F '"registry_hash":"' >/dev/null
    test "$(printf '%s' "$binding" |
        grep -o '"executor_id":"' | wc -l | tr -d ' ')" -eq \
        "$expected_nodes"
    test "$(printf '%s' "$binding" |
        grep -o '"source":{"kind":"runtime"}' | wc -l | tr -d ' ')" -eq \
        "$expected_nodes"
    test "$(printf '%s' "$binding" |
        grep -o '"boundary":"runtime_logic"' | wc -l | tr -d ' ')" -eq \
        "$expected_nodes"
}

assert_exact_execution_contract() {
    inspection=$1
    expected_nodes=$2
    plan_hash=$3
    registry_hash=$4
    contract_tail=$(printf '%s' "$inspection" |
        sed 's/.*"execution_contract"://')
    contract=$(printf '%s' "$contract_tail" |
        sed 's/,"failed_nodes":.*$//')
    test -n "$contract"
    printf '%s' "$contract" |
        grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
    printf '%s' "$contract" |
        grep -F "\"registry_hash\":\"$registry_hash\"" >/dev/null
    test "$(printf '%s' "$contract" |
        grep -o '"executor_id":"' | wc -l | tr -d ' ')" -eq \
        "$expected_nodes"
}

parent_run() {
    "$cli" run "execute arbitrary Graph B child orchestration" \
        --session "$parent_id" \
        --provider deterministic-mock \
        --model mock-model \
        --cancellation-id "$parent_turn_id" \
        --option 'mock_scenario="graph_b_review_sequence"' \
        --json
}

resolve_spawn_approvals() {
    result=$1
    expected=${2:-2}
    approval=0
    while [ "$approval" -lt "$expected" ]; do
        continuation=$(printf '%s' "$result" |
            sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
        if [ -z "$continuation" ]; then
            echo "expected $expected Graph B spawn approvals, found $approval" >&2
            return 1
        fi
        result=$("$cli" approval resolve "$parent_id" \
            "$continuation" approve --json)
        approval=$((approval + 1))
    done
    printf '%s' "$result"
}

collect_children() {
    : >"$run_root/children.txt"
    "$cli" session list --limit 64 --json |
        grep -o '"id":"[0-9a-f-]*"' |
        sed 's/"id":"//;s/"$//' |
        while IFS= read -r child_id; do
            [ "$child_id" = "$parent_id" ] && continue
            inspected=$("$cli" session inspect "$child_id" --json)
            if ! printf '%s' "$inspected" |
                grep -F "\"parent_session_id\":\"$parent_id\"" >/dev/null
            then
                continue
            fi
            task_id=$(printf '%s' "$inspected" |
                sed -n 's/.*"task_id":"\([^"]*\)".*/\1/p')
            revision=$(printf '%s' "$inspected" |
                sed -n 's/.*"revision":\([0-9][0-9]*\).*/\1/p')
            lifecycle=$(printf '%s' "$inspected" |
                sed -n 's/.*"lifecycle":"\([^"]*\)".*/\1/p')
            assert_generation_three_binding "$inspected" 3
            printf '%s %s %s %s\n' \
                "$child_id" "$task_id" "$revision" "$lifecycle" \
                >>"$run_root/children.txt"
        done
}

child_id_for() {
    task_id=$1
    revision=$2
    awk -v task="$task_id" -v revision="$revision" \
        '$2 == task && $3 == revision { print $1; exit }' \
        "$run_root/children.txt"
}

complete_child() {
    child_id=$1
    text=$2
    inspected=$("$cli" session inspect "$child_id" --json)
    task=$(printf '%s' "$inspected" |
        sed -n 's/.*"task":"\([^"]*\)".*/\1/p')
    cancellation_id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null ||
        uuidgen | tr 'A-F' 'a-f')
    "$cli" run "$task" \
        --session "$child_id" \
        --provider deterministic-mock \
        --model mock-model \
        --option 'mock_scenario="streaming_text"' \
        --option "mock_text=\"$text\"" \
        --cancellation-id "$cancellation_id" \
        --json >/dev/null
    "$cli" session inspect "$child_id" --json |
        grep -F '"lifecycle":"completed"' >/dev/null
}

start_runtime
"$cli" style validate "$run_root/styles/user/arbitrary-graph-b.toml" --json |
    grep -F '"valid":true' >/dev/null
style=$("$cli" style inspect user-graph-b@1.0.0 --json)
printf '%s' "$style" | grep -F '"availability":"available"' >/dev/null
printf '%s' "$style" | grep -F '"source":"user"' >/dev/null
created=$("$cli" session create \
    --workspace "$run_root/workspace" \
    --style user-graph-b@1.0.0 --json)
parent_id=$(printf '%s' "$created" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$parent_id"
parent_journal="$run_root/sessions/$parent_id/events.jsonl"

inspection=$("$cli" session inspect "$parent_id" --json)
assert_generation_three_binding "$inspection" 12
plan_hash=$(printf '%s' "$inspection" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
registry_hash=$(printf '%s' "$inspection" |
    sed -n 's/.*"registry_hash":"\([^"]*\)".*/\1/p')

parent_turn_id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null ||
    uuidgen | tr 'A-F' 'a-f')
waiting=$(parent_run)
printf '%s' "$waiting" | grep -F '"awaiting_continuation":"' >/dev/null
stop_runtime
start_runtime
waiting=$(resolve_spawn_approvals "$waiting")
printf '%s' "$waiting" | grep -F '"awaiting_continuation":"' >/dev/null
active_parent=$("$cli" session inspect "$parent_id" --json)
assert_generation_three_binding "$active_parent" 12
assert_exact_execution_contract \
    "$active_parent" 12 "$plan_hash" "$registry_hash"

collect_children
test "$(wc -l <"$run_root/children.txt" | tr -d ' ')" -eq 2
while read -r child_id _task _revision lifecycle; do
    test "$lifecycle" = active
    child_journal="$run_root/sessions/$child_id/events.jsonl"
    test "$(grep -c '"event_type":"child_agent.message_received"' \
        "$child_journal")" -eq 1
    if grep -E \
        '("kind":"user_message".*revision_context|revision_context.*"kind":"user_message")' \
        "$child_journal" >/dev/null
    then
        echo "Graph B child message was fabricated as a user message" >&2
        exit 1
    fi
done <"$run_root/children.txt"

if [ "${AGENTMOD_GRAPH_B_CANCELLATION_ONLY:-0}" = 1 ]; then
    sleep 1.5
    cancellation=$(parent_run)
    cancellation_continuation=$(printf '%s' "$cancellation" |
        sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
    assert_event_count child_agent.generic_cancellation_requested 2
    assert_event_count child_agent.generic_cancellation_authorized 0
    assert_event_count child_agent.generic_cancellation_dispatched 0
    test -n "$cancellation_continuation"
    stop_runtime
    start_runtime
    resolve_spawn_approvals "$cancellation" 2 >/dev/null
    assert_event_count child_agent.generic_cancellation_requested 2
    assert_event_count child_agent.generic_cancellation_authorized 2
    assert_event_count child_agent.generic_cancellation_dispatched 2
    assert_event_count child_agent.generic_cancellation_completed 2
    collect_children
    while read -r child_id _task _revision _lifecycle; do
        child=$("$cli" session inspect "$child_id" --json)
        printf '%s' "$child" | grep -F '"lifecycle":"cancelled"' >/dev/null
        child_journal="$run_root/sessions/$child_id/events.jsonl"
        if grep -E \
            '"event_type":"conversation.entry_committed".*"kind":"user_message"' \
            "$child_journal" >/dev/null
        then
            echo "Graph B cancellation fabricated child user input" >&2
            exit 1
        fi
    done <"$run_root/children.txt"
    stop_runtime
    start_runtime
    replayed_parent=$("$cli" session inspect "$parent_id" --json)
    printf '%s' "$replayed_parent" | grep -F '"lifecycle":"failed"' >/dev/null
    assert_event_count child_agent.generic_cancellation_dispatched 2
    assert_event_count child_agent.generic_cancellation_completed 2
    echo "runtime arbitrary Graph B accepted cancellation/Ask-restart/exact-receipt E2E passed"
    succeeded=true
    exit 0
fi

assert_event_count child_agent.generic_created 2
assert_event_count child_agent.message_delivered 2

worker_b=$(child_id_for worker-b-0 0)
worker_a=$(child_id_for worker-a-0 0)
test -n "$worker_a"
test -n "$worker_b"
complete_child "$worker_b" worker-b-complete-first
stop_runtime
start_runtime
waiting=$(parent_run)
test "$(event_count graph.generic_join_ready)" -eq 0
test "$(event_count child_agent.wait_projected)" -ge 2

complete_child "$worker_a" worker-a-complete-second
waiting=$(parent_run)
waiting=$(resolve_spawn_approvals "$waiting")
printf '%s' "$waiting" | grep -F '"awaiting_continuation":"' >/dev/null
collect_children
test "$(wc -l <"$run_root/children.txt" | tr -d ' ')" -eq 4
worker_a_revision=$(child_id_for worker-a-0 1)
worker_b_revision=$(child_id_for worker-b-0 1)
test -n "$worker_a_revision"
test -n "$worker_b_revision"
complete_child "$worker_b_revision" revision-worker-b-complete
complete_child "$worker_a_revision" revision-worker-a-complete

completed=$("$cli" session inspect "$parent_id" --json)
prior_sequence=$(printf '%s' "$completed" |
    sed -n 's/.*"last_sequence":\([0-9][0-9]*\).*/\1/p')
recovery=0
while [ "$recovery" -lt 4 ]; do
    waiting=$(parent_run)
    completed=$("$cli" session inspect "$parent_id" --json)
    if printf '%s' "$completed" |
        grep -F '"lifecycle":"completed"' >/dev/null
    then
        break
    fi
    current_sequence=$(printf '%s' "$completed" |
        sed -n 's/.*"last_sequence":\([0-9][0-9]*\).*/\1/p')
    continuation=$(printf '%s' "$waiting" |
        sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
    if [ -z "$continuation" ] ||
        [ "$current_sequence" -le "$prior_sequence" ]
    then
        echo "Graph B recovery drive stalled without a durable wait" >&2
        exit 1
    fi
    prior_sequence=$current_sequence
    recovery=$((recovery + 1))
done
printf '%s' "$completed" | grep -F '"lifecycle":"completed"' >/dev/null
printf '%s' "$completed" |
    grep -F '"termination_reason":"complete_session"' >/dev/null
resources=$("$tui" --smoke-session-command "$parent_id" /resources)
printf '%s' "$resources" | grep -F "selected=$parent_id" >/dev/null
printf '%s' "$resources" | grep -F 'resources=0/4/0' >/dev/null

assert_event_count style.execution_initialized 1
assert_event_count child_agent.generic_creation_dispatched 4
assert_event_count child_agent.generic_created 4
assert_event_count child_agent.message_dispatched 4
assert_event_count child_agent.message_delivered 4
assert_event_count graph.parallel_initialized 2
assert_event_count graph.generic_join_ready 2
assert_event_count style.generic_review_routed 2
test "$(grep -c \
    '"event_type":"style.generic_review_routed".*"disposition":"revision"' \
    "$parent_journal")" -eq 1
test "$(grep -c \
    '"event_type":"style.generic_review_routed".*"disposition":"approved"' \
    "$parent_journal")" -eq 1
test "$(grep -c \
    '"event_type":"graph.variable_assigned".*"variable":"worker_a"' \
    "$parent_journal")" -eq 2
test "$(grep -c \
    '"event_type":"graph.variable_assigned".*"variable":"worker_b"' \
    "$parent_journal")" -eq 2

before_replay=$(wc -l <"$parent_journal" | tr -d ' ')
replayed=$("$cli" session replay "$parent_id" --json)
printf '%s' "$replayed" | grep -F '"lifecycle":"completed"' >/dev/null
printf '%s' "$replayed" |
    grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
assert_generation_three_binding "$replayed" 12
assert_exact_execution_contract "$replayed" 12 "$plan_hash" "$registry_hash"
test "$(wc -l <"$parent_journal" | tr -d ' ')" -eq "$before_replay"

echo "runtime arbitrary Graph B spawn/message/wait/join/review/revision/restart/replay E2E passed"
succeeded=true
