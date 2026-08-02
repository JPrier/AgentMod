#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
temp_base=$(CDPATH= cd -- "${TMPDIR:-/tmp}" && pwd -P)
run_root=$(mktemp -d "$temp_base/agentmod-ephemeral-e2e.XXXXXX")
daemon_pid=
turn_pid=
succeeded=false

stop_runtime() {
    if [ -n "$daemon_pid" ] && kill -0 "$daemon_pid" 2>/dev/null; then
        kill "$daemon_pid"
        wait "$daemon_pid" 2>/dev/null || true
    fi
    daemon_pid=
}

stop_background_turn() {
    if [ -n "$turn_pid" ] && kill -0 "$turn_pid" 2>/dev/null; then
        kill "$turn_pid"
        wait "$turn_pid" 2>/dev/null || true
    fi
    turn_pid=
}

cleanup() {
    stop_background_turn
    stop_runtime
    case "$run_root" in
        "$temp_base"/agentmod-ephemeral-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed Ephemeral E2E root: $run_root" >&2
            fi
            ;;
        *)
            echo "refusing to remove unexpected E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT HUP INT TERM

target_dir=${CARGO_TARGET_DIR:-"$run_root/cargo-target"}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
export CARGO_TARGET_DIR="$target_dir"

cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-scheduler -p agentmod-cli

runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
scheduler="$target_dir/debug/agentmod-scheduler"
cli="$target_dir/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
0123456789abcdef0123456789abcdef0123456789abcdef
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log" &
    daemon_pid=$!
    attempt=0
    while [ "$attempt" -lt 80 ]; do
        if "$cli" doctor --json >/dev/null 2>&1; then
            return
        fi
        if ! kill -0 "$daemon_pid" 2>/dev/null; then
            break
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "runtime did not become ready" >&2
    cat "$run_root/runtime.stderr.log" >&2
    exit 1
}

journal_path() {
    printf '%s/sessions/%s/events.jsonl' "$run_root" "$1"
}

assert_event_count() {
    session_id=$1
    event_type=$2
    expected=$3
    journal=$(journal_path "$session_id")
    actual=$(grep -c "\"event_type\":\"$event_type\"" "$journal" || true)
    if [ "$actual" -ne "$expected" ]; then
        echo "expected $expected $event_type events, found $actual" >&2
        exit 1
    fi
}

assert_generic_plan() {
    inspection=$1
    python3 - "$inspection" <<'PY'
import json
import sys

inspection = json.loads(sys.argv[1])
binding = inspection["state"]["style_binding"]
plan = binding["execution_plan"]
assert binding["id"] == "ephemeral-turn"
assert binding["version"] == "1.2.0"
assert plan["compilation"]["compiler"] == "agentmod-runtime-node-plan@3"
assert binding["execution_plan_hash"]
assert plan["registry_hash"]
expected = {
    "fresh-context": ("context_transform", "runtime.context-construction", "1.0.0"),
    "respond": ("model_call", "runtime.model-request", "1.0.0"),
    "tool-batch": ("tool_execution_gate", "runtime.tool-gate", "1.1.0"),
    "done": ("complete_turn", "runtime.turn-completion", "1.0.0"),
}
nodes = plan["nodes"]
assert len(nodes) == len(expected)
for node in nodes:
    assert (
        node["node_kind"],
        node["executor_id"],
        node["executor_version"],
    ) == expected.pop(node["node_id"])
    assert node["source"]["kind"] == "runtime"
    assert node["boundary"] == "runtime_logic"
assert not expected
PY
}

assert_generic_contract() {
    inspection=$1
    plan_hash=$2
    registry_hash=$3
    python3 - "$inspection" "$plan_hash" "$registry_hash" <<'PY'
import json
import sys

inspection = json.loads(sys.argv[1])
contract = inspection["state"]["style_execution"]["execution_contract"]
assert contract["execution_plan_hash"] == sys.argv[2]
assert contract["registry_hash"] == sys.argv[3]
expected = {
    "fresh-context": ("context_transform", "runtime.context-construction", "1.0.0"),
    "respond": ("model_call", "runtime.model-request", "1.0.0"),
    "tool-batch": ("tool_execution_gate", "runtime.tool-gate", "1.1.0"),
    "done": ("complete_turn", "runtime.turn-completion", "1.0.0"),
}
nodes = contract["node_executors"]
assert len(nodes) == len(expected)
for node in nodes:
    assert (
        node["node_kind"],
        node["executor_id"],
        node["executor_version"],
    ) == expected.pop(node["node_id"])
    assert node["source"]["kind"] == "runtime"
    assert node["boundary"] == "runtime_logic"
assert not expected
PY
}

assert_empty_projection() {
    printf '%s' "$1" | grep -F '"provider_projection":[]' >/dev/null
}

assert_history_entry() {
    session_id=$1
    kind=$2
    text=$3
    journal=$(journal_path "$session_id")
    matches=$(grep -F '"event_type":"conversation.entry_committed"' "$journal" |
        grep -F "\"kind\":\"$kind\"" |
        grep -F "\"text\":\"$text\"" |
        wc -l | tr -d ' ')
    if [ "$matches" -ne 1 ]; then
        echo "expected one canonical $kind history entry for $text" >&2
        exit 1
    fi
}

assert_no_legacy_adapter_evidence() {
    inspection=$1
    session_id=$2
    journal=$(journal_path "$session_id")
    for marker in \
        '"method":"ephemeral_fresh_context"' \
        '"projection_id":"ephemeral-fresh-context' \
        '"result_reference":"fresh-context:'
    do
        if printf '%s' "$inspection" | grep -F "$marker" >/dev/null ||
            grep -F "$marker" "$journal" >/dev/null
        then
            echo "legacy Ephemeral adapter evidence survived: $marker" >&2
            exit 1
        fi
    done
    grep -F '"method":"generic_fresh_context"' "$journal" >/dev/null
}

assert_legacy_binding() {
    inspection=$1
    python3 - "$inspection" <<'PY'
import json
import sys

binding = json.loads(sys.argv[1])["state"]["style_binding"]
assert binding["id"] == "ephemeral-turn"
assert binding["version"] == "1.1.0"
PY
}

wait_provider_completion_receipt() {
    session_id=$1
    directory="$run_root/sessions/$session_id/artifacts/provider-completion-receipts"
    attempt=0
    while [ "$attempt" -lt 80 ]; do
        receipt_count=$(find "$directory" -type f -name '*.json' 2>/dev/null |
            wc -l | tr -d ' ')
        if [ "$receipt_count" -eq 1 ]; then
            receipt_path=$(find "$directory" -type f -name '*.json')
            grep -F "\"session_id\":\"$session_id\"" "$receipt_path" >/dev/null
            grep -F '"invocation_id":"' "$receipt_path" >/dev/null
            grep -F '"receipt_json":"' "$receipt_path" >/dev/null
            printf '%s' "$receipt_path"
            return
        fi
        if [ "$receipt_count" -gt 1 ]; then
            echo "provider completion receipt was persisted more than once" >&2
            exit 1
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "provider completion receipt was not durable during crash window" >&2
    exit 1
}

export AGENTMOD_PROVIDER_COMPLETION_RECEIPT_DELAY_MS=5000
start_runtime
crash_session=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.2.0 --json)
crash_session_id=$(printf '%s' "$crash_session" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$crash_session_id"
crash_created=$("$cli" session inspect "$crash_session_id" --json)
assert_generic_plan "$crash_created"
assert_empty_projection "$crash_created"
crash_prompt=receipt-window-input
crash_output=receipt-window-output
crash_assistant="alpha beta $crash_output"
crash_cancellation=0193f7d4-8c10-7000-8000-000000000012
"$cli" run "$crash_prompt" --session "$crash_session_id" \
    --cancellation-id "$crash_cancellation" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$crash_output\"" --json \
    >"$run_root/receipt-run.stdout.log" \
    2>"$run_root/receipt-run.stderr.log" &
turn_pid=$!
receipt=$(wait_provider_completion_receipt "$crash_session_id")
receipt_hash=$(cksum "$receipt")
assert_event_count "$crash_session_id" model.request_started 1
assert_event_count "$crash_session_id" model.response_completed 0

stop_runtime
wait "$turn_pid" 2>/dev/null || true
turn_pid=
unset AGENTMOD_PROVIDER_COMPLETION_RECEIPT_DELAY_MS
start_runtime
"$cli" run "$crash_prompt" --session "$crash_session_id" \
    --cancellation-id "$crash_cancellation" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$crash_output\"" --json >/dev/null
crash_recovered=$("$cli" session inspect "$crash_session_id" --json)
assert_generic_plan "$crash_recovered"
crash_plan_hash=$(printf '%s' "$crash_created" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
crash_registry_hash=$(printf '%s' "$crash_created" |
    sed -n 's/.*"registry_hash":"\([^"]*\)".*/\1/p')
test -n "$crash_plan_hash"
test -n "$crash_registry_hash"
assert_generic_contract \
    "$crash_recovered" "$crash_plan_hash" "$crash_registry_hash"
assert_empty_projection "$crash_recovered"
assert_history_entry "$crash_session_id" user_message "$crash_prompt"
assert_history_entry "$crash_session_id" assistant_message "$crash_assistant"
assert_no_legacy_adapter_evidence "$crash_recovered" "$crash_session_id"
printf '%s' "$crash_recovered" | grep -F '"active_node":null' >/dev/null
assert_event_count "$crash_session_id" model.request_started 1
assert_event_count "$crash_session_id" model.response_completed 1
test "$(find "$(dirname "$receipt")" -type f -name '*.json' |
    wc -l | tr -d ' ')" -eq 1
test "$(cksum "$receipt")" = "$receipt_hash"

crash_journal=$(journal_path "$crash_session_id")
crash_before_replay=$(wc -l <"$crash_journal" | tr -d ' ')
crash_replayed=$("$cli" session replay "$crash_session_id" --json)
printf '%s' "$crash_replayed" |
    grep -F '"command":"session_replay"' >/dev/null
assert_generic_plan "$crash_replayed"
assert_generic_contract \
    "$crash_replayed" "$crash_plan_hash" "$crash_registry_hash"
assert_empty_projection "$crash_replayed"
printf '%s' "$crash_replayed" | grep -F "$crash_prompt" >/dev/null
printf '%s' "$crash_replayed" | grep -F "$crash_output" >/dev/null
test "$(wc -l <"$crash_journal" | tr -d ' ')" -eq "$crash_before_replay"
test "$(cksum "$receipt")" = "$receipt_hash"
assert_event_count "$crash_session_id" model.request_started 1
assert_event_count "$crash_session_id" model.response_completed 1

session=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.2.0 --json)
session_id=$(printf '%s' "$session" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"
created=$("$cli" session inspect "$session_id" --json)
assert_generic_plan "$created"
assert_empty_projection "$created"
plan_hash=$(printf '%s' "$created" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
registry_hash=$(printf '%s' "$created" |
    sed -n 's/.*"registry_hash":"\([^"]*\)".*/\1/p')
test -n "$plan_hash"
test -n "$registry_hash"

first_prompt=turn-one-secret-input
first_output=turn-one-secret-output
first_assistant="alpha beta $first_output"
"$cli" run "$first_prompt" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$first_output\"" --json >/dev/null
first_inspection=$("$cli" session inspect "$session_id" --json)
assert_generic_plan "$first_inspection"
assert_generic_contract "$first_inspection" "$plan_hash" "$registry_hash"
assert_empty_projection "$first_inspection"
assert_history_entry "$session_id" user_message "$first_prompt"
assert_history_entry "$session_id" assistant_message "$first_assistant"
assert_no_legacy_adapter_evidence "$first_inspection" "$session_id"

stop_runtime
start_runtime
restart_inspection=$("$cli" session inspect "$session_id" --json)
assert_generic_plan "$restart_inspection"
assert_generic_contract "$restart_inspection" "$plan_hash" "$registry_hash"
assert_empty_projection "$restart_inspection"
printf '%s' "$restart_inspection" | grep -F "$first_prompt" >/dev/null
printf '%s' "$restart_inspection" | grep -F "$first_output" >/dev/null

second_prompt=turn-two-current-input
second_output=turn-two-output
second_assistant="alpha beta $second_output"
"$cli" run "$second_prompt" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$second_output\"" --json >/dev/null
final_inspection=$("$cli" session inspect "$session_id" --json)
assert_generic_plan "$final_inspection"
assert_generic_contract "$final_inspection" "$plan_hash" "$registry_hash"
assert_empty_projection "$final_inspection"
for expected in "$first_prompt" "$first_assistant" "$second_prompt" "$second_assistant"; do
    printf '%s' "$final_inspection" | grep -F "$expected" >/dev/null
done
assert_history_entry "$session_id" user_message "$second_prompt"
assert_history_entry "$session_id" assistant_message "$second_assistant"
assert_no_legacy_adapter_evidence "$final_inspection" "$session_id"

journal=$(journal_path "$session_id")
test "$(grep -c '"method":"generic_fresh_context"' "$journal")" -eq 2
test "$(grep -c '"method":"ephemeral_discard"' "$journal")" -eq 2
second_fresh=$(grep '"method":"generic_fresh_context"' "$journal" | tail -n 1)
printf '%s' "$second_fresh" | grep -F "$second_prompt" >/dev/null
if printf '%s' "$second_fresh" |
    grep -E "$first_prompt|$first_output" >/dev/null
then
    echo "second fresh projection leaked unselected turn-one state" >&2
    exit 1
fi
assert_event_count "$session_id" model.request_started 2
assert_event_count "$session_id" model.response_completed 2

before_replay=$(wc -l <"$journal" | tr -d ' ')
replayed=$("$cli" session replay "$session_id" --json)
printf '%s' "$replayed" | grep -F '"command":"session_replay"' >/dev/null
assert_generic_plan "$replayed"
assert_generic_contract "$replayed" "$plan_hash" "$registry_hash"
assert_empty_projection "$replayed"
assert_no_legacy_adapter_evidence "$replayed" "$session_id"
test "$(wc -l <"$journal" | tr -d ' ')" -eq "$before_replay"
assert_event_count "$session_id" model.request_started 2
assert_event_count "$session_id" model.response_completed 2

legacy=$("$cli" session create --workspace "$repository" \
    --style ephemeral-turn@1.1.0 --json)
legacy_id=$(printf '%s' "$legacy" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$legacy_id"
legacy_created=$("$cli" session inspect "$legacy_id" --json)
assert_legacy_binding "$legacy_created"
legacy_lock="$run_root/sessions/$legacy_id/style.lock"
legacy_lock_before=$(cksum "$legacy_lock")
"$cli" run "legacy-turn-input" --session "$legacy_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="legacy-turn-output"' --json >/dev/null
legacy_after=$("$cli" session inspect "$legacy_id" --json)
assert_legacy_binding "$legacy_after"
assert_empty_projection "$legacy_after"
test "$(cksum "$legacy_lock")" = "$legacy_lock_before"
legacy_journal=$(journal_path "$legacy_id")
legacy_before_replay=$(wc -l <"$legacy_journal" | tr -d ' ')
legacy_replayed=$("$cli" session replay "$legacy_id" --json)
printf '%s' "$legacy_replayed" |
    grep -F '"command":"session_replay"' >/dev/null
assert_legacy_binding "$legacy_replayed"
assert_empty_projection "$legacy_replayed"
test "$(cksum "$legacy_lock")" = "$legacy_lock_before"
test "$(wc -l <"$legacy_journal" | tr -d ' ')" -eq "$legacy_before_replay"
assert_event_count "$legacy_id" model.request_started 1
assert_event_count "$legacy_id" model.response_completed 1

succeeded=true
echo "runtime ephemeral-turn v1.2 receipt recovery/exact-plan/generic-dispatch/isolated-restart/replay and explicit v1.1 compatibility E2E passed"
