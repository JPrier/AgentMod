#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
    -p agentmod-cli -p agentmod-plugin-host \
    -p agentmod-plugin-fixture-worker -p agentmod-filesystem-host

target_dir=${CARGO_TARGET_DIR:-target}
case "$target_dir" in
    /*) ;;
    *) target_dir="$repository/$target_dir" ;;
esac
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
cli="$target_dir/debug/agentmod"
plugin_host="$target_dir/debug/agentmod-plugin-host"
filesystem_host="$target_dir/debug/agentmod-filesystem-host"
source_worker="$target_dir/debug/agentmod-plugin-fixture-worker"
run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-graph-c-e2e.XXXXXX")
workspace="$run_root/workspace"
user_styles="$run_root/styles/user"
fixture_bin="$run_root/fixture-bin"
mkdir -p "$workspace" "$user_styles" "$fixture_bin"
printf '%s\n' 'Graph C plugin branch action evidence.' >"$workspace/README.md"
plugin_worker="$fixture_bin/agentmod-plugin-fixture-worker"
cp "$source_worker" "$plugin_worker"
style_template="$repository/tests/fixtures/styles/arbitrary-graph-c.toml"
cp "$style_template" "$user_styles/arbitrary-graph-c.toml"

make_variant() {
    id=$1
    capability=$2
    executor=$3
    terminal=$4
    sed \
        -e "s/id = \"user-graph-c\"/id = \"$id\"/" \
        -e "s/fixture\\.graph/$executor/g" \
        -e "s/plugin\\.graph/$capability/g" \
        -e "s/renamed_done/$terminal/g" \
        "$style_template" >"$user_styles/$id.toml"
}
make_variant user-graph-c-invalid-transition \
    plugin.graph_invalid fixture.graph.invalid_transition different_done
make_variant user-graph-c-invalid-output \
    plugin.invalid fixture.invalid renamed_done
make_variant user-graph-c-timeout \
    plugin.timeout fixture.timeout renamed_done
make_variant user-graph-c-cancel \
    plugin.cancel fixture.cancel renamed_done
make_variant user-graph-c-unavailable \
    plugin.unavailable fixture.unavailable renamed_done

manifest="$run_root/arbitrary-graph-c-node.toml"
sed "s|__PLUGIN_WORKER__|$plugin_worker|g" \
    "$repository/tests/fixtures/plugins/arbitrary-graph-c-node.toml" \
    >"$manifest"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_PLUGIN_HOST_PROGRAM="$plugin_host"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem_host"
export AGENTMOD_PLUGIN_MANIFESTS="$manifest"
export AGENTMOD_PLUGIN_EXECUTABLE_ROOTS="$fixture_bin"
export AGENTMOD_PERMISSION_MODE=allow
runtime_out="$run_root/runtime.stdout.log"
runtime_err="$run_root/runtime.stderr.log"
daemon_pid=
turn_pid=
succeeded=false

stop_runtime() {
    if [ -n "$daemon_pid" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=
    fi
}
cleanup() {
    if [ -n "$turn_pid" ]; then
        kill "$turn_pid" 2>/dev/null || true
        wait "$turn_pid" 2>/dev/null || true
        turn_pid=
    fi
    stop_runtime
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-graph-c-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed Graph C E2E root: $run_root" >&2
            fi
            ;;
        *)
            echo "refusing unexpected Graph C E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$runtime_out" 2>>"$runtime_err" &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 120 ]; then
            cat "$runtime_err" >&2
            echo "Graph C runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
}
journal_path() {
    printf '%s/sessions/%s/events.jsonl' "$run_root" "$1"
}
journal_count() {
    path=$(journal_path "$1")
    if [ -f "$path" ]; then
        wc -l <"$path" | tr -d ' '
    else
        printf '0'
    fi
}
event_count() {
    path=$(journal_path "$1")
    if [ -f "$path" ]; then
        grep -c "\"event_type\":\"$2\"" "$path" || true
    else
        printf '0'
    fi
}
invocation_count() {
    find "$run_root" -name fixture-node-invocations.log -type f \
        -exec cat {} + 2>/dev/null | wc -l | tr -d ' '
}
new_session() {
    created=$("$cli" session create --workspace "$workspace" \
        --style "$1@1.0.0" --json)
    printf '%s' "$created" |
        sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p'
}
assert_plugin_plan() {
    inspection=$1
    executor=$2
    capability=$3
    printf '%s' "$inspection" |
        grep -F '"compiler":"agentmod-runtime-node-plan@3"' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"execution_plan_hash":"' >/dev/null
    printf '%s' "$inspection" | grep -F '"registry_hash":"' >/dev/null
    printf '%s' "$inspection" |
        grep -F "\"executor_id\":\"$executor\",\"executor_version\":\"1.0.0\",\"node_id\":\"renamed_plugin\",\"node_kind\":\"model_call\"" >/dev/null
    printf '%s' "$inspection" |
        grep -F '"source":{"kind":"plugin","plugin_id":"fixture.node"}' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"boundary":"plugin_host"' >/dev/null
    printf '%s' "$inspection" | grep -F "\"$capability\"" >/dev/null
    printf '%s' "$inspection" |
        grep -F '"executor_declaration_hash":"' >/dev/null
    printf '%s' "$inspection" |
        grep -F '"executor_id":"runtime.context-construction","executor_version":"1.0.0","node_id":"runtime_context"' >/dev/null
}
assert_no_redispatch() {
    session_id=$1
    before_invocations=$2
    before_events=$3
    "$cli" run "recover Graph C without redispatch" \
        --session "$session_id" --provider deterministic-mock \
        --model mock-model --json >/dev/null 2>&1 || true
    test "$(invocation_count)" -eq "$before_invocations"
    test "$(journal_count "$session_id")" -eq "$before_events"
}

start_runtime
for style_id in \
    user-graph-c \
    user-graph-c-invalid-transition \
    user-graph-c-invalid-output \
    user-graph-c-timeout \
    user-graph-c-cancel \
    user-graph-c-unavailable
do
    style=$("$cli" style inspect "$style_id@1.0.0" --json)
    printf '%s' "$style" | grep -F '"source":"user"' >/dev/null
    printf '%s' "$style" | grep -F '"availability":"available"' >/dev/null
done

success_id=$(new_session user-graph-c)
test -n "$success_id"
success_created=$("$cli" session inspect "$success_id" --json)
assert_plugin_plan "$success_created" fixture.graph plugin.graph
"$cli" run "execute arbitrary user Graph C" --session "$success_id" \
    --provider deterministic-mock --model mock-model --json >/dev/null
success_inspection=$("$cli" session inspect "$success_id" --json)
printf '%s' "$success_inspection" | grep -F '"lifecycle":"active"' >/dev/null
printf '%s' "$success_inspection" |
    grep -F '"state":"completed"' >/dev/null
printf '%s' "$success_inspection" |
    grep -F '"outcome_application":{' >/dev/null
for expectation in \
    "style.execution_initialized 1" \
    "plugin.set_activated 1" \
    "plugin.node_invocation_proposed 1" \
    "plugin.node_invocation_authorized 1" \
    "plugin.node_invocation_dispatched 1" \
    "plugin.node_invocation_completed 1" \
    "plugin.node_outcome_validated 1" \
    "plugin.node_budget_charged 1" \
    "plugin.node_action_proposed 1" \
    "plugin.node_action_applied 1" \
    "tool.call_proposed 1" \
    "tool.call_approved 1" \
    "tool.execution_dispatched 1" \
    "tool.execution_started 1" \
    "tool.execution_completed 1" \
    "artifact.persistence_completed 1"
do
    # shellcheck disable=SC2086
    set -- $expectation
    test "$(event_count "$success_id" "$1")" -eq "$2"
done
python3 - "$(journal_path "$success_id")" <<'PY'
import json
import sys

events = [json.loads(line)["event"] for line in open(sys.argv[1], encoding="utf-8")]
by_type = {}
for event in events:
    by_type.setdefault(event["metadata"]["event_type"], []).append(
        event["payload"]["payload"]
    )
artifact = by_type["artifact.persistence_completed"][0]["artifact_reference"]
plugin_outcomes = [
    event
    for event in by_type["graph.parallel_branch_effect_outcome_recorded"]
    if event["identity"]["work"]["node_id"] == "renamed_plugin"
]
assert len(plugin_outcomes) == 1
assert artifact in plugin_outcomes[0]["outcome"]["output"]["artifact_references"]
join_ready = by_type["graph.generic_join_ready"]
assert len(join_ready) == 1
assert artifact in {
    reference
    for result in join_ready[0]["decision"]["results"]
    for reference in result["artifact_references"]
}
tool_proposals = by_type["tool.call_proposed"]
assert len(tool_proposals) == 1
assert tool_proposals[0]["tool"] == "filesystem.read"
PY
success_invocations=$(invocation_count)
test "$success_invocations" -eq 1
success_events=$(journal_count "$success_id")
success_plan_hash=$(printf '%s' "$success_created" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
test -n "$success_plan_hash"
stop_runtime
start_runtime
restarted=$("$cli" session inspect "$success_id" --json)
assert_plugin_plan "$restarted" fixture.graph plugin.graph
printf '%s' "$restarted" |
    grep -F "\"execution_plan_hash\":\"$success_plan_hash\"" >/dev/null
replayed=$("$cli" session replay "$success_id" --json)
assert_plugin_plan "$replayed" fixture.graph plugin.graph
test "$(journal_count "$success_id")" -eq "$success_events"
test "$(invocation_count)" -eq "$success_invocations"

invalid_transition_id=$(new_session user-graph-c-invalid-transition)
test -n "$invalid_transition_id"
invalid_transition_created=$("$cli" session inspect \
    "$invalid_transition_id" --json)
assert_plugin_plan "$invalid_transition_created" \
    fixture.graph.invalid_transition plugin.graph_invalid
before_invalid_transition=$(invocation_count)
"$cli" run "reject invalid Graph C transition" \
    --session "$invalid_transition_id" --provider deterministic-mock \
    --model mock-model --json >/dev/null 2>&1 || true
test "$(event_count "$invalid_transition_id" \
    plugin.node_invocation_completed)" -eq 1
test "$(event_count "$invalid_transition_id" \
    plugin.node_outcome_rejected)" -eq 1
test "$(invocation_count)" -eq $((before_invalid_transition + 1))
invalid_transition_events=$(journal_count "$invalid_transition_id")
assert_no_redispatch "$invalid_transition_id" \
    "$(invocation_count)" "$invalid_transition_events"

invalid_output_id=$(new_session user-graph-c-invalid-output)
test -n "$invalid_output_id"
invalid_output_created=$("$cli" session inspect "$invalid_output_id" --json)
assert_plugin_plan "$invalid_output_created" fixture.invalid plugin.invalid
before_invalid_output=$(invocation_count)
"$cli" run "reject invalid Graph C output" \
    --session "$invalid_output_id" --provider deterministic-mock \
    --model mock-model --json >/dev/null 2>&1 || true
test "$(event_count "$invalid_output_id" \
    plugin.node_invocation_ambiguous)" -eq 1
test "$(invocation_count)" -eq $((before_invalid_output + 1))
invalid_output_events=$(journal_count "$invalid_output_id")
assert_no_redispatch "$invalid_output_id" \
    "$(invocation_count)" "$invalid_output_events"

timeout_id=$(new_session user-graph-c-timeout)
test -n "$timeout_id"
timeout_created=$("$cli" session inspect "$timeout_id" --json)
assert_plugin_plan "$timeout_created" fixture.timeout plugin.timeout
before_timeout=$(invocation_count)
"$cli" run "time out Graph C plugin effect" \
    --session "$timeout_id" --provider deterministic-mock \
    --model mock-model --json >/dev/null 2>&1 || true
test "$(event_count "$timeout_id" plugin.node_invocation_ambiguous)" -eq 1
test "$(invocation_count)" -eq $((before_timeout + 1))
timeout_events=$(journal_count "$timeout_id")
stop_runtime
start_runtime
assert_no_redispatch "$timeout_id" "$(invocation_count)" "$timeout_events"

cancelled_id=$(new_session user-graph-c-cancel)
test -n "$cancelled_id"
cancelled_created=$("$cli" session inspect "$cancelled_id" --json)
assert_plugin_plan "$cancelled_created" fixture.cancel plugin.cancel
cancellation_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
"$cli" run cancel-Graph-C-plugin --session "$cancelled_id" \
    --provider deterministic-mock --model mock-model \
    --cancellation-id "$cancellation_id" --json \
    >"$run_root/cancel-turn.stdout.log" \
    2>"$run_root/cancel-turn.stderr.log" &
turn_pid=$!
dispatched=false
attempt=0
while [ "$attempt" -lt 100 ]; do
    if [ "$(event_count "$cancelled_id" \
        plugin.node_invocation_dispatched)" -eq 1 ]; then
        dispatched=true
        break
    fi
    if ! kill -0 "$turn_pid" 2>/dev/null; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.05
done
test "$dispatched" = true
"$cli" cancel "$cancellation_id" \
    --reason "cancel plugin parallel branch" --json >/dev/null
wait "$turn_pid" || true
turn_pid=
test "$(event_count "$cancelled_id" \
    graph.parallel_cancellation_requested)" -eq 1
test "$(event_count "$cancelled_id" \
    graph.parallel_cancellation_completed)" -eq 1
cancelled_terminal_count=$((
    $(event_count "$cancelled_id" plugin.node_invocation_failed) +
    $(event_count "$cancelled_id" plugin.node_invocation_ambiguous)
))
test "$cancelled_terminal_count" -eq 1
test "$(event_count "$cancelled_id" \
    plugin.node_invocation_completed)" -eq 0
cancelled_invocations=$(invocation_count)
cancelled_events=$(journal_count "$cancelled_id")
if "$cli" cancel "$cancellation_id" \
    --reason "duplicate terminal cancellation" --json >/dev/null 2>&1; then
    echo "Graph C retained a live cancellation binding after terminal" >&2
    exit 1
fi
test "$(journal_count "$cancelled_id")" -eq "$cancelled_events"
stop_runtime
start_runtime
assert_no_redispatch "$cancelled_id" \
    "$cancelled_invocations" "$cancelled_events"

unavailable_id=$(new_session user-graph-c-unavailable)
test -n "$unavailable_id"
unavailable_created=$("$cli" session inspect "$unavailable_id" --json)
assert_plugin_plan "$unavailable_created" \
    fixture.unavailable plugin.unavailable
rm -- "$plugin_worker"
before_unavailable=$(invocation_count)
if "$cli" run "execute unavailable Graph C plugin" \
    --session "$unavailable_id" --provider deterministic-mock \
    --model mock-model --json >/dev/null 2>&1; then
    echo "unavailable Graph C plugin unexpectedly executed" >&2
    exit 1
fi
test "$(event_count "$unavailable_id" \
    plugin.node_invocation_proposed)" -eq 0
test "$(event_count "$unavailable_id" \
    plugin.node_invocation_dispatched)" -eq 0
test "$(invocation_count)" -eq "$before_unavailable"
unavailable_events=$(journal_count "$unavailable_id")
assert_no_redispatch "$unavailable_id" \
    "$before_unavailable" "$unavailable_events"

succeeded=true
echo "runtime arbitrary Graph C user-style/plugin-plan/invocation/validation/restart/timeout/ambiguity/unavailability/replay E2E passed"
