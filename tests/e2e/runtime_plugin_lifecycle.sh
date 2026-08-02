#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-scheduler -p agentmod-harness \
    -p agentmod-cli -p agentmod-tui -p agentmod-plugin-host \
    -p agentmod-plugin-fixture-worker

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-plugin-lifecycle-e2e.XXXXXX")
workspace="$run_root/workspace"
user_styles="$run_root/styles/user"
fixture_bin="$run_root/fixture-bin"
mkdir -p "$workspace" "$user_styles" "$fixture_bin"

case ${CARGO_TARGET_DIR:-} in
    "") target_root="$repository/target" ;;
    /*) target_root=$CARGO_TARGET_DIR ;;
    *) target_root="$repository/$CARGO_TARGET_DIR" ;;
esac
runtime="$target_root/debug/agentmod-runtime"
harness="$target_root/debug/agentmod-harness"
cli="$target_root/debug/agentmod"
tui="$target_root/debug/agentmod-tui"
plugin_host="$target_root/debug/agentmod-plugin-host"
source_worker="$target_root/debug/agentmod-plugin-fixture-worker"
plugin_worker="$fixture_bin/agentmod-plugin-fixture-worker"
cp "$source_worker" "$plugin_worker"

sed \
    -e 's/id = "user-graph-c"/id = "plugin-lifecycle"/' \
    -e 's/plugin\.graph/plugin.timeout/g' \
    -e 's/fixture\.graph/fixture.timeout/g' \
    "$repository/tests/fixtures/styles/arbitrary-graph-c.toml" \
    >"$user_styles/plugin-lifecycle.toml"

manifest="$run_root/plugin-lifecycle-node.toml"
sed "s|__PLUGIN_WORKER__|$plugin_worker|g" \
    "$repository/tests/fixtures/plugins/arbitrary-graph-c-node.toml" |
    sed '0,/timeout_ms = 1000/s//timeout_ms = 5000/' |
    sed 's/^timeout_ms = 50$/timeout_ms = 5000/' >"$manifest"

dispatch_marker="$run_root/lifecycle-dispatch.log"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
"0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_PLUGIN_HOST_PROGRAM="$plugin_host"
export AGENTMOD_PLUGIN_MANIFESTS="$manifest"
export AGENTMOD_PLUGIN_EXECUTABLE_ROOTS="$fixture_bin"
export AGENTMOD_PERMISSION_MODE=allow
export RUST_MIN_STACK=16777216
export AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_DELAY_MS=1200
export AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_MARKER="$dispatch_marker"
succeeded=0

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    if [ "$succeeded" -eq 1 ]; then
        rm -rf -- "$run_root"
    else
        echo "preserving failed lifecycle E2E at $run_root" >&2
    fi
}
trap cleanup EXIT INT TERM

start_runtime() {
    (
        cd "$run_root"
        "$runtime" serve >"$run_root/runtime.stdout.log" \
            2>"$run_root/runtime.stderr.log"
    ) &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 150 ]; then
            cat "$run_root/runtime.stderr.log" >&2
            return 1
        fi
        sleep 0.1
    done
}

stop_runtime() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=
    fi
}

marker_count() {
    name=$1
    find "$run_root" -name "$name" -type f -exec cat {} \; 2>/dev/null |
        awk 'END { print NR + 0 }'
}

event_count() {
    journal=$1
    event_type=$2
    if [ ! -f "$journal" ]; then
        printf '0\n'
        return
    fi
    grep -F "\"event_type\":\"$event_type\"" "$journal" 2>/dev/null |
        awk 'END { print NR + 0 }'
}

event_sequence() {
    journal=$1
    event_type=$2
    grep -F "\"event_type\":\"$event_type\"" "$journal" |
        sed -n 's/.*"sequence":\([0-9][0-9]*\).*/\1/p' |
        head -n 1
}

assert_lifecycle_action() {
    action=$1
    created=$("$cli" session create --workspace "$workspace" \
        --style plugin-lifecycle@1.0.0 --json)
    session_id=$(printf '%s' "$created" |
        sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
    test -n "$session_id"
    journal="$run_root/sessions/$session_id/events.jsonl"

    before_invocations=$(marker_count fixture-node-invocations.log)
    turn_cancellation=$(cat /proc/sys/kernel/random/uuid)
    "$cli" run "lifecycle-$action" --session "$session_id" \
        --cancellation-id "$turn_cancellation" --json \
        >"$run_root/$action-turn.stdout.log" \
        2>"$run_root/$action-turn.stderr.log" &
    turn_pid=$!
    attempt=0
    while [ "$(marker_count fixture-node-invocations.log)" -eq \
        "$before_invocations" ]; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 150 ] || ! kill -0 "$turn_pid" 2>/dev/null; then
            echo "plugin worker did not enter for $action" >&2
            return 1
        fi
        sleep 0.05
    done
    test "$(marker_count fixture-node-invocations.log)" -eq \
        $((before_invocations + 1))

    lifecycle_cancellation=$(cat /proc/sys/kernel/random/uuid)
    before_dispatches=$(marker_count lifecycle-dispatch.log)
    if [ "$action" = quarantine ]; then
        set -- plugin quarantine fixture.node --session "$session_id" \
            --reason integrity_failure \
            --cancellation-id "$lifecycle_cancellation" --json
    else
        set -- plugin disable fixture.node --session "$session_id" \
            --cancellation-id "$lifecycle_cancellation" --json
    fi
    "$cli" "$@" >"$run_root/$action-lifecycle.stdout.log" \
        2>"$run_root/$action-lifecycle.stderr.log" &
    lifecycle_pid=$!
    attempt=0
    while [ "$(event_count "$journal" plugin.lifecycle_change_requested)" \
        -ne 1 ]; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 150 ] ||
            ! kill -0 "$lifecycle_pid" 2>/dev/null; then
            echo "canonical request was not observed for $action" >&2
            return 1
        fi
        sleep 0.025
    done
    test "$(event_count "$journal" plugin.lifecycle_changed)" -eq 0
    test "$(marker_count lifecycle-dispatch.log)" -eq "$before_dispatches"
    kill -0 "$lifecycle_pid" 2>/dev/null

    wait "$lifecycle_pid"
    expected_state=disabled
    if [ "$action" = quarantine ]; then
        expected_state=quarantined
    fi
    grep -F "\"state\":\"$expected_state\"" \
        "$run_root/$action-lifecycle.stdout.log" >/dev/null
    grep -F '"replayed":false' \
        "$run_root/$action-lifecycle.stdout.log" >/dev/null
    test "$(marker_count lifecycle-dispatch.log)" -eq \
        $((before_dispatches + 1))

    wait "$turn_pid" || true
    test "$(event_count "$journal" plugin.node_invocation_completed)" -eq 0
    test "$(event_count "$journal" plugin.node_invocation_ambiguous)" -eq 1
    test "$(event_count "$journal" plugin.lifecycle_change_requested)" -eq 1
    test "$(event_count "$journal" plugin.lifecycle_changed)" -eq 1
    requested_sequence=$(event_sequence \
        "$journal" plugin.lifecycle_change_requested)
    changed_sequence=$(event_sequence "$journal" plugin.lifecycle_changed)
    test -n "$requested_sequence"
    test -n "$changed_sequence"
    test "$requested_sequence" -lt "$changed_sequence"

    journal_lines=$(wc -l <"$journal" | tr -d ' ')
    retry=$("$cli" "$@")
    printf '%s' "$retry" | grep -F '"replayed":true' >/dev/null
    printf '%s' "$retry" | grep -F "\"state\":\"$expected_state\"" >/dev/null
    test "$(marker_count lifecycle-dispatch.log)" -eq \
        $((before_dispatches + 1))
    test "$(wc -l <"$journal" | tr -d ' ')" -eq "$journal_lines"

    before_future=$(marker_count fixture-node-invocations.log)
    future_cancellation=$(cat /proc/sys/kernel/random/uuid)
    if "$cli" run "future-$action" --session "$session_id" \
        --cancellation-id "$future_cancellation" --json \
        >"$run_root/$action-future.stdout.log" \
        2>"$run_root/$action-future.stderr.log"; then
        echo "future turn did not fail closed after $action" >&2
        return 1
    fi
    test "$(marker_count fixture-node-invocations.log)" -eq "$before_future"

    reverse_action=enable
    if [ "$action" = quarantine ]; then
        reverse_action=unquarantine
    fi
    reverse_cancellation=$(cat /proc/sys/kernel/random/uuid)
    before_reverse_dispatches=$(marker_count lifecycle-dispatch.log)
    set -- plugin "$reverse_action" fixture.node --session "$session_id" \
        --cancellation-id "$reverse_cancellation" --json
    reverse=$("$cli" "$@")
    printf '%s' "$reverse" | grep -F '"state":"active"' >/dev/null
    printf '%s' "$reverse" | grep -F '"replayed":false' >/dev/null
    test "$(marker_count lifecycle-dispatch.log)" -eq \
        $((before_reverse_dispatches + 1))
    reverse_journal_lines=$(wc -l <"$journal" | tr -d ' ')
    reverse_retry=$("$cli" "$@")
    printf '%s' "$reverse_retry" | grep -F '"state":"active"' >/dev/null
    printf '%s' "$reverse_retry" | grep -F '"replayed":true' >/dev/null
    test "$(marker_count lifecycle-dispatch.log)" -eq \
        $((before_reverse_dispatches + 1))
    test "$(wc -l <"$journal" | tr -d ' ')" -eq \
        "$reverse_journal_lines"

    if [ "$action" = disable ]; then
        watch_out="$run_root/tui-watch.stdout.log"
        watch_err="$run_root/tui-watch.stderr.log"
        "$tui" --smoke-watch 6000 >"$watch_out" 2>"$watch_err" &
        watch_pid=$!
        sleep 0.4
        before_tui_dispatches=$(marker_count lifecycle-dispatch.log)
        tui_result=$("$tui" --smoke-command "/plugin-disable fixture.node")
        printf '%s' "$tui_result" | grep -F 'plugin fixture.node@' >/dev/null
        printf '%s' "$tui_result" | grep -F 'disabled' >/dev/null
        test "$(marker_count lifecycle-dispatch.log)" -eq \
            $((before_tui_dispatches + 1))
        tui_enable=$("$cli" plugin enable fixture.node --session "$session_id" \
            --cancellation-id "$(cat /proc/sys/kernel/random/uuid)" --json)
        printf '%s' "$tui_enable" | grep -F '"state":"active"' >/dev/null
        wait "$watch_pid"
        watch_delta=$(sed -n \
            's/.*events_delta=\([0-9][0-9]*\).*/\1/p' "$watch_out")
        test -n "$watch_delta"
        test "$watch_delta" -ge 4
    fi
}

assert_startup_lifecycle_recovery() {
    stop_runtime
    export AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS=5000
    export AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER=\
"$run_root/lifecycle-post-receipt.log"
    start_runtime
    created=$("$cli" session create --workspace "$workspace" \
        --style plugin-lifecycle@1.0.0 --json)
    session_id=$(printf '%s' "$created" |
        sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
    journal="$run_root/sessions/$session_id/events.jsonl"
    before_invocations=$(marker_count fixture-node-invocations.log)
    "$cli" run startup-lifecycle-recovery --session "$session_id" \
        --cancellation-id "$(cat /proc/sys/kernel/random/uuid)" --json \
        >"$run_root/startup-turn.stdout.log" \
        2>"$run_root/startup-turn.stderr.log" &
    turn_pid=$!
    attempt=0
    while [ "$(marker_count fixture-node-invocations.log)" -eq \
        "$before_invocations" ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 150
        sleep 0.05
    done
    lifecycle_cancellation=$(cat /proc/sys/kernel/random/uuid)
    before_dispatches=$(marker_count lifecycle-dispatch.log)
    before_receipts=$(marker_count lifecycle-post-receipt.log)
    "$cli" plugin disable fixture.node --session "$session_id" \
        --cancellation-id "$lifecycle_cancellation" --json \
        >"$run_root/startup-lifecycle.stdout.log" \
        2>"$run_root/startup-lifecycle.stderr.log" &
    lifecycle_pid=$!
    attempt=0
    while [ "$(marker_count lifecycle-post-receipt.log)" -eq \
        "$before_receipts" ]; do
        attempt=$((attempt + 1))
        test "$attempt" -lt 200
        sleep 0.025
    done
    test "$(event_count "$journal" plugin.lifecycle_change_requested)" -eq 1
    test "$(event_count "$journal" plugin.lifecycle_changed)" -eq 0
    stop_runtime
    kill "$turn_pid" "$lifecycle_pid" 2>/dev/null || true
    wait "$turn_pid" "$lifecycle_pid" 2>/dev/null || true
    start_runtime
    test "$(event_count "$journal" plugin.lifecycle_change_requested)" -eq 1
    test "$(event_count "$journal" plugin.lifecycle_changed)" -eq 1
    test "$(marker_count lifecycle-dispatch.log)" -eq \
        $((before_dispatches + 2))
    test "$(marker_count fixture-node-invocations.log)" -eq \
        $((before_invocations + 1))
    retry=$("$cli" plugin disable fixture.node --session "$session_id" \
        --cancellation-id "$lifecycle_cancellation" --json)
    printf '%s' "$retry" | grep -F '"replayed":true' >/dev/null
    unset AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS
    unset AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER
}

start_runtime
"$cli" style inspect plugin-lifecycle --json |
    grep -F '"availability":"available"' >/dev/null
assert_lifecycle_action disable
assert_lifecycle_action quarantine
sleep 3.5
test "$(marker_count fixture-node-late-effects.log)" -eq 0
assert_startup_lifecycle_recovery

succeeded=1
echo "runtime daemon/CLI plugin disable/enable and quarantine/unquarantine plus TUI management lifecycle E2E passed"
