#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli \
    -p agentmod-plugin-host -p agentmod-plugin-fixture-worker \
    -p agentmod-filesystem-host

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-plugin-e2e.XXXXXX")
workspace="$run_root/workspace"
plugin_styles="$run_root/styles/plugins"
mkdir -p "$workspace" "$plugin_styles"
cp "$repository/tests/fixtures/plugins/plugin-composed-style.toml" \
    "$plugin_styles/"
printf '%s\n' "plugin rewrite reached the selected file" \
    >"$workspace/plugin-selected.txt"

runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
plugin_host="$repository/target/debug/agentmod-plugin-host"
plugin_worker="$repository/target/debug/agentmod-plugin-fixture-worker"
filesystem_host="$repository/target/debug/agentmod-filesystem-host"
rewriter="$run_root/fixture-rewriter.toml"
observer="$run_root/fixture-observer.toml"

cat >"$rewriter" <<EOF
schema_version = 1
category = "interceptor"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = ["action.proposed"]
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.rewriter"
version = "1.0.0"
runtime_api = "^1.0"

[entrypoint]
kind = "process"
program = "$plugin_worker"
args = []

[authorities]
read = ["session_state"]
proposed_write = ["canonical_state"]

[permissions]
tools = ["filesystem.read"]
network = []

[ordering]
stage = 20
priority = 100
before = []
after = []

[configuration]
schema_id = "fixture.rewriter.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"
EOF

cat >"$observer" <<EOF
schema_version = 1
category = "observer"
scope = "session"
classification = "observer"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = ["plugin.invocation_completed", "tool.execution_completed"]
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.observer"
version = "1.0.0"
runtime_api = "^1.0"

[entrypoint]
kind = "process"
program = "$plugin_worker"
args = []

[authorities]
read = ["session_state"]
proposed_write = []

[permissions]
tools = []
network = []

[ordering]
stage = 100
priority = 0
before = []
after = ["fixture.rewriter"]

[configuration]
schema_id = "fixture.observer.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "continue"
EOF

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
"0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem_host"
export AGENTMOD_PLUGIN_HOST_PROGRAM="$plugin_host"
export AGENTMOD_PLUGIN_MANIFESTS="$rewriter:$observer"
export AGENTMOD_PLUGIN_EXECUTABLE_ROOTS="$repository/target/debug"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log") &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 100 ]; then
            cat "$run_root/runtime.stderr.log" >&2
            return 1
        fi
        sleep 0.1
    done
}

create_and_run() {
    created=$("$cli" session create --workspace "$workspace" \
        --style plugin-composed@1.0.0 --json)
    session_id=$(printf '%s' "$created" | sed -n \
        's/.*"session_id":"\([^"]*\)".*/\1/p')
    test -n "$session_id"
    "$cli" run "read through the selected plugin" --session "$session_id" \
        --option 'mock_scenario="one_tool_call"' --json >/dev/null
    journal="$run_root/sessions/$session_id/events.jsonl"
    grep -F "plugin rewrite reached the selected file" "$journal" >/dev/null
    grep -F '"event_type":"plugin.set_activated"' "$journal" >/dev/null
    grep -F '"event_type":"plugin.invocation_completed"' "$journal" >/dev/null
    attempt=0
    marker="$run_root/sessions/$session_id/fixture-observer-received.log"
    while [ ! -f "$marker" ] && [ "$attempt" -lt 100 ]; do
        attempt=$((attempt + 1))
        sleep 0.05
    done
    grep -F "plugin.invocation_completed" "$marker" >/dev/null
    grep -F '"event_type":"plugin.observer_delivery_proposed"' "$journal" >/dev/null
    grep -F '"event_type":"plugin.observer_delivery_dispatched"' "$journal" >/dev/null
    grep -F '"event_type":"plugin.observer_delivery_completed"' "$journal" >/dev/null
    inspection=$("$cli" session inspect "$session_id" --json)
    printf '%s' "$inspection" | grep -F '"fixture.observer"' >/dev/null
    printf '%s' "$inspection" | grep -F '"observer_deliveries"' >/dev/null
}

start_runtime
style=$("$cli" style inspect plugin-composed --json)
printf '%s' "$style" | grep -F '"source":"plugin"' >/dev/null
create_and_run
first_session=$session_id

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
start_runtime
"$cli" session inspect "$first_session" --json |
    grep -F '"id":"plugin-composed"' >/dev/null
create_and_run

echo "runtime plugin interceptor, observer, canonical audit, and restart E2E passed"
