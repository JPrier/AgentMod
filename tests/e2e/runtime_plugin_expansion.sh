#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli \
    -p agentmod-plugin-host -p agentmod-plugin-fixture-worker \
    -p agentmod-filesystem-host -p agentmod-scheduler

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-plugin-expansion-e2e.XXXXXX")
workspace="$run_root/workspace"
plugin_styles="$run_root/styles/plugins"
mkdir -p "$workspace" "$plugin_styles"
cp "$repository/tests/fixtures/plugins/plugin-expanded-style.toml" \
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
observer="$run_root/fixture-durable-observer.toml"
graph_node="$run_root/fixture-graph-node.toml"
memory="$run_root/fixture-memory.toml"
compaction="$run_root/fixture-compaction.toml"
transform="$run_root/fixture-context-transform.toml"

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
runtime_api = "^0.1"

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
id = "fixture.durable-observer"
version = "1.0.0"
runtime_api = "^0.1"

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
after = []

[configuration]
schema_id = "fixture.durable-observer.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "continue"

[observer_delivery]
mode = "at_least_once"
max_attempts = 3
retry_backoff_ms = 50
EOF

cat >"$graph_node" <<EOF
schema_version = 1
category = "graph_node"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.graph-node"
version = "1.0.0"
runtime_api = "^0.1"

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
after = []

[configuration]
schema_id = "fixture.graph-node.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[[node_executors]]
executor_id = "fixture.node"
version = "1.0.0"
node_kind = "emit_event"
runtime_api = "^1.0"
required_capabilities = ["events"]
input_schema = '{"type":"object"}'
output_schema = '{"type":"object"}'
timeout_ms = 3000
failure_policy = "reject"
idempotent = true
external_effect = false
read_authority = ["session_state"]
state_scope = "plugin_state"
EOF

cat >"$memory" <<EOF
schema_version = 1
category = "memory"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.memory"
version = "1.0.0"
runtime_api = "^0.1"

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
after = []

[configuration]
schema_id = "fixture.memory.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[memory]
scopes = ["session", "project"]
capabilities = ["retrieve", "write"]
bounded_bytes = 1048576
EOF

cat >"$compaction" <<EOF
schema_version = 1
category = "compaction"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.compaction"
version = "1.0.0"
runtime_api = "^0.1"

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
after = []

[configuration]
schema_id = "fixture.compaction.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[compaction]
strategy_id = "fixture.plugin-summary"
idempotent = true
bounded_bytes = 65536
EOF

cat >"$transform" <<EOF
schema_version = 1
category = "context_transform"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 5000
state_migration_version = 1

[identity]
id = "fixture.context-transform"
version = "1.0.0"
runtime_api = "^0.1"

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
after = []

[configuration]
schema_id = "fixture.context-transform.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[[context_transforms]]
transform_id = "fixture.anonymize"
boundary = "before_provider_projection"
stage = 10
priority = 5
before = []
after = []
EOF

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf "$run_root"
}
trap cleanup EXIT INT TERM

export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem_host"
export AGENTMOD_PLUGIN_HOST_PROGRAM="$plugin_host"
export AGENTMOD_PLUGIN_MANIFESTS="$rewriter:$observer:$graph_node:$memory:$compaction:$transform"
export AGENTMOD_PLUGIN_EXECUTABLE_ROOTS="$(dirname "$plugin_worker")"
export AGENTMOD_PLUGIN_IDLE_TIMEOUT_MS="2000"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"

"$runtime" serve >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
daemon_pid=$!
for _ in $(seq 1 100); do
    if "$cli" doctor --json >/dev/null 2>&1; then break; fi
    sleep 0.1
done

"$cli" style list --json | grep -q '"id":"plugin-expanded"'
session_json=$("$cli" session create --workspace "$workspace" \
    --style plugin-expanded@1.0.0 --json)
session_id=$(printf '%s' "$session_json" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
[ -n "$session_id" ]

"$cli" run "read through the expanded plugin set" --session "$session_id" \
    --option 'mock_scenario="one_tool_call"' --json >/dev/null

session_dir="$run_root/sessions/$session_id"
journal="$session_dir/events.jsonl"
grep -q "plugin rewrite reached the selected file" "$journal"
grep -q "plugin.set_activated" "$journal"
grep -q "plugin.invocation_completed" "$journal"
for _ in $(seq 1 600); do
    if grep -q "plugin.audit_recorded" "$journal" &&
        grep -q "observer_delivery_attempted" "$journal"; then
        break
    fi
    sleep 0.05
done
grep -q "plugin.audit_recorded" "$journal"
grep -q "observer_delivery_attempted" "$journal"

observer_marker="$session_dir/fixture-observer-received.log"
for _ in $(seq 1 200); do
    [ -f "$observer_marker" ] && break
    sleep 0.05
done
[ -f "$observer_marker" ]
delivery_file=$(find "$session_dir/.agentmod/plugin-state" \
    -name "deliveries.json*" | head -n1)
[ -n "$delivery_file" ]
grep -q "observer_delivery_completed" "$delivery_file"
marker_lines=$(wc -l <"$observer_marker")

# Idle teardown: the supervised host must exit while the session is dormant.
sleep 6
baseline_hosts=$(pgrep -f "agentmod-plugin-host" | wc -l)
idle_hosts=$(pgrep -f "agentmod-plugin-host" | wc -l)
[ "$idle_hosts" -le "$baseline_hosts" ]

# Lazy restart on a fresh session (the deterministic mock reuses tool call IDs
# on one session, which the runtime correctly rejects as a duplicate dispatch).
second_json=$("$cli" session create --workspace "$workspace" \
    --style plugin-expanded@1.0.0 --json)
second_id=$(printf '%s' "$second_json" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
"$cli" run "read again after idle host teardown" --session "$second_id" \
    --option 'mock_scenario="one_tool_call"' --json >/dev/null
second_dir="$run_root/sessions/$second_id"
grep -q "plugin rewrite reached the selected file" "$second_dir/events.jsonl"
second_marker="$second_dir/fixture-observer-received.log"
for _ in $(seq 1 200); do
    [ -f "$second_marker" ] && break
    sleep 0.05
done
[ -f "$second_marker" ]

# Daemon restart: durable deliveries and activated state survive without
# redelivering already committed events.
kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
"$runtime" serve >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
daemon_pid=$!
for _ in $(seq 1 100); do
    if "$cli" doctor --json >/dev/null 2>&1; then break; fi
    sleep 0.1
done
recovered=$("$cli" session inspect "$session_id" --json)
printf '%s' "$recovered" | grep -q '"id":"plugin-expanded"'
printf '%s' "$recovered" | grep -q "fixture.durable-observer"
restart_marker_lines=$(wc -l <"$observer_marker")
[ "$restart_marker_lines" -eq "$marker_lines" ]

echo "runtime plugin expansion E2E passed"
