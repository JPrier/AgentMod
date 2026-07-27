#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-mcp-host -p agentmod-cli
rustc tests/fixtures/mcp_stdio_server.rs --edition=2024 \
    -o target/mcp-stdio-fixture

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-mcp-invoke-e2e.XXXXXX")
mkdir -p "$run_root/workspace"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_MCP_HOST_PROGRAM="$repository/target/debug/agentmod-mcp-host"
export AGENTMOD_PERMISSION_MODE="allow"
fixture="$repository/target/mcp-stdio-fixture"
export AGENTMOD_MCP_SERVERS_JSON="[{\"id\":\"fixture\",\"display_name\":\"Runtime E2E fixture\",\"active\":true,\"transport\":\"stdio\",\"program\":\"$fixture\",\"arguments\":[],\"environment\":{}}]"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$repository/target/debug/agentmod-runtime" serve) &
daemon_pid=$!

attempt=0
until "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "runtime did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done

created=$("$repository/target/debug/agentmod" session create \
    --workspace "$run_root/workspace" --style persistent-chat --json)
session_id=$(printf '%s' "$created" | sed -n \
    's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"

turn=$("$repository/target/debug/agentmod" run \
    "invoke the configured MCP echo tool" --session "$session_id" \
    --option 'mock_scenario="mcp_fixture_call"' --json)
printf '%s' "$turn" | grep -F \
    '"text":"continued after approved runtime decision"' >/dev/null

session_root="$run_root/sessions/$session_id"
journal="$session_root/events.jsonl"
for event_type in tool.call_proposed tool.call_approved \
    tool.execution_dispatched tool.execution_started \
    tool.execution_completed; do
    test "$(grep -c "\"event_type\":\"$event_type\"" "$journal")" -eq 1
done
test "$(grep -c '"event_type":"tool.output_observed"' "$journal")" -ge 1
grep -F 'echoed-through-runtime' "$journal" >/dev/null
test -d "$session_root/artifacts/mcp/authorization-replay"

echo "configured stdio MCP invocation through runtime E2E passed"
