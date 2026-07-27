#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-browser-host -p agentmod-cli
rustc tests/fixtures/webdriver_server.rs --edition=2024 \
    -o target/webdriver-fixture

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-browser-e2e.XXXXXX")
mkdir -p "$run_root/workspace"
"$repository/target/webdriver-fixture" \
    "$run_root/webdriver.ready" "$run_root/webdriver.log" &
driver_pid=$!

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    kill "$driver_pid" 2>/dev/null || true
    wait "$driver_pid" 2>/dev/null || true
    rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

attempt=0
while [ ! -f "$run_root/webdriver.ready" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "WebDriver fixture did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_BROWSER_HOST_PROGRAM="$repository/target/debug/agentmod-browser-host"
export AGENTMOD_BROWSER_WEBDRIVER_URL
AGENTMOD_BROWSER_WEBDRIVER_URL=$(cat "$run_root/webdriver.ready")
export AGENTMOD_BROWSER_NAME="fixture"
export AGENTMOD_BROWSER_ALLOW_LOOPBACK="true"
export AGENTMOD_PERMISSION_MODE="allow"

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
    "exercise the managed browser" --session "$session_id" \
    --option 'mock_scenario="browser_fixture_flow"' --json)
printf '%s' "$turn" | grep -F \
    '"text":"continued after approved runtime decision"' >/dev/null

session_root="$run_root/sessions/$session_id"
journal="$session_root/events.jsonl"
test "$(grep -c '"event_type":"tool.execution_completed"' "$journal")" -eq 9
test "$(grep -c '"event_type":"tool.execution_failed"' "$journal" || true)" -eq 0
test "$(find "$session_root/artifacts/browser" -maxdepth 1 -name '*.bin' | wc -l)" -eq 2
test "$(find "$session_root/artifacts/browser" -maxdepth 1 -name '*.metadata.json' | wc -l)" -eq 2
test "$(find "$session_root/artifacts/browser/authorization-replay" -name '*.used' | wc -l)" -eq 9
for request in \
    "POST /session" \
    "GET /session/fixture-session/source" \
    "GET /session/fixture-session/screenshot" \
    "POST /session/fixture-session/execute/async" \
    "DELETE /session/fixture-session"; do
    grep -Fx "$request" "$run_root/webdriver.log" >/dev/null
done

echo "managed browser runtime E2E passed"
