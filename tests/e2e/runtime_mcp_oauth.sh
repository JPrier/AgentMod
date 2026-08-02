#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
if [ "${AGENTMOD_E2E_SKIP_BUILD:-0}" != 1 ]; then
    cargo build --locked -p agentmod-runtime -p agentmod-mcp-host \
        -p agentmod-cli -p agentmod-tui
fi

runtime="$repository/target/debug/agentmod-runtime"
mcp_host="$repository/target/debug/agentmod-mcp-host"
cli="$repository/target/debug/agentmod"
tui="$repository/target/debug/agentmod-tui"
run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-mcp-oauth-e2e.XXXXXX")
mkdir -p "$run_root/workspace"
fixture_port=$(python3 -c \
    'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
callback_port=$(python3 -c \
    'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
origin="http://127.0.0.1:$fixture_port"

python3 "$repository/tests/fixtures/mcp_oauth_server.py" \
    --port "$fixture_port" >"$run_root/fixture.log" 2>&1 &
fixture_pid=$!
attempt=0
until curl --silent --output /dev/null "$origin/mcp"; do
    status=$?
    if [ "$status" -eq 22 ]; then
        break
    fi
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 100 ]; then
        cat "$run_root/fixture.log" >&2
        exit 1
    fi
    sleep 0.05
done
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN=\
"0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_MCP_HOST_PROGRAM="$mcp_host"
export AGENTMOD_MCP_OAUTH_KEY=\
"5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b"
test "${#AGENTMOD_MCP_OAUTH_KEY}" -eq 64
export AGENTMOD_MCP_SERVERS_JSON="[{\"id\":\"protected\",\"display_name\":\"Protected process fixture\",\"active\":true,\"transport\":\"streamable_http_oauth\",\"url\":\"$origin/mcp\",\"authorization_server\":\"$origin/issuer\",\"client_id\":\"agentmod-process-client\",\"client_secret_environment\":null,\"redirect_uri\":\"http://127.0.0.1:$callback_port/callback\",\"scopes\":[\"tools.read\"]}]"
succeeded=0

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    kill "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
    if [ "$succeeded" -eq 1 ]; then
        rm -rf -- "$run_root"
    else
        echo "preserving failed MCP OAuth E2E at $run_root" >&2
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

start_runtime
created=$("$cli" session create --workspace "$run_root/workspace" \
    --style persistent-chat --json)
session_id=$(printf '%s' "$created" | python3 -c \
    'import json,sys;print(json.load(sys.stdin)["session_id"])')
resources=$("$tui" --smoke-session-command "$session_id" /resources)
printf '%s' "$resources" | grep -F "selected=$session_id" >/dev/null
printf '%s' "$resources" | grep -F 'resources=0/0/0' >/dev/null
tui_begun=$("$tui" --smoke-session-command "$session_id" \
    '/mcp-oauth-begin protected')
printf '%s' "$tui_begun" | grep -F "selected=$session_id" >/dev/null
tui_transaction=$(printf '%s' "$tui_begun" | sed -n \
    's/.*mcp=protected:pending:\([^ ]*\).*/\1/p')
test -n "$tui_transaction"
tui_status=$("$tui" --smoke-session-command "$session_id" \
    '/mcp-oauth-status protected')
printf '%s' "$tui_status" | grep -F "selected=$session_id" >/dev/null
printf '%s' "$tui_status" | \
    grep -F "mcp=protected:pending:$tui_transaction" >/dev/null
tui_cancelled=$("$tui" --smoke-session-command "$session_id" \
    "/mcp-oauth-cancel protected $tui_transaction")
printf '%s' "$tui_cancelled" | grep -F "selected=$session_id" >/dev/null
printf '%s' "$tui_cancelled" | \
    grep -F 'mcp=protected:unauthorized:none' >/dev/null
cancelled_status=$("$cli" mcp oauth status protected \
    --session "$session_id" --json)
printf '%s' "$cancelled_status" | grep -F '"status":"unauthorized"' >/dev/null
printf '%s' "$cancelled_status" | grep -F '"transaction_id":null' >/dev/null
begun=$("$cli" mcp oauth begin protected --session "$session_id" --json)
authorization_url=$(printf '%s' "$begun" | python3 -c \
    'import json,sys;print(json.load(sys.stdin)["authorization_url"])')
authorization_url_hash=$(printf '%s' "$begun" | python3 -c \
    'import json,sys;print(json.load(sys.stdin)["authorization_url_hash"])')
test "${#authorization_url_hash}" -eq 64

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
unset daemon_pid
sleep 0.25
start_runtime
recovered=$("$cli" mcp oauth status protected --session "$session_id" --json)
test "$(printf '%s' "$recovered" | python3 -c \
    'import json,sys;print(json.load(sys.stdin)["status"])')" = pending
curl --fail --silent --show-error --location "$authorization_url" >/dev/null

attempt=0
status=pending
while [ "$status" = pending ]; do
    projection=$("$cli" mcp oauth status protected \
        --session "$session_id" --json)
    status=$(printf '%s' "$projection" | python3 -c \
        'import json,sys;print(json.load(sys.stdin)["status"])')
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 100 ]; then
        printf '%s\n' "$projection" >&2
        exit 1
    fi
    sleep 0.05
done
test "$status" = authorized
printf '%s' "$projection" | grep -F '"tools.read"' >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c '"event_type":"mcp.oauth_management_audited"' "$journal")" -ge 2
test "$(grep -o '"request_hash":"[0-9a-f]\{64\}"' "$journal" | wc -l)" -ge 2
test "$(grep -o '"configuration_hash":"[0-9a-f]\{64\}"' "$journal" | wc -l)" -ge 2
test "$(grep -o '"result_hash":"[0-9a-f]\{64\}"' "$journal" | wc -l)" -ge 2
for secret in "$authorization_url" process-authorization-code \
    process-access-token process-refresh-token code_verifier; do
    if grep -F "$secret" "$journal" >/dev/null; then
        echo "secret or transient OAuth material leaked into journal" >&2
        exit 1
    fi
done
for secret in process-authorization-code process-access-token \
    process-refresh-token; do
    if grep -R -F "$secret" "$run_root/sessions" >/dev/null 2>&1; then
        echo "OAuth secret leaked into durable host state" >&2
        exit 1
    fi
done

succeeded=1
echo "runtime MCP OAuth restart/audit process E2E passed"
