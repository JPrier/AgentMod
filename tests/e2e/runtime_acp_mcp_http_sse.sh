#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""
fixture_pid=""
cleanup() {
  status=$?
  if [[ -n "$fixture_pid" ]]; then
    kill "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
  fi
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
  fi
  if [[ $status -ne 0 && -f "$run_root/runtime.log" ]]; then
    cat "$run_root/runtime.log" >&2
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build --locked -p agentmod-runtime -p agentmod-harness \
  -p agentmod-acp -p agentmod-mcp-host -p agentmod-cli
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
acp="$repository/target/debug/agentmod-acp"
mcp_host="$repository/target/debug/agentmod-mcp-host"
cli="$repository/target/debug/agentmod"
fixture="$repository/tests/fixtures/mcp_http_sse_server.py"
driver="$repository/tests/e2e/runtime_acp_mcp_http_sse_driver.py"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_MCP_HOST_PROGRAM="$mcp_host"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
export AGENTMOD_ACP_PROVIDER_OPTIONS='{"mock_scenario":"mcp_fixture_call"}'
unset AGENTMOD_MCP_SERVERS_JSON || true
(
  cd "$run_root"
  exec "$runtime" serve
) >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"
for _ in $(seq 1 100); do
  if "$cli" doctor --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
"$cli" doctor --json >/dev/null

for mode in streamable_http legacy_sse; do
  port_file="$run_root/$mode.port"
  audit_file="$run_root/$mode.audit.jsonl"
  fixture_error="$run_root/$mode.fixture.err.log"
  python3 "$fixture" --mode "$mode" --port-file "$port_file" \
    --audit-file "$audit_file" >"$run_root/$mode.fixture.out.log" \
    2>"$fixture_error" &
  fixture_pid="$!"
  for _ in $(seq 1 100); do
    if [[ -s "$port_file" ]]; then
      break
    fi
    if ! kill -0 "$fixture_pid" 2>/dev/null; then
      cat "$fixture_error" >&2
      echo "$mode fixture stopped before publishing a port" >&2
      exit 1
    fi
    sleep 0.05
  done
  if [[ ! -s "$port_file" ]]; then
    echo "$mode fixture did not publish a port" >&2
    exit 1
  fi
  port="$(tr -d '\r\n' <"$port_file")"
  python3 "$driver" --acp "$acp" --root "$run_root" --mode "$mode" \
    --origin "http://127.0.0.1:$port" --audit-file "$audit_file"
  kill "$fixture_pid" 2>/dev/null || true
  wait "$fixture_pid" 2>/dev/null || true
  fixture_pid=""
done

echo "runtime ACP Streamable HTTP and legacy SSE MCP E2E passed"
