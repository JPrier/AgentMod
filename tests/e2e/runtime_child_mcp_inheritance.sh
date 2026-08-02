#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-child-mcp-e2e.XXXXXX")"
runtime_pid=""
succeeded=false

stop_runtime() {
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
    runtime_pid=""
  fi
}

cleanup() {
  status=$?
  stop_runtime
  if [[ $status -ne 0 ]]; then
    for log in "$run_root"/*.log; do
      if [[ -f "$log" ]]; then
        echo "--- $(basename "$log") ---" >&2
        cat "$log" >&2
      fi
    done
    if [[ -f "$run_root/mcp-effects.jsonl" ]]; then
      echo "--- mcp-effects.jsonl ---" >&2
      cat "$run_root/mcp-effects.jsonl" >&2
    fi
    echo "retained failed child MCP E2E root: $run_root" >&2
    return
  fi
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-child-mcp-e2e.*)
      rm -rf -- "$run_root"
      ;;
    *) echo "refusing unexpected child MCP E2E root: $run_root" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM
cd "$repository"

cargo build --locked -p agentmod-runtime -p agentmod-harness \
  -p agentmod-acp -p agentmod-mcp-host -p agentmod-cli
target_dir="${CARGO_TARGET_DIR:-target}"
case "$target_dir" in
  /*) ;;
  *) target_dir="$repository/$target_dir" ;;
esac
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
acp="$target_dir/debug/agentmod-acp"
mcp_host="$target_dir/debug/agentmod-mcp-host"
cli="$target_dir/debug/agentmod"
driver="$repository/tests/e2e/runtime_child_mcp_inheritance_driver.py"
fixture_source="$repository/tests/fixtures/mcp_stdio_server.rs"
fixture="$run_root/mcp-stdio-fixture"
workspace="$run_root/workspace"
audit_file="$run_root/mcp-effects.jsonl"
state_file="$run_root/child-mcp-state.json"
rustc "$fixture_source" --edition=2024 -o "$fixture"
python3 "$driver" --phase prepare --root "$run_root" --workspace "$workspace"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_MCP_HOST_PROGRAM="$mcp_host"
export AGENTMOD_USER_STYLES_DIR="$run_root/styles/user"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
export AGENTMOD_ACP_PROVIDER_OPTIONS='{"mock_scenario":"mcp_fixture_call"}'
unset AGENTMOD_MCP_SERVERS_JSON || true

start_runtime() {
  local log_stem="$1"
  (
    cd "$run_root"
    exec "$runtime" serve
  ) >"$run_root/$log_stem.log" 2>&1 &
  runtime_pid="$!"
  for _ in $(seq 1 120); do
    if "$cli" doctor --json >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$runtime_pid" 2>/dev/null; then
      echo "runtime stopped before becoming ready" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "runtime did not become ready" >&2
  return 1
}

start_runtime runtime-initial
python3 "$driver" --phase execute --acp "$acp" --cli "$cli" \
  --fixture "$fixture" --root "$run_root" --workspace "$workspace" \
  --audit-file "$audit_file" --state-file "$state_file"

stop_runtime
start_runtime runtime-restarted
python3 "$driver" --phase replay --acp "$acp" --cli "$cli" \
  --fixture "$fixture" --root "$run_root" --workspace "$workspace" \
  --audit-file "$audit_file" --state-file "$state_file"

succeeded=true
echo "runtime exact child MCP inheritance E2E passed"
