#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""
cleanup() {
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness \
  -p agentmod-filesystem-host -p agentmod-cli -p agentmod-acp
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"
export AGENTMOD_ACP_PROVIDER_OPTIONS='{"mock_scenario":"approval_write"}'
export AGENTMOD_SCHEDULER_POLL_MS=0
unset AGENTMOD_PERMISSION_MODE || true
"$repository/target/debug/agentmod-runtime" serve >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"
for _ in $(seq 1 50); do
  if "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
"$repository/target/debug/agentmod" doctor --json >/dev/null

python - "$repository/target/debug/agentmod-acp" "$run_root" <<'PY'
import json
import pathlib
import subprocess
import sys

program, root = sys.argv[1], pathlib.Path(sys.argv[2])
process = subprocess.Popen(
    [program],
    cwd=root,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
)

def send(message):
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()

def receive():
    line = process.stdout.readline()
    if not line:
        raise RuntimeError("ACP closed: " + process.stderr.read())
    return json.loads(line)

send({"jsonrpc":"2.0","id":1,"method":"initialize",
      "params":{"protocolVersion":1,"clientCapabilities":{}}})
assert receive()["id"] == 1

def scenario(base_id, decision, cancelled):
    workspace = root / f"workspace-{base_id}"
    workspace.mkdir()
    send({"jsonrpc":"2.0","id":base_id,"method":"session/new",
          "params":{"cwd":str(workspace),"mcpServers":[]}})
    created = receive()
    assert created["id"] == base_id
    session_id = created["result"]["sessionId"]
    prompt_id = base_id + 1
    send({"jsonrpc":"2.0","id":prompt_id,"method":"session/prompt",
          "params":{"sessionId":session_id,
                    "prompt":[{"type":"text","text":"permission scenario"}]}})
    saw_tool = False
    saw_permission = False
    stop_reason = None
    for _ in range(30):
        message = receive()
        if message.get("method") == "session/update":
            saw_tool |= message["params"]["update"].get("sessionUpdate") == "tool_call"
        if message.get("method") == "session/request_permission":
            saw_permission = True
            if cancelled:
                send({"jsonrpc":"2.0","method":"session/cancel",
                      "params":{"sessionId":session_id}})
                send({"jsonrpc":"2.0","id":message["id"],
                      "result":{"outcome":{"outcome":"cancelled"}}})
            else:
                send({"jsonrpc":"2.0","id":message["id"],
                      "result":{"outcome":{"outcome":"selected",
                                           "optionId":decision}}})
        if message.get("id") == prompt_id:
            stop_reason = message["result"]["stopReason"]
            break
    assert saw_tool and saw_permission
    assert stop_reason == ("cancelled" if cancelled else "end_turn")
    return session_id, workspace

allowed_id, allowed_workspace = scenario(10, "allow-once", False)
assert (allowed_workspace / "approved.txt").read_text() == "executed once\n"

denied_id, denied_workspace = scenario(20, "reject-once", False)
assert not (denied_workspace / "approved.txt").exists()

cancelled_id, cancelled_workspace = scenario(30, "reject-once", True)
assert not (cancelled_workspace / "approved.txt").exists()
events = [
    json.loads(line)["event"]["metadata"]["event_type"]
    for line in (root / "sessions" / cancelled_id / "events.jsonl").read_text().splitlines()
]
assert events.count("model.request_started") == 1
assert events.count("model.request_cancelled") == 1
assert "model.response_completed" not in events

process.stdin.close()
assert process.wait(timeout=5) == 0
PY
echo "runtime ACP permission E2E passed"
