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
  pkill -f "$repository/target/debug/agentmod-harness" 2>/dev/null || true
  pkill -f "$repository/target/debug/agentmod-process-host" 2>/dev/null || true
  pkill -f "$repository/target/debug/agentmod-scheduler" 2>/dev/null || true
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness \
  -p agentmod-process-host -p agentmod-scheduler -p agentmod-cli -p agentmod-acp
shell_path="$(command -v sh)"
workspace="$run_root/workspace"
mkdir -p "$workspace"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_PROCESS_HOST_PROGRAM="$repository/target/debug/agentmod-process-host"
export AGENTMOD_PROCESS_ALLOWED_EXECUTABLES="$shell_path"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
export AGENTMOD_ACP_PROVIDER_OPTIONS="$(
  python - "$shell_path" "$run_root/process.pid" <<'PY'
import json
import sys

shell, pid_file = sys.argv[1:]
arguments = {
    "executable": shell,
    "arguments": [
        "-c",
        f"echo $$ > {json.dumps(pid_file)}; echo process-started; sleep 30",
    ],
    "output_limit_bytes": 65536,
    "timeout_ms": 60000,
    "cleanup": "remove_logs_always",
}
print(json.dumps({
    "mock_scenario": "process_action",
    "mock_process_tool": "process.run",
    "mock_process_arguments": json.dumps(arguments, separators=(",", ":")),
}, separators=(",", ":")))
PY
)"

"$repository/target/debug/agentmod-runtime" serve >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"
for _ in $(seq 1 50); do
  if "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
"$repository/target/debug/agentmod" doctor --json >/dev/null

python - "$repository/target/debug/agentmod-acp" "$run_root" "$workspace" <<'PY'
import json
import os
import pathlib
import subprocess
import sys
import time

program, root_text, workspace_text = sys.argv[1:]
root = pathlib.Path(root_text)
workspace = pathlib.Path(workspace_text)
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
send({"jsonrpc":"2.0","id":2,"method":"session/new",
      "params":{"cwd":str(workspace),"mcpServers":[]}})
session_id = receive()["result"]["sessionId"]
send({"jsonrpc":"2.0","id":3,"method":"session/prompt",
      "params":{"sessionId":session_id,
                "prompt":[{"type":"text","text":"cancel process tool"}]}})
for _ in range(20):
    message = receive()
    if (message.get("method") == "session/update"
            and message["params"]["update"].get("sessionUpdate") == "tool_call"):
        break
else:
    raise AssertionError("ACP omitted process tool proposal")

pid_file = root / "process.pid"
for _ in range(50):
    if pid_file.exists() and pid_file.read_text().strip():
        break
    time.sleep(0.1)
else:
    raise AssertionError("process tool did not start")
tool_pid = int(pid_file.read_text().strip())
send({"jsonrpc":"2.0","method":"session/cancel",
      "params":{"sessionId":session_id}})
for _ in range(30):
    message = receive()
    if message.get("id") == 3:
        assert message["result"]["stopReason"] == "cancelled"
        break
else:
    raise AssertionError("process-tool cancellation response missing")

for _ in range(50):
    try:
        os.kill(tool_pid, 0)
    except ProcessLookupError:
        break
    time.sleep(0.1)
else:
    raise AssertionError("cancelled process tool remained alive")

process.stdin.close()
assert process.wait(timeout=5) == 0
(root / "session-id").write_text(session_id)
PY

kill "$runtime_pid"
wait "$runtime_pid" || true
runtime_pid=""
python - "$run_root" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
session_id = (root / "session-id").read_text()
events = [
    json.loads(line)["event"]
    for line in (root / "sessions" / session_id / "events.jsonl").read_text().splitlines()
]
types = [event["metadata"]["event_type"] for event in events]
for required in (
    "tool.execution_started",
    "tool.execution_failed",
    "model.request_cancelled",
):
    assert required in types, f"process-tool cancellation omitted {required}"
assert "tool.execution_completed" not in types
assert "model.response_completed" not in types
assert any(
    event["metadata"]["event_type"] == "tool.execution_failed"
    and event["payload"]["payload"]["code"] == "cancelled"
    for event in events
)
PY
echo "runtime ACP tool cancellation E2E passed"
