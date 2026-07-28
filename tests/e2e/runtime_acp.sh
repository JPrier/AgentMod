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

cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-acp
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_HARNESS_FRAME_PACING_MS=750
export AGENTMOD_SCHEDULER_POLL_MS=0
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
import time

program, root = sys.argv[1], sys.argv[2]
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
initialized = receive()
assert initialized["result"]["agentCapabilities"]["loadSession"] is True
send({"jsonrpc":"2.0","id":2,"method":"session/new",
      "params":{"cwd":root,"mcpServers":[]}})
session_id = receive()["result"]["sessionId"]
send({"jsonrpc":"2.0","id":3,"method":"session/prompt",
      "params":{"sessionId":session_id,
                "prompt":[{"type":"text","text":"hello through ACP"}]}})
saw_text = False
text_observed_at = None
completed_at = None
for _ in range(20):
    message = receive()
    if message.get("method") == "session/update":
        update = message["params"]["update"]
        saw_text |= (
            update.get("sessionUpdate") == "agent_message_chunk"
            and update["content"].get("text") == "deterministic response"
        )
        if saw_text and text_observed_at is None:
            text_observed_at = time.monotonic()
    if message.get("id") == 3:
        assert message["result"]["stopReason"] == "end_turn"
        completed_at = time.monotonic()
        break
else:
    raise AssertionError("ACP prompt response missing")
assert saw_text
assert completed_at - text_observed_at >= 0.5, \
    "ACP buffered the runtime stream instead of forwarding incrementally"
journal = pathlib.Path(root, "sessions", session_id, "events.jsonl").read_text()
assert journal.count('"event_type":"model.response_completed"') == 1
process.stdin.close()
assert process.wait(timeout=5) == 0
PY
echo "runtime ACP E2E passed"
