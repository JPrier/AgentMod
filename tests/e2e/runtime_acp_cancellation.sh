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
export AGENTMOD_ACP_PROVIDER_OPTIONS='{"mock_scenario":"streaming_text"}'
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

def new_session(request_id):
    workspace = root / f"workspace-{request_id}"
    workspace.mkdir()
    send({"jsonrpc":"2.0","id":request_id,"method":"session/new",
          "params":{"cwd":str(workspace),"mcpServers":[]}})
    response = receive()
    assert response["id"] == request_id
    return response["result"]["sessionId"]

send({"jsonrpc":"2.0","id":1,"method":"initialize",
      "params":{"protocolVersion":1,"clientCapabilities":{}}})
assert receive()["id"] == 1

before_session = new_session(10)
send({"jsonrpc":"2.0","id":11,"method":"session/prompt",
      "params":{"sessionId":before_session,
                "prompt":[{"type":"text","text":"cancel immediately"}]}})
send({"jsonrpc":"2.0","method":"session/cancel",
      "params":{"sessionId":before_session}})
for _ in range(20):
    message = receive()
    if message.get("id") == 11:
        assert message["result"]["stopReason"] == "cancelled"
        break
else:
    raise AssertionError("pre-start cancellation response missing")

stream_session = new_session(20)
send({"jsonrpc":"2.0","id":21,"method":"session/prompt",
      "params":{"sessionId":stream_session,
                "prompt":[{"type":"text","text":"cancel during stream"}]}})
saw_text = False
for _ in range(30):
    message = receive()
    if (message.get("method") == "session/update"
            and message["params"]["update"].get("sessionUpdate")
            == "agent_message_chunk" and not saw_text):
        saw_text = True
        send({"jsonrpc":"2.0","method":"session/cancel",
              "params":{"sessionId":stream_session}})
    if message.get("id") == 21:
        assert message["result"]["stopReason"] == "cancelled"
        break
else:
    raise AssertionError("stream cancellation response missing")
assert saw_text
process.stdin.close()
assert process.wait(timeout=5) == 0
(root / "cancelled-sessions.json").write_text(
    json.dumps([before_session, stream_session])
)
PY

kill "$runtime_pid"
wait "$runtime_pid" || true
runtime_pid=""
python - "$run_root" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
before_session, stream_session = json.loads(
    (root / "cancelled-sessions.json").read_text()
)
for session_id, require_text in (
    (before_session, False),
    (stream_session, True),
):
    events = [
        json.loads(line)["event"]["metadata"]["event_type"]
        for line in (root / "sessions" / session_id / "events.jsonl").read_text().splitlines()
    ]
    assert events.count("model.request_cancelled") == 1
    assert "model.response_completed" not in events
    if require_text:
        assert "model.output_delta_observed" in events
PY
echo "runtime ACP cancellation E2E passed"
