#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""
cleanup() {
  status=$?
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

cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-acp \
  -p agentmod-mcp-host -p agentmod-cli
rustc tests/fixtures/mcp_stdio_server.rs --edition=2024 \
  -o target/acp-mcp-stdio-fixture
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_MCP_HOST_PROGRAM="$repository/target/debug/agentmod-mcp-host"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
unset AGENTMOD_MCP_SERVERS_JSON || true
(
  cd "$run_root"
  exec "$repository/target/debug/agentmod-runtime" serve
) >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"
sleep 0.3

python3 - "$repository/target/debug/agentmod-acp" "$run_root" \
  "$repository/target/acp-mcp-stdio-fixture" \
  "$repository/target/debug/agentmod" <<'PY'
import json
import pathlib
import subprocess
import sys
import time

program, root, fixture, cli = sys.argv[1:5]
secret = "acp-per-session-secret-never-persist-plaintext"
mcp_servers = [{"name":"fixture","command":fixture,"args":[],
                "env":[{"name":"FIXTURE_TOKEN","value":secret}]}]
process = subprocess.Popen(
    [program], cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    stderr=subprocess.PIPE, text=True, bufsize=1,
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
capabilities = receive()["result"]["agentCapabilities"]["promptCapabilities"]
assert capabilities == {"image": True, "audio": True, "embeddedContext": True}
session_id = None
for attempt in range(50):
    send({"jsonrpc":"2.0","id":100 + attempt,"method":"session/new",
          "params":{"cwd":root,"mcpServers":mcp_servers}})
    created = receive()
    if created.get("result", {}).get("sessionId"):
        session_id = created["result"]["sessionId"]
        break
    time.sleep(0.1)
assert session_id, "runtime did not become ready"
session_root = pathlib.Path(root, "sessions", session_id)
style_lock = session_root.joinpath("style.lock").read_text()
encrypted = session_root.joinpath("mcp-bootstrap.enc.json").read_text()
assert secret not in style_lock and secret not in encrypted
configuration_reference = json.loads(style_lock)["binding"]["mcp"]["configuration_reference"]
assert configuration_reference.startswith("session-mcp:blake3:")
send({"jsonrpc":"2.0","id":2,"method":"session/load",
      "params":{"sessionId":session_id,"cwd":root,"mcpServers":mcp_servers}})
assert receive().get("result") == {}
send({"jsonrpc":"2.0","id":3,"method":"session/prompt",
      "params":{"sessionId":session_id,"prompt":[
          {"type":"image","data":"not-base64","mimeType":"image/png"}
      ]}})
invalid = receive()
assert invalid["id"] == 3 and "error" in invalid
send({"jsonrpc":"2.0","id":4,"method":"session/prompt",
      "params":{"sessionId":session_id,"prompt":[
          {"type":"text","text":"inspect the attached content"},
          {"type":"image","data":"iVBORw==","mimeType":"image/png",
           "uri":"file:///workspace/image.png"},
          {"type":"audio","data":"c291bmQ=","mimeType":"audio/wav"},
          {"type":"resource","resource":{"uri":"file:///workspace/context.txt",
           "mimeType":"text/plain","text":"embedded context"}},
          {"type":"resource","resource":{"uri":"file:///workspace/data.bin",
           "mimeType":"application/octet-stream","blob":"YmxvYg=="}},
      ]}})
saw_text = False
for _ in range(20):
    message = receive()
    if message.get("method") == "session/update":
        update = message["params"]["update"]
        saw_text |= update.get("sessionUpdate") == "agent_message_chunk" and \
            update["content"].get("text") == "deterministic response"
    if message.get("id") == 4:
        assert message["result"]["stopReason"] == "end_turn"
        break
else:
    raise AssertionError("rich prompt response missing")
assert saw_text
journal = pathlib.Path(root, "sessions", session_id, "events.jsonl").read_text()
for required in ("agentmod_acp_content_version", "iVBORw==", "c291bmQ=",
                 "embedded context", "YmxvYg=="):
    assert required in journal, f"canonical rich prompt omitted {required}"
assert journal.count('"event_type":"model.response_completed"') == 1
turn = subprocess.run(
    [cli, "run", "invoke the configured MCP echo tool", "--session", session_id,
     "--option", 'mock_scenario="mcp_fixture_call"', "--json"],
    check=True, text=True, capture_output=True,
)
assert '"text":"continued after approved runtime decision"' in turn.stdout
journal = session_root.joinpath("events.jsonl").read_text()
assert secret not in journal
assert "echoed-through-runtime" in journal
substituted = [{"name":"fixture","command":fixture,"args":[],
                "env":[{"name":"FIXTURE_TOKEN","value":"substituted"}]}]
send({"jsonrpc":"2.0","id":22,"method":"session/load",
      "params":{"sessionId":session_id,"cwd":root,"mcpServers":substituted}})
assert "error" in receive(), "substituted MCP declaration was accepted"
process.stdin.close()
assert process.wait(timeout=5) == 0
PY
echo "runtime ACP rich-content and per-session MCP E2E passed"
