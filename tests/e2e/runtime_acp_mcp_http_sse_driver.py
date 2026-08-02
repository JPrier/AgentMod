#!/usr/bin/env python3
"""ACP client-side driver for the HTTP and legacy SSE MCP process proof."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import time
import uuid


SECRET = "acp-http-secret-never-persist-plaintext"
PROTOCOL_VERSION = "2025-06-18"
SESSION_ID_HEADER = "agentmod-process-mcp-session"


def wait_for(predicate, message: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


def read_audit(path: pathlib.Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acp", required=True)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--mode", choices=("streamable_http", "legacy_sse"), required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--audit-file", type=pathlib.Path, required=True)
    args = parser.parse_args()
    endpoint = args.origin + ("/mcp" if args.mode == "streamable_http" else "/events")
    declaration = {
        "type": "http" if args.mode == "streamable_http" else "sse",
        "name": "fixture",
        "url": endpoint,
        "headers": [{"name": "X-AgentMod-Secret", "value": SECRET}],
    }
    process = subprocess.Popen(
        [args.acp],
        cwd=args.root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    def send(message: dict[str, object]) -> None:
        assert process.stdin is not None
        process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        process.stdin.flush()

    def receive() -> dict[str, object]:
        assert process.stdout is not None
        line = process.stdout.readline()
        if not line:
            assert process.stderr is not None
            raise RuntimeError("ACP closed: " + process.stderr.read())
        value = json.loads(line)
        assert isinstance(value, dict)
        return value

    try:
        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": 1, "clientCapabilities": {}},
            }
        )
        assert receive().get("id") == 1
        session_id = None
        for attempt in range(50):
            send(
                {
                    "jsonrpc": "2.0",
                    "id": 100 + attempt,
                    "method": "session/new",
                    "params": {"cwd": str(args.root), "mcpServers": [declaration]},
                }
            )
            created = receive()
            session_id = created.get("result", {}).get("sessionId")  # type: ignore[union-attr]
            if session_id:
                break
            time.sleep(0.1)
        assert isinstance(session_id, str) and session_id
        session_root = args.root / "sessions" / session_id
        style_lock_text = (session_root / "style.lock").read_text()
        encrypted_text = (session_root / "mcp-bootstrap.enc.json").read_text()
        binding = json.loads(style_lock_text)["binding"]["mcp"]
        transport = binding["servers"][0]["transport"]
        assert transport["legacy_sse"] is (args.mode == "legacy_sse")
        assert binding["configuration_reference"].startswith("session-mcp:blake3:")
        assert SECRET not in style_lock_text and SECRET not in encrypted_text

        send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/load",
                "params": {
                    "sessionId": session_id,
                    "cwd": str(args.root),
                    "mcpServers": [declaration],
                },
            }
        )
        assert receive().get("result") == {}

        send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": "invoke configured MCP tool"}],
                },
            }
        )
        normal_completed = False
        for _ in range(40):
            message = receive()
            if message.get("id") == 3:
                assert message.get("result", {}).get("stopReason") == "end_turn"  # type: ignore[union-attr]
                normal_completed = True
                break
        assert normal_completed, "normal MCP turn did not complete"
        if args.mode == "streamable_http":
            wait_for(
                lambda: any(
                    event.get("event") == "resume"
                    and event.get("cursor") == "http-progress-1"
                    and event.get("session") == SESSION_ID_HEADER
                    and event.get("protocol") == PROTOCOL_VERSION
                    for event in read_audit(args.audit_file)
                ),
                "Streamable HTTP resume headers/cursor were not observed",
            )

        substituted = dict(declaration)
        substituted["headers"] = [
            {"name": "X-AgentMod-Secret", "value": "substituted"}
        ]
        send(
            {
                "jsonrpc": "2.0",
                "id": 22,
                "method": "session/load",
                "params": {
                    "sessionId": session_id,
                    "cwd": str(args.root),
                    "mcpServers": [substituted],
                },
            }
        )
        assert "error" in receive(), "substituted MCP declaration was accepted"

        send(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": "cancel configured MCP tool"}],
                },
            }
        )
        saw_tool = False
        for _ in range(30):
            message = receive()
            if (
                message.get("method") == "session/update"
                and message.get("params", {}).get("update", {}).get("sessionUpdate")  # type: ignore[union-attr]
                == "tool_call"
            ):
                saw_tool = True
                break
        assert saw_tool, "ACP omitted MCP tool proposal"
        wait_for(
            lambda: any(
                event.get("event") == "cancellation_wait"
                and event.get("call_index") == 2
                for event in read_audit(args.audit_file)
            ),
            "second MCP call did not reach cancellation fixture",
        )
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": session_id},
            }
        )
        for _ in range(40):
            message = receive()
            if message.get("id") == 4:
                stop_reason = message.get("result", {}).get("stopReason")  # type: ignore[union-attr]
                assert stop_reason == "cancelled", (
                    "MCP cancellation returned an unexpected terminal frame: "
                    + json.dumps(message, sort_keys=True)
                )
                break
        else:
            raise AssertionError("MCP cancellation response missing")
        wait_for(
            lambda: any(
                event.get("event") == "cancellation_observed"
                and event.get("disconnected") is True
                for event in read_audit(args.audit_file)
            ),
            "MCP transport did not observe cancellation disconnect",
        )

        journal = (session_root / "events.jsonl").read_text()
        assert "echoed-through-runtime" in journal
        assert SECRET not in journal
        events = [json.loads(line)["event"] for line in journal.splitlines()]
        event_types = [event["metadata"]["event_type"] for event in events]
        assert event_types.count("tool.execution_completed") == 1
        assert "model.request_cancelled" in event_types
        assert any(
            event["metadata"]["event_type"] == "tool.execution_failed"
            and event["payload"]["payload"].get("code") == "cancelled"
            for event in events
        )
        assert SECRET not in args.audit_file.read_text()
        secret_bytes = SECRET.encode()
        assert not any(
            secret_bytes in path.read_bytes()
            for path in session_root.rglob("*")
            if path.is_file()
        )
        (args.root / f"{args.mode}.session-id").write_text(session_id)
    finally:
        if process.stdin is not None:
            process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
