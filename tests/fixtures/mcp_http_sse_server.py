#!/usr/bin/env python3
"""Deterministic MCP Streamable HTTP / legacy SSE process fixture."""

from __future__ import annotations

import argparse
import json
import queue
import socket
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


PROTOCOL_VERSION = "2025-06-18"
SESSION_ID = "agentmod-process-mcp-session"
SECRET = "acp-http-secret-never-persist-plaintext"


class FixtureState:
    def __init__(self, mode: str, audit_path: Path) -> None:
        self.mode = mode
        self.audit_path = audit_path
        self.lock = threading.Lock()
        self.call_count = 0
        self.connections: dict[str, queue.Queue[tuple[str, str, str]]] = {}

    def audit(self, **fields: object) -> None:
        record = {"mode": self.mode, **fields}
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"))
        with self.lock:
            with self.audit_path.open("a", encoding="utf-8") as stream:
                stream.write(encoded + "\n")

    def next_call(self) -> int:
        with self.lock:
            self.call_count += 1
            return self.call_count


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "AgentModMcpFixture/1"

    @property
    def state(self) -> FixtureState:
        return self.server.fixture_state  # type: ignore[attr-defined]

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _headers_valid(self, *, initialized: bool) -> bool:
        secret_ok = self.headers.get("X-AgentMod-Secret") == SECRET
        session = self.headers.get("MCP-Session-Id")
        protocol = self.headers.get("MCP-Protocol-Version")
        runtime_headers_ok = not initialized or (
            session == SESSION_ID and protocol == PROTOCOL_VERSION
        )
        if not secret_ok or not runtime_headers_ok:
            self.state.audit(
                event="invalid_headers",
                secret_ok=secret_ok,
                session_ok=session == SESSION_ID,
                protocol_ok=protocol == PROTOCOL_VERSION,
            )
        return secret_ok and runtime_headers_ok

    def _read_json(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 1024 * 1024:
            raise ValueError("invalid body length")
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("request must be an object")
        return value

    def _empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def _json(self, value: object, *, session: bool = False) -> None:
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if session:
            self.send_header("MCP-Session-Id", SESSION_ID)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _chunk(self, body: str) -> None:
        encoded = body.encode()
        self.wfile.write(f"{len(encoded):X}\r\n".encode())
        self.wfile.write(encoded)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _client_disconnected(self, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        self.connection.settimeout(0.05)
        while time.monotonic() < deadline:
            try:
                if self.connection.recv(1, socket.MSG_PEEK) == b"":
                    return True
            except (TimeoutError, socket.timeout):
                pass
            except (ConnectionResetError, OSError):
                return True
            time.sleep(0.05)
        return False

    def _rpc_result(self, request: dict[str, object]) -> dict[str, object]:
        method = request.get("method")
        request_id = request.get("id")
        if method == "initialize":
            result: object = {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "agentmod-process-fixture", "version": "1.0.0"},
            }
        elif method == "tools/call":
            result = {
                "content": [{"type": "text", "text": "echoed-through-runtime"}],
                "isError": False,
            }
        else:
            result = {}
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        try:
            request = self._read_json()
        except (ValueError, json.JSONDecodeError):
            self._empty(400)
            return
        method = str(request.get("method", ""))
        initialized = method != "initialize"
        if not self._headers_valid(initialized=initialized):
            self._empty(403)
            return
        if self.state.mode == "streamable_http" and self.path == "/mcp":
            self._streamable_post(request)
            return
        if self.state.mode == "legacy_sse" and self.path.startswith("/messages?"):
            self._legacy_post(request)
            return
        self._empty(404)

    def _streamable_post(self, request: dict[str, object]) -> None:
        method = str(request.get("method", ""))
        self.state.audit(
            event="request",
            transport="streamable_http",
            rpc_method=method,
            secret_ok=True,
            session=self.headers.get("MCP-Session-Id"),
            protocol=self.headers.get("MCP-Protocol-Version"),
        )
        if method == "initialize":
            self._json(self._rpc_result(request), session=True)
            return
        self.server.last_request_id = str(request.get("id", ""))  # type: ignore[attr-defined]
        call_index = self.state.next_call()
        request_id = str(request.get("id", ""))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("MCP-Session-Id", SESSION_ID)
        self.send_header("Connection", "close")
        if call_index == 1:
            body = (
                "id: http-progress-1\n"
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\","
                "\"params\":{\"progress\":1}}\n\n"
            ).encode()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.state.audit(event="resume_required", call_index=call_index)
            return
        self.end_headers()
        self.state.audit(event="cancellation_wait", call_index=call_index)
        disconnected = self._client_disconnected()
        terminal = (
            "id: http-cancel-late\n"
            f"data: {json.dumps(self._rpc_result(request), separators=(',', ':'))}\n\n"
        )
        try:
            self.wfile.write(terminal.encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        self.state.audit(
            event="cancellation_observed",
            call_index=call_index,
            disconnected=disconnected,
            request_id_present=bool(request_id),
        )

    def _legacy_post(self, request: dict[str, object]) -> None:
        connection_id = parse_qs(urlparse(self.path).query).get("connection", [""])[0]
        with self.state.lock:
            events = self.state.connections.get(connection_id)
        if events is None:
            self._empty(404)
            return
        method = str(request.get("method", ""))
        self.state.audit(
            event="request",
            transport="legacy_sse",
            rpc_method=method,
            secret_ok=True,
            session=self.headers.get("MCP-Session-Id"),
            protocol=self.headers.get("MCP-Protocol-Version"),
        )
        self._empty(202)
        if method == "tools/call":
            call_index = self.state.next_call()
            events.put(
                (
                    f"legacy-progress-{call_index}",
                    "message",
                    '{"jsonrpc":"2.0","method":"notifications/progress",'
                    f'"params":{{"progress":{call_index}}}}}',
                )
            )
            if call_index > 1:
                self.state.audit(event="cancellation_wait", call_index=call_index)
                result = json.dumps(self._rpc_result(request), separators=(",", ":"))

                def late_result() -> None:
                    time.sleep(2.0)
                    events.put(("legacy-cancel-late", "message", result))

                threading.Thread(target=late_result, daemon=True).start()
                return
        else:
            call_index = 0
        events.put(
            (
                f"legacy-result-{method}-{call_index}",
                "message",
                json.dumps(self._rpc_result(request), separators=(",", ":")),
            )
        )

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        if self.state.mode == "streamable_http" and self.path == "/mcp":
            if not self._headers_valid(initialized=True):
                self._empty(403)
                return
            cursor = self.headers.get("Last-Event-ID")
            if cursor != "http-progress-1":
                self._empty(409)
                return
            self.state.audit(
                event="resume",
                cursor=cursor,
                session=self.headers.get("MCP-Session-Id"),
                protocol=self.headers.get("MCP-Protocol-Version"),
                secret_ok=True,
            )
            body = (
                "id: http-result-1\n"
                'data: {"jsonrpc":"2.0","id":"'
                + self.server.last_request_id  # type: ignore[attr-defined]
                + '","result":{"content":[{"type":"text",'
                '"text":"echoed-through-runtime"}],"isError":false}}\n\n'
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("MCP-Session-Id", SESSION_ID)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            return
        if self.state.mode != "legacy_sse" or self.path != "/events":
            self._empty(404)
            return
        if not self._headers_valid(initialized=False):
            self._empty(403)
            return
        connection_id = uuid.uuid4().hex
        events: queue.Queue[tuple[str, str, str]] = queue.Queue()
        with self.state.lock:
            self.state.connections[connection_id] = events
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("MCP-Session-Id", SESSION_ID)
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self._chunk(f"event: endpoint\ndata: /messages?connection={connection_id}\n\n")
        self.state.audit(
            event="legacy_connected",
            cursor=self.headers.get("Last-Event-ID"),
            secret_ok=True,
        )
        try:
            while True:
                event_id, event_type, data = events.get(timeout=30)
                if event_id == "legacy-cancel-late" and self._client_disconnected():
                    self.state.audit(event="cancellation_observed", disconnected=True)
                    break
                self._chunk(f"id: {event_id}\nevent: {event_type}\ndata: {data}\n\n")
        except queue.Empty:
            self.state.audit(event="legacy_idle_timeout")
        except (BrokenPipeError, ConnectionResetError):
            self.state.audit(event="cancellation_observed", disconnected=True)
        finally:
            with self.state.lock:
                self.state.connections.pop(connection_id, None)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("streamable_http", "legacy_sse"), required=True)
    parser.add_argument("--port-file", type=Path, required=True)
    parser.add_argument("--audit-file", type=Path, required=True)
    args = parser.parse_args()
    args.audit_file.parent.mkdir(parents=True, exist_ok=True)
    args.audit_file.write_text("", encoding="utf-8")
    server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
    server.fixture_state = FixtureState(args.mode, args.audit_file)  # type: ignore[attr-defined]
    server.last_request_id = ""  # type: ignore[attr-defined]
    args.port_file.write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever(poll_interval=0.05)


if __name__ == "__main__":
    main()
