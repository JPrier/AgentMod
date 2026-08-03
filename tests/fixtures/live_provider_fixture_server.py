#!/usr/bin/env python3
"""Deterministic local OpenAI-compatible fixture server for live-provider E2Es.

Writes its bound port to the file given as argv[1], then serves chunked SSE
chat-completion responses that echo a stable marker. Requires no network and no
credentials. Used by `tests/e2e/runtime_live_provider_fixture.{sh,ps1}`.
"""

import http.server
import json
import socketserver
import sys


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        body = json.dumps(
            {
                "id": "chatcmpl-fixture",
                "object": "chat.completion.chunk",
                "model": "fixture-model",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": "runtime-live-fixture-ok"},
                        "finish_reason": None,
                    }
                ],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        chunk = b"data: " + body + b"\n\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
        done = b"data: [DONE]\n\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(done), done))
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass


class Server(socketserver.TCPServer):
    def server_bind(self):
        super().server_bind()
        with open(sys.argv[1], "w") as f:
            f.write(str(self.server_address[1]))
        self.port_ready = True


with Server(("127.0.0.1", 0), Handler) as server:
    server.serve_forever()
