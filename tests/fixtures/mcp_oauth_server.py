#!/usr/bin/env python3
"""Small process fixture for the MCP OAuth authorization-code/PKCE E2Es."""

from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format: str, *_args: object) -> None:
        return

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def send_json(self, status: int, value: object) -> None:
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        print(f"GET {parsed.path}", file=sys.stderr, flush=True)
        if parsed.path == "/mcp":
            self.send_response(401)
            self.send_header(
                "WWW-Authenticate",
                f'Bearer resource_metadata="{self.origin}/resource-metadata", '
                'scope="tools.read"',
            )
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if parsed.path == "/resource-metadata":
            self.send_json(
                200,
                {
                    "resource": f"{self.origin}/mcp",
                    "authorization_servers": [f"{self.origin}/issuer"],
                    "scopes_supported": ["tools.read"],
                    "bearer_methods_supported": ["header"],
                },
            )
            return
        if parsed.path == "/.well-known/oauth-authorization-server/issuer":
            self.send_json(
                200,
                {
                    "issuer": f"{self.origin}/issuer",
                    "authorization_endpoint": f"{self.origin}/authorize",
                    "token_endpoint": f"{self.origin}/token",
                    "code_challenge_methods_supported": ["S256"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    "token_endpoint_auth_methods_supported": ["none"],
                    "protected_resources": [f"{self.origin}/mcp"],
                },
            )
            return
        if parsed.path == "/authorize":
            query = parse_qs(parsed.query)
            required = (
                query.get("response_type") == ["code"]
                and query.get("code_challenge_method") == ["S256"]
                and bool(query.get("code_challenge", [""])[0])
                and query.get("resource") == [f"{self.origin}/mcp"]
                and bool(query.get("state", [""])[0])
            )
            if not required:
                self.send_error(400)
                return
            redirect = query.get("redirect_uri", [""])[0]
            separator = "&" if "?" in redirect else "?"
            location = (
                redirect
                + separator
                + urlencode(
                    {
                        "code": "process-authorization-code",
                        "state": query["state"][0],
                    }
                )
            )
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_error(404)

    def do_POST(self) -> None:
        print(f"POST {self.path}", file=sys.stderr, flush=True)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if self.path == "/token":
            form = parse_qs(body.decode())
            if form.get("resource") != [f"{self.origin}/mcp"]:
                self.send_error(400)
                return
            if form.get("grant_type") == ["authorization_code"]:
                if (
                    form.get("code") != ["process-authorization-code"]
                    or not form.get("code_verifier", [""])[0]
                ):
                    self.send_error(400)
                    return
                self.send_json(
                    200,
                    {
                        "access_token": "process-access-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "refresh_token": "process-refresh-token",
                        "scope": "tools.read",
                    },
                )
                return
            self.send_error(400)
            return
        if self.path == "/mcp":
            if self.headers.get("Authorization") != "Bearer process-access-token":
                self.send_error(401)
                return
            request = json.loads(body)
            if request.get("method") == "initialize":
                result = {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                }
            else:
                result = {
                    "tools": [
                        {
                            "name": "echo",
                            "description": "Echo",
                            "inputSchema": {"type": "object"},
                        }
                    ]
                }
            self.send_json(
                200,
                {"jsonrpc": "2.0", "id": request.get("id"), "result": result},
            )
            return
        self.send_error(404)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
