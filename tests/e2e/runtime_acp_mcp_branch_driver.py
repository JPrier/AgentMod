#!/usr/bin/env python3
"""Cross-platform driver for exact ACP MCP bootstrap branching."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess


SECRET = "acp-branch-secret-never-persist-plaintext"


def run_cli(
    cli: str, *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [cli, *arguments], capture_output=True, text=True, check=False, timeout=30
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"CLI failed ({result.returncode}): {' '.join(arguments)}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return result


def cli_json(cli: str, *arguments: str) -> dict[str, object]:
    value = json.loads(run_cli(cli, *arguments).stdout)
    assert isinstance(value, dict)
    return value


class AcpClient:
    def __init__(self, program: str, root: pathlib.Path) -> None:
        self.process = subprocess.Popen(
            [program],
            cwd=root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        response = self.request(
            1,
            "initialize",
            {"protocolVersion": 1, "clientCapabilities": {}},
        )
        assert "result" in response, response

    def send(self, message: dict[str, object]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.process.stdin.flush()

    def receive(self) -> dict[str, object]:
        assert self.process.stdout is not None
        line = self.process.stdout.readline()
        if not line:
            assert self.process.stderr is not None
            raise RuntimeError("ACP closed: " + self.process.stderr.read())
        value = json.loads(line)
        assert isinstance(value, dict)
        return value

    def request(
        self, request_id: int, method: str, params: dict[str, object]
    ) -> dict[str, object]:
        self.send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        for _ in range(100):
            response = self.receive()
            if response.get("id") == request_id:
                return response
        raise AssertionError(f"ACP response {request_id} missing")

    def close(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()
        try:
            code = self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            code = self.process.wait(timeout=5)
        assert code == 0, "ACP did not shut down cleanly"


def declaration(fixture: str, audit: pathlib.Path, secret: str = SECRET) -> dict[str, object]:
    return {
        "name": "fixture",
        "command": fixture,
        "args": [],
        "env": [
            {"name": "FIXTURE_TOKEN", "value": secret},
            {"name": "FIXTURE_AUDIT_PATH", "value": str(audit)},
        ],
    }


def create_session(
    client: AcpClient,
    request_id: int,
    workspace: pathlib.Path,
    server: dict[str, object],
) -> str:
    response = client.request(
        request_id,
        "session/new",
        {"cwd": str(workspace), "mcpServers": [server]},
    )
    session_id = response.get("result", {}).get("sessionId")  # type: ignore[union-attr]
    assert isinstance(session_id, str) and session_id, response
    return session_id


def invoke_parent(client: AcpClient, session_id: str) -> None:
    response = client.request(
        10,
        "session/prompt",
        {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": "invoke parent MCP fixture"}],
        },
    )
    assert response.get("result", {}).get("stopReason") == "end_turn", response  # type: ignore[union-attr]


def effect_count(audit: pathlib.Path) -> int:
    if not audit.exists():
        return 0
    events = [json.loads(line) for line in audit.read_text().splitlines() if line]
    assert all(event == {"event": "tools/call"} for event in events)
    return len(events)


def session_head(cli: str, session_id: str) -> int:
    value = cli_json(cli, "session", "inspect", session_id, "--json")
    head = value.get("head_sequence")
    assert isinstance(head, int) and head > 0, value
    return head


def branch(
    cli: str, parent: str, sequence: int, *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return run_cli(
        cli,
        "session",
        "branch",
        parent,
        "--at",
        str(sequence),
        "--json",
        check=check,
    )


def assert_no_secret(root: pathlib.Path) -> None:
    secret = SECRET.encode()
    assert not any(
        secret in path.read_bytes() for path in root.rglob("*") if path.is_file()
    )


def execute(args: argparse.Namespace) -> None:
    args.audit_file.write_text("", encoding="utf-8")
    server = declaration(args.fixture, args.audit_file)
    client = AcpClient(args.acp, args.root)
    try:
        parent = create_session(client, 2, args.workspace, server)
        invoke_parent(client, parent)
        missing = create_session(client, 3, args.workspace, server)
        tampered = create_session(client, 4, args.workspace, server)
    finally:
        client.close()
    assert effect_count(args.audit_file) == 1

    parent_head = session_head(args.cli, parent)
    branched = json.loads(branch(args.cli, parent, parent_head).stdout)
    child = branched["session_id"]
    assert branched["parent_session_id"] == parent
    assert branched["fork_sequence"] == parent_head
    parent_root = args.root / "sessions" / parent
    child_root = args.root / "sessions" / child
    parent_binding = json.loads((parent_root / "style.lock").read_text())["binding"]["mcp"]
    child_binding = json.loads((child_root / "style.lock").read_text())["binding"]["mcp"]
    assert child_binding == parent_binding
    parent_bootstrap = json.loads((parent_root / "mcp-bootstrap.enc.json").read_text())
    child_bootstrap = json.loads((child_root / "mcp-bootstrap.enc.json").read_text())
    assert parent_bootstrap["session_id"] == parent
    assert child_bootstrap["session_id"] == child
    assert child_bootstrap["declaration_hash"] == parent_bootstrap["declaration_hash"]
    assert child_bootstrap["binding_hash"] == parent_bootstrap["binding_hash"]
    assert child_bootstrap["nonce_base64"] != parent_bootstrap["nonce_base64"]
    assert child_bootstrap["ciphertext_base64"] != parent_bootstrap["ciphertext_base64"]

    loader = AcpClient(args.acp, args.root)
    try:
        exact = loader.request(
            20,
            "session/load",
            {"sessionId": child, "cwd": str(args.workspace), "mcpServers": [server]},
        )
        assert exact.get("result") == {}, exact
        substituted = declaration(args.fixture, args.audit_file, "substituted")
        rejected = loader.request(
            21,
            "session/load",
            {
                "sessionId": child,
                "cwd": str(args.workspace),
                "mcpServers": [substituted],
            },
        )
        assert "error" in rejected, rejected
    finally:
        loader.close()

    run_cli(
        args.cli,
        "run",
        "invoke inherited MCP fixture",
        "--session",
        child,
        "--option",
        'mock_scenario="mcp_fixture_call"',
        "--json",
    )
    assert effect_count(args.audit_file) == 2

    missing_root = args.root / "sessions" / missing
    (missing_root / "mcp-bootstrap.enc.json").unlink()
    missing_branch = branch(args.cli, missing, session_head(args.cli, missing), check=False)
    assert missing_branch.returncode != 0, "branch accepted missing source bootstrap"

    tampered_root = args.root / "sessions" / tampered
    tampered_path = tampered_root / "mcp-bootstrap.enc.json"
    tampered_bootstrap = json.loads(tampered_path.read_text())
    ciphertext = tampered_bootstrap["ciphertext_base64"]
    tampered_bootstrap["ciphertext_base64"] = (
        ("A" if ciphertext[0] != "A" else "B") + ciphertext[1:]
    )
    tampered_path.write_text(json.dumps(tampered_bootstrap, separators=(",", ":")))
    tampered_branch = branch(args.cli, tampered, session_head(args.cli, tampered), check=False)
    assert tampered_branch.returncode != 0, "branch accepted unauthenticated bootstrap"
    assert effect_count(args.audit_file) == 2
    assert_no_secret(args.root)
    child_head = session_head(args.cli, child)
    args.state_file.write_text(
        json.dumps({"child": child, "head": child_head, "effects": 2}),
        encoding="utf-8",
    )


def replay(args: argparse.Namespace) -> None:
    state = json.loads(args.state_file.read_text())
    child = state["child"]
    expected = state["effects"]
    run_cli(
        args.cli,
        "session",
        "replay",
        child,
        "--at",
        str(state["head"]),
        "--json",
    )
    assert effect_count(args.audit_file) == expected
    run_cli(
        args.cli,
        "run",
        "invoke inherited MCP fixture after restart",
        "--session",
        child,
        "--option",
        'mock_scenario="mcp_fixture_call"',
        "--json",
    )
    assert effect_count(args.audit_file) == expected + 1
    replay_head = session_head(args.cli, child)
    run_cli(
        args.cli,
        "session",
        "replay",
        child,
        "--at",
        str(replay_head),
        "--json",
    )
    assert effect_count(args.audit_file) == expected + 1
    assert_no_secret(args.root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("execute", "replay"), required=True)
    parser.add_argument("--acp", required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--workspace", type=pathlib.Path, required=True)
    parser.add_argument("--audit-file", type=pathlib.Path, required=True)
    parser.add_argument("--state-file", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.phase == "execute":
        execute(args)
    else:
        replay(args)


if __name__ == "__main__":
    main()
