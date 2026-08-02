#!/usr/bin/env python3
"""Cross-platform process proof for exact child-session MCP inheritance."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import uuid


SECRET = "child-mcp-secret-never-persist-plaintext"
TRUE_STYLE = "persistent-chat@99.0.0"
DEFAULT_STYLE = "persistent-chat@99.1.0"
WORKER_STYLE = "child-mcp-worker@1.0.0"
BOOTSTRAP_FILE = "mcp-bootstrap.enc.json"


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
        for _ in range(200):
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


def declaration(
    fixture: str, audit: pathlib.Path, secret: str = SECRET
) -> dict[str, object]:
    return {
        "name": "fixture",
        "command": fixture,
        "args": [],
        "env": [
            {"name": "FIXTURE_TOKEN", "value": secret},
            {"name": "FIXTURE_AUDIT_PATH", "value": str(audit)},
        ],
    }


def parent_manifest(version: str, inherit_mcp: bool) -> str:
    inheritance = "inherit_mcp = true\n" if inherit_mcp else ""
    return f'''schema_version = 1
kind = "custom"
required_capabilities = ["agents", "approval", "model", "tools"]
allowed_tool_groups = ["mcp"]
allowed_providers = ["deterministic-mock"]
allowed_plugins = ["runtime.security"]

[identity]
id = "persistent-chat"
version = "{version}"
runtime_api = "^1.0"

[harness]
id = "native"
required_capabilities = [
  "cancellation",
  "streaming",
  "structured_context_replacement",
  "token_usage",
  "tool_calls",
]

[graph]
kind = "inline"
source = \'\'\'
format_version = 1
entry = "spawn-worker"

[budget]
max_steps = 24
max_tokens = 16000
max_cost_micros = 1000000
max_duration_ms = 120000

[declarations]
capabilities = ["agents", "approval", "model", "tools"]
providers = ["deterministic-mock"]

[[variables]]
name = "worker"
type = {{ kind = "child_id" }}
scope = "run"
producer = "spawn-worker"
consumers = ["wait-worker"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "spawn-worker"
kind = "spawn_child_agent"
write_variables = ["worker"]
configuration = {{ type = "spawn_child_agent", task_input = {{ kind = "static", value = "invoke the inherited MCP fixture" }}, task_id_prefix = "mcp-worker", child_style = "{WORKER_STYLE}", tool_groups = ["mcp"], maximum_children = 1, maximum_depth = 2, token_budget = 8000, context_budget_tokens = 4000, cost_budget_micros = 500000, workspace = {{ mode = "temporary_copy" }}, artifact_references = [], security_classification = "internal", approval_required = true }}

[[nodes]]
id = "wait-worker"
kind = "wait_for_agents"
read_variables = ["worker"]
configuration = {{ type = "wait_for_agents", children = {{ kind = "variable", variable = "worker" }}, maximum_children = 1, minimum_successes = 1, timeout_ms = 60000, cancellation = "cascade" }}

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "spawn-worker"
to = "wait-worker"

[[edges]]
from = "wait-worker"
to = "done"
\'\'\'

[[interceptors]]
id = "runtime-style-policy"
owner = "runtime.security"
event = "action.proposed"
stage = 10
priority = 100
before = []
after = []
supported_decisions = [
  "continue",
  "replace",
  "reject",
  "require_approval",
  "cancel",
]
required_capabilities = ["approval"]

[memory]
provider = "none"
scopes = []
retrieval_timing = "never"
max_items = 0
max_injected_bytes = 0
write_policy = "never"
injection_location = "none"

[memory.query]
source = "current_input"
include_active_artifacts = false
include_style_context = false
max_query_bytes = 16384

[compaction]
strategy = "none"
reserved_context_tokens = 0
max_provider_projection_tokens = 0
preserve_unresolved_tasks = true
preserve_active_processes = true
preservation_requirements = [
  "system_instructions",
  "current_input",
  "pending_control_state",
  "artifact_references",
  "memory_provenance",
  "active_graph_state",
  "tool_call_correlation",
]

[approvals]
default = "allow"
groups = {{ child_agent_creation = "ask", child_agent_cancellation = "ask" }}

[budgets]
max_iterations = 2
max_steps = 24
max_tokens = 16000
max_cost_micros = 1000000
max_duration_ms = 120000

[child_agents]
max_children = 1
max_concurrent = 1
max_depth = 2
per_child_token_budget = 8000
child_style = "{WORKER_STYLE}"
workspace_mode = "temporary_copy"
inherit_provider = true
inherit_model = true
{inheritance}context_budget_tokens = 4000
per_child_cost_budget_micros = 500000
tool_groups = ["mcp"]
memory_access = "none"
join_behavior = "all"
cancellation_behavior = "cascade"
reviewer_max_attempts = 1

[retry]
max_attempts = 2
initial_backoff_ms = 10
max_backoff_ms = 100
retryable_failures = ["provider.unavailable"]

[termination]
allowed_outcomes = ["complete_session", "fail"]
on_hard_limit = "fail"
require_explicit_terminal_node = true

[selection]
requires_explicit_selection = true
model_may_select = false
'''


WORKER_MANIFEST = '''schema_version = 1
kind = "custom"
required_capabilities = ["approval", "context", "model", "tools"]
allowed_tool_groups = ["mcp"]
allowed_providers = ["deterministic-mock"]
allowed_plugins = ["runtime.security"]

[identity]
id = "child-mcp-worker"
version = "1.0.0"
runtime_api = "^1.0"

[harness]
id = "native"
required_capabilities = [
  "cancellation",
  "streaming",
  "structured_context_replacement",
  "token_usage",
  "tool_calls",
]

[graph]
kind = "inline"
source = \'\'\'
format_version = 1
entry = "prepare-context"

[budget]
max_steps = 24
max_tokens = 8000
max_cost_micros = 500000
max_duration_ms = 60000

[declarations]
capabilities = ["context", "model", "tools"]
tools = ["mcp.server.list", "mcp.capabilities", "mcp.invoke"]
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "turn_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[nodes]]
id = "prepare-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }

[[nodes]]
id = "respond"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 1
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result", provider_options = { mock_scenario = "mcp_fixture_call" } }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["turn_result"]
read_scopes = ["workspace"]
[nodes.configuration]
type = "provider_tool_batch_execution"
request_reference_variable = "model_result"
disposition_variable = "model_disposition"
maximum_calls = 1
allowed_tools = ["mcp.server.list", "mcp.capabilities", "mcp.invoke"]

[[nodes]]
id = "done"
kind = "complete_turn"
read_variables = ["turn_result"]
configuration = { type = "complete_turn", result_reference_variable = "turn_result", cleanup = "preserve_projection" }

[[edges]]
from = "prepare-context"
to = "respond"

[[edges]]
from = "respond"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "done"
\'\'\'

[[interceptors]]
id = "runtime-style-policy"
owner = "runtime.security"
event = "action.proposed"
stage = 10
priority = 100
before = []
after = []
supported_decisions = ["continue", "replace", "reject", "require_approval", "cancel"]
required_capabilities = ["approval"]

[memory]
provider = "none"
scopes = []
retrieval_timing = "never"
max_items = 0
max_injected_bytes = 0
write_policy = "never"
injection_location = "none"

[memory.query]
source = "current_input"
include_active_artifacts = false
include_style_context = false
max_query_bytes = 16384

[compaction]
strategy = "none"
reserved_context_tokens = 0
max_provider_projection_tokens = 0
preserve_unresolved_tasks = true
preserve_active_processes = true
preservation_requirements = ["system_instructions", "current_input", "pending_control_state", "artifact_references", "memory_provenance", "active_graph_state", "tool_call_correlation"]

[approvals]
default = "allow"
groups = {}

[budgets]
max_iterations = 4
max_steps = 24
max_tokens = 8000
max_cost_micros = 500000
max_duration_ms = 60000

[child_agents]
max_children = 0
max_concurrent = 0
max_depth = 0
per_child_token_budget = 0

[retry]
max_attempts = 2
initial_backoff_ms = 10
max_backoff_ms = 100
retryable_failures = ["provider.unavailable"]

[termination]
allowed_outcomes = ["complete_turn", "fail"]
on_hard_limit = "fail"
require_explicit_terminal_node = true

[selection]
requires_explicit_selection = true
model_may_select = false
'''


def write_text(path: pathlib.Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8", newline="\n")


def prepare(args: argparse.Namespace) -> None:
    styles = args.root / "styles" / "user"
    write_text(styles / "child-mcp-worker.toml", WORKER_MANIFEST)
    # ACP creates the unversioned `persistent-chat` selector. Only the explicit
    # inheritance version exists for the first three parents, so exact selection
    # is deterministic even though ACP intentionally owns the style name.
    write_text(styles / "child-mcp-parent-true.toml", parent_manifest("99.0.0", True))
    args.workspace.mkdir(parents=True, exist_ok=True)


def add_default_style(root: pathlib.Path) -> None:
    # Catalog discovery occurs on every session creation. Adding the higher
    # version now makes ACP's next unversioned selection the omitted/default-
    # false policy while retaining 99.0.0 for restart compatibility checks.
    write_text(
        root / "styles" / "user" / "child-mcp-parent-default.toml",
        parent_manifest("99.1.0", False),
    )


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
    result = response.get("result")
    assert isinstance(result, dict), response
    session_id = result.get("sessionId")
    assert isinstance(session_id, str) and session_id, response
    return session_id


def inspect(cli: str, session_id: str) -> dict[str, object]:
    return cli_json(cli, "session", "inspect", session_id, "--json")


def state_of(inspection: dict[str, object]) -> dict[str, object]:
    state = inspection.get("state")
    assert isinstance(state, dict), inspection
    return state


def workspace_of(state: dict[str, object]) -> str:
    workspace = state.get("workspace")
    assert isinstance(workspace, str) and workspace.strip(), state
    assert pathlib.Path(workspace).is_absolute(), workspace
    return workspace


def session_head(cli: str, session_id: str) -> int:
    head = inspect(cli, session_id).get("head_sequence")
    assert isinstance(head, int) and head > 0
    return head


def style_binding(root: pathlib.Path, session_id: str) -> dict[str, object]:
    value = json.loads(
        (root / "sessions" / session_id / "style.lock").read_text(encoding="utf-8")
    )
    binding = value.get("binding")
    assert isinstance(binding, dict), value
    return binding


def assert_parent_policy(
    cli: str, session_id: str, version: str, expected: bool | None
) -> None:
    binding = state_of(inspect(cli, session_id)).get("style_binding")
    assert isinstance(binding, dict)
    assert binding.get("id") == "persistent-chat"
    assert binding.get("version") == version
    compiled_json = binding.get("compiled_style_json")
    assert isinstance(compiled_json, str)
    compiled = json.loads(compiled_json)
    assert compiled.get("kind") == "custom"
    child_agents = compiled.get("child_agents")
    assert isinstance(child_agents, dict)
    assert child_agents.get("inherit_mcp") is expected


def child_sessions(cli: str, parent_id: str) -> list[dict[str, object]]:
    listed = cli_json(cli, "session", "list", "--limit", "128", "--json")
    summaries = listed.get("sessions")
    assert isinstance(summaries, list), listed
    children: list[dict[str, object]] = []
    for summary in summaries:
        assert isinstance(summary, dict)
        session_id = summary.get("id")
        if not isinstance(session_id, str) or session_id == parent_id:
            continue
        candidate = inspect(cli, session_id)
        state = state_of(candidate)
        origin = state.get("child_origin")
        if isinstance(origin, dict) and origin.get("parent_session_id") == parent_id:
            children.append(candidate)
    return children


def spawn_child(cli: str, parent_id: str) -> dict[str, object]:
    result = cli_json(
        cli,
        "run",
        "spawn the MCP inheritance worker",
        "--session",
        parent_id,
        "--provider",
        "deterministic-mock",
        "--model",
        "mock-model",
        "--cancellation-id",
        str(uuid.uuid4()),
        "--json",
    )
    continuation = result.get("awaiting_continuation")
    assert isinstance(continuation, str) and continuation, result
    cli_json(
        cli,
        "approval",
        "resolve",
        parent_id,
        continuation,
        "approve",
        "--json",
    )
    children = child_sessions(cli, parent_id)
    assert len(children) == 1, children
    return children[0]


def tamper_ciphertext(path: pathlib.Path) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    ciphertext = value.get("ciphertext_base64")
    assert isinstance(ciphertext, str) and ciphertext
    value["ciphertext_base64"] = (
        ("A" if ciphertext[0] != "A" else "B") + ciphertext[1:]
    )
    path.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")


def assert_tampered_parent_rejected(cli: str, root: pathlib.Path, parent_id: str) -> None:
    parent_root = root / "sessions" / parent_id
    tamper_ciphertext(parent_root / BOOTSTRAP_FILE)
    started = run_cli(
        cli,
        "run",
        "reject the tampered source bootstrap",
        "--session",
        parent_id,
        "--provider",
        "deterministic-mock",
        "--model",
        "mock-model",
        "--cancellation-id",
        str(uuid.uuid4()),
        "--json",
        check=False,
    )
    if started.returncode != 0:
        assert not child_sessions(cli, parent_id)
        assert not any(root.joinpath("sessions").glob(".creating-*"))
        return
    result = json.loads(started.stdout)
    assert isinstance(result, dict)
    continuation = result.get("awaiting_continuation")
    assert isinstance(continuation, str) and continuation, result
    rejected = run_cli(
        cli,
        "approval",
        "resolve",
        parent_id,
        continuation,
        "approve",
        "--json",
        check=False,
    )
    children = child_sessions(cli, parent_id)
    state = state_of(inspect(cli, parent_id))
    assert rejected.returncode != 0 or state.get("lifecycle") == "failed", rejected
    assert not children, children
    assert not any(root.joinpath("sessions").glob(".creating-*"))


def mcp_binding(binding: dict[str, object]) -> dict[str, object]:
    mcp = binding.get("mcp")
    assert isinstance(mcp, dict), binding
    return mcp


def assert_exact_inheritance(
    root: pathlib.Path, parent_id: str, child_id: str
) -> None:
    parent_mcp = mcp_binding(style_binding(root, parent_id))
    child_mcp = mcp_binding(style_binding(root, child_id))
    assert child_mcp == parent_mcp
    parent_envelope = json.loads(
        (root / "sessions" / parent_id / BOOTSTRAP_FILE).read_text(encoding="utf-8")
    )
    child_envelope = json.loads(
        (root / "sessions" / child_id / BOOTSTRAP_FILE).read_text(encoding="utf-8")
    )
    assert parent_envelope["session_id"] == parent_id
    assert child_envelope["session_id"] == child_id
    assert child_envelope["declaration_hash"] == parent_envelope["declaration_hash"]
    assert child_envelope["binding_hash"] == parent_envelope["binding_hash"]
    assert child_envelope["nonce_base64"] != parent_envelope["nonce_base64"]
    assert child_envelope["ciphertext_base64"] != parent_envelope["ciphertext_base64"]


def assert_default_false(root: pathlib.Path, child_id: str) -> None:
    child_root = root / "sessions" / child_id
    child_mcp = mcp_binding(style_binding(root, child_id))
    assert child_mcp.get("configuration_reference") is None
    assert child_mcp.get("servers", []) == []
    assert not (child_root / BOOTSTRAP_FILE).exists()


def effect_count(audit: pathlib.Path) -> int:
    if not audit.exists():
        return 0
    events = [json.loads(line) for line in audit.read_text().splitlines() if line]
    assert all(event == {"event": "tools/call"} for event in events)
    return len(events)


def event_types(root: pathlib.Path, session_id: str) -> list[str]:
    journal = root / "sessions" / session_id / "events.jsonl"
    events = [
        json.loads(line)["event"]
        for line in journal.read_text(encoding="utf-8").splitlines()
    ]
    return [event["metadata"]["event_type"] for event in events]


def assert_no_secret(root: pathlib.Path) -> None:
    secret = SECRET.encode()
    assert not any(
        secret in path.read_bytes() for path in root.rglob("*") if path.is_file()
    )


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(args: argparse.Namespace) -> None:
    args.audit_file.write_text("", encoding="utf-8")
    server = declaration(args.fixture, args.audit_file)
    for path in [
        args.root / "styles" / "user" / "child-mcp-worker.toml",
        args.root / "styles" / "user" / "child-mcp-parent-true.toml",
    ]:
        validation = cli_json(args.cli, "style", "validate", str(path), "--json")
        assert validation.get("valid") is True, validation

    creator = AcpClient(args.acp, args.root)
    try:
        inherited_parent = create_session(creator, 10, args.workspace, server)
        aad_parent = create_session(creator, 11, args.workspace, server)
        tampered_parent = create_session(creator, 12, args.workspace, server)
        for parent_id in [inherited_parent, aad_parent, tampered_parent]:
            assert_parent_policy(args.cli, parent_id, "99.0.0", True)

        add_default_style(args.root)
        default_path = (
            args.root / "styles" / "user" / "child-mcp-parent-default.toml"
        )
        validation = cli_json(
            args.cli, "style", "validate", str(default_path), "--json"
        )
        assert validation.get("valid") is True, validation
        default_parent = create_session(creator, 13, args.workspace, server)
        assert_parent_policy(args.cli, default_parent, "99.1.0", None)
    finally:
        creator.close()

    inherited_child_state = state_of(spawn_child(args.cli, inherited_parent))
    aad_child_state = state_of(spawn_child(args.cli, aad_parent))
    default_child_state = state_of(spawn_child(args.cli, default_parent))
    inherited_child = inherited_child_state.get("id")
    aad_child = aad_child_state.get("id")
    default_child = default_child_state.get("id")
    assert isinstance(inherited_child, str)
    assert isinstance(aad_child, str)
    assert isinstance(default_child, str)
    inherited_child_workspace = workspace_of(inherited_child_state)
    aad_child_workspace = workspace_of(aad_child_state)
    workspace_of(default_child_state)

    assert_exact_inheritance(args.root, inherited_parent, inherited_child)
    assert_exact_inheritance(args.root, aad_parent, aad_child)
    assert_default_false(args.root, default_child)
    assert_tampered_parent_rejected(args.cli, args.root, tampered_parent)
    assert effect_count(args.audit_file) == 0

    aad_child_root = args.root / "sessions" / aad_child
    aad_child_bootstrap = aad_child_root / BOOTSTRAP_FILE
    child_envelope = aad_child_bootstrap.read_bytes()
    parent_envelope = (
        args.root / "sessions" / aad_parent / BOOTSTRAP_FILE
    ).read_bytes()
    aad_child_bootstrap.write_bytes(parent_envelope)
    verifier = AcpClient(args.acp, args.root)
    verifier_failed_closed = False
    try:
        loaded = verifier.request(
            20,
            "session/load",
            {
                "sessionId": aad_child,
                "cwd": aad_child_workspace,
                "mcpServers": [server],
            },
        )
        assert loaded.get("result") == {}, loaded
        try:
            unexpected = verifier.request(
                21,
                "session/prompt",
                {
                    "sessionId": aad_child,
                    "prompt": [
                        {"type": "text", "text": "invoke the inherited MCP fixture"}
                    ],
                },
            )
        except RuntimeError as error:
            verifier_failed_closed = True
            assert "ACP runtime transport is unavailable" in str(error), error
            assert verifier.process.wait(timeout=5) != 0, (
                "tampered child bootstrap did not close ACP with a failure"
            )
        else:
            raise AssertionError(
                f"tampered child bootstrap unexpectedly returned an ACP response: {unexpected}"
            )
        assert effect_count(args.audit_file) == 0
    finally:
        if verifier_failed_closed:
            for stream in (
                verifier.process.stdin,
                verifier.process.stdout,
                verifier.process.stderr,
            ):
                if stream is not None:
                    stream.close()
        else:
            verifier.close()
        aad_child_bootstrap.write_bytes(child_envelope)

    runner = AcpClient(args.acp, args.root)
    try:
        substituted = declaration(args.fixture, args.audit_file, "substituted")
        rejected = runner.request(
            30,
            "session/load",
            {
                "sessionId": inherited_child,
                "cwd": inherited_child_workspace,
                "mcpServers": [substituted],
            },
        )
        assert "error" in rejected, rejected
        exact = runner.request(
            31,
            "session/load",
            {
                "sessionId": inherited_child,
                "cwd": inherited_child_workspace,
                "mcpServers": [server],
            },
        )
        assert exact.get("result") == {}, exact
        invoked = runner.request(
            32,
            "session/prompt",
            {
                "sessionId": inherited_child,
                "prompt": [{"type": "text", "text": "invoke the inherited MCP fixture"}],
            },
        )
        result = invoked.get("result")
        assert isinstance(result, dict), invoked
        assert result.get("stopReason") == "end_turn", invoked
    finally:
        runner.close()
    assert effect_count(args.audit_file) == 1
    assert_no_secret(args.root)

    child_root = args.root / "sessions" / inherited_child
    journal = child_root / "events.jsonl"
    bootstrap = child_root / BOOTSTRAP_FILE
    args.state_file.write_text(
        json.dumps(
            {
                "child": inherited_child,
                "child_workspace": inherited_child_workspace,
                "parent": inherited_parent,
                "default_parent": default_parent,
                "default_child": default_child,
                "head": session_head(args.cli, inherited_child),
                "effects": 1,
                "journal_sha256": sha256(journal),
                "bootstrap_sha256": sha256(bootstrap),
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def replay(args: argparse.Namespace) -> None:
    state = json.loads(args.state_file.read_text(encoding="utf-8"))
    child = state["child"]
    assert isinstance(child, str)
    child_workspace = state["child_workspace"]
    assert isinstance(child_workspace, str) and child_workspace.strip(), state
    assert pathlib.Path(child_workspace).is_absolute(), child_workspace
    assert effect_count(args.audit_file) == state["effects"]
    child_root = args.root / "sessions" / child
    journal = child_root / "events.jsonl"
    bootstrap = child_root / BOOTSTRAP_FILE
    assert sha256(journal) == state["journal_sha256"]
    assert sha256(bootstrap) == state["bootstrap_sha256"]
    run_cli(
        args.cli,
        "session",
        "replay",
        child,
        "--at",
        str(state["head"]),
        "--json",
    )
    assert effect_count(args.audit_file) == state["effects"]
    assert sha256(journal) == state["journal_sha256"]
    assert sha256(bootstrap) == state["bootstrap_sha256"]

    server = declaration(args.fixture, args.audit_file)
    loader = AcpClient(args.acp, args.root)
    try:
        loaded = loader.request(
            40,
            "session/load",
            {
                "sessionId": child,
                "cwd": child_workspace,
                "mcpServers": [server],
            },
        )
        assert loaded.get("result") == {}, loaded
    finally:
        loader.close()
    assert effect_count(args.audit_file) == state["effects"]
    assert_parent_policy(args.cli, state["parent"], "99.0.0", True)
    assert_parent_policy(args.cli, state["default_parent"], "99.1.0", None)
    assert_default_false(args.root, state["default_child"])
    assert_no_secret(args.root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("prepare", "execute", "replay"), required=True)
    parser.add_argument("--acp")
    parser.add_argument("--cli")
    parser.add_argument("--fixture")
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--workspace", type=pathlib.Path, required=True)
    parser.add_argument("--audit-file", type=pathlib.Path)
    parser.add_argument("--state-file", type=pathlib.Path)
    args = parser.parse_args()
    if args.phase == "prepare":
        prepare(args)
        return
    for name in ("acp", "cli", "fixture", "audit_file", "state_file"):
        if getattr(args, name) is None:
            parser.error(f"--{name.replace('_', '-')} is required for {args.phase}")
    if args.phase == "execute":
        execute(args)
    else:
        replay(args)


if __name__ == "__main__":
    main()
