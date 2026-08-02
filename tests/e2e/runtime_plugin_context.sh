#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"

python3 - "$repository" <<'PY'
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

repo = pathlib.Path(sys.argv[1])
subprocess.run(
    [
        "cargo", "build", "--locked",
        "-p", "agentmod-runtime",
        "-p", "agentmod-scheduler",
        "-p", "agentmod-harness",
        "-p", "agentmod-cli",
        "-p", "agentmod-plugin-host",
        "-p", "agentmod-plugin-fixture-worker",
    ],
    cwd=repo,
    check=True,
)
configured_target = os.environ.get("CARGO_TARGET_DIR")
target_root = pathlib.Path(configured_target) if configured_target else repo / "target"
if not target_root.is_absolute():
    target_root = repo / target_root
target = target_root / "debug"
runtime = target / "agentmod-runtime"
harness = target / "agentmod-harness"
cli = target / "agentmod"
plugin_host = target / "agentmod-plugin-host"
fixture_worker = target / "agentmod-plugin-fixture-worker"
run_root = pathlib.Path(tempfile.mkdtemp(prefix="agentmod-plugin-context-e2e-"))
workspace = run_root / "workspace"
style_root = run_root / "styles" / "user"
workspace.mkdir(parents=True)
style_root.mkdir(parents=True)
marker = run_root / "context-invocations.log"
plugin_executable = run_root / "plugin-fixture-worker"
offline_plugin_executable = run_root / "plugin-fixture-worker.offline"
shutil.copy2(fixture_worker, plugin_executable)
plugin_executable.chmod(0o755)

def manifest(fixture, destination):
    document = (repo / fixture).read_text()
    document = document.replace("__PLUGIN_PROGRAM__", str(plugin_executable))
    document = document.replace(
        "__PLUGIN_ARGS__",
        json.dumps(
            ["--memory-marker", str(marker)],
            separators=(",", ":"),
        ),
    )
    destination.write_text(document)

memory_manifest = run_root / "plugin-context-memory.toml"
compactor_manifest = run_root / "plugin-context-compactor.toml"
manifest(
    "tests/fixtures/plugins/plugin-context.toml",
    memory_manifest,
)
manifest(
    "tests/fixtures/plugins/plugin-compaction.toml",
    compactor_manifest,
)

provider_hashes = {
    "fixture.context-memory.success":
        "2b72f4cd63fe66f74672098d981ba2e38d1d2b1ba3e51f9df178966d7885fc40",
    "fixture.context-memory.invalid":
        "a4028f30754d127c743f064045462ffce0ca7daae1d590834d1979a284010cc6",
    "fixture.context-memory.timeout":
        "2e59bf93479578db0e76f547b37b02a6a7dc6d44c99c9a3157ad8993def50f96",
}
compactor_hashes = {
    "fixture.context-compactor.success":
        "e5c9320e9582f147a37f6f1f7fd0726c83e4f1c996bf8c52d8a32ad037723938",
    "fixture.context-compactor.invalid":
        "6135a6b4689d5cf3a7d23a406da04ba9731e2ef2c8873ffe97351352c0e9d1a2",
    "fixture.context-compactor.timeout":
        "4dc2c55dd5d476170166aabc04684f9e09c27a8974c0b2eee1abd9b34817af33",
}
configuration_reference = (
    "6e46dd10defc9b56c29a6ec56b508c21f54c08192194e4df25bf36f0c9c3c279"
)
memory_template = (
    repo / "tests/fixtures/styles/plugin-automatic-memory.toml"
).read_text()
memory_template = (
    memory_template
    .replace("fixture.automatic-memory", "fixture.plugin-context")
    .replace('write_policy = "turn_completion"', 'write_policy = "never"')
    .replace('injection_location = "none"', 'injection_location = "before_current_input"')
    .replace("max_query_bytes = 0", "max_query_bytes = 16384")
    .replace("1" * 64, configuration_reference)
)
combined_template = (
    repo / "tests/fixtures/styles/plugin-context.toml"
).read_text()
iteration_template = (
    repo / "tests/fixtures/styles/plugin-context-iteration.toml"
).read_text()

def memory_style(name, provider_id, timing):
    document = (
        memory_template
        .replace("__STYLE_ID__", f"e2e-plugin-context-{name}")
        .replace("__PROVIDER_ID__", provider_id)
        .replace("__DECLARATION_HASH__", provider_hashes[provider_id])
        .replace('retrieval_timing = "never"', f'retrieval_timing = "{timing}"')
    )
    (style_root / f"{name}.toml").write_text(document)

def combined_style(name, provider_id, timing, compactor_id):
    document = (
        combined_template
        .replace("__STYLE_ID__", f"e2e-plugin-context-{name}")
        .replace("__PROVIDER_ID__", provider_id)
        .replace("__PROVIDER_HASH__", provider_hashes[provider_id])
        .replace("__RETRIEVAL_TIMING__", timing)
        .replace("__COMPACTION_STRATEGY__", "plugin")
        .replace("__COMPACTOR_ID__", compactor_id)
        .replace("__COMPACTOR_HASH__", compactor_hashes[compactor_id])
    )
    (style_root / f"{name}.toml").write_text(document)

memory_style("turn-start", "fixture.context-memory.success", "turn_start")
memory_style("before-model", "fixture.context-memory.success", "before_model_request")
combined_style(
    "context-node",
    "fixture.context-memory.success",
    "context_node",
    "fixture.context-compactor.success",
)
(style_root / "iteration-start.toml").write_text(
    iteration_template
    .replace("__STYLE_ID__", "e2e-plugin-context-iteration-start")
    .replace("__PROVIDER_ID__", "fixture.context-memory.success")
    .replace(
        "__PROVIDER_HASH__",
        provider_hashes["fixture.context-memory.success"],
    )
)
memory_style("invalid-memory", "fixture.context-memory.invalid", "before_model_request")
memory_style("timeout-memory", "fixture.context-memory.timeout", "before_model_request")
combined_style(
    "invalid-compactor",
    "fixture.context-memory.success",
    "context_node",
    "fixture.context-compactor.invalid",
)
combined_style(
    "timeout-compactor",
    "fixture.context-memory.success",
    "context_node",
    "fixture.context-compactor.timeout",
)

env = os.environ.copy()
env.update(
    {
        "AGENTMOD_RUNTIME_ENDPOINT": str(run_root / "runtime.sock"),
        "AGENTMOD_RUNTIME_AUTH_TOKEN": "0123456789abcdef" * 3,
        "AGENTMOD_HARNESS_PROGRAM": str(harness),
        "AGENTMOD_PLUGIN_HOST_PROGRAM": str(plugin_host),
        "AGENTMOD_PLUGIN_MANIFESTS": os.pathsep.join(
            [str(memory_manifest), str(compactor_manifest)]
        ),
        "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS": str(run_root),
    }
)
daemon = None
daemon_log = None
runtime_starts = 0
succeeded = False

def invoke(arguments, *, check=True):
    result = subprocess.run(
        [str(cli), *arguments],
        cwd=repo,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"CLI failed ({result.returncode}): {' '.join(arguments)}\n{result.stderr}"
        )
    return result

def start_runtime():
    global daemon, daemon_log, runtime_starts
    runtime_starts += 1
    daemon_log = (run_root / f"runtime-{runtime_starts}.stderr.log").open("wb")
    daemon = subprocess.Popen(
        [str(runtime), "serve"],
        cwd=run_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=daemon_log,
    )
    for _ in range(200):
        if invoke(["doctor", "--json"], check=False).returncode == 0:
            return
        if daemon.poll() is not None:
            break
        time.sleep(0.05)
    raise RuntimeError("runtime did not become ready")

def stop_runtime():
    global daemon, daemon_log
    if daemon is not None and daemon.poll() is None:
        daemon.kill()
        daemon.wait(timeout=5)
    daemon = None
    if daemon_log is not None:
        daemon_log.close()
        daemon_log = None

def events(session_id):
    path = run_root / "sessions" / session_id / "events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]

def count(items, event_type):
    return sum(
        item["event"]["metadata"]["event_type"] == event_type
        for item in items
    )

def marker_lines():
    return marker.read_text().splitlines() if marker.exists() else []

def invocation_count(invocation_id):
    return sum(line.startswith(invocation_id + "|") for line in marker_lines())

def create_session(name):
    result = invoke(
        [
            "session", "create", "--workspace", str(workspace),
            "--style", f"e2e-plugin-context-{name}", "--json",
        ]
    )
    return json.loads(result.stdout)["session_id"]

def run_turn(session_id, run_id, success):
    result = invoke(
        [
            "run", "plugin context process proof",
            "--session", session_id,
            "--cancellation-id", run_id,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="plugin-context-output"',
            "--json",
        ],
        check=False,
    )
    if success != (result.returncode == 0):
        raise RuntimeError(
            f"turn expectation failed: {result.returncode} {result.stderr}"
        )

def exact_operation(items, kind, implementation, declaration_hash, boundary):
    records = [
        item for item in items
        if item["event"]["metadata"]["event_type"] ==
            "plugin.context_operation_proposed"
        and item["event"]["payload"]["payload"]["identity"]["kind"] == kind
    ]
    if len(records) != 1:
        raise RuntimeError(f"expected one {kind} proposal, got {len(records)}")
    identity = records[0]["event"]["payload"]["payload"]["identity"]
    if (
        identity["implementation_id"] != implementation
        or identity["declaration_hash"] != declaration_hash
        or identity["configuration_reference"] != configuration_reference
        or identity["phase"]["boundary"]["boundary"] != boundary
    ):
        raise RuntimeError(f"exact {kind} identity mismatch: {identity}")
    return identity["invocation_id"]

try:
    start_runtime()
    for style in style_root.glob("*.toml"):
        validated = json.loads(invoke(["style", "validate", str(style), "--json"]).stdout)
        if not validated["valid"]:
            raise RuntimeError(f"invalid style: {style}")

    for name, boundary, combined in [
        ("turn-start", "turn_start", False),
        ("before-model", "before_model_request", False),
        ("context-node", "context_node", True),
    ]:
        session_id = create_session(name)
        run_turn(session_id, str(uuid.uuid4()), True)
        journal = events(session_id)
        memory_invocation = exact_operation(
            journal,
            "memory_retrieve",
            "fixture.context-memory.success",
            provider_hashes["fixture.context-memory.success"],
            boundary,
        )
        expected = 2 if combined else 1
        if (
            invocation_count(memory_invocation) != 1
            or count(journal, "plugin.context_operation_completed") != expected
            or count(journal, "plugin.context_operation_applied") != expected
        ):
            raise RuntimeError(f"incomplete {name} lifecycle")
        if combined:
            compaction_invocation = exact_operation(
                journal,
                "compaction",
                "fixture.context-compactor.success",
                compactor_hashes["fixture.context-compactor.success"],
                "before_model_request",
            )
            if invocation_count(compaction_invocation) != 1:
                raise RuntimeError("compaction was not invoked exactly once")

    for name, handler in [
        ("invalid-memory", "invalid_memory_retrieve"),
        ("timeout-memory", "timeout_memory_retrieve"),
        ("invalid-compactor", "invalid_compaction"),
        ("timeout-compactor", "timeout_compaction"),
    ]:
        session_id = create_session(name)
        run_id = str(uuid.uuid4())
        run_turn(session_id, run_id, False)
        journal = events(session_id)
        terminal = (
            count(journal, "plugin.context_operation_failed")
            + count(journal, "plugin.context_operation_ambiguous")
        )
        if not any(line.endswith("|" + handler) for line in marker_lines()) or terminal != 1:
            raise RuntimeError(f"{name} did not fail closed exactly once")
        run_turn(session_id, run_id, False)
        after = events(session_id)
        if (
            count(after, "plugin.context_operation_failed")
            + count(after, "plugin.context_operation_ambiguous")
        ) != 1:
            raise RuntimeError(f"{name} duplicated terminal evidence")

    iteration_session = create_session("iteration-start")
    run_turn(iteration_session, str(uuid.uuid4()), True)
    iteration_journal = events(iteration_session)
    operations = [
        item["event"]["payload"]["payload"]
        for item in iteration_journal
        if item["event"]["metadata"]["event_type"]
        == "plugin.context_operation_proposed"
        and item["event"]["payload"]["payload"]["identity"]["kind"]
        == "memory_retrieve"
    ]
    if (
        len(operations) != 2
        or count(iteration_journal, "plugin.context_operation_completed") != 2
        or count(iteration_journal, "plugin.context_operation_applied") != 2
        or count(iteration_journal, "model.request_started") != 3
    ):
        raise RuntimeError("iteration-start plugin retrieval lifecycle is incomplete")
    invocation_ids = set()
    source_heads = set()
    projection_hashes = set()
    loop_iterations = set()
    projection_counts = set()
    for payload in operations:
        identity = payload["identity"]
        runtime_values = payload["readable_state"]["readable_state"][
            "recorded_runtime_values"
        ]
        if (
            identity["implementation_id"] != "fixture.context-memory.success"
            or identity["declaration_hash"]
            != provider_hashes["fixture.context-memory.success"]
            or identity["configuration_reference"] != configuration_reference
            or identity["phase"]["boundary"]["boundary"] != "iteration_start"
            or invocation_count(identity["invocation_id"]) != 1
            or identity["invocation_id"] in invocation_ids
            or runtime_values["source_head"]
            != identity["phase"]["boundary"]["source_head"]
            or len(runtime_values["projection_hash"]) != 64
        ):
            raise RuntimeError(f"invalid iteration-start identity: {payload}")
        invocation_ids.add(identity["invocation_id"])
        source_heads.add(runtime_values["source_head"])
        projection_hashes.add(runtime_values["projection_hash"])
        loop_iterations.add(runtime_values["loop_iteration"])
        projection_counts.add(runtime_values["projection_entry_count"])
    if (
        len(source_heads) != 2
        or len(projection_hashes) != 2
        or loop_iterations != {1, 2}
        or projection_counts != {1, 2}
    ):
        raise RuntimeError(
            "iteration-start identity omitted prior state or loop counters"
        )
    iteration_path = (
        run_root / "sessions" / iteration_session / "events.jsonl"
    )
    before_iteration = iteration_path.read_bytes()
    invoke(["session", "replay", iteration_session, "--json"])
    if iteration_path.read_bytes() != before_iteration:
        raise RuntimeError("iteration-start replay mutated canonical history")

    print("runtime/plugin-host plugin-context Linux E2E passed")
    succeeded = True
finally:
    stop_runtime()
    if succeeded:
        shutil.rmtree(run_root)
    else:
        print(f"retained failed E2E root: {run_root}", file=sys.stderr)
PY
