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
run_root = pathlib.Path(tempfile.mkdtemp(prefix="agentmod-plugin-automatic-memory-e2e-"))
workspace = run_root / "workspace"
style_root = run_root / "styles" / "user"
workspace.mkdir(parents=True)
style_root.mkdir(parents=True)
marker = run_root / "memory-invocations.log"
plugin_executable = run_root / "plugin-fixture-worker"
offline_plugin_executable = run_root / "plugin-fixture-worker.offline"
shutil.copy2(fixture_worker, plugin_executable)
plugin_executable.chmod(0o755)

manifest_template = (
    repo / "tests" / "fixtures" / "plugins" / "automatic-memory.toml"
).read_text()
manifest = manifest_template.replace(
    "__PLUGIN_PROGRAM__", str(plugin_executable)
).replace(
    "__PLUGIN_ARGS__",
    json.dumps(["--memory-marker", str(marker)], separators=(",", ":")),
)
manifest_path = run_root / "automatic-memory-plugin.toml"
manifest_path.write_text(manifest)

providers = {
    "success": (
        "fixture.memory.success",
        "2e4f6dc7fa1e3ad211c32148bacb5c208c99ed726e021866ac31699742240266",
    ),
    "invalid": (
        "fixture.memory.invalid",
        "572f03ae7b5fde6771c40521785c287a722da29a2814a0c228e320eaa94aca66",
    ),
    "timeout": (
        "fixture.memory.timeout",
        "6025dd4db5a87e2d72055147bc5f9022ab1ffd8850d034be47d98384d08ad338",
    ),
}
style_template = (
    repo / "tests" / "fixtures" / "styles" / "plugin-automatic-memory.toml"
).read_text()
for name, (provider_id, declaration_hash) in providers.items():
    (style_root / f"{name}.toml").write_text(
        style_template.replace("__STYLE_ID__", f"e2e-plugin-memory-{name}")
        .replace("__PROVIDER_ID__", provider_id)
        .replace("__DECLARATION_HASH__", declaration_hash)
    )

env = os.environ.copy()
env.update(
    {
        "AGENTMOD_RUNTIME_ENDPOINT": str(run_root / "runtime.sock"),
        "AGENTMOD_RUNTIME_AUTH_TOKEN": "0123456789abcdef" * 3,
        "AGENTMOD_HARNESS_PROGRAM": str(harness),
        "AGENTMOD_PLUGIN_HOST_PROGRAM": str(plugin_host),
        "AGENTMOD_PLUGIN_MANIFESTS": str(manifest_path),
        "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS": os.pathsep.join(
            [str(run_root), str(target)]
        ),
        "AGENTMOD_MEMORY_WRITE_PERMISSION_MODE": "ask",
        "AGENTMOD_FIXTURE_WORKER_PROGRAM": str(fixture_worker),
        "AGENTMOD_FIXTURE_MEMORY_MARKER": str(marker),
    }
)
daemon = None
daemon_log = None
runtime_starts = 0
succeeded = False

def invoke(arguments, *, check=True, stderr=None):
    result = subprocess.run(
        [str(cli), *arguments],
        cwd=repo,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=stderr if stderr is not None else subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"CLI failed ({result.returncode}): {' '.join(arguments)}\n{result.stderr}"
        )
    return result

def stop_runtime():
    global daemon, daemon_log
    if daemon is not None and daemon.poll() is None:
        daemon.kill()
        daemon.wait(timeout=5)
    daemon = None
    if daemon_log is not None:
        daemon_log.close()
        daemon_log = None

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

def events(session_id):
    path = run_root / "sessions" / session_id / "events.jsonl"
    for _ in range(80):
        try:
            return [json.loads(line) for line in path.read_text().splitlines()]
        except (FileNotFoundError, PermissionError):
            time.sleep(0.025)
    raise RuntimeError(f"journal unavailable: {path}")

def event_count(session_events, event_type):
    return sum(
        item["event"]["metadata"]["event_type"] == event_type
        for item in session_events
    )

def marker_lines():
    return marker.read_text().splitlines() if marker.exists() else []

def invocation_count(invocation_id):
    return sum(line.startswith(invocation_id + "|") for line in marker_lines())

def create_session(provider):
    result = invoke(
        [
            "session", "create", "--workspace", str(workspace),
            "--style", f"e2e-plugin-memory-{provider}", "--json",
        ]
    )
    return json.loads(result.stdout)["session_id"]

def begin_approval(session_id, run_id, prompt):
    result = invoke(
        [
            "run", prompt, "--session", session_id, "--cancellation-id", run_id,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="plugin-memory-output"', "--json",
        ]
    )
    continuation = json.loads(result.stdout).get("awaiting_continuation")
    journal = events(session_id)
    proposals = [
        item for item in journal
        if item["event"]["metadata"]["event_type"] == "memory.write_proposed"
    ]
    if (
        not continuation
        or len(proposals) != 1
        or event_count(journal, "memory.write_approved") != 0
        or event_count(journal, "memory.write_dispatched") != 0
    ):
        raise RuntimeError("plugin write did not suspend at exact approval")
    invocation = proposals[0]["event"]["payload"]["payload"]["identity"]["plugin"][
        "invocation_id"
    ]
    return continuation, invocation

def set_plugin_offline(offline):
    if offline and plugin_executable.exists():
        plugin_executable.rename(offline_plugin_executable)
    elif not offline and offline_plugin_executable.exists():
        offline_plugin_executable.rename(plugin_executable)

def resolve_approval(session_id, continuation, expect_success):
    result = invoke(
        ["approval", "resolve", session_id, continuation, "approve", "--json"],
        check=False,
    )
    if expect_success != (result.returncode == 0):
        raise RuntimeError(
            f"approval recovery expectation failed: {result.returncode} {result.stderr}"
        )

def run_receipt_cut(mode, restart_pending):
    session_id = create_session("success")
    continuation, invocation = begin_approval(
        session_id, str(uuid.uuid4()), f"remember plugin automatic memory {mode}"
    )
    if restart_pending:
        stop_runtime()
        start_runtime()
    env["AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS"] = "10000"
    stop_runtime()
    start_runtime()
    approval = subprocess.Popen(
        [str(cli), "approval", "resolve", session_id, continuation, "approve", "--json"],
        cwd=repo,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    receipt = None
    receipt_dir = (
        run_root / "sessions" / session_id / "artifacts" / "plugin-invocation-receipts"
    )
    for _ in range(300):
        receipts = list(receipt_dir.glob("*.json")) if receipt_dir.exists() else []
        journal = events(session_id)
        if (
            invocation_count(invocation) == 1
            and len(receipts) == 1
            and event_count(journal, "memory.write_dispatched") == 1
            and event_count(journal, "memory.write_completed") == 0
        ):
            receipt = receipts[0]
            break
        if approval.poll() is not None:
            raise RuntimeError(
                "approval exited before crash cut: " + approval.stderr.read()
            )
        time.sleep(0.05)
    if receipt is None:
        raise RuntimeError("durable plugin receipt crash cut was not reached")
    stop_runtime()
    try:
        approval.wait(timeout=5)
    except subprocess.TimeoutExpired:
        approval.kill()
        approval.wait(timeout=5)
    if mode == "missing":
        receipt.unlink()
    elif mode == "corrupt":
        content = bytearray(receipt.read_bytes())
        content[len(content) // 2] ^= 1
        receipt.write_bytes(content)
    set_plugin_offline(True)
    env["AGENTMOD_PLUGIN_RECEIPT_POST_PERSIST_DELAY_MS"] = "0"
    start_runtime()
    resolve_approval(session_id, continuation, mode == "complete")
    journal = events(session_id)
    if (
        invocation_count(invocation) != 1
        or event_count(journal, "memory.write_proposed") != 1
        or event_count(journal, "memory.write_approved") != 1
        or event_count(journal, "memory.write_dispatched") != 1
    ):
        raise RuntimeError("receipt recovery duplicated lifecycle or dispatch")
    completed = event_count(journal, "memory.write_completed")
    ambiguous = event_count(journal, "memory.write_ambiguous")
    if (mode == "complete" and (completed, ambiguous) != (1, 0)) or (
        mode != "complete" and (completed, ambiguous) != (0, 1)
    ):
        raise RuntimeError(f"{mode} receipt recovery classification is invalid")
    stop_runtime()
    set_plugin_offline(False)
    start_runtime()

def run_ambiguous_provider(provider, handler):
    session_id = create_session(provider)
    run_id = str(uuid.uuid4())
    prompt = f"exercise plugin automatic memory {provider}"
    continuation, invocation = begin_approval(session_id, run_id, prompt)
    resolve_approval(session_id, continuation, False)
    journal = events(session_id)
    exact = [line for line in marker_lines() if line.startswith(invocation + "|")]
    if (
        exact != [f"{invocation}|{handler}"]
        or event_count(journal, "memory.write_ambiguous") != 1
        or event_count(journal, "memory.write_completed") != 0
    ):
        raise RuntimeError(f"{provider} ambiguity evidence is incomplete")
    repeated = invoke(
        [
            "run", prompt, "--session", session_id, "--cancellation-id", run_id,
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="plugin-memory-output"', "--json",
        ],
        check=False,
    )
    if (
        repeated.returncode == 0
        or invocation_count(invocation) != 1
        or event_count(events(session_id), "memory.write_ambiguous") != 1
    ):
        raise RuntimeError(f"{provider} ambiguous effect was redispatched")

try:
    start_runtime()
    for provider in providers:
        result = invoke(
            ["style", "validate", str(style_root / f"{provider}.toml"), "--json"]
        )
        if not json.loads(result.stdout)["valid"]:
            raise RuntimeError(f"{provider} style did not validate")
    run_receipt_cut("complete", True)
    run_receipt_cut("missing", False)
    run_receipt_cut("corrupt", False)
    run_ambiguous_provider("invalid", "wrong_identity_memory_write")
    run_ambiguous_provider("timeout", "timeout_memory_write")

    unavailable_session = create_session("success")
    stop_runtime()
    set_plugin_offline(True)
    start_runtime()
    before = len(marker_lines())
    result = invoke(
        [
            "run", "plugin unavailable after session creation",
            "--session", unavailable_session,
            "--cancellation-id", str(uuid.uuid4()),
            "--option", 'mock_scenario="streaming_text"',
            "--option", 'mock_text="plugin-memory-output"', "--json",
        ],
        check=False,
    )
    unavailable_events = events(unavailable_session)
    if (
        result.returncode == 0
        or len(marker_lines()) != before
        or any(
            event_count(unavailable_events, event_type) != 0
            for event_type in (
                "memory.write_proposed",
                "memory.write_dispatched",
                "memory.write_completed",
                "memory.write_ambiguous",
            )
        )
    ):
        raise RuntimeError("plugin unavailability was not fail-closed before worker entry")
    succeeded = True
    print("runtime/plugin-host automatic-memory Linux E2E passed")
finally:
    stop_runtime()
    set_plugin_offline(False)
    if succeeded:
        shutil.rmtree(run_root)
    else:
        print(f"retained failed E2E root: {run_root}", file=sys.stderr)
PY
