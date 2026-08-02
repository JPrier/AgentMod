#!/usr/bin/env python3
"""Cross-platform process proof for session-completion automatic memory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid


def invoke(cli: Path, env: dict[str, str], *arguments: str) -> dict:
    completed = subprocess.run(
        [str(cli), *arguments],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def start_runtime(runtime: Path, cli: Path, root: Path, env: dict[str, str]):
    stderr = open(root / "runtime.stderr.log", "a", encoding="utf-8")
    daemon = subprocess.Popen(
        [str(runtime), "serve"],
        cwd=root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=stderr,
    )
    for _ in range(150):
        ready = subprocess.run(
            [str(cli), "doctor", "--json"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ready.returncode == 0:
            return daemon, stderr
        if daemon.poll() is not None:
            raise RuntimeError("runtime exited before becoming ready")
        time.sleep(0.1)
    daemon.kill()
    daemon.wait()
    stderr.close()
    raise RuntimeError("runtime did not become ready")


def stop(process, stream) -> None:
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    if stream is not None:
        stream.close()


def read_events(journal: Path) -> list[dict]:
    with journal.open(encoding="utf-8") as source:
        return [json.loads(line)["event"] for line in source if line.strip()]


def count(events: list[dict], event_type: str) -> int:
    return sum(
        event["metadata"]["event_type"] == event_type for event in events
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--platform", choices=("windows", "linux"), required=True)
    arguments = parser.parse_args()

    repository = arguments.repository.resolve()
    run_root = Path(
        tempfile.mkdtemp(prefix="agentmod-session-completion-memory-e2e-")
    )
    workspace = run_root / "workspace"
    style_root = run_root / "styles" / "user"
    workspace.mkdir(parents=True)
    style_root.mkdir(parents=True)
    shutil.copyfile(
        repository / "tests" / "fixtures" / "styles"
        / "session-completion-memory.toml",
        style_root / "session-completion-memory.toml",
    )

    environment = os.environ.copy()
    if arguments.platform == "windows":
        environment["AGENTMOD_RUNTIME_ENDPOINT"] = (
            rf"\\.\pipe\agentmod-session-completion-memory-{uuid.uuid4().hex}"
        )
    else:
        environment["AGENTMOD_RUNTIME_ENDPOINT"] = str(run_root / "runtime.sock")
    environment["AGENTMOD_RUNTIME_AUTH_TOKEN"] = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    environment["AGENTMOD_HARNESS_PROGRAM"] = str(arguments.harness.resolve())
    environment["AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS"] = "10000"

    daemon = None
    daemon_stream = None
    turn = None
    succeeded = False
    try:
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            run_root,
            environment,
        )
        validation = invoke(
            arguments.cli,
            environment,
            "style",
            "validate",
            str(style_root / "session-completion-memory.toml"),
            "--json",
        )
        if validation["valid"] is not True:
            raise RuntimeError(
                "session-completion style invalid: "
                + json.dumps(validation, sort_keys=True)
            )
        created = invoke(
            arguments.cli,
            environment,
            "session",
            "create",
            "--workspace",
            str(workspace),
            "--style",
            "e2e-session-completion-memory",
            "--json",
        )
        session_id = created["session_id"]
        run_id = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d4f"
        journal = run_root / "sessions" / session_id / "events.jsonl"
        memory_file = run_root / "memory" / "file.jsonl"
        command = [
            str(arguments.cli.resolve()),
            "run",
            "finish and retain exact terminal evidence",
            "--session",
            session_id,
            "--cancellation-id",
            run_id,
            "--json",
        ]
        turn = subprocess.Popen(
            command,
            cwd=repository,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(250):
            if turn.poll() is not None:
                raise RuntimeError(
                    "turn exited before crash cut: " + turn.stderr.read()
                )
            if journal.exists() and memory_file.exists():
                events = read_events(journal)
                if (
                    count(events, "memory.write_dispatched") == 1
                    and len(memory_file.read_text(encoding="utf-8").splitlines())
                    == 1
                ):
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("session-completion memory crash cut was not reached")

        before_cut = read_events(journal)
        assert count(before_cut, "session.lifecycle_changed") == 1
        assert count(before_cut, "memory.write_completed") == 0
        stop(daemon, daemon_stream)
        daemon = None
        daemon_stream = None
        if turn.poll() is None:
            turn.kill()
            turn.wait(timeout=10)
        turn = None

        environment["AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS"] = "0"
        environment["AGENTMOD_HARNESS_PROGRAM"] = str(
            run_root / "harness-must-not-run"
        )
        environment["AGENTMOD_FIXTURE_HARNESS_PROGRAM"] = str(
            run_root / "fixture-harness-must-not-run"
        )
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            run_root,
            environment,
        )
        recovered = invoke(
            arguments.cli,
            environment,
            "run",
            "finish and retain exact terminal evidence",
            "--session",
            session_id,
            "--cancellation-id",
            run_id,
            "--json",
        )
        assert recovered["events"] == []

        events = read_events(journal)
        for event_type in (
            "artifact.persistence_proposed",
            "artifact.persistence_approved",
            "artifact.persistence_dispatched",
            "artifact.persistence_completed",
            "session.lifecycle_changed",
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed",
        ):
            assert count(events, event_type) == 1, event_type
        assert count(events, "model.request_started") == 0
        memory_events = [
            event
            for event in events
            if event["metadata"]["event_type"].startswith("memory.write_")
        ]
        assert [event["metadata"]["event_type"] for event in memory_events] == [
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed",
        ]
        payloads = [event["payload"]["payload"] for event in memory_events]
        identity = payloads[0]["identity"]
        assert all(payload["identity"] == identity for payload in payloads)
        assert identity["policy"] == "session_completion"
        assert identity["run_id"] == run_id
        assert identity["scope"] == f"session:{session_id}"
        assert identity["session_completion"] is not None
        completion = next(
            event
            for event in events
            if event["metadata"]["sequence"]
            == identity["session_completion"]["sequence"]
        )
        assert completion["metadata"]["event_type"] == "session.lifecycle_changed"
        assert (
            completion["integrity_checksum"]
            == identity["session_completion"]["event_checksum"]
        )
        action_digest = payloads[1]["action_digest"]
        assert payloads[2]["action_digest"] == action_digest
        assert payloads[3]["action_digest"] == action_digest
        assert payloads[3]["retained"] is True

        records = [
            json.loads(line)
            for line in memory_file.read_text(encoding="utf-8").splitlines()
        ]
        assert len(records) == 1
        retained = records[0]
        assert retained["id"] == payloads[3]["reference"]
        assert retained["source"] == (
            f"runtime.automatic_memory:session_completion:{run_id}"
        )
        typed = json.loads(retained["content"])
        assert typed["schema"] == "agentmod.session-completion-memory.v1"
        assert typed["successful_completion"] == identity["session_completion"]
        assert typed["artifact_references"]
        assert any(
            output["node_id"] == "save-result"
            and output.get("artifact_reference")
            for output in typed["node_outputs"]
        )
        information_flow = typed["conversation_summary"]["information_flow"]
        assert information_flow["schema"] == "agentmod.information-flow.v1"
        assert [
            (entry["entry_kind"], entry["classification"])
            for entry in information_flow["entries"]
        ] == [("user_message", "private")]

        inspection = invoke(
            arguments.cli,
            environment,
            "session",
            "inspect",
            session_id,
            "--json",
        )
        automatic = inspection["state"]["automatic_memory_writes"]
        assert len(automatic) == 1
        record = next(iter(automatic.values()))
        assert record["state"] == "completed"
        assert record["identity"] == identity
        journal_count = len(events)
        journal_hash = digest(journal)
        memory_hash = digest(memory_file)
        last_sequence = inspection["state"]["last_sequence"]

        stop(daemon, daemon_stream)
        daemon = None
        daemon_stream = None
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            run_root,
            environment,
        )
        replayed = invoke(
            arguments.cli,
            environment,
            "session",
            "replay",
            session_id,
            "--json",
        )
        assert replayed["state"]["last_sequence"] == last_sequence
        assert replayed["state"]["automatic_memory_writes"] == automatic
        assert len(read_events(journal)) == journal_count
        assert digest(journal) == journal_hash
        assert digest(memory_file) == memory_hash

        for index, hostile in enumerate(
            (
                "Authorization: Bearer 0123456789abcdef0123456789abcdef",
                "../workspace/private.json",
                "https://example.invalid/private-context",
                r"\\.\pipe\private-runtime",
                "HANDLE:0000000000000042",
            )
        ):
            hostile_created = invoke(
                arguments.cli,
                environment,
                "session",
                "create",
                "--workspace",
                str(workspace),
                "--style",
                "e2e-session-completion-memory",
                "--json",
            )
            hostile_session = hostile_created["session_id"]
            invoke(
                arguments.cli,
                environment,
                "run",
                hostile,
                "--session",
                hostile_session,
                "--cancellation-id",
                f"018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d{index:02d}",
                "--json",
            )
            hostile_events = read_events(
                run_root / "sessions" / hostile_session / "events.jsonl"
            )
            assert count(hostile_events, "session.lifecycle_changed") == 1
            assert all(
                not event["metadata"]["event_type"].startswith("memory.write_")
                for event in hostile_events
            )
        assert digest(memory_file) == memory_hash
        succeeded = True
        print(
            "runtime session-completion automatic-memory "
            f"{arguments.platform} crash/restart/replay E2E passed"
        )
    finally:
        if turn is not None and turn.poll() is None:
            turn.kill()
            turn.wait()
        stop(daemon, daemon_stream)
        if succeeded:
            shutil.rmtree(run_root)
        else:
            print(f"retained failed E2E root: {run_root}")


if __name__ == "__main__":
    main()
