#!/usr/bin/env python3
"""Cross-platform artifact-handoff finalize crash/recovery process proof."""

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
    result = subprocess.run(
        [str(cli), *arguments],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def start_runtime(runtime: Path, cli: Path, root: Path, env: dict[str, str]):
    stream = open(root / "runtime.stderr.log", "a", encoding="utf-8")
    process = subprocess.Popen(
        [str(runtime), "serve"],
        cwd=root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=stream,
    )
    for _ in range(150):
        ready = subprocess.run(
            [str(cli), "doctor", "--json"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ready.returncode == 0:
            return process, stream
        if process.poll() is not None:
            raise RuntimeError("runtime exited before becoming ready")
        time.sleep(0.1)
    raise RuntimeError("runtime did not become ready")


def stop(process, stream) -> None:
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    if stream is not None:
        stream.close()


def events(path: Path) -> list[dict]:
    for attempt in range(50):
        try:
            return [
                json.loads(line)["event"]
                for line in path.read_text(encoding="utf-8").splitlines()
                if line
            ]
        except (PermissionError, json.JSONDecodeError):
            if attempt == 49:
                raise
            time.sleep(0.01)
    raise RuntimeError("journal retry exhausted")


def count(values: list[dict], event_type: str) -> int:
    return sum(
        value["metadata"]["event_type"] == event_type for value in values
    )


def file_hash(path: Path) -> str:
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
    root = Path(tempfile.mkdtemp(prefix="agentmod-artifact-finalize-e2e-"))
    workspace = root / "workspace"
    styles = root / "styles" / "user"
    workspace.mkdir(parents=True)
    styles.mkdir(parents=True)
    source = (
        repository
        / "tests"
        / "fixtures"
        / "styles"
        / "persistent-none-sliding.toml"
    ).read_text(encoding="utf-8")
    source = source.replace(
        'id = "e2e-persistent-sliding"',
        'id = "e2e-persistent-artifact-finalize"',
    ).replace('strategy = "sliding_window"', 'strategy = "artifact_handoff"')
    style_path = styles / "artifact-finalize.toml"
    style_path.write_text(source, encoding="utf-8")

    environment = os.environ.copy()
    environment["AGENTMOD_RUNTIME_ENDPOINT"] = (
        rf"\\.\pipe\agentmod-artifact-finalize-{uuid.uuid4().hex}"
        if arguments.platform == "windows"
        else str(root / "runtime.sock")
    )
    environment["AGENTMOD_RUNTIME_AUTH_TOKEN"] = (
        "0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    environment["AGENTMOD_HARNESS_PROGRAM"] = str(arguments.harness.resolve())
    environment["AGENTMOD_ARTIFACT_FINALIZE_POST_PERSIST_DELAY_MS"] = "10000"
    daemon = None
    daemon_stream = None
    turn = None
    succeeded = False
    try:
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            root,
            environment,
        )
        validation = invoke(
            arguments.cli,
            environment,
            "style",
            "validate",
            str(style_path),
            "--json",
        )
        assert validation["valid"] is True
        created = invoke(
            arguments.cli,
            environment,
            "session",
            "create",
            "--workspace",
            str(workspace),
            "--style",
            "e2e-persistent-artifact-finalize",
            "--json",
        )
        session_id = created["session_id"]
        run_id = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d50"
        journal = root / "sessions" / session_id / "events.jsonl"
        command = [
            str(arguments.cli.resolve()),
            "run",
            "persist the complete provider context",
            "--session",
            session_id,
            "--cancellation-id",
            run_id,
            "--option",
            'mock_scenario="streaming_text"',
            "--option",
            'mock_text="artifact-output"',
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
        objects = (
            root
            / "sessions"
            / session_id
            / "artifacts"
            / "context"
            / "objects"
        )
        for _ in range(300):
            if turn.poll() is not None:
                raise RuntimeError(
                    "turn exited before finalize cut: " + turn.stderr.read()
                )
            stored = list(objects.glob("*/*/content")) if objects.exists() else []
            if journal.exists():
                current = events(journal)
                if (
                    len(stored) == 1
                    and count(current, "artifact.persistence_dispatched") == 1
                    and count(current, "artifact.persistence_completed") == 0
                ):
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("artifact finalize crash cut was not reached")

        cut_events = events(journal)
        assert count(cut_events, "model.request_started") == 0
        assert count(cut_events, "context.projection_replaced") == 0
        stop(daemon, daemon_stream)
        daemon = None
        daemon_stream = None
        if turn.poll() is None:
            turn.kill()
            turn.wait(timeout=10)
        turn = None

        environment["AGENTMOD_ARTIFACT_FINALIZE_POST_PERSIST_DELAY_MS"] = "0"
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            root,
            environment,
        )
        recovered = invoke(
            arguments.cli,
            environment,
            *command[1:],
        )
        visible = "".join(
            event.get("text", "")
            for event in recovered["events"]
            if event.get("event") == "text"
        )
        assert visible == "alpha beta artifact-output"
        recovered_events = events(journal)
        for event_type in (
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.response_completed",
            "artifact.persistence_proposed",
            "artifact.persistence_approved",
            "artifact.persistence_dispatched",
            "artifact.persistence_completed",
            "context.projection_replacement_approved",
        ):
            assert count(recovered_events, event_type) == 1, event_type
        replacements = [
            event
            for event in recovered_events
            if event["metadata"]["event_type"] == "context.projection_replaced"
            and event["payload"]["payload"]["provenance"]["method"]
            == "artifact_handoff"
        ]
        assert len(replacements) == 1
        assert len(list(objects.glob("*/*/content"))) == 1

        inspection = invoke(
            arguments.cli,
            environment,
            "session",
            "inspect",
            session_id,
            "--json",
        )
        persistence = inspection["state"]["artifact_persistences"]
        assert len(persistence) == 1
        record = next(iter(persistence.values()))
        assert record["state"] == "completed"
        assert record["identity"]["context_phase"]["phase"] == "compaction"
        content_hash = record["identity"]["content_hash"]
        content = (
            objects / content_hash[:2] / content_hash / "content"
        )
        assert content.exists()
        document = json.loads(content.read_text(encoding="utf-8"))
        assert document["schema"] == "agentmod.context-artifact.v1"
        projection = inspection["state"]["conversation"]["provider_projection"]
        artifact_entries = [
            entry for entry in projection if entry["kind"] == "artifact_reference"
        ]
        assert len(artifact_entries) == 1
        assert (
            artifact_entries[0]["content"]["artifact_reference"]
            == record["artifact_reference"]
        )
        assert (
            replacements[0]["metadata"]["artifacts"][0]["content_hash"]
            == content_hash
        )
        before_count = len(recovered_events)
        before_journal_hash = file_hash(journal)
        before_content_hash = file_hash(content)
        before_projection = projection

        stop(daemon, daemon_stream)
        daemon = None
        daemon_stream = None
        daemon, daemon_stream = start_runtime(
            arguments.runtime.resolve(),
            arguments.cli.resolve(),
            root,
            environment,
        )
        replay = invoke(
            arguments.cli,
            environment,
            "session",
            "replay",
            session_id,
            "--json",
        )
        assert replay["state"]["conversation"]["provider_projection"] == before_projection
        assert len(events(journal)) == before_count
        assert file_hash(journal) == before_journal_hash
        assert file_hash(content) == before_content_hash
        succeeded = True
        print(
            "runtime artifact-handoff finalize "
            f"{arguments.platform} crash/restart/replay E2E passed"
        )
    finally:
        if turn is not None and turn.poll() is None:
            turn.kill()
            turn.wait()
        stop(daemon, daemon_stream)
        if succeeded:
            shutil.rmtree(root)
        else:
            print(f"retained failed E2E root: {root}")


if __name__ == "__main__":
    main()
