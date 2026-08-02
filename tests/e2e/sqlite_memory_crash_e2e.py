#!/usr/bin/env python3
"""Cross-platform SQLite post-commit automatic-memory crash-cut proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
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


def event_count(events: list[dict], event_type: str) -> int:
    return sum(
        event["metadata"]["event_type"] == event_type for event in events
    )


def journal_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sqlite_snapshot(path: Path) -> list[tuple[str, str, str, str, int]]:
    connection = sqlite3.connect(path, timeout=0.25)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise AssertionError(f"SQLite integrity check failed: {integrity!r}")
        rows = connection.execute(
            "SELECT id, scope, source, content, created_at_millis "
            "FROM memory_fts ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    return [
        (str(row[0]), str(row[1]), str(row[2]), str(row[3]), int(row[4]))
        for row in rows
    ]


def reached_post_commit_cut(database: Path) -> bool:
    if not database.exists():
        return False
    try:
        return len(sqlite_snapshot(database)) == 1
    except (sqlite3.DatabaseError, sqlite3.OperationalError, AssertionError):
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--platform", choices=("windows", "linux"), required=True)
    arguments = parser.parse_args()

    repository = arguments.repository.resolve()
    run_root = Path(tempfile.mkdtemp(prefix="agentmod-sqlite-memory-crash-e2e-"))
    workspace = run_root / "workspace"
    style_root = run_root / "styles" / "user"
    workspace.mkdir(parents=True)
    style_root.mkdir(parents=True)
    style = (
        repository
        / "tests"
        / "fixtures"
        / "styles"
        / "persistent-file-none.toml"
    ).read_text(encoding="utf-8")
    style = style.replace(
        'id = "e2e-persistent-file"', 'id = "e2e-sqlite-memory-crash"'
    )
    style = style.replace(
        'provider = "file"', 'provider = "sqlite-fts"'
    )
    style = style.replace(
        'write_policy = "explicit_only"', 'write_policy = "turn_completion"'
    )
    style_path = style_root / "sqlite-memory-crash.toml"
    style_path.write_text(style, encoding="utf-8")

    environment = os.environ.copy()
    if arguments.platform == "windows":
        environment["AGENTMOD_RUNTIME_ENDPOINT"] = (
            rf"\\.\pipe\agentmod-sqlite-memory-crash-{uuid.uuid4().hex}"
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
            str(style_path),
            "--json",
        )
        if validation["valid"] is not True:
            raise RuntimeError(
                "SQLite automatic-memory style invalid: "
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
            "e2e-sqlite-memory-crash",
            "--json",
        )
        session_id = created["session_id"]
        run_id = "018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d6e"
        journal = run_root / "sessions" / session_id / "events.jsonl"
        database = run_root / "memory" / "sqlite-fts.sqlite3"
        prompt = "remember the SQLite post-commit boundary"
        command = [
            str(arguments.cli.resolve()),
            "run",
            prompt,
            "--session",
            session_id,
            "--cancellation-id",
            run_id,
            "--option",
            'mock_scenario="streaming_text"',
            "--option",
            'mock_text="sqlite-memory-output"',
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
                    "turn exited before SQLite crash cut: " + turn.stderr.read()
                )
            if journal.exists() and reached_post_commit_cut(database):
                events = read_events(journal)
                if event_count(events, "memory.write_dispatched") == 1:
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("SQLite post-commit memory crash cut was not reached")

        before_cut = read_events(journal)
        assert event_count(before_cut, "memory.write_dispatched") == 1
        assert event_count(before_cut, "memory.write_completed") == 0
        assert len(sqlite_snapshot(database)) == 1
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
            prompt,
            "--session",
            session_id,
            "--cancellation-id",
            run_id,
            "--option",
            'mock_scenario="streaming_text"',
            "--option",
            'mock_text="sqlite-memory-output"',
            "--json",
        )
        assert recovered["events"] == []

        events = read_events(journal)
        for event_type in (
            "memory.write_proposed",
            "memory.write_approved",
            "memory.write_dispatched",
            "memory.write_completed",
            "model.request_proposed",
            "model.request_approved",
            "model.request_started",
            "model.response_completed",
        ):
            assert event_count(events, event_type) == 1, event_type
        assert event_count(events, "model.request_failed") == 0
        assert event_count(events, "model.request_cancelled") == 0

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
        assert identity["provider"] == "sqlite-fts"
        assert identity["policy"] == "turn_completion"
        assert identity["run_id"] == run_id
        assert identity["scope"] == f"session:{session_id}"
        action_digest = payloads[1]["action_digest"]
        assert payloads[2]["action_digest"] == action_digest
        assert payloads[3]["action_digest"] == action_digest
        assert payloads[3]["retained"] is True

        rows = sqlite_snapshot(database)
        assert len(rows) == 1
        retained_id, scope, source, content, created_at_millis = rows[0]
        assert retained_id == payloads[3]["reference"]
        assert scope == identity["scope"]
        assert source == identity["source"]
        assert source == f"runtime.automatic_memory:turn_completion:{run_id}"
        assert content == payloads[0]["content"]
        assert created_at_millis == identity["created_at_millis"]
        assert len(content.encode("utf-8")) == identity["byte_size"]
        typed = json.loads(content)
        assert typed["schema"] == "agentmod.context-summary.v1"
        assert prompt in content
        assert "sqlite-memory-output" in content

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
        assert record["retained"] is True
        assert record["identity"] == identity
        journal_count = len(events)
        stable_journal_digest = journal_digest(journal)
        stable_rows = rows
        last_sequence = inspection["state"]["last_sequence"]

        stop(daemon, daemon_stream)
        daemon = None
        daemon_stream = None
        assert sqlite_snapshot(database) == stable_rows
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
        assert journal_digest(journal) == stable_journal_digest
        assert sqlite_snapshot(database) == stable_rows

        succeeded = True
        print(
            "runtime SQLite automatic-memory "
            f"{arguments.platform} post-commit crash/restart/replay E2E passed"
        )
    finally:
        if turn is not None and turn.poll() is None:
            turn.kill()
            turn.wait(timeout=10)
        stop(daemon, daemon_stream)
        if succeeded:
            shutil.rmtree(run_root)
        else:
            print(f"retained failed E2E root: {run_root}")


if __name__ == "__main__":
    main()
