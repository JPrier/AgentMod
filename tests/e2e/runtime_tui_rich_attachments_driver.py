#!/usr/bin/env python3
"""Validate the canonical effect of a real TUI rich-attachment turn."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import pathlib
import subprocess


PROMPT = "inspect the TUI image and blob"


def run(program: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [program, *arguments], capture_output=True, text=True, check=False, timeout=30
    )
    if result.returncode != 0:
        raise AssertionError(
            f"process failed ({result.returncode}): {program} {' '.join(arguments)}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return result


def journal_bytes(root: pathlib.Path, session_id: str) -> bytes:
    return (root / "sessions" / session_id / "events.jsonl").read_bytes()


def events(root: pathlib.Path, session_id: str) -> list[dict[str, object]]:
    return [
        json.loads(line)["event"]
        for line in journal_bytes(root, session_id).decode().splitlines()
        if line
    ]


def entry_text(entry: dict[str, object]) -> str | None:
    text = entry.get("text")
    if isinstance(text, str):
        return text
    content = entry.get("content")
    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"]
        if isinstance(content.get("content"), str):
            return content["content"]
    return None


def exact_envelope(image: pathlib.Path, blob: pathlib.Path) -> dict[str, object]:
    return {
        "agentmod_acp_content_version": 1,
        "blocks": [
            {"type": "text", "text": PROMPT},
            {
                "type": "image",
                "data": base64.b64encode(image.read_bytes()).decode(),
                "mime_type": "image/png",
                "uri": image.resolve().as_uri(),
            },
            {
                "type": "resource",
                "resource": {
                    "kind": "blob",
                    "data": base64.b64encode(blob.read_bytes()).decode(),
                    "uri": blob.resolve().as_uri(),
                    "mime_type": "application/octet-stream",
                },
            },
        ],
    }


def execute(args: argparse.Namespace) -> None:
    rich_entries: list[dict[str, object]] = []
    user_entries: list[dict[str, object]] = []
    all_events = events(args.root, args.session)
    for event in all_events:
        if event["metadata"]["event_type"] != "conversation.entry_committed":  # type: ignore[index]
            continue
        entry = event["payload"]["payload"]["entry"]  # type: ignore[index]
        if not isinstance(entry, dict) or entry.get("kind") != "user_message":
            continue
        user_entries.append(entry)
        text = entry_text(entry)
        if text and "agentmod_acp_content_version" in text:
            rich_entries.append(json.loads(text))
    expected = [exact_envelope(args.image, args.blob)]
    if rich_entries != expected:
        raise AssertionError(
            "rich envelope mismatch\nactual="
            + json.dumps(rich_entries, indent=2)
            + "\nuser_entries="
            + json.dumps(user_entries, indent=2)
            + "\nexpected="
            + json.dumps(expected, indent=2)
        )
    assert sum(
        event["metadata"]["event_type"] == "model.response_completed"  # type: ignore[index]
        for event in all_events
    ) == 1
    journal = journal_bytes(args.root, args.session)
    inspection = json.loads(
        run(args.cli, "session", "inspect", args.session, "--json").stdout
    )
    args.state.write_text(
        json.dumps(
            {
                "head": inspection["head_sequence"],
                "journal_sha256": hashlib.sha256(journal).hexdigest(),
                "journal_bytes": len(journal),
            }
        ),
        encoding="utf-8",
    )


def replay(args: argparse.Namespace) -> None:
    state = json.loads(args.state.read_text())
    before = journal_bytes(args.root, args.session)
    assert hashlib.sha256(before).hexdigest() == state["journal_sha256"]
    run(
        args.cli,
        "session",
        "replay",
        args.session,
        "--at",
        str(state["head"]),
        "--json",
    )
    assert journal_bytes(args.root, args.session) == before
    transient = run(
        args.tui,
        "--smoke-session-command",
        args.session,
        "/attachments",
    ).stdout
    assert "attachments: none" in transient and "attachments=0" in transient
    assert journal_bytes(args.root, args.session) == before
    inspection = json.loads(
        run(args.cli, "session", "inspect", args.session, "--json").stdout
    )
    assert inspection["head_sequence"] == state["head"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("execute", "replay"), required=True)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--image", type=pathlib.Path, required=True)
    parser.add_argument("--blob", type=pathlib.Path, required=True)
    parser.add_argument("--state", type=pathlib.Path, required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--tui", required=True)
    args = parser.parse_args()
    if args.phase == "execute":
        execute(args)
    else:
        replay(args)


if __name__ == "__main__":
    main()
