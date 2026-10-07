"""py-wordcount: a tool plugin written in Python (native runtime only)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sdk"))
from agentmod import Plugin  # noqa: E402

TOOL = {"name": "wordcount", "description": "Count words and characters in text.", "parameters": {"text": {"type": "string"}}}


def offer(ctx):
    if not any(t.get("name") == "wordcount" for t in ctx.slot("tools")):
        ctx.add("tools", TOOL)


def tool_call(ctx):
    if ctx.payload.get("name") != "wordcount":
        return
    text = str((ctx.payload.get("args") or {}).get("text", ""))
    output = f"{len(text.split())} words, {len(text)} characters"
    ctx.publish("tool-result", {"call_id": ctx.payload["call_id"], "name": "wordcount", "output": output},
                ui={"v": 1, "kind": "tool", "name": "wordcount", "status": "done", "result": output})


Plugin(
    {
        "name": "py-wordcount",
        "version": "0.1.0",
        "description": "Tool written in Python: word counts.",
        "consumes": [
            {"event": "session-started"},
            {"event": "config-applied"},
            {"event": "tool-call", "demands": ["call_id", "name", "args"], "mode": "async", "context": False},
        ],
        "emits": [{"event": "tool-result", "supplies": ["call_id", "name", "output"]}],
    },
    {"session-started": offer, "config-applied": offer, "tool-call": tool_call},
).run()
