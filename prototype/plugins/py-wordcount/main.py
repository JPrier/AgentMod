"""py-wordcount: a tool plugin written in Python (native runtime only)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sdk"))
from agentmod import Plugin  # noqa: E402

# The tool exists because this plugin subscribes to its member; the member's
# `receives.args` is the tool's argument schema (compiler.md §3.4).
TOOL_CALL = {
    "call_id": {"shape": "string"},
    "name": {"shape": "string"},
    "args": {"shape": "object", "keys": {"text": {"shape": "string", "optional": True, "description": "the text to count"}}},
}


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
        "subscribe": [
            {"event": "tool-call/wordcount", "mode": "async", "receives": TOOL_CALL,
             "description": "Count words and characters in text."},
        ],
        "emit": [{"event": "tool-result", "keys": {
            "call_id": {"shape": "string"}, "name": {"shape": "string"}, "output": {"shape": "string"}}}],
    },
    {"tool-call/*": tool_call},
).run()
