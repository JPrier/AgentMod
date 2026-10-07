"""Minimal AgentMod plugin SDK for Python (stdlib only).

Speaks the same newline-delimited JSON-RPC 2.0 protocol as the JS SDK over
stdin/stdout. Demonstrates that plugin authorship is language-agnostic.
"""

import json
import sys
import threading
import traceback

PROTOCOL = "agentmod/0.1"


class Ctx:
    def __init__(self, plugin, req):
        self.plugin = plugin
        self.event = req["event"]
        self.payload = self.event.get("payload", {})
        self.context = self.event.get("context") or []
        self.session_id = self.event["session_id"]
        self.invocation_id = req["invocation_id"]
        self.config = plugin.config
        self.result = {"contributions": []}

    def publish(self, event_name, payload=None, ui=None, lane=None, target_session=None):
        return self.plugin.request("publish", {
            "invocation_id": self.invocation_id, "event_name": event_name,
            "payload": payload or {}, "ui": ui, "lane": lane, "target_session": target_session,
        })

    def add(self, slot, value):
        self.result["contributions"].append({"op": "add", "slot": slot, "value": value})

    def slot(self, name):
        return [c["value"] for c in self.context if c.get("slot") == name]


class Plugin:
    def __init__(self, manifest, handlers):
        self.manifest = manifest
        self.handlers = handlers
        self.config = {}
        self._next = 1
        self._pending = {}
        self._lock = threading.Lock()
        self._out = threading.Lock()

    def send(self, obj):
        obj["jsonrpc"] = "2.0"
        with self._out:
            sys.stdout.write(json.dumps(obj) + "\n")
            sys.stdout.flush()

    def request(self, method, params):
        with self._lock:
            rid = self._next
            self._next += 1
            ev = threading.Event()
            self._pending[rid] = [ev, None]
        self.send({"id": rid, "method": method, "params": params})
        ev.wait()
        msg = self._pending.pop(rid)[1]
        if "error" in msg:
            raise RuntimeError(msg["error"]["message"])
        return msg.get("result")

    def _invoke(self, rid, req):
        name = req["event"]["event_name"]
        handler = self.handlers.get(name) or self.handlers.get("*")
        ctx = Ctx(self, req)
        try:
            if handler:
                handler(ctx)
            self.send({"id": rid, "result": ctx.result})
        except Exception as e:  # noqa: BLE001 - report every failure to the runtime
            traceback.print_exc(file=sys.stderr)
            self.send({"id": rid, "result": {"contributions": [], "error": str(e)}})

    def run(self):
        for line in sys.stdin:
            if not line.strip():
                continue
            msg = json.loads(line)
            method = msg.get("method")
            if method is None:
                slot = self._pending.get(msg.get("id"))
                if slot:
                    slot[1] = msg
                    slot[0].set()
                continue
            rid = msg.get("id")
            params = msg.get("params") or {}
            if method == "initialize":
                self.config = params.get("config") or {}
                self.send({"id": rid, "result": {"protocol": PROTOCOL, "manifest": self.manifest}})
            elif method == "invoke":
                threading.Thread(target=self._invoke, args=(rid, params), daemon=True).start()
            elif method == "shutdown":
                self.send({"id": rid, "result": None})
                return
            elif method in ("cancel", "record"):
                pass
            elif rid is not None:
                self.send({"id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}})
