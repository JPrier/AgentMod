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
        # Projected delivery: exactly the keys (and slots) the subscription declared.
        self.payload = self.event.get("payload", {})
        self.slots = self.event.get("slots") or {}
        self.subscription = req.get("subscription")
        self.session_id = self.event["session_id"]
        self.invocation_id = req["invocation_id"]
        self.config = plugin.config
        self.result = {"contributions": []}

    @property
    def context(self):
        """The `context` slot (only when the subscription requested it)."""
        if "context" not in self.slots:
            raise RuntimeError(
                f"read the context on {self.event['event_name']}, but the subscription "
                f"({self.subscription}) does not request the `context` slot (receives.slots.context)")
        return self.slots["context"] or []

    def publish(self, event_name, payload=None, ui=None, lane=None, target_session=None):
        return self.plugin.request("publish", {
            "invocation_id": self.invocation_id, "event_name": event_name,
            "payload": payload or {}, "ui": ui, "lane": lane, "target_session": target_session,
        })

    def add(self, slot, value):
        self.result["contributions"].append({"op": "add", "slot": slot, "value": value})

    def returns(self, **keys):
        """Blocking subscribers: hand keys back to the chain (declared in `returns`)."""
        self.result.setdefault("returns", {}).update(keys)

    def veto(self, reason):
        self.result["veto"] = reason

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

    def handler_for(self, name, subscription):
        """Exact event name, the subscription's pattern, a matching family (`tool-call/*`), or '*'."""
        if name in self.handlers:
            return self.handlers[name]
        if subscription in self.handlers:
            return self.handlers[subscription]
        families = sorted((k for k in self.handlers if k != "*" and k.endswith("*")
                           and name.startswith(k[:-1]) and len(name) > len(k) - 1), key=len, reverse=True)
        return self.handlers[families[0]] if families else self.handlers.get("*")

    def _invoke(self, rid, req):
        name = req["event"]["event_name"]
        handler = self.handler_for(name, req.get("subscription"))
        ctx = Ctx(self, req)
        try:
            if handler:
                handler(ctx)
            if not ctx.result["contributions"]:
                del ctx.result["contributions"]
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
