//! WebAssembly binding of the AgentMod core for the in-browser runtime.
//!
//! The browser host (`ui/runtime/`) runs the *same* deterministic kernel as the
//! native runtime. Every call takes and returns JSON strings shaped exactly like
//! the core's serde types, so both hosts execute identical effect streams.

use agentmod_core::compiler;
use agentmod_core::kernel::{InvocationResult, Kernel, PublishRequest, Scope, StartRequest};
use agentmod_core::manifest::{DeploymentConfig, Manifest};
use agentmod_core::projection;
use agentmod_core::record::{Command, Record};
use agentmod_core::stream::{
    InFrame, Materialized, RecoveryOp, StreamHub, StreamOutput, StreamSettings, StreamStatus,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

fn out<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|e| json!({ "error": e.to_string() }).to_string())
}

fn err(e: impl std::fmt::Display) -> String {
    json!({ "error": e.to_string() }).to_string()
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn ms(now: f64) -> u64 {
    now.max(0.0) as u64
}

/// Compile a deployment config against manifests (both JSON).
#[wasm_bindgen]
#[must_use]
pub fn compile(config_json: &str, manifests_json: &str) -> String {
    let config: DeploymentConfig = match serde_json::from_str(config_json) {
        Ok(c) => c,
        Err(e) => return err(format!("invalid config: {e}")),
    };
    let manifests: BTreeMap<String, Manifest> = match serde_json::from_str(manifests_json) {
        Ok(m) => m,
        Err(e) => return err(format!("invalid manifests: {e}")),
    };
    out(&compiler::compile(&config, &manifests))
}

/// Project a session's records into its session → events → invocations tree.
#[wasm_bindgen]
#[must_use]
pub fn project(records_json: &str) -> String {
    match serde_json::from_str::<Vec<Record>>(records_json) {
        Ok(r) => out(&projection::project(&r)),
        Err(e) => err(e),
    }
}

/// Model-efficiency and control-plane metrics of a session's records.
#[wasm_bindgen]
#[must_use]
pub fn session_metrics(records_json: &str) -> String {
    match serde_json::from_str::<Vec<Record>>(records_json) {
        Ok(r) => out(&agentmod_core::metrics::session_metrics(&r)),
        Err(e) => err(e),
    }
}

/// Context as it stood after a sequence number.
#[wasm_bindgen]
#[must_use]
pub fn context_at(records_json: &str, sequence: f64) -> String {
    match serde_json::from_str::<Vec<Record>>(records_json) {
        Ok(r) => out(&projection::context_at(&r, ms(sequence))),
        Err(e) => err(e),
    }
}

/// The kernel, owned by the browser host.
#[wasm_bindgen]
#[derive(Default)]
pub struct WasmKernel {
    k: Kernel,
}

#[wasm_bindgen]
impl WasmKernel {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self { k: Kernel::new() }
    }

    /// Install a compilation; returns `{hash}` or `{error}`.
    pub fn install(&mut self, compilation_json: &str) -> String {
        match serde_json::from_str(compilation_json) {
            Ok(c) => match self.k.install(c) {
                Ok(hash) => json!({ "hash": hash }).to_string(),
                Err(e) => err(e),
            },
            Err(e) => err(e),
        }
    }

    pub fn set_active(&mut self, hash: &str) -> String {
        match self.k.set_active(hash) {
            Ok(()) => json!({ "ok": true }).to_string(),
            Err(e) => err(e),
        }
    }

    #[must_use]
    pub fn active_hash(&self) -> String {
        self.k.active_hash().to_owned()
    }

    #[must_use]
    pub fn active(&self) -> String {
        out(&self.k.active())
    }

    #[must_use]
    pub fn config(&self, hash: &str) -> String {
        out(&self.k.config(hash))
    }

    #[must_use]
    pub fn revisions_in_use(&self) -> String {
        out(&self.k.revisions_in_use())
    }

    /// `[[name, stamp, config], …]` of every plugin instance a revision in use needs.
    #[must_use]
    pub fn plugins_in_use(&self) -> String {
        out(&self
            .k
            .plugins_in_use()
            .into_iter()
            .map(|((n, s), c)| json!([n, s, c]))
            .collect::<Vec<_>>())
    }

    /// Install a persisted revision; returns `{revision}` or `{error}`.
    pub fn install_revision(&mut self, revision_json: &str) -> String {
        match serde_json::from_str(revision_json) {
            Ok(r) => json!({ "revision": self.k.install_revision(r) }).to_string(),
            Err(e) => err(e),
        }
    }

    /// A revision by hash (or null).
    #[must_use]
    pub fn revision(&self, hash: &str) -> String {
        out(&self.k.revision(hash))
    }

    /// The revision a loaded session runs under (or null).
    #[must_use]
    pub fn session_revision(&self, session_id: &str) -> String {
        out(&self.k.session_revision(session_id))
    }

    pub fn start_session(&mut self, req_json: &str, now: f64) -> String {
        match serde_json::from_str::<StartRequest>(req_json) {
            Ok(r) => out(&self.k.start_session(&r, ms(now))),
            Err(e) => err(e),
        }
    }

    pub fn publish(&mut self, req_json: &str, now: f64) -> String {
        match serde_json::from_str::<PublishRequest>(req_json) {
            Ok(r) => out(&self.k.publish(&r, ms(now))),
            Err(e) => err(e),
        }
    }

    pub fn complete(&mut self, invocation_id: &str, result_json: &str, now: f64) -> String {
        let result: InvocationResult =
            serde_json::from_str(result_json).unwrap_or_else(|e| InvocationResult {
                error: Some(format!("malformed result: {e}")),
                ..Default::default()
            });
        out(&json!({ "effects": self.k.complete(invocation_id, &result, ms(now)) }))
    }

    pub fn retry(
        &mut self,
        invocation_id: &str,
        reason: &str,
        max_attempts: u32,
        now: f64,
    ) -> String {
        out(&json!({ "effects": self.k.retry(invocation_id, reason, max_attempts, ms(now)) }))
    }

    pub fn command(&mut self, session_id: &str, command: &str, by: &str, now: f64) -> String {
        let cmd: Command = match serde_json::from_value(Value::String(command.to_owned())) {
            Ok(c) => c,
            Err(e) => return err(e),
        };
        match self.k.command(session_id, cmd, by, ms(now)) {
            Ok(fx) => out(&json!({ "effects": fx, "status": self.k.status(session_id) })),
            Err(e) => err(e),
        }
    }

    pub fn apply_config(&mut self, hash: &str, scope_json: &str, now: f64) -> String {
        match serde_json::from_str::<Scope>(scope_json) {
            Ok(s) => out(&self.k.apply_config(hash, &s, ms(now))),
            Err(e) => err(e),
        }
    }

    pub fn load_session(&mut self, records_json: &str) -> String {
        match serde_json::from_str::<Vec<Record>>(records_json) {
            Ok(r) => match self.k.load_session(&r) {
                Ok(sid) => json!({ "session_id": sid }).to_string(),
                Err(e) => err(e),
            },
            Err(e) => err(e),
        }
    }

    pub fn recover(&mut self, session_id: &str, max_attempts: u32, now: f64) -> String {
        out(&json!({ "effects": self.k.recover(session_id, max_attempts, ms(now)) }))
    }

    #[must_use]
    pub fn status(&self, session_id: &str) -> String {
        out(&self.k.status(session_id))
    }

    #[must_use]
    pub fn context(&self, session_id: &str) -> String {
        out(&self.k.context(session_id))
    }

    #[must_use]
    pub fn is_idle(&self, session_id: &str) -> bool {
        self.k.is_idle(session_id)
    }

    #[must_use]
    pub fn open_invocations(&self, plugin: &str) -> String {
        out(&self.k.open_invocations(plugin))
    }

    pub fn unload_if_idle(&mut self, session_id: &str) -> bool {
        self.k.unload_if_idle(session_id)
    }

    #[must_use]
    pub fn is_loaded(&self, session_id: &str) -> bool {
        self.k.is_loaded(session_id)
    }

    /// A plugin's resolved emit set under an installed revision (`null` when
    /// the revision is not installed).
    #[must_use]
    pub fn emit_set(&self, revision: &str, plugin: &str) -> String {
        out(&self.k.emit_set(revision, plugin))
    }

    /// `[plugin, open]` for an invocation, or null.
    #[must_use]
    pub fn invocation(&self, invocation_id: &str) -> String {
        out(&self.k.invocation(invocation_id))
    }
}

/// The live stream hub (same code as the native host), for the browser host.
#[wasm_bindgen]
pub struct WasmStreamHub {
    h: StreamHub,
}

fn status_of(s: &str) -> StreamStatus {
    serde_json::from_value(Value::String(s.to_owned())).unwrap_or(StreamStatus::Interrupted)
}

#[wasm_bindgen]
impl WasmStreamHub {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(settings_json: &str) -> Self {
        let settings: StreamSettings = serde_json::from_str(settings_json).unwrap_or_default();
        Self {
            h: StreamHub::new(settings),
        }
    }

    /// Ingest frames; returns `{ result, output }`.
    pub fn ingest(&mut self, stream_id: &str, frames_json: &str, now: f64) -> String {
        let frames: Vec<InFrame> = match serde_json::from_str(frames_json) {
            Ok(f) => f,
            Err(e) => return err(format!("invalid frames: {e}")),
        };
        let mut o = StreamOutput::default();
        let r = self.h.ingest(stream_id, frames, ms(now), &mut o);
        out(&json!({ "result": r, "output": o }))
    }

    pub fn poll(&mut self, now: f64) -> String {
        let mut o = StreamOutput::default();
        self.h.poll(ms(now), &mut o);
        out(&o)
    }

    /// Next flush deadline in ms, or -1.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn next_deadline(&self) -> f64 {
        self.h.next_deadline().map_or(-1.0, |d| d as f64)
    }

    #[must_use]
    pub fn is_open(&self, stream_id: &str) -> bool {
        self.h.is_open(stream_id)
    }

    pub fn finalize(&mut self, stream_id: &str, outcome: &str) -> String {
        let mut o = StreamOutput::default();
        self.h.finalize(stream_id, status_of(outcome), &mut o);
        out(&o)
    }

    pub fn interrupt(&mut self, stream_id: &str) -> String {
        let mut o = StreamOutput::default();
        self.h.interrupt(stream_id, &mut o);
        out(&o)
    }

    /// Attach a client (`session` may be empty for all); `{ client, streams }`.
    pub fn attach(&mut self, session: &str) -> String {
        let (client, streams) = self.h.attach(Some(session).filter(|s| !s.is_empty()));
        out(&json!({ "client": client, "streams": streams }))
    }

    pub fn detach(&mut self, client: u32) {
        self.h.detach(u64::from(client));
    }

    pub fn drain(&mut self, client: u32, max_bytes: u32) -> String {
        out(&self.h.drain(u64::from(client), max_bytes as usize))
    }

    pub fn resync(&mut self, client: u32) -> String {
        out(&self.h.resync(u64::from(client)))
    }

    #[must_use]
    pub fn snapshots(&self, session: &str) -> String {
        out(&self.h.snapshots(Some(session).filter(|s| !s.is_empty())))
    }

    pub fn restore(&mut self, state_json: &str) -> String {
        match serde_json::from_str::<Materialized>(state_json) {
            Ok(s) => {
                self.h.restore(s);
                json!({ "ok": true }).to_string()
            }
            Err(e) => err(e),
        }
    }

    #[must_use]
    pub fn counters(&self) -> String {
        out(&self.h.counters)
    }
}

/// Fold stored recovery ops back into a materialized stream (or null).
#[wasm_bindgen]
#[must_use]
pub fn replay_recovery(ops_json: &str, settings_json: &str) -> String {
    let ops: Vec<RecoveryOp> = match serde_json::from_str(ops_json) {
        Ok(o) => o,
        Err(e) => return err(e),
    };
    let settings: StreamSettings = serde_json::from_str(settings_json).unwrap_or_default();
    out(&StreamHub::replay_recovery(&ops, &settings))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_api_round_trip() {
        let manifests = json!({
            "ui": { "name": "ui", "version": "1", "sink": true,
                    "subscribe": [{ "event": "session-started", "mode": "async" }],
                    "emit": [{ "event": "user-message", "keys": { "text": { "shape": "string" } }, "deferred": true }],
                    "capabilities": ["start-session", "deferred-publish"] },
            "echo": { "name": "echo", "version": "1", "subscribe": [{ "event": "user-message", "mode": "blocking", "receives": { "text": { "shape": "string" } }, "actions": ["veto"] }] }
        });
        let config = json!({
            "plugins": { "ui": { "module": "ui.js" }, "echo": { "module": "echo.js" } },
            "definitions": { "chat": { "runtime": "0.1", "phases": ["main"], "plugins": { "ui": {}, "echo": { "phase": "main" } } } }
        });
        let comp = compile(&config.to_string(), &manifests.to_string());
        let c: Value = serde_json::from_str(&comp).unwrap();
        assert_eq!(c["ok"], true, "{comp}");
        let mut k = WasmKernel::new();
        let h: Value = serde_json::from_str(&k.install(&comp)).unwrap();
        assert!(h["hash"].is_string());
        let start = k.start_session(
            &json!({ "plugin": "ui", "definition": "chat", "input": { "text": "hi" } }).to_string(),
            1000.0,
        );
        let s: Value = serde_json::from_str(&start).unwrap();
        assert_eq!(s["session_id"], "s0001");
        let rev = s["revision"].as_str().unwrap();
        let set: Value = serde_json::from_str(&k.emit_set(rev, "ui")).unwrap();
        assert_eq!(set[0]["event"], "user-message");
        assert_eq!(
            serde_json::from_str::<Value>(&k.emit_set("nope", "ui")).unwrap(),
            Value::Null
        );
        let fx = s["effects"].as_array().unwrap();
        let invoke = fx.iter().find(|e| e["type"] == "invoke").unwrap();
        let inv = invoke["request"]["invocation_id"].as_str().unwrap();
        let done: Value = serde_json::from_str(&k.complete(inv, "{}", 1001.0)).unwrap();
        assert!(done["effects"].as_array().is_some());
        let records: Vec<Value> = fx
            .iter()
            .chain(done["effects"].as_array().unwrap())
            .filter(|e| e["type"] == "append")
            .map(|e| e["record"].clone())
            .collect();
        let view: Value =
            serde_json::from_str(&project(&serde_json::to_string(&records).unwrap())).unwrap();
        assert_eq!(view["session_id"], "s0001");
        let cmd: Value =
            serde_json::from_str(&k.command("s0001", "soft-stop", "test", 1002.0)).unwrap();
        assert_eq!(cmd["status"]["state"], "draining");
        assert!(serde_json::from_str::<Value>(&k.command("s0001", "explode", "x", 1.0)).unwrap()["error"].is_string());
    }
}
