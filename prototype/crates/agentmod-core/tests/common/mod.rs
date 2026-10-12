//! Shared test fixtures: manifests and definitions from JSON, and a
//! deterministic simulator that plays plugins against the kernel.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};

use agentmod_core::compiler::{Compilation, Revision};
use agentmod_core::kernel::{
    Effect, InvocationResult, InvokeRequest, Kernel, PublishRequest, StartRequest,
};
use agentmod_core::manifest::{Definition, DeploymentConfig, Manifest, PluginConfig};
use agentmod_core::record::{Body, Record};
use agentmod_core::types::Stamp;
use serde_json::{Value, json};

/// Manifests from `{ name: { subscribe, emit, … } }` (name and version filled in).
pub fn manifests(v: Value) -> BTreeMap<String, Manifest> {
    v.as_object()
        .expect("object of manifests")
        .iter()
        .map(|(name, m)| {
            let mut m = m.clone();
            m["name"] = json!(name);
            if m.get("version").is_none() {
                m["version"] = json!("1.0.0");
            }
            let parsed: Manifest =
                serde_json::from_value(m).unwrap_or_else(|e| panic!("manifest `{name}`: {e}"));
            (name.clone(), parsed)
        })
        .collect()
}

pub fn definition(v: Value) -> Definition {
    serde_json::from_value(v).expect("definition")
}

/// A deployment with every manifest's plugin configured and these definitions.
pub fn deployment(ms: &BTreeMap<String, Manifest>, defs: Value) -> DeploymentConfig {
    let mut cfg = DeploymentConfig::default();
    for name in ms.keys() {
        cfg.plugins.insert(
            name.clone(),
            PluginConfig {
                command: vec![name.clone()],
                config: json!({}),
                binary_hash: Some(format!("bin-{name}")),
                ..Default::default()
            },
        );
    }
    for (name, d) in defs.as_object().expect("object of definitions") {
        cfg.definitions.insert(name.clone(), definition(d.clone()));
    }
    cfg
}

pub const S: &str = "string";

/// `{ "shape": s }`.
pub fn k(shape: &str) -> Value {
    json!({ "shape": shape })
}

/// `{ "shape": s, "optional": true }`.
pub fn opt(shape: &str) -> Value {
    json!({ "shape": shape, "optional": true })
}

pub fn codes(diags: &[agentmod_core::compiler::Diagnostic]) -> Vec<&str> {
    diags.iter().map(|d| d.code.as_str()).collect()
}

/// Plays plugins against the kernel deterministically.
#[derive(Default)]
pub struct Sim {
    pub k: Kernel,
    pub logs: BTreeMap<String, Vec<Record>>,
    pub pending: VecDeque<(String, Stamp, InvokeRequest)>,
    pub cancelled: Vec<String>,
    pub saved: Vec<Revision>,
    pub now: u64,
}

impl Sim {
    pub fn new(comp: Compilation) -> Self {
        let mut k = Kernel::new();
        assert!(comp.ok, "{:#?}", comp.errors());
        k.install(comp).unwrap();
        Sim {
            k,
            ..Default::default()
        }
    }

    pub fn take(&mut self, fx: Vec<Effect>) {
        for f in fx {
            match f {
                Effect::Append { record } => {
                    let log = self.logs.entry(record.session_id.clone()).or_default();
                    assert_eq!(
                        record.sequence,
                        log.len() as u64 + 1,
                        "sequences are dense and monotonic"
                    );
                    log.push(record);
                }
                Effect::SaveRevision { revision } => self.saved.push(*revision),
                Effect::Invoke {
                    plugin,
                    stamp,
                    request,
                    ..
                } => self.pending.push_back((plugin, stamp, request)),
                Effect::Cancel { invocation_id, .. } => self.cancelled.push(invocation_id),
            }
        }
    }

    pub fn tick(&mut self) -> u64 {
        self.now += 10;
        self.now
    }

    pub fn start_req(&mut self, req: &StartRequest) -> Result<String, String> {
        let now = self.tick();
        let out = self.k.start_session(req, now);
        self.take(out.effects);
        match (out.session_id, out.error) {
            (Some(s), _) => Ok(s),
            (None, e) => Err(e.unwrap_or_default()),
        }
    }

    /// Start a session of `def` from `plugin` (a `start-session` call),
    /// optionally with an input for `session-started`.
    pub fn start(&mut self, plugin: &str, def: &str, input: Option<Value>) -> String {
        let req = StartRequest {
            plugin: plugin.into(),
            definition: def.into(),
            spec: None,
            invocation_id: None,
            input,
            ui: None,
            fork_from: None,
        };
        self.start_req(&req)
            .unwrap_or_else(|e| panic!("start {def}: {e}"))
    }

    pub fn publish(&mut self, req: PublishRequest) -> Result<String, String> {
        let now = self.tick();
        let out = self.k.publish(&req, now);
        self.take(out.effects);
        match (out.event_id, out.blocked) {
            (Some(id), None) => Ok(id),
            (_, Some(b)) => Err(b),
            _ => unreachable!(),
        }
    }

    /// Publish as output of an open invocation.
    pub fn emit(
        &mut self,
        plugin: &str,
        inv: &str,
        name: &str,
        payload: Value,
    ) -> Result<String, String> {
        self.publish(PublishRequest {
            plugin: plugin.into(),
            invocation_id: Some(inv.into()),
            cite: None,
            event_name: name.into(),
            payload,
            ui: None,
            lane: None,
            target_session: None,
        })
    }

    pub fn complete(&mut self, inv: &str, result: InvocationResult) {
        let now = self.tick();
        let fx = self.k.complete(inv, &result, now);
        self.take(fx);
    }

    /// Drive pending invocations through `behave` until none are left.
    pub fn run(
        &mut self,
        behave: &mut dyn FnMut(&mut Sim, &str, &InvokeRequest) -> InvocationResult,
    ) {
        self.run_steps(10_000, behave);
        assert!(self.pending.is_empty(), "runaway");
    }

    /// Drive at most `n` invocations.
    pub fn run_steps(
        &mut self,
        n: usize,
        behave: &mut dyn FnMut(&mut Sim, &str, &InvokeRequest) -> InvocationResult,
    ) {
        for _ in 0..n {
            let Some((plugin, _, req)) = self.pending.pop_front() else {
                return;
            };
            if self.cancelled.contains(&req.invocation_id) {
                continue;
            }
            let r = behave(self, &plugin, &req);
            self.complete(&req.invocation_id, r);
        }
    }

    pub fn log(&self, sid: &str) -> &[Record] {
        &self.logs[sid]
    }

    pub fn kinds(&self, sid: &str) -> Vec<String> {
        self.log(sid)
            .iter()
            .map(|r| r.body.kind().to_owned())
            .collect()
    }

    pub fn events(&self, sid: &str) -> Vec<String> {
        self.log(sid)
            .iter()
            .filter_map(|r| match &r.body {
                Body::EventAppended { event } => Some(event.event_name.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn payloads(&self, sid: &str, name: &str) -> Vec<Value> {
        self.log(sid)
            .iter()
            .filter_map(|r| match &r.body {
                Body::EventAppended { event } if event.event_name == name => {
                    Some(event.payload.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn breaches(&self, sid: &str) -> Vec<Value> {
        self.log(sid)
            .iter()
            .filter(|r| matches!(r.body, Body::Breach { .. }))
            .map(|r| serde_json::to_value(r).unwrap())
            .collect()
    }

    /// The first invocation dispatched to `plugin` in a session.
    pub fn first_invocation(&self, sid: &str, plugin: &str) -> String {
        self.log(sid)
            .iter()
            .find_map(|r| match &r.body {
                Body::InvocationStarted {
                    invocation_id,
                    plugin: p,
                    ..
                } if p == plugin => Some(invocation_id.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{plugin} has an invocation in {sid}"))
    }
}
