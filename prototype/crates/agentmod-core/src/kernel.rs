//! The kernel: deterministic per-session dispatch along the compiled graph.
//!
//! The kernel is sans-I/O. Every operation returns [`Effect`]s: records to
//! append (write-ahead), revisions to persist, and invocations to start or
//! cancel. All state changes go through [`Kernel::apply`], the same function
//! replay uses, so a session rebuilt from its log is indistinguishable from
//! the live one.
//!
//! Routing never scans: each event's destinations are the compiled graph's
//! edges for its emitter and name (compiler.md §5). Delivery is projected, and
//! anything that contradicts a declaration is a recorded hard failure (§7).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::compiler::{
    Compilation, Diagnostic, Edge, FamilyHandlers, Revision, compile_definition,
};
use crate::context::ContextState;
use crate::core_manifest::{
    BOUNDARY, CONFIG_APPLIED, DISPATCH_FAILED, SESSION_START_FAILED, SESSION_STARTED,
};
use crate::manifest::{
    ACTION_CONTRIBUTE, ACTION_VETO, CONTEXT_SLOT, Capability, Definition, Manifest, PluginConfig,
    Subscription,
};
use crate::record::{Body, Command, DispatchState, Outcome, Record, Settlement};
use crate::shape::{Keys, check_keys, check_value, keys_compatible, kind_of};
use crate::types::{
    Cause, ContextItem, ContextOp, Envelope, EventRecord, Lane, LogRef, Mode, Origin, Stamp,
    hash_json, session_of,
};

/// What the host must do, in order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Effect {
    /// Durably append before acting on any later effect.
    Append { record: Record },
    /// Persist a revision sessions reference (compiled at start or apply).
    SaveRevision { revision: Box<Revision> },
    /// Dispatch an invocation to a plugin instance.
    Invoke {
        plugin: String,
        stamp: Stamp,
        attempt: u32,
        request: InvokeRequest,
    },
    /// Cancel an in-flight invocation (hard stop or timeout).
    Cancel {
        plugin: String,
        stamp: Stamp,
        invocation_id: String,
    },
}

/// The `invoke` request sent to a plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeRequest {
    pub invocation_id: String,
    pub mode: Mode,
    pub position: usize,
    pub attempt: u32,
    /// The subscription (event pattern) this invocation was compiled from.
    pub subscription: String,
    pub event: Envelope,
}

/// A plugin's answer to an invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct InvocationResult {
    /// `contribute-context` action: context operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<ContextOp>,
    /// Blocking subscribers: keys handed back to the chain (overwrite or add).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<Map<String, Value>>,
    /// `veto` action: stop the event with a reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub veto: Option<String>,
    /// Plugin-reported failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The one way to publish.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishRequest {
    /// Publisher identity, derived from the connection by the host.
    #[serde(default)]
    pub plugin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    /// Standing trigger cited by a deferred publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cite: Option<String>,
    pub event_name: String,
    #[serde(default = "empty_object")]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
    /// Defaults to the emit declaration's lane; a different lane is a breach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<Lane>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session: Option<String>,
}

fn empty_object() -> Value {
    json!({})
}

impl PublishRequest {
    /// Sessions that must be loaded before this request can be classified.
    #[must_use]
    pub fn sessions(&self) -> Vec<String> {
        let mut out = BTreeSet::new();
        for id in [&self.invocation_id, &self.cite, &self.target_session]
            .into_iter()
            .flatten()
        {
            if let Some(s) = session_of(id) {
                out.insert(s.to_owned());
            }
        }
        out.into_iter().collect()
    }
}

/// Initial event carried by `start-session`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitialEvent {
    pub event_name: String,
    #[serde(default = "empty_object")]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
}

/// The sole rootless act.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartRequest {
    #[serde(default)]
    pub plugin: String,
    /// A definition of the active config, by name (ignored with `spec`).
    #[serde(default)]
    pub definition: String,
    /// A definition written by the caller (compiled now, against the active
    /// config's plugins).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<Definition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial: Option<InitialEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_from: Option<LogRef>,
}

/// Outcome of a publish.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PublishOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
    /// True when an identical output was already recorded (idempotent retry).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub duplicate: bool,
    pub effects: Vec<Effect>,
}

/// Outcome of a start-session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StartOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// A failed compile: the diagnostics (also delivered to the caller's
    /// session as `session-start-failed` when the start came from an invocation).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    pub effects: Vec<Effect>,
}

/// Configuration apply scope: which running sessions an apply targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Scope {
    /// New sessions start from this config, and every loaded session that
    /// follows the global layer adopts it.
    Global,
    /// One session (a per-session layer).
    Session { session_id: String },
    /// These sessions (per-session layers).
    Sessions { session_ids: Vec<String> },
}

/// Outcome of an apply. Rejected whole on any error: then no session changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ApplyOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Session → the revision it adopts at its next event boundary.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub revisions: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Diagnostic>,
    pub effects: Vec<Effect>,
}

/// Snapshot of a session's dispatch status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionStatus {
    pub session_id: String,
    pub definition: String,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_revision: Option<String>,
    pub state: DispatchState,
    /// `running`, `idle`, `draining`, `parked`, or `halted`.
    pub activity: String,
    pub queued_interrupt: usize,
    pub queued_normal: usize,
    pub open_invocations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_event: Option<String>,
    pub last_sequence: u64,
}

#[derive(Debug, Clone)]
struct EventInfo {
    record: EventRecord,
    sequence: u64,
    at: u64,
}

#[derive(Debug, Clone)]
struct InvInfo {
    plugin: String,
    event_id: String,
    mode: Mode,
    position: usize,
    subscription: String,
    slots: Vec<String>,
    open: bool,
    attempt: u32,
    start_sequence: u64,
    /// What was delivered (the projected payload).
    payload: Value,
    outputs: Vec<String>,
    attempt_outputs: usize,
    /// Provenance bound at dispatch: retries and cancels use this exact
    /// executor identity even if the session's revision changed since.
    stamp: Stamp,
    revision: String,
    /// Family member of a handler dispatch.
    route: Option<String>,
}

#[derive(Debug, Clone)]
struct Run {
    event_id: String,
    /// The event's payload with every earlier `returns` applied.
    payload: Value,
    next_blocking: usize,
    open: Option<String>,
}

#[derive(Debug, Clone)]
struct Session {
    id: String,
    definition: String,
    revision: String,
    pending: Option<(String, bool)>,
    session_scoped: bool,
    state: DispatchState,
    next_sequence: u64,
    next_invocation: u64,
    interrupt: VecDeque<String>,
    normal: VecDeque<String>,
    events: BTreeMap<String, EventInfo>,
    invocations: BTreeMap<String, InvInfo>,
    run: Option<Run>,
    open_async: BTreeSet<String>,
    context: ContextState,
}

impl Session {
    fn quiescent(&self) -> bool {
        self.run.is_none()
            && self.interrupt.is_empty()
            && self.normal.is_empty()
            && self.open_async.is_empty()
    }
}

/// The deterministic kernel.
#[derive(Debug, Default)]
pub struct Kernel {
    /// Installed deployment configs by hash.
    configs: BTreeMap<String, Arc<Compilation>>,
    /// Every installed revision by hash (sessions reference these).
    revisions: BTreeMap<String, Arc<Revision>>,
    active: String,
    sessions: BTreeMap<String, Session>,
    next_session: u64,
}

const CORE_PLUGIN: &str = "core";

/// The emitter of an event in the compiled graph.
fn emitter_of(origin: &Origin) -> String {
    match origin {
        Origin::Pipeline { plugin, .. }
        | Origin::Deferred { plugin, .. }
        | Origin::Start { plugin } => plugin.clone(),
        Origin::Core | Origin::CrossSession { .. } => BOUNDARY.to_owned(),
    }
}

/// The payload a subscription receives: declared keys (optional ones when
/// present), or everything with `unlisted_keys`.
fn project(payload: &Value, sub: &Subscription) -> Value {
    let Some(obj) = payload.as_object() else {
        return json!({});
    };
    if sub.unlisted_keys {
        return payload.clone();
    }
    let mut out = Map::new();
    for k in sub.receives.keys.keys() {
        if let Some(v) = obj.get(k) {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

impl Kernel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_session: 1,
            ..Default::default()
        }
    }

    /// Install a compiled deployment config (must compile cleanly) and its
    /// revisions. Installing does not apply it.
    ///
    /// # Errors
    /// Returns the compiler errors if the compilation is not ok.
    pub fn install(&mut self, compilation: Compilation) -> Result<String, String> {
        if !compilation.ok {
            let errs: Vec<String> = compilation
                .errors()
                .iter()
                .map(|d| d.message.clone())
                .collect();
            return Err(format!("config rejected: {}", errs.join("; ")));
        }
        let hash = compilation.hash.clone();
        if self.active.is_empty() {
            self.active.clone_from(&hash);
        }
        for r in compilation.definitions.values() {
            self.revisions
                .entry(r.revision.clone())
                .or_insert_with(|| Arc::new(r.clone()));
        }
        self.configs.insert(hash.clone(), Arc::new(compilation));
        Ok(hash)
    }

    /// Install one revision (persisted by the host from an earlier start or apply).
    pub fn install_revision(&mut self, revision: Revision) -> String {
        let h = revision.revision.clone();
        self.revisions
            .entry(h.clone())
            .or_insert_with(|| Arc::new(revision));
        h
    }

    /// Set the active config without touching sessions (startup).
    ///
    /// # Errors
    /// Fails if the hash is not installed.
    pub fn set_active(&mut self, hash: &str) -> Result<(), String> {
        if !self.configs.contains_key(hash) {
            return Err(format!("config {hash} is not installed"));
        }
        self.active = hash.to_owned();
        Ok(())
    }

    #[must_use]
    pub fn active(&self) -> Option<&Compilation> {
        self.configs.get(&self.active).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn active_hash(&self) -> &str {
        &self.active
    }

    #[must_use]
    pub fn config(&self, hash: &str) -> Option<&Compilation> {
        self.configs.get(hash).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn revision(&self, hash: &str) -> Option<&Revision> {
        self.revisions.get(hash).map(AsRef::as_ref)
    }

    /// The revision a loaded session runs under.
    #[must_use]
    pub fn session_revision(&self, session_id: &str) -> Option<&Revision> {
        let s = self.sessions.get(session_id)?;
        self.revision(&s.revision)
    }

    /// Revisions referenced by loaded sessions (current or pending) and the
    /// active config: the plugin instances that must stay available.
    #[must_use]
    pub fn revisions_in_use(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> =
            self.sessions.values().map(|s| s.revision.clone()).collect();
        out.extend(
            self.sessions
                .values()
                .filter_map(|s| s.pending.as_ref().map(|p| p.0.clone())),
        );
        if let Some(c) = self.active() {
            out.extend(c.definitions.values().map(|r| r.revision.clone()));
        }
        out
    }

    /// Every (plugin, stamp) → config a referenced revision or the active config uses.
    #[must_use]
    pub fn plugins_in_use(&self) -> BTreeMap<(String, Stamp), PluginConfig> {
        let mut out = BTreeMap::new();
        for h in self.revisions_in_use() {
            if let Some(r) = self.revision(&h) {
                for (n, m) in r.plugins.iter().filter(|(_, m)| m.member) {
                    out.insert((n.clone(), m.stamp.clone()), m.config.clone());
                }
            }
        }
        if let Some(c) = self.active() {
            for (n, p) in c.config.plugins.iter().filter(|(_, p)| !p.disabled) {
                out.insert((n.clone(), p.stamp()), p.clone());
            }
        }
        out
    }

    /// Ensure future session ids start at or after `next`.
    pub fn reserve_session_ids(&mut self, next: u64) {
        self.next_session = self.next_session.max(next);
    }

    #[must_use]
    pub fn next_session_number(&self) -> u64 {
        self.next_session
    }

    #[must_use]
    pub fn is_loaded(&self, session_id: &str) -> bool {
        self.sessions.contains_key(session_id)
    }

    #[must_use]
    pub fn loaded_sessions(&self) -> Vec<String> {
        self.sessions.keys().cloned().collect()
    }

    /// Current context of a loaded session.
    #[must_use]
    pub fn context(&self, session_id: &str) -> Option<Vec<ContextItem>> {
        self.sessions
            .get(session_id)
            .map(|s| s.context.items().to_vec())
    }

    /// Open invocations dispatched to a plugin (for crash handling).
    #[must_use]
    pub fn open_invocations(&self, plugin: &str) -> Vec<String> {
        self.sessions
            .values()
            .flat_map(|s| {
                s.invocations
                    .iter()
                    .filter(|(_, i)| i.open && i.plugin == plugin)
                    .map(|(id, _)| id.clone())
            })
            .collect()
    }

    #[must_use]
    pub fn status(&self, session_id: &str) -> Option<SessionStatus> {
        let s = self.sessions.get(session_id)?;
        let mut open: Vec<String> = s.open_async.iter().cloned().collect();
        if let Some(o) = s.run.as_ref().and_then(|r| r.open.clone()) {
            open.insert(0, o);
        }
        let activity = match s.state {
            DispatchState::Halted => "halted",
            DispatchState::Draining if s.run.is_none() => "parked",
            DispatchState::Draining => "draining",
            DispatchState::Parked => "parked",
            DispatchState::Running if s.run.is_some() || !s.open_async.is_empty() => "running",
            DispatchState::Running if !s.interrupt.is_empty() || !s.normal.is_empty() => "running",
            DispatchState::Running => "idle",
        };
        Some(SessionStatus {
            session_id: s.id.clone(),
            definition: s.definition.clone(),
            revision: s.revision.clone(),
            pending_revision: s.pending.as_ref().map(|p| p.0.clone()),
            state: s.state,
            activity: activity.to_owned(),
            queued_interrupt: s.interrupt.len(),
            queued_normal: s.normal.len(),
            open_invocations: open,
            current_event: s.run.as_ref().map(|r| r.event_id.clone()),
            last_sequence: s.next_sequence.saturating_sub(1),
        })
    }

    /// The compiled `exactly_one_handler` families of a session's revision.
    #[must_use]
    pub fn families(&self, session_id: &str) -> Option<BTreeMap<String, FamilyHandlers>> {
        Some(self.session_revision(session_id)?.report.families.clone())
    }

    // ------------------------------------------------------------------
    // Recording and the single state-transition function.
    // ------------------------------------------------------------------

    fn emit(&mut self, session_id: &str, now: u64, body: Body, fx: &mut Vec<Effect>) -> u64 {
        let seq = self.sessions.get(session_id).map_or(1, |s| s.next_sequence);
        let record = Record {
            session_id: session_id.to_owned(),
            sequence: seq,
            at: now,
            body,
        };
        self.apply(&record);
        fx.push(Effect::Append { record });
        seq
    }

    /// Apply one record to state. Used identically by live operation and replay.
    pub fn apply(&mut self, r: &Record) {
        if let Body::SessionCreated {
            definition,
            revision,
            ..
        } = &r.body
        {
            if let Some(n) = r
                .session_id
                .strip_prefix('s')
                .and_then(|n| n.parse::<u64>().ok())
            {
                self.next_session = self.next_session.max(n + 1);
            }
            self.sessions.insert(
                r.session_id.clone(),
                Session {
                    id: r.session_id.clone(),
                    definition: definition.clone(),
                    revision: revision.clone(),
                    pending: None,
                    session_scoped: false,
                    state: DispatchState::Running,
                    next_sequence: r.sequence + 1,
                    next_invocation: 1,
                    interrupt: VecDeque::new(),
                    normal: VecDeque::new(),
                    events: BTreeMap::new(),
                    invocations: BTreeMap::new(),
                    run: None,
                    open_async: BTreeSet::new(),
                    context: ContextState::default(),
                },
            );
            return;
        }
        let Some(s) = self.sessions.get_mut(&r.session_id) else {
            return;
        };
        s.next_sequence = s.next_sequence.max(r.sequence + 1);
        match &r.body {
            Body::SessionCreated { .. } | Body::PublishBlocked { .. } | Body::Breach { .. } => {}
            Body::ContextSeeded { items, .. } => s.context.seed(r.sequence, items.clone()),
            Body::EventAppended { event } => {
                if let Origin::Pipeline { invocation_id, .. } = &event.origin
                    && let Some(i) = s.invocations.get_mut(invocation_id)
                {
                    i.outputs.push(event.event_id.clone());
                }
                match event.lane {
                    Lane::Interrupt => s.interrupt.push_back(event.event_id.clone()),
                    Lane::Normal => s.normal.push_back(event.event_id.clone()),
                }
                s.events.insert(
                    event.event_id.clone(),
                    EventInfo {
                        record: event.clone(),
                        sequence: r.sequence,
                        at: r.at,
                    },
                );
            }
            Body::PipelineStarted { event_id } => {
                s.interrupt.retain(|e| e != event_id);
                s.normal.retain(|e| e != event_id);
                let payload = s
                    .events
                    .get(event_id)
                    .map_or(Value::Null, |e| e.record.payload.clone());
                s.run = Some(Run {
                    event_id: event_id.clone(),
                    payload,
                    next_blocking: 0,
                    open: None,
                });
            }
            Body::InvocationStarted {
                invocation_id,
                event_id,
                plugin,
                mode,
                position,
                stamp,
                revision,
                payload,
                route,
                subscription,
                ..
            } => {
                if let Some(n) = invocation_id
                    .rsplit("/i")
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    s.next_invocation = s.next_invocation.max(n + 1);
                }
                let slots: Vec<String> = self
                    .revisions
                    .get(revision)
                    .and_then(|rev| rev.manifest(plugin))
                    .and_then(|m| {
                        m.subscribe
                            .iter()
                            .find(|x| x.event == *subscription && x.mode == *mode)
                    })
                    .map(|x| x.receives.slots.keys().cloned().collect())
                    .unwrap_or_default();
                let subscription = subscription.clone();
                s.invocations.insert(
                    invocation_id.clone(),
                    InvInfo {
                        plugin: plugin.clone(),
                        event_id: event_id.clone(),
                        mode: *mode,
                        position: *position,
                        subscription,
                        slots,
                        open: true,
                        attempt: 1,
                        start_sequence: r.sequence,
                        payload: payload.clone(),
                        outputs: Vec::new(),
                        attempt_outputs: 0,
                        stamp: stamp.clone(),
                        revision: revision.clone(),
                        route: route.clone(),
                    },
                );
                match mode {
                    Mode::Blocking => {
                        if let Some(run) = s.run.as_mut() {
                            run.open = Some(invocation_id.clone());
                            run.next_blocking += 1;
                        }
                    }
                    Mode::Async => {
                        s.open_async.insert(invocation_id.clone());
                    }
                }
            }
            Body::InvocationRetried {
                invocation_id,
                attempt,
                ..
            } => {
                if let Some(i) = s.invocations.get_mut(invocation_id) {
                    i.attempt = *attempt;
                    i.attempt_outputs = 0;
                }
            }
            Body::InvocationCompleted {
                invocation_id,
                outcome,
                returns,
                contributions,
                ..
            } => {
                let Some(inv) = s.invocations.get_mut(invocation_id) else {
                    return;
                };
                inv.open = false;
                let plugin = inv.plugin.clone();
                s.open_async.remove(invocation_id);
                if let Some(run) = s
                    .run
                    .as_mut()
                    .filter(|r| r.open.as_deref() == Some(invocation_id.as_str()))
                {
                    run.open = None;
                    if let (Outcome::Ok, Some(Value::Object(ret)), Some(obj)) =
                        (outcome, returns, run.payload.as_object_mut())
                    {
                        for (k, v) in ret {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
                if *outcome == Outcome::Ok {
                    s.context
                        .apply(r.sequence, invocation_id, &plugin, contributions);
                }
            }
            Body::PipelineSettled { .. } => s.run = None,
            Body::CrossSessionSent { event_id, .. } => {
                // Output ids are `{invocation}.o{n}`: credit the publishing invocation.
                if let Some((inv, _)) = event_id.rsplit_once(".o")
                    && let Some(i) = s.invocations.get_mut(inv)
                {
                    i.outputs.push(event_id.clone());
                }
            }
            Body::DispatcherCommand { state, .. } => s.state = *state,
            Body::ConfigApplied {
                revision, scoped, ..
            } => {
                s.revision.clone_from(revision);
                s.session_scoped = *scoped;
                s.pending = None;
            }
        }
    }

    // ------------------------------------------------------------------
    // Dispatch.
    // ------------------------------------------------------------------

    fn rev_of(&self, s: &Session) -> Option<Arc<Revision>> {
        self.revisions.get(&s.revision).cloned()
    }

    /// The edges an event takes, in order: (blocking, asyncs, handler, decl info).
    fn route_edges(rev: &Revision, ev: &EventRecord) -> Option<RouteEdges> {
        let emitter = emitter_of(&ev.origin);
        let (decl, route) = rev.route(&emitter, &ev.event_name)?;
        let Some(route) = route else {
            return Some(RouteEdges {
                family: decl.event.clone(),
                exactly_one_handler: decl.exactly_one_handler,
                ..Default::default()
            });
        };
        Some(RouteEdges {
            blocking: route
                .blocking
                .iter()
                .map(|i| rev.graph.edge(*i).clone())
                .collect(),
            asyncs: route
                .asyncs
                .iter()
                .map(|i| rev.graph.edge(*i).clone())
                .collect(),
            handler: route.handler.map(|i| rev.graph.edge(i).clone()),
            exactly_one_handler: route.exactly_one_handler,
            family: decl.event.clone(),
        })
    }

    fn breach(&mut self, sid: &str, now: u64, b: BreachInfo, fx: &mut Vec<Effect>) {
        let s = &self.sessions[sid];
        let stamp = self
            .rev_of(s)
            .and_then(|r| r.plugins.get(&b.plugin).map(|m| m.stamp.clone()))
            .unwrap_or_default();
        let revision = s.revision.clone();
        self.emit(
            sid,
            now,
            Body::Breach {
                event_id: b.event_id,
                invocation_id: b.invocation_id,
                plugin: b.plugin,
                stamp,
                revision,
                declaration: b.declaration,
                observed: b.observed,
                reason: b.reason,
                reaction: b.reaction.to_owned(),
            },
            fx,
        );
    }

    fn envelope(
        s: &Session,
        event_id: &str,
        payload: Value,
        slots: &[String],
        at_sequence: Option<u64>,
    ) -> Envelope {
        let e = &s.events[event_id];
        let slots = (!slots.is_empty()).then(|| {
            let mut m = Map::new();
            for slot in slots {
                if slot == CONTEXT_SLOT {
                    let items = match at_sequence {
                        Some(seq) => s.context.at(seq).to_vec(),
                        None => s.context.items().to_vec(),
                    };
                    m.insert(
                        slot.clone(),
                        serde_json::to_value(items).unwrap_or(Value::Null),
                    );
                } else {
                    // Other slots have no write path yet (how slot writes combine is deferred).
                    m.insert(slot.clone(), Value::Null);
                }
            }
            m
        });
        Envelope {
            event_id: event_id.to_owned(),
            session_id: s.id.clone(),
            event_name: e.record.event_name.clone(),
            sequence: e.sequence,
            cause: e.record.cause.clone(),
            lane: e.record.lane,
            arrived_at: e.at,
            payload,
            ui: e.record.ui.clone(),
            slots,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start_invocation(
        &mut self,
        sid: &str,
        edge: &Edge,
        event_id: &str,
        payload: &Value,
        route: Option<String>,
        now: u64,
        fx: &mut Vec<Effect>,
    ) {
        let s = &self.sessions[sid];
        let Some(rev) = self.rev_of(s) else { return };
        let invocation_id = format!("{sid}/i{}", s.next_invocation);
        let stamp = rev
            .plugins
            .get(&edge.to)
            .map(|m| m.stamp.clone())
            .unwrap_or_default();
        let sub = rev.subscription(edge).cloned();
        let delivered = sub
            .as_ref()
            .map_or_else(|| json!({}), |sub| project(payload, sub));
        let position = edge.position.as_ref().map_or(0, |p| p.chain);
        self.emit(
            sid,
            now,
            Body::InvocationStarted {
                invocation_id: invocation_id.clone(),
                event_id: event_id.to_owned(),
                plugin: edge.to.clone(),
                mode: edge.mode,
                subscription: edge.subscription.clone(),
                position,
                phase: edge.position.as_ref().map(|p| p.phase.clone()),
                stamp: stamp.clone(),
                revision: rev.revision.clone(),
                payload: delivered.clone(),
                route,
            },
            fx,
        );
        // Runtime enforcement on delivery: the declared keys must be present and well-shaped.
        if let Some(sub) = &sub
            && let Err(e) = check_keys(
                payload.as_object().unwrap_or(&Map::new()),
                &sub.receives.keys,
                "",
            )
        {
            let reason = format!(
                "delivery to `{}` contradicts its `receives` on `{}`: {e}",
                edge.to, edge.event
            );
            self.breach(
                sid,
                now,
                BreachInfo {
                    event_id: Some(event_id.to_owned()),
                    invocation_id: Some(invocation_id.clone()),
                    plugin: edge.to.clone(),
                    declaration: json!({ "kind": "receives", "plugin": edge.to, "subscription": edge.subscription, "event": edge.event, "key": e.path, "keys": sub.receives.keys }),
                    observed: payload.clone(),
                    reason: reason.clone(),
                    reaction: "invocation-failed",
                },
                fx,
            );
            // Recorded as a failed invocation; the dispatch loop moves on.
            self.emit(
                sid,
                now,
                Body::InvocationCompleted {
                    invocation_id: invocation_id.clone(),
                    outcome: Outcome::Failed {
                        error: reason.clone(),
                    },
                    returns: None,
                    contributions: Vec::new(),
                    published: Vec::new(),
                },
                fx,
            );
            if edge.mode == Mode::Blocking {
                self.emit(
                    sid,
                    now,
                    Body::PipelineSettled {
                        event_id: event_id.to_owned(),
                        settlement: Settlement::Failed {
                            plugin: edge.to.clone(),
                            error: reason,
                        },
                    },
                    fx,
                );
            }
            return;
        }
        let s = &self.sessions[sid];
        fx.push(Effect::Invoke {
            plugin: edge.to.clone(),
            stamp,
            attempt: 1,
            request: InvokeRequest {
                invocation_id,
                mode: edge.mode,
                position,
                attempt: 1,
                subscription: edge.subscription.clone(),
                event: Self::envelope(s, event_id, delivered, &edge.slots, None),
            },
        });
    }

    fn apply_pending_config(&mut self, sid: &str, now: u64, fx: &mut Vec<Effect>) {
        let Some((hash, scoped)) = self.sessions[sid].pending.clone() else {
            return;
        };
        let previous = self.sessions[sid].revision.clone();
        if hash == previous {
            if let Some(s) = self.sessions.get_mut(sid) {
                s.pending = None;
                s.session_scoped = scoped;
            }
            return;
        }
        self.emit(
            sid,
            now,
            Body::ConfigApplied {
                revision: hash.clone(),
                previous: previous.clone(),
                scoped,
            },
            fx,
        );
        self.append_event(
            sid,
            now,
            CONFIG_APPLIED,
            json!({ "revision": hash, "previous": previous }),
            None,
            Lane::Interrupt,
            Cause::Core {
                reason: CONFIG_APPLIED.into(),
            },
            Origin::Core,
            None,
            fx,
        );
    }

    /// Drive a session forward until it waits on a plugin or runs out of work.
    fn advance(&mut self, sid: &str, now: u64, fx: &mut Vec<Effect>) {
        loop {
            let Some(s) = self.sessions.get(sid) else {
                return;
            };
            if let Some(run) = s.run.clone() {
                if run.open.is_some() {
                    return;
                }
                let ev = s.events[&run.event_id].record.clone();
                let route = self
                    .rev_of(s)
                    .and_then(|rev| Self::route_edges(&rev, &ev))
                    .unwrap_or_default();
                if let Some(edge) = route.blocking.get(run.next_blocking) {
                    let edge = edge.clone();
                    self.start_invocation(sid, &edge, &run.event_id, &run.payload, None, now, fx);
                    if self
                        .sessions
                        .get(sid)
                        .is_some_and(|s| s.run.as_ref().is_some_and(|r| r.open.is_some()))
                    {
                        return;
                    }
                    continue;
                }
                self.emit(
                    sid,
                    now,
                    Body::PipelineSettled {
                        event_id: run.event_id.clone(),
                        settlement: Settlement::Delivered {
                            payload: run.payload.clone(),
                        },
                    },
                    fx,
                );
                for edge in &route.asyncs {
                    self.start_invocation(sid, edge, &run.event_id, &run.payload, None, now, fx);
                }
                // Family handler: exactly the one compiled handler, never a scan.
                if let Some(edge) = &route.handler {
                    self.start_invocation(
                        sid,
                        edge,
                        &run.event_id,
                        &run.payload,
                        Some(member_of(&ev.event_name, &route.family)),
                        now,
                        fx,
                    );
                } else if route.exactly_one_handler {
                    let handled: Vec<String> = self
                        .rev_of(&self.sessions[sid])
                        .and_then(|r| r.report.families.get(&route.family).cloned())
                        .map(|f| {
                            f.members
                                .keys()
                                .chain(f.subfamilies.keys())
                                .map(|m| member_of(m, &route.family))
                                .collect()
                        })
                        .unwrap_or_default();
                    self.append_event(
                        sid,
                        now,
                        DISPATCH_FAILED,
                        json!({
                            "reason": "no-handler", "event_id": run.event_id, "event_name": ev.event_name,
                            "family": route.family, "member": member_of(&ev.event_name, &route.family),
                            "payload": run.payload, "handled": handled,
                        }),
                        None,
                        Lane::Normal,
                        Cause::Core {
                            reason: DISPATCH_FAILED.into(),
                        },
                        Origin::Core,
                        None,
                        fx,
                    );
                }
                continue;
            }
            if s.pending.is_some() {
                self.apply_pending_config(sid, now, fx);
                continue;
            }
            if s.state != DispatchState::Running {
                return;
            }
            let Some(next) = s.interrupt.front().or_else(|| s.normal.front()).cloned() else {
                return;
            };
            self.emit(sid, now, Body::PipelineStarted { event_id: next }, fx);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_event(
        &mut self,
        sid: &str,
        now: u64,
        name: &str,
        payload: Value,
        ui: Option<Value>,
        lane: Lane,
        cause: Cause,
        origin: Origin,
        id: Option<String>,
        fx: &mut Vec<Effect>,
    ) -> String {
        let seq = self.sessions[sid].next_sequence;
        let event_id = id.unwrap_or_else(|| format!("{sid}/e{seq}"));
        let event = EventRecord {
            event_id: event_id.clone(),
            event_name: name.to_owned(),
            lane,
            cause,
            origin,
            payload,
            ui,
        };
        self.emit(sid, now, Body::EventAppended { event }, fx);
        event_id
    }

    // ------------------------------------------------------------------
    // Public operations.
    // ------------------------------------------------------------------

    /// Create a new session. Every start compiles its definition first; a
    /// failed compile creates no session and returns the diagnostics (as a
    /// `session-start-failed` event to the calling invocation's session too).
    pub fn start_session(&mut self, req: &StartRequest, now: u64) -> StartOutcome {
        let fail = |m: String| StartOutcome {
            error: Some(m),
            ..Default::default()
        };
        let Some(config) = self.configs.get(&self.active).cloned() else {
            return fail("no active config".into());
        };
        let is_core = req.plugin == CORE_PLUGIN;
        if !is_core {
            let Some(m) = config.manifest(&req.plugin) else {
                return fail(format!("unknown plugin `{}`", req.plugin));
            };
            if !m.has(Capability::StartSession) {
                return fail(format!(
                    "`{}` lacks the start-session capability",
                    req.plugin
                ));
            }
        }
        // Cause: the starting invocation, or a true root.
        let cause = match &req.invocation_id {
            Some(inv) => {
                let sid = session_of(inv).unwrap_or_default();
                let Some(src) = self.sessions.get(sid) else {
                    return fail(format!("session `{sid}` is not loaded"));
                };
                let Some(info) = src.invocations.get(inv) else {
                    return fail(format!("unknown invocation `{inv}`"));
                };
                if info.plugin != req.plugin {
                    return fail(format!(
                        "invocation `{inv}` was not dispatched to `{}`",
                        req.plugin
                    ));
                }
                Cause::Invocation {
                    invocation_id: inv.clone(),
                }
            }
            None => Cause::Root {
                plugin: req.plugin.clone(),
            },
        };
        let mut out = StartOutcome::default();
        // Compile (a cached result for a named definition: identical inputs, identical revision).
        let (rev, fresh) = match &req.spec {
            Some(spec) => {
                let name = format!("inline:{}", &hash_json(&json!(spec))[..12]);
                let r = compile_definition(&name, spec, &config.config, &config.manifests);
                let fresh = !self.revisions.contains_key(&r.revision);
                (Arc::new(r), fresh)
            }
            None => match config.definitions.get(&req.definition) {
                Some(r) => (
                    self.revisions
                        .get(&r.revision)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(r.clone())),
                    false,
                ),
                None => return fail(format!("unknown session definition `{}`", req.definition)),
            },
        };
        if !rev.ok {
            let errors: Vec<String> = rev
                .errors()
                .iter()
                .map(|d| format!("{} [{}]", d.message, d.code))
                .collect();
            let error = format!(
                "definition `{}` failed to compile: {}",
                rev.name,
                errors.join("; ")
            );
            out.error = Some(error.clone());
            out.diagnostics = rev.diagnostics.clone();
            if let Some(inv) = &req.invocation_id {
                let caller = session_of(inv).unwrap_or_default().to_owned();
                self.append_event(
                    &caller,
                    now,
                    SESSION_START_FAILED,
                    json!({ "definition": rev.name, "error": error, "diagnostics": rev.diagnostics }),
                    None,
                    Lane::Normal,
                    Cause::Invocation { invocation_id: inv.clone() },
                    Origin::Core,
                    None,
                    &mut out.effects,
                );
                self.advance(&caller, now, &mut out.effects);
            }
            return out;
        }
        if let Some(init) = &req.initial
            && !is_core
        {
            let Some(decl) = rev.emit_decl(&req.plugin, &init.event_name) else {
                return fail(format!(
                    "`{}` does not declare `{}` in its emits",
                    req.plugin, init.event_name
                ));
            };
            if let Err(e) = init
                .payload
                .as_object()
                .ok_or_else(|| "payload must be a JSON object".to_owned())
                .and_then(|o| check_keys(o, &decl.keys, "").map_err(|e| e.to_string()))
            {
                return fail(format!(
                    "initial `{}` contradicts `{}`'s emit declaration: {e}",
                    init.event_name, req.plugin
                ));
            }
        }
        let seeded = match &req.fork_from {
            Some(r) => match self.sessions.get(&r.session_id) {
                Some(src) => Some(src.context.at(r.sequence).to_vec()),
                None => return fail(format!("fork source `{}` is not loaded", r.session_id)),
            },
            None => None,
        };
        if fresh {
            out.effects.push(Effect::SaveRevision {
                revision: Box::new((*rev).clone()),
            });
        }
        self.revisions
            .entry(rev.revision.clone())
            .or_insert_with(|| rev.clone());
        let sid = format!("s{:04}", self.next_session);
        let fx = &mut out.effects;
        self.emit(
            &sid,
            now,
            Body::SessionCreated {
                definition: rev.name.clone(),
                revision: rev.revision.clone(),
                cause: cause.clone(),
                fork_of: req.fork_from.clone(),
            },
            fx,
        );
        if let (Some(items), Some(from)) = (seeded, &req.fork_from) {
            self.emit(
                &sid,
                now,
                Body::ContextSeeded {
                    from: from.clone(),
                    items,
                },
                fx,
            );
        }
        let mut started = json!({ "definition": rev.name, "revision": rev.revision });
        if let Some(f) = &req.fork_from {
            started["fork_of"] = json!(f);
        }
        if let Some(inv) = &req.invocation_id {
            started["parent_session"] = json!(session_of(inv));
        }
        self.append_event(
            &sid,
            now,
            SESSION_STARTED,
            started,
            None,
            Lane::Normal,
            cause.clone(),
            Origin::Core,
            None,
            fx,
        );
        if let Some(init) = &req.initial {
            self.append_event(
                &sid,
                now,
                &init.event_name,
                init.payload.clone(),
                init.ui.clone(),
                Lane::Normal,
                cause,
                Origin::Start {
                    plugin: req.plugin.clone(),
                },
                None,
                fx,
            );
        }
        self.advance(&sid, now, fx);
        out.session_id = Some(sid);
        out.revision = Some(rev.revision.clone());
        out
    }

    fn block(
        &mut self,
        session: Option<&str>,
        req: &PublishRequest,
        reason: String,
        now: u64,
    ) -> PublishOutcome {
        let mut fx = Vec::new();
        if let Some(sid) = session.filter(|s| self.sessions.contains_key(*s)) {
            self.emit(
                sid,
                now,
                Body::PublishBlocked {
                    plugin: req.plugin.clone(),
                    event_name: req.event_name.clone(),
                    invocation_id: req.invocation_id.clone(),
                    reason: reason.clone(),
                },
                &mut fx,
            );
        }
        PublishOutcome {
            blocked: Some(reason),
            effects: fx,
            ..Default::default()
        }
    }

    /// A publish that contradicts a declaration: a recorded breach; blocked.
    fn breach_block(
        &mut self,
        sid: &str,
        req: &PublishRequest,
        declaration: Value,
        reason: String,
        now: u64,
    ) -> PublishOutcome {
        let mut fx = Vec::new();
        self.breach(
            sid,
            now,
            BreachInfo {
                event_id: None,
                invocation_id: req.invocation_id.clone().or_else(|| req.cite.clone()),
                plugin: req.plugin.clone(),
                declaration,
                observed: json!({ "event_name": req.event_name, "payload": req.payload, "lane": req.lane, "target_session": req.target_session }),
                reason: reason.clone(),
                reaction: "publish-blocked",
            },
            &mut fx,
        );
        PublishOutcome {
            blocked: Some(reason),
            effects: fx,
            ..Default::default()
        }
    }

    /// Classify, check, and record a publish.
    #[allow(clippy::too_many_lines)]
    pub fn publish(&mut self, req: &PublishRequest, now: u64) -> PublishOutcome {
        for sid in req.sessions() {
            if !self.sessions.contains_key(&sid) {
                return PublishOutcome {
                    blocked: Some(format!("session `{sid}` is not loaded")),
                    ..Default::default()
                };
            }
        }
        // Classify by invocation id.
        let open = req.invocation_id.as_ref().and_then(|inv| {
            let s = self.sessions.get(session_of(inv)?)?;
            let i = s.invocations.get(inv)?;
            (i.open && i.plugin == req.plugin).then(|| inv.clone())
        });
        let (source_inv, deferred) = match (&open, &req.invocation_id, &req.cite) {
            (Some(inv), _, _) => (inv.clone(), false),
            (None, _, Some(cite)) => (cite.clone(), true),
            (None, Some(inv), None) => (inv.clone(), true),
            (None, None, None) => {
                return self.block(req.target_session.as_deref(), req, "no open invocation and no citation: a deferred publish must cite a prior invocation dispatched to this plugin".into(), now);
            }
        };
        let source = session_of(&source_inv).unwrap_or_default().to_owned();
        let Some(src) = self.sessions.get(&source) else {
            return self.block(
                None,
                req,
                format!("unknown session for `{source_inv}`"),
                now,
            );
        };
        let Some(info) = src.invocations.get(&source_inv).cloned() else {
            return self.block(
                Some(&source),
                req,
                format!("uncitable: invocation `{source_inv}` is not in the record"),
                now,
            );
        };
        if info.plugin != req.plugin {
            return self.block(
                Some(&source),
                req,
                format!(
                    "uncitable: `{source_inv}` was dispatched to `{}`, not `{}`",
                    info.plugin, req.plugin
                ),
                now,
            );
        }
        let Some(rev) = self.rev_of(src) else {
            return self.block(Some(&source), req, "session revision missing".into(), now);
        };
        let Some(manifest) = rev.manifest(&req.plugin).cloned() else {
            return self.block(
                Some(&source),
                req,
                format!("`{}` is not in this session's revision", req.plugin),
                now,
            );
        };
        if !req.payload.is_object() {
            return self.breach_block(
                &source,
                req,
                json!({ "kind": "emit", "plugin": req.plugin, "event": req.event_name }),
                "payload must be a JSON object".into(),
                now,
            );
        }
        if req.event_name.contains('*') {
            return self.breach_block(
                &source,
                req,
                json!({ "kind": "emit", "plugin": req.plugin, "event": req.event_name }),
                format!("`{}` is a pattern, not an event name", req.event_name),
                now,
            );
        }
        if let Some(ui) = &req.ui
            && !ui.get("kind").is_some_and(Value::is_string)
        {
            return self.block(
                Some(&source),
                req,
                "ui hint must be an object with a string `kind`".into(),
                now,
            );
        }
        let target = req.target_session.clone().unwrap_or_else(|| source.clone());
        if target != source {
            return self.publish_cross(
                req,
                &source,
                &source_inv,
                &info,
                &target,
                &manifest,
                deferred,
                now,
            );
        }
        // An event of this session: the emit declaration decides everything.
        let Some(decl) = manifest.emit_for(&req.event_name).cloned() else {
            let declared: Vec<&str> = manifest.emit.iter().map(|e| e.event.as_str()).collect();
            return self.breach_block(&source, req, json!({ "kind": "emit", "plugin": req.plugin, "event": req.event_name, "declared": declared }), format!("undeclared emit `{}` (declared: {})", req.event_name, declared.join(", ")), now);
        };
        let decl_json = json!({ "kind": "emit", "plugin": req.plugin, "event": decl.event, "lane": decl.lane, "deferred": decl.deferred, "keys": decl.keys });
        if deferred && !(decl.deferred && manifest.has(Capability::DeferredPublish)) {
            return self.breach_block(&source, req, decl_json, format!("`{}` may not be published deferred (invocation closed or absent; the emit is not declared `deferred`)", req.event_name), now);
        }
        let lane = req.lane.unwrap_or(decl.lane);
        if lane != decl.lane {
            return self.breach_block(
                &source,
                req,
                decl_json,
                format!(
                    "`{}` is declared on the {:?} lane but was published on the {lane:?} lane",
                    decl.event, decl.lane
                ),
                now,
            );
        }
        if let Err(e) = check_keys(
            req.payload.as_object().unwrap_or(&Map::new()),
            &decl.keys,
            "",
        ) {
            return self.breach_block(
                &source,
                req,
                decl_json,
                format!(
                    "`{}` contradicts `{}`'s emit declaration: {e}",
                    req.event_name, req.plugin
                ),
                now,
            );
        }
        let mut out = PublishOutcome::default();
        let id = if deferred {
            None
        } else {
            Some(format!("{source_inv}.o{}", info.attempt_outputs))
        };
        if let Some(id) = &id {
            if let Some(s) = self.sessions.get_mut(&source)
                && let Some(i) = s.invocations.get_mut(&source_inv)
            {
                i.attempt_outputs += 1;
            }
            if self.sessions[&source].events.contains_key(id) {
                out.event_id = Some(id.clone());
                out.duplicate = true;
                return out;
            }
        }
        let origin = if deferred {
            Origin::Deferred {
                plugin: req.plugin.clone(),
                cites: source_inv.clone(),
            }
        } else {
            Origin::Pipeline {
                plugin: req.plugin.clone(),
                invocation_id: source_inv.clone(),
            }
        };
        let fx = &mut out.effects;
        let event_id = self.append_event(
            &source,
            now,
            &req.event_name,
            req.payload.clone(),
            req.ui.clone(),
            lane,
            Cause::Invocation {
                invocation_id: source_inv.clone(),
            },
            origin,
            id,
            fx,
        );
        self.advance(&source, now, fx);
        out.event_id = Some(event_id);
        out
    }

    /// A cross-session publish: the sender's `outbound` declaration, paired at
    /// publish time against the target's compiled `inbound` declaration.
    #[allow(clippy::too_many_arguments)]
    fn publish_cross(
        &mut self,
        req: &PublishRequest,
        source: &str,
        source_inv: &str,
        info: &InvInfo,
        target: &str,
        manifest: &Manifest,
        deferred: bool,
        now: u64,
    ) -> PublishOutcome {
        let Some(out_decl) = manifest.outbound_for(&req.event_name).cloned() else {
            let declared: Vec<&str> = manifest.outbound.iter().map(|o| o.event.as_str()).collect();
            return self.breach_block(source, req, json!({ "kind": "outbound", "plugin": req.plugin, "event": req.event_name, "declared": declared }), format!("undeclared outbound `{}` to `{target}`", req.event_name), now);
        };
        let decl_json = json!({ "kind": "outbound", "plugin": req.plugin, "event": out_decl.event, "keys": out_decl.keys });
        if !manifest.has(Capability::CrossSession) {
            return self.breach_block(
                source,
                req,
                decl_json,
                format!("targeting `{target}` requires the cross-session capability"),
                now,
            );
        }
        if deferred && !manifest.has(Capability::DeferredPublish) {
            return self.breach_block(
                source,
                req,
                decl_json,
                format!(
                    "`{}` may not be sent deferred without the deferred-publish capability",
                    req.event_name
                ),
                now,
            );
        }
        if let Err(e) = check_keys(
            req.payload.as_object().unwrap_or(&Map::new()),
            &out_decl.keys,
            "",
        ) {
            return self.breach_block(
                source,
                req,
                decl_json,
                format!(
                    "`{}` contradicts `{}`'s outbound declaration: {e}",
                    req.event_name, req.plugin
                ),
                now,
            );
        }
        // Pairing against the target: a mismatch fails the publish back to the
        // sender (recorded in the sender's log) without stopping its session.
        let Some(tgt) = self.sessions.get(target) else {
            return self.block(
                Some(source),
                req,
                format!("unknown target session `{target}`"),
                now,
            );
        };
        let Some(trev) = self.rev_of(tgt) else {
            return self.block(
                Some(source),
                req,
                format!("target `{target}` has no revision"),
                now,
            );
        };
        let Some(inbound) = trev.definition.inbound.get(&req.event_name).cloned() else {
            return self.block(Some(source), req, format!("cross-session pairing failed: `{target}` (definition `{}`, revision {}) does not accept `{}` (inbound: {})", trev.name, trev.revision, req.event_name, trev.definition.inbound.keys().cloned().collect::<Vec<_>>().join(", ")), now);
        };
        if let Err(mm) = keys_compatible(&out_decl.keys, &inbound, "") {
            return self.block(Some(source), req, format!("cross-session pairing failed: `{}`'s outbound `{}` does not satisfy `{target}`'s inbound declaration: {mm}", req.plugin, req.event_name), now);
        }
        if let Err(e) = check_keys(req.payload.as_object().unwrap_or(&Map::new()), &inbound, "") {
            return self.block(Some(source), req, format!("cross-session pairing failed: the payload contradicts `{target}`'s inbound `{}`: {e}", req.event_name), now);
        }
        let mut out = PublishOutcome::default();
        let id = (!deferred).then(|| format!("{source_inv}.o{}", info.attempt_outputs));
        if let Some(id) = &id {
            if let Some(s) = self.sessions.get_mut(source)
                && let Some(i) = s.invocations.get_mut(source_inv)
            {
                i.attempt_outputs += 1;
            }
            if self.sessions[target].events.contains_key(id) {
                out.event_id = Some(id.clone());
                out.duplicate = true;
                return out;
            }
        }
        let fx = &mut out.effects;
        let event_id = self.append_event(
            target,
            now,
            &req.event_name,
            req.payload.clone(),
            req.ui.clone(),
            Lane::Normal,
            Cause::Invocation {
                invocation_id: source_inv.to_owned(),
            },
            Origin::CrossSession {
                from_session: source.to_owned(),
                plugin: req.plugin.clone(),
            },
            id,
            fx,
        );
        self.emit(
            source,
            now,
            Body::CrossSessionSent {
                event_id: event_id.clone(),
                event_name: req.event_name.clone(),
                target_session: target.to_owned(),
            },
            fx,
        );
        self.advance(target, now, fx);
        out.event_id = Some(event_id);
        out
    }

    /// Check a plugin's answer against the compiled declarations.
    fn check_result(
        &self,
        sid: &str,
        info: &InvInfo,
        result: &InvocationResult,
    ) -> Result<Option<Value>, (String, Value, Value)> {
        let s = &self.sessions[sid];
        let rev = self
            .rev_of(s)
            .ok_or_else(|| ("revision missing".to_owned(), Value::Null, Value::Null))?;
        let manifest = rev.manifest(&info.plugin);
        let sub = manifest.and_then(|m| {
            m.subscribe
                .iter()
                .find(|x| x.event == info.subscription && x.mode == info.mode)
        });
        let Some(sub) = sub else {
            // The invocation's subscription is not in the revision (it changed
            // mid-flight): nothing to hold it to beyond read-only behaviour.
            if result.veto.is_some() || result.returns.is_some() || !result.contributions.is_empty()
            {
                return Err((
                    "the subscription this invocation ran under is not in the session's revision"
                        .into(),
                    json!({ "kind": "subscription", "plugin": info.plugin, "subscription": info.subscription }),
                    json!(result),
                ));
            }
            return Ok(None);
        };
        let decl = |kind: &str, extra: Value| {
            let mut d = json!({ "kind": kind, "plugin": info.plugin, "subscription": sub.event, "mode": sub.mode, "actions": sub.actions });
            if let (Some(o), Some(e)) = (d.as_object_mut(), extra.as_object()) {
                o.extend(e.clone());
            }
            d
        };
        if result.veto.is_some() && !(info.mode == Mode::Blocking && sub.has_action(ACTION_VETO)) {
            return Err((
                format!(
                    "`{}` vetoed `{}` without declaring the `veto` action{}",
                    info.plugin,
                    sub.event,
                    if info.mode == Mode::Async {
                        " (async subscribers cannot veto)"
                    } else {
                        ""
                    }
                ),
                decl("actions", json!({})),
                json!({ "veto": result.veto }),
            ));
        }
        if !result.contributions.is_empty() && !sub.has_action(ACTION_CONTRIBUTE) {
            return Err((
                format!(
                    "`{}` contributed context on `{}` without declaring the `contribute-context` action",
                    info.plugin, sub.event
                ),
                decl("actions", json!({})),
                json!({ "contributions": result.contributions }),
            ));
        }
        let Some(returned) = &result.returns else {
            if info.mode == Mode::Blocking
                && sub.returns.iter().any(|(_, d)| !d.optional)
                && result.veto.is_none()
            {
                let missing: Vec<&String> = sub
                    .returns
                    .iter()
                    .filter(|(_, d)| !d.optional)
                    .map(|(k, _)| k)
                    .collect();
                return Err((
                    format!(
                        "`{}` returned nothing on `{}`, but its `returns` requires [{}]",
                        info.plugin,
                        sub.event,
                        missing
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    decl("returns", json!({ "returns": sub.returns })),
                    Value::Null,
                ));
            }
            return Ok(None);
        };
        if info.mode == Mode::Async {
            return Err((
                format!(
                    "`{}` is an async subscriber of `{}` and returned keys; async subscribers are read-only",
                    info.plugin, sub.event
                ),
                decl("returns", json!({ "returns": sub.returns })),
                json!(returned),
            ));
        }
        if result.veto.is_some() {
            return Ok(None);
        }
        let current = s
            .run
            .as_ref()
            .map(|r| r.payload.clone())
            .unwrap_or(Value::Null);
        let current = current.as_object().cloned().unwrap_or_default();
        // Keys the emitter declared (the edge's supply) are not "unlisted".
        let supplied: Keys = rev
            .route(
                &emitter_of(&s.events[&info.event_id].record.origin),
                &s.events[&info.event_id].record.event_name,
            )
            .and_then(|(_, r)| {
                r.and_then(|r| {
                    r.blocking
                        .iter()
                        .map(|i| rev.graph.edge(*i))
                        .find(|e| e.to == info.plugin)
                        .map(|e| e.supply.clone())
                })
            })
            .unwrap_or_default();
        for (k, v) in returned {
            match sub.returns.get(k) {
                Some(d) => {
                    if let Err(e) = check_value(v, &d.shape, k) {
                        return Err((
                            format!(
                                "`{}` returned `{k}` on `{}` contradicting its `returns`: {e}",
                                info.plugin, sub.event
                            ),
                            decl("returns", json!({ "key": k, "shape": d })),
                            v.clone(),
                        ));
                    }
                }
                None => {
                    let unlisted = !supplied.contains_key(k) && !sub.receives.keys.contains_key(k);
                    match current.get(k) {
                        Some(old) if sub.unlisted_keys && unlisted => {
                            if kind_of(old) != kind_of(v) {
                                return Err((
                                    format!(
                                        "`{}` rewrote unlisted key `{k}` on `{}` from {} to {}; a return may not change a key's kind",
                                        info.plugin,
                                        sub.event,
                                        kind_of(old),
                                        kind_of(v)
                                    ),
                                    decl("returns", json!({ "key": k, "unlisted_keys": true })),
                                    v.clone(),
                                ));
                            }
                        }
                        _ => {
                            return Err((
                                format!(
                                    "`{}` returned `{k}` on `{}`, which its `returns` does not declare",
                                    info.plugin, sub.event
                                ),
                                decl("returns", json!({ "returns": sub.returns })),
                                json!({ k.clone(): v }),
                            ));
                        }
                    }
                }
            }
        }
        for (k, d) in &sub.returns {
            if !d.optional && !returned.contains_key(k) {
                return Err((
                    format!(
                        "`{}` did not return required `{k}` on `{}`",
                        info.plugin, sub.event
                    ),
                    decl("returns", json!({ "key": k, "shape": d })),
                    json!(returned),
                ));
            }
        }
        Ok(Some(Value::Object(returned.clone())))
    }

    /// Record an invocation's completion and advance its session.
    pub fn complete(
        &mut self,
        invocation_id: &str,
        result: &InvocationResult,
        now: u64,
    ) -> Vec<Effect> {
        let mut fx = Vec::new();
        let Some(sid) = session_of(invocation_id).map(str::to_owned) else {
            return fx;
        };
        let Some(s) = self.sessions.get(&sid) else {
            return fx;
        };
        let Some(info) = s.invocations.get(invocation_id).cloned() else {
            return fx;
        };
        if !info.open {
            return fx; // late completion after cancel: already settled.
        }
        let mut returns = None;
        let outcome = if let Some(e) = &result.error {
            Outcome::Failed { error: e.clone() }
        } else {
            match self.check_result(&sid, &info, result) {
                Err((reason, declaration, observed)) => {
                    self.breach(
                        &sid,
                        now,
                        BreachInfo {
                            event_id: Some(info.event_id.clone()),
                            invocation_id: Some(invocation_id.to_owned()),
                            plugin: info.plugin.clone(),
                            declaration,
                            observed,
                            reason: reason.clone(),
                            reaction: "invocation-failed",
                        },
                        &mut fx,
                    );
                    Outcome::Failed { error: reason }
                }
                Ok(r) => {
                    if let Some(reason) = &result.veto {
                        Outcome::Veto {
                            reason: reason.clone(),
                        }
                    } else {
                        returns = r;
                        Outcome::Ok
                    }
                }
            }
        };
        let contributions = if outcome == Outcome::Ok {
            result.contributions.clone()
        } else {
            Vec::new()
        };
        fx.extend(self.finish(&sid, invocation_id, outcome, returns, contributions, now));
        fx
    }

    /// Record a completion (already checked) and its consequences.
    fn finish(
        &mut self,
        sid: &str,
        invocation_id: &str,
        outcome: Outcome,
        returns: Option<Value>,
        contributions: Vec<ContextOp>,
        now: u64,
    ) -> Vec<Effect> {
        let mut fx = Vec::new();
        let Some(info) = self.sessions[sid].invocations.get(invocation_id).cloned() else {
            return fx;
        };
        self.emit(
            sid,
            now,
            Body::InvocationCompleted {
                invocation_id: invocation_id.to_owned(),
                outcome: outcome.clone(),
                returns,
                contributions,
                published: info.outputs.clone(),
            },
            &mut fx,
        );
        if let (Some(member), Outcome::Failed { error }) = (&info.route, &outcome) {
            let ev = self.sessions[sid].events[&info.event_id].record.clone();
            let family = self
                .rev_of(&self.sessions[sid])
                .and_then(|r| r.emit_decl(&emitter_of(&ev.origin), &ev.event_name))
                .map(|d| d.event)
                .unwrap_or_default();
            self.append_event(
                sid,
                now,
                DISPATCH_FAILED,
                json!({
                    "reason": "handler-failed", "event_id": info.event_id, "event_name": ev.event_name,
                    "family": family, "member": member, "payload": ev.payload,
                    "plugin": info.plugin, "error": error,
                }),
                None,
                Lane::Normal,
                Cause::Core {
                    reason: DISPATCH_FAILED.into(),
                },
                Origin::Core,
                None,
                &mut fx,
            );
        }
        if info.mode == Mode::Blocking {
            let settlement = match outcome {
                Outcome::Veto { reason } => Some(Settlement::Vetoed {
                    plugin: info.plugin.clone(),
                    reason,
                }),
                Outcome::Failed { error } => Some(Settlement::Failed {
                    plugin: info.plugin.clone(),
                    error,
                }),
                Outcome::Ok | Outcome::Cancelled { .. } => None,
            };
            if let Some(settlement) = settlement {
                self.emit(
                    sid,
                    now,
                    Body::PipelineSettled {
                        event_id: info.event_id.clone(),
                        settlement,
                    },
                    &mut fx,
                );
            }
        }
        self.advance(sid, now, &mut fx);
        fx
    }

    /// Restart an open invocation (plugin crash, orphan after restart).
    pub fn retry(
        &mut self,
        invocation_id: &str,
        reason: &str,
        max_attempts: u32,
        now: u64,
    ) -> Vec<Effect> {
        let mut fx = Vec::new();
        let Some(sid) = session_of(invocation_id).map(str::to_owned) else {
            return fx;
        };
        let Some(s) = self.sessions.get(&sid) else {
            return fx;
        };
        let Some(info) = s.invocations.get(invocation_id).cloned() else {
            return fx;
        };
        if !info.open {
            return fx;
        }
        let attempt = info.attempt + 1;
        if attempt > max_attempts {
            let result = InvocationResult {
                error: Some(format!("gave up after {} attempts: {reason}", info.attempt)),
                ..Default::default()
            };
            return self.complete(invocation_id, &result, now);
        }
        self.emit(
            &sid,
            now,
            Body::InvocationRetried {
                invocation_id: invocation_id.to_owned(),
                attempt,
                reason: reason.to_owned(),
            },
            &mut fx,
        );
        let s = &self.sessions[&sid];
        fx.push(Effect::Invoke {
            plugin: info.plugin.clone(),
            stamp: info.stamp.clone(),
            attempt,
            request: InvokeRequest {
                invocation_id: invocation_id.to_owned(),
                mode: info.mode,
                position: info.position,
                attempt,
                subscription: info.subscription.clone(),
                event: Self::envelope(
                    s,
                    &info.event_id,
                    info.payload.clone(),
                    &info.slots,
                    Some(info.start_sequence.saturating_sub(1)),
                ),
            },
        });
        fx
    }

    /// Apply a dispatcher command. Commands never queue; they take effect now.
    ///
    /// # Errors
    /// Fails for an unknown or unloaded session.
    pub fn command(
        &mut self,
        session_id: &str,
        command: Command,
        by: &str,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let s = self
            .sessions
            .get(session_id)
            .ok_or_else(|| format!("session `{session_id}` is not loaded"))?;
        let mut fx = Vec::new();
        match command {
            Command::SoftStop => {
                let state = if s.state == DispatchState::Running {
                    DispatchState::Draining
                } else {
                    s.state
                };
                self.emit(
                    session_id,
                    now,
                    Body::DispatcherCommand {
                        command,
                        by: by.to_owned(),
                        state,
                    },
                    &mut fx,
                );
            }
            Command::HardStop => {
                let mut open: Vec<String> = s
                    .run
                    .as_ref()
                    .and_then(|r| r.open.clone())
                    .into_iter()
                    .collect();
                open.extend(s.open_async.iter().cloned());
                let run = s.run.clone();
                self.emit(
                    session_id,
                    now,
                    Body::DispatcherCommand {
                        command,
                        by: by.to_owned(),
                        state: DispatchState::Halted,
                    },
                    &mut fx,
                );
                for inv in open {
                    let info = self.sessions[session_id].invocations[&inv].clone();
                    fx.push(Effect::Cancel {
                        plugin: info.plugin.clone(),
                        stamp: info.stamp.clone(),
                        invocation_id: inv.clone(),
                    });
                    self.emit(
                        session_id,
                        now,
                        Body::InvocationCompleted {
                            invocation_id: inv,
                            outcome: Outcome::Cancelled {
                                reason: format!("hard stop by {by}"),
                            },
                            returns: None,
                            contributions: vec![],
                            published: info.outputs,
                        },
                        &mut fx,
                    );
                }
                if let Some(run) = run {
                    self.emit(
                        session_id,
                        now,
                        Body::PipelineSettled {
                            event_id: run.event_id,
                            settlement: Settlement::Aborted {
                                reason: format!("hard stop by {by}"),
                            },
                        },
                        &mut fx,
                    );
                }
            }
            Command::Resume => {
                self.emit(
                    session_id,
                    now,
                    Body::DispatcherCommand {
                        command,
                        by: by.to_owned(),
                        state: DispatchState::Running,
                    },
                    &mut fx,
                );
                self.advance(session_id, now, &mut fx);
            }
        }
        Ok(fx)
    }

    /// Apply an installed config to running sessions. The compiler builds each
    /// target's candidate revision from scratch; any error rejects the whole
    /// apply and every target keeps its revision. Targets switch at their next
    /// event boundary.
    pub fn apply_config(&mut self, hash: &str, scope: &Scope, now: u64) -> ApplyOutcome {
        let mut out = ApplyOutcome::default();
        let Some(config) = self.configs.get(hash).cloned() else {
            out.error = Some(format!("config {hash} is not installed"));
            return out;
        };
        let targets: Vec<String> = match scope {
            Scope::Global => self
                .sessions
                .iter()
                .filter(|(_, s)| !s.session_scoped)
                .map(|(id, _)| id.clone())
                .collect(),
            Scope::Session { session_id } => vec![session_id.clone()],
            Scope::Sessions { session_ids } => session_ids.clone(),
        };
        let mut candidates: BTreeMap<String, Arc<Revision>> = BTreeMap::new();
        let mut errors = Vec::new();
        for sid in &targets {
            let Some(s) = self.sessions.get(sid) else {
                errors.push(format!("session `{sid}` is not loaded"));
                continue;
            };
            let rev = match config.definitions.get(&s.definition) {
                Some(r) => Arc::new(r.clone()),
                None => match self.rev_of(s) {
                    // A definition passed to start-session: recompile it against the new config.
                    Some(cur) if cur.name.starts_with("inline:") => Arc::new(compile_definition(
                        &cur.name,
                        &cur.definition,
                        &config.config,
                        &config.manifests,
                    )),
                    _ => {
                        errors.push(format!("session `{sid}` runs definition `{}`, which the applied config does not define", s.definition));
                        continue;
                    }
                },
            };
            if !rev.ok {
                errors.push(format!(
                    "session `{sid}`: definition `{}` does not compile",
                    rev.name
                ));
                out.diagnostics.extend(
                    rev.diagnostics
                        .iter()
                        .filter(|d| d.severity == crate::compiler::Severity::Error)
                        .cloned(),
                );
                continue;
            }
            candidates.insert(sid.clone(), rev);
        }
        if !errors.is_empty() {
            out.error = Some(format!(
                "apply rejected; no session changed: {}",
                errors.join("; ")
            ));
            return out;
        }
        if matches!(scope, Scope::Global) {
            self.active = hash.to_owned();
        }
        let scoped = !matches!(scope, Scope::Global);
        for (sid, rev) in candidates {
            if !self.revisions.contains_key(&rev.revision) {
                out.effects.push(Effect::SaveRevision {
                    revision: Box::new((*rev).clone()),
                });
                self.revisions.insert(rev.revision.clone(), rev.clone());
            }
            out.revisions.insert(sid.clone(), rev.revision.clone());
            if let Some(s) = self.sessions.get_mut(&sid) {
                s.pending = Some((rev.revision.clone(), scoped));
            }
            self.advance(&sid, now, &mut out.effects);
        }
        out
    }

    /// Rebuild a session from its log (replay-as-reading runs no plugins).
    ///
    /// # Errors
    /// Fails when the log does not start with `session-created` or references
    /// an uninstalled revision.
    pub fn load_session(&mut self, records: &[Record]) -> Result<String, String> {
        let first = records.first().ok_or("empty log")?;
        let Body::SessionCreated { revision, .. } = &first.body else {
            return Err("log must start with session-created".into());
        };
        for r in records {
            if let Body::SessionCreated { revision, .. } | Body::ConfigApplied { revision, .. } =
                &r.body
                && !self.revisions.contains_key(revision)
            {
                return Err(format!(
                    "log references revision {revision} which is not installed"
                ));
            }
        }
        let _ = revision;
        for r in records {
            self.apply(r);
        }
        Ok(first.session_id.clone())
    }

    /// After a load: restart orphaned invocations and resume dispatch.
    pub fn recover(&mut self, session_id: &str, max_attempts: u32, now: u64) -> Vec<Effect> {
        let mut fx = Vec::new();
        let Some(s) = self.sessions.get(session_id) else {
            return fx;
        };
        let mut orphans: Vec<String> = s
            .run
            .as_ref()
            .and_then(|r| r.open.clone())
            .into_iter()
            .collect();
        orphans.extend(s.open_async.iter().cloned());
        for inv in orphans {
            fx.extend(self.retry(&inv, "orphaned by runtime restart", max_attempts, now));
        }
        self.advance(session_id, now, &mut fx);
        fx
    }

    /// Drop a quiescent session from memory (inactive sessions cost nothing).
    pub fn unload_if_idle(&mut self, session_id: &str) -> bool {
        let idle = self
            .sessions
            .get(session_id)
            .is_some_and(|s| s.quiescent() && s.pending.is_none());
        if idle {
            self.sessions.remove(session_id);
        }
        idle
    }

    /// An invocation's plugin and whether it is still open (loaded sessions only).
    #[must_use]
    pub fn invocation(&self, invocation_id: &str) -> Option<(String, bool)> {
        let s = self.sessions.get(session_of(invocation_id)?)?;
        s.invocations
            .get(invocation_id)
            .map(|i| (i.plugin.clone(), i.open))
    }

    /// The revision an invocation ran under (loaded sessions only).
    #[must_use]
    pub fn invocation_revision(&self, invocation_id: &str) -> Option<String> {
        let s = self.sessions.get(session_of(invocation_id)?)?;
        s.invocations.get(invocation_id).map(|i| i.revision.clone())
    }

    /// Is the session quiescent (no queued or running work)?
    #[must_use]
    pub fn is_idle(&self, session_id: &str) -> bool {
        self.sessions
            .get(session_id)
            .is_some_and(Session::quiescent)
    }
}

#[derive(Debug, Clone, Default)]
struct RouteEdges {
    blocking: Vec<Edge>,
    asyncs: Vec<Edge>,
    handler: Option<Edge>,
    exactly_one_handler: bool,
    family: String,
}

struct BreachInfo {
    event_id: Option<String>,
    invocation_id: Option<String>,
    plugin: String,
    declaration: Value,
    observed: Value,
    reason: String,
    reaction: &'static str,
}

/// The member part of a family event name (`tool-call/read_file` → `read_file`).
fn member_of(name: &str, family: &str) -> String {
    let base = family.trim_end_matches('*');
    let base = base.rsplit_once('/').map_or(base, |(b, _)| b);
    name.strip_prefix(base)
        .and_then(|r| r.strip_prefix('/'))
        .unwrap_or(name)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::member_of;

    #[test]
    fn members() {
        assert_eq!(member_of("tool-call/read_file", "tool-call/*"), "read_file");
        assert_eq!(
            member_of("tool-call/mcp__gh__x", "tool-call/*"),
            "mcp__gh__x"
        );
        assert_eq!(
            member_of("tool-call/mcp__gh__*", "tool-call/*"),
            "mcp__gh__*"
        );
    }
}
