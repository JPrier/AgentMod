//! The kernel: deterministic per-session dispatch over compiled pipelines.
//!
//! The kernel is sans-I/O. Every operation returns [`Effect`]s: records to
//! append (write-ahead) and invocations to start or cancel. All state changes
//! go through [`Kernel::apply`], the same function replay uses, so a session
//! rebuilt from its log is indistinguishable from the live one.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::compiler::{Compilation, DISPATCH_FAILED, RouteTable, Slot};
use crate::context::ContextState;
use crate::manifest::Capability;
use crate::record::{Body, Command, DispatchState, Outcome, Record, Settlement};
use crate::types::{
    Cause, ContextItem, ContextOp, Envelope, EventRecord, Lane, LogRef, Mode, Origin, Stamp,
    lookup, session_of,
};

/// What the host must do, in order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Effect {
    /// Durably append before acting on any later effect.
    Append { record: Record },
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
    pub event: Envelope,
}

/// A plugin's answer to an invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct InvocationResult {
    #[serde(default)]
    pub contributions: Vec<ContextOp>,
    /// Blocking transformers: the replacement payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Value>,
    /// Blocking subscribers: veto the event with a reason.
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
    #[serde(default)]
    pub lane: Lane,
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
    pub definition: String,
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
    pub error: Option<String>,
    pub effects: Vec<Effect>,
}

/// Configuration apply scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Scope {
    Global,
    Session { session_id: String },
}

/// Outcome of an apply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ApplyOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Sessions that could not adopt the config (their definition is absent).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<String>,
    pub effects: Vec<Effect>,
}

/// Snapshot of a session's dispatch status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionStatus {
    pub session_id: String,
    pub definition: String,
    pub config: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_config: Option<String>,
    pub state: DispatchState,
    /// `running`, `idle`, `draining`, `parked`, or `halted`.
    pub activity: String,
    pub queued_priority: usize,
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
    context: bool,
    open: bool,
    attempt: u32,
    start_sequence: u64,
    payload: Value,
    outputs: Vec<String>,
    attempt_outputs: usize,
    /// Provenance bound at dispatch: retries and cancels use this exact
    /// executor identity even if the session's config changed since.
    stamp: Stamp,
    /// Key value of a keyed (owner) dispatch.
    route: Option<String>,
}

#[derive(Debug, Clone)]
struct Run {
    event_id: String,
    payload: Value,
    next_blocking: usize,
    open: Option<String>,
}

#[derive(Debug, Clone)]
struct Session {
    id: String,
    definition: String,
    config: String,
    pending: Option<(String, bool)>,
    session_scoped: bool,
    state: DispatchState,
    next_sequence: u64,
    next_invocation: u64,
    priority: VecDeque<String>,
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
            && self.priority.is_empty()
            && self.normal.is_empty()
            && self.open_async.is_empty()
    }
}

/// The deterministic kernel.
#[derive(Debug, Default)]
pub struct Kernel {
    configs: BTreeMap<String, Arc<Compilation>>,
    active: String,
    sessions: BTreeMap<String, Session>,
    next_session: u64,
}

const CORE_PLUGIN: &str = "core";

impl Kernel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_session: 1,
            ..Default::default()
        }
    }

    /// Install a compiled config (must compile cleanly). Installing does not apply it.
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
        self.configs.insert(hash.clone(), Arc::new(compilation));
        Ok(hash)
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

    /// Hashes of every installed config referenced by a loaded session or active.
    #[must_use]
    pub fn configs_in_use(&self) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = self.sessions.values().map(|s| s.config.clone()).collect();
        out.extend(
            self.sessions
                .values()
                .filter_map(|s| s.pending.as_ref().map(|p| p.0.clone())),
        );
        out.insert(self.active.clone());
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
            DispatchState::Running if !s.priority.is_empty() || !s.normal.is_empty() => "running",
            DispatchState::Running => "idle",
        };
        Some(SessionStatus {
            session_id: s.id.clone(),
            definition: s.definition.clone(),
            config: s.config.clone(),
            pending_config: s.pending.as_ref().map(|p| p.0.clone()),
            state: s.state,
            activity: activity.to_owned(),
            queued_priority: s.priority.len(),
            queued_normal: s.normal.len(),
            open_invocations: open,
            current_event: s.run.as_ref().map(|r| r.event_id.clone()),
            last_sequence: s.next_sequence.saturating_sub(1),
        })
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
            definition, config, ..
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
                    config: config.clone(),
                    pending: None,
                    session_scoped: false,
                    state: DispatchState::Running,
                    next_sequence: r.sequence + 1,
                    next_invocation: 1,
                    priority: VecDeque::new(),
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
            Body::SessionCreated { .. } => {}
            Body::ContextSeeded { items, .. } => s.context.seed(r.sequence, items.clone()),
            Body::EventAppended { event } => {
                if let Origin::Pipeline { invocation_id, .. } = &event.origin
                    && let Some(i) = s.invocations.get_mut(invocation_id)
                {
                    i.outputs.push(event.event_id.clone());
                }
                match event.lane {
                    Lane::Priority => s.priority.push_back(event.event_id.clone()),
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
                s.priority.retain(|e| e != event_id);
                s.normal.retain(|e| e != event_id);
                let payload = s
                    .events
                    .get(event_id)
                    .map(|e| e.record.payload.clone())
                    .unwrap_or(Value::Null);
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
                payload,
                stamp,
                route,
            } => {
                if let Some(n) = invocation_id
                    .rsplit("/i")
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    s.next_invocation = s.next_invocation.max(n + 1);
                }
                let context = self
                    .configs
                    .get(&s.config)
                    .and_then(|c| c.definitions.get(&s.definition))
                    .and_then(|d| {
                        let name = s
                            .events
                            .get(event_id)
                            .map(|e| e.record.event_name.clone())
                            .unwrap_or_default();
                        d.pipeline(&name).slot_of(plugin).map(|sl| sl.context)
                    })
                    .unwrap_or(true);
                s.invocations.insert(
                    invocation_id.clone(),
                    InvInfo {
                        plugin: plugin.clone(),
                        event_id: event_id.clone(),
                        mode: *mode,
                        position: *position,
                        context,
                        open: true,
                        attempt: 1,
                        start_sequence: r.sequence,
                        payload: payload.clone(),
                        outputs: Vec::new(),
                        attempt_outputs: 0,
                        stamp: stamp.clone(),
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
                transform,
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
                    if let (Outcome::Ok, Some(t)) = (outcome, transform) {
                        run.payload = t.clone();
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
            Body::PublishBlocked { .. } => {}
            Body::DispatcherCommand { state, .. } => s.state = *state,
            Body::ConfigApplied { config, scoped, .. } => {
                s.config.clone_from(config);
                s.session_scoped = *scoped;
                s.pending = None;
            }
        }
    }

    // ------------------------------------------------------------------
    // Dispatch.
    // ------------------------------------------------------------------

    fn slot_for(&self, session: &Session, event_name: &str, index: usize) -> Option<Slot> {
        let c = self.configs.get(&session.config)?;
        let d = c.definitions.get(&session.definition)?;
        d.pipeline(event_name).blocking.get(index).cloned()
    }

    fn asyncs_for(&self, session: &Session, event_name: &str) -> (Vec<Slot>, Option<RouteTable>) {
        self.configs
            .get(&session.config)
            .and_then(|c| c.definitions.get(&session.definition))
            .map(|d| {
                let p = d.pipeline(event_name);
                (p.asyncs.clone(), p.routed.clone())
            })
            .unwrap_or_default()
    }

    /// The compiled keyed-dispatch tables of a session's current config.
    #[must_use]
    pub fn routes(&self, session_id: &str) -> Option<BTreeMap<String, RouteTable>> {
        let s = self.sessions.get(session_id)?;
        let d = self
            .configs
            .get(&s.config)?
            .definitions
            .get(&s.definition)?;
        Some(
            d.pipelines
                .iter()
                .filter_map(|(e, p)| p.routed.clone().map(|r| (e.clone(), r)))
                .collect(),
        )
    }

    fn stamp(&self, session: &Session, plugin: &str) -> Stamp {
        self.configs
            .get(&session.config)
            .and_then(|c| c.stamps.get(plugin).cloned())
            .unwrap_or_default()
    }

    fn envelope(
        s: &Session,
        event_id: &str,
        payload: Value,
        context: Option<Vec<ContextItem>>,
    ) -> Envelope {
        let e = &s.events[event_id];
        Envelope {
            event_id: event_id.to_owned(),
            session_id: s.id.clone(),
            event_name: e.record.event_name.clone(),
            sequence: e.sequence,
            cause: e.record.cause.clone(),
            lane: e.record.lane,
            arrived_at: e.at,
            depth: e.record.depth,
            payload,
            ui: e.record.ui.clone(),
            context,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start_invocation(
        &mut self,
        sid: &str,
        slot: &Slot,
        event_id: &str,
        payload: Value,
        route: Option<String>,
        now: u64,
        fx: &mut Vec<Effect>,
    ) {
        let s = &self.sessions[sid];
        let invocation_id = format!("{sid}/i{}", s.next_invocation);
        let stamp = self.stamp(s, &slot.plugin);
        self.emit(
            sid,
            now,
            Body::InvocationStarted {
                invocation_id: invocation_id.clone(),
                event_id: event_id.to_owned(),
                plugin: slot.plugin.clone(),
                mode: slot.mode,
                position: slot.position,
                stamp: stamp.clone(),
                payload: payload.clone(),
                route,
            },
            fx,
        );
        let s = &self.sessions[sid];
        let context = slot.context.then(|| s.context.items().to_vec());
        fx.push(Effect::Invoke {
            plugin: slot.plugin.clone(),
            stamp,
            attempt: 1,
            request: InvokeRequest {
                invocation_id,
                mode: slot.mode,
                position: slot.position,
                attempt: 1,
                event: Self::envelope(s, event_id, payload, context),
            },
        });
    }

    fn apply_pending_config(&mut self, sid: &str, now: u64, fx: &mut Vec<Effect>) {
        let Some((hash, scoped)) = self.sessions[sid].pending.clone() else {
            return;
        };
        let previous = self.sessions[sid].config.clone();
        if hash == previous {
            if let Some(s) = self.sessions.get_mut(sid) {
                s.pending = None;
                s.session_scoped = scoped;
            }
            return;
        }
        let scope = if scoped { "session" } else { "global" };
        self.emit(
            sid,
            now,
            Body::ConfigApplied {
                config: hash.clone(),
                previous,
                scoped,
            },
            fx,
        );
        self.append_event(
            sid,
            now,
            "config-applied",
            json!({ "config": hash, "scope": scope }),
            None,
            Lane::Priority,
            Cause::Core {
                reason: "config-applied".into(),
            },
            Origin::Core,
            0,
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
                let name = s.events[&run.event_id].record.event_name.clone();
                if let Some(slot) = self.slot_for(s, &name, run.next_blocking) {
                    self.start_invocation(
                        sid,
                        &slot,
                        &run.event_id,
                        run.payload.clone(),
                        None,
                        now,
                        fx,
                    );
                    return;
                }
                let (asyncs, routed) = self.asyncs_for(s, &name);
                let depth = s.events[&run.event_id].record.depth;
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
                for slot in asyncs {
                    self.start_invocation(
                        sid,
                        &slot,
                        &run.event_id,
                        run.payload.clone(),
                        None,
                        now,
                        fx,
                    );
                }
                // Keyed dispatch: exactly the one compiled owner, never a scan.
                if let Some(table) = routed {
                    match table.route(&run.payload) {
                        (Some(value), Some(owner)) => {
                            let owner = owner.clone();
                            self.start_invocation(
                                sid,
                                &owner,
                                &run.event_id,
                                run.payload.clone(),
                                Some(value),
                                now,
                                fx,
                            );
                        }
                        (value, _) => {
                            let reason = if value.is_some() {
                                "no-owner"
                            } else {
                                "no-key"
                            };
                            self.append_event(
                                sid,
                                now,
                                DISPATCH_FAILED,
                                json!({
                                    "reason": reason, "event_id": run.event_id, "event_name": name,
                                    "key": table.key, "value": value, "payload": run.payload,
                                    "owners": table.entries().into_iter().map(|(v, _)| v).collect::<Vec<_>>(),
                                }),
                                None,
                                Lane::Normal,
                                Cause::Core {
                                    reason: DISPATCH_FAILED.into(),
                                },
                                Origin::Core,
                                depth + 1,
                                None,
                                fx,
                            );
                        }
                    }
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
            let Some(next) = s.priority.front().or_else(|| s.normal.front()).cloned() else {
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
        depth: u32,
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
            depth,
            payload,
            ui,
        };
        self.emit(sid, now, Body::EventAppended { event }, fx);
        event_id
    }

    // ------------------------------------------------------------------
    // Public operations.
    // ------------------------------------------------------------------

    /// Create a new session from a named definition.
    pub fn start_session(&mut self, req: &StartRequest, now: u64) -> StartOutcome {
        let mut out = StartOutcome::default();
        let fail = |m: String| StartOutcome {
            error: Some(m),
            ..Default::default()
        };
        let Some(config) = self.configs.get(&self.active).cloned() else {
            return fail("no active config".into());
        };
        let is_core = req.plugin == CORE_PLUGIN;
        let manifest = config.manifest(&req.plugin);
        if !is_core {
            let Some(m) = manifest else {
                return fail(format!("unknown plugin `{}`", req.plugin));
            };
            if !m.has(Capability::StartSession) {
                return fail(format!(
                    "`{}` lacks the start-session capability",
                    req.plugin
                ));
            }
        }
        if !config.definitions.contains_key(&req.definition) {
            return fail(format!("unknown session definition `{}`", req.definition));
        }
        if let (Some(init), Some(m)) = (&req.initial, manifest) {
            let Some(emit) = m.emit(&init.event_name) else {
                return fail(format!(
                    "`{}` does not declare `{}` in its emits",
                    req.plugin, init.event_name
                ));
            };
            let missing = missing_supplies(&emit.supplies, &init.payload);
            if !missing.is_empty() {
                return fail(format!(
                    "initial `{}` is missing supplied keys [{}]",
                    init.event_name,
                    missing.join(", ")
                ));
            }
        }
        // Cause: the starting invocation, or a true root.
        let mut depth = 0;
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
                depth = src.events.get(&info.event_id).map_or(0, |e| e.record.depth) + 1;
                Cause::Invocation {
                    invocation_id: inv.clone(),
                }
            }
            None => Cause::Root {
                plugin: req.plugin.clone(),
            },
        };
        if depth > config.config.runtime.max_causal_depth {
            return fail(format!("causal depth {depth} exceeds max_causal_depth"));
        }
        let seeded = match &req.fork_from {
            Some(r) => match self.sessions.get(&r.session_id) {
                Some(src) => Some(src.context.at(r.sequence).to_vec()),
                None => return fail(format!("fork source `{}` is not loaded", r.session_id)),
            },
            None => None,
        };
        let sid = format!("s{:04}", self.next_session);
        let fx = &mut out.effects;
        self.emit(
            &sid,
            now,
            Body::SessionCreated {
                definition: req.definition.clone(),
                config: config.hash.clone(),
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
        let mut started = json!({ "definition": req.definition });
        if let Some(f) = &req.fork_from {
            started["fork_of"] = json!(f);
        }
        if let Some(inv) = &req.invocation_id {
            started["parent_session"] = json!(session_of(inv));
        }
        self.append_event(
            &sid,
            now,
            "session-started",
            started,
            None,
            Lane::Normal,
            cause.clone(),
            Origin::Core,
            depth,
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
                depth,
                None,
                fx,
            );
        }
        self.advance(&sid, now, fx);
        out.session_id = Some(sid);
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

    /// Classify and record a publish.
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
        let Some(config) = self.configs.get(&src.config).cloned() else {
            return self.block(Some(&source), req, "session config missing".into(), now);
        };
        let Some(manifest) = config.manifest(&req.plugin) else {
            return self.block(
                Some(&source),
                req,
                format!("`{}` is not in this session's config", req.plugin),
                now,
            );
        };
        let Some(emit) = manifest.emit(&req.event_name) else {
            return self.block(
                Some(&source),
                req,
                format!("undeclared emit `{}`", req.event_name),
                now,
            );
        };
        if deferred && !(emit.deferred && manifest.has(Capability::DeferredPublish)) {
            return self.block(
                Some(&source),
                req,
                format!(
                    "`{}` may not be published deferred (invocation closed or absent)",
                    req.event_name
                ),
                now,
            );
        }
        if !req.payload.is_object() {
            return self.block(
                Some(&source),
                req,
                "payload must be a JSON object".into(),
                now,
            );
        }
        let missing = missing_supplies(&emit.supplies, &req.payload);
        if !missing.is_empty() {
            return self.block(
                Some(&source),
                req,
                format!("missing supplied keys [{}]", missing.join(", ")),
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
        if target != source && !manifest.has(Capability::CrossSession) {
            return self.block(
                Some(&source),
                req,
                format!("targeting `{target}` requires the cross-session capability"),
                now,
            );
        }
        if !self.sessions.contains_key(&target) {
            return self.block(
                Some(&source),
                req,
                format!("unknown target session `{target}`"),
                now,
            );
        }
        let depth = src.events.get(&info.event_id).map_or(0, |e| e.record.depth) + 1;
        if depth > config.config.runtime.max_causal_depth {
            return self.block(
                Some(&source),
                req,
                format!(
                    "causal depth {depth} exceeds max_causal_depth={}",
                    config.config.runtime.max_causal_depth
                ),
                now,
            );
        }

        let mut out = PublishOutcome::default();
        let id = if deferred {
            None
        } else {
            let n = info.attempt_outputs;
            Some(format!("{source_inv}.o{n}"))
        };
        if let Some(id) = &id {
            if let Some(s) = self.sessions.get_mut(&source)
                && let Some(i) = s.invocations.get_mut(&source_inv)
            {
                i.attempt_outputs += 1;
            }
            if self.sessions[&target].events.contains_key(id) {
                out.event_id = Some(id.clone());
                out.duplicate = true;
                return out;
            }
        }
        let origin = if target != source {
            Origin::CrossSession {
                from_session: source.clone(),
                plugin: req.plugin.clone(),
            }
        } else if deferred {
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
            &target,
            now,
            &req.event_name,
            req.payload.clone(),
            req.ui.clone(),
            req.lane,
            Cause::Invocation {
                invocation_id: source_inv.clone(),
            },
            origin,
            depth,
            id,
            fx,
        );
        if target != source {
            self.emit(
                &source,
                now,
                Body::CrossSessionSent {
                    event_id: event_id.clone(),
                    event_name: req.event_name.clone(),
                    target_session: target.clone(),
                },
                fx,
            );
        }
        self.advance(&target, now, fx);
        out.event_id = Some(event_id);
        out
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
        let event_name = s
            .events
            .get(&info.event_id)
            .map(|e| e.record.event_name.clone())
            .unwrap_or_default();
        let manifest_transform = self
            .configs
            .get(&s.config)
            .and_then(|c| c.manifest(&info.plugin))
            .and_then(|m| m.transform(&event_name).cloned());
        let mut transform = None;
        let outcome = if let Some(e) = &result.error {
            Outcome::Failed { error: e.clone() }
        } else if info.mode == Mode::Async && (result.veto.is_some() || result.transform.is_some())
        {
            Outcome::Failed {
                error: "async subscribers are read-only: they cannot veto or transform".into(),
            }
        } else if let Some(reason) = &result.veto {
            Outcome::Veto {
                reason: reason.clone(),
            }
        } else if let Some(t) = &result.transform {
            match (
                &manifest_transform,
                check_transform(manifest_transform.as_ref(), &info.payload, t),
            ) {
                (Some(_), Ok(())) => {
                    transform = Some(t.clone());
                    Outcome::Ok
                }
                (None, _) => Outcome::Failed {
                    error: format!("undeclared transform of `{event_name}`"),
                },
                (_, Err(e)) => Outcome::Failed { error: e },
            }
        } else {
            Outcome::Ok
        };
        let contributions = if outcome == Outcome::Ok {
            result.contributions.clone()
        } else {
            Vec::new()
        };
        self.emit(
            &sid,
            now,
            Body::InvocationCompleted {
                invocation_id: invocation_id.to_owned(),
                outcome: outcome.clone(),
                transform,
                contributions,
                published: info.outputs.clone(),
            },
            &mut fx,
        );
        if let (Some(value), Outcome::Failed { error }) = (&info.route, &outcome) {
            let ev = &self.sessions[&sid].events[&info.event_id];
            let (depth, key) = (
                ev.record.depth,
                self.asyncs_for(&self.sessions[&sid], &event_name)
                    .1
                    .map(|r| r.key)
                    .unwrap_or_default(),
            );
            self.append_event(
                &sid,
                now,
                DISPATCH_FAILED,
                json!({
                    "reason": "owner-failed", "event_id": info.event_id, "event_name": event_name,
                    "key": key, "value": value, "payload": info.payload,
                    "plugin": info.plugin, "error": error,
                }),
                None,
                Lane::Normal,
                Cause::Core {
                    reason: DISPATCH_FAILED.into(),
                },
                Origin::Core,
                depth + 1,
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
                    &sid,
                    now,
                    Body::PipelineSettled {
                        event_id: info.event_id.clone(),
                        settlement,
                    },
                    &mut fx,
                );
            }
        }
        self.advance(&sid, now, &mut fx);
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
        let context = info
            .context
            .then(|| s.context.at(info.start_sequence.saturating_sub(1)).to_vec());
        let stamp = info.stamp.clone();
        fx.push(Effect::Invoke {
            plugin: info.plugin.clone(),
            stamp,
            attempt,
            request: InvokeRequest {
                invocation_id: invocation_id.to_owned(),
                mode: info.mode,
                position: info.position,
                attempt,
                event: Self::envelope(s, &info.event_id, info.payload.clone(), context),
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
                    let s = &self.sessions[session_id];
                    let info = s.invocations[&inv].clone();
                    let stamp = info.stamp.clone();
                    fx.push(Effect::Cancel {
                        plugin: info.plugin.clone(),
                        stamp,
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
                            transform: None,
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

    /// Apply an installed config. Sessions switch at their next event boundary.
    pub fn apply_config(&mut self, hash: &str, scope: &Scope, now: u64) -> ApplyOutcome {
        let mut out = ApplyOutcome::default();
        let Some(config) = self.configs.get(hash).cloned() else {
            out.error = Some(format!("config {hash} is not installed"));
            return out;
        };
        let targets: Vec<String> = match scope {
            Scope::Global => {
                self.active = hash.to_owned();
                self.sessions
                    .iter()
                    .filter(|(_, s)| !s.session_scoped)
                    .map(|(id, _)| id.clone())
                    .collect()
            }
            Scope::Session { session_id } => {
                if !self.sessions.contains_key(session_id) {
                    out.error = Some(format!("session `{session_id}` is not loaded"));
                    return out;
                }
                vec![session_id.clone()]
            }
        };
        let scoped = matches!(scope, Scope::Session { .. });
        for sid in targets {
            let def = self.sessions[&sid].definition.clone();
            if !config.definitions.contains_key(&def) {
                out.skipped.push(sid);
                continue;
            }
            if let Some(s) = self.sessions.get_mut(&sid) {
                s.pending = Some((hash.to_owned(), scoped));
            }
            self.advance(&sid, now, &mut out.effects);
        }
        out
    }

    /// Rebuild a session from its log (replay-as-reading runs no plugins).
    ///
    /// # Errors
    /// Fails when the log does not start with `session-created` or references an
    /// uninstalled config.
    pub fn load_session(&mut self, records: &[Record]) -> Result<String, String> {
        let first = records.first().ok_or("empty log")?;
        let Body::SessionCreated { config, .. } = &first.body else {
            return Err("log must start with session-created".into());
        };
        if !self.configs.contains_key(config) {
            return Err(format!(
                "log references config {config} which is not installed"
            ));
        }
        for r in records {
            self.apply(r);
        }
        // Sessions that follow the global layer adopt the active config on load.
        let sid = first.session_id.clone();
        let active = self.active.clone();
        let def_ok = self
            .configs
            .get(&active)
            .is_some_and(|c| c.definitions.contains_key(&self.sessions[&sid].definition));
        if let Some(s) = self.sessions.get_mut(&sid)
            && !s.session_scoped
            && s.config != active
            && def_ok
        {
            s.pending = Some((active, false));
        }
        Ok(sid)
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

    /// Is the session quiescent (no queued or running work)?
    #[must_use]
    pub fn is_idle(&self, session_id: &str) -> bool {
        self.sessions
            .get(session_id)
            .is_some_and(Session::quiescent)
    }
}

fn missing_supplies(supplies: &[String], payload: &Value) -> Vec<String> {
    supplies
        .iter()
        .filter(|k| !k.ends_with('?') && lookup(payload, k).is_none())
        .cloned()
        .collect()
}

fn check_transform(
    t: Option<&crate::manifest::Transform>,
    input: &Value,
    output: &Value,
) -> Result<(), String> {
    let Some(t) = t else { return Ok(()) };
    if !output.is_object() {
        return Err("transform output must be an object".into());
    }
    let preserve_all = t.preserves.iter().any(|p| p == "*");
    let keys: Vec<String> = if preserve_all {
        input
            .as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default()
    } else {
        t.preserves.clone()
    };
    for k in keys {
        if lookup(input, &k).is_some() && lookup(output, &k).is_none() {
            return Err(format!("transform dropped preserved key `{k}`"));
        }
    }
    for k in &t.adds {
        if lookup(output, k).is_none() {
            return Err(format!("transform did not add declared key `{k}`"));
        }
    }
    Ok(())
}
