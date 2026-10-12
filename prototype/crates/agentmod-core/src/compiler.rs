//! The compiler (compiler.md §5–§6): manifests plus one flattened session
//! definition in, one compiled graph out. The graph is also the routing table
//! the runtime dispatches from; its content hash is the session's revision.
//!
//! Deterministic: the same manifests, definition, and runtime version produce
//! the same graph and the same diagnostics, byte for byte.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core_manifest::{BOUNDARY, core_manifest};
use crate::manifest::{
    ACTION_CONTRIBUTE, ACTION_VETO, ACTIONS, Capability, DefPlugin, Definition, DeploymentConfig,
    EmitDecl, Manifest, PluginConfig, SourceRef, Subscription, is_family, pattern_contains,
    pattern_error, pattern_matches,
};
use crate::shape::{KeyDecl, Keys, MismatchKind, Shape, compatible, keys_mismatches};
use crate::types::{Lane, Mode, Stamp, hash_json};

// ----------------------------------------------------------------------
// Output types.
// ----------------------------------------------------------------------

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

/// A compiler finding: what, where it was written, and who is involved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    /// The config path the finding traces to (`definitions.coder.plugins.policy`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// The file and line that path was written at, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Dotted key path inside the payload (`args.path`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<String>,
    pub message: String,
}

impl Diagnostic {
    /// One-line location: `file:line definitions.x / event / key / plugins`.
    #[must_use]
    pub fn location(&self) -> String {
        let mut parts = Vec::new();
        if let Some(s) = &self.source {
            parts.push(s.to_string());
        }
        if let Some(p) = self.path.as_ref().or(self.definition.as_ref()) {
            parts.push(p.clone());
        }
        if let Some(e) = &self.event {
            parts.push(e.clone());
        }
        if let Some(k) = &self.key {
            parts.push(format!("key `{k}`"));
        }
        if !self.plugins.is_empty() {
            parts.push(self.plugins.join(", "));
        }
        parts.join(" / ")
    }
}

/// Where a blocking edge runs in its event's chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub phase: String,
    pub phase_index: usize,
    /// Position inside the phase (from a tie-break list, else by plugin name).
    pub rank: usize,
    /// Position in the whole blocking chain of this event class.
    pub chain: usize,
}

/// How an emitter reaches the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Via {
    /// A member plugin's declared emit.
    Emit,
    /// The core manifest (boundary node).
    Core,
    /// An inbound cross-session event (boundary node).
    Inbound,
    /// A possible `start-session` initial event from a plugin outside the session.
    StartSession,
}

/// One edge: *when `from` emits `event`, `to` is a destination*.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    /// The concrete event (a family emit is resolved into its members).
    pub event: String,
    /// The emitter's declaration it came from: the event itself, or the family
    /// (`tool-call/*`) it was resolved from.
    pub emit: String,
    pub to: String,
    /// The subscriber's event pattern.
    pub subscription: String,
    pub mode: Mode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Position>,
    pub lane: Lane,
    /// Keys and shapes present when the event reaches `to` (after earlier returns).
    pub supply: Keys,
    /// Slots delivered with the event.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deferred: bool,
    pub via: Via,
}

/// Node kind in the compiled graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeKind {
    Boundary,
    Plugin,
    /// A plugin outside the session that may start it with an initial event.
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub kind: NodeKind,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sink: bool,
}

/// The per-event view of the graph: one emitter's concrete event and its
/// ordered destinations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    pub from: String,
    /// The declaration the event came from (the event, or its family).
    pub emit: String,
    /// The concrete event.
    pub event: String,
    /// Edge indices of the blocking chain, in run order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocking: Vec<usize>,
    /// Edge indices started together once the chain settles.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asyncs: Vec<usize>,
}

/// The compiled graph: nodes, edges, and the routes the runtime reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub routes: Vec<Route>,
}

impl Graph {
    /// The route of a concrete event published by `from`. The graph holds only
    /// concrete events, so this is an exact lookup, never a pattern scan.
    #[must_use]
    pub fn route(&self, from: &str, name: &str) -> Option<&Route> {
        self.routes
            .iter()
            .find(|r| r.from == from && r.event == name)
    }

    #[must_use]
    pub fn edge(&self, i: usize) -> &Edge {
        &self.edges[i]
    }
}

/// A member plugin of a compiled definition (everything the runtime needs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Member {
    pub manifest: Manifest,
    pub stamp: Stamp,
    pub config: PluginConfig,
    /// False for external starters (they emit into the session but are never invoked).
    #[serde(default = "yes", skip_serializing_if = "Clone::clone")]
    pub member: bool,
}

fn yes() -> bool {
    true
}

/// A subscriber of a concrete family member, as its emitter sees it in the
/// resolved emit set: who subscribes and what the member must carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberSubscriber {
    pub plugin: String,
    pub mode: Mode,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub hints: Value,
    /// The subscriber's `receives`: for a tool, its argument schema.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub receives: Keys,
}

/// One concrete event a plugin may emit under a revision. A plugin's resolved
/// emit set is every one of these: its single-event emits, plus each family
/// emit replaced by the members the definition's subscribers name (§3.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedEmit {
    pub event: String,
    /// The family emit this member was resolved from (`tool-call/*`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    pub lane: Lane,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deferred: bool,
    /// The keys the event carries. For a family member, every open `object`
    /// key of the family (`args`) is narrowed to the keys its member
    /// subscribers declare.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: Keys,
    /// Family members only: the subscribers of this member (not the family's
    /// observers and gates, which never add members).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscribers: Vec<MemberSubscriber>,
}

impl ResolvedEmit {
    /// The concrete emit declaration (what the runtime checks a publish against).
    #[must_use]
    pub fn decl(&self) -> EmitDecl {
        EmitDecl {
            event: self.event.clone(),
            lane: self.lane,
            deferred: self.deferred,
            keys: self.keys.clone(),
        }
    }
}

/// The compile report's information sections (compiler.md §6, "Info").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Report {
    /// Plugin cycles (strongly connected components), sorted.
    pub cycles: Vec<Vec<String>>,
    /// Capability → plugins holding it.
    pub capabilities: BTreeMap<String, Vec<String>>,
    /// Interrupt-lane emits (`plugin: event`) and interrupt capability holders.
    pub interrupt: Vec<String>,
    /// `plugin: event` subscriptions that receive unlisted keys untyped.
    pub unlisted_keys: Vec<String>,
    /// The external surface: deferred and rootless entry points, inbound and outbound events.
    pub external_surface: BTreeMap<String, Vec<String>>,
    /// Blocking orders the compiler fixed by plugin name (no observable tie).
    pub fixed_orders: Vec<String>,
    /// Resolved families: family emit → emitter → the concrete members.
    pub families: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

/// One compiled session definition: the unit a session runs under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Revision {
    /// Content hash of the graph and its diagnostics.
    pub revision: String,
    /// The definition's name (or `inline:<hash>` for a definition passed to start-session).
    pub name: String,
    pub description: String,
    pub runtime: String,
    pub ok: bool,
    /// The flattened definition that was compiled.
    pub definition: Definition,
    pub plugins: BTreeMap<String, Member>,
    pub graph: Graph,
    /// Each member plugin's resolved emit set: the concrete events it may emit.
    pub emit_sets: BTreeMap<String, Vec<ResolvedEmit>>,
    pub diagnostics: Vec<Diagnostic>,
    pub report: Report,
}

impl Revision {
    #[must_use]
    pub fn errors(&self) -> Vec<&Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .collect()
    }

    #[must_use]
    pub fn manifest(&self, plugin: &str) -> Option<&Manifest> {
        self.plugins.get(plugin).map(|m| &m.manifest)
    }

    #[must_use]
    pub fn is_member(&self, plugin: &str) -> bool {
        self.plugins.get(plugin).is_some_and(|m| m.member)
    }

    /// The core manifest this revision was compiled against.
    #[must_use]
    pub fn core(&self) -> Manifest {
        core_manifest(&self.runtime).unwrap_or_default()
    }

    /// A plugin's resolved emit set under this revision (empty for a plugin
    /// that is not a member).
    #[must_use]
    pub fn emit_set(&self, plugin: &str) -> &[ResolvedEmit] {
        self.emit_sets.get(plugin).map_or(&[], Vec::as_slice)
    }

    /// The concrete emit of `emitter` named `name`, if it may emit it: an
    /// entry of its resolved emit set, or for the boundary node a core or
    /// inbound event. A family member outside the resolved set is not
    /// declared (publishing it is an undeclared emit, §3.4 step 5).
    #[must_use]
    pub fn resolved_emit(&self, emitter: &str, name: &str) -> Option<ResolvedEmit> {
        if emitter == BOUNDARY {
            let decl = match self.core().emit_for(name) {
                Some(e) => e.clone(),
                None => EmitDecl {
                    event: name.to_owned(),
                    lane: Lane::Normal,
                    deferred: false,
                    keys: self.definition.inbound.get(name)?.clone(),
                },
            };
            return Some(ResolvedEmit {
                event: name.to_owned(),
                family: None,
                lane: decl.lane,
                deferred: decl.deferred,
                keys: decl.keys,
                subscribers: Vec::new(),
            });
        }
        self.emit_set(emitter)
            .iter()
            .find(|e| e.event == name)
            .cloned()
    }

    /// The emit declaration `emitter` publishes `name` under (concrete).
    #[must_use]
    pub fn emit_decl(&self, emitter: &str, name: &str) -> Option<EmitDecl> {
        self.resolved_emit(emitter, name).map(|e| e.decl())
    }

    /// The route an event takes: by emitter and concrete name.
    #[must_use]
    pub fn route(&self, emitter: &str, name: &str) -> Option<(EmitDecl, Option<&Route>)> {
        let decl = self.emit_decl(emitter, name)?;
        Some((decl, self.graph.route(emitter, name)))
    }

    /// The subscription an edge was compiled from.
    #[must_use]
    pub fn subscription(&self, edge: &Edge) -> Option<&Subscription> {
        self.manifest(&edge.to)?
            .subscribe
            .iter()
            .find(|s| s.event == edge.subscription && s.mode == edge.mode)
    }
}

/// External surface entry: a plugin holding a capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceEntry {
    pub plugin: String,
    pub capability: Capability,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred_events: Vec<String>,
}

/// The compile output for a whole deployment config: every definition compiled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Compilation {
    /// Content hash of config + manifests; identifies this deployment config.
    pub hash: String,
    pub ok: bool,
    pub config: DeploymentConfig,
    pub manifests: BTreeMap<String, Manifest>,
    pub stamps: BTreeMap<String, Stamp>,
    /// Definition name → its compiled revision.
    pub definitions: BTreeMap<String, Revision>,
    /// Deployment-level findings plus every definition's, in report order.
    pub diagnostics: Vec<Diagnostic>,
    pub surface: Vec<SurfaceEntry>,
}

impl Compilation {
    #[must_use]
    pub fn errors(&self) -> Vec<&Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .collect()
    }

    #[must_use]
    pub fn manifest(&self, plugin: &str) -> Option<&Manifest> {
        self.manifests.get(plugin)
    }
}

// ----------------------------------------------------------------------
// Diagnostics context.
// ----------------------------------------------------------------------

struct Diags<'a> {
    out: Vec<Diagnostic>,
    seen: BTreeSet<String>,
    def: Option<String>,
    config: &'a DeploymentConfig,
    /// Config path prefix of the definition being compiled (`definitions.coder`).
    base: String,
}

struct D<'s> {
    sev: Severity,
    code: &'s str,
    path: Option<String>,
    event: Option<&'s str>,
    key: Option<String>,
    plugins: Vec<String>,
}

impl<'a> Diags<'a> {
    fn new(config: &'a DeploymentConfig) -> Self {
        Self {
            out: Vec::new(),
            seen: BTreeSet::new(),
            def: None,
            config,
            base: String::new(),
        }
    }

    fn at(&self, sub: &str) -> String {
        if sub.is_empty() {
            self.base.clone()
        } else if self.base.is_empty() {
            sub.to_owned()
        } else {
            format!("{}.{sub}", self.base)
        }
    }

    fn push(&mut self, d: D<'_>, message: String) {
        let path = d
            .path
            .or_else(|| (!self.base.is_empty()).then(|| self.base.clone()));
        let source = path.as_deref().and_then(|p| self.config.source(p)).cloned();
        let mut plugins = d.plugins;
        plugins.sort();
        plugins.dedup();
        let diag = Diagnostic {
            severity: d.sev,
            code: d.code.to_owned(),
            definition: self.def.clone(),
            path,
            source,
            event: d.event.map(str::to_owned),
            key: d.key,
            plugins,
            message,
        };
        let id = serde_json::to_string(&diag).unwrap_or_default();
        if self.seen.insert(id) {
            self.out.push(diag);
        }
    }

    fn err(
        &mut self,
        code: &str,
        path: Option<String>,
        event: Option<&str>,
        plugins: &[&str],
        msg: String,
    ) {
        self.push(
            D {
                sev: Severity::Error,
                code,
                path,
                event,
                key: None,
                plugins: plugins.iter().map(|p| (*p).to_owned()).collect(),
            },
            msg,
        );
    }

    fn warn(
        &mut self,
        code: &str,
        path: Option<String>,
        event: Option<&str>,
        plugins: &[&str],
        msg: String,
    ) {
        self.push(
            D {
                sev: Severity::Warning,
                code,
                path,
                event,
                key: None,
                plugins: plugins.iter().map(|p| (*p).to_owned()).collect(),
            },
            msg,
        );
    }

    fn info(
        &mut self,
        code: &str,
        path: Option<String>,
        event: Option<&str>,
        plugins: &[&str],
        msg: String,
    ) {
        self.push(
            D {
                sev: Severity::Info,
                code,
                path,
                event,
                key: None,
                plugins: plugins.iter().map(|p| (*p).to_owned()).collect(),
            },
            msg,
        );
    }
}

fn sort_diags(diags: &mut [Diagnostic]) {
    diags.sort_by(|a, b| {
        a.severity
            .cmp(&b.severity)
            .then(a.definition.cmp(&b.definition))
            .then(a.code.cmp(&b.code))
            .then(a.event.cmp(&b.event))
            .then(a.plugins.cmp(&b.plugins))
            .then(a.key.cmp(&b.key))
            .then(a.message.cmp(&b.message))
    });
}

// ----------------------------------------------------------------------
// Plugin-level checks (manifest consistency, capabilities, actions).
// ----------------------------------------------------------------------

fn check_manifest(dx: &mut Diags<'_>, name: &str, m: &Manifest, path: Option<String>) {
    let p = || path.clone();
    if name == BOUNDARY {
        dx.err(
            "reserved-name",
            p(),
            None,
            &[name],
            format!("`{BOUNDARY}` is the runtime's boundary node; a plugin cannot use that name"),
        );
    }
    let mut seen = BTreeSet::new();
    for e in &m.emit {
        if let Some(why) = pattern_error(&e.event, true) {
            dx.err(
                "invalid-event",
                p(),
                Some(&e.event),
                &[name],
                format!("`{name}` emit: {why}"),
            );
            continue;
        }
        if !seen.insert(e.event.as_str()) {
            dx.err(
                "duplicate-emit",
                p(),
                Some(&e.event),
                &[name],
                format!("`{name}` declares emit `{}` twice", e.event),
            );
        }
        if e.deferred && !m.has(Capability::DeferredPublish) {
            dx.err(
                "missing-capability",
                p(),
                Some(&e.event),
                &[name],
                format!(
                    "`{name}` flags `{}` deferred but lacks the deferred-publish capability",
                    e.event
                ),
            );
        }
        if e.lane == Lane::Interrupt && !m.has(Capability::Interrupt) {
            dx.err(
                "missing-capability",
                p(),
                Some(&e.event),
                &[name],
                format!(
                    "`{name}` emits `{}` on the interrupt lane but lacks the interrupt capability",
                    e.event
                ),
            );
        }
    }
    for (i, a) in m.emit.iter().enumerate() {
        for b in &m.emit[i + 1..] {
            if a.event != b.event
                && is_family(&a.event)
                && is_family(&b.event)
                && crate::manifest::patterns_overlap(&a.event, &b.event)
            {
                dx.err(
                    "duplicate-emit",
                    p(),
                    Some(&a.event),
                    &[name],
                    format!(
                        "`{name}` declares overlapping families `{}` and `{}`",
                        a.event, b.event
                    ),
                );
            }
        }
    }
    for o in &m.outbound {
        if let Some(why) = pattern_error(&o.event, true)
            .or_else(|| is_family(&o.event).then(|| "outbound events are single events".into()))
        {
            dx.err(
                "invalid-event",
                p(),
                Some(&o.event),
                &[name],
                format!("`{name}` outbound: {why}"),
            );
        }
        if !m.has(Capability::CrossSession) {
            dx.err(
                "missing-capability",
                p(),
                Some(&o.event),
                &[name],
                format!(
                    "`{name}` declares outbound `{}` but lacks the cross-session capability",
                    o.event
                ),
            );
        }
    }
    let mut subs = BTreeSet::new();
    for s in &m.subscribe {
        if let Some(why) = pattern_error(&s.event, false) {
            dx.err(
                "invalid-event",
                p(),
                Some(&s.event),
                &[name],
                format!("`{name}` subscription: {why}"),
            );
            continue;
        }
        if !subs.insert((s.event.as_str(), s.mode)) {
            dx.err(
                "duplicate-subscription",
                p(),
                Some(&s.event),
                &[name],
                format!("`{name}` subscribes to `{}` twice", s.event),
            );
        }
        for a in &s.actions {
            if !ACTIONS.contains(&a.as_str()) {
                dx.err(
                    "unknown-action",
                    p(),
                    Some(&s.event),
                    &[name],
                    format!(
                        "`{name}` declares unknown action `{a}` (known: {})",
                        ACTIONS.join(", ")
                    ),
                );
            }
        }
        if s.mode == Mode::Async {
            if s.has_action(ACTION_VETO) {
                dx.err("invalid-action", p(), Some(&s.event), &[name], format!("`{name}` is an async subscriber of `{}` and declares `veto`; async subscribers cannot veto", s.event));
            }
            if !s.returns.is_empty() {
                dx.err("invalid-action", p(), Some(&s.event), &[name], format!("`{name}` is an async subscriber of `{}` and declares `returns`; only blocking subscribers return keys", s.event));
            }
        }
        if s.receives.keys.contains_key("slots") {
            dx.err(
                "invalid-key",
                p(),
                Some(&s.event),
                &[name],
                "`slots` is reserved in `receives` for slot requests".into(),
            );
        }
    }
}

// ----------------------------------------------------------------------
// Flattening (fragments) and the compile entry points.
// ----------------------------------------------------------------------

/// Merge a definition's fragments into it. Diagnostics trace to the fragment.
fn flatten(dx: &mut Diags<'_>, name: &str, def: &Definition) -> Definition {
    if def.include.is_empty() {
        return def.clone();
    }
    let mut out = Definition {
        description: def.description.clone(),
        deny_warnings: def.deny_warnings,
        interface: def.interface.clone(),
        ..Default::default()
    };
    let mut phases_from: Option<(String, Vec<String>)> = None;
    let mut parts: Vec<(String, &Definition)> = Vec::new();
    for f in &def.include {
        match dx.config.fragments.get(f) {
            Some(frag) => {
                if !frag.include.is_empty() {
                    dx.err(
                        "nested-fragment",
                        Some(format!("fragments.{f}.include")),
                        None,
                        &[],
                        format!(
                            "fragment `{f}` includes other fragments; includes are one level deep"
                        ),
                    );
                }
                parts.push((format!("fragments.{f}"), frag));
            }
            None => dx.err(
                "unknown-fragment",
                Some(dx.at("include")),
                None,
                &[],
                format!("`{name}` includes unknown fragment `{f}`"),
            ),
        }
    }
    parts.push((format!("definitions.{name}"), def));
    for (from, part) in parts {
        if !part.runtime.is_empty() {
            if !out.runtime.is_empty() && out.runtime != part.runtime {
                dx.err(
                    "conflicting-runtime",
                    Some(format!("{from}.runtime")),
                    None,
                    &[],
                    format!(
                        "`{from}` targets runtime {} but another part targets {}",
                        part.runtime, out.runtime
                    ),
                );
            } else {
                out.runtime.clone_from(&part.runtime);
            }
        }
        if !part.phases.is_empty() {
            match &phases_from {
                Some((other, list)) if *list != part.phases => {
                    dx.err("conflicting-phases", Some(format!("{from}.phases")), None, &[], format!("`{from}` declares phases [{}] but `{other}` declares [{}]; a flattened definition has one phase list", part.phases.join(", "), list.join(", ")));
                }
                Some(_) => {}
                None => {
                    phases_from = Some((from.clone(), part.phases.clone()));
                    out.phases.clone_from(&part.phases);
                }
            }
        }
        for (p, entry) in &part.plugins {
            match out.plugins.get(p) {
                Some(prev) if prev != entry => dx.err(
                    "conflicting-plugin",
                    Some(format!("{from}.plugins.{p}")),
                    None,
                    &[p],
                    format!("`{p}` is configured differently in `{from}` and an earlier part"),
                ),
                _ => {
                    out.plugins.insert(p.clone(), entry.clone());
                }
            }
        }
        for (ev, phases) in &part.order {
            for (ph, list) in phases {
                let slot = out.order.entry(ev.clone()).or_default();
                match slot.get(ph) {
                    Some(prev) if prev != list => dx.err("conflicting-order", Some(format!("{from}.order")), Some(ev), &[], format!("tie-break for `{ev}` phase `{ph}` differs between `{from}` and an earlier part")),
                    _ => {
                        slot.insert(ph.clone(), list.clone());
                    }
                }
            }
        }
        for (k, v) in &part.inbound {
            match out.inbound.get(k) {
                Some(prev) if prev != v => dx.err(
                    "conflicting-inbound",
                    Some(format!("{from}.inbound.{k}")),
                    Some(k),
                    &[],
                    format!(
                        "inbound `{k}` is declared differently in `{from}` and an earlier part"
                    ),
                ),
                _ => {
                    out.inbound.insert(k.clone(), v.clone());
                }
            }
        }
        for (k, v) in &part.slots {
            match out.slots.get(k) {
                Some(prev) if prev != v => dx.err(
                    "conflicting-slot",
                    Some(format!("{from}.slots.{k}")),
                    None,
                    &[],
                    format!("slot `{k}` is declared differently in `{from}` and an earlier part"),
                ),
                _ => {
                    out.slots.insert(k.clone(), v.clone());
                }
            }
        }
        out.deny_warnings |= part.deny_warnings;
    }
    out
}

/// Compile a deployment config against the manifests obtained at handshake.
#[must_use]
pub fn compile(config: &DeploymentConfig, manifests: &BTreeMap<String, Manifest>) -> Compilation {
    let mut dx = Diags::new(config);
    let active: BTreeMap<&String, &PluginConfig> =
        config.plugins.iter().filter(|(_, p)| !p.disabled).collect();
    for (name, pc) in &active {
        let path = Some(format!("plugins.{name}"));
        let Some(m) = manifests.get(*name) else {
            dx.err(
                "missing-manifest",
                path,
                None,
                &[name],
                format!("plugin `{name}` produced no manifest (failed to start or handshake?)"),
            );
            continue;
        };
        if let Some(want) = pc.version.as_deref()
            && m.version != want
            && !m.version.starts_with(&format!("{want}."))
        {
            dx.err("plugin-version-unavailable", path.clone(), None, &[name], format!("the config requires `{name}` version {want}, but the running plugin reports {}", m.version));
        }
        check_manifest(&mut dx, name, m, path);
    }
    let mut definitions = BTreeMap::new();
    let deployment = std::mem::take(&mut dx.out);
    let mut all = deployment.clone();
    for (def_name, def) in &config.definitions {
        let rev = compile_definition(def_name, def, config, manifests);
        // A host disables plugins it cannot run (a worker-only plugin natively,
        // a process-only one in the browser). A definition broken only because
        // of that is unavailable on this host, not a configuration error.
        let host_scoped = def
            .plugins
            .keys()
            .any(|p| config.plugins.get(p).is_some_and(|c| c.disabled));
        if host_scoped && !rev.ok {
            let first = rev
                .errors()
                .first()
                .map(|d| d.message.clone())
                .unwrap_or_default();
            let mut d2 = Diags::new(config);
            d2.def = Some(def_name.clone());
            d2.base = format!("definitions.{def_name}");
            d2.warn("definition-unavailable", None, None, &[], format!("`{def_name}` cannot run on this host (its disabled plugins leave it incomplete: {first}); skipped"));
            all.extend(d2.out);
            continue;
        }
        // Plugin-level findings are already reported once for the deployment.
        let dup = |d: &Diagnostic| {
            deployment.iter().any(|x| {
                x.code == d.code
                    && x.event == d.event
                    && x.plugins == d.plugins
                    && x.message == d.message
            })
        };
        all.extend(rev.diagnostics.iter().filter(|d| !dup(d)).cloned());
        definitions.insert(def_name.clone(), rev);
    }
    sort_diags(&mut all);
    let mut surface = Vec::new();
    for (name, m) in manifests.iter().filter(|(n, _)| active.contains_key(n)) {
        for cap in &m.capabilities {
            let deferred_events = if *cap == Capability::DeferredPublish {
                m.emit
                    .iter()
                    .filter(|e| e.deferred)
                    .map(|e| e.event.clone())
                    .collect()
            } else {
                Vec::new()
            };
            surface.push(SurfaceEntry {
                plugin: name.clone(),
                capability: *cap,
                deferred_events,
            });
        }
    }
    let stamps: BTreeMap<String, Stamp> = active
        .iter()
        .map(|(n, p)| ((*n).clone(), p.stamp()))
        .collect();
    let used: BTreeMap<String, Manifest> = manifests
        .iter()
        .filter(|(n, _)| config.plugins.contains_key(*n))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let hash = hash_json(
        &json!({ "config": config.without_sources(), "manifests": used, "stamps": stamps }),
    )[..16]
        .to_owned();
    let ok = !all.iter().any(|d| d.severity == Severity::Error);
    Compilation {
        hash,
        ok,
        config: config.clone(),
        manifests: used,
        stamps,
        definitions,
        diagnostics: all,
        surface,
    }
}

/// Compile one definition (a named one, or one passed to `start-session`).
#[must_use]
pub fn compile_definition(
    name: &str,
    def: &Definition,
    config: &DeploymentConfig,
    manifests: &BTreeMap<String, Manifest>,
) -> Revision {
    let mut dx = Diags::new(config);
    dx.def = Some(name.to_owned());
    dx.base = if config.definitions.contains_key(name) {
        format!("definitions.{name}")
    } else {
        format!("start-session.{name}")
    };
    let flat = flatten(&mut dx, name, def);
    let flat = crate::embed::inline_workflows(
        &mut EmbedCx { dx: &mut dx },
        name,
        &flat,
        config,
        manifests,
    );
    let mut c = DefCompiler::new(&mut dx, &flat, config, manifests);
    c.run();
    let (graph, report, plugins, emit_sets) = (c.graph, c.report, c.plugins, c.emit_sets);
    let mut diags = std::mem::take(&mut dx.out);
    if flat.deny_warnings {
        for d in &mut diags {
            if d.severity == Severity::Warning {
                d.severity = Severity::Error;
                d.message = format!("{} (deny_warnings)", d.message);
            }
        }
    }
    sort_diags(&mut diags);
    let ok = !diags.iter().any(|d| d.severity == Severity::Error);
    let core = core_manifest(&flat.runtime);
    let unsourced: Vec<Diagnostic> = diags
        .iter()
        .map(|d| Diagnostic {
            source: None,
            ..d.clone()
        })
        .collect();
    let mut def_hashed = flat.clone();
    def_hashed.description = String::new();
    let revision = hash_json(&json!({
        "runtime": flat.runtime,
        "core": core,
        "definition": def_hashed,
        "plugins": plugins,
        "graph": graph,
        "emit_sets": emit_sets,
        "diagnostics": unsourced,
    }))[..16]
        .to_owned();
    Revision {
        revision,
        name: name.to_owned(),
        description: flat.description.clone(),
        runtime: flat.runtime.clone(),
        ok,
        definition: flat,
        plugins,
        graph,
        emit_sets,
        diagnostics: diags,
        report,
    }
}

/// Context handed to the embedded-workflow inliner.
pub(crate) struct EmbedCx<'x, 'a> {
    dx: &'x mut Diags<'a>,
}

impl EmbedCx<'_, '_> {
    pub(crate) fn error(
        &mut self,
        code: &str,
        path: &str,
        event: Option<&str>,
        plugins: &[&str],
        msg: String,
    ) {
        let p = self.dx.at(path);
        self.dx.err(code, Some(p), event, plugins, msg);
    }
}

// ----------------------------------------------------------------------
// The per-definition compiler.
// ----------------------------------------------------------------------

/// One concrete emit feeding the session (a family emit appears once per
/// resolved member).
#[derive(Clone)]
struct Emitter {
    from: String,
    /// The concrete declaration (a member's keys already narrowed).
    decl: EmitDecl,
    /// The family emit the member was resolved from.
    family: Option<String>,
    via: Via,
}

impl Emitter {
    /// The declaration this emit came from: the family, or the event itself.
    fn declared(&self) -> &str {
        self.family.as_deref().unwrap_or(&self.decl.event)
    }
}

struct SubRef<'m> {
    plugin: String,
    sub: &'m Subscription,
}

struct DefCompiler<'x, 'a, 'm> {
    dx: &'x mut Diags<'a>,
    def: &'m Definition,
    config: &'m DeploymentConfig,
    manifests: &'m BTreeMap<String, Manifest>,
    members: BTreeMap<String, &'m Manifest>,
    core: Option<Manifest>,
    graph: Graph,
    report: Report,
    plugins: BTreeMap<String, Member>,
    emit_sets: BTreeMap<String, Vec<ResolvedEmit>>,
    /// (plugin, subscription event, mode) → fed?
    fed: BTreeMap<(String, String, Mode), bool>,
}

impl<'x, 'a, 'm> DefCompiler<'x, 'a, 'm> {
    fn new(
        dx: &'x mut Diags<'a>,
        def: &'m Definition,
        config: &'m DeploymentConfig,
        manifests: &'m BTreeMap<String, Manifest>,
    ) -> Self {
        Self {
            dx,
            def,
            config,
            manifests,
            members: BTreeMap::new(),
            core: None,
            graph: Graph::default(),
            report: Report::default(),
            plugins: BTreeMap::new(),
            emit_sets: BTreeMap::new(),
            fed: BTreeMap::new(),
        }
    }

    fn at(&self, sub: &str) -> Option<String> {
        Some(self.dx.at(sub))
    }

    fn run(&mut self) {
        self.resolve();
        if self.core.is_none() {
            return;
        }
        let emitters = self.emitters();
        self.check_self_subscription();
        self.check_phases();
        self.check_slots();
        self.build(&emitters);
        self.analyse(&emitters);
        self.report_sections(&emitters);
    }

    /// Resolve: the core manifest for the target runtime and every member's manifest.
    fn resolve(&mut self) {
        let def = self.def;
        if def.runtime.is_empty() {
            let p = self.at("runtime");
            self.dx.err("missing-runtime", p, None, &[], format!("a session definition names the runtime version it targets (`runtime = \"{}\"`)", crate::core_manifest::RUNTIME_VERSION));
        } else {
            self.core = core_manifest(&def.runtime);
            if self.core.is_none() {
                let p = self.at("runtime");
                self.dx.err(
                    "unknown-runtime",
                    p,
                    None,
                    &[],
                    format!(
                        "runtime {} is not supported by this build (supported: {})",
                        def.runtime,
                        crate::core_manifest::SUPPORTED_RUNTIMES.join(", ")
                    ),
                );
            }
        }
        let mut seen_phase = BTreeSet::new();
        for ph in &def.phases {
            if !seen_phase.insert(ph) {
                let p = self.at("phases");
                self.dx.err(
                    "duplicate-phase",
                    p,
                    None,
                    &[],
                    format!("phase `{ph}` is listed twice"),
                );
            }
        }
        for name in def.plugins.keys() {
            let path = self.at(&format!("plugins.{name}"));
            match self.config.plugins.get(name) {
                None => self.dx.err(
                    "unknown-plugin",
                    path,
                    None,
                    &[name],
                    format!("definition references unknown plugin `{name}`"),
                ),
                Some(p) if p.disabled => self.dx.info(
                    "disabled-plugin",
                    path,
                    None,
                    &[name],
                    format!("`{name}` is disabled on this host and skipped"),
                ),
                Some(pc) => match self.manifests.get(name) {
                    Some(m) => {
                        check_manifest(self.dx, name, m, Some(format!("plugins.{name}")));
                        self.members.insert(name.clone(), m);
                        self.plugins.insert(
                            name.clone(),
                            Member {
                                manifest: m.clone(),
                                stamp: pc.stamp(),
                                config: pc.clone(),
                                member: true,
                            },
                        );
                    }
                    None => self.dx.err(
                        "missing-manifest",
                        path,
                        None,
                        &[name],
                        format!(
                            "plugin `{name}` produced no manifest (failed to start or handshake?)"
                        ),
                    ),
                },
            }
        }
        // External starters: plugins outside the session holding start-session.
        for (name, pc) in self.config.plugins.iter().filter(|(_, p)| !p.disabled) {
            if self.members.contains_key(name) {
                continue;
            }
            if let Some(m) = self
                .manifests
                .get(name)
                .filter(|m| m.has(Capability::StartSession))
            {
                self.plugins.insert(
                    name.clone(),
                    Member {
                        manifest: m.clone(),
                        stamp: pc.stamp(),
                        config: pc.clone(),
                        member: false,
                    },
                );
            }
        }
    }

    /// Every emitter declaration feeding this session.
    fn emitters(&mut self) -> Vec<Emitter> {
        let mut out = Vec::new();
        if let Some(core) = &self.core {
            for e in &core.emit {
                out.push(Emitter {
                    from: BOUNDARY.into(),
                    decl: e.clone(),
                    family: None,
                    via: Via::Core,
                });
            }
        }
        for (ev, keys) in &self.def.inbound {
            let path = self.at(&format!("inbound.{ev}"));
            if let Some(why) = pattern_error(ev, true)
                .or_else(|| is_family(ev).then(|| "inbound events are single events".into()))
            {
                self.dx.err(
                    "invalid-event",
                    path,
                    Some(ev),
                    &[],
                    format!("inbound: {why}"),
                );
                continue;
            }
            if self.core.as_ref().is_some_and(|c| c.emit_for(ev).is_some()) {
                self.dx.err(
                    "invalid-event",
                    path,
                    Some(ev),
                    &[],
                    format!("`{ev}` is a core event; it cannot also be inbound"),
                );
                continue;
            }
            out.push(Emitter {
                from: BOUNDARY.into(),
                decl: EmitDecl {
                    event: ev.clone(),
                    lane: Lane::Normal,
                    deferred: false,
                    keys: keys.clone(),
                },
                family: None,
                via: Via::Inbound,
            });
        }
        let subs = self.subs();
        let members: Vec<(String, &'m Manifest)> =
            self.members.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (name, m) in members {
            for e in m
                .emit
                .iter()
                .filter(|e| pattern_error(&e.event, true).is_none())
            {
                if is_family(&e.event) {
                    out.extend(self.resolve_family(&name, e, &subs, Via::Emit));
                } else {
                    out.push(Emitter {
                        from: name.clone(),
                        decl: e.clone(),
                        family: None,
                        via: Via::Emit,
                    });
                }
            }
        }
        let starters: Vec<(String, Manifest)> = self
            .plugins
            .iter()
            .filter(|(_, m)| !m.member)
            .map(|(n, m)| (n.clone(), m.manifest.clone()))
            .collect();
        for (name, manifest) in starters {
            for e in manifest
                .emit
                .iter()
                .filter(|e| pattern_error(&e.event, true).is_none() && !is_family(&e.event))
            {
                out.push(Emitter {
                    from: name.clone(),
                    decl: e.clone(),
                    family: None,
                    via: Via::StartSession,
                });
            }
        }
        // Every plugin's resolved emit set, in event order.
        for e in out.iter().filter(|e| e.from != BOUNDARY) {
            let subscribers = match &e.family {
                Some(_) => subs
                    .iter()
                    .filter(|sr| sr.sub.event == e.decl.event && sr.plugin != e.from)
                    .map(|sr| MemberSubscriber {
                        plugin: sr.plugin.clone(),
                        mode: sr.sub.mode,
                        description: sr.sub.description.clone(),
                        hints: sr.sub.hints.clone(),
                        receives: sr.sub.receives.keys.clone(),
                    })
                    .collect(),
                None => Vec::new(),
            };
            self.emit_sets
                .entry(e.from.clone())
                .or_default()
                .push(ResolvedEmit {
                    event: e.decl.event.clone(),
                    family: e.family.clone(),
                    lane: e.decl.lane,
                    deferred: e.decl.deferred,
                    keys: e.decl.keys.clone(),
                    subscribers,
                });
        }
        for set in self.emit_sets.values_mut() {
            set.sort_by(|a, b| a.event.cmp(&b.event));
        }
        out
    }

    /// Resolve a family emit into the concrete members the definition's
    /// subscribers name (compiler.md §3.3, §3.4). Only subscriptions to a
    /// concrete member define members; family and sub-family subscriptions only
    /// observe or gate. Each member's open `object` keys (a tool call's `args`)
    /// are narrowed to what its member subscribers declare, so the member's
    /// argument schema is its subscribers' `receives`.
    fn resolve_family(
        &mut self,
        emitter: &str,
        family: &EmitDecl,
        subs: &[SubRef<'m>],
        via: Via,
    ) -> Vec<Emitter> {
        let mut members: BTreeMap<&str, Vec<&SubRef<'m>>> = BTreeMap::new();
        for sr in subs {
            let ev = sr.sub.event.as_str();
            if !sr.sub.wildcard()
                && !is_family(ev)
                && sr.plugin != emitter
                && pattern_matches(&family.event, ev)
            {
                members.entry(ev).or_default().push(sr);
            }
        }
        let path = self.at(&format!("plugins.{emitter}"));
        let names: Vec<String> = members.keys().map(|m| (*m).to_owned()).collect();
        self.dx.info(
            "resolved-family",
            path,
            Some(&family.event),
            &[emitter],
            if names.is_empty() {
                format!(
                    "`{emitter}` emits `{}`, which resolves to nothing: no plugin of this definition subscribes to a member",
                    family.event
                )
            } else {
                format!(
                    "`{emitter}` emits `{}`, resolved to {} member{}: {}",
                    family.event,
                    names.len(),
                    if names.len() == 1 { "" } else { "s" },
                    names.join(", ")
                )
            },
        );
        self.report
            .families
            .entry(family.event.clone())
            .or_default()
            .insert(emitter.to_owned(), names);
        let mut out = Vec::new();
        for (member, subscribers) in members {
            let keys = self.narrow(emitter, &family.event, member, &family.keys, &subscribers);
            out.push(Emitter {
                from: emitter.to_owned(),
                decl: EmitDecl {
                    event: member.to_owned(),
                    lane: family.lane,
                    deferred: family.deferred,
                    keys,
                },
                family: Some(family.event.clone()),
                via,
            });
        }
        out
    }

    /// A member's keys: the family's, with every open `object` key replaced by
    /// the union of the nested keys its member subscribers declare for it (a
    /// nested key is required if any subscriber requires it).
    fn narrow(
        &mut self,
        emitter: &str,
        family: &str,
        member: &str,
        keys: &Keys,
        subscribers: &[&SubRef<'m>],
    ) -> Keys {
        let mut out = keys.clone();
        for (k, decl) in keys {
            if decl.shape != Shape::Object(None) {
                continue;
            }
            let mut merged: Option<(Keys, String)> = None;
            for sr in subscribers {
                let Some(Shape::Object(Some(nested))) =
                    sr.sub.receives.keys.get(k).map(|d| &d.shape)
                else {
                    continue;
                };
                let Some((acc, first)) = merged.as_mut() else {
                    merged = Some((nested.clone(), sr.plugin.clone()));
                    continue;
                };
                for (nk, nd) in nested {
                    match acc.get_mut(nk) {
                        None => {
                            acc.insert(nk.clone(), nd.clone());
                        }
                        Some(have) if same_shape(&have.shape, &nd.shape) => {
                            have.optional &= nd.optional;
                            if have.description.is_none() {
                                have.description.clone_from(&nd.description);
                            }
                        }
                        Some(have) => {
                            let path = self.at(&format!("plugins.{}", sr.plugin));
                            let msg = format!(
                                "`{first}` and `{}` both subscribe to `{member}` but declare `{k}.{nk}` as `{}` and `{}`: the member's `{k}` (resolved from `{emitter}`'s `{family}`) has no single shape; make them agree",
                                sr.plugin, have.shape, nd.shape
                            );
                            let plugins =
                                vec![first.clone(), sr.plugin.clone(), emitter.to_owned()];
                            self.dx.push(
                                D {
                                    sev: Severity::Error,
                                    code: "conflicting-member-shape",
                                    path,
                                    event: Some(member),
                                    key: Some(format!("{k}.{nk}")),
                                    plugins,
                                },
                                msg,
                            );
                        }
                    }
                }
            }
            if let Some((nested, _)) = merged {
                out.insert(
                    k.clone(),
                    KeyDecl {
                        shape: Shape::Object(Some(nested)),
                        optional: decl.optional,
                        description: decl.description.clone(),
                    },
                );
            }
        }
        out
    }

    fn subs(&self) -> Vec<SubRef<'m>> {
        let mut out = Vec::new();
        for (name, m) in &self.members {
            for s in m
                .subscribe
                .iter()
                .filter(|s| pattern_error(&s.event, false).is_none())
            {
                out.push(SubRef {
                    plugin: name.clone(),
                    sub: s,
                });
            }
        }
        out
    }

    /// A plugin emits and subscribes to the same event name, including through a family.
    fn check_self_subscription(&mut self) {
        let members: Vec<(String, &'m Manifest)> =
            self.members.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (name, m) in members {
            for s in m.subscribe.iter().filter(|s| !s.wildcard()) {
                for e in &m.emit {
                    if crate::manifest::patterns_overlap(&s.event, &e.event) {
                        let path = self.at(&format!("plugins.{name}"));
                        let via = if s.event == e.event {
                            String::new()
                        } else {
                            format!(
                                " (through `{}`)",
                                if is_family(&e.event) {
                                    &e.event
                                } else {
                                    &s.event
                                }
                            )
                        };
                        self.dx.err("self-subscription", path, Some(&s.event), &[&name], format!("`{name}` subscribes to `{}` and emits `{}`{via}: a plugin may not consume its own emits; emit a different event and let another plugin react to it", s.event, e.event));
                    }
                }
            }
        }
    }

    fn phase_of(&self, plugin: &str, sub: &Subscription) -> Option<&'m str> {
        let entry: &'m DefPlugin = self.def.plugins.get(plugin)?;
        entry
            .phases
            .get(&sub.event)
            .or(entry.phase.as_ref())
            .map(String::as_str)
    }

    fn check_phases(&mut self) {
        let def = self.def;
        for sr in self.subs() {
            if sr.sub.mode != Mode::Blocking {
                continue;
            }
            let path = self.at(&format!("plugins.{}", sr.plugin));
            match self.phase_of(&sr.plugin, sr.sub) {
                None => self.dx.err("missing-phase", path, Some(&sr.sub.event), &[&sr.plugin], format!("`{}` is a blocking subscriber of `{}` but has no phase; set `phase` for the plugin (or `phases.\"{}\"` for this event) in the definition", sr.plugin, sr.sub.event, sr.sub.event)),
                Some(ph) if !def.phases.iter().any(|p| p == ph) => self.dx.err("unknown-phase", path, Some(&sr.sub.event), &[&sr.plugin], format!("`{}` is assigned phase `{ph}`, which is not in the definition's phases [{}]", sr.plugin, def.phases.join(", "))),
                Some(_) => {}
            }
        }
        for (plugin, entry) in &def.plugins {
            let Some(m) = self.members.get(plugin) else {
                continue;
            };
            for ev in entry.phases.keys() {
                if !m
                    .subscribe
                    .iter()
                    .any(|s| &s.event == ev && s.mode == Mode::Blocking)
                {
                    let path = self.at(&format!("plugins.{plugin}"));
                    self.dx.err("stale-phase", path, Some(ev), &[plugin], format!("phase override for `{ev}`, but `{plugin}` has no blocking subscription to `{ev}`"));
                }
            }
        }
    }

    fn check_slots(&mut self) {
        let slots = self.def.effective_slots();
        for sr in self.subs() {
            for (slot, want) in &sr.sub.receives.slots {
                let path = self.at(&format!("plugins.{}", sr.plugin));
                match slots.get(slot) {
                    None => self.dx.err("unknown-slot", path, Some(&sr.sub.event), &[&sr.plugin], format!("`{}` requests slot `{slot}` on `{}`, which the definition does not declare (slots: {})", sr.plugin, sr.sub.event, slots.keys().cloned().collect::<Vec<_>>().join(", "))),
                    Some(have) => {
                        if let Err(e) = compatible(&have.shape, &want.shape, slot) {
                            self.dx.push(D { sev: Severity::Error, code: "shape-mismatch", path, event: Some(&sr.sub.event), key: Some(format!("slots.{}", e.path)), plugins: vec![sr.plugin.clone()] }, format!("slot `{slot}` is `{}` in the definition but `{}` requests `{}` ({e})", have.shape, sr.plugin, want.shape));
                        }
                    }
                }
            }
        }
    }

    /// The tie-break list that orders `class` in `phase`.
    fn order_list(&self, class: &str, emit: &str, phase: &str) -> Option<&'m Vec<String>> {
        self.order_entry(class, emit, phase).map(|(_, l)| l)
    }

    /// The tie-break list for `class` in `phase`, with the event key it is written under.
    fn order_entry(
        &self,
        class: &str,
        emit: &str,
        phase: &str,
    ) -> Option<(&'m str, &'m Vec<String>)> {
        let order: &'m BTreeMap<String, BTreeMap<String, Vec<String>>> = &self.def.order;
        let base = emit.strip_suffix("/*");
        let mut keys: Vec<&str> = vec![class];
        if is_family(emit) {
            keys.push(emit);
        }
        if let Some(b) = base {
            keys.push(b);
        }
        for k in keys {
            if let Some((key, by_phase)) = order.get_key_value(k)
                && let Some(l) = by_phase.get(phase)
            {
                return Some((key.as_str(), l));
            }
        }
        order
            .iter()
            .filter(|(k, _)| {
                (is_family(k) && pattern_contains(k, class)) || class.starts_with(&format!("{k}/"))
            })
            .max_by_key(|(k, _)| k.len())
            .and_then(|(k, p)| p.get(phase).map(|l| (k.as_str(), l)))
    }

    /// Listed in the definition but disabled on this host (skipped, not stale).
    fn host_disabled(&self, plugin: &str) -> bool {
        self.def.plugins.contains_key(plugin)
            && self.config.plugins.get(plugin).is_some_and(|p| p.disabled)
    }

    fn wildcard_subscribers(&self) -> BTreeSet<String> {
        self.members
            .iter()
            .filter(|(_, m)| m.subscribe.iter().any(Subscription::wildcard))
            .map(|(n, _)| n.clone())
            .collect()
    }

    fn build(&mut self, emitters: &[Emitter]) {
        let subs = self.subs();
        let wild = self.wildcard_subscribers();
        for sr in &subs {
            self.fed.insert(
                (sr.plugin.clone(), sr.sub.event.clone(), sr.sub.mode),
                false,
            );
        }
        let mut fixed: BTreeSet<String> = BTreeSet::new();
        for e in emitters {
            let event = e.decl.event.clone();
            // Destinations of this concrete event, by the expansion rules.
            let mut dests: Vec<&SubRef<'m>> = Vec::new();
            for sr in &subs {
                if sr.sub.wildcard() {
                    // A wildcard subscriber never receives its own plugin's emits,
                    // nor events emitted by any wildcard subscriber.
                    if sr.plugin == e.from || wild.contains(&e.from) {
                        continue;
                    }
                } else if !pattern_matches(&sr.sub.event, &event) || sr.plugin == e.from {
                    continue;
                }
                dests.push(sr);
            }
            // Order the blocking chain.
            let mut blocking: Vec<(usize, usize, String, usize)> = Vec::new(); // (phase_idx, listed rank, plugin, dest idx)
            for (i, d) in dests
                .iter()
                .enumerate()
                .filter(|(_, d)| d.sub.mode == Mode::Blocking)
            {
                let Some(ph) = self.phase_of(&d.plugin, d.sub) else {
                    continue;
                };
                let Some(pi) = self.def.phases.iter().position(|p| p == ph) else {
                    continue;
                };
                let listed = self
                    .order_list(&event, e.declared(), ph)
                    .and_then(|l| l.iter().position(|x| *x == d.plugin))
                    .unwrap_or(usize::MAX);
                blocking.push((pi, listed, d.plugin.clone(), i));
            }
            blocking.sort();
            self.check_ties(e, &event, &dests, &blocking, &mut fixed);
            self.check_tie_lists(&event, e.declared(), &dests);
            // Walk supply through the chain and lay down the edges.
            let mut supply = e.decl.keys.clone();
            let mut route = Route {
                from: e.from.clone(),
                emit: e.declared().to_owned(),
                event: event.clone(),
                blocking: Vec::new(),
                asyncs: Vec::new(),
            };
            let mut rank_in_phase: BTreeMap<usize, usize> = BTreeMap::new();
            for (chain, (pi, _, _, di)) in blocking.iter().enumerate() {
                let d = dests[*di];
                let r = rank_in_phase.entry(*pi).or_default();
                let position = Position {
                    phase: self.def.phases[*pi].clone(),
                    phase_index: *pi,
                    rank: *r,
                    chain,
                };
                *r += 1;
                self.check_receives(e, &event, d, &supply);
                let idx = self.push_edge(e, &event, d, Some(position), &supply);
                route.blocking.push(idx);
                supply = self.walk_returns(e, &event, d, supply);
            }
            for d in dests.iter().filter(|d| d.sub.mode == Mode::Async) {
                self.check_receives(e, &event, d, &supply);
                let idx = self.push_edge(e, &event, d, None, &supply);
                route.asyncs.push(idx);
            }
            if !route.blocking.is_empty() || !route.asyncs.is_empty() {
                self.graph.routes.push(route);
            }
        }
        for f in fixed {
            self.report.fixed_orders.push(f);
        }
    }

    /// Observable ties inside a phase (compiler.md §6).
    fn check_ties(
        &mut self,
        e: &Emitter,
        class: &str,
        dests: &[&SubRef<'m>],
        blocking: &[(usize, usize, String, usize)],
        fixed: &mut BTreeSet<String>,
    ) {
        for (i, a) in blocking.iter().enumerate() {
            for b in &blocking[i + 1..] {
                if a.0 != b.0 {
                    continue;
                }
                let both_listed = a.1 != usize::MAX && b.1 != usize::MAX;
                let (sa, sb) = (dests[a.3].sub, dests[b.3].sub);
                let reason = tie_reason(sa, sb)
                    .or_else(|| tie_reason(sb, sa))
                    .or_else(|| {
                        if sa.has_action(ACTION_CONTRIBUTE) && sb.has_action(ACTION_CONTRIBUTE) {
                            Some("both contribute context".to_owned())
                        } else if sa.has_action(ACTION_VETO) || sb.has_action(ACTION_VETO) {
                            let who = if sa.has_action(ACTION_VETO) {
                                &a.2
                            } else {
                                &b.2
                            };
                            Some(format!("`{who}` can veto"))
                        } else {
                            None
                        }
                    });
                let phase = &self.def.phases[a.0];
                match reason {
                    Some(why) if !both_listed => {
                        let path = self.at("order");
                        let key = match &e.family {
                            Some(f) => f.trim_end_matches("/*"),
                            None => class,
                        };
                        self.dx.err("observable-tie", path, Some(class), &[&a.2, &b.2], format!("`{}` and `{}` are both in phase `{phase}` for `{class}` and {why}; order them: `order.\"{key}\".{phase} = [\"{}\", \"{}\"]`", a.2, b.2, a.2, b.2));
                    }
                    None if !both_listed => {
                        fixed.insert(format!(
                            "{class} [{phase}]: {} before {} (no observable tie; by name)",
                            a.2, b.2
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    /// Tie-break lists that name a plugin with no blocking edge there.
    fn check_tie_lists(&mut self, class: &str, emit: &str, dests: &[&SubRef<'m>]) {
        let phases: Vec<String> = self.def.phases.clone();
        for ph in phases {
            let Some((key, list)) = self.order_entry(class, emit, &ph) else {
                continue;
            };
            for p in list {
                if self.host_disabled(p) {
                    continue;
                }
                let present = dests.iter().any(|d| {
                    d.plugin == *p
                        && d.sub.mode == Mode::Blocking
                        && self.phase_of(&d.plugin, d.sub) == Some(ph.as_str())
                });
                let anywhere = self.members.contains_key(p);
                let path = self.at(&format!("order.{key}.{ph}"));
                if !anywhere {
                    self.dx.err("stale-tie-break", path, Some(class), &[p], format!("tie-break for `{class}` phase `{ph}` names `{p}`, which is not a plugin of this definition"));
                } else if !present && dests.iter().any(|d| d.plugin == *p) {
                    self.dx.err("stale-tie-break", path, Some(class), &[p], format!("tie-break for `{class}` phase `{ph}` names `{p}`, which is not a blocking subscriber of `{class}` in phase `{ph}`"));
                }
            }
        }
        for (ev, by_phase) in &self.def.order {
            for ph in by_phase.keys() {
                if !self.def.phases.contains(ph) {
                    let path = self.at("order");
                    self.dx.err("unknown-phase", path, Some(ev), &[], format!("tie-break for `{ev}` names phase `{ph}`, which is not in the definition's phases"));
                }
            }
        }
    }

    fn check_receives(&mut self, e: &Emitter, class: &str, d: &SubRef<'m>, supply: &Keys) {
        for mm in keys_mismatches(supply, &d.sub.receives.keys, "") {
            let code = match mm.kind {
                MismatchKind::Missing { .. } => "starved-consumer",
                MismatchKind::Incompatible { .. } => "shape-mismatch",
            };
            let path = self.at(&format!("plugins.{}", d.plugin));
            self.dx.push(
                D {
                    sev: Severity::Error,
                    code,
                    path,
                    event: Some(class),
                    key: Some(mm.path.clone()),
                    plugins: vec![e.from.clone(), d.plugin.clone()],
                },
                format!(
                    "on the `{} → {}` edge for `{class}`: {mm}",
                    e.from, d.plugin
                ),
            );
        }
    }

    fn walk_returns(&mut self, e: &Emitter, class: &str, d: &SubRef<'m>, mut supply: Keys) -> Keys {
        for (k, r) in &d.sub.returns {
            match supply.get(k) {
                Some(have) => {
                    if let Err(mm) = compatible(&r.shape, &have.shape, k) {
                        let path = self.at(&format!("plugins.{}", d.plugin));
                        self.dx.push(D { sev: Severity::Error, code: "returns-shape-conflict", path, event: Some(class), key: Some(mm.path.clone()), plugins: vec![e.from.clone(), d.plugin.clone()] }, format!("`{}` returns `{k}` as `{}`, but `{}` supplies it as `{}` on `{class}`; a return keeps the key's shape", d.plugin, r.shape, e.from, have.shape));
                    } else if !r.optional {
                        // Always returned: from here on the key is present, with the returned shape.
                        supply.insert(k.clone(), KeyDecl::required(r.shape.clone()));
                    }
                }
                None => {
                    supply.insert(k.clone(), r.clone());
                }
            }
        }
        supply
    }

    fn push_edge(
        &mut self,
        e: &Emitter,
        class: &str,
        d: &SubRef<'m>,
        position: Option<Position>,
        supply: &Keys,
    ) -> usize {
        if let Some(f) = self
            .fed
            .get_mut(&(d.plugin.clone(), d.sub.event.clone(), d.sub.mode))
        {
            *f = true;
        }
        self.graph.edges.push(Edge {
            from: e.from.clone(),
            event: class.to_owned(),
            emit: e.declared().to_owned(),
            to: d.plugin.clone(),
            subscription: d.sub.event.clone(),
            mode: d.sub.mode,
            position,
            lane: e.decl.lane,
            supply: supply.clone(),
            slots: d.sub.receives.slots.keys().cloned().collect(),
            deferred: e.decl.deferred,
            via: e.via,
        });
        self.graph.edges.len() - 1
    }

    /// Connectivity, reachability, and cycles.
    fn analyse(&mut self, emitters: &[Emitter]) {
        // Nodes.
        self.graph.nodes.push(Node {
            name: BOUNDARY.into(),
            kind: NodeKind::Boundary,
            sink: false,
        });
        for (name, m) in &self.plugins {
            self.graph.nodes.push(Node {
                name: name.clone(),
                kind: if m.member {
                    NodeKind::Plugin
                } else {
                    NodeKind::External
                },
                sink: m.manifest.sink,
            });
        }
        // Never fires / unfed subscriptions.
        let members: Vec<(String, &'m Manifest)> =
            self.members.iter().map(|(k, v)| (k.clone(), *v)).collect();
        for (name, m) in &members {
            let path = self.at(&format!("plugins.{name}"));
            let subs: Vec<&Subscription> = m
                .subscribe
                .iter()
                .filter(|s| pattern_error(&s.event, false).is_none())
                .collect();
            let fed: Vec<bool> = subs
                .iter()
                .map(|s| {
                    self.fed
                        .get(&(name.clone(), s.event.clone(), s.mode))
                        .copied()
                        .unwrap_or(false)
                })
                .collect();
            if !fed.iter().any(|f| *f) {
                let list: Vec<&str> = subs.iter().map(|s| s.event.as_str()).collect();
                self.dx.err("never-fires", path, None, &[name], if list.is_empty() {
                    format!("`{name}` subscribes to nothing, so it can never be invoked in this session")
                } else {
                    format!("`{name}` never fires: nothing in this definition emits any event it subscribes to ({})", list.join(", "))
                });
                continue;
            }
            for (s, f) in subs.iter().zip(&fed) {
                if !f {
                    self.dx.warn("unfed-subscription", path.clone(), Some(&s.event), &[name], format!("`{name}` subscribes to `{}`, which nothing in this definition emits and no `inbound` declares", s.event));
                }
            }
        }
        // No effect.
        for (name, m) in &members {
            let consumed = self.graph.edges.iter().any(|e| e.from == *name);
            let returns = m.subscribe.iter().any(|s| !s.returns.is_empty());
            let actions = m.subscribe.iter().any(|s| !s.actions.is_empty());
            let outbound = !m.outbound.is_empty();
            if !(consumed || returns || actions || outbound || m.sink) {
                let path = self.at(&format!("plugins.{name}"));
                self.dx.warn("no-effect", path, None, &[name], format!("`{name}` has no effect here: none of its emits is consumed, it returns no keys, declares no actions, sends nothing outbound, and is not a `sink`"));
            }
        }
        // Reachability from roots.
        let mut reached: BTreeSet<String> = BTreeSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let starters: BTreeSet<String> = self
            .plugins
            .iter()
            .filter(|(_, m)| m.manifest.has(Capability::StartSession))
            .map(|(n, _)| n.clone())
            .collect();
        for e in &self.graph.edges {
            let root = e.from == BOUNDARY
                || e.deferred
                || e.via == Via::StartSession
                || starters.contains(&e.from);
            if root && reached.insert(e.to.clone()) {
                queue.push_back(e.to.clone());
            }
        }
        while let Some(p) = queue.pop_front() {
            for e in self.graph.edges.iter().filter(|e| e.from == p) {
                if reached.insert(e.to.clone()) {
                    queue.push_back(e.to.clone());
                }
            }
        }
        let island: Vec<String> = members
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| !reached.contains(n) && self.graph.edges.iter().any(|e| e.to == *n))
            .collect();
        if !island.is_empty() {
            let names: Vec<&str> = island.iter().map(String::as_str).collect();
            let path = self.at("plugins");
            self.dx.warn("unreachable-island", path, None, &names, format!("{} feed{} each other, but no root (core event, start-session initial event, inbound event, or deferred emit) reaches {}", names.join(", "), if names.len() == 1 { "s" } else { "" }, if names.len() == 1 { "it" } else { "them" }));
        }
        // Cycles (information) and interrupt-only cycles (warning).
        let all: Vec<(String, String)> = self
            .graph
            .edges
            .iter()
            .filter(|e| e.from != BOUNDARY && self.is_member(&e.from))
            .map(|e| (e.from.clone(), e.to.clone()))
            .collect();
        let cycles = scc(&all);
        for c in &cycles {
            let names: Vec<&str> = c.iter().map(String::as_str).collect();
            let path = self.at("plugins");
            self.dx.info("cycle", path, None, &names, format!("cycle {}: legal (agent loops are cycles); termination is a plugin's business, not the core's", c.join(" → ")));
        }
        self.report.cycles = cycles;
        let interrupt: Vec<(String, String)> = self
            .graph
            .edges
            .iter()
            .filter(|e| e.lane == Lane::Interrupt && e.from != BOUNDARY && self.is_member(&e.from))
            .map(|e| (e.from.clone(), e.to.clone()))
            .collect();
        for c in scc(&interrupt) {
            let names: Vec<&str> = c.iter().map(String::as_str).collect();
            let path = self.at("plugins");
            self.dx.warn(
                "interrupt-cycle",
                path,
                None,
                &names,
                format!(
                    "cycle {} consists only of interrupt-lane edges; it can starve the normal lane",
                    c.join(" → ")
                ),
            );
        }
        let _ = emitters;
    }

    fn is_member(&self, name: &str) -> bool {
        self.members.contains_key(name)
    }

    fn report_sections(&mut self, emitters: &[Emitter]) {
        let mut caps: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, m) in &self.plugins {
            for c in &m.manifest.capabilities {
                caps.entry(c.name().to_owned())
                    .or_default()
                    .push(name.clone());
            }
        }
        let mut interrupt = Vec::new();
        for e in emitters.iter().filter(|e| e.decl.lane == Lane::Interrupt) {
            interrupt.push(format!("{}: {} (interrupt lane)", e.from, e.decl.event));
        }
        for p in caps.get("interrupt").cloned().unwrap_or_default() {
            interrupt.push(format!("{p}: holds the interrupt capability"));
        }
        let mut unlisted = Vec::new();
        for (name, m) in &self.members {
            for s in m.subscribe.iter().filter(|s| s.unlisted_keys) {
                unlisted.push(format!("{name}: {}", s.event));
            }
        }
        let mut surface: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, m) in &self.plugins {
            if m.manifest.has(Capability::DeferredPublish) {
                let evs: Vec<String> = m
                    .manifest
                    .emit
                    .iter()
                    .filter(|e| e.deferred)
                    .map(|e| e.event.clone())
                    .collect();
                surface
                    .entry("deferred-publish".into())
                    .or_default()
                    .push(format!("{name}: {}", evs.join(", ")));
            }
            if m.manifest.has(Capability::StartSession) {
                surface
                    .entry("start-session".into())
                    .or_default()
                    .push(name.clone());
            }
            for o in &m.manifest.outbound {
                surface
                    .entry("outbound".into())
                    .or_default()
                    .push(format!("{name}: {}", o.event));
            }
        }
        for ev in self.def.inbound.keys() {
            surface
                .entry("inbound".into())
                .or_default()
                .push(ev.clone());
        }
        for (p, list) in &caps {
            let names: Vec<&str> = list.iter().map(String::as_str).collect();
            let path = self.at("plugins");
            self.dx.info(
                "capability",
                path,
                None,
                &names,
                format!("capability `{p}` held by {}", list.join(", ")),
            );
        }
        for u in &unlisted {
            let path = self.at("plugins");
            let plugin = u.split(':').next().unwrap_or_default().to_owned();
            self.dx.info(
                "unlisted-keys",
                path,
                None,
                &[&plugin],
                format!("{u} receives unlisted keys untyped (outside the compiler's guarantee)"),
            );
        }
        for i in &interrupt {
            let path = self.at("plugins");
            self.dx.info("interrupt", path, None, &[], i.clone());
        }
        for (k, v) in &surface {
            let path = self.at("plugins");
            self.dx.info(
                "external-surface",
                path,
                None,
                &[],
                format!("{k}: {}", v.join("; ")),
            );
        }
        self.report.capabilities = caps;
        self.report.interrupt = interrupt;
        self.report.unlisted_keys = unlisted;
        self.report.external_surface = surface;
    }
}

/// Are two shapes the same, ignoring documentation (descriptions)?
fn same_shape(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Array(x), Shape::Array(y)) => same_shape(x, y),
        (Shape::Object(Some(x)), Shape::Object(Some(y))) => {
            x.len() == y.len()
                && x.iter().zip(y).all(|((ka, da), (kb, db))| {
                    ka == kb && da.optional == db.optional && same_shape(&da.shape, &db.shape)
                })
        }
        (Shape::Union(x), Shape::Union(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_shape(p, q))
        }
        _ => a == b,
    }
}

/// Does `a` return a key `b` receives or returns (or, with unlisted keys, any key)?
fn tie_reason(a: &Subscription, b: &Subscription) -> Option<String> {
    for k in a.returns.keys() {
        if b.receives.keys.contains_key(k) {
            return Some(format!("one returns `{k}`, which the other receives"));
        }
        if b.returns.contains_key(k) {
            return Some(format!("both return `{k}`"));
        }
    }
    if !a.returns.is_empty() && b.unlisted_keys {
        return Some("one returns keys and the other receives unlisted keys".into());
    }
    None
}

/// Strongly connected components with more than one node (Tarjan), sorted.
fn scc(edges: &[(String, String)]) -> Vec<Vec<String>> {
    let mut graph: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (a, b) in edges {
        graph.entry(a).or_default().insert(b);
        graph.entry(b).or_default();
    }
    struct T<'g> {
        g: &'g BTreeMap<&'g str, BTreeSet<&'g str>>,
        index: usize,
        idx: BTreeMap<&'g str, usize>,
        low: BTreeMap<&'g str, usize>,
        stack: Vec<&'g str>,
        on: BTreeSet<&'g str>,
        out: Vec<Vec<String>>,
    }
    fn strong<'g>(t: &mut T<'g>, v: &'g str) {
        t.idx.insert(v, t.index);
        t.low.insert(v, t.index);
        t.index += 1;
        t.stack.push(v);
        t.on.insert(v);
        let next: Vec<&'g str> =
            t.g.get(v)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
        for w in next {
            if !t.idx.contains_key(w) {
                strong(t, w);
                let lw = t.low[w];
                let lv = t.low.get_mut(v).expect("visited");
                *lv = (*lv).min(lw);
            } else if t.on.contains(w) {
                let iw = t.idx[w];
                let lv = t.low.get_mut(v).expect("visited");
                *lv = (*lv).min(iw);
            }
        }
        if t.low[v] == t.idx[v] {
            let mut comp = Vec::new();
            while let Some(w) = t.stack.pop() {
                t.on.remove(w);
                comp.push(w.to_owned());
                if w == v {
                    break;
                }
            }
            if comp.len() > 1 {
                comp.sort();
                t.out.push(comp);
            }
        }
    }
    let mut t = T {
        g: &graph,
        index: 0,
        idx: BTreeMap::new(),
        low: BTreeMap::new(),
        stack: Vec::new(),
        on: BTreeSet::new(),
        out: Vec::new(),
    };
    let nodes: Vec<&str> = graph.keys().copied().collect();
    for n in nodes {
        if !t.idx.contains_key(n) {
            strong(&mut t, n);
        }
    }
    t.out.sort();
    t.out
}

/// Summarize a compilation as JSON for display.
#[must_use]
pub fn summary(c: &Compilation) -> Value {
    json!({
        "hash": c.hash,
        "ok": c.ok,
        "definitions": c.definitions.iter().map(|(k, r)| (k.clone(), json!({ "revision": r.revision, "ok": r.ok }))).collect::<BTreeMap<_, _>>(),
        "errors": c.errors().len(),
        "diagnostics": c.diagnostics,
    })
}
