//! The compiler: walks every declared subscription and emission and produces a
//! validated per-definition graph. The graph is a compile-time artifact, never
//! an executor — dispatch only looks up the compiled pipeline for an event name.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::manifest::{
    Capability, DeploymentConfig, Emit, Keyed, Manifest, PluginConfig, Transform, WILDCARD,
};
use crate::types::{Mode, Stamp, hash_json, lookup};

/// Core lifecycle events and the keys they supply.
pub const CORE_EVENTS: &[(&str, &[&str])] = &[
    ("session-started", &["definition"]),
    ("config-applied", &["config"]),
    // A keyed event found no owner, or its owner's invocation failed (so the
    // loop that waits on an answer can be told instead of waiting forever).
    (
        DISPATCH_FAILED,
        &["reason", "event_id", "event_name", "key", "payload"],
    ),
];

/// Core event published when keyed dispatch cannot deliver an answerable result.
pub const DISPATCH_FAILED: &str = "dispatch-failed";

/// The emitter name used for core lifecycle events in the graph.
pub const CORE: &str = "core";

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

/// A compiler finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    pub message: String,
}

/// One subscriber slot in a compiled pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slot {
    pub plugin: String,
    pub mode: Mode,
    /// Position in the definition's declared order.
    pub position: usize,
    pub demands: Vec<String>,
    pub context: bool,
    pub wildcard: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Transform>,
}

/// Compiled pipeline for one event name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Pipeline {
    pub blocking: Vec<Slot>,
    /// Observers: every delivered event reaches all of them.
    #[serde(rename = "async")]
    pub asyncs: Vec<Slot>,
    /// Keyed owners: each delivered event reaches exactly the one owner its key
    /// selects (compiled from [`Keyed`] consumers; never a runtime scan).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routed: Option<RouteTable>,
}

impl Pipeline {
    /// The slot a plugin occupies in this pipeline (blocking, observer, or owner).
    #[must_use]
    pub fn slot_of(&self, plugin: &str) -> Option<&Slot> {
        self.blocking
            .iter()
            .chain(&self.asyncs)
            .find(|s| s.plugin == plugin)
            .or_else(|| self.routed.as_ref().and_then(|r| r.slot_of(plugin)))
    }
}

/// A prefix-owned family of key values (`mcp__github__*`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixRoute {
    pub prefix: String,
    pub slot: Slot,
}

/// Deterministic owner table for one keyed event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteTable {
    /// Payload key whose value selects the owner.
    pub key: String,
    pub exact: BTreeMap<String, Slot>,
    /// Longest prefix wins; overlaps between owners are rejected at compile time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefixes: Vec<PrefixRoute>,
}

impl RouteTable {
    /// The owner of a key value.
    #[must_use]
    pub fn owner(&self, value: &str) -> Option<&Slot> {
        self.exact.get(value).or_else(|| {
            self.prefixes
                .iter()
                .filter(|p| value.starts_with(&p.prefix))
                .max_by_key(|p| p.prefix.len())
                .map(|p| &p.slot)
        })
    }

    /// The owner selected by an event payload, with the key value.
    #[must_use]
    pub fn route<'a>(&'a self, payload: &Value) -> (Option<String>, Option<&'a Slot>) {
        let value = lookup(payload, &self.key).and_then(Value::as_str);
        (value.map(str::to_owned), value.and_then(|v| self.owner(v)))
    }

    fn slot_of(&self, plugin: &str) -> Option<&Slot> {
        self.exact
            .values()
            .chain(self.prefixes.iter().map(|p| &p.slot))
            .find(|s| s.plugin == plugin)
    }

    /// Every owned value (prefixes keep their `*`), with its owner.
    #[must_use]
    pub fn entries(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .exact
            .iter()
            .map(|(v, s)| (v.clone(), s.plugin.clone()))
            .collect();
        out.extend(
            self.prefixes
                .iter()
                .map(|p| (format!("{}*", p.prefix), p.slot.plugin.clone())),
        );
        out
    }
}

/// Edge in the compiled graph (for inspection UIs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// `emits` (plugin → event) or `consumes` (event → plugin).
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deferred: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
}

/// One compiled session definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CompiledDefinition {
    pub description: String,
    pub plugins: Vec<String>,
    /// Explicit pipelines keyed by event name (wildcards already placed).
    pub pipelines: BTreeMap<String, Pipeline>,
    /// Pipeline used for events with no explicit subscribers (wildcards only).
    pub fallback: Pipeline,
    pub edges: Vec<Edge>,
    /// Event cycles (legal; bounded at runtime by max causal depth).
    pub cycles: Vec<Vec<String>>,
    /// Content hash of every route table: the routing revision this config
    /// compiled (changes only when ownership changes).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub routes_revision: String,
}

impl CompiledDefinition {
    /// The pipeline an event name dispatches through.
    #[must_use]
    pub fn pipeline(&self, event: &str) -> &Pipeline {
        self.pipelines.get(event).unwrap_or(&self.fallback)
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

/// The compile output artifact; reused by apply-time revalidation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Compilation {
    /// Content hash of config + manifests; identifies this compiled config.
    pub hash: String,
    pub ok: bool,
    pub config: DeploymentConfig,
    pub manifests: BTreeMap<String, Manifest>,
    pub stamps: BTreeMap<String, Stamp>,
    pub definitions: BTreeMap<String, CompiledDefinition>,
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

struct Ctx<'a> {
    diags: Vec<Diagnostic>,
    def: Option<&'a str>,
}

impl Ctx<'_> {
    fn push(
        &mut self,
        severity: Severity,
        code: &str,
        plugin: Option<&str>,
        event: Option<&str>,
        message: String,
    ) {
        self.diags.push(Diagnostic {
            severity,
            code: code.to_owned(),
            definition: self.def.map(str::to_owned),
            plugin: plugin.map(str::to_owned),
            event: event.map(str::to_owned),
            message,
        });
    }
}

fn strip_optional(key: &str) -> Option<&str> {
    key.strip_suffix('?')
}

/// Does a supplied key set cover a demanded key? Supplying `a` covers `a.b`;
/// supplying `a.b` covers a demand for `a` (the object exists).
fn covers(supply: &BTreeSet<String>, demand: &str) -> bool {
    supply.iter().any(|s| {
        s == demand || demand.starts_with(&format!("{s}.")) || s.starts_with(&format!("{demand}."))
    })
}

/// Walk a supply set through one transformer.
fn walk_transform(supply: &BTreeSet<String>, t: &Transform) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = if t.preserves.iter().any(|p| p == WILDCARD) {
        supply.clone()
    } else {
        supply
            .iter()
            .filter(|k| {
                t.preserves
                    .iter()
                    .any(|p| covers(&BTreeSet::from([p.clone()]), k))
            })
            .cloned()
            .collect()
    };
    out.extend(t.adds.iter().cloned());
    out
}

/// Compile a deployment config against the manifests obtained at handshake.
#[must_use]
pub fn compile(config: &DeploymentConfig, manifests: &BTreeMap<String, Manifest>) -> Compilation {
    let mut cx = Ctx {
        diags: Vec::new(),
        def: None,
    };
    let active: BTreeMap<&String, &PluginConfig> =
        config.plugins.iter().filter(|(_, p)| !p.disabled).collect();

    // Plugin-level checks.
    for name in active.keys() {
        let Some(m) = manifests.get(*name) else {
            cx.push(
                Severity::Error,
                "missing-manifest",
                Some(name),
                None,
                format!("plugin `{name}` produced no manifest (failed to start or handshake?)"),
            );
            continue;
        };
        let mut seen = BTreeSet::new();
        for e in &m.emits {
            if e.event == WILDCARD {
                cx.push(
                    Severity::Error,
                    "emit-wildcard",
                    Some(name),
                    None,
                    "emit-wildcards do not exist; declare each emitted event".into(),
                );
            }
            if !seen.insert(&e.event) {
                cx.push(
                    Severity::Error,
                    "duplicate-emit",
                    Some(name),
                    Some(&e.event),
                    format!("`{}` is declared twice in emits", e.event),
                );
            }
            if e.deferred && !m.has(Capability::DeferredPublish) {
                cx.push(Severity::Error, "missing-capability", Some(name), Some(&e.event), format!("`{}` is flagged deferred but the plugin lacks the deferred-publish capability", e.event));
            }
        }
        for c in &m.consumes {
            if c.event == WILDCARD && c.demands.iter().any(|d| strip_optional(d).is_none()) {
                cx.push(
                    Severity::Error,
                    "wildcard-demand",
                    Some(name),
                    Some(WILDCARD),
                    "wildcard consumers may demand nothing beyond the envelope".into(),
                );
            }
        }
        check_tools(&mut cx, name, m);
        if let Some(want) = active[*name].version.as_deref()
            && m.version != want
            && !m.version.starts_with(&format!("{want}."))
        {
            cx.push(
                Severity::Error,
                "plugin-version-unavailable",
                Some(name),
                None,
                format!(
                    "the config requires `{name}` version {want}, but the running plugin reports {}",
                    m.version
                ),
            );
        }
        for t in &m.transforms {
            if !m.consumes.iter().any(|c| c.event == t.event) {
                cx.push(
                    Severity::Error,
                    "transform-unconsumed",
                    Some(name),
                    Some(&t.event),
                    format!(
                        "transform declared for `{}` which the plugin does not consume",
                        t.event
                    ),
                );
            }
        }
    }

    let mut definitions = BTreeMap::new();
    for (def_name, def) in &config.definitions {
        cx.def = Some(def_name);
        let first_diag = cx.diags.len();
        // A host disables plugins it cannot run (a worker-only plugin natively,
        // a process-only one in the browser). A definition that is broken only
        // because of that is unavailable on this host, not a configuration error.
        let host_scoped = def
            .subscribers
            .iter()
            .any(|s| config.plugins.get(&s.plugin).is_some_and(|p| p.disabled));
        let mut cd = CompiledDefinition {
            description: def.description.clone(),
            ..Default::default()
        };
        let mut members: Vec<(usize, &str, &Manifest, Option<Mode>)> = Vec::new();
        let mut seen = BTreeSet::new();
        for (pos, sub) in def.subscribers.iter().enumerate() {
            if !seen.insert(sub.plugin.as_str()) {
                cx.push(
                    Severity::Error,
                    "duplicate-subscriber",
                    Some(&sub.plugin),
                    None,
                    format!("`{}` listed twice", sub.plugin),
                );
                continue;
            }
            match config.plugins.get(&sub.plugin) {
                None => cx.push(
                    Severity::Error,
                    "unknown-plugin",
                    Some(&sub.plugin),
                    None,
                    format!("definition references unknown plugin `{}`", sub.plugin),
                ),
                Some(p) if p.disabled => cx.push(
                    Severity::Info,
                    "disabled-plugin",
                    Some(&sub.plugin),
                    None,
                    format!("`{}` is disabled on this host and skipped", sub.plugin),
                ),
                Some(_) => {
                    if let Some(m) = manifests.get(&sub.plugin) {
                        members.push((pos, sub.plugin.as_str(), m, sub.mode));
                        cd.plugins.push(sub.plugin.clone());
                    }
                }
            }
        }

        // Emitters per event: members, core lifecycle, plus outside plugins that can
        // reach this session (start-session initial events, cross-session publishes).
        let mut emitters: BTreeMap<String, Vec<(String, Emit)>> = BTreeMap::new();
        for (ev, keys) in CORE_EVENTS {
            emitters.entry((*ev).to_owned()).or_default().push((
                CORE.to_owned(),
                Emit {
                    event: (*ev).to_owned(),
                    supplies: keys.iter().map(|k| (*k).to_owned()).collect(),
                    deferred: false,
                },
            ));
        }
        let member_names: BTreeSet<&str> = members.iter().map(|m| m.1).collect();
        for (name, m) in manifests.iter().filter(|(n, _)| active.contains_key(n)) {
            let inside = member_names.contains(name.as_str());
            let reaches = m.has(Capability::StartSession) || m.has(Capability::CrossSession);
            if inside || reaches {
                for e in &m.emits {
                    emitters
                        .entry(e.event.clone())
                        .or_default()
                        .push((name.clone(), e.clone()));
                }
            }
            for e in m.emits.iter().filter(|_| inside) {
                cd.edges.push(Edge {
                    from: name.clone(),
                    to: e.event.clone(),
                    kind: "emits".into(),
                    mode: None,
                    deferred: e.deferred,
                    keys: e.supplies.clone(),
                });
            }
            if !inside && reaches {
                for e in &m.emits {
                    cd.edges.push(Edge {
                        from: name.clone(),
                        to: e.event.clone(),
                        kind: "external".into(),
                        mode: None,
                        deferred: e.deferred,
                        keys: e.supplies.clone(),
                    });
                }
            }
        }
        for (ev, keys) in CORE_EVENTS {
            cd.edges.push(Edge {
                from: CORE.into(),
                to: (*ev).into(),
                kind: "emits".into(),
                mode: None,
                deferred: false,
                keys: keys.iter().map(|k| (*k).to_owned()).collect(),
            });
        }

        // Build slots per event name.
        let mut wildcard_slots: Vec<Slot> = Vec::new();
        let mut explicit: BTreeMap<String, Vec<Slot>> = BTreeMap::new();
        let mut keyed: BTreeMap<String, Vec<(Slot, Keyed)>> = BTreeMap::new();
        for (pos, name, m, override_mode) in &members {
            for c in &m.consumes {
                let mode = override_mode.or(c.mode).unwrap_or_default();
                if let Some(k) = &c.keyed {
                    if c.event == WILDCARD || mode != Mode::Async {
                        cx.push(
                            Severity::Error,
                            "keyed-consumer",
                            Some(name),
                            Some(&c.event),
                            "keyed (owner) consumers must name an event and run async: they execute, they do not gate".into(),
                        );
                        continue;
                    }
                    cd.edges.push(Edge {
                        from: c.event.clone(),
                        to: (*name).to_owned(),
                        kind: "owns".into(),
                        mode: Some(mode),
                        deferred: false,
                        keys: k.values.clone(),
                    });
                    keyed.entry(c.event.clone()).or_default().push((
                        Slot {
                            plugin: (*name).to_owned(),
                            mode,
                            position: *pos,
                            demands: c.demands.clone(),
                            context: c.context,
                            wildcard: false,
                            transform: None,
                        },
                        k.clone(),
                    ));
                    continue;
                }
                let transform = m.transform(&c.event).cloned();
                if transform.is_some() && mode == Mode::Async {
                    cx.push(
                        Severity::Error,
                        "async-transform",
                        Some(name),
                        Some(&c.event),
                        "a transform requires blocking mode; async subscribers are read-only"
                            .into(),
                    );
                }
                let slot = Slot {
                    plugin: (*name).to_owned(),
                    mode,
                    position: *pos,
                    demands: c.demands.clone(),
                    context: c.context,
                    wildcard: c.event == WILDCARD,
                    transform: if mode == Mode::Blocking {
                        transform
                    } else {
                        None
                    },
                };
                cd.edges.push(Edge {
                    from: c.event.clone(),
                    to: (*name).to_owned(),
                    kind: "consumes".into(),
                    mode: Some(mode),
                    deferred: false,
                    keys: c.demands.clone(),
                });
                if c.event == WILDCARD {
                    wildcard_slots.push(slot);
                } else {
                    explicit.entry(c.event.clone()).or_default().push(slot);
                }
            }
        }

        let place = |mut slots: Vec<Slot>| -> Pipeline {
            slots.extend(wildcard_slots.iter().cloned());
            slots.sort_by_key(|s| (s.position, s.wildcard));
            let (blocking, asyncs) = slots.into_iter().partition(|s| s.mode == Mode::Blocking);
            Pipeline {
                blocking,
                asyncs,
                routed: None,
            }
        };
        cd.fallback = place(Vec::new());
        let def_owners = &def.route_owners;
        let mut routes: BTreeMap<String, RouteTable> = BTreeMap::new();
        for (event, owners) in &keyed {
            explicit.entry(event.clone()).or_default();
            let table = build_routes(&mut cx, event, owners, def_owners.get(event));
            routes.insert(event.clone(), table);
        }
        for (event, pins) in def_owners {
            if !keyed.contains_key(event) {
                for (value, plugin) in pins {
                    cx.push(Severity::Error, "stale-route-owner", Some(plugin), Some(event), format!("route_owners pins `{value}` to `{plugin}`, but no member of this definition owns keyed `{event}` events"));
                }
            }
        }

        for (event, slots) in explicit {
            let mut pipeline = place(slots);
            pipeline.routed = routes.remove(&event);
            if let Some(r) = &pipeline.routed {
                for s in pipeline.asyncs.iter().filter(|s| !s.wildcard) {
                    cx.push(Severity::Info, "unkeyed-consumer", Some(&s.plugin), Some(&event), format!("`{}` observes every `{event}` (it is not one of the keyed owners of `{}`); declare `keyed` values if it executes some of them", s.plugin, r.key));
                }
            }
            let owner_slots: Vec<Slot> = keyed
                .get(&event)
                .map(|v| v.iter().map(|(s, _)| s.clone()).collect())
                .unwrap_or_default();
            // Dead listener: an explicit consumer of an event nobody can emit.
            let ems = emitters.get(&event).cloned().unwrap_or_default();
            if ems.is_empty() {
                for s in pipeline
                    .blocking
                    .iter()
                    .chain(&pipeline.asyncs)
                    .chain(&owner_slots)
                    .filter(|s| !s.wildcard)
                {
                    cx.push(Severity::Error, "dead-listener", Some(&s.plugin), Some(&event), format!("`{}` consumes `{event}` but no configured plugin emits it; this pipeline can never fire", s.plugin));
                }
            }
            // Starved consumer: walk supply through the blocking transform chain.
            for (emitter, emit) in &ems {
                let base: BTreeSet<String> = emit.supplies.iter().cloned().collect();
                let mut supply = base.clone();
                for s in &pipeline.blocking {
                    check_demands(&mut cx, s, &supply, emitter, &event);
                    if let Some(t) = &s.transform {
                        supply = walk_transform(&supply, t);
                    }
                }
                for s in pipeline.asyncs.iter().chain(&owner_slots) {
                    check_demands(&mut cx, s, &supply, emitter, &event);
                }
            }
            cd.pipelines.insert(event, pipeline);
        }
        let tables: BTreeMap<&String, &RouteTable> = cd
            .pipelines
            .iter()
            .filter_map(|(e, p)| p.routed.as_ref().map(|r| (e, r)))
            .collect();
        if !tables.is_empty() {
            cd.routes_revision = hash_json(&json!(tables))[..12].to_owned();
        }

        cd.cycles = find_cycles(&members);
        for cycle in &cd.cycles {
            cx.push(
                Severity::Info,
                "cycle",
                None,
                cycle.first().map(String::as_str),
                format!(
                    "event cycle {} is bounded at runtime by max_causal_depth={}",
                    cycle.join(" → "),
                    config.runtime.max_causal_depth
                ),
            );
        }
        if host_scoped {
            let broken: Vec<Diagnostic> = cx.diags[first_diag..]
                .iter()
                .filter(|d| d.severity == Severity::Error)
                .cloned()
                .collect();
            if let Some(first) = broken.first() {
                cx.diags.retain(|d| {
                    d.definition.as_deref() != Some(def_name.as_str())
                        || d.severity != Severity::Error
                });
                cx.push(
                    Severity::Warning,
                    "definition-unavailable",
                    None,
                    None,
                    format!(
                        "`{def_name}` cannot run on this host (its disabled plugins leave it incomplete: {}); skipped",
                        first.message
                    ),
                );
                continue;
            }
        }
        definitions.insert(def_name.clone(), cd);
    }
    cx.def = None;

    // External surface: every capability holder.
    let mut surface = Vec::new();
    for (name, m) in manifests.iter().filter(|(n, _)| active.contains_key(n)) {
        for cap in &m.capabilities {
            let deferred_events = if *cap == Capability::DeferredPublish {
                m.emits
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
    let used: BTreeMap<&String, &Manifest> = manifests
        .iter()
        .filter(|(n, _)| config.plugins.contains_key(*n))
        .collect();
    let hash = hash_json(&json!({ "config": config, "manifests": used, "stamps": stamps }))[..16]
        .to_owned();
    cx.diags.sort_by(|a, b| {
        a.severity
            .cmp(&b.severity)
            .then(a.code.cmp(&b.code))
            .then(a.definition.cmp(&b.definition))
            .then(a.plugin.cmp(&b.plugin))
            .then(a.event.cmp(&b.event))
    });
    let ok = !cx.diags.iter().any(|d| d.severity == Severity::Error);
    Compilation {
        hash,
        ok,
        config: config.clone(),
        manifests: used
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        stamps,
        definitions,
        diagnostics: cx.diags,
        surface,
    }
}

/// Validate a plugin's tool declarations against its own consumes.
fn check_tools(cx: &mut Ctx<'_>, name: &str, m: &Manifest) {
    let mut seen = BTreeSet::new();
    for t in &m.tools {
        if !seen.insert(t.name.as_str()) {
            cx.push(
                Severity::Error,
                "duplicate-tool",
                Some(name),
                None,
                format!("tool `{}` is declared twice", t.name),
            );
        }
        if !(t.parameters.is_null() || t.parameters.is_object()) {
            cx.push(
                Severity::Error,
                "invalid-tool-schema",
                Some(name),
                None,
                format!(
                    "tool `{}`: parameters must be an object of properties",
                    t.name
                ),
            );
            continue;
        }
        let missing: Vec<&str> = t
            .required
            .iter()
            .filter(|r| t.parameters.get(r.as_str()).is_none())
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            cx.push(
                Severity::Error,
                "invalid-tool-schema",
                Some(name),
                None,
                format!(
                    "tool `{}` requires [{}] which its parameters do not define",
                    t.name,
                    missing.join(", ")
                ),
            );
        }
    }
    // A plugin that declares tools and owns keyed events must own each tool.
    let keyed: Vec<&Keyed> = m.consumes.iter().filter_map(|c| c.keyed.as_ref()).collect();
    if keyed.is_empty() && !m.tools.is_empty() {
        cx.push(Severity::Error, "missing-route-owner", Some(name), None, format!("`{name}` declares tools ({}) but owns no keyed event, so nothing would route them to it", m.tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", ")));
    }
    if let [k] = keyed.as_slice() {
        for t in &m.tools {
            if !k.owns(&t.name) && !k.values.contains(&t.name) {
                cx.push(Severity::Error, "undeclared-tool-route", Some(name), None, format!("tool `{}` is declared but `{name}` does not own it in its keyed consume (values: {})", t.name, k.values.join(", ")));
            }
        }
    }
}

/// Compile the owner table of one keyed event, rejecting ambiguity.
fn build_routes(
    cx: &mut Ctx<'_>,
    event: &str,
    owners: &[(Slot, Keyed)],
    pins: Option<&BTreeMap<String, String>>,
) -> RouteTable {
    let key = owners[0].1.key.clone();
    for (slot, k) in owners.iter().filter(|(_, k)| k.key != key) {
        cx.push(
            Severity::Error,
            "route-key-mismatch",
            Some(&slot.plugin),
            Some(event),
            format!(
                "`{}` keys `{event}` by `{}` but other owners key it by `{key}`",
                slot.plugin, k.key
            ),
        );
    }
    // Every claim: value -> claimants (exact and prefix claims kept apart).
    let mut exact: BTreeMap<String, Vec<&Slot>> = BTreeMap::new();
    let mut prefix: BTreeMap<String, Vec<&Slot>> = BTreeMap::new();
    for (slot, k) in owners {
        for v in &k.values {
            match v.strip_suffix('*') {
                Some(p) => prefix.entry(p.to_owned()).or_default().push(slot),
                None => exact.entry(v.clone()).or_default().push(slot),
            }
        }
    }
    let empty = BTreeMap::new();
    let pins = pins.unwrap_or(&empty);
    for (value, plugin) in pins {
        let claimed = exact
            .get(value)
            .into_iter()
            .chain(prefix.get(value.trim_end_matches('*')))
            .flatten()
            .any(|s| &s.plugin == plugin)
            || prefix.iter().any(|(p, ss)| {
                value.starts_with(p.as_str()) && ss.iter().any(|s| &s.plugin == plugin)
            });
        if !claimed {
            cx.push(Severity::Error, "stale-route-owner", Some(plugin), Some(event), format!("route_owners pins `{event}` `{value}` to `{plugin}`, which does not own it in this definition (stale or disabled plugin?)"));
        }
    }
    let pick = |cx: &mut Ctx<'_>, value: &str, claimants: &[&Slot]| -> Option<Slot> {
        let mut distinct: Vec<&Slot> = Vec::new();
        for s in claimants {
            if !distinct.iter().any(|d| d.plugin == s.plugin) {
                distinct.push(s);
            }
        }
        if let Some(plugin) = pins.get(value) {
            return distinct
                .iter()
                .find(|s| &s.plugin == plugin)
                .map(|s| (*s).clone());
        }
        if distinct.len() > 1 {
            let names: Vec<&str> = distinct.iter().map(|s| s.plugin.as_str()).collect();
            cx.push(Severity::Error, "duplicate-route-owner", Some(names[1]), Some(event), format!("`{event}` `{value}` is owned by more than one plugin ({}); remove one or pin it with route_owners", names.join(", ")));
            return None;
        }
        distinct.first().map(|s| (*s).clone())
    };
    let mut table = RouteTable {
        key,
        ..Default::default()
    };
    for (value, claimants) in &exact {
        // An exact value also claimed through another plugin's prefix is ambiguous.
        let mut all: Vec<&Slot> = claimants.clone();
        for (p, ss) in &prefix {
            if value.starts_with(p.as_str()) {
                all.extend(
                    ss.iter()
                        .copied()
                        .filter(|s| !claimants.iter().any(|c| c.plugin == s.plugin)),
                );
            }
        }
        if let Some(slot) = pick(cx, value, &all) {
            table.exact.insert(value.clone(), slot);
        }
    }
    let prefixes: Vec<&String> = prefix.keys().collect();
    for (p, claimants) in &prefix {
        let mut all: Vec<&Slot> = claimants.clone();
        for other in prefixes
            .iter()
            .filter(|o| **o != p && (o.starts_with(p.as_str()) || p.starts_with(o.as_str())))
        {
            all.extend(
                prefix[*other]
                    .iter()
                    .copied()
                    .filter(|s| !claimants.iter().any(|c| c.plugin == s.plugin)),
            );
        }
        if let Some(slot) = pick(cx, &format!("{p}*"), &all) {
            table.prefixes.push(PrefixRoute {
                prefix: p.clone(),
                slot,
            });
        }
    }
    table
}

fn check_demands(
    cx: &mut Ctx<'_>,
    slot: &Slot,
    supply: &BTreeSet<String>,
    emitter: &str,
    event: &str,
) {
    let missing: Vec<&str> = slot
        .demands
        .iter()
        .filter(|d| strip_optional(d).is_none() && !covers(supply, d))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        cx.push(
            Severity::Error,
            "starved-consumer",
            Some(&slot.plugin),
            Some(event),
            format!("`{}` demands [{}] on `{event}` but emitter `{emitter}` (after upstream transforms) does not supply them", slot.plugin, missing.join(", ")),
        );
    }
}

/// Strongly connected components of the event graph (consume E → emit F).
fn find_cycles(members: &[(usize, &str, &Manifest, Option<Mode>)]) -> Vec<Vec<String>> {
    let mut graph: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (_, _, m, _) in members {
        for c in m.consumes.iter().filter(|c| c.event != WILDCARD) {
            for e in &m.emits {
                graph
                    .entry(c.event.clone())
                    .or_default()
                    .insert(e.event.clone());
            }
        }
    }
    // Tarjan's algorithm, iterative over sorted nodes for determinism.
    struct T<'g> {
        g: &'g BTreeMap<String, BTreeSet<String>>,
        index: usize,
        idx: BTreeMap<String, usize>,
        low: BTreeMap<String, usize>,
        stack: Vec<String>,
        on: BTreeSet<String>,
        out: Vec<Vec<String>>,
    }
    fn strong(t: &mut T<'_>, v: &str) {
        t.idx.insert(v.to_owned(), t.index);
        t.low.insert(v.to_owned(), t.index);
        t.index += 1;
        t.stack.push(v.to_owned());
        t.on.insert(v.to_owned());
        let next: Vec<String> =
            t.g.get(v)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
        for w in next {
            if !t.idx.contains_key(&w) {
                strong(t, &w);
                let lw = t.low[&w];
                let lv = t.low.get_mut(v).expect("visited");
                *lv = (*lv).min(lw);
            } else if t.on.contains(&w) {
                let iw = t.idx[&w];
                let lv = t.low.get_mut(v).expect("visited");
                *lv = (*lv).min(iw);
            }
        }
        if t.low[v] == t.idx[v] {
            let mut comp = Vec::new();
            while let Some(w) = t.stack.pop() {
                t.on.remove(&w);
                let done = w == v;
                comp.push(w);
                if done {
                    break;
                }
            }
            let self_loop = comp.len() == 1 && t.g.get(v).is_some_and(|s| s.contains(v));
            if comp.len() > 1 || self_loop {
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
    let nodes: Vec<String> = graph.keys().cloned().collect();
    for n in nodes {
        if !t.idx.contains_key(&n) {
            strong(&mut t, &n);
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
        "definitions": c.definitions.keys().collect::<Vec<_>>(),
        "errors": c.errors().len(),
        "diagnostics": c.diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Consume, Definition, Subscriber};

    fn consume(event: &str, demands: &[&str]) -> Consume {
        Consume {
            event: event.into(),
            demands: demands.iter().map(|s| (*s).into()).collect(),
            mode: None,
            context: true,
            keyed: None,
        }
    }
    fn emit(event: &str, supplies: &[&str]) -> Emit {
        Emit {
            event: event.into(),
            supplies: supplies.iter().map(|s| (*s).into()).collect(),
            deferred: false,
        }
    }
    fn manifest(name: &str, consumes: Vec<Consume>, emits: Vec<Emit>) -> Manifest {
        Manifest {
            name: name.into(),
            version: "1".into(),
            description: String::new(),
            consumes,
            emits,
            transforms: vec![],
            capabilities: vec![],
            config_schema: Value::Null,
            ..Default::default()
        }
    }
    fn setup(
        ms: Vec<Manifest>,
        subs: &[(&str, Option<Mode>)],
    ) -> (DeploymentConfig, BTreeMap<String, Manifest>) {
        let mut cfg = DeploymentConfig::default();
        for m in &ms {
            cfg.plugins.insert(
                m.name.clone(),
                PluginConfig {
                    command: vec![],
                    module: None,
                    config: Value::Null,
                    timeout_ms: None,
                    binary_hash: None,
                    disabled: false,
                    version: None,
                },
            );
        }
        cfg.definitions.insert(
            "d".into(),
            Definition {
                description: String::new(),
                subscribers: subs
                    .iter()
                    .map(|(p, m)| Subscriber {
                        plugin: (*p).into(),
                        mode: *m,
                    })
                    .collect(),
                ..Default::default()
            },
        );
        (cfg, ms.into_iter().map(|m| (m.name.clone(), m)).collect())
    }
    fn codes(c: &Compilation) -> Vec<String> {
        c.diagnostics.iter().map(|d| d.code.clone()).collect()
    }

    #[test]
    fn valid_chain_compiles_in_declared_order() {
        let a = manifest(
            "a",
            vec![consume("session-started", &["definition"])],
            vec![emit("x", &["k"])],
        );
        let b = manifest("b", vec![consume("x", &["k"])], vec![]);
        let c = manifest("c", vec![consume("x", &[])], vec![]);
        let (cfg, ms) = setup(vec![a, b, c], &[("c", None), ("a", None), ("b", None)]);
        let out = compile(&cfg, &ms);
        assert!(out.ok, "{:?}", out.diagnostics);
        let p = out.definitions["d"].pipeline("x");
        assert_eq!(
            p.blocking
                .iter()
                .map(|s| s.plugin.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "b"]
        );
    }

    #[test]
    fn a_definition_left_incomplete_by_host_disabled_plugins_is_skipped() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[])],
            vec![emit("x", &["k"])],
        );
        let b = manifest("b", vec![consume("x", &["k"])], vec![]);
        let (mut cfg, ms) = setup(vec![a, b], &[("a", None), ("b", None)]);
        let healthy = cfg.definitions["d"].clone();
        cfg.definitions.insert("ok".into(), healthy);
        cfg.plugins.get_mut("a").unwrap().disabled = true;
        cfg.definitions
            .get_mut("ok")
            .unwrap()
            .subscribers
            .retain(|s| s.plugin == "b");
        let out = compile(&cfg, &ms);
        // `d` lost its only emitter of `x` to the host: unavailable, not an error.
        assert!(
            codes(&out).contains(&"definition-unavailable".to_owned()),
            "{:?}",
            out.diagnostics
        );
        assert!(!out.definitions.contains_key("d"));
        // `ok` has no disabled plugin, so the same dead listener is still an error.
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|d| d.code == "dead-listener" && d.definition.as_deref() == Some("ok"))
        );
        cfg.definitions.remove("ok");
        let out = compile(&cfg, &ms);
        assert!(out.ok, "{:?}", out.diagnostics);
    }

    #[test]
    fn dead_listener_is_an_error() {
        let b = manifest("b", vec![consume("never", &[])], vec![]);
        let (cfg, ms) = setup(vec![b], &[("b", None)]);
        let out = compile(&cfg, &ms);
        assert!(!out.ok);
        assert!(codes(&out).contains(&"dead-listener".to_owned()));
    }

    #[test]
    fn starved_consumer_is_an_error_and_optional_keys_are_not() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[])],
            vec![emit("x", &["k"])],
        );
        let b = manifest("b", vec![consume("x", &["k", "missing", "maybe?"])], vec![]);
        let (cfg, ms) = setup(vec![a, b], &[("a", None), ("b", None)]);
        let out = compile(&cfg, &ms);
        let d: Vec<_> = out
            .diagnostics
            .iter()
            .filter(|d| d.code == "starved-consumer")
            .collect();
        assert_eq!(d.len(), 1);
        assert!(d[0].message.contains("missing"));
        assert!(!d[0].message.contains("maybe"));
    }

    #[test]
    fn transforms_change_supply_for_downstream_only() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[])],
            vec![emit("x", &["k", "drop"])],
        );
        let mut t = manifest("t", vec![consume("x", &[])], vec![]);
        t.transforms = vec![Transform {
            event: "x".into(),
            preserves: vec!["k".into()],
            adds: vec!["new".into()],
        }];
        let early = manifest("early", vec![consume("x", &["drop"])], vec![]);
        let late = manifest("late", vec![consume("x", &["new", "k"])], vec![]);
        let late_bad = manifest("late-bad", vec![consume("x", &["drop"])], vec![]);
        let (cfg, ms) = setup(
            vec![a, t, early, late, late_bad],
            &[
                ("a", None),
                ("early", None),
                ("t", None),
                ("late", None),
                ("late-bad", Some(Mode::Async)),
            ],
        );
        let out = compile(&cfg, &ms);
        let starved: Vec<_> = out
            .diagnostics
            .iter()
            .filter(|d| d.code == "starved-consumer")
            .map(|d| d.plugin.clone().unwrap())
            .collect();
        assert_eq!(starved, vec!["late-bad".to_owned()]);
    }

    #[test]
    fn async_transform_and_wildcard_demands_rejected() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[])],
            vec![emit("x", &[])],
        );
        let mut t = manifest("t", vec![consume("x", &[])], vec![]);
        t.transforms = vec![Transform {
            event: "x".into(),
            preserves: vec!["*".into()],
            adds: vec![],
        }];
        let w = manifest("w", vec![consume("*", &["k"])], vec![]);
        let (cfg, ms) = setup(
            vec![a, t, w],
            &[("a", None), ("t", Some(Mode::Async)), ("w", None)],
        );
        let out = compile(&cfg, &ms);
        let c = codes(&out);
        assert!(c.contains(&"async-transform".to_owned()));
        assert!(c.contains(&"wildcard-demand".to_owned()));
    }

    #[test]
    fn wildcard_placed_by_position_in_every_pipeline() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[])],
            vec![emit("x", &[])],
        );
        let w = manifest(
            "w",
            vec![Consume {
                event: "*".into(),
                demands: vec![],
                mode: Some(Mode::Async),
                context: false,
                keyed: None,
            }],
            vec![],
        );
        let (cfg, ms) = setup(vec![a, w], &[("w", None), ("a", None)]);
        let out = compile(&cfg, &ms);
        assert!(out.ok, "{:?}", out.diagnostics);
        let d = &out.definitions["d"];
        assert_eq!(d.pipeline("x").asyncs[0].plugin, "w");
        assert_eq!(d.pipeline("unlisted").asyncs[0].plugin, "w");
        assert_eq!(d.pipeline("session-started").blocking[0].plugin, "a");
    }

    #[test]
    fn deferred_without_capability_and_emit_wildcard_rejected() {
        let mut a = manifest(
            "a",
            vec![],
            vec![
                Emit {
                    event: "x".into(),
                    supplies: vec![],
                    deferred: true,
                },
                emit("*", &[]),
            ],
        );
        a.capabilities = vec![];
        let (cfg, ms) = setup(vec![a], &[("a", None)]);
        let c = codes(&compile(&cfg, &ms));
        assert!(c.contains(&"missing-capability".to_owned()));
        assert!(c.contains(&"emit-wildcard".to_owned()));
    }

    #[test]
    fn cycles_are_reported_as_info() {
        let a = manifest(
            "a",
            vec![consume("session-started", &[]), consume("y", &[])],
            vec![emit("x", &[])],
        );
        let b = manifest("b", vec![consume("x", &[])], vec![emit("y", &[])]);
        let (cfg, ms) = setup(vec![a, b], &[("a", None), ("b", None)]);
        let out = compile(&cfg, &ms);
        assert!(out.ok);
        assert_eq!(
            out.definitions["d"].cycles,
            vec![vec!["x".to_owned(), "y".to_owned()]]
        );
    }

    #[test]
    fn external_starters_count_as_emitters() {
        let mut ui = manifest("ui", vec![], vec![emit("user-message", &["text"])]);
        ui.capabilities = vec![Capability::StartSession];
        let chat = manifest("chat", vec![consume("user-message", &["text"])], vec![]);
        let (mut cfg, ms) = setup(vec![ui, chat], &[("chat", None)]);
        let out = compile(&cfg, &ms);
        assert!(out.ok, "{:?}", out.diagnostics);
        // Without the capability it becomes a dead listener.
        let mut ms2 = ms.clone();
        ms2.get_mut("ui").unwrap().capabilities.clear();
        assert!(!compile(&cfg, &ms2).ok);
        // Unknown and missing plugins are errors.
        cfg.definitions
            .get_mut("d")
            .unwrap()
            .subscribers
            .push(Subscriber {
                plugin: "ghost".into(),
                mode: None,
            });
        assert!(codes(&compile(&cfg, &ms)).contains(&"unknown-plugin".to_owned()));
    }

    #[test]
    fn hash_is_stable_and_config_sensitive() {
        let a = manifest("a", vec![consume("session-started", &[])], vec![]);
        let (mut cfg, ms) = setup(vec![a], &[("a", None)]);
        let h1 = compile(&cfg, &ms).hash;
        assert_eq!(h1, compile(&cfg, &ms).hash);
        cfg.plugins.get_mut("a").unwrap().config = json!({"x": 1});
        assert_ne!(h1, compile(&cfg, &ms).hash);
    }

    #[test]
    fn covers_nested_keys() {
        let s: BTreeSet<String> = ["a".into(), "b.c".into()].into();
        assert!(covers(&s, "a"));
        assert!(covers(&s, "a.x"));
        assert!(covers(&s, "b"));
        assert!(covers(&s, "b.c"));
        assert!(!covers(&s, "b.d"));
        assert!(!covers(&s, "ab"));
    }
}
