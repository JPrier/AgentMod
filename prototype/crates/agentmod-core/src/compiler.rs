//! The compiler: walks every declared subscription and emission and produces a
//! validated per-definition graph. The graph is a compile-time artifact, never
//! an executor — dispatch only looks up the compiled pipeline for an event name.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::manifest::{
    Capability, DeploymentConfig, Emit, Manifest, PluginConfig, Transform, WILDCARD,
};
use crate::types::{Mode, Stamp, hash_json};

/// Core lifecycle events and the keys they supply.
pub const CORE_EVENTS: &[(&str, &[&str])] = &[
    ("session-started", &["definition"]),
    ("config-applied", &["config"]),
];

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
    #[serde(rename = "async")]
    pub asyncs: Vec<Slot>,
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
        for (pos, name, m, override_mode) in &members {
            for c in &m.consumes {
                let mode = override_mode.or(c.mode).unwrap_or_default();
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
            Pipeline { blocking, asyncs }
        };
        cd.fallback = place(Vec::new());

        for (event, slots) in explicit {
            let pipeline = place(slots);
            // Dead listener: an explicit consumer of an event nobody can emit.
            let ems = emitters.get(&event).cloned().unwrap_or_default();
            if ems.is_empty() {
                for s in pipeline
                    .blocking
                    .iter()
                    .chain(&pipeline.asyncs)
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
                for s in &pipeline.asyncs {
                    check_demands(&mut cx, s, &supply, emitter, &event);
                }
            }
            cd.pipelines.insert(event, pipeline);
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
