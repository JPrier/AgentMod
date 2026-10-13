//! Plugin manifests, session definitions, and the deployment configuration.
//!
//! A manifest is a plugin's complete declaration of what it receives, returns,
//! emits, and may call (compiler.md §3). Payload contracts are typed
//! [`Shape`](crate::shape::Shape)s matched structurally: there is no central
//! registry, no event ownership, and no dependency between plugins.
//!
//! A session definition (§4) lists the plugins a session runs, the phases that
//! order its blocking subscribers, tie-breaks, the events it accepts from other
//! sessions, and its slots. It never lists edges: those are compiler output.
//!
//! Manifests and definitions reject fields they do not know, so a field the
//! design removed fails loudly instead of being silently ignored.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::shape::{KeyDecl, Keys, Shape};
use crate::types::{Lane, Mode, Stamp, hash_json};

/// Wildcard event pattern: every event.
pub const WILDCARD: &str = "*";

/// The default slot: the session's assembled context.
pub const CONTEXT_SLOT: &str = "context";

/// Actions a subscription may declare (names provisional; compiler.md §3.2).
pub const ACTIONS: &[&str] = &[ACTION_VETO, ACTION_CONTRIBUTE];
pub const ACTION_VETO: &str = "veto";
pub const ACTION_CONTRIBUTE: &str = "contribute-context";

/// Plugin-wide runtime powers (compiler.md §3.4). Any call that isn't
/// declared is declined and recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Capability {
    /// Publish without an open invocation, citing a standing trigger.
    #[serde(rename = "deferred-publish")]
    DeferredPublish,
    /// Create new sessions (the sole rootless act).
    #[serde(rename = "start-session")]
    StartSession,
    /// Publish to a session other than the invocation's own (`outbound`).
    #[serde(rename = "cross-session")]
    CrossSession,
    /// Emit on the interrupt lane.
    #[serde(rename = "interrupt")]
    Interrupt,
    /// Queue method: list pending events (declared; not built in this prototype).
    #[serde(rename = "queue.list")]
    QueueList,
    /// Queue method: cancel a pending event (declared; not built in this prototype).
    #[serde(rename = "queue.cancel")]
    QueueCancel,
    /// Session directory lookup (declared; the directory API is deferred).
    #[serde(rename = "sessions.find")]
    SessionsFind,
    /// Session directory read (declared; the directory API is deferred).
    #[serde(rename = "sessions.read")]
    SessionsRead,
    /// Dispatcher commands: soft stop, hard stop, resume (compiler.md §3.5).
    /// The prototype also gates config apply and service calls on it.
    #[serde(rename = "control")]
    Control,
    /// Read a session's records from outside the bus, as a watcher
    /// (compiler.md §3.5). Watching is reading, not participating: a watcher
    /// is not a node in the compiled graph.
    #[serde(rename = "observe")]
    Observe,
}

impl Capability {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Capability::DeferredPublish => "deferred-publish",
            Capability::StartSession => "start-session",
            Capability::CrossSession => "cross-session",
            Capability::Interrupt => "interrupt",
            Capability::QueueList => "queue.list",
            Capability::QueueCancel => "queue.cancel",
            Capability::SessionsFind => "sessions.find",
            Capability::SessionsRead => "sessions.read",
            Capability::Control => "control",
            Capability::Observe => "observe",
        }
    }
}

// ----------------------------------------------------------------------
// Event names and patterns.
// ----------------------------------------------------------------------

/// Does an event pattern match a concrete event name?
///
/// Patterns are an exact name, `*` (every event), or a family / sub-family
/// ending in `*` (`tool-call/*`, `tool-call/mcp__github__*`), which matches
/// every name with that prefix.
#[must_use]
pub fn pattern_matches(pattern: &str, name: &str) -> bool {
    if pattern == WILDCARD {
        return true;
    }
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix) && name.len() > prefix.len(),
        None => pattern == name,
    }
}

/// Is a pattern a family (or sub-family) pattern?
#[must_use]
pub fn is_family(pattern: &str) -> bool {
    pattern != WILDCARD && pattern.ends_with('*')
}

/// Does pattern `a` match every name pattern `b` matches?
#[must_use]
pub fn pattern_contains(a: &str, b: &str) -> bool {
    if a == WILDCARD {
        return true;
    }
    if b == WILDCARD {
        return false;
    }
    match (a.strip_suffix('*'), b.strip_suffix('*')) {
        (Some(pa), Some(pb)) => pb.starts_with(pa),
        (Some(pa), None) => b.starts_with(pa) && b.len() > pa.len(),
        (None, Some(_)) => false,
        (None, None) => a == b,
    }
}

/// Could some event name match both patterns?
#[must_use]
pub fn patterns_overlap(a: &str, b: &str) -> bool {
    pattern_contains(a, b) || pattern_contains(b, a)
}

/// Why an event name or pattern is malformed, if it is.
#[must_use]
pub fn pattern_error(pattern: &str, emit: bool) -> Option<String> {
    if pattern.is_empty() {
        return Some("empty event name".into());
    }
    if pattern == WILDCARD {
        return emit.then(|| {
            "emits are declared events or declared families, never open wildcards".into()
        });
    }
    let stars = pattern.matches('*').count();
    if stars > 1 || (stars == 1 && !pattern.ends_with('*')) {
        return Some(format!(
            "`{pattern}`: `*` may only end a family pattern (`family/*`, `family/prefix*`)"
        ));
    }
    if stars == 1 && !pattern.contains('/') {
        return Some(format!(
            "`{pattern}`: a family pattern names its family first (`{}/*`)",
            pattern.trim_end_matches('*')
        ));
    }
    if pattern.chars().any(char::is_whitespace) {
        return Some(format!("`{pattern}`: event names contain no whitespace"));
    }
    None
}

// ----------------------------------------------------------------------
// The manifest.
// ----------------------------------------------------------------------

/// One subscription: an event, a family, or `*`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub event: String,
    pub mode: Mode,
    /// Runtime calls scoped to this event's pipeline (`veto`, `contribute-context`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<String>,
    /// Also receive every undeclared key, untyped and outside the compiler's guarantee.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unlisted_keys: bool,
    /// The demand: the payload keys this subscriber needs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub receives: Keys,
    /// Session slots this subscriber requests, like keys (`[subscribe.slots]`,
    /// a sibling of `receives`, so no key name is reserved; compiler.md §3.2).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: Keys,
    /// Blocking only: keys handed back to the chain (overwrite or add; never remove).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub returns: Keys,
    /// What this subscriber does with the event, for people and for the
    /// emitter of a family it subscribes a member of (`chat-context` shows a
    /// tool member's description to the model). Documentation only.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Free-form hints for a member's emitter and its observers (a tool's
    /// tier, group, effects). Never checked by the compiler; documentation for
    /// other plugins, delivered with the resolved emit set.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub hints: Value,
}

impl Subscription {
    #[must_use]
    pub fn has_action(&self, action: &str) -> bool {
        self.actions.iter().any(|a| a == action)
    }

    #[must_use]
    pub fn wildcard(&self) -> bool {
        self.event == WILDCARD
    }
}

/// One emitted event or event family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitDecl {
    pub event: String,
    /// The lanes a publish of this event may choose from (compiler.md §3.3);
    /// each publish picks one. Allowing `interrupt` requires the interrupt
    /// capability.
    #[serde(default = "normal_only", skip_serializing_if = "is_normal_only")]
    pub lanes: Vec<Lane>,
    /// May be published without an open invocation (requires deferred-publish).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deferred: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: Keys,
}

/// The default lanes of an emit: the normal lane only.
#[must_use]
pub fn normal_only() -> Vec<Lane> {
    vec![Lane::Normal]
}

fn is_normal_only(l: &[Lane]) -> bool {
    l == [Lane::Normal]
}

impl EmitDecl {
    /// May a publish of this event use the interrupt lane?
    #[must_use]
    pub fn may_interrupt(&self) -> bool {
        self.lanes.contains(&Lane::Interrupt)
    }
}

/// A cross-session event a plugin may send (requires cross-session).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundDecl {
    pub event: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: Keys,
}

/// Everything a plugin declares at handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscribe: Vec<Subscription>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emit: Vec<EmitDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<Capability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbound: Vec<OutboundDecl>,
    /// The plugin's effect lies outside the bus (a frontend, a log exporter).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sink: bool,
    /// Free-form description of accepted configuration (documentation only).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub config_schema: Value,
    /// Read-only services a host may route to this plugin for frontends
    /// (`[{ name, description }]`, e.g. a provider's `model-catalog`). Service
    /// calls are not invocations: they are not recorded and change no state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<Value>,
    /// Settings a frontend may present to configure this plugin
    /// (`[{ key, label, secret?, required?, type? }]`); documentation for UIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settings: Vec<Value>,
    /// Roles the plugin fills, for frontends (e.g. `["model"]`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provides: Vec<String>,
    /// Host devices the plugin uses (browser runtime; enforced by that host).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<String>,
}

impl Manifest {
    #[must_use]
    pub fn has(&self, cap: Capability) -> bool {
        self.capabilities.contains(&cap)
    }

    /// The emit declaration covering a concrete event name: the exact
    /// declaration if any, else the most specific matching family.
    #[must_use]
    pub fn emit_for(&self, name: &str) -> Option<&EmitDecl> {
        self.emit.iter().find(|e| e.event == name).or_else(|| {
            self.emit
                .iter()
                .filter(|e| is_family(&e.event) && pattern_matches(&e.event, name))
                .max_by_key(|e| e.event.len())
        })
    }

    #[must_use]
    pub fn outbound_for(&self, name: &str) -> Option<&OutboundDecl> {
        self.outbound.iter().find(|o| o.event == name)
    }

    /// Does the manifest declare this service?
    #[must_use]
    pub fn has_service(&self, name: &str) -> bool {
        self.services
            .iter()
            .any(|s| s.get("name").and_then(Value::as_str) == Some(name))
    }
}

// ----------------------------------------------------------------------
// Deployment: plugin instances and session definitions.
// ----------------------------------------------------------------------

/// One configured plugin instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PluginConfig {
    /// Command line for process hosts (native runtime).
    #[serde(default)]
    pub command: Vec<String>,
    /// Module entry for worker hosts (browser runtime).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// Plugin-specific configuration delivered at handshake.
    #[serde(default)]
    pub config: Value,
    /// Invocation timeout before cancellation escalates to a kill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Hash of the plugin's code, filled in by the host before compilation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_hash: Option<String>,
    /// Hosts may disable a plugin they cannot run (e.g. native-only plugins in a browser).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disabled: bool,
    /// Required plugin version (exact, or a `major.minor` prefix). The compiler
    /// rejects a config whose plugin reports another version at handshake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl PluginConfig {
    /// Provenance stamp: code hash + config hash.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        let cfg = serde_json::json!({
            "command": self.command,
            "module": self.module,
            "config": self.config,
        });
        Stamp {
            binary: self
                .binary_hash
                .clone()
                .unwrap_or_else(|| "unhashed".to_owned()),
            config: hash_json(&cfg)[..16].to_owned(),
        }
    }
}

/// A plugin's participation in a session definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DefPlugin {
    /// Phase of every blocking subscription of this plugin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Per-event phase overrides (`{ "model-request" = "prep" }`); keys are the
    /// subscription's event pattern.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub phases: BTreeMap<String, String>,
    /// Embedded workflow: use this session definition as the plugin, inlined
    /// under the entry's name as namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    /// Embedded workflows: slots of the workflow mapped to the parent's
    /// (`{ context = "context" }`); unmapped slots are namespaced.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: BTreeMap<String, String>,
}

/// The receive/emit interface of a definition used as an embedded workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Interface {
    /// Parent events the workflow receives (keys it needs).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub receives: BTreeMap<String, Keys>,
    /// Events the workflow emits to its parent (keys it sends).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub emits: BTreeMap<String, Keys>,
}

impl Interface {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.receives.is_empty() && self.emits.is_empty()
    }
}

/// A named session definition (or a reusable fragment of one).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// The runtime version this definition targets; selects the core manifest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub runtime: String,
    /// Fragments merged into this definition before compilation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    /// Ordered phase names; blocking subscribers run in phase order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<String>,
    /// The plugins the session runs.
    #[serde(default)]
    pub plugins: BTreeMap<String, DefPlugin>,
    /// Tie-breaks: `order[event][phase] = [plugins in order]`. The event key is
    /// an event name, a family pattern (`tool-call/*`), or a family name
    /// (`tool-call`); a key naming a family applies to every member of it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub order: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    /// Events accepted from other sessions, with their shapes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inbound: BTreeMap<String, Keys>,
    /// Named, typed session state. `context` is the default slot.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: Keys,
    /// Promote warnings to errors.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deny_warnings: bool,
    /// Declared interface: this definition may be embedded as a plugin.
    #[serde(default, skip_serializing_if = "Interface::is_empty")]
    pub interface: Interface,
}

impl Definition {
    /// The definition's slots, with the default `context` slot unless replaced.
    #[must_use]
    pub fn effective_slots(&self) -> Keys {
        let mut slots = self.slots.clone();
        slots
            .entry(CONTEXT_SLOT.to_owned())
            .or_insert_with(default_context_slot);
        slots
    }
}

/// Shape of the default `context` slot: the attributed context items.
#[must_use]
pub fn default_context_slot() -> KeyDecl {
    KeyDecl::required(Shape::Array(Box::new(Shape::Object(None))))
}

/// Core runtime settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettings {
    /// Payloads larger than this spill to content-addressed files.
    #[serde(default = "default_spill")]
    pub spill_threshold_bytes: usize,
    /// Invocation attempts before an orphan is recorded as failed.
    #[serde(default = "default_attempts")]
    pub max_attempts: u32,
    /// Live stream delivery (coalescing, client bounds, recovery compaction).
    #[serde(default)]
    pub streaming: crate::stream::StreamSettings,
}

fn default_spill() -> usize {
    16 * 1024
}
fn default_attempts() -> u32 {
    3
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            spill_threshold_bytes: default_spill(),
            max_attempts: default_attempts(),
            streaming: crate::stream::StreamSettings::default(),
        }
    }
}

/// Where a part of the configuration was written (diagnostics trace to it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRef {
    pub file: String,
    pub line: usize,
}

impl std::fmt::Display for SourceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

/// The whole deployment configuration (global layer).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DeploymentConfig {
    #[serde(default)]
    pub runtime: RuntimeSettings,
    #[serde(default)]
    pub plugins: BTreeMap<String, PluginConfig>,
    #[serde(default)]
    pub definitions: BTreeMap<String, Definition>,
    /// Reusable partial definitions, merged in through `include`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fragments: BTreeMap<String, Definition>,
    /// Source locations by config path (`definitions.coder.plugins.policy`),
    /// filled by the loader; not part of any hash.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sources: BTreeMap<String, SourceRef>,
}

impl DeploymentConfig {
    /// The config without source locations (for hashing and round trips).
    #[must_use]
    pub fn without_sources(&self) -> DeploymentConfig {
        DeploymentConfig {
            sources: BTreeMap::new(),
            ..self.clone()
        }
    }

    /// The most specific recorded source location for a config path.
    #[must_use]
    pub fn source(&self, path: &str) -> Option<&SourceRef> {
        let mut p = path;
        loop {
            if let Some(s) = self.sources.get(p) {
                return Some(s);
            }
            p = p.rsplit_once('.').map(|(a, _)| a)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn patterns() {
        assert!(pattern_matches("*", "x"));
        assert!(pattern_matches("tool-call/*", "tool-call/read_file"));
        assert!(!pattern_matches("tool-call/*", "tool-call/"));
        assert!(!pattern_matches("tool-call/*", "tool-result"));
        assert!(pattern_matches(
            "tool-call/mcp__gh__*",
            "tool-call/mcp__gh__x"
        ));
        assert!(pattern_contains("tool-call/*", "tool-call/mcp__gh__*"));
        assert!(!pattern_contains("tool-call/mcp__gh__*", "tool-call/*"));
        assert!(patterns_overlap("tool-call/mcp__*", "tool-call/mcp__gh__x"));
        assert!(!patterns_overlap("tool-call/a", "tool-call/b"));
        assert!(pattern_error("*", true).is_some());
        assert!(pattern_error("*", false).is_none());
        assert!(pattern_error("a*b", false).is_some());
        assert!(pattern_error("tools*", false).is_some());
        assert!(pattern_error("tool-call/*", true).is_none());
    }

    #[test]
    fn manifest_json_round_trip() {
        let m: Manifest = serde_json::from_value(json!({
            "name": "p", "version": "1",
            "subscribe": [{
                "event": "model-request", "mode": "blocking", "actions": ["contribute-context"],
                "receives": { "model": { "shape": "string" }, "slots": { "shape": "integer" } },
                "slots": { "context": { "shape": "array<object>" } },
                "returns": { "temperature": { "shape": "number" } }
            }],
            "emit": [{ "event": "tool-call/*", "keys": { "call_id": { "shape": "string" } } }],
            "capabilities": ["interrupt", "sessions.find"],
            "outbound": [{ "event": "answer", "keys": { "text": { "shape": "string" } } }],
            "sink": true
        }))
        .unwrap();
        // Slots are a sibling of `receives`, so `slots` is an ordinary key name.
        assert_eq!(m.subscribe[0].slots.len(), 1);
        assert_eq!(
            m.subscribe[0].receives["slots"].shape.to_string(),
            "integer"
        );
        assert!(m.emit_for("tool-call/x").is_some());
        assert!(m.emit_for("tool-result").is_none());
        let back: Manifest = serde_json::from_value(serde_json::to_value(&m).unwrap()).unwrap();
        assert_eq!(back, m);
        assert!(
            serde_json::from_value::<Manifest>(
                json!({ "name": "p", "version": "1", "subscribe": [{ "event": "x" }] })
            )
            .is_err(),
            "mode is required"
        );
    }

    #[test]
    fn definition_from_toml_shape() {
        let d: Definition = serde_json::from_value(json!({
            "runtime": "0.1", "phases": ["guard", "loop"],
            "plugins": { "policy": { "phase": "guard", "phases": { "model-request": "loop" } }, "web-ui": {} },
            "order": { "tool-call": { "guard": ["policy", "approval-gate"] } },
            "inbound": { "subagent-result": { "call_id": { "shape": "string" } } },
            "deny_warnings": true
        }))
        .unwrap();
        assert_eq!(d.effective_slots()["context"], default_context_slot());
        assert_eq!(d.plugins["policy"].phases["model-request"], "loop");
    }

    #[test]
    fn emits_declare_lanes_defaulting_to_normal() {
        let m: Manifest = serde_json::from_value(json!({ "name": "p", "version": "1", "emit": [
            { "event": "a" },
            { "event": "user-message", "lanes": ["normal", "interrupt"] }
        ] }))
        .unwrap();
        assert_eq!(m.emit[0].lanes, vec![Lane::Normal]);
        assert!(!m.emit[0].may_interrupt());
        assert!(m.emit[1].may_interrupt());
        let back = serde_json::to_value(&m).unwrap();
        assert!(
            back["emit"][0].get("lanes").is_none(),
            "the default is not written out"
        );
        assert!(
            serde_json::from_value::<Manifest>(json!({ "name": "p", "version": "1", "emit": [{ "event": "a", "lane": "interrupt" }] })).is_err(),
            "the single `lane` field is gone"
        );
    }

    #[test]
    fn removed_fields_are_rejected_not_ignored() {
        let emit = |extra: serde_json::Value| {
            let mut e = json!({ "event": "tool-call/*" });
            e.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            serde_json::from_value::<Manifest>(json!({ "name": "p", "version": "1", "emit": [e] }))
        };
        assert!(emit(json!({})).is_ok());
        assert!(emit(json!({ "exactly_one_handler": true })).is_err());
        assert!(
            serde_json::from_value::<Definition>(
                json!({ "runtime": "0.1", "handlers": { "tool-call/x": "p" } })
            )
            .is_err(),
            "handler picks are gone"
        );
    }
}
