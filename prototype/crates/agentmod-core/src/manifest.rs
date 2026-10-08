//! Plugin manifests and deployment configuration.
//!
//! Payload contracts are demand/supply declarations, not schemas: emitters
//! declare the keys they supply, consumers declare the keys they demand.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{Mode, Stamp, hash_json};

/// Wildcard event name: subscribe to every pipeline.
pub const WILDCARD: &str = "*";

/// Capabilities form the deployment's complete external surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    /// Publish without an open invocation, citing a standing trigger.
    DeferredPublish,
    /// Create new sessions (the sole rootless act).
    StartSession,
    /// Target a session other than the invocation's own.
    CrossSession,
    /// Issue dispatcher commands and apply configuration (frontends).
    Control,
    /// Tail live records of any session (frontends, inspectors).
    Observe,
}

/// A consumed event with demanded payload keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consume {
    pub event: String,
    /// Required payload keys (dotted paths). A trailing `?` marks a key optional.
    #[serde(default)]
    pub demands: Vec<String>,
    /// Default mode; a session definition may override it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    /// Whether the assembled context is delivered with the envelope.
    #[serde(default = "yes")]
    pub context: bool,
    /// Keyed dispatch: this consumer owns only the events whose payload `key`
    /// equals one of `values` (a value ending in `*` is a prefix). The compiler
    /// turns every keyed consumer of an event into one deterministic owner
    /// table, and the kernel dispatches each event to exactly the one owner its
    /// key selects instead of broadcasting it (tool calls → the tool's plugin).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyed: Option<Keyed>,
}

/// The values of one payload key a consumer owns (see [`Consume::keyed`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Keyed {
    /// Payload key (dotted path) whose value selects the owner, e.g. `name`.
    pub key: String,
    /// Owned values; a trailing `*` makes a value a prefix (`mcp__github__*`).
    pub values: Vec<String>,
}

impl Keyed {
    /// Does this declaration own `value`?
    #[must_use]
    pub fn owns(&self, value: &str) -> bool {
        self.values.iter().any(|v| match v.strip_suffix('*') {
            Some(prefix) => value.starts_with(prefix),
            None => v == value,
        })
    }
}

/// A tool a plugin answers, declared at handshake so ownership and schemas are
/// compile-time facts (the model-facing spec is still contributed to context).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolDecl {
    /// Exact name, or a prefix ending in `*` for dynamic families (MCP servers).
    pub name: String,
    /// JSON-schema `properties` of the arguments (object), when known.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub parameters: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required: Vec<String>,
    /// `core` or `deferred` (documentation; the projection reads the context spec).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
}

fn yes() -> bool {
    true
}

/// An emitted event with supplied payload keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Emit {
    pub event: String,
    #[serde(default)]
    pub supplies: Vec<String>,
    /// May this event be published without an open invocation?
    #[serde(default)]
    pub deferred: bool,
}

/// A blocking transformer's declaration for one event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transform {
    pub event: String,
    /// Keys preserved from the incoming payload; `["*"]` preserves everything.
    #[serde(default = "preserve_all")]
    pub preserves: Vec<String>,
    /// Keys the transform adds.
    #[serde(default)]
    pub adds: Vec<String>,
}

fn preserve_all() -> Vec<String> {
    vec![WILDCARD.to_owned()]
}

/// Everything a plugin declares at handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub consumes: Vec<Consume>,
    #[serde(default)]
    pub emits: Vec<Emit>,
    #[serde(default)]
    pub transforms: Vec<Transform>,
    #[serde(default)]
    pub capabilities: Vec<Capability>,
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
    /// Tools this plugin answers (validated by the compiler; see [`ToolDecl`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDecl>,
}

impl Manifest {
    #[must_use]
    pub fn has(&self, cap: Capability) -> bool {
        self.capabilities.contains(&cap)
    }

    #[must_use]
    pub fn emit(&self, event: &str) -> Option<&Emit> {
        self.emits.iter().find(|e| e.event == event)
    }

    #[must_use]
    pub fn transform(&self, event: &str) -> Option<&Transform> {
        self.transforms.iter().find(|t| t.event == event)
    }

    /// Does the manifest declare this service?
    #[must_use]
    pub fn has_service(&self, name: &str) -> bool {
        self.services
            .iter()
            .any(|s| s.get("name").and_then(Value::as_str) == Some(name))
    }
}

/// One configured plugin instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscriber {
    pub plugin: String,
    /// Overrides every consume's default mode for this plugin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
}

/// A named session definition: which plugins participate, in what order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Definition {
    #[serde(default)]
    pub description: String,
    /// Declared order: blocking subscribers run in this order.
    pub subscribers: Vec<Subscriber>,
    /// Explicit owners for keyed dispatch, `{ event: { value: plugin } }`, to
    /// resolve two plugins claiming the same key value (e.g. the same tool).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub route_owners: BTreeMap<String, BTreeMap<String, String>>,
}

/// Core runtime settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSettings {
    /// Runtime bound on causal chains (recursion bounding for cyclic graphs).
    #[serde(default = "default_depth")]
    pub max_causal_depth: u32,
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

fn default_depth() -> u32 {
    256
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
            max_causal_depth: default_depth(),
            spill_threshold_bytes: default_spill(),
            max_attempts: default_attempts(),
            streaming: crate::stream::StreamSettings::default(),
        }
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
}
