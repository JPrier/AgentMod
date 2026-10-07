//! Envelope, identity, and value types shared by every layer.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON object used for payloads and plugin configuration.
pub type Object = serde_json::Map<String, Value>;

/// Dispatch lane. The priority lane wins only the "what's next" decision.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "kebab-case")]
pub enum Lane {
    #[default]
    Normal,
    Priority,
}

/// Subscriber mode inside a pipeline.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// Runs sequentially in declared order; may contribute, transform, or veto.
    #[default]
    Blocking,
    /// Observes the settled event; read-only, cannot veto.
    Async,
}

/// Why an event exists. Every chain is walkable back to a root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Cause {
    /// Published by (or citing) a recorded invocation.
    Invocation { invocation_id: String },
    /// Emitted by the core itself as part of another record (lifecycle events).
    Core { reason: String },
    /// A true root: a rootless `start-session` by a plugin with no invocation.
    Root { plugin: String },
}

/// How a published event entered the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Origin {
    /// Output of a currently open invocation.
    Pipeline {
        plugin: String,
        invocation_id: String,
    },
    /// Fresh arrival citing a standing trigger dispatched to the same plugin.
    Deferred { plugin: String, cites: String },
    /// Initial event carried by `start-session`.
    Start { plugin: String },
    /// Core lifecycle event.
    Core,
    /// Mirror of an event published from another session.
    CrossSession {
        from_session: String,
        plugin: String,
    },
}

/// A durable event as stored in the log (the envelope minus assembled context).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub event_id: String,
    pub event_name: String,
    pub lane: Lane,
    pub cause: Cause,
    pub origin: Origin,
    /// Causal depth from the nearest root; bounded at runtime.
    pub depth: u32,
    pub payload: Value,
    /// Optional UI hint attachment (versioned vocabulary, rendered by frontends).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
}

/// One attributed context contribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub slot: String,
    pub value: Value,
    pub plugin: String,
    pub invocation_id: String,
}

/// Context contribution operations. Context changes are appends to the log:
/// nothing here rewrites history, it only changes the folded view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum ContextOp {
    /// Append a new item to a slot.
    Add { slot: String, value: Value },
    /// Replace the value of an existing item (keeps its position).
    Replace { id: String, value: Value },
    /// Remove one item.
    Remove { id: String },
    /// Remove every item in a slot.
    ClearSlot { slot: String },
    /// Restore the whole context to its state as of a log sequence number.
    Restore { to_sequence: u64 },
}

/// Full envelope delivered to a plugin invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub event_id: String,
    pub session_id: String,
    pub event_name: String,
    pub sequence: u64,
    pub cause: Cause,
    pub lane: Lane,
    pub arrived_at: u64,
    pub depth: u32,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
    /// Session context as of dispatch (omitted when the subscriber opts out).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<ContextItem>>,
}

/// Provenance stamp bound at handshake and recorded on every invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Stamp {
    pub binary: String,
    pub config: String,
}

/// Reference to a point in a session log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRef {
    pub session_id: String,
    pub sequence: u64,
}

/// Lowercase hex SHA-256 of canonical JSON (serde_json maps are sorted).
#[must_use]
pub fn hash_json(value: &Value) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// Resolve a dotted key path (`a.b.c`) inside a JSON value.
#[must_use]
pub fn lookup<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = value;
    for part in path.split('.') {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

/// Session identifier encoded in an invocation id (`s0001/i3` → `s0001`).
#[must_use]
pub fn session_of(id: &str) -> Option<&str> {
    id.split('/').next().filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lookup_dotted_paths() {
        let v = json!({"a": {"b": {"c": 1}}, "x": 2});
        assert_eq!(lookup(&v, "a.b.c"), Some(&json!(1)));
        assert_eq!(lookup(&v, "x"), Some(&json!(2)));
        assert_eq!(lookup(&v, "a.z"), None);
        assert_eq!(lookup(&v, "x.y"), None);
    }

    #[test]
    fn hash_is_order_independent() {
        let a: Value = serde_json::from_str(r#"{"b":1,"a":2}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a":2,"b":1}"#).unwrap();
        assert_eq!(hash_json(&a), hash_json(&b));
        assert_eq!(hash_json(&a).len(), 64);
    }

    #[test]
    fn session_of_ids() {
        assert_eq!(session_of("s0001/i3"), Some("s0001"));
        assert_eq!(session_of("s0001/i3.o1"), Some("s0001"));
        assert_eq!(session_of(""), None);
    }
}
