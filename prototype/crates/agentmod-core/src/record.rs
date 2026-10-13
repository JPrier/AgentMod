//! Typed log records. Each session *is* its append-only log of these.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{Cause, ContextItem, ContextOp, EventRecord, LogRef, Mode, Stamp};

/// One record in a session log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub session_id: String,
    /// Monotonic position in the session log; order-of-record is replay truth.
    pub sequence: u64,
    /// Recorded arrival time in ms since the epoch (a fact; the core never acts on time).
    pub at: u64,
    #[serde(flatten)]
    pub body: Body,
}

/// Invocation outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum Outcome {
    Ok,
    Veto { reason: String },
    Failed { error: String },
    Cancelled { reason: String },
}

/// How a pipeline settled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum Settlement {
    /// Blocking chain completed; async subscribers observe `payload`.
    Delivered {
        payload: Value,
    },
    Vetoed {
        plugin: String,
        reason: String,
    },
    Failed {
        plugin: String,
        error: String,
    },
    /// Interrupted by a hard stop.
    Aborted {
        reason: String,
    },
}

/// Dispatcher state of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DispatchState {
    #[default]
    Running,
    /// Soft stop: in-flight pipeline finishes, then parks.
    Draining,
    Parked,
    /// Hard stop: in-flight invocations were killed.
    Halted,
}

/// Dispatcher commands: edge-triggered, never queued, never events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    SoftStop,
    HardStop,
    Resume,
}

/// Record bodies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Body {
    SessionCreated {
        definition: String,
        /// The revision (compiled graph) the session runs under.
        revision: String,
        cause: Cause,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fork_of: Option<LogRef>,
    },
    /// Context copied from another session at fork time (keeps replay self-contained).
    ContextSeeded {
        from: LogRef,
        items: Vec<ContextItem>,
    },
    EventAppended {
        event: EventRecord,
    },
    /// The event left its lane and its pipeline began.
    PipelineStarted {
        event_id: String,
    },
    InvocationStarted {
        invocation_id: String,
        event_id: String,
        plugin: String,
        mode: Mode,
        /// The subscription (event pattern) the compiled edge came from.
        #[serde(default)]
        subscription: String,
        /// Position in the event's blocking chain (blocking), or 0.
        position: usize,
        /// The blocking subscriber's phase.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        phase: Option<String>,
        stamp: Stamp,
        /// The revision this invocation ran under.
        #[serde(default)]
        revision: String,
        /// Input state: exactly what was delivered (the projected payload).
        payload: Value,
    },
    /// An orphaned or crashed invocation was restarted.
    InvocationRetried {
        invocation_id: String,
        attempt: u32,
        reason: String,
    },
    InvocationCompleted {
        invocation_id: String,
        outcome: Outcome,
        /// Blocking subscribers: the keys returned to the chain.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        returns: Option<Value>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        contributions: Vec<ContextOp>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        published: Vec<String>,
    },
    PipelineSettled {
        event_id: String,
        settlement: Settlement,
    },
    /// A publish was refused; refusals are recorded, never silent.
    PublishBlocked {
        plugin: String,
        event_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation_id: Option<String>,
        reason: String,
    },
    /// Mirror in the sender's log of an event delivered to another session.
    CrossSessionSent {
        event_id: String,
        event_name: String,
        target_session: String,
    },
    DispatcherCommand {
        command: Command,
        by: String,
        state: DispatchState,
    },
    ConfigApplied {
        revision: String,
        previous: String,
        /// True when applied to this session only (a per-session layer).
        #[serde(default)]
        scoped: bool,
    },
    /// Behaviour contradicted a compiled declaration (compiler.md §7): a hard
    /// failure, recorded against the exact declaration it violated.
    Breach {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation_id: Option<String>,
        plugin: String,
        stamp: Stamp,
        revision: String,
        /// The compiled declaration violated (kind, event, key, shape, …).
        declaration: Value,
        /// The value actually observed.
        observed: Value,
        reason: String,
        /// What the runtime did about it.
        reaction: String,
    },
}

impl Body {
    /// Short kebab-case name of the record type.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Body::SessionCreated { .. } => "session-created",
            Body::ContextSeeded { .. } => "context-seeded",
            Body::EventAppended { .. } => "event-appended",
            Body::PipelineStarted { .. } => "pipeline-started",
            Body::InvocationStarted { .. } => "invocation-started",
            Body::InvocationRetried { .. } => "invocation-retried",
            Body::InvocationCompleted { .. } => "invocation-completed",
            Body::PipelineSettled { .. } => "pipeline-settled",
            Body::PublishBlocked { .. } => "publish-blocked",
            Body::CrossSessionSent { .. } => "cross-session-sent",
            Body::DispatcherCommand { .. } => "dispatcher-command",
            Body::ConfigApplied { .. } => "config-applied",
            Body::Breach { .. } => "breach",
        }
    }
}
