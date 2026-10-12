//! The read model: the flat record stream projected losslessly into a
//! session → events → invocations tree. Rebuildable from the log in one pass.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::fold;
use crate::record::{Body, Command, DispatchState, Outcome, Record, Settlement};
use crate::types::{Cause, ContextItem, ContextOp, Lane, LogRef, Mode, Origin, Stamp};

/// One invocation in an event's pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationView {
    pub invocation_id: String,
    pub plugin: String,
    pub mode: Mode,
    /// The subscription (event pattern) the compiled edge came from.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subscription: String,
    pub position: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    pub stamp: Stamp,
    /// The revision the invocation ran under.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub revision: String,
    pub started_seq: u64,
    pub started_at: u64,
    pub attempts: u32,
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// Keys returned to the chain (blocking subscribers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<Value>,
    #[serde(default)]
    pub contributions: Vec<ContextOp>,
    /// Events published by this invocation (fan-out links).
    #[serde(default)]
    pub published: Vec<String>,
}

/// One event and its pipeline record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventView {
    pub event_id: String,
    pub event_name: String,
    pub sequence: u64,
    pub at: u64,
    pub lane: Lane,
    pub cause: Cause,
    pub origin: Origin,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<Value>,
    /// `queued`, `running`, `delivered`, `vetoed`, `failed`, or `aborted`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement: Option<Settlement>,
    pub invocations: Vec<InvocationView>,
}

/// A blocked publish, surfaced for inspection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockedView {
    pub sequence: u64,
    pub plugin: String,
    pub event_name: String,
    pub reason: String,
}

/// A dispatcher command or config change on the session timeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlView {
    pub sequence: u64,
    pub at: u64,
    pub kind: String,
    pub detail: Value,
}

/// The whole session tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionView {
    pub session_id: String,
    pub definition: String,
    /// The revision the session runs under now.
    pub revision: String,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Cause>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_of: Option<LogRef>,
    pub state: DispatchState,
    pub last_sequence: u64,
    pub records: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub events: Vec<EventView>,
    pub context: Vec<ContextItem>,
    pub blocked: Vec<BlockedView>,
    /// Contract breaches: behaviour that contradicted a compiled declaration.
    #[serde(default)]
    pub breaches: Vec<Value>,
    pub control: Vec<ControlView>,
    /// Cross-session publishes recorded in this log (mirrors).
    pub sent: Vec<Value>,
}

/// Project a session's records into its tree.
#[must_use]
pub fn project(records: &[Record]) -> SessionView {
    let mut v = SessionView::default();
    let mut events: Vec<EventView> = Vec::new();
    let mut by_event: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_inv: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for r in records {
        v.last_sequence = r.sequence;
        v.records += 1;
        match &r.body {
            Body::SessionCreated {
                definition,
                revision,
                cause,
                fork_of,
            } => {
                v.session_id.clone_from(&r.session_id);
                v.definition.clone_from(definition);
                v.revision.clone_from(revision);
                v.created_at = r.at;
                v.cause = Some(cause.clone());
                v.fork_of.clone_from(fork_of);
            }
            Body::ContextSeeded { .. } => {}
            Body::EventAppended { event } => {
                by_event.insert(event.event_id.clone(), events.len());
                events.push(EventView {
                    event_id: event.event_id.clone(),
                    event_name: event.event_name.clone(),
                    sequence: r.sequence,
                    at: r.at,
                    lane: event.lane,
                    cause: event.cause.clone(),
                    origin: event.origin.clone(),
                    payload: event.payload.clone(),
                    ui: event.ui.clone(),
                    status: "queued".into(),
                    settlement: None,
                    invocations: Vec::new(),
                });
            }
            Body::PipelineStarted { event_id } => {
                if let Some(&i) = by_event.get(event_id) {
                    events[i].status = "running".into();
                }
            }
            Body::InvocationStarted {
                invocation_id,
                event_id,
                plugin,
                mode,
                subscription,
                position,
                phase,
                stamp,
                revision,
                payload,
            } => {
                if let Some(&i) = by_event.get(event_id) {
                    by_inv.insert(invocation_id.clone(), (i, events[i].invocations.len()));
                    events[i].invocations.push(InvocationView {
                        invocation_id: invocation_id.clone(),
                        plugin: plugin.clone(),
                        mode: *mode,
                        subscription: subscription.clone(),
                        position: *position,
                        phase: phase.clone(),
                        stamp: stamp.clone(),
                        revision: revision.clone(),
                        started_seq: r.sequence,
                        started_at: r.at,
                        attempts: 1,
                        input: payload.clone(),
                        completed_seq: None,
                        completed_at: None,
                        outcome: None,
                        returns: None,
                        contributions: Vec::new(),
                        published: Vec::new(),
                    });
                }
            }
            Body::InvocationRetried {
                invocation_id,
                attempt,
                ..
            } => {
                if let Some(&(e, i)) = by_inv.get(invocation_id) {
                    events[e].invocations[i].attempts = *attempt;
                }
            }
            Body::InvocationCompleted {
                invocation_id,
                outcome,
                returns,
                contributions,
                published,
            } => {
                if let Some(&(e, i)) = by_inv.get(invocation_id) {
                    let inv = &mut events[e].invocations[i];
                    inv.completed_seq = Some(r.sequence);
                    inv.completed_at = Some(r.at);
                    inv.outcome = Some(outcome.clone());
                    inv.returns.clone_from(returns);
                    inv.contributions.clone_from(contributions);
                    inv.published.clone_from(published);
                    if let Some(title) = contributions.iter().rev().find_map(|c| match c {
                        ContextOp::Add { slot, value } if slot == "title" => {
                            value.as_str().map(str::to_owned)
                        }
                        _ => None,
                    }) {
                        v.title = Some(title);
                    }
                }
            }
            Body::PipelineSettled {
                event_id,
                settlement,
            } => {
                if let Some(&i) = by_event.get(event_id) {
                    events[i].status = match settlement {
                        Settlement::Delivered { .. } => "delivered",
                        Settlement::Vetoed { .. } => "vetoed",
                        Settlement::Failed { .. } => "failed",
                        Settlement::Aborted { .. } => "aborted",
                    }
                    .into();
                    events[i].settlement = Some(settlement.clone());
                }
            }
            Body::PublishBlocked {
                plugin,
                event_name,
                reason,
                ..
            } => v.blocked.push(BlockedView {
                sequence: r.sequence,
                plugin: plugin.clone(),
                event_name: event_name.clone(),
                reason: reason.clone(),
            }),
            Body::CrossSessionSent {
                event_id,
                event_name,
                target_session,
            } => {
                v.sent.push(serde_json::json!({ "sequence": r.sequence, "event_id": event_id, "event_name": event_name, "target_session": target_session }));
            }
            Body::DispatcherCommand { command, by, state } => {
                v.state = *state;
                let name = match command {
                    Command::SoftStop => "soft-stop",
                    Command::HardStop => "hard-stop",
                    Command::Resume => "resume",
                };
                v.control.push(ControlView {
                    sequence: r.sequence,
                    at: r.at,
                    kind: name.into(),
                    detail: serde_json::json!({ "by": by, "state": state }),
                });
            }
            Body::ConfigApplied {
                revision,
                previous,
                scoped,
            } => {
                v.revision.clone_from(revision);
                v.control.push(ControlView { sequence: r.sequence, at: r.at, kind: "config-applied".into(), detail: serde_json::json!({ "revision": revision, "previous": previous, "scoped": scoped }) });
            }
            Body::Breach { .. } => {
                let mut b = serde_json::to_value(&r.body).unwrap_or(Value::Null);
                if let Some(o) = b.as_object_mut() {
                    o.insert("sequence".into(), serde_json::json!(r.sequence));
                    o.insert("at".into(), serde_json::json!(r.at));
                }
                v.breaches.push(b);
            }
        }
    }
    v.context = fold(records, None).items().to_vec();
    v.events = events;
    v
}

/// Context exactly as it stood after `sequence` (C1/C3: reconstruct from the record).
#[must_use]
pub fn context_at(records: &[Record], sequence: u64) -> Vec<ContextItem> {
    fold(records, Some(sequence)).items().to_vec()
}
