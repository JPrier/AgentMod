//! Runtime-owned native control-flow node executors.
//!
//! This module implements the six native graph-node behaviors that Task 2's
//! generic dispatcher must be able to select:
//!
//! - child-agent message delivery (`send_child_agent_message`);
//! - generic join (`join_results`);
//! - bounded parallel branch execution (`parallel_branch`);
//! - durable delay (`delay`);
//! - graph-owned schedule creation (`schedule`);
//! - constrained user-space event emission (`emit_event`).
//!
//! Executors are pure logic components: they validate an exact run/session/
//! node/executor identity, produce typed canonical outcomes compatible with
//! generic dispatch, classify crash/restart positions, enforce hard bounds,
//! propagate cancellation, and never redispatch an ambiguous external effect.
//! External capabilities are reached only through [`crate::node_executors::ports`].
//!
//! The dispatcher contract in this module mirrors Task 2's capability
//! resolution (`agentmod_runtime_data::node_executor`): each executor
//! declares the same kind/implementation/version/boundary/capability record
//! shape, and the generic dispatcher selects exactly one executor per node.

pub mod child_message;
pub mod delay;
pub mod dispatcher;
pub mod event_emission;
pub mod events;
pub mod join;
pub mod parallel;
pub mod ports;
pub mod schedule;
pub mod state;

use agentmod_event_model::ArtifactReference;
use agentmod_graph_engine::NodeKind;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::node_executors::{
    events::{
        ChildMessageClassification, GraphScheduleTrigger, JoinArtifactCollection, JoinOrdering,
        JoinProjection,
    },
    ports::NodeExecutorPorts,
    state::{ExecutorIdentity, NodeExecutorState, ReplayClassification},
};

/// Hard bound on child message content bytes.
pub const MAX_CHILD_MESSAGE_BYTES: usize = 256 * 1024;
/// Hard bound on emitted event payload bytes.
pub const MAX_EMITTED_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Hard bound on artifact references attached to one message or event.
pub const MAX_ARTIFACT_REFERENCES: usize = 64;
/// Maximum number of parallel sub-branches one parallel node may declare.
pub const MAX_PARALLEL_BRANCHES: usize = 64;
/// Maximum participants one generic join may track.
pub const MAX_JOIN_PARTICIPANTS: usize = 256;
/// Minimum permitted parallel dispatch count.
pub const MIN_PARALLELISM: u32 = 1;
/// Maximum permitted parallel dispatch count.
pub const MAX_PARALLELISM: u32 = 64;
/// Minimum recurring schedule interval millis.
pub const MIN_RECURRING_INTERVAL_MS: u64 = 1_000;
/// Hard bound on free-form emitted-event metadata bytes.
pub const MAX_EVENT_METADATA_BYTES: usize = 8 * 1024;
/// Maximum delay duration millis.
pub const MAX_DELAY_MILLIS: i64 = 365 * 24 * 60 * 60 * 1_000;
/// Hard bound on schedule binding text.
pub const MAX_SCHEDULE_BINDING_BYTES: usize = 4_096;
/// Prefixes owned by the runtime; user-space event emission may not forge
/// these categories.
pub const RUNTIME_OWNED_EVENT_PREFIXES: &[&str] = &[
    "provider.",
    "tool.",
    "permission.",
    "scheduler.",
    "lifecycle.",
    "security.",
    "audit.",
    "style.",
    "child_agent.",
    "parallel.",
    "delay.",
    "schedule.",
    "event.",
    "process.",
    "context.",
    "model.",
    "artifact.",
    "approval.",
    "plugin.",
    "session.",
    "graph.",
];

/// Stable native node-executor identity used by the generic dispatcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeExecutorKind {
    /// `send_child_agent_message` implementation.
    ChildMessage,
    /// `join_results` implementation.
    Join,
    /// `parallel_branch` implementation.
    ParallelBranch,
    /// `delay` implementation.
    Delay,
    /// `schedule` implementation.
    Schedule,
    /// `emit_event` implementation.
    EmitEvent,
}

impl NodeExecutorKind {
    /// Maps the executor to the compiled node kind used by the Task 2
    /// capability registry.
    #[must_use]
    pub const fn node_kind(self) -> NodeKind {
        match self {
            Self::ChildMessage => NodeKind::SendChildAgentMessage,
            Self::Join => NodeKind::JoinResults,
            Self::ParallelBranch => NodeKind::ParallelBranch,
            Self::Delay => NodeKind::Delay,
            Self::Schedule => NodeKind::Schedule,
            Self::EmitEvent => NodeKind::EmitEvent,
        }
    }

    /// Maps the executor to the stable capability implementation ID matching
    /// the checked-in Task 2 registry records.
    #[must_use]
    pub const fn implementation_id(self) -> &'static str {
        match self {
            Self::ChildMessage => "runtime.child-message",
            Self::Join => "runtime.join",
            Self::ParallelBranch => "runtime.parallel",
            Self::Delay => "runtime.delay",
            Self::Schedule => "runtime.schedule",
            Self::EmitEvent => "runtime.event-emission",
        }
    }

    /// Exact implementation version.
    #[must_use]
    pub const fn implementation_version(self) -> &'static str {
        "1.0.0"
    }

    /// Business capabilities supported by the implementation.
    #[must_use]
    pub const fn capabilities(self) -> &'static [&'static str] {
        match self {
            Self::ChildMessage | Self::Join => &["agents"],
            Self::ParallelBranch => &[],
            Self::Delay | Self::Schedule => &["scheduling"],
            Self::EmitEvent => &["events"],
        }
    }

    /// Serialized `snake_case` node kind used by capability records.
    #[must_use]
    pub const fn serialized_node_kind(self) -> &'static str {
        match self {
            Self::ChildMessage => "send_child_agent_message",
            Self::Join => "join_results",
            Self::ParallelBranch => "parallel_branch",
            Self::Delay => "delay",
            Self::Schedule => "schedule",
            Self::EmitEvent => "emit_event",
        }
    }
}

/// Typed configuration supplied by the compiled graph for one node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "config", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum NodeExecutorConfig {
    /// `send_child_agent_message` node configuration.
    ChildMessage(ChildMessageConfig),
    /// `join_results` node configuration.
    Join(JoinConfig),
    /// `parallel_branch` node configuration.
    ParallelBranch(ParallelBranchConfig),
    /// `delay` node configuration.
    Delay(DelayConfig),
    /// `schedule` node configuration.
    Schedule(ScheduleConfig),
    /// `emit_event` node configuration.
    EmitEvent(EmitEventConfig),
}

/// `send_child_agent_message` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageConfig {
    /// Exact child session to receive the message.
    pub child_session_id: String,
    /// Caller-supplied idempotency key.
    pub idempotency_key: String,
    /// Bounded typed content.
    pub content: String,
    /// Approved artifact references.
    #[serde(default)]
    pub artifact_references: Vec<ArtifactReference>,
    /// Security classification.
    #[serde(default)]
    pub classification: ChildMessageClassification,
    /// Optional expiration wall-clock millis.
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

/// `join_results` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinConfig {
    /// Required children/branches in declaration order.
    pub required_participants: Vec<String>,
    /// Optional participants that may join when ready.
    #[serde(default)]
    pub optional_participants: Vec<String>,
    /// Minimum successful results required for success.
    #[serde(default)]
    pub min_success: u32,
    /// Allowed participant failures before failure.
    #[serde(default)]
    pub allowed_failures: u32,
    /// Optional wall-clock timeout millis.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Result ordering policy.
    #[serde(default)]
    pub ordering: JoinOrdering,
    /// Result projection policy.
    #[serde(default)]
    pub result_projection: JoinProjection,
    /// Artifact collection policy.
    #[serde(default)]
    pub artifact_collection: JoinArtifactCollection,
}

/// `parallel_branch` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchConfig {
    /// Stable declared sub-branch IDs in declaration order.
    pub branch_ids: Vec<String>,
    /// Explicit maximum parallelism.
    pub max_parallelism: u32,
    /// Canonical variables written by more than one member; these require an
    /// explicit merge/serialization policy supplied by graph state.
    #[serde(default)]
    pub shared_write_scopes: Vec<String>,
    /// Explicit merge/serialization policy for shared writes, when graph
    /// state supplies one. Shared writes without a policy fail closed.
    #[serde(default)]
    pub merge_policy: Option<String>,
}

/// `delay` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayConfig {
    /// Delay duration millis from the resolved scheduling instant.
    pub duration_ms: i64,
    /// Optional expiration wall-clock millis.
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

/// `schedule` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    /// Schedule idempotency key.
    pub idempotency_key: String,
    /// Trigger description.
    pub trigger: GraphScheduleTrigger,
    /// Exact style binding.
    pub style: String,
    /// Exact workspace binding.
    pub workspace: String,
    /// Permission policy binding.
    pub permission_policy: String,
    /// Provider binding.
    pub provider: String,
    /// Model binding.
    pub model: String,
    /// Hard token budget.
    pub token_budget: u64,
    /// Hard cost budget micros.
    pub cost_budget_micros: u64,
}

/// `emit_event` node configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmitEventConfig {
    /// Declared user-space namespace. Runtime-owned categories are rejected.
    pub namespace: String,
    /// Declared event type within the namespace.
    pub event_type: String,
    /// Bounded typed payload as canonical JSON.
    pub payload_json: String,
    /// Approved artifact references.
    #[serde(default)]
    pub artifact_references: Vec<ArtifactReference>,
    /// Bounded non-secret metadata as canonical JSON.
    #[serde(default)]
    pub metadata_json: String,
}

/// Wall-clock instant supplied by the caller; executors never read a clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeExecutorClock {
    /// Current wall-clock millis.
    pub now_ms: i64,
}

/// Participant outcome folded into a join or parallel node between
/// invocations. The dispatcher derives these from verified canonical child
/// completion events; executors never observe external state directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParticipantOutcome {
    /// Participant reached a canonical successful result.
    Completed {
        /// Exact participant execution identity.
        participant: String,
        /// Bounded result references.
        result_references: Vec<String>,
        /// Exact serialized result bytes.
        result_bytes: u64,
    },
    /// Participant failed.
    Failed {
        /// Exact participant execution identity.
        participant: String,
        /// Stable failure classification.
        reason: String,
    },
    /// Participant was cancelled.
    Cancelled {
        /// Exact participant execution identity.
        participant: String,
        /// Stable cancellation classification.
        reason: String,
    },
}

/// Bounded input consumed by every native node executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecutorInput {
    /// Exact canonical session.
    pub session_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// Exact graph node.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact implementation identity the dispatcher resolved.
    pub executor_kind: NodeExecutorKind,
    /// Typed node configuration.
    pub config: NodeExecutorConfig,
    /// Caller-supplied clock.
    pub clock: NodeExecutorClock,
    /// Participant outcomes folded since the last invocation (join/parallel).
    pub participant_outcomes: Vec<ParticipantOutcome>,
    /// Durable wake claim supplied by the scheduler (delay).
    pub wake_claim: Option<ports::ClaimDelayWakeCommand>,
    /// Whether cancellation was requested for this node execution.
    pub cancel_requested: bool,
    /// Whether removal of a graph-owned schedule was requested.
    pub remove_requested: bool,
}

impl NodeExecutorInput {
    /// Returns the exact replay identity.
    #[must_use]
    pub fn identity(&self) -> ExecutorIdentity {
        ExecutorIdentity {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            node_id: self.node_id.clone(),
            attempt: self.attempt,
            loop_iteration: self.loop_iteration,
            step: self.step,
        }
    }
}

/// External effect a node executor asks the dispatcher to perform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeExecutorEffect {
    /// Deliver one child-agent message.
    DeliverChildMessage(ports::DeliverChildMessageCommand),
    /// Create one graph-owned schedule.
    UpsertSchedule(ports::UpsertGraphScheduleCommand),
    /// Remove one graph-owned schedule.
    RemoveSchedule {
        /// Stable schedule identity.
        schedule_id: String,
    },
    /// Create one durable delay continuation.
    CreateDelayContinuation(ports::CreateDelayContinuationCommand),
    /// Claim one durable delay wake.
    ClaimDelayWake(ports::ClaimDelayWakeCommand),
    /// Cancel one durable delay continuation.
    CancelDelayContinuation(ports::CancelDelayContinuationCommand),
}

/// External-effect receipt consumed by the dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeExecutorEffectReceipt {
    /// Child delivery outcome.
    ChildMessage(ports::ChildMessageReceipt),
    /// Schedule upsert outcome.
    Schedule(ports::ScheduleStoreReceipt),
    /// Schedule removal outcome (existence flag).
    ScheduleRemoved(bool),
    /// Delay continuation creation.
    DelayCreated,
    /// Delay wake claim outcome.
    DelayWake(ports::DelayWakeResult),
    /// Delay continuation cancellation outcome.
    DelayCancelled(bool),
}

/// Committed events plus transition state produced by one executor step.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeExecutorStep {
    /// Canonical event payloads to commit in order.
    pub events: Vec<events::NodeExecutorEventPayload>,
    /// Transition variables for the generic dispatcher's edge selection.
    pub transition_variables: serde_json::Value,
}

/// Typed outcome compatible with generic dispatch.
#[derive(Clone, Debug, PartialEq)]
pub enum NodeExecutorOutcome {
    /// The node completed: commit the events and select the next transition.
    Complete {
        /// Committed events plus transition variables.
        step: NodeExecutorStep,
    },
    /// The node awaits an external wake (join participants, delay wake).
    Awaiting {
        /// Committed events plus transition variables.
        step: NodeExecutorStep,
        /// Stable awaiting classification.
        reason: String,
    },
    /// The node failed terminally with a classification.
    Failed {
        /// Committed events plus transition variables.
        step: NodeExecutorStep,
        /// Stable failure classification.
        classification: NodeExecutorFailureClassification,
        /// Human-safe reason.
        reason: String,
    },
}

/// Terminal failure classification of a native control-flow node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeExecutorFailureClassification {
    /// The node failed without external uncertainty.
    Failed,
    /// The node timed out.
    TimedOut,
    /// The node was cancelled.
    Cancelled,
    /// The node expired.
    Expired,
    /// Policy rejected the consequential effect.
    Rejected,
}

/// One deterministic phase of executor behavior.
#[derive(Clone, Debug, PartialEq)]
pub enum ExecutorPhaseResult {
    /// The executor produced a terminal outcome without external effects.
    Done(NodeExecutorOutcome),
    /// The executor produced events and requests one external effect; the
    /// dispatcher commits the events, performs the idempotent effect, and
    /// calls [`NativeNodeExecutor::finalize`] with the receipt.
    Effect {
        /// Events to commit before the effect.
        events: Vec<events::NodeExecutorEventPayload>,
        /// The idempotent external effect to perform.
        effect: NodeExecutorEffect,
    },
    /// The executor awaits an external wake; the dispatcher commits the
    /// events and leaves the node active.
    Await {
        /// Events to commit.
        events: Vec<events::NodeExecutorEventPayload>,
        /// Stable awaiting classification.
        reason: String,
    },
}

/// The generic dispatcher contract every native node executor implements.
pub trait NativeNodeExecutor: Send + Sync {
    /// Returns the stable executor identity.
    fn kind(&self) -> NodeExecutorKind;

    /// Validates the input and produces the first deterministic phase.
    ///
    /// Executors never call external capabilities and never write canonical
    /// state outside their committed events.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorError`] with an explicit recovery classification
    /// when the input, state, or replay position is invalid or ambiguous.
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError>;

    /// Processes one external-effect receipt and produces the terminal phase.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorError`] with an explicit recovery classification
    /// when the receipt is inconsistent with canonical state.
    fn finalize(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
        receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError>;
}

/// Identity/validation failure with an explicit crash/restart classification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeExecutorError {
    /// The input identity does not match the resolved executor.
    #[error("node executor identity mismatch for node `{node_id}`")]
    IdentityMismatch {
        /// Graph node.
        node_id: String,
    },
    /// The run/session/node identity is invalid or unbounded.
    #[error("node executor input is invalid: {reason}")]
    InvalidInput {
        /// Stable reason.
        reason: String,
    },
    /// The replay position is ambiguous; work must fail closed.
    #[error("node executor replay position is ambiguous: {detail}")]
    Ambiguous {
        /// Stable detail.
        detail: String,
    },
    /// The state machine rejected the requested transition.
    #[error("node executor invalid transition: {detail}")]
    InvalidTransition {
        /// Stable detail.
        detail: String,
    },
    /// A hard bound was exceeded.
    #[error("node executor bound exceeded: {detail}")]
    BoundExceeded {
        /// Stable detail.
        detail: String,
    },
    /// A port boundary failed.
    #[error("node executor port failed: {port}")]
    PortFailure {
        /// Stable port name.
        port: &'static str,
    },
}

impl NodeExecutorError {
    /// Returns the crash/restart classification of this failure.
    #[must_use]
    pub const fn recovery_classification(&self) -> ReplayClassification {
        match self {
            Self::Ambiguous { .. } | Self::PortFailure { .. } => {
                ReplayClassification::ExternallyUncertain
            }
            Self::InvalidTransition { .. } | Self::IdentityMismatch { .. } => {
                ReplayClassification::InvalidTransition
            }
            Self::InvalidInput { .. } | Self::BoundExceeded { .. } => {
                ReplayClassification::Consistent
            }
        }
    }
}

/// The generic dispatcher contract: resolve one compiled node to exactly one
/// native executor and drive it through its prepare/effect/finalize phases.
pub struct NativeNodeDispatcher {
    /// Executors keyed by stable implementation ID.
    implementations: std::collections::BTreeMap<&'static str, Box<dyn NativeNodeExecutor>>,
}

impl NativeNodeDispatcher {
    /// Assembles the checked-in first-party executor set.
    #[must_use]
    pub fn native() -> Self {
        let mut implementations: std::collections::BTreeMap<
            &'static str,
            Box<dyn NativeNodeExecutor>,
        > = std::collections::BTreeMap::new();
        for executor in [
            Box::new(child_message::ChildMessageExecutor) as Box<dyn NativeNodeExecutor>,
            Box::new(join::JoinExecutor),
            Box::new(parallel::ParallelBranchExecutor),
            Box::new(delay::DelayExecutor),
            Box::new(schedule::ScheduleExecutor),
            Box::new(event_emission::EmitEventExecutor),
        ] {
            implementations.insert(executor.kind().implementation_id(), executor);
        }
        Self { implementations }
    }

    /// Returns the resolved executor for one node kind, mirroring Task 2's
    /// single-compatible-implementation resolution.
    #[must_use]
    pub fn resolve(&self, kind: NodeExecutorKind) -> Option<&dyn NativeNodeExecutor> {
        self.implementations
            .get(kind.implementation_id())
            .map(AsRef::as_ref)
    }

    /// Resolves by serialized node kind string.
    #[must_use]
    pub fn resolve_serialized(&self, node_kind: &str) -> Option<&dyn NativeNodeExecutor> {
        let kind = match node_kind {
            "send_child_agent_message" => NodeExecutorKind::ChildMessage,
            "join_results" => NodeExecutorKind::Join,
            "parallel_branch" => NodeExecutorKind::ParallelBranch,
            "delay" => NodeExecutorKind::Delay,
            "schedule" => NodeExecutorKind::Schedule,
            "emit_event" => NodeExecutorKind::EmitEvent,
            _ => return None,
        };
        self.resolve(kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_dispatcher_resolves_every_owned_node_kind_exactly_once() {
        let dispatcher = NativeNodeDispatcher::native();
        for kind in [
            NodeExecutorKind::ChildMessage,
            NodeExecutorKind::Join,
            NodeExecutorKind::ParallelBranch,
            NodeExecutorKind::Delay,
            NodeExecutorKind::Schedule,
            NodeExecutorKind::EmitEvent,
        ] {
            let executor = dispatcher.resolve(kind).expect("resolved");
            assert_eq!(executor.kind(), kind);
            assert_eq!(
                dispatcher
                    .resolve_serialized(kind.serialized_node_kind())
                    .expect("serialized")
                    .kind(),
                kind
            );
        }
        assert!(dispatcher.resolve_serialized("model_call").is_none());
    }

    #[test]
    fn identity_strings_are_stable_and_namespaced() {
        let identity = ExecutorIdentity {
            session_id: String::from("s1"),
            run_id: String::from("r1"),
            node_id: String::from("message"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
        };
        assert_eq!(
            identity.message_id("child-1", 1),
            "msg:s1:r1:message:4:child-1:1"
        );
        assert_eq!(identity.delay_id(), "delay:s1:r1:message:4");
        assert_eq!(
            identity.delay_continuation_id(),
            "delay-cont:s1:r1:message:4"
        );
    }

    #[test]
    fn error_recovery_classification_fails_closed_for_ambiguity() {
        assert_eq!(
            NodeExecutorError::Ambiguous {
                detail: String::from("unknown")
            }
            .recovery_classification(),
            ReplayClassification::ExternallyUncertain
        );
        assert_eq!(
            NodeExecutorError::PortFailure { port: "child" }.recovery_classification(),
            ReplayClassification::ExternallyUncertain
        );
        assert_eq!(
            NodeExecutorError::InvalidInput {
                reason: String::from("bad")
            }
            .recovery_classification(),
            ReplayClassification::Consistent
        );
    }
}
