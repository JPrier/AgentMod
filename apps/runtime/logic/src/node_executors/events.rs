//! Canonical committed events owned by native control-flow node executors.
//!
//! These payloads are committed through the ordinary runtime journal as
//! `RuntimeCommittedEvent` variants and reduce into bounded executor state.
//! Every payload is layer-local: it references runtime identifiers and
//! bounded content only, never dependency or SDK types.

use agentmod_event_model::ArtifactReference;
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};

/// Security classification of one child-agent message.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMessageClassification {
    /// Ordinary bounded instruction content.
    #[default]
    Instruction,
    /// Content references approved artifacts only.
    ArtifactReference,
    /// Content that must never enter a provider projection.
    Private,
}

/// Canonical intent to deliver one child-agent message.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageProposedEvent {
    /// Stable per-session message identity.
    pub message_id: String,
    /// Caller-supplied idempotency key for duplicate suppression.
    pub idempotency_key: String,
    /// Exact parent session.
    pub parent_session_id: String,
    /// Exact child session.
    pub child_session_id: String,
    /// Monotonic per-child message sequence.
    pub sequence: u64,
    /// Exact graph node that owns the message.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Bounded typed message content.
    pub content: String,
    /// Exact hash of the bounded content.
    pub content_hash: ContentHash,
    /// Approved artifact references carried by the message.
    pub artifact_references: Vec<ArtifactReference>,
    /// Security classification.
    pub classification: ChildMessageClassification,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
}

/// Canonical delivery of a child-agent message to the exact child session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageDeliveredEvent {
    /// Stable per-session message identity.
    pub message_id: String,
    /// Exact child session that received delivery.
    pub child_session_id: String,
    /// Bounded child-session receipt summary.
    pub receipt: String,
}

/// Canonical rejection of a child-agent message without delivery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageRejectedEvent {
    /// Stable per-session message identity.
    pub message_id: String,
    /// Exact child session that rejected the message.
    pub child_session_id: String,
    /// Stable rejection classification.
    pub reason: String,
    /// Human-safe rejection detail.
    pub detail: String,
}

/// Ordering policy for a generic join.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinOrdering {
    /// Results are ordered by participant declaration order.
    #[default]
    DeclarationOrder,
    /// Results are ordered by completion order.
    CompletionOrder,
}

/// Result projection policy for a generic join.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinProjection {
    /// The join produces bounded summary text only.
    #[default]
    SummaryOnly,
    /// The join produces typed result references only.
    TypedResults,
}

/// Artifact collection policy for a generic join.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinArtifactCollection {
    /// No artifacts are collected.
    #[default]
    None,
    /// Artifacts are collected up to the shared result bound.
    Bounded,
}

/// Terminal classification of a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinTerminalState {
    /// The join released with a successful outcome.
    Success,
    /// The join failed terminally.
    Failed,
    /// The join timed out before its required participants finished.
    TimedOut,
    /// The join was cancelled.
    Cancelled,
}

/// Canonical initialization of one generic join.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinInitializedEvent {
    /// Stable join identity (graph node execution).
    pub join_id: String,
    /// Graph node that owns the join.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Wall-clock instant at which the join was initialized; used for the
    /// caller-supplied timeout without reading a clock inside logic.
    pub initialized_at_ms: i64,
    /// Deterministically ordered required participants.
    pub expected_participants: Vec<String>,
    /// Optional participants that may join when ready.
    pub optional_participants: Vec<String>,
    /// Minimum number of successful participants required for success.
    pub min_success: u32,
    /// Maximum failures/cancellations allowed before failure.
    pub allowed_failures: u32,
    /// Optional wall-clock timeout millis.
    pub timeout_ms: Option<u64>,
    /// Result ordering policy.
    pub ordering: JoinOrdering,
    /// Result projection policy.
    pub result_projection: JoinProjection,
    /// Artifact collection policy.
    pub artifact_collection: JoinArtifactCollection,
}

/// Canonical successful outcome of one join participant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinParticipantCompletedEvent {
    /// Stable join identity.
    pub join_id: String,
    /// Exact participant execution identity.
    pub participant_execution_id: String,
    /// Bounded typed result references in policy order.
    pub result_references: Vec<String>,
    /// Exact serialized result bytes.
    pub result_bytes: u64,
}

/// Canonical failed outcome of one join participant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinParticipantFailedEvent {
    /// Stable join identity.
    pub join_id: String,
    /// Exact participant execution identity.
    pub participant_execution_id: String,
    /// Stable failure classification.
    pub reason: String,
}

/// Canonical cancelled outcome of one join participant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinParticipantCancelledEvent {
    /// Stable join identity.
    pub join_id: String,
    /// Exact participant execution identity.
    pub participant_execution_id: String,
    /// Stable cancellation classification.
    pub reason: String,
}

/// Canonical release of one generic join.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinReleasedEvent {
    /// Stable join identity.
    pub join_id: String,
    /// Terminal outcome.
    pub state: JoinTerminalState,
    /// Exact collected result references in policy order.
    pub collected_result_references: Vec<String>,
    /// Participants still missing at the release.
    pub missing_participants: Vec<String>,
    /// Stable release reason.
    pub reason: String,
}

/// Lifecycle of one parallel sub-branch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelBranchMemberState {
    /// The sub-branch has not been dispatched yet.
    Pending,
    /// The sub-branch was dispatched with an independent cancellation ID.
    Dispatched,
    /// The sub-branch completed successfully.
    Completed,
    /// The sub-branch failed.
    Failed,
    /// The sub-branch was cancelled.
    Cancelled,
}

/// Terminal classification of a parallel branch node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelBranchTerminalState {
    /// Every member reached a terminal outcome and the join policy released.
    FinishedSuccess,
    /// The node failed terminally.
    FinishedFailure,
    /// The node was cancelled.
    Cancelled,
}

/// Canonical initialization of one bounded parallel branch node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchInitializedEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Graph node that owns the branches.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Stable declared sub-branch IDs in declaration order.
    pub branch_ids: Vec<String>,
    /// Explicit maximum parallelism.
    pub max_parallelism: u32,
    /// Canonical variables written by more than one member without a merge
    /// policy; the runtime revalidates these against graph state.
    pub shared_write_scopes: Vec<String>,
}

/// Canonical dispatch of one parallel sub-branch with an independent
/// cancellation identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchMemberDispatchedEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Stable sub-branch ID.
    pub member_id: String,
    /// Deterministic zero-based dispatch order.
    pub dispatch_index: u32,
    /// Independent cancellation identity for the sub-branch.
    pub cancellation_id: String,
}

/// Canonical successful completion of one parallel sub-branch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchMemberCompletedEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Stable sub-branch ID.
    pub member_id: String,
    /// Bounded typed result references.
    pub result_references: Vec<String>,
}

/// Canonical failure of one parallel sub-branch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchMemberFailedEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Stable sub-branch ID.
    pub member_id: String,
    /// Stable failure classification.
    pub reason: String,
}

/// Canonical cancellation of one parallel sub-branch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchMemberCancelledEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Stable sub-branch ID.
    pub member_id: String,
    /// Stable cancellation classification.
    pub reason: String,
}

/// Canonical terminal outcome of one parallel branch node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchFinishedEvent {
    /// Stable node-execution identity.
    pub branch_id: String,
    /// Terminal outcome.
    pub state: ParallelBranchTerminalState,
    /// Stable terminal reason.
    pub reason: String,
}

/// Lifecycle of a durable delay node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DelayState {
    /// The exact wake time is canonical; the wake has not fired.
    Pending,
    /// The wake fired exactly once and the node resumed.
    Resumed,
    /// The delay was cancelled before its wake.
    Cancelled,
    /// The delay expired before its wake.
    Expired,
}

/// Canonical scheduling of one durable delay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayScheduledEvent {
    /// Stable node-execution identity.
    pub delay_id: String,
    /// Graph node that owns the delay.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical session containing the delay.
    pub session_id: String,
    /// Exact resolved wake time in wall-clock millis, recorded once.
    pub wake_time_ms: i64,
    /// Deterministic durable continuation bound to session/run/node/transition.
    pub continuation_id: String,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
}

/// Canonical exactly-once resume of one durable delay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayResumedEvent {
    /// Stable node-execution identity.
    pub delay_id: String,
    /// Exact canonical wake time.
    pub wake_time_ms: i64,
    /// Stable wake-proof classification.
    pub proof: String,
}

/// Canonical cancellation of one durable delay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayCancelledEvent {
    /// Stable node-execution identity.
    pub delay_id: String,
    /// Stable cancellation reason.
    pub reason: String,
}

/// Canonical expiry of one durable delay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayExpiredEvent {
    /// Stable node-execution identity.
    pub delay_id: String,
    /// Stable expiry reason.
    pub reason: String,
}

/// Trigger of a graph-owned schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum GraphScheduleTrigger {
    /// One-time wall-clock wakeup.
    AtMillis {
        /// Exact wake time.
        wake_time_ms: i64,
    },
    /// Recurring interval when permitted.
    Interval {
        /// First occurrence.
        starts_at_ms: i64,
        /// Exact period millis.
        every_ms: u64,
    },
    /// Wake on an exact committed runtime event type.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
    },
    /// Wake on exact process-output bytes.
    ProcessOutput {
        /// Runtime process identity.
        process_id: String,
        /// Literal bounded match pattern.
        contains: String,
    },
    /// Wake a durable continuation.
    Continuation {
        /// Durable continuation identity.
        continuation_id: String,
    },
}

/// Canonical proposal to create one graph-owned schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleProposedEvent {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// Graph node that owns the schedule.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical session owning the schedule.
    pub session_id: String,
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

/// Canonical creation of one graph-owned schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleCreatedEvent {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// Graph node that owns the schedule.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical session owning the schedule.
    pub session_id: String,
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

/// Canonical rejection of a graph-owned schedule proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleRejectedEvent {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// Graph node that owns the schedule.
    pub node_id: String,
    /// Stable rejection classification.
    pub reason: String,
}

/// Canonical removal of one graph-owned schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleRemovedEvent {
    /// Stable schedule identity.
    pub schedule_id: String,
    /// Graph node that owned the schedule.
    pub node_id: String,
    /// Stable removal reason.
    pub reason: String,
}

/// Canonical emission of one declared user-space event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserEventEmittedEvent {
    /// Stable emission identity (canonical event ID).
    pub emission_id: String,
    /// Graph node that emitted the event.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Declared user-space namespace.
    pub namespace: String,
    /// Declared event type within the namespace.
    pub event_type: String,
    /// Canonical sequence of the emitted event.
    pub sequence: u64,
    /// Bounded typed payload as canonical JSON.
    pub payload_json: String,
    /// Exact hash of the bounded payload.
    pub payload_hash: ContentHash,
    /// Approved artifact references.
    pub artifact_references: Vec<ArtifactReference>,
    /// Bounded non-secret metadata as canonical JSON.
    pub metadata_json: String,
    /// Correlation identity constructed by the runtime.
    pub correlation_id: String,
    /// Causation identity constructed by the runtime.
    pub causation_id: String,
}

/// Typed payload of one native control-flow node event.
///
/// The runtime maps these payloads into `RuntimeCommittedEvent` variants at
/// the canonical journal boundary; the reducer reconstructs executor state
/// from the same payloads during replay.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum NodeExecutorEventPayload {
    /// Child-agent message intent.
    ChildMessageProposed(ChildMessageProposedEvent),
    /// Child-agent message delivery.
    ChildMessageDelivered(ChildMessageDeliveredEvent),
    /// Child-agent message rejection.
    ChildMessageRejected(ChildMessageRejectedEvent),
    /// Generic join initialization.
    JoinInitialized(JoinInitializedEvent),
    /// Generic join participant success.
    JoinParticipantCompleted(JoinParticipantCompletedEvent),
    /// Generic join participant failure.
    JoinParticipantFailed(JoinParticipantFailedEvent),
    /// Generic join participant cancellation.
    JoinParticipantCancelled(JoinParticipantCancelledEvent),
    /// Generic join release.
    JoinReleased(JoinReleasedEvent),
    /// Parallel branch initialization.
    ParallelBranchInitialized(ParallelBranchInitializedEvent),
    /// Parallel sub-branch dispatch.
    ParallelBranchMemberDispatched(ParallelBranchMemberDispatchedEvent),
    /// Parallel sub-branch completion.
    ParallelBranchMemberCompleted(ParallelBranchMemberCompletedEvent),
    /// Parallel sub-branch failure.
    ParallelBranchMemberFailed(ParallelBranchMemberFailedEvent),
    /// Parallel sub-branch cancellation.
    ParallelBranchMemberCancelled(ParallelBranchMemberCancelledEvent),
    /// Parallel branch terminal outcome.
    ParallelBranchFinished(ParallelBranchFinishedEvent),
    /// Durable delay scheduling.
    DelayScheduled(DelayScheduledEvent),
    /// Durable delay resume.
    DelayResumed(DelayResumedEvent),
    /// Durable delay cancellation.
    DelayCancelled(DelayCancelledEvent),
    /// Durable delay expiry.
    DelayExpired(DelayExpiredEvent),
    /// Graph-owned schedule proposal.
    GraphScheduleProposed(GraphScheduleProposedEvent),
    /// Graph-owned schedule creation.
    GraphScheduleCreated(GraphScheduleCreatedEvent),
    /// Graph-owned schedule proposal rejection.
    GraphScheduleRejected(GraphScheduleRejectedEvent),
    /// Graph-owned schedule removal.
    GraphScheduleRemoved(GraphScheduleRemovedEvent),
    /// Declared user-space event emission.
    UserEventEmitted(UserEventEmittedEvent),
}

impl NodeExecutorEventPayload {
    /// Returns the stable metadata event type required for this payload.
    #[must_use]
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::ChildMessageProposed(_) => "child_agent.message_proposed",
            Self::ChildMessageDelivered(_) => "child_agent.message_delivered",
            Self::ChildMessageRejected(_) => "child_agent.message_rejected",
            Self::JoinInitialized(_) => "child_agent.join_initialized",
            Self::JoinParticipantCompleted(_) => "child_agent.join_participant_completed",
            Self::JoinParticipantFailed(_) => "child_agent.join_participant_failed",
            Self::JoinParticipantCancelled(_) => "child_agent.join_participant_cancelled",
            Self::JoinReleased(_) => "child_agent.join_released",
            Self::ParallelBranchInitialized(_) => "parallel.branch_initialized",
            Self::ParallelBranchMemberDispatched(_) => "parallel.branch_member_dispatched",
            Self::ParallelBranchMemberCompleted(_) => "parallel.branch_member_completed",
            Self::ParallelBranchMemberFailed(_) => "parallel.branch_member_failed",
            Self::ParallelBranchMemberCancelled(_) => "parallel.branch_member_cancelled",
            Self::ParallelBranchFinished(_) => "parallel.branch_finished",
            Self::DelayScheduled(_) => "delay.scheduled",
            Self::DelayResumed(_) => "delay.resumed",
            Self::DelayCancelled(_) => "delay.cancelled",
            Self::DelayExpired(_) => "delay.expired",
            Self::GraphScheduleProposed(_) => "schedule.graph_proposed",
            Self::GraphScheduleCreated(_) => "schedule.graph_created",
            Self::GraphScheduleRejected(_) => "schedule.graph_rejected",
            Self::GraphScheduleRemoved(_) => "schedule.graph_removed",
            Self::UserEventEmitted(_) => "event.user_emitted",
        }
    }
}
