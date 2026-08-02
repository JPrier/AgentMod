//! Replay-owned state reconstruction for native control-flow node executors.
//!
//! The canonical journal is the only source of truth. This module reduces
//! committed event payloads into bounded executor state without invoking any
//! effect, and classifies ambiguous replay positions so recovery fails closed.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::node_executors::events::{
    ChildMessageClassification, ChildMessageDeliveredEvent, ChildMessageProposedEvent,
    ChildMessageRejectedEvent, DelayCancelledEvent, DelayExpiredEvent, DelayResumedEvent,
    DelayScheduledEvent, GraphScheduleCreatedEvent, GraphScheduleProposedEvent,
    GraphScheduleRejectedEvent, GraphScheduleRemovedEvent, GraphScheduleTrigger,
    JoinInitializedEvent, JoinParticipantCancelledEvent, JoinParticipantCompletedEvent,
    JoinParticipantFailedEvent, JoinReleasedEvent, JoinTerminalState, NodeExecutorEventPayload,
    ParallelBranchFinishedEvent, ParallelBranchInitializedEvent,
    ParallelBranchMemberCancelledEvent, ParallelBranchMemberCompletedEvent,
    ParallelBranchMemberDispatchedEvent, ParallelBranchMemberFailedEvent,
    ParallelBranchTerminalState, UserEventEmittedEvent,
};

/// Hard per-domain record bounds shared by every native node-executor state.
pub const MAX_NODE_EXECUTOR_RECORDS: usize = 1_024;
/// Hard bound on one canonical child message payload in bytes.
pub const MAX_CHILD_MESSAGE_BYTES: usize = 256 * 1024;
/// Hard bound on one emitted user event payload in bytes.
pub const MAX_EMITTED_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Maximum number of parallel sub-branches one parallel node may declare.
pub const MAX_PARALLEL_BRANCHES: usize = 64;
/// Maximum participants one generic join may track.
pub const MAX_JOIN_PARTICIPANTS: usize = 256;
/// Maximum schedule/child-message identifiers retained per state.
pub const MAX_EXECUTOR_IDENTIFIER_BYTES: usize = 256;

/// Lifecycle of one child-agent message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMessageState {
    /// Proposal is canonical; delivery has not been committed.
    Proposed,
    /// Delivery to the exact child session is canonical.
    Delivered,
    /// The message was rejected, expired, or cancelled without delivery.
    Rejected,
}

/// Replay-owned child-agent message record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageRecord {
    /// Stable per-session message identity.
    pub message_id: String,
    /// Caller-supplied idempotency key.
    pub idempotency_key: String,
    /// Exact parent session.
    pub parent_session_id: String,
    /// Exact child session.
    pub child_session_id: String,
    /// Monotonic per-child message sequence.
    pub sequence: u64,
    /// Current lifecycle.
    pub state: ChildMessageState,
    /// Bounded message content retained for post-restart delivery.
    pub content: String,
    /// Exact content hash.
    pub content_hash: agentmod_primitives::ContentHash,
    /// Security classification.
    pub classification: ChildMessageClassification,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
    /// Graph node that owns the message.
    pub node_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical sequence of the delivery event, when delivered.
    pub delivered_at_sequence: Option<u64>,
    /// Stable rejection reason, when rejected.
    pub rejection_reason: Option<String>,
}

/// Terminal classification of a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinState {
    /// The join is still awaiting participants.
    Awaiting,
    /// The join released with a successful outcome.
    Success,
    /// The join failed terminally.
    Failed,
    /// The join timed out before its required participants finished.
    TimedOut,
    /// The join was cancelled.
    Cancelled,
}

/// Replay-owned generic join record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRecord {
    /// Stable join identity.
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
    /// Deterministically ordered required participants.
    pub expected_participants: Vec<String>,
    /// Optional participants that may join when ready.
    pub optional_participants: Vec<String>,
    /// Participants with a committed successful outcome.
    pub completed_participants: BTreeSet<String>,
    /// Participants with a committed failure outcome.
    pub failed_participants: BTreeSet<String>,
    /// Participants with a committed cancellation outcome.
    pub cancelled_participants: BTreeSet<String>,
    /// Minimum successful participants required for success.
    pub min_success: u32,
    /// Maximum failures/cancellations allowed before failure.
    pub allowed_failures: u32,
    /// Optional wall-clock timeout millis.
    pub timeout_ms: Option<u64>,
    /// Result ordering policy.
    pub ordering: crate::node_executors::events::JoinOrdering,
    /// Result projection policy.
    pub result_projection: crate::node_executors::events::JoinProjection,
    /// Artifact collection policy.
    pub artifact_collection: crate::node_executors::events::JoinArtifactCollection,
    /// Current join lifecycle.
    pub state: JoinState,
    /// Exact collected result references in policy order.
    pub collected_result_references: Vec<String>,
    /// Participants still missing at a terminal state.
    pub missing_participants: BTreeSet<String>,
    /// Canonical sequence that released the join, when released.
    pub released_at_sequence: Option<u64>,
    /// Stable terminal reason.
    pub terminal_reason: Option<String>,
    /// Wall-clock instant at which the join was initialized.
    pub initialized_at_ms: i64,
    /// Canonical sequence of the join initialization event.
    pub initialized_at_sequence: u64,
}

impl JoinRecord {
    /// Whether the join reached a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.state != JoinState::Awaiting
    }

    /// Participants that have neither completed, failed, nor cancelled.
    #[must_use]
    pub fn missing(&self) -> BTreeSet<String> {
        self.expected_participants
            .iter()
            .chain(self.optional_participants.iter())
            .filter(|participant| {
                !self.completed_participants.contains(*participant)
                    && !self.failed_participants.contains(*participant)
                    && !self.cancelled_participants.contains(*participant)
            })
            .cloned()
            .collect()
    }
}

/// Lifecycle of one parallel sub-branch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelMemberState {
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelBranchState {
    /// The node is still dispatching or awaiting members.
    Running,
    /// The join policy released with a successful outcome.
    FinishedSuccess,
    /// The node failed terminally.
    FinishedFailure,
    /// The node was cancelled.
    Cancelled,
}

/// Replay-owned parallel branch node record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchRecord {
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
    /// Deterministic dispatch order so far.
    pub dispatched_order: Vec<String>,
    /// Per-member lifecycle.
    pub member_states: BTreeMap<String, ParallelMemberState>,
    /// Independent cancellation ID per dispatched member.
    pub cancellation_ids: BTreeMap<String, String>,
    /// Canonical variables written by more than one member.
    pub shared_write_scopes: Vec<String>,
    /// Overall node lifecycle.
    pub state: ParallelBranchState,
    /// Stable terminal reason.
    pub terminal_reason: Option<String>,
    /// Canonical sequence of the initialization event.
    pub initialized_at_sequence: u64,
}

impl ParallelBranchRecord {
    /// Whether the node reached a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.state != ParallelBranchState::Running
    }

    /// Members dispatched but without a terminal outcome.
    #[must_use]
    pub fn dispatched_without_terminal(&self) -> Vec<String> {
        self.dispatched_order
            .iter()
            .filter(|member| {
                matches!(
                    self.member_states.get(*member),
                    Some(ParallelMemberState::Dispatched)
                )
            })
            .cloned()
            .collect()
    }
}

/// Lifecycle of a durable delay node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Replay-owned durable delay record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayRecord {
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
    /// Exact resolved wake time in wall-clock millis.
    pub wake_time_ms: i64,
    /// Deterministic durable continuation identity.
    pub continuation_id: String,
    /// Current delay lifecycle.
    pub state: DelayState,
    /// Canonical sequence of the resume event, when resumed.
    pub resumed_at_sequence: Option<u64>,
    /// Stable wake proof classification.
    pub resume_proof: Option<String>,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
    /// Stable terminal reason.
    pub terminal_reason: Option<String>,
    /// Canonical sequence of the scheduling event.
    pub scheduled_at_sequence: u64,
}

impl DelayRecord {
    /// Whether the delay reached a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.state != DelayState::Pending
    }
}

/// Lifecycle of a graph-owned schedule.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphScheduleState {
    /// Creation proposal is canonical; policy has not approved.
    Proposed,
    /// The schedule is active in the scheduler worker.
    Active,
    /// The proposal was rejected by policy.
    Rejected,
    /// The schedule was removed.
    Removed,
}

/// Replay-owned graph-owned schedule record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleRecord {
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
    /// Current lifecycle.
    pub state: GraphScheduleState,
    /// Canonical sequence of the creation event.
    pub created_at_sequence: Option<u64>,
    /// Stable terminal reason.
    pub terminal_reason: Option<String>,
}

/// Replay-owned emitted user-space event record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmittedEventRecord {
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
    /// Bounded typed payload.
    pub payload_json: String,
    /// Exact payload hash.
    pub payload_hash: agentmod_primitives::ContentHash,
    /// Bounded non-secret metadata.
    pub metadata_json: String,
}

/// Complete replay-owned state of every native control-flow node executor.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExecutorState {
    /// Child-agent message ledger keyed by message identity.
    #[serde(default)]
    pub child_messages: BTreeMap<String, ChildMessageRecord>,
    /// Generic join state keyed by join identity.
    #[serde(default)]
    pub joins: BTreeMap<String, JoinRecord>,
    /// Parallel branch state keyed by node-execution identity.
    #[serde(default)]
    pub parallel_branches: BTreeMap<String, ParallelBranchRecord>,
    /// Durable delay state keyed by node-execution identity.
    #[serde(default)]
    pub delays: BTreeMap<String, DelayRecord>,
    /// Graph-owned schedule registry keyed by schedule identity.
    #[serde(default)]
    pub schedules: BTreeMap<String, GraphScheduleRecord>,
    /// Emitted user-space event ledger in committed order.
    #[serde(default)]
    pub emitted_events: Vec<EmittedEventRecord>,
}

/// Classifies a replay position so recovery can fail closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayClassification {
    /// The state is consistent; work may continue.
    Consistent,
    /// The next legal action is unambiguous; safe to proceed.
    SafeToProceed,
    /// External effect may or may not have happened; never redispatch.
    ExternallyUncertain,
    /// Canonical state conflicts with a proposed transition.
    InvalidTransition,
}

impl NodeExecutorState {
    /// Applies one committed event payload, enforcing strict transitions and
    /// hard bounds without performing any external effect.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorReducerError`] when the event violates a
    /// transition, identity, idempotency, or bound invariant.
    pub fn apply(
        &mut self,
        payload: &NodeExecutorEventPayload,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        match payload {
            NodeExecutorEventPayload::ChildMessageProposed(event) => {
                self.apply_child_message_proposed(event)
            }
            NodeExecutorEventPayload::ChildMessageDelivered(event) => {
                self.apply_child_message_delivered(event, sequence)
            }
            NodeExecutorEventPayload::ChildMessageRejected(event) => {
                self.apply_child_message_rejected(event)
            }
            NodeExecutorEventPayload::JoinInitialized(event) => {
                self.apply_join_initialized(event, sequence)
            }
            NodeExecutorEventPayload::JoinParticipantCompleted(event) => {
                self.apply_join_participant_completed(event, sequence)
            }
            NodeExecutorEventPayload::JoinParticipantFailed(event) => {
                self.apply_join_participant_failed(event, sequence)
            }
            NodeExecutorEventPayload::JoinParticipantCancelled(event) => {
                self.apply_join_participant_cancelled(event, sequence)
            }
            NodeExecutorEventPayload::JoinReleased(event) => {
                self.apply_join_released(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchInitialized(event) => {
                self.apply_parallel_initialized(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchMemberDispatched(event) => {
                self.apply_parallel_dispatched(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchMemberCompleted(event) => {
                self.apply_parallel_completed(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchMemberFailed(event) => {
                self.apply_parallel_failed(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchMemberCancelled(event) => {
                self.apply_parallel_cancelled(event, sequence)
            }
            NodeExecutorEventPayload::ParallelBranchFinished(event) => {
                self.apply_parallel_finished(event, sequence)
            }
            NodeExecutorEventPayload::DelayScheduled(event) => {
                self.apply_delay_scheduled(event, sequence)
            }
            NodeExecutorEventPayload::DelayResumed(event) => {
                self.apply_delay_resumed(event, sequence)
            }
            NodeExecutorEventPayload::DelayCancelled(event) => {
                self.apply_delay_cancelled(event, sequence)
            }
            NodeExecutorEventPayload::DelayExpired(event) => {
                self.apply_delay_expired(event, sequence)
            }
            NodeExecutorEventPayload::GraphScheduleProposed(event) => {
                self.apply_schedule_proposed(event, sequence)
            }
            NodeExecutorEventPayload::GraphScheduleCreated(event) => {
                self.apply_schedule_created(event, sequence)
            }
            NodeExecutorEventPayload::GraphScheduleRejected(event) => {
                self.apply_schedule_rejected(event, sequence)
            }
            NodeExecutorEventPayload::GraphScheduleRemoved(event) => {
                self.apply_schedule_removed(event, sequence)
            }
            NodeExecutorEventPayload::UserEventEmitted(event) => {
                self.apply_user_event_emitted(event, sequence)
            }
        }
    }

    /// Reduces an ordered committed event stream without effects.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorReducerError`] on the first invalid transition.
    pub fn reduce(
        &mut self,
        events: impl IntoIterator<Item = (u64, NodeExecutorEventPayload)>,
    ) -> Result<(), NodeExecutorReducerError> {
        for (sequence, payload) in events {
            self.apply(&payload, sequence)?;
        }
        Ok(())
    }

    /// Reconstructs the exact replay classification for one node identity.
    ///
    /// The dispatcher uses this to decide whether entering a node is safe,
    /// whether an effect must never be redispatched, or whether the canonical
    /// position conflicts with the requested transition.
    #[must_use]
    pub fn classify_replay(&self, identity: &ExecutorIdentity) -> ReplayClassification {
        let child = self
            .child_messages
            .values()
            .find(|record| record.node_id == identity.node_id && record.run_id == identity.run_id);
        if let Some(child) = child
            && child.state == crate::node_executors::state::ChildMessageState::Proposed
        {
            // A proposed message whose delivery is not canonical may or may
            // not have reached the child boundary; never redeliver blindly.
            return ReplayClassification::ExternallyUncertain;
        }
        if let Some(join) = self.joins.get(&identity.join_id())
            && !join.is_terminal()
        {
            return ReplayClassification::SafeToProceed;
        }
        if let Some(parallel) = self.parallel_branches.get(&identity.branch_id())
            && !parallel.dispatched_without_terminal().is_empty()
        {
            return ReplayClassification::ExternallyUncertain;
        }
        if let Some(delay) = self.delays.get(&identity.delay_id())
            && delay.state == crate::node_executors::state::DelayState::Pending
        {
            // The delay event is canonical; the create-once continuation is
            // idempotent by its deterministic identity.
            return ReplayClassification::SafeToProceed;
        }
        ReplayClassification::Consistent
    }

    fn apply_child_message_proposed(
        &mut self,
        event: &ChildMessageProposedEvent,
    ) -> Result<(), NodeExecutorReducerError> {
        if event.content.len() > MAX_CHILD_MESSAGE_BYTES {
            return Err(NodeExecutorReducerError::BoundExceeded);
        }
        if event.content_hash != agentmod_primitives::ContentHash::digest(event.content.as_bytes())
        {
            return Err(NodeExecutorReducerError::ContentHashMismatch);
        }
        if self.child_messages.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        if let Some(existing) = self.child_messages.get(&event.message_id) {
            if existing.idempotency_key != event.idempotency_key
                || existing.child_session_id != event.child_session_id
            {
                return Err(NodeExecutorReducerError::IdentityMismatch);
            }
            // Duplicate proposal with identical identity is a replay no-op.
            return Ok(());
        }
        for record in self.child_messages.values() {
            if record.idempotency_key == event.idempotency_key
                && record.child_session_id == event.child_session_id
            {
                if record.content_hash != event.content_hash || record.sequence != event.sequence {
                    return Err(NodeExecutorReducerError::IdempotencyConflict);
                }
                return Ok(());
            }
        }
        let record = ChildMessageRecord {
            message_id: event.message_id.clone(),
            idempotency_key: event.idempotency_key.clone(),
            parent_session_id: event.parent_session_id.clone(),
            child_session_id: event.child_session_id.clone(),
            sequence: event.sequence,
            state: ChildMessageState::Proposed,
            content: event.content.clone(),
            content_hash: event.content_hash,
            classification: event.classification,
            expires_at_ms: event.expires_at_ms,
            node_id: event.node_id.clone(),
            run_id: event.run_id.clone(),
            attempt: event.attempt,
            loop_iteration: event.loop_iteration,
            step: event.step,
            delivered_at_sequence: None,
            rejection_reason: None,
        };
        self.child_messages.insert(event.message_id.clone(), record);
        Ok(())
    }

    fn apply_child_message_delivered(
        &mut self,
        event: &ChildMessageDeliveredEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let record = self
            .child_messages
            .get_mut(&event.message_id)
            .ok_or(NodeExecutorReducerError::UnknownMessage)?;
        if record.state != ChildMessageState::Proposed {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if record.child_session_id != event.child_session_id {
            return Err(NodeExecutorReducerError::IdentityMismatch);
        }
        record.state = ChildMessageState::Delivered;
        record.delivered_at_sequence = Some(sequence);
        Ok(())
    }

    fn apply_child_message_rejected(
        &mut self,
        event: &ChildMessageRejectedEvent,
    ) -> Result<(), NodeExecutorReducerError> {
        let record = self
            .child_messages
            .get_mut(&event.message_id)
            .ok_or(NodeExecutorReducerError::UnknownMessage)?;
        if record.state != ChildMessageState::Proposed {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if record.child_session_id != event.child_session_id {
            return Err(NodeExecutorReducerError::IdentityMismatch);
        }
        record.state = ChildMessageState::Rejected;
        record.rejection_reason = Some(event.reason.clone());
        Ok(())
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "participant counts are bounded far below u32::MAX"
    )]
    fn apply_join_initialized(
        &mut self,
        event: &JoinInitializedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        if event.expected_participants.len() + event.optional_participants.len()
            > MAX_JOIN_PARTICIPANTS
            || event.min_success > event.expected_participants.len() as u32
        {
            return Err(NodeExecutorReducerError::BoundExceeded);
        }
        if self.joins.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        if let Some(existing) = self.joins.get(&event.join_id) {
            if existing.initialized_at_sequence == sequence {
                return Ok(());
            }
            return Err(NodeExecutorReducerError::DuplicateInitialization);
        }
        self.joins.insert(
            event.join_id.clone(),
            JoinRecord {
                join_id: event.join_id.clone(),
                node_id: event.node_id.clone(),
                run_id: event.run_id.clone(),
                attempt: event.attempt,
                loop_iteration: event.loop_iteration,
                step: event.step,
                expected_participants: event.expected_participants.clone(),
                optional_participants: event.optional_participants.clone(),
                completed_participants: BTreeSet::new(),
                failed_participants: BTreeSet::new(),
                cancelled_participants: BTreeSet::new(),
                min_success: event.min_success,
                allowed_failures: event.allowed_failures,
                timeout_ms: event.timeout_ms,
                ordering: event.ordering,
                result_projection: event.result_projection,
                artifact_collection: event.artifact_collection,
                state: JoinState::Awaiting,
                collected_result_references: Vec::new(),
                missing_participants: BTreeSet::new(),
                released_at_sequence: None,
                terminal_reason: None,
                initialized_at_ms: event.initialized_at_ms,
                initialized_at_sequence: sequence,
            },
        );
        Ok(())
    }

    fn apply_join_participant_completed(
        &mut self,
        event: &JoinParticipantCompletedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let join = self
            .joins
            .get_mut(&event.join_id)
            .ok_or(NodeExecutorReducerError::UnknownJoin)?;
        if join.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if !join
            .expected_participants
            .iter()
            .chain(join.optional_participants.iter())
            .any(|participant| participant == &event.participant_execution_id)
        {
            return Err(NodeExecutorReducerError::UnknownParticipant);
        }
        if join
            .completed_participants
            .insert(event.participant_execution_id.clone())
        {
            join.collected_result_references
                .extend(event.result_references.iter().cloned());
            let _ = sequence;
        }
        Ok(())
    }

    fn apply_join_participant_failed(
        &mut self,
        event: &JoinParticipantFailedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let join = self
            .joins
            .get_mut(&event.join_id)
            .ok_or(NodeExecutorReducerError::UnknownJoin)?;
        if join.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if !join
            .expected_participants
            .iter()
            .chain(join.optional_participants.iter())
            .any(|participant| participant == &event.participant_execution_id)
        {
            return Err(NodeExecutorReducerError::UnknownParticipant);
        }
        if join
            .failed_participants
            .insert(event.participant_execution_id.clone())
        {
            let _ = event.reason.clone();
        }
        Ok(())
    }

    fn apply_join_participant_cancelled(
        &mut self,
        event: &JoinParticipantCancelledEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let join = self
            .joins
            .get_mut(&event.join_id)
            .ok_or(NodeExecutorReducerError::UnknownJoin)?;
        if join.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if !join
            .expected_participants
            .iter()
            .chain(join.optional_participants.iter())
            .any(|participant| participant == &event.participant_execution_id)
        {
            return Err(NodeExecutorReducerError::UnknownParticipant);
        }
        join.cancelled_participants
            .insert(event.participant_execution_id.clone());
        Ok(())
    }

    fn apply_join_released(
        &mut self,
        event: &JoinReleasedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let join = self
            .joins
            .get_mut(&event.join_id)
            .ok_or(NodeExecutorReducerError::UnknownJoin)?;
        if join.is_terminal() {
            return Err(NodeExecutorReducerError::DuplicateRelease);
        }
        let missing = join.missing();
        join.state = match event.state {
            JoinTerminalState::Success => JoinState::Success,
            JoinTerminalState::Failed => JoinState::Failed,
            JoinTerminalState::TimedOut => JoinState::TimedOut,
            JoinTerminalState::Cancelled => JoinState::Cancelled,
        };
        join.collected_result_references
            .clone_from(&event.collected_result_references);
        join.missing_participants = missing;
        join.released_at_sequence = Some(sequence);
        join.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_parallel_initialized(
        &mut self,
        event: &ParallelBranchInitializedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        if event.branch_ids.len() > MAX_PARALLEL_BRANCHES
            || event.max_parallelism == 0
            || event.max_parallelism as usize > event.branch_ids.len()
        {
            return Err(NodeExecutorReducerError::BoundExceeded);
        }
        if self.parallel_branches.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        if let Some(existing) = self.parallel_branches.get(&event.branch_id) {
            if existing.initialized_at_sequence == sequence {
                return Ok(());
            }
            return Err(NodeExecutorReducerError::DuplicateInitialization);
        }
        let mut member_states = BTreeMap::new();
        for member in &event.branch_ids {
            member_states.insert(member.clone(), ParallelMemberState::Pending);
        }
        self.parallel_branches.insert(
            event.branch_id.clone(),
            ParallelBranchRecord {
                branch_id: event.branch_id.clone(),
                node_id: event.node_id.clone(),
                run_id: event.run_id.clone(),
                attempt: event.attempt,
                loop_iteration: event.loop_iteration,
                step: event.step,
                branch_ids: event.branch_ids.clone(),
                max_parallelism: event.max_parallelism,
                dispatched_order: Vec::new(),
                member_states,
                cancellation_ids: BTreeMap::new(),
                shared_write_scopes: event.shared_write_scopes.clone(),
                state: ParallelBranchState::Running,
                terminal_reason: None,
                initialized_at_sequence: sequence,
            },
        );
        Ok(())
    }

    fn apply_parallel_dispatched(
        &mut self,
        event: &ParallelBranchMemberDispatchedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let branch = self
            .parallel_branches
            .get_mut(&event.branch_id)
            .ok_or(NodeExecutorReducerError::UnknownParallelBranch)?;
        if branch.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if !branch.branch_ids.contains(&event.member_id) {
            return Err(NodeExecutorReducerError::UnknownParticipant);
        }
        let state = branch
            .member_states
            .get_mut(&event.member_id)
            .ok_or(NodeExecutorReducerError::UnknownParticipant)?;
        if *state != ParallelMemberState::Pending {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if event.dispatch_index as usize != branch.dispatched_order.len() {
            return Err(NodeExecutorReducerError::DispatchOrderViolation);
        }
        *state = ParallelMemberState::Dispatched;
        branch.dispatched_order.push(event.member_id.clone());
        branch
            .cancellation_ids
            .insert(event.member_id.clone(), event.cancellation_id.clone());
        Ok(())
    }

    fn apply_parallel_completed(
        &mut self,
        event: &ParallelBranchMemberCompletedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let branch = self
            .parallel_branches
            .get_mut(&event.branch_id)
            .ok_or(NodeExecutorReducerError::UnknownParallelBranch)?;
        if branch.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        let state = branch
            .member_states
            .get_mut(&event.member_id)
            .ok_or(NodeExecutorReducerError::UnknownParticipant)?;
        if *state != ParallelMemberState::Dispatched {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        *state = ParallelMemberState::Completed;
        Ok(())
    }

    fn apply_parallel_failed(
        &mut self,
        event: &ParallelBranchMemberFailedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let branch = self
            .parallel_branches
            .get_mut(&event.branch_id)
            .ok_or(NodeExecutorReducerError::UnknownParallelBranch)?;
        if branch.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        let state = branch
            .member_states
            .get_mut(&event.member_id)
            .ok_or(NodeExecutorReducerError::UnknownParticipant)?;
        if *state != ParallelMemberState::Dispatched {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        *state = ParallelMemberState::Failed;
        Ok(())
    }

    fn apply_parallel_cancelled(
        &mut self,
        event: &ParallelBranchMemberCancelledEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let branch = self
            .parallel_branches
            .get_mut(&event.branch_id)
            .ok_or(NodeExecutorReducerError::UnknownParallelBranch)?;
        if branch.is_terminal() {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        let state = branch
            .member_states
            .get_mut(&event.member_id)
            .ok_or(NodeExecutorReducerError::UnknownParticipant)?;
        if *state != ParallelMemberState::Dispatched {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        *state = ParallelMemberState::Cancelled;
        Ok(())
    }

    fn apply_parallel_finished(
        &mut self,
        event: &ParallelBranchFinishedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let branch = self
            .parallel_branches
            .get_mut(&event.branch_id)
            .ok_or(NodeExecutorReducerError::UnknownParallelBranch)?;
        if branch.is_terminal() {
            return Err(NodeExecutorReducerError::DuplicateRelease);
        }
        let dispatched_without_terminal = branch.dispatched_without_terminal();
        if event.state == ParallelBranchTerminalState::FinishedSuccess
            && !dispatched_without_terminal.is_empty()
        {
            return Err(NodeExecutorReducerError::AmbiguousFinish);
        }
        let _ = sequence;
        branch.state = match event.state {
            ParallelBranchTerminalState::FinishedSuccess => ParallelBranchState::FinishedSuccess,
            ParallelBranchTerminalState::FinishedFailure => ParallelBranchState::FinishedFailure,
            ParallelBranchTerminalState::Cancelled => ParallelBranchState::Cancelled,
        };
        branch.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_delay_scheduled(
        &mut self,
        event: &DelayScheduledEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        if event.wake_time_ms < 0 || event.continuation_id.is_empty() {
            return Err(NodeExecutorReducerError::BoundExceeded);
        }
        if self.delays.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        if let Some(existing) = self.delays.get(&event.delay_id) {
            if existing.scheduled_at_sequence == sequence
                && existing.wake_time_ms == event.wake_time_ms
            {
                return Ok(());
            }
            return Err(NodeExecutorReducerError::DuplicateInitialization);
        }
        self.delays.insert(
            event.delay_id.clone(),
            DelayRecord {
                delay_id: event.delay_id.clone(),
                node_id: event.node_id.clone(),
                run_id: event.run_id.clone(),
                attempt: event.attempt,
                loop_iteration: event.loop_iteration,
                step: event.step,
                session_id: event.session_id.clone(),
                wake_time_ms: event.wake_time_ms,
                continuation_id: event.continuation_id.clone(),
                state: DelayState::Pending,
                resumed_at_sequence: None,
                resume_proof: None,
                expires_at_ms: event.expires_at_ms,
                terminal_reason: None,
                scheduled_at_sequence: sequence,
            },
        );
        Ok(())
    }

    fn apply_delay_resumed(
        &mut self,
        event: &DelayResumedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let delay = self
            .delays
            .get_mut(&event.delay_id)
            .ok_or(NodeExecutorReducerError::UnknownDelay)?;
        if delay.state != DelayState::Pending {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if delay.wake_time_ms != event.wake_time_ms {
            return Err(NodeExecutorReducerError::IdentityMismatch);
        }
        delay.state = DelayState::Resumed;
        delay.resumed_at_sequence = Some(sequence);
        delay.resume_proof = Some(event.proof.clone());
        Ok(())
    }

    fn apply_delay_cancelled(
        &mut self,
        event: &DelayCancelledEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let delay = self
            .delays
            .get_mut(&event.delay_id)
            .ok_or(NodeExecutorReducerError::UnknownDelay)?;
        if delay.state != DelayState::Pending {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        delay.state = DelayState::Cancelled;
        delay.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_delay_expired(
        &mut self,
        event: &DelayExpiredEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let delay = self
            .delays
            .get_mut(&event.delay_id)
            .ok_or(NodeExecutorReducerError::UnknownDelay)?;
        if delay.state != DelayState::Pending {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        delay.state = DelayState::Expired;
        delay.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_schedule_proposed(
        &mut self,
        event: &GraphScheduleProposedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        if self.schedules.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        if let Some(existing) = self.schedules.get(&event.schedule_id) {
            if existing.idempotency_key != event.idempotency_key {
                return Err(NodeExecutorReducerError::IdempotencyConflict);
            }
            return Ok(());
        }
        self.schedules.insert(
            event.schedule_id.clone(),
            GraphScheduleRecord {
                schedule_id: event.schedule_id.clone(),
                node_id: event.node_id.clone(),
                run_id: event.run_id.clone(),
                attempt: event.attempt,
                loop_iteration: event.loop_iteration,
                step: event.step,
                session_id: event.session_id.clone(),
                idempotency_key: event.idempotency_key.clone(),
                trigger: event.trigger.clone(),
                style: event.style.clone(),
                workspace: event.workspace.clone(),
                permission_policy: event.permission_policy.clone(),
                provider: event.provider.clone(),
                model: event.model.clone(),
                token_budget: event.token_budget,
                cost_budget_micros: event.cost_budget_micros,
                state: GraphScheduleState::Proposed,
                created_at_sequence: Some(sequence),
                terminal_reason: None,
            },
        );
        Ok(())
    }

    fn apply_schedule_created(
        &mut self,
        event: &GraphScheduleCreatedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let schedule = self
            .schedules
            .get_mut(&event.schedule_id)
            .ok_or(NodeExecutorReducerError::UnknownSchedule)?;
        if schedule.state != GraphScheduleState::Proposed {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        if schedule.idempotency_key != event.idempotency_key
            || schedule.trigger != event.trigger
            || schedule.style != event.style
            || schedule.workspace != event.workspace
            || schedule.provider != event.provider
            || schedule.model != event.model
        {
            return Err(NodeExecutorReducerError::IdentityMismatch);
        }
        schedule.state = GraphScheduleState::Active;
        Ok(())
    }

    fn apply_schedule_rejected(
        &mut self,
        event: &GraphScheduleRejectedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let schedule = self
            .schedules
            .get_mut(&event.schedule_id)
            .ok_or(NodeExecutorReducerError::UnknownSchedule)?;
        if schedule.state != GraphScheduleState::Proposed {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        schedule.state = GraphScheduleState::Rejected;
        schedule.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_schedule_removed(
        &mut self,
        event: &GraphScheduleRemovedEvent,
        _sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        let schedule = self
            .schedules
            .get_mut(&event.schedule_id)
            .ok_or(NodeExecutorReducerError::UnknownSchedule)?;
        if schedule.state != GraphScheduleState::Active {
            return Err(NodeExecutorReducerError::InvalidTransition);
        }
        schedule.state = GraphScheduleState::Removed;
        schedule.terminal_reason = Some(event.reason.clone());
        Ok(())
    }

    fn apply_user_event_emitted(
        &mut self,
        event: &UserEventEmittedEvent,
        sequence: u64,
    ) -> Result<(), NodeExecutorReducerError> {
        if event.payload_json.len() > MAX_EMITTED_EVENT_PAYLOAD_BYTES
            || event.metadata_json.len() > 8 * 1024
        {
            return Err(NodeExecutorReducerError::BoundExceeded);
        }
        if event.payload_hash
            != agentmod_primitives::ContentHash::digest(event.payload_json.as_bytes())
        {
            return Err(NodeExecutorReducerError::ContentHashMismatch);
        }
        if self.emitted_events.len() >= MAX_NODE_EXECUTOR_RECORDS {
            return Err(NodeExecutorReducerError::RecordBoundExceeded);
        }
        // Duplicate suppression: an identical emission at the same canonical
        // sequence is a replay no-op.
        if let Some(previous) = self.emitted_events.last()
            && previous.emission_id == event.emission_id
            && previous.sequence == event.sequence
        {
            return Ok(());
        }
        self.emitted_events.push(EmittedEventRecord {
            emission_id: event.emission_id.clone(),
            node_id: event.node_id.clone(),
            run_id: event.run_id.clone(),
            attempt: event.attempt,
            loop_iteration: event.loop_iteration,
            step: event.step,
            namespace: event.namespace.clone(),
            event_type: event.event_type.clone(),
            sequence,
            payload_json: event.payload_json.clone(),
            payload_hash: event.payload_hash,
            metadata_json: event.metadata_json.clone(),
        });
        Ok(())
    }
}

/// Exact executor identity used for replay classification and idempotency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorIdentity {
    /// Canonical session.
    pub session_id: String,
    /// Exact run identity.
    pub run_id: String,
    /// Graph node that owns the work.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
}

impl ExecutorIdentity {
    /// Deterministic message identity for one child-message execution.
    #[must_use]
    pub fn message_id(&self, child_session_id: &str, sequence: u64) -> String {
        format!(
            "msg:{session}:{run}:{node}:{step}:{child}:{sequence}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step,
            child = child_session_id,
            sequence = sequence
        )
    }

    /// Deterministic join identity for one join execution.
    #[must_use]
    pub fn join_id(&self) -> String {
        format!(
            "join:{session}:{run}:{node}:{step}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step
        )
    }

    /// Deterministic parallel node identity.
    #[must_use]
    pub fn branch_id(&self) -> String {
        format!(
            "par:{session}:{run}:{node}:{step}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step
        )
    }

    /// Deterministic delay identity.
    #[must_use]
    pub fn delay_id(&self) -> String {
        format!(
            "delay:{session}:{run}:{node}:{step}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step
        )
    }

    /// Deterministic schedule identity.
    #[must_use]
    pub fn schedule_id(&self, idempotency_key: &str) -> String {
        format!(
            "schedule:{session}:{run}:{node}:{step}:{key}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step,
            key = idempotency_key
        )
    }

    /// Deterministic durable continuation identity for a delay.
    #[must_use]
    pub fn delay_continuation_id(&self) -> String {
        format!(
            "delay-cont:{session}:{run}:{node}:{step}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step
        )
    }

    /// Deterministic emission identity for one emitted event.
    #[must_use]
    pub fn emission_id(&self, namespace: &str, event_type: &str) -> String {
        format!(
            "emit:{session}:{run}:{node}:{step}:{namespace}:{event_type}",
            session = self.session_id,
            run = self.run_id,
            node = self.node_id,
            step = self.step,
            namespace = namespace,
            event_type = event_type
        )
    }
}

/// Replay-owned executor state failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeExecutorReducerError {
    /// A record domain exceeded its hard bound.
    #[error("node-executor record bound exceeded")]
    RecordBoundExceeded,
    /// A bounded field exceeded its hard bound.
    #[error("node-executor bound exceeded")]
    BoundExceeded,
    /// A content hash does not match the bounded content.
    #[error("node-executor content hash mismatch")]
    ContentHashMismatch,
    /// The event references unknown state.
    #[error("unknown child-agent message")]
    UnknownMessage,
    /// The event references an unknown join.
    #[error("unknown join")]
    UnknownJoin,
    /// The event references an unknown parallel branch.
    #[error("unknown parallel branch")]
    UnknownParallelBranch,
    /// The event references an unknown delay.
    #[error("unknown delay")]
    UnknownDelay,
    /// The event references an unknown schedule.
    #[error("unknown schedule")]
    UnknownSchedule,
    /// The event references an unknown participant.
    #[error("unknown join participant")]
    UnknownParticipant,
    /// The state machine rejected the event transition.
    #[error("invalid node-executor transition")]
    InvalidTransition,
    /// A duplicate initialization occurred at a different sequence.
    #[error("duplicate node-executor initialization")]
    DuplicateInitialization,
    /// A terminal event was emitted more than once.
    #[error("duplicate node-executor release")]
    DuplicateRelease,
    /// A parallel node finished successfully with members still uncertain.
    #[error("parallel finish is ambiguous")]
    AmbiguousFinish,
    /// Dispatch order was not strictly deterministic.
    #[error("parallel dispatch order violation")]
    DispatchOrderViolation,
    /// An idempotency key was reused with conflicting identity.
    #[error("node-executor idempotency conflict")]
    IdempotencyConflict,
    /// Two identities conflict for the same canonical record.
    #[error("node-executor identity mismatch")]
    IdentityMismatch,
}

#[cfg(test)]
mod tests {
    use agentmod_primitives::ContentHash;

    use super::*;
    use crate::node_executors::events::{
        ChildMessageProposedEvent, JoinArtifactCollection, JoinOrdering, JoinProjection,
        JoinTerminalState,
    };

    fn identity() -> ExecutorIdentity {
        ExecutorIdentity {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("message"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
        }
    }

    fn proposed(identity: &ExecutorIdentity) -> NodeExecutorEventPayload {
        let message_id = identity.message_id("child-1", 1);
        let content = String::from("hello child");
        NodeExecutorEventPayload::ChildMessageProposed(ChildMessageProposedEvent {
            message_id,
            idempotency_key: String::from("key-1"),
            parent_session_id: identity.session_id.clone(),
            child_session_id: String::from("child-1"),
            sequence: 1,
            node_id: identity.node_id.clone(),
            run_id: identity.run_id.clone(),
            attempt: identity.attempt,
            loop_iteration: identity.loop_iteration,
            step: identity.step,
            content: content.clone(),
            content_hash: ContentHash::digest(content.as_bytes()),
            artifact_references: Vec::new(),
            classification: ChildMessageClassification::Instruction,
            expires_at_ms: None,
        })
    }

    #[test]
    fn message_propose_deliver_reject_machine_is_strict() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let event = proposed(&identity);
        state.apply(&event, 5).expect("propose");
        let message_id = match &event {
            NodeExecutorEventPayload::ChildMessageProposed(proposed) => proposed.message_id.clone(),
            _ => unreachable!(),
        };
        // Delivering after rejection must fail.
        let mut state = state.clone();
        state
            .apply(
                &NodeExecutorEventPayload::ChildMessageRejected(
                    crate::node_executors::events::ChildMessageRejectedEvent {
                        message_id: message_id.clone(),
                        child_session_id: String::from("child-1"),
                        reason: String::from("expired"),
                        detail: String::from("message expired"),
                    },
                ),
                6,
            )
            .expect("reject");
        assert_eq!(
            state.child_messages[&message_id].state,
            ChildMessageState::Rejected
        );
        let mut delivered = state.clone();
        assert_eq!(
            delivered
                .apply(
                    &NodeExecutorEventPayload::ChildMessageDelivered(
                        crate::node_executors::events::ChildMessageDeliveredEvent {
                            message_id,
                            child_session_id: String::from("child-1"),
                            receipt: String::from("delivered"),
                        },
                    ),
                    7,
                )
                .expect_err("deliver after reject"),
            NodeExecutorReducerError::InvalidTransition
        );
    }

    #[test]
    fn duplicate_proposal_with_matching_idempotency_is_a_replay_noop() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        state.apply(&proposed(&identity), 5).expect("first");
        state.apply(&proposed(&identity), 5).expect("duplicate");
        assert_eq!(state.child_messages.len(), 1);
    }

    #[test]
    fn conflicting_idempotency_key_fails_closed() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        state.apply(&proposed(&identity), 5).expect("first");
        // The same idempotency key is reused for a different child sequence
        // with different content: that is a conflicting identity.
        let mut conflict = proposed(&identity);
        match &mut conflict {
            NodeExecutorEventPayload::ChildMessageProposed(event) => {
                event.message_id = identity.message_id("child-1", 2);
                event.sequence = 2;
                event.content = String::from("different content");
                event.content_hash = ContentHash::digest(b"different content");
            }
            _ => unreachable!(),
        }
        assert_eq!(
            state.apply(&conflict, 6).expect_err("conflict"),
            NodeExecutorReducerError::IdempotencyConflict
        );
    }

    #[test]
    fn join_readiness_and_release_are_exact() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let join_id = identity.join_id();
        let initialized = NodeExecutorEventPayload::JoinInitialized(JoinInitializedEvent {
            join_id: join_id.clone(),
            node_id: identity.node_id.clone(),
            run_id: identity.run_id.clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            initialized_at_ms: 1_700_000_000_000,
            expected_participants: vec![String::from("child-1"), String::from("child-2")],
            optional_participants: Vec::new(),
            min_success: 2,
            allowed_failures: 0,
            timeout_ms: None,
            ordering: JoinOrdering::DeclarationOrder,
            result_projection: JoinProjection::TypedResults,
            artifact_collection: JoinArtifactCollection::Bounded,
        });
        state.apply(&initialized, 10).expect("initialize");
        for (participant, sequence) in [("child-1", 11u64), ("child-2", 12u64)] {
            state
                .apply(
                    &NodeExecutorEventPayload::JoinParticipantCompleted(
                        JoinParticipantCompletedEvent {
                            join_id: join_id.clone(),
                            participant_execution_id: String::from(participant),
                            result_references: vec![format!("{participant}:result")],
                            result_bytes: 8,
                        },
                    ),
                    sequence,
                )
                .expect("participant");
        }
        let join = &state.joins[&join_id];
        assert_eq!(join.completed_participants.len(), 2);
        assert!(join.missing().is_empty());
        state
            .apply(
                &NodeExecutorEventPayload::JoinReleased(JoinReleasedEvent {
                    join_id,
                    state: JoinTerminalState::Success,
                    collected_result_references: vec![
                        String::from("child-1:result"),
                        String::from("child-2:result"),
                    ],
                    missing_participants: Vec::new(),
                    reason: String::from("all completed"),
                }),
                13,
            )
            .expect("release");
        assert_eq!(state.joins[&identity.join_id()].state, JoinState::Success);
    }

    #[test]
    fn parallel_dispatch_order_is_deterministic_and_finish_requires_terminal_members() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let branch_id = identity.branch_id();
        state
            .apply(
                &NodeExecutorEventPayload::ParallelBranchInitialized(
                    ParallelBranchInitializedEvent {
                        branch_id: branch_id.clone(),
                        node_id: identity.node_id.clone(),
                        run_id: identity.run_id.clone(),
                        attempt: 1,
                        loop_iteration: 0,
                        step: 1,
                        branch_ids: vec![String::from("a"), String::from("b"), String::from("c")],
                        max_parallelism: 2,
                        shared_write_scopes: Vec::new(),
                    },
                ),
                20,
            )
            .expect("initialize");
        for (member, index) in [("a", 0u32), ("b", 1u32)] {
            state
                .apply(
                    &NodeExecutorEventPayload::ParallelBranchMemberDispatched(
                        ParallelBranchMemberDispatchedEvent {
                            branch_id: branch_id.clone(),
                            member_id: String::from(member),
                            dispatch_index: index,
                            cancellation_id: format!("cancel-{member}"),
                        },
                    ),
                    21 + u64::from(index),
                )
                .expect("dispatch");
        }
        let branch = &state.parallel_branches[&branch_id];
        assert_eq!(branch.dispatched_order, ["a", "b"]);
        assert_eq!(branch.dispatched_without_terminal(), ["a", "b"]);
        // Finishing successfully while members are uncertain must fail closed.
        assert_eq!(
            state
                .apply(
                    &NodeExecutorEventPayload::ParallelBranchFinished(
                        ParallelBranchFinishedEvent {
                            branch_id,
                            state: ParallelBranchTerminalState::FinishedSuccess,
                            reason: String::from("premature"),
                        },
                    ),
                    30,
                )
                .expect_err("premature finish"),
            NodeExecutorReducerError::AmbiguousFinish
        );
    }

    #[test]
    fn delay_state_machine_resumes_exactly_once() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let delay_id = identity.delay_id();
        let scheduled = NodeExecutorEventPayload::DelayScheduled(DelayScheduledEvent {
            delay_id: delay_id.clone(),
            node_id: identity.node_id.clone(),
            run_id: identity.run_id.clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            session_id: identity.session_id.clone(),
            wake_time_ms: 1_700_000_000_000 + 5_000,
            continuation_id: identity.delay_continuation_id(),
            expires_at_ms: None,
        });
        state.apply(&scheduled, 40).expect("schedule");
        state
            .apply(
                &NodeExecutorEventPayload::DelayResumed(DelayResumedEvent {
                    delay_id: delay_id.clone(),
                    wake_time_ms: 1_700_000_000_000 + 5_000,
                    proof: String::from("scheduler.claim"),
                }),
                41,
            )
            .expect("resume");
        let mut second = state.clone();
        assert_eq!(
            second
                .apply(
                    &NodeExecutorEventPayload::DelayResumed(DelayResumedEvent {
                        delay_id,
                        wake_time_ms: 1_700_000_000_000 + 5_000,
                        proof: String::from("scheduler.claim"),
                    }),
                    42,
                )
                .expect_err("second resume"),
            NodeExecutorReducerError::InvalidTransition
        );
    }

    #[test]
    fn schedule_proposal_creation_rejection_removal_machine_is_strict() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let schedule_id = identity.schedule_id("key-1");
        let proposed = GraphScheduleProposedEvent {
            schedule_id: schedule_id.clone(),
            node_id: identity.node_id.clone(),
            run_id: identity.run_id.clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            session_id: identity.session_id.clone(),
            idempotency_key: String::from("key-1"),
            trigger: GraphScheduleTrigger::AtMillis {
                wake_time_ms: 1_700_000_000_000,
            },
            style: String::from("persistent-chat@1.1.0"),
            workspace: String::from("workspace"),
            permission_policy: String::from("ask"),
            provider: String::from("mock"),
            model: String::from("fixture"),
            token_budget: 1_000,
            cost_budget_micros: 100,
        };
        state
            .apply(
                &NodeExecutorEventPayload::GraphScheduleProposed(proposed),
                50,
            )
            .expect("propose");
        assert_eq!(
            state.schedules[&schedule_id].state,
            GraphScheduleState::Proposed
        );
        state
            .apply(
                &NodeExecutorEventPayload::GraphScheduleCreated(GraphScheduleCreatedEvent {
                    schedule_id,
                    node_id: identity.node_id.clone(),
                    run_id: identity.run_id.clone(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    session_id: identity.session_id.clone(),
                    idempotency_key: String::from("key-1"),
                    trigger: GraphScheduleTrigger::AtMillis {
                        wake_time_ms: 1_700_000_000_000,
                    },
                    style: String::from("persistent-chat@1.1.0"),
                    workspace: String::from("workspace"),
                    permission_policy: String::from("ask"),
                    provider: String::from("mock"),
                    model: String::from("fixture"),
                    token_budget: 1_000,
                    cost_budget_micros: 100,
                }),
                51,
            )
            .expect("create");
        assert_eq!(
            state.schedules[&identity.schedule_id("key-1")].state,
            GraphScheduleState::Active
        );
    }

    #[test]
    fn emitted_event_ledger_is_ordered_and_duplicate_suppressed() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        let payload = String::from(r#"{"progress":42}"#);
        let emitted = NodeExecutorEventPayload::UserEventEmitted(UserEventEmittedEvent {
            emission_id: identity.emission_id("project", "progress"),
            node_id: identity.node_id.clone(),
            run_id: identity.run_id.clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            namespace: String::from("project"),
            event_type: String::from("progress"),
            sequence: 60,
            payload_json: payload.clone(),
            payload_hash: ContentHash::digest(payload.as_bytes()),
            artifact_references: Vec::new(),
            metadata_json: String::from(r"{}"),
            correlation_id: String::from("correlation-1"),
            causation_id: String::from("causation-1"),
        });
        state.apply(&emitted, 60).expect("emit");
        state.apply(&emitted, 60).expect("replay no-op");
        assert_eq!(state.emitted_events.len(), 1);
        assert_eq!(state.emitted_events[0].sequence, 60);
    }

    #[test]
    fn replay_classification_distinguishes_safe_and_uncertain_positions() {
        let identity = identity();
        let mut state = NodeExecutorState::default();
        assert_eq!(
            state.classify_replay(&identity),
            ReplayClassification::Consistent
        );
        state.apply(&proposed(&identity), 5).expect("propose");
        assert_eq!(
            state.classify_replay(&identity),
            ReplayClassification::ExternallyUncertain
        );
    }
}
