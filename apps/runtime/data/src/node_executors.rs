//! Bounded canonical record shapes for runtime-owned native node executors.
//!
//! Runtime logic owns the executable behavior of native control-flow nodes
//! (child-agent messaging, generic joins, parallel branches, durable delays,
//! graph-owned schedules, and constrained event emission). This data set owns
//! the layer-local record shapes those executors produce, plus the narrow
//! state snapshot seam used to reconstruct durable executor state after a
//! restart. It contains capability-independent record data only; no executor
//! logic lives here and no dependency implementation is required to compile.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use agentmod_event_model::ArtifactReference;
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Hard per-domain record bounds shared by every native node-executor state.
pub const MAX_NODE_EXECUTOR_RECORDS: usize = 1_024;
/// Hard bound on one canonical child message payload in bytes.
pub const MAX_CHILD_MESSAGE_BYTES: usize = 256 * 1024;
/// Hard bound on one emitted user event payload in bytes.
pub const MAX_EMITTED_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Hard bound on one join result reference set in bytes.
pub const MAX_JOIN_RESULT_REFERENCE_BYTES: usize = 256 * 1024;
/// Hard bound on one node executor state snapshot in records.
pub const MAX_NODE_EXECUTOR_SNAPSHOT_RECORDS: usize = 8 * MAX_NODE_EXECUTOR_RECORDS;
/// Hard bound on artifact references attached to one message or event.
pub const MAX_ARTIFACT_REFERENCES: usize = 64;
/// Hard bound on identifiers used by executor records.
pub const MAX_EXECUTOR_IDENTIFIER_BYTES: usize = 256;
/// Hard bound on free-form metadata attached to an emitted user event.
pub const MAX_EVENT_METADATA_BYTES: usize = 8 * 1024;
/// Maximum number of parallel sub-branches one parallel node may declare.
pub const MAX_PARALLEL_BRANCHES: usize = 64;

/// Stable lifecycle of one child-agent message record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMessageStateData {
    /// Proposal is canonical; delivery has not been committed.
    Proposed,
    /// Delivery to the exact child session is canonical.
    Delivered,
    /// The message was rejected, expired, or cancelled without delivery.
    Rejected,
}

/// Security classification of one child-agent message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMessageClassificationData {
    /// Ordinary bounded instruction content.
    Instruction,
    /// Content references approved artifacts only.
    ArtifactReference,
    /// Content that must never enter a provider projection.
    Private,
}

/// Normalized child-agent message record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildMessageDataRecord {
    /// Stable per-session message identity.
    pub message_id: String,
    /// Caller-supplied idempotency key (duplicate suppression).
    pub idempotency_key: String,
    /// Exact parent session.
    pub parent_session_id: String,
    /// Exact child session.
    pub child_session_id: String,
    /// Monotonic per-child message sequence.
    pub sequence: u64,
    /// Current canonical lifecycle.
    pub state: ChildMessageStateData,
    /// Exact hash of the bounded message content.
    pub content_hash: ContentHash,
    /// Exact serialized content bytes (bounded).
    pub content_bytes: u64,
    /// Approved artifact references carried by the message.
    pub artifact_references: Vec<ArtifactReference>,
    /// Security classification.
    pub classification: ChildMessageClassificationData,
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

/// Ordering policy for a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinOrderingData {
    /// Results are ordered by participant declaration order.
    DeclarationOrder,
    /// Results are ordered by completion order.
    CompletionOrder,
}

/// Result projection policy for a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinProjectionData {
    /// The join produces bounded summary text only.
    SummaryOnly,
    /// The join produces typed result references only.
    TypedResults,
}

/// Artifact collection policy for a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinArtifactCollectionData {
    /// No artifacts are collected.
    None,
    /// Artifacts are collected up to the shared result bound.
    Bounded,
}

/// Terminal classification of a generic join.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinStateData {
    /// The join is still awaiting participants.
    Awaiting,
    /// Minimum success and failure allowances are satisfied.
    Ready,
    /// The join released with a successful outcome.
    Success,
    /// The join failed terminally.
    Failed,
    /// The join timed out before its required participants finished.
    TimedOut,
    /// The join was cancelled.
    Cancelled,
}

/// Normalized generic join record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinDataRecord {
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
    /// Minimum number of successful participants required for success.
    pub min_success: u32,
    /// Maximum number of failures/cancellations allowed before failure.
    pub allowed_failures: u32,
    /// Optional wall-clock timeout millis.
    pub timeout_ms: Option<u64>,
    /// Result ordering policy.
    pub ordering: JoinOrderingData,
    /// Result projection policy.
    pub result_projection: JoinProjectionData,
    /// Artifact collection policy.
    pub artifact_collection: JoinArtifactCollectionData,
    /// Current join lifecycle.
    pub state: JoinStateData,
    /// Exact collected result references in policy order.
    pub collected_result_references: Vec<String>,
    /// Participants still missing at a terminal state.
    pub missing_participants: BTreeSet<String>,
    /// Canonical sequence that released the join, when released.
    pub released_at_sequence: Option<u64>,
    /// Stable terminal reason for failure/timeout/cancellation.
    pub terminal_reason: Option<String>,
}

/// Lifecycle of one parallel sub-branch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelBranchMemberStateData {
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
pub enum ParallelBranchStateData {
    /// The node is still dispatching or awaiting members.
    Running,
    /// Every member reached a terminal outcome and the join policy released.
    FinishedSuccess,
    /// The node failed terminally.
    FinishedFailure,
    /// The node was cancelled.
    Cancelled,
}

/// Normalized parallel branch node record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelBranchDataRecord {
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
    pub member_states: BTreeMap<String, ParallelBranchMemberStateData>,
    /// Independent cancellation ID per dispatched member.
    pub cancellation_ids: BTreeMap<String, String>,
    /// Canonical variables shared by members that require merge policy.
    pub shared_write_scopes: Vec<String>,
    /// Overall node lifecycle.
    pub state: ParallelBranchStateData,
    /// Terminal reason for failure/cancellation.
    pub terminal_reason: Option<String>,
}

/// Lifecycle of a durable delay node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DelayStateData {
    /// The exact wake time is canonical; the wake has not fired.
    Pending,
    /// The wake fired exactly once and the node resumed.
    Resumed,
    /// The delay was cancelled before its wake.
    Cancelled,
    /// The delay expired before its wake.
    Expired,
}

/// Normalized durable delay record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelayDataRecord {
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
    /// Durable continuation bound to session/run/node/transition.
    pub continuation_id: String,
    /// Current delay lifecycle.
    pub state: DelayStateData,
    /// Canonical sequence of the resume event, when resumed.
    pub resumed_at_sequence: Option<u64>,
    /// Stable wake proof classification.
    pub resume_proof: Option<String>,
    /// Optional expiration wall-clock millis.
    pub expires_at_ms: Option<i64>,
    /// Stable terminal reason for cancellation/expiry.
    pub terminal_reason: Option<String>,
}

/// Trigger of a graph-owned schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum GraphScheduleTriggerData {
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

/// Lifecycle of a graph-owned schedule.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphScheduleStateData {
    /// The schedule is active in the scheduler worker.
    Active,
    /// The schedule was removed.
    Removed,
}

/// Normalized graph-owned schedule record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphScheduleDataRecord {
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
    pub trigger: GraphScheduleTriggerData,
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
    /// Current schedule lifecycle.
    pub state: GraphScheduleStateData,
    /// Stable removal reason.
    pub removed_reason: Option<String>,
}

/// Normalized emitted user-space event record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmittedEventDataRecord {
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
    /// Exact hash of the bounded typed payload.
    pub payload_hash: ContentHash,
    /// Exact serialized payload bytes (bounded).
    pub payload_bytes: u64,
    /// Approved artifact references.
    pub artifact_references: Vec<ArtifactReference>,
    /// Bounded non-secret metadata.
    pub metadata_json: String,
    /// Correlation identity constructed by the runtime.
    pub correlation_id: String,
    /// Causation identity constructed by the runtime.
    pub causation_id: String,
}

/// Complete bounded snapshot of native node-executor state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExecutorStateDataSnapshot {
    /// Child-agent message ledger.
    pub child_messages: Vec<ChildMessageDataRecord>,
    /// Generic join state.
    pub joins: Vec<JoinDataRecord>,
    /// Parallel branch state.
    pub parallel_branches: Vec<ParallelBranchDataRecord>,
    /// Durable delay state.
    pub delays: Vec<DelayDataRecord>,
    /// Graph-owned schedule registry.
    pub schedules: Vec<GraphScheduleDataRecord>,
    /// Emitted user-space event ledger.
    pub emitted_events: Vec<EmittedEventDataRecord>,
}

/// Narrow state snapshot seam consumed by runtime logic.
///
/// The canonical source of executor state is the session journal; this port
/// lets a composition root bind a durable read model (for example a
/// journal-derived bounded projection) without exposing dependency types to
/// logic. Logic treats a missing snapshot as an empty state.
pub trait NodeExecutorStateDataPort: Send + Sync {
    /// Loads the current bounded snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorStateDataError`] when the bound projection is
    /// unavailable or internally invalid.
    fn load_node_executor_state(
        &self,
    ) -> Result<NodeExecutorStateDataSnapshot, NodeExecutorStateDataError>;

    /// Atomically replaces the current snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorStateDataError`] when the replacement exceeds a
    /// hard bound or the store rejects it.
    fn store_node_executor_state(
        &self,
        snapshot: NodeExecutorStateDataSnapshot,
    ) -> Result<(), NodeExecutorStateDataError>;
}

/// Immutable in-memory node-executor state store used by tests and mocks.
#[derive(Clone, Debug)]
pub struct InMemoryNodeExecutorStateData {
    inner: Arc<std::sync::Mutex<Option<NodeExecutorStateDataSnapshot>>>,
}

impl InMemoryNodeExecutorStateData {
    /// Constructs an empty in-memory store.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Constructs a store preloaded with a validated snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorStateDataError`] when the snapshot exceeds a
    /// hard bound.
    pub fn seeded(
        snapshot: NodeExecutorStateDataSnapshot,
    ) -> Result<Self, NodeExecutorStateDataError> {
        validate_snapshot(&snapshot)?;
        Ok(Self {
            inner: Arc::new(std::sync::Mutex::new(Some(snapshot))),
        })
    }
}

impl NodeExecutorStateDataPort for InMemoryNodeExecutorStateData {
    fn load_node_executor_state(
        &self,
    ) -> Result<NodeExecutorStateDataSnapshot, NodeExecutorStateDataError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| NodeExecutorStateDataError::Unavailable)?;
        Ok(guard.as_ref().cloned().unwrap_or_default())
    }

    fn store_node_executor_state(
        &self,
        snapshot: NodeExecutorStateDataSnapshot,
    ) -> Result<(), NodeExecutorStateDataError> {
        validate_snapshot(&snapshot)?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| NodeExecutorStateDataError::Unavailable)?;
        *guard = Some(snapshot);
        Ok(())
    }
}

/// Validates one normalized record collection against the shared hard bounds.
///
/// # Errors
///
/// Returns [`NodeExecutorStateDataError::RecordBoundExceeded`] when a record
/// collection or the complete snapshot exceeds a hard bound, or
/// [`NodeExecutorStateDataError::InvalidRecord`] when a record contains
/// invalid or unbounded data.
pub fn validate_snapshot(
    snapshot: &NodeExecutorStateDataSnapshot,
) -> Result<(), NodeExecutorStateDataError> {
    let total = snapshot.child_messages.len()
        + snapshot.joins.len()
        + snapshot.parallel_branches.len()
        + snapshot.delays.len()
        + snapshot.schedules.len()
        + snapshot.emitted_events.len();
    if total > MAX_NODE_EXECUTOR_SNAPSHOT_RECORDS {
        return Err(NodeExecutorStateDataError::SnapshotRecordBoundExceeded);
    }
    if snapshot.child_messages.len() > MAX_NODE_EXECUTOR_RECORDS
        || snapshot.joins.len() > MAX_NODE_EXECUTOR_RECORDS
        || snapshot.parallel_branches.len() > MAX_NODE_EXECUTOR_RECORDS
        || snapshot.delays.len() > MAX_NODE_EXECUTOR_RECORDS
        || snapshot.schedules.len() > MAX_NODE_EXECUTOR_RECORDS
        || snapshot.emitted_events.len() > MAX_NODE_EXECUTOR_RECORDS
    {
        return Err(NodeExecutorStateDataError::RecordBoundExceeded);
    }
    if snapshot.child_messages.iter().any(|record| {
        record.content_bytes > MAX_CHILD_MESSAGE_BYTES as u64
            || record.artifact_references.len() > MAX_ARTIFACT_REFERENCES
            || !valid_identifier(&record.message_id)
            || !valid_identifier(&record.idempotency_key)
    }) || snapshot.joins.iter().any(|record| {
        !valid_identifier(&record.join_id)
            || record
                .collected_result_references
                .iter()
                .any(|reference| reference.len() > MAX_JOIN_RESULT_REFERENCE_BYTES)
    }) || snapshot.parallel_branches.iter().any(|record| {
        !valid_identifier(&record.branch_id)
            || record.branch_ids.len() > MAX_PARALLEL_BRANCHES
            || record.dispatched_order.len() > record.branch_ids.len()
            || record.member_states.len() > record.branch_ids.len()
            || record.cancellation_ids.len() > record.branch_ids.len()
    }) || snapshot.delays.iter().any(|record| {
        !valid_identifier(&record.delay_id) || !valid_identifier(&record.continuation_id)
    }) || snapshot.schedules.iter().any(|record| {
        !valid_identifier(&record.schedule_id) || !valid_identifier(&record.idempotency_key)
    }) || snapshot.emitted_events.iter().any(|record| {
        !valid_identifier(&record.emission_id)
            || record.payload_bytes > MAX_EMITTED_EVENT_PAYLOAD_BYTES as u64
            || record.metadata_json.len() > MAX_EVENT_METADATA_BYTES
            || record.artifact_references.len() > MAX_ARTIFACT_REFERENCES
    }) {
        return Err(NodeExecutorStateDataError::InvalidRecord);
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_EXECUTOR_IDENTIFIER_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

/// Node-executor state data failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeExecutorStateDataError {
    /// A record collection exceeded its per-domain hard bound.
    #[error("node-executor record bound exceeded")]
    RecordBoundExceeded,
    /// The complete snapshot exceeded its hard bound.
    #[error("node-executor snapshot record bound exceeded")]
    SnapshotRecordBoundExceeded,
    /// A record contains invalid or unbounded data.
    #[error("node-executor record is invalid")]
    InvalidRecord,
    /// The state store is unavailable.
    #[error("node-executor state store is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> ChildMessageDataRecord {
        ChildMessageDataRecord {
            message_id: String::from("parent:1:msg:1"),
            idempotency_key: String::from("key-1"),
            parent_session_id: String::from("parent"),
            child_session_id: String::from("child"),
            sequence: 1,
            state: ChildMessageStateData::Proposed,
            content_hash: ContentHash::digest(b"hello"),
            content_bytes: 5,
            artifact_references: Vec::new(),
            classification: ChildMessageClassificationData::Instruction,
            expires_at_ms: None,
            node_id: String::from("message"),
            run_id: String::from("run:1"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
            delivered_at_sequence: None,
            rejection_reason: None,
        }
    }

    #[test]
    fn in_memory_store_round_trips_a_seeded_snapshot() {
        let store = InMemoryNodeExecutorStateData::seeded(NodeExecutorStateDataSnapshot {
            child_messages: vec![message()],
            ..NodeExecutorStateDataSnapshot::default()
        })
        .expect("seeded store");
        let loaded = store
            .load_node_executor_state()
            .expect("load")
            .child_messages;
        assert_eq!(loaded, [message()]);
    }

    #[test]
    fn snapshot_enforces_domain_and_payload_bounds() {
        let oversized = NodeExecutorStateDataSnapshot {
            child_messages: vec![ChildMessageDataRecord {
                content_bytes: MAX_CHILD_MESSAGE_BYTES as u64 + 1,
                ..message()
            }],
            ..NodeExecutorStateDataSnapshot::default()
        };
        assert_eq!(
            validate_snapshot(&oversized).expect_err("oversized"),
            NodeExecutorStateDataError::InvalidRecord
        );

        let too_many = NodeExecutorStateDataSnapshot {
            joins: vec![
                JoinDataRecord {
                    join_id: String::from("join"),
                    node_id: String::from("join-node"),
                    run_id: String::from("run:1"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    expected_participants: Vec::new(),
                    optional_participants: Vec::new(),
                    completed_participants: BTreeSet::new(),
                    failed_participants: BTreeSet::new(),
                    cancelled_participants: BTreeSet::new(),
                    min_success: 0,
                    allowed_failures: 0,
                    timeout_ms: None,
                    ordering: JoinOrderingData::DeclarationOrder,
                    result_projection: JoinProjectionData::SummaryOnly,
                    artifact_collection: JoinArtifactCollectionData::None,
                    state: JoinStateData::Awaiting,
                    collected_result_references: Vec::new(),
                    missing_participants: BTreeSet::new(),
                    released_at_sequence: None,
                    terminal_reason: None,
                };
                MAX_NODE_EXECUTOR_RECORDS + 1
            ],
            ..NodeExecutorStateDataSnapshot::default()
        };
        assert_eq!(
            validate_snapshot(&too_many).expect_err("too many"),
            NodeExecutorStateDataError::RecordBoundExceeded
        );
    }
}
