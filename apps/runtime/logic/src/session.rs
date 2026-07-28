//! Pure runtime session reducers and typed committed payloads.

use std::collections::BTreeMap;

use agentmod_event_model::{EventClassification, EventEnvelope, EventModelError, EventScope};
use agentmod_primitives::{ContentHash, ContinuationId, Sequence, SessionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conversation::{
    ConversationEntry, ConversationError, ConversationState, ProjectionProvenance,
};

/// Durable session lifecycle reconstructed by replay.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLifecycle {
    /// Ready to process work.
    Active,
    /// Persisted without active execution.
    Suspended,
    /// Successfully finished.
    Completed,
    /// Terminated by a business failure.
    Failed,
    /// Explicitly cancelled.
    Cancelled,
    /// Retained but excluded from ordinary active listings.
    Archived,
}

/// Durable approval state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    /// Awaiting a user/supervisor decision.
    Pending,
    /// Approved exactly once.
    Approved,
    /// Denied exactly once.
    Denied,
}

/// Approval reconstructed from canonical events.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalRecord {
    /// Opaque continuation.
    pub continuation_id: ContinuationId,
    /// Redacted action summary.
    pub action_summary: String,
    /// Current decision.
    pub state: ApprovalState,
    /// Sequence at which approval became pending.
    pub requested_at: Sequence,
    /// Sequence at which it became terminal.
    pub resolved_at: Option<Sequence>,
}

/// Session creation payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionCreatedEvent {
    /// Normalized workspace selected by runtime logic.
    pub workspace: String,
    /// Explicit top-level session style.
    pub style: String,
}

/// Immutable ancestry recorded when a session is branched.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionBranchedEvent {
    /// Source session whose state was replayed.
    pub parent_session_id: SessionId,
    /// Inclusive source sequence used to construct the child.
    pub fork_sequence: Sequence,
}

/// Structured content append payload.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationEntryCommittedEvent {
    /// Canonical typed entry.
    pub entry: ConversationEntry,
}

/// Context projection replacement payload.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextProjectionReplacedEvent {
    /// Approved replacement provider state.
    pub replacement: Vec<ConversationEntry>,
    /// Source/method/artifact provenance.
    pub provenance: ProjectionProvenance,
}

/// Auditable model request proposal before authorization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestProposedEvent {
    /// Logic proposal identifier.
    pub proposal_id: String,
    /// Requested provider.
    pub provider: String,
    /// Requested model.
    pub model: String,
    /// Exact structured projection hash.
    pub projection_hash: ContentHash,
}

/// Final model request action authorized before dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestApprovedEvent {
    /// Original logic proposal identifier.
    pub proposal_id: String,
    /// Final provider after interception.
    pub provider: String,
    /// Final model after interception.
    pub model: String,
    /// Digest bound into the short-lived harness grant.
    pub action_digest: ContentHash,
}

/// Provider execution began.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestStartedEvent {
    /// Runtime cancellation reference.
    pub cancellation_id: String,
}

/// Visible provider delta observed after execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelOutputDeltaObservedEvent {
    /// Runtime cancellation reference.
    pub cancellation_id: String,
    /// Visible text only.
    pub text: String,
}

/// Partial provider tool-call output observed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelToolCallDeltaObservedEvent {
    /// Provider-independent call ID.
    pub call_id: String,
    /// Tool-name fragment.
    pub name: String,
    /// Argument JSON fragment.
    pub arguments: String,
}

/// Provider tool call proposed; no tool execution is implied.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelToolCallProposedEvent {
    /// Harness continuation.
    pub continuation_id: String,
    /// Provider-independent call ID.
    pub call_id: String,
    /// Tool ID.
    pub tool: String,
    /// Structured arguments.
    pub arguments: serde_json::Value,
}

/// Normal provider response completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelResponseCompletedEvent {
    /// Runtime cancellation reference.
    pub cancellation_id: String,
    /// Provider-neutral finish reason.
    pub finish_reason: String,
    /// Provider-reported input tokens.
    pub input_tokens: u64,
    /// Provider-reported output tokens.
    pub output_tokens: u64,
}

/// Provider execution cancellation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestCancelledEvent {
    /// Runtime cancellation reference.
    pub cancellation_id: String,
}

/// Classified provider lifecycle failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestFailedEvent {
    /// Stable failure code.
    pub code: String,
    /// Redacted message.
    pub message: String,
    /// Whether runtime policy may retry.
    pub retryable: bool,
}

/// Runtime-normalized tool action before authorization.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCallProposedEvent {
    /// Stable proposal identifier.
    pub proposal_id: String,
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Namespaced tool ID.
    pub tool: String,
    /// Original structured arguments.
    pub arguments: serde_json::Value,
}

/// Final tool action authorized before host dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolCallApprovedEvent {
    /// Stable proposal identifier.
    pub proposal_id: String,
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Digest of the final intercepted action.
    pub action_digest: ContentHash,
}

/// Isolated tool-host execution began.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolExecutionStartedEvent {
    /// Provider tool-call identifier.
    pub call_id: String,
}

/// Durable dispatch outbox record committed before calling a tool host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolExecutionDispatchedEvent {
    /// Stable execution identity used for reconciliation.
    pub execution_id: String,
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Exact approved action digest.
    pub action_digest: ContentHash,
}

/// Bounded tool output observed before completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolOutputObservedEvent {
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Exact supervised process identity when this output came from a process operation.
    #[serde(default)]
    pub process_id: Option<String>,
    /// Durable process-log stream used to identify an exact output range.
    #[serde(default)]
    pub source_stream: Option<String>,
    /// Inclusive process-log range start when emitted by `process.read`.
    #[serde(default)]
    pub source_offset: Option<u64>,
    /// Exclusive process-log range end when emitted by `process.read`.
    #[serde(default)]
    pub source_end: Option<u64>,
    /// Stable output stream.
    pub stream: String,
    /// Bounded visible fragment.
    pub content: String,
}

/// Isolated tool execution completed.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolExecutionCompletedEvent {
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Bounded structured result.
    pub result: serde_json::Value,
    /// Optional full-output artifact reference.
    pub artifact: Option<String>,
    /// Whether the projection is incomplete.
    pub truncated: bool,
}

/// Isolated tool execution failed or was cancelled.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolExecutionFailedEvent {
    /// Provider tool-call identifier.
    pub call_id: String,
    /// Stable failure code.
    pub code: String,
    /// Redacted failure message.
    pub message: String,
    /// Whether policy may retry.
    pub retryable: bool,
}

/// Runtime-to-process-host reconciliation began.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessReconciliationStartedEvent {
    /// Provider tool-call identifier that owns this exact reconciliation.
    pub call_id: String,
    /// `AgentMod` process identity being reconciled.
    pub process_id: String,
}

/// Stable terminal process-reconciliation classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessReconciliationStatus {
    /// The surviving capability host retained the live process handles.
    Live,
    /// The exact child exists but inherited handles cannot be reconstructed.
    RecoveredRunningUnattached,
    /// The durable record was reconciled to an exited process.
    RecoveredExited,
    /// Dispatch may have occurred and therefore is never repeated.
    DispatchUncertain,
    /// Reconciliation failed before a safe process classification was returned.
    Failed,
}

/// Runtime-to-process-host reconciliation reached a canonical outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessReconciliationCompletedEvent {
    /// Provider tool-call identifier that owns this exact reconciliation.
    pub call_id: String,
    /// `AgentMod` process identity that was reconciled.
    pub process_id: String,
    /// Safe result reported through the process-host boundary.
    pub status: ProcessReconciliationStatus,
}

/// Approval request payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalRequestedEvent {
    /// Durable continuation.
    pub continuation_id: ContinuationId,
    /// Redacted action summary.
    pub action_summary: String,
}

/// Approval resolution payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalResolvedEvent {
    /// Durable continuation.
    pub continuation_id: ContinuationId,
    /// Decision.
    pub approved: bool,
}

/// Lifecycle transition payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionLifecycleChangedEvent {
    /// Requested terminal/nonterminal lifecycle.
    pub lifecycle: SessionLifecycle,
    /// Redacted reason.
    pub reason: Option<String>,
}

/// Durable provenance for one scheduler-owned execution claim.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchedulerFiredEvent {
    /// Deterministic occurrence identifier.
    pub execution_id: String,
    /// Owning schedule.
    pub schedule_id: String,
    /// Trigger occurrence timestamp.
    pub scheduled_for_ms: i64,
}

/// Typed committed events consumed by the pure session reducer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum RuntimeCommittedEvent {
    /// Establishes a new session.
    SessionCreated(SessionCreatedEvent),
    /// Establishes immutable parent/fork ancestry for a child session.
    SessionBranched(SessionBranchedEvent),
    /// Adds structured canonical content.
    ConversationEntryCommitted(ConversationEntryCommittedEvent),
    /// Replaces only provider-visible structured state.
    ContextProjectionReplaced(ContextProjectionReplacedEvent),
    /// Records the original model request proposal.
    ModelRequestProposed(ModelRequestProposedEvent),
    /// Records the final approved request before its side effect.
    ModelRequestApproved(ModelRequestApprovedEvent),
    /// Records provider dispatch.
    ModelRequestStarted(ModelRequestStartedEvent),
    /// Records visible provider output.
    ModelOutputDeltaObserved(ModelOutputDeltaObservedEvent),
    /// Records partial provider tool-call output.
    ModelToolCallDeltaObserved(ModelToolCallDeltaObservedEvent),
    /// Records a provider tool-call proposal.
    ModelToolCallProposed(ModelToolCallProposedEvent),
    /// Records normal provider completion.
    ModelResponseCompleted(ModelResponseCompletedEvent),
    /// Records provider cancellation.
    ModelRequestCancelled(ModelRequestCancelledEvent),
    /// Records a classified provider failure.
    ModelRequestFailed(ModelRequestFailedEvent),
    /// Records the original normalized tool proposal.
    ToolCallProposed(ToolCallProposedEvent),
    /// Records the final authorized tool action.
    ToolCallApproved(ToolCallApprovedEvent),
    /// Records durable dispatch intent before the external call.
    ToolExecutionDispatched(ToolExecutionDispatchedEvent),
    /// Records isolated host dispatch.
    ToolExecutionStarted(ToolExecutionStartedEvent),
    /// Records bounded tool output.
    ToolOutputObserved(ToolOutputObservedEvent),
    /// Records successful isolated tool completion.
    ToolExecutionCompleted(ToolExecutionCompletedEvent),
    /// Records failed or cancelled isolated tool execution.
    ToolExecutionFailed(ToolExecutionFailedEvent),
    /// Records reconciliation intent before contacting the process host.
    ProcessReconciliationStarted(ProcessReconciliationStartedEvent),
    /// Records the terminal reconciliation classification.
    ProcessReconciliationCompleted(ProcessReconciliationCompletedEvent),
    /// Creates a durable approval continuation.
    ApprovalRequested(ApprovalRequestedEvent),
    /// Resolves a durable approval.
    ApprovalResolved(ApprovalResolvedEvent),
    /// Changes session lifecycle.
    SessionLifecycleChanged(SessionLifecycleChangedEvent),
    /// Records a scheduler claim before its prompt enters the normal turn path.
    SchedulerFired(SchedulerFiredEvent),
}

impl RuntimeCommittedEvent {
    /// Returns the stable metadata event type required for this payload.
    #[must_use]
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::SessionCreated(_) => "session.created",
            Self::SessionBranched(_) => "session.branched",
            Self::ConversationEntryCommitted(_) => "conversation.entry_committed",
            Self::ContextProjectionReplaced(_) => "context.projection_replaced",
            Self::ModelRequestProposed(_) => "model.request_proposed",
            Self::ModelRequestApproved(_) => "model.request_approved",
            Self::ModelRequestStarted(_) => "model.request_started",
            Self::ModelOutputDeltaObserved(_) => "model.output_delta_observed",
            Self::ModelToolCallDeltaObserved(_) => "model.tool_call_delta_observed",
            Self::ModelToolCallProposed(_) => "model.tool_call_proposed",
            Self::ModelResponseCompleted(_) => "model.response_completed",
            Self::ModelRequestCancelled(_) => "model.request_cancelled",
            Self::ModelRequestFailed(_) => "model.request_failed",
            Self::ToolCallProposed(_) => "tool.call_proposed",
            Self::ToolCallApproved(_) => "tool.call_approved",
            Self::ToolExecutionDispatched(_) => "tool.execution_dispatched",
            Self::ToolExecutionStarted(_) => "tool.execution_started",
            Self::ToolOutputObserved(_) => "tool.output_observed",
            Self::ToolExecutionCompleted(_) => "tool.execution_completed",
            Self::ToolExecutionFailed(_) => "tool.execution_failed",
            Self::ProcessReconciliationStarted(_) => "process.reconciliation_started",
            Self::ProcessReconciliationCompleted(_) => "process.reconciliation_completed",
            Self::ApprovalRequested(_) => "approval.requested",
            Self::ApprovalResolved(_) => "approval.resolved",
            Self::SessionLifecycleChanged(_) => "session.lifecycle_changed",
            Self::SchedulerFired(_) => "scheduler.fired",
        }
    }
}

/// Canonical session projection reconstructed only from committed events.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionState {
    /// Session ID from the event scope.
    pub id: SessionId,
    /// Normalized workspace.
    pub workspace: String,
    /// Explicit top-level style.
    pub style: String,
    /// Parent session and fork point for a branch.
    #[serde(default)]
    pub ancestry: Option<SessionAncestry>,
    /// Durable lifecycle.
    pub lifecycle: SessionLifecycle,
    /// Canonical content and provider projection.
    pub conversation: ConversationState,
    /// Durable approval continuations.
    pub approvals: BTreeMap<ContinuationId, ApprovalRecord>,
    /// Durable tool-dispatch outbox projection keyed by provider call ID.
    #[serde(default)]
    pub tool_executions: BTreeMap<String, ToolExecutionRecord>,
    /// Restart/reconnect reconciliation state keyed by provider call ID.
    #[serde(default)]
    pub process_reconciliations: BTreeMap<String, ProcessReconciliationRecord>,
    /// Last applied sequence.
    pub last_sequence: Sequence,
    /// Integrity checksum of the last applied event.
    pub last_event_checksum: ContentHash,
}

/// Replay-derived branch ancestry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionAncestry {
    /// Immutable parent session.
    pub parent_session_id: SessionId,
    /// Inclusive parent sequence used for the branch.
    pub fork_sequence: Sequence,
}

/// Durable tool-dispatch reconciliation state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionState {
    /// Runtime committed dispatch intent before the external call.
    Dispatched,
    /// Tool host reported start.
    Started,
    /// Tool host returned a terminal result.
    Terminal,
}

/// Reducer-owned tool execution outbox record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolExecutionRecord {
    /// Stable execution identity.
    pub execution_id: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Exact approved action digest.
    pub action_digest: ContentHash,
    /// Latest durable execution state.
    pub state: ToolExecutionState,
    /// Dispatch sequence.
    pub dispatched_at: Sequence,
    /// Terminal sequence when known.
    pub terminal_at: Option<Sequence>,
    /// Number of host lifecycle items durably projected into the journal.
    #[serde(default)]
    pub observed_event_count: u64,
}

/// Replay-derived process reconciliation record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessReconciliationRecord {
    /// Provider tool-call identifier.
    pub call_id: String,
    /// `AgentMod` process identity.
    pub process_id: String,
    /// Sequence at which reconciliation intent became canonical.
    pub started_at: Sequence,
    /// Terminal classification, absent while reconciliation is incomplete.
    pub status: Option<ProcessReconciliationStatus>,
    /// Sequence at which the terminal classification became canonical.
    pub completed_at: Option<Sequence>,
}

/// Applies one verified committed event without dispatching external effects.
///
/// # Errors
///
/// Returns [`SessionReducerError`] when event integrity, classification, scope,
/// sequence, transition, or payload invariants fail.
pub fn reduce(
    current: Option<SessionState>,
    event: &EventEnvelope<RuntimeCommittedEvent>,
) -> Result<SessionState, SessionReducerError> {
    event
        .verify()
        .map_err(SessionReducerError::EventIntegrity)?;
    if event.metadata.classification != EventClassification::Committed {
        return Err(SessionReducerError::NotCommitted);
    }
    if event.metadata.event_type != event.payload.event_type() {
        return Err(SessionReducerError::EventTypeMismatch {
            metadata: event.metadata.event_type.clone(),
            payload: event.payload.event_type(),
        });
    }
    let EventScope::Session(session_id) = event.metadata.scope else {
        return Err(SessionReducerError::RuntimeScopedEvent);
    };

    match current {
        None => initialize(session_id, event),
        Some(mut state) => {
            if state.id != session_id {
                return Err(SessionReducerError::SessionMismatch);
            }
            let expected = state
                .last_sequence
                .checked_next()
                .map_err(|_| SessionReducerError::SequenceOverflow)?;
            if event.metadata.sequence != expected {
                return Err(SessionReducerError::SequenceMismatch {
                    expected: expected.get(),
                    actual: event.metadata.sequence.get(),
                });
            }
            apply_payload(&mut state, event)?;
            state.last_sequence = event.metadata.sequence;
            state.last_event_checksum = event.integrity_checksum;
            Ok(state)
        }
    }
}

fn initialize(
    session_id: SessionId,
    event: &EventEnvelope<RuntimeCommittedEvent>,
) -> Result<SessionState, SessionReducerError> {
    if event.metadata.sequence != Sequence::FIRST {
        return Err(SessionReducerError::SequenceMismatch {
            expected: Sequence::FIRST.get(),
            actual: event.metadata.sequence.get(),
        });
    }
    let RuntimeCommittedEvent::SessionCreated(created) = &event.payload else {
        return Err(SessionReducerError::FirstEventMustCreateSession);
    };
    if created.workspace.trim().is_empty() {
        return Err(SessionReducerError::EmptyWorkspace);
    }
    if created.style.trim().is_empty() {
        return Err(SessionReducerError::EmptyStyle);
    }
    Ok(SessionState {
        id: session_id,
        workspace: created.workspace.clone(),
        style: created.style.clone(),
        ancestry: None,
        lifecycle: SessionLifecycle::Active,
        conversation: ConversationState::new(),
        approvals: BTreeMap::new(),
        tool_executions: BTreeMap::new(),
        process_reconciliations: BTreeMap::new(),
        last_sequence: event.metadata.sequence,
        last_event_checksum: event.integrity_checksum,
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive pure reducer keeps every canonical event transition in one match"
)]
fn apply_payload(
    state: &mut SessionState,
    event: &EventEnvelope<RuntimeCommittedEvent>,
) -> Result<(), SessionReducerError> {
    if state.lifecycle == SessionLifecycle::Archived {
        return Err(SessionReducerError::ArchivedSessionIsImmutable);
    }
    match &event.payload {
        RuntimeCommittedEvent::SessionCreated(_) => {
            Err(SessionReducerError::DuplicateSessionCreation)
        }
        RuntimeCommittedEvent::SessionBranched(branched) => apply_branch_ancestry(state, branched),
        RuntimeCommittedEvent::ConversationEntryCommitted(committed) => state
            .conversation
            .append(committed.entry.clone())
            .map_err(SessionReducerError::Conversation),
        RuntimeCommittedEvent::ContextProjectionReplaced(replaced) => {
            if replaced.provenance.committed_at != event.metadata.sequence {
                return Err(SessionReducerError::ProjectionSequenceMismatch);
            }
            state
                .conversation
                .replace_projection(replaced.replacement.clone(), replaced.provenance.clone())
                .map_err(SessionReducerError::Conversation)
        }
        RuntimeCommittedEvent::ModelRequestProposed(_)
        | RuntimeCommittedEvent::ModelRequestApproved(_)
        | RuntimeCommittedEvent::ModelRequestStarted(_)
        | RuntimeCommittedEvent::ModelOutputDeltaObserved(_)
        | RuntimeCommittedEvent::ModelToolCallDeltaObserved(_)
        | RuntimeCommittedEvent::ModelToolCallProposed(_)
        | RuntimeCommittedEvent::ModelResponseCompleted(_)
        | RuntimeCommittedEvent::ModelRequestCancelled(_)
        | RuntimeCommittedEvent::ModelRequestFailed(_)
        | RuntimeCommittedEvent::ToolCallProposed(_)
        | RuntimeCommittedEvent::ToolCallApproved(_)
        | RuntimeCommittedEvent::SchedulerFired(_) => Ok(()),
        RuntimeCommittedEvent::ToolExecutionDispatched(dispatched) => {
            apply_tool_dispatch(state, dispatched, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ToolExecutionStarted(started) => {
            if let Some(record) = state.tool_executions.get_mut(&started.call_id) {
                if record.state != ToolExecutionState::Dispatched {
                    return Err(SessionReducerError::InvalidToolExecutionTransition);
                }
                record.state = ToolExecutionState::Started;
                record.observed_event_count = record
                    .observed_event_count
                    .checked_add(1)
                    .ok_or(SessionReducerError::ToolEventCountOverflow)?;
            }
            Ok(())
        }
        RuntimeCommittedEvent::ToolOutputObserved(observed) => {
            increment_tool_event_count(state, &observed.call_id)
        }
        RuntimeCommittedEvent::ToolExecutionCompleted(completed) => {
            mark_tool_terminal(state, &completed.call_id, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ToolExecutionFailed(failed) => {
            mark_tool_terminal(state, &failed.call_id, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ProcessReconciliationStarted(started) => {
            apply_process_reconciliation_started(state, started, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ProcessReconciliationCompleted(completed) => {
            apply_process_reconciliation_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ApprovalRequested(requested) => {
            if state.approvals.contains_key(&requested.continuation_id) {
                return Err(SessionReducerError::DuplicateContinuation);
            }
            state.approvals.insert(
                requested.continuation_id,
                ApprovalRecord {
                    continuation_id: requested.continuation_id,
                    action_summary: requested.action_summary.clone(),
                    state: ApprovalState::Pending,
                    requested_at: event.metadata.sequence,
                    resolved_at: None,
                },
            );
            Ok(())
        }
        RuntimeCommittedEvent::ApprovalResolved(resolved) => {
            let approval = state
                .approvals
                .get_mut(&resolved.continuation_id)
                .ok_or(SessionReducerError::UnknownContinuation)?;
            if approval.state != ApprovalState::Pending {
                return Err(SessionReducerError::ContinuationAlreadyResolved);
            }
            approval.state = if resolved.approved {
                ApprovalState::Approved
            } else {
                ApprovalState::Denied
            };
            approval.resolved_at = Some(event.metadata.sequence);
            Ok(())
        }
        RuntimeCommittedEvent::SessionLifecycleChanged(changed) => {
            if !valid_lifecycle_transition(state.lifecycle, changed.lifecycle) {
                return Err(SessionReducerError::InvalidLifecycleTransition {
                    from: state.lifecycle,
                    to: changed.lifecycle,
                });
            }
            state.lifecycle = changed.lifecycle;
            Ok(())
        }
    }
}

fn apply_branch_ancestry(
    state: &mut SessionState,
    branched: &SessionBranchedEvent,
) -> Result<(), SessionReducerError> {
    if state.ancestry.is_some() {
        return Err(SessionReducerError::DuplicateBranchAncestry);
    }
    state.ancestry = Some(SessionAncestry {
        parent_session_id: branched.parent_session_id,
        fork_sequence: branched.fork_sequence,
    });
    Ok(())
}

fn apply_tool_dispatch(
    state: &mut SessionState,
    dispatched: &ToolExecutionDispatchedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if state.tool_executions.contains_key(&dispatched.call_id) {
        return Err(SessionReducerError::DuplicateToolExecution);
    }
    state.tool_executions.insert(
        dispatched.call_id.clone(),
        ToolExecutionRecord {
            execution_id: dispatched.execution_id.clone(),
            call_id: dispatched.call_id.clone(),
            action_digest: dispatched.action_digest,
            state: ToolExecutionState::Dispatched,
            dispatched_at: sequence,
            terminal_at: None,
            observed_event_count: 0,
        },
    );
    Ok(())
}

fn mark_tool_terminal(
    state: &mut SessionState,
    call_id: &str,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if let Some(record) = state.tool_executions.get_mut(call_id) {
        if record.state == ToolExecutionState::Terminal {
            return Err(SessionReducerError::InvalidToolExecutionTransition);
        }
        record.state = ToolExecutionState::Terminal;
        record.terminal_at = Some(sequence);
        record.observed_event_count = record
            .observed_event_count
            .checked_add(1)
            .ok_or(SessionReducerError::ToolEventCountOverflow)?;
    }
    Ok(())
}

fn increment_tool_event_count(
    state: &mut SessionState,
    call_id: &str,
) -> Result<(), SessionReducerError> {
    if let Some(record) = state.tool_executions.get_mut(call_id) {
        if record.state == ToolExecutionState::Terminal {
            return Err(SessionReducerError::InvalidToolExecutionTransition);
        }
        record.observed_event_count = record
            .observed_event_count
            .checked_add(1)
            .ok_or(SessionReducerError::ToolEventCountOverflow)?;
    }
    Ok(())
}

fn apply_process_reconciliation_started(
    state: &mut SessionState,
    started: &ProcessReconciliationStartedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if started.call_id.trim().is_empty()
        || started.process_id.trim().is_empty()
        || state.process_reconciliations.contains_key(&started.call_id)
    {
        return Err(SessionReducerError::InvalidProcessReconciliationTransition);
    }
    state.process_reconciliations.insert(
        started.call_id.clone(),
        ProcessReconciliationRecord {
            call_id: started.call_id.clone(),
            process_id: started.process_id.clone(),
            started_at: sequence,
            status: None,
            completed_at: None,
        },
    );
    Ok(())
}

fn apply_process_reconciliation_completed(
    state: &mut SessionState,
    completed: &ProcessReconciliationCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = state
        .process_reconciliations
        .get_mut(&completed.call_id)
        .ok_or(SessionReducerError::InvalidProcessReconciliationTransition)?;
    if record.process_id != completed.process_id || record.completed_at.is_some() {
        return Err(SessionReducerError::InvalidProcessReconciliationTransition);
    }
    record.status = Some(completed.status);
    record.completed_at = Some(sequence);
    Ok(())
}

fn valid_lifecycle_transition(from: SessionLifecycle, to: SessionLifecycle) -> bool {
    matches!(
        (from, to),
        (
            SessionLifecycle::Active,
            SessionLifecycle::Suspended
                | SessionLifecycle::Completed
                | SessionLifecycle::Failed
                | SessionLifecycle::Cancelled
        ) | (
            SessionLifecycle::Suspended,
            SessionLifecycle::Active | SessionLifecycle::Cancelled
        ) | (
            SessionLifecycle::Completed
                | SessionLifecycle::Failed
                | SessionLifecycle::Cancelled
                | SessionLifecycle::Suspended,
            SessionLifecycle::Archived
        )
    )
}

/// Reconstructs a complete state from ordered committed events.
///
/// # Errors
///
/// Returns the first [`SessionReducerError`] encountered.
pub fn replay<'a>(
    events: impl IntoIterator<Item = &'a EventEnvelope<RuntimeCommittedEvent>>,
) -> Result<SessionState, SessionReducerError> {
    let mut state = None;
    for event in events {
        state = Some(reduce(state, event)?);
    }
    state.ok_or(SessionReducerError::EmptyReplay)
}

/// Reconstructs state through an inclusive sequence.
///
/// # Errors
///
/// Returns [`SessionReducerError`] if no event exists through the target or any
/// included event is invalid.
pub fn replay_to<'a>(
    events: impl IntoIterator<Item = &'a EventEnvelope<RuntimeCommittedEvent>>,
    target: Sequence,
) -> Result<SessionState, SessionReducerError> {
    replay(
        events
            .into_iter()
            .take_while(|event| event.metadata.sequence <= target),
    )
}

/// Pure reducer failure.
#[derive(Debug, Error)]
pub enum SessionReducerError {
    /// Generic event checksum/serialization failed.
    #[error("event integrity failed: {0}")]
    EventIntegrity(EventModelError),
    /// Reducers accept committed authority only.
    #[error("session reducer accepts committed events only")]
    NotCommitted,
    /// Envelope metadata does not identify its typed payload.
    #[error("event metadata type {metadata:?} does not match payload type {payload:?}")]
    EventTypeMismatch {
        /// Type carried in metadata.
        metadata: String,
        /// Type required by the typed payload.
        payload: &'static str,
    },
    /// Event did not address a session.
    #[error("runtime-scoped event cannot reduce a session")]
    RuntimeScopedEvent,
    /// Event addresses another session.
    #[error("event session does not match reconstructed session")]
    SessionMismatch,
    /// Event sequence is not the exact next value.
    #[error("event sequence mismatch: expected {expected}, received {actual}")]
    SequenceMismatch {
        /// Expected sequence.
        expected: u64,
        /// Actual sequence.
        actual: u64,
    },
    /// Sequence arithmetic overflowed.
    #[error("event sequence overflow")]
    SequenceOverflow,
    /// First event was not creation.
    #[error("first session event must be session creation")]
    FirstEventMustCreateSession,
    /// Session creation workspace was empty.
    #[error("session workspace is empty")]
    EmptyWorkspace,
    /// Session creation style was empty.
    #[error("session style is empty")]
    EmptyStyle,
    /// Creation appeared after initialization.
    #[error("session creation cannot be committed twice")]
    DuplicateSessionCreation,
    /// A branch may declare ancestry only once.
    #[error("session branch ancestry is already established")]
    DuplicateBranchAncestry,
    /// Conversation state invariant failed.
    #[error("conversation state failed: {0}")]
    Conversation(ConversationError),
    /// Replacement provenance did not reference its commit event.
    #[error("projection provenance commit sequence does not match event sequence")]
    ProjectionSequenceMismatch,
    /// Continuation ID already exists.
    #[error("approval continuation already exists")]
    DuplicateContinuation,
    /// Resolution addressed an unknown continuation.
    #[error("approval continuation does not exist")]
    UnknownContinuation,
    /// Resolution attempted more than once.
    #[error("approval continuation was already resolved")]
    ContinuationAlreadyResolved,
    /// Tool execution identity was dispatched more than once.
    #[error("tool execution was dispatched more than once")]
    DuplicateToolExecution,
    /// Tool execution state transition is invalid.
    #[error("tool execution state transition is invalid")]
    InvalidToolExecutionTransition,
    /// Host lifecycle event accounting exceeded its bounded integer.
    #[error("tool execution event count overflowed")]
    ToolEventCountOverflow,
    /// Process reconciliation did not follow one start and one completion.
    #[error("process reconciliation state transition is invalid")]
    InvalidProcessReconciliationTransition,
    /// Lifecycle transition is illegal.
    #[error("invalid lifecycle transition from {from:?} to {to:?}")]
    InvalidLifecycleTransition {
        /// Current lifecycle.
        from: SessionLifecycle,
        /// Requested lifecycle.
        to: SessionLifecycle,
    },
    /// Archived sessions cannot accept further events.
    #[error("archived session projection is immutable")]
    ArchivedSessionIsImmutable,
    /// Replay input was empty.
    #[error("cannot replay an empty event stream")]
    EmptyReplay,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use agentmod_event_model::{EventMetadata, EventOrigin};
    use agentmod_primitives::{CausationId, CorrelationId, EventId, TimestampMillis, Version};
    use uuid::Uuid;

    use crate::conversation::{ContextSummaryEntry, ConversationEntryId, TextEntry};

    use super::*;

    fn uuid(value: &str) -> Uuid {
        Uuid::from_str(value).expect("fixture UUID")
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000001"))
    }

    fn continuation_id() -> ContinuationId {
        ContinuationId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000002"))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        let event_type = payload.event_type();
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(1_700_000_000_000),
                event_type: event_type.into(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000003",
                )),
                causation_id: CausationId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000004")),
                parent_graph_node_id: None,
                origin: EventOrigin {
                    subsystem: "runtime".into(),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            payload,
        )
        .expect("seal")
    }

    fn created() -> EventEnvelope<RuntimeCommittedEvent> {
        envelope(
            1,
            RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                workspace: "fixture".into(),
                style: "persistent-chat".into(),
            }),
        )
    }

    #[test]
    fn context_replacement_preserves_canonical_history() {
        let user = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId("user-1".into()),
            text: "original".into(),
            source_sequence: Sequence::new(2).expect("sequence"),
        });
        let summary = ConversationEntry::ContextSummary(ContextSummaryEntry {
            id: ConversationEntryId("summary".into()),
            text: "summary".into(),
            source_start: Sequence::FIRST,
            source_end: Sequence::new(2).expect("sequence"),
            method: "summary".into(),
            artifact_id: None,
        });
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: user.clone(),
                    },
                ),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                    replacement: vec![summary.clone()],
                    provenance: ProjectionProvenance {
                        projection_id: "projection-3".into(),
                        source_range: Some((Sequence::FIRST, Sequence::new(2).expect("sequence"))),
                        method: "summary".into(),
                        committed_at: Sequence::new(3).expect("sequence"),
                        artifact_id: None,
                    },
                }),
            ),
        ];
        let state = replay(&events).expect("replay");
        assert_eq!(state.conversation.history(), [user]);
        assert_eq!(state.conversation.provider_projection(), [summary]);
    }

    #[test]
    fn approval_resolves_once_under_replay_semantics() {
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::ApprovalRequested(ApprovalRequestedEvent {
                    continuation_id: continuation_id(),
                    action_summary: "write fixture".into(),
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                    continuation_id: continuation_id(),
                    approved: true,
                }),
            ),
        ];
        let state = replay(&events).expect("replay");
        assert_eq!(
            state.approvals[&continuation_id()].state,
            ApprovalState::Approved
        );
        assert!(matches!(
            reduce(
                Some(state),
                &envelope(
                    4,
                    RuntimeCommittedEvent::ApprovalResolved(ApprovalResolvedEvent {
                        continuation_id: continuation_id(),
                        approved: true
                    })
                )
            ),
            Err(SessionReducerError::ContinuationAlreadyResolved)
        ));
    }

    #[test]
    fn tool_dispatch_outbox_replays_to_a_terminal_state() {
        let digest = ContentHash::digest(b"approved tool action");
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::ToolExecutionDispatched(ToolExecutionDispatchedEvent {
                    execution_id: "execution-1".into(),
                    call_id: "call-1".into(),
                    action_digest: digest,
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ToolExecutionStarted(ToolExecutionStartedEvent {
                    call_id: "call-1".into(),
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ToolExecutionCompleted(ToolExecutionCompletedEvent {
                    call_id: "call-1".into(),
                    result: serde_json::json!({"ok":true}),
                    artifact: None,
                    truncated: false,
                }),
            ),
        ];

        let state = replay(&events).expect("replay");
        let execution = &state.tool_executions["call-1"];
        assert_eq!(execution.execution_id, "execution-1");
        assert_eq!(execution.action_digest, digest);
        assert_eq!(execution.state, ToolExecutionState::Terminal);
        assert_eq!(execution.observed_event_count, 2);
        assert_eq!(
            execution.terminal_at,
            Some(Sequence::new(4).expect("sequence"))
        );
    }

    #[test]
    fn process_reconciliation_replays_as_one_exact_pair() {
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::ProcessReconciliationStarted(
                    ProcessReconciliationStartedEvent {
                        call_id: "reattach-1".into(),
                        process_id: "process-1".into(),
                    },
                ),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ProcessReconciliationCompleted(
                    ProcessReconciliationCompletedEvent {
                        call_id: "reattach-1".into(),
                        process_id: "process-1".into(),
                        status: ProcessReconciliationStatus::Live,
                    },
                ),
            ),
        ];
        let state = replay(&events).expect("replay");
        let reconciliation = &state.process_reconciliations["reattach-1"];
        assert_eq!(reconciliation.process_id, "process-1");
        assert_eq!(
            reconciliation.status,
            Some(ProcessReconciliationStatus::Live)
        );
        assert_eq!(
            reconciliation.completed_at,
            Some(Sequence::new(3).expect("sequence"))
        );

        assert!(matches!(
            reduce(
                Some(state),
                &envelope(
                    4,
                    RuntimeCommittedEvent::ProcessReconciliationCompleted(
                        ProcessReconciliationCompletedEvent {
                            call_id: "reattach-1".into(),
                            process_id: "process-1".into(),
                            status: ProcessReconciliationStatus::Live,
                        },
                    )
                )
            ),
            Err(SessionReducerError::InvalidProcessReconciliationTransition)
        ));
    }

    #[test]
    fn replay_to_returns_exact_prefix_state() {
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                    lifecycle: SessionLifecycle::Suspended,
                    reason: None,
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                    lifecycle: SessionLifecycle::Active,
                    reason: None,
                }),
            ),
        ];
        assert_eq!(
            replay_to(&events, Sequence::new(2).expect("sequence"))
                .expect("prefix")
                .lifecycle,
            SessionLifecycle::Suspended
        );
        assert_eq!(
            replay(&events).expect("full").lifecycle,
            SessionLifecycle::Active
        );
    }

    #[test]
    fn tampering_and_sequence_gaps_are_rejected() {
        let mut event = created();
        if let RuntimeCommittedEvent::SessionCreated(payload) = &mut event.payload {
            payload.workspace = "tampered".into();
        }
        assert!(matches!(
            reduce(None, &event),
            Err(SessionReducerError::EventIntegrity(_))
        ));

        assert!(matches!(
            reduce(
                Some(reduce(None, &created()).expect("created")),
                &envelope(
                    3,
                    RuntimeCommittedEvent::SessionLifecycleChanged(SessionLifecycleChangedEvent {
                        lifecycle: SessionLifecycle::Suspended,
                        reason: None
                    })
                )
            ),
            Err(SessionReducerError::SequenceMismatch {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn metadata_event_type_must_match_typed_payload() {
        let payload = RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
            workspace: "fixture".into(),
            style: "persistent-chat".into(),
        });
        let mut metadata = created().metadata;
        metadata.event_type = "tool.execution.completed".into();
        let mismatched = EventEnvelope::seal(metadata, payload).expect("seal mismatch");
        assert!(matches!(
            reduce(None, &mismatched),
            Err(SessionReducerError::EventTypeMismatch {
                payload: "session.created",
                ..
            })
        ));
    }
}
