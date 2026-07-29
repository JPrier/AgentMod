//! Pure runtime session reducers and typed committed payloads.

use std::collections::BTreeMap;

use agentmod_event_model::{EventClassification, EventEnvelope, EventModelError, EventScope};
use agentmod_graph_engine::{ExecutableGraph, GRAPH_FORMAT_VERSION};
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

/// Provenance class for the immutable style selected by a session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStyleSource {
    /// Shipped with the runtime.
    BuiltIn,
    /// Loaded from the configured user style directory.
    User,
    /// Loaded from the configured project style directory.
    Project,
    /// Supplied by a validated plugin package.
    Plugin,
    /// Supplied directly by a calling client.
    Inline,
}

/// Session-owned memory selection copied from the compiled style.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionMemoryConfiguration {
    /// Stable provider ID, including `none`.
    pub provider: String,
    /// Allowed memory scopes in deterministic order.
    pub scopes: Vec<String>,
    /// Lifecycle boundary selected for retrieval.
    #[serde(default)]
    pub retrieval_timing: String,
    /// Canonical SDK query-construction configuration.
    #[serde(default)]
    pub query_json: String,
    /// Maximum records injected into one context.
    pub max_items: u32,
    /// Maximum injected bytes.
    pub max_injected_bytes: u64,
    /// Lifecycle boundary selected for automatic writes.
    #[serde(default)]
    pub write_policy: String,
    /// Typed provider-projection injection location.
    #[serde(default)]
    pub injection_location: String,
}

/// Session-owned compaction selection copied from the compiled style.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionCompactionConfiguration {
    /// Stable strategy ID.
    pub strategy: String,
    /// Automatic trigger threshold.
    pub trigger_tokens: Option<u64>,
    /// Context tokens reserved outside compacted history.
    #[serde(default)]
    pub reserved_context_tokens: u64,
    /// Maximum provider-visible projection after compaction.
    #[serde(default)]
    pub max_provider_projection_tokens: u64,
    /// Whether unresolved tasks must survive compaction.
    pub preserve_unresolved_tasks: bool,
    /// Whether active process state must survive compaction.
    pub preserve_active_processes: bool,
    /// Typed records that the selected compactor must retain.
    #[serde(default)]
    pub preservation_requirements: Vec<String>,
}

/// Immutable style execution budgets bound at session creation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionStyleBudgets {
    /// Maximum style iterations.
    pub max_iterations: u32,
    /// Maximum graph transitions.
    pub max_steps: u64,
    /// Maximum provider tokens.
    pub max_tokens: u64,
    /// Maximum cost in configured currency micros.
    pub max_cost_micros: u64,
    /// Maximum wall-clock duration.
    pub max_duration_ms: u64,
}

/// Immutable permission defaults bound at session creation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionPermissionDefaults {
    /// Fallback decision.
    pub default: String,
    /// Deterministically ordered action/tool-group overrides.
    pub groups: BTreeMap<String, String>,
}

/// Complete immutable identity and selected components for one session style.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionStyleBinding {
    /// Stable style ID.
    pub id: String,
    /// Semantic style version.
    pub version: String,
    /// Canonical manifest content hash.
    pub content_hash: ContentHash,
    /// Compatibility-bound compiled cache key.
    pub compiled_cache_key: ContentHash,
    /// Hash of the exact compiled descriptor retained in the style lock.
    pub compiled_style_hash: ContentHash,
    /// Source class.
    pub source: SessionStyleSource,
    /// Safe source locator used for diagnostics and migration.
    pub source_locator: String,
    /// Validated plugin-set hash used during compilation.
    pub plugin_set_hash: ContentHash,
    /// Runtime capability-set hash used during compilation.
    pub capability_set_hash: ContentHash,
    /// Runtime API version used during compilation.
    pub runtime_api_version: String,
    /// Canonical style-specific manifest configuration.
    pub configuration_json: String,
    /// Canonical compiled descriptor used by the generic executor.
    pub compiled_style_json: String,
    /// Selected memory configuration.
    pub memory: SessionMemoryConfiguration,
    /// Selected compaction configuration.
    pub compaction: SessionCompactionConfiguration,
    /// Tool groups exposed to this session.
    pub tool_groups: Vec<String>,
    /// Harness selected for this session.
    pub harness: String,
    /// Runtime capabilities required by the style.
    pub required_capabilities: Vec<String>,
    /// Ordered blocking interceptor IDs.
    pub interceptor_order: Vec<String>,
    /// Hard execution budgets.
    pub budgets: SessionStyleBudgets,
    /// Permission defaults applied before mandatory policy.
    pub permission_defaults: SessionPermissionDefaults,
    /// Canonical child-agent policy copied from the compiled style.
    pub child_agent_policy_json: String,
    /// Canonical retry policy copied from the compiled style.
    pub retry_policy_json: String,
    /// Canonical termination policy copied from the compiled style.
    pub termination_policy_json: String,
}

/// Session creation payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionCreatedEvent {
    /// Normalized workspace selected by runtime logic.
    pub workspace: String,
    /// Explicit top-level session style.
    pub style: String,
    /// Immutable compiled-style binding for style-driven sessions.
    ///
    /// Legacy journals remain replayable, but execution must explicitly
    /// migrate them before another style-driven turn may start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style_binding: Option<Box<SessionStyleBinding>>,
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

/// Canonical outcome of restart reconciliation for one scheduler claim.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchedulerDeliveryReconciledEvent {
    /// Deterministic occurrence identifier.
    pub execution_id: String,
    /// Owning schedule.
    pub schedule_id: String,
    /// Stable recovery outcome.
    pub outcome: String,
    /// Pending approval continuation when recovery found an approval boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_id: Option<String>,
}

/// Initializes canonical execution state from an immutable compiled graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleExecutionInitializedEvent {
    /// Exact compiled graph selected for this session execution.
    pub graph: Box<ExecutableGraph>,
}

/// Records entry into one compiled graph node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleNodeEnteredEvent {
    /// Stable compiled graph node ID.
    pub node_id: String,
    /// One-based execution attempt for this node.
    pub attempt: u32,
    /// Zero-based loop iteration containing this attempt.
    pub loop_iteration: u32,
    /// One-based graph step counter.
    pub step: u64,
}

/// Records successful completion of one compiled graph node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleNodeCompletedEvent {
    /// Stable compiled graph node ID.
    pub node_id: String,
    /// One-based execution attempt for this node.
    pub attempt: u32,
    /// Zero-based loop iteration containing this attempt.
    pub loop_iteration: u32,
    /// One-based graph step counter.
    pub step: u64,
    /// Durable reference to the node result, when one was produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_reference: Option<String>,
    /// Durable reference to a full result artifact, when one was produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_reference: Option<String>,
}

/// Records a classified failure of one compiled graph node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleNodeFailedEvent {
    /// Stable compiled graph node ID.
    pub node_id: String,
    /// One-based execution attempt for this node.
    pub attempt: u32,
    /// Zero-based loop iteration containing this attempt.
    pub loop_iteration: u32,
    /// One-based graph step counter.
    pub step: u64,
    /// Stable, redacted failure reason.
    pub reason: String,
    /// Durable reference to failure details, when retained separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_reference: Option<String>,
    /// Explicit terminal reason when this failure ends style execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
}

/// Records the compiled transition selected after one node outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleTransitionSelectedEvent {
    /// Stable source node ID.
    pub from_node_id: String,
    /// Stable destination node ID.
    pub to_node_id: String,
    /// One-based execution attempt for the source node.
    pub attempt: u32,
    /// Zero-based loop iteration containing this transition.
    pub loop_iteration: u32,
    /// One-based graph step counter.
    pub step: u64,
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
    /// Restart recovery reconciled a durable scheduler claim.
    SchedulerDeliveryReconciled(SchedulerDeliveryReconciledEvent),
    /// Establishes the compiled graph used for canonical style execution.
    StyleExecutionInitialized(Box<StyleExecutionInitializedEvent>),
    /// Records entry into a compiled graph node.
    StyleNodeEntered(StyleNodeEnteredEvent),
    /// Records successful completion of a compiled graph node.
    StyleNodeCompleted(StyleNodeCompletedEvent),
    /// Records a classified compiled graph node failure.
    StyleNodeFailed(StyleNodeFailedEvent),
    /// Records the compiled edge selected for the next node.
    StyleTransitionSelected(StyleTransitionSelectedEvent),
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
            Self::SchedulerDeliveryReconciled(_) => "scheduler.delivery_reconciled",
            Self::StyleExecutionInitialized(_) => "style.execution_initialized",
            Self::StyleNodeEntered(_) => "style.node_entered",
            Self::StyleNodeCompleted(_) => "style.node_completed",
            Self::StyleNodeFailed(_) => "style.node_failed",
            Self::StyleTransitionSelected(_) => "style.transition_selected",
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
    /// Immutable style identity and selected component configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style_binding: Option<SessionStyleBinding>,
    /// Replay-owned compiled graph execution state for style-driven sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style_execution: Option<StyleExecutionState>,
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

/// Canonical style execution projection reconstructed without running nodes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleExecutionState {
    /// Exact compiled graph selected by the initialization event.
    pub graph: Box<ExecutableGraph>,
    /// Node currently executing, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_node: Option<StyleNodeEnteredEvent>,
    /// Completed node outcomes in committed order.
    #[serde(default)]
    pub completed_nodes: Vec<StyleNodeCompletedEvent>,
    /// Failed node outcomes in committed order.
    #[serde(default)]
    pub failed_nodes: Vec<StyleNodeFailedEvent>,
    /// Previously selected compiled transitions in committed order.
    #[serde(default)]
    pub transitions: Vec<StyleTransitionSelectedEvent>,
    /// Terminal reason supplied by a failed node, if execution has ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    /// Provider-reported input tokens accumulated from canonical completions.
    #[serde(default)]
    pub input_tokens: u64,
    /// Provider-reported output tokens accumulated from canonical completions.
    #[serde(default)]
    pub output_tokens: u64,
    /// Cumulative provider tokens observed when compaction last committed.
    #[serde(default)]
    pub tokens_at_last_compaction: u64,
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
        style_binding: created.style_binding.as_deref().cloned(),
        style_execution: None,
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
                .map_err(SessionReducerError::Conversation)?;
            if matches!(
                replaced.provenance.method.as_str(),
                "sliding_window" | "summary" | "artifact_handoff" | "tool_output_eviction"
            ) && let Some(execution) = state.style_execution.as_mut()
            {
                execution.tokens_at_last_compaction = execution
                    .input_tokens
                    .checked_add(execution.output_tokens)
                    .ok_or(SessionReducerError::StyleTokenUsageOverflow)?;
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelRequestProposed(_)
        | RuntimeCommittedEvent::ModelRequestApproved(_)
        | RuntimeCommittedEvent::ModelRequestStarted(_)
        | RuntimeCommittedEvent::ModelOutputDeltaObserved(_)
        | RuntimeCommittedEvent::ModelToolCallDeltaObserved(_)
        | RuntimeCommittedEvent::ModelToolCallProposed(_)
        | RuntimeCommittedEvent::ModelRequestCancelled(_)
        | RuntimeCommittedEvent::ModelRequestFailed(_)
        | RuntimeCommittedEvent::ToolCallProposed(_)
        | RuntimeCommittedEvent::ToolCallApproved(_)
        | RuntimeCommittedEvent::SchedulerFired(_)
        | RuntimeCommittedEvent::SchedulerDeliveryReconciled(_) => Ok(()),
        RuntimeCommittedEvent::ModelResponseCompleted(completed) => {
            if let Some(execution) = state.style_execution.as_mut() {
                execution.input_tokens = execution
                    .input_tokens
                    .checked_add(completed.input_tokens)
                    .ok_or(SessionReducerError::StyleTokenUsageOverflow)?;
                execution.output_tokens = execution
                    .output_tokens
                    .checked_add(completed.output_tokens)
                    .ok_or(SessionReducerError::StyleTokenUsageOverflow)?;
            }
            Ok(())
        }
        RuntimeCommittedEvent::StyleExecutionInitialized(initialized) => {
            apply_style_execution_initialized(state, initialized)
        }
        RuntimeCommittedEvent::StyleNodeEntered(entered) => {
            apply_style_node_entered(state, entered)
        }
        RuntimeCommittedEvent::StyleNodeCompleted(completed) => {
            apply_style_node_completed(state, completed)
        }
        RuntimeCommittedEvent::StyleNodeFailed(failed) => apply_style_node_failed(state, failed),
        RuntimeCommittedEvent::StyleTransitionSelected(selected) => {
            apply_style_transition_selected(state, selected)
        }
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

fn apply_style_execution_initialized(
    state: &mut SessionState,
    initialized: &StyleExecutionInitializedEvent,
) -> Result<(), SessionReducerError> {
    if state.style_execution.is_some() {
        return Err(SessionReducerError::DuplicateStyleExecutionInitialization);
    }
    if !valid_compiled_graph(&initialized.graph) {
        return Err(SessionReducerError::InvalidCompiledStyleGraph);
    }
    state.style_execution = Some(StyleExecutionState {
        graph: initialized.graph.clone(),
        active_node: None,
        completed_nodes: Vec::new(),
        failed_nodes: Vec::new(),
        transitions: Vec::new(),
        termination_reason: None,
        input_tokens: 0,
        output_tokens: 0,
        tokens_at_last_compaction: 0,
    });
    Ok(())
}

fn apply_style_node_entered(
    state: &mut SessionState,
    entered: &StyleNodeEnteredEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    if execution.active_node.is_some()
        || execution.termination_reason.is_some()
        || !valid_style_counters(entered.attempt, entered.step)
        || !graph_has_node(&execution.graph, &entered.node_id)
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.active_node = Some(entered.clone());
    Ok(())
}

fn apply_style_node_completed(
    state: &mut SessionState,
    completed: &StyleNodeCompletedEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    if !valid_style_counters(completed.attempt, completed.step)
        || !graph_has_node(&execution.graph, &completed.node_id)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &completed.node_id,
            completed.attempt,
            completed.loop_iteration,
            completed.step,
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.active_node = None;
    execution.completed_nodes.push(completed.clone());
    Ok(())
}

fn apply_style_node_failed(
    state: &mut SessionState,
    failed: &StyleNodeFailedEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    if failed.reason.trim().is_empty()
        || failed
            .termination_reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        || !valid_style_counters(failed.attempt, failed.step)
        || !graph_has_node(&execution.graph, &failed.node_id)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &failed.node_id,
            failed.attempt,
            failed.loop_iteration,
            failed.step,
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.active_node = None;
    execution
        .termination_reason
        .clone_from(&failed.termination_reason);
    execution.failed_nodes.push(failed.clone());
    Ok(())
}

fn apply_style_transition_selected(
    state: &mut SessionState,
    selected: &StyleTransitionSelectedEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    if execution.active_node.is_some()
        || execution.termination_reason.is_some()
        || !valid_style_counters(selected.attempt, selected.step)
        || !graph_has_transition(
            &execution.graph,
            &selected.from_node_id,
            &selected.to_node_id,
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.transitions.push(selected.clone());
    Ok(())
}

fn style_execution_mut(
    state: &mut SessionState,
) -> Result<&mut StyleExecutionState, SessionReducerError> {
    state
        .style_execution
        .as_mut()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)
}

fn valid_compiled_graph(graph: &ExecutableGraph) -> bool {
    graph.format_version == GRAPH_FORMAT_VERSION
        && graph.entry_index < graph.nodes.len()
        && graph
            .nodes
            .iter()
            .enumerate()
            .all(|(index, node)| node.index == index)
        && graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == graph.nodes.len()
        && graph
            .edges
            .iter()
            .all(|edge| edge.from < graph.nodes.len() && edge.to < graph.nodes.len())
}

fn graph_has_node(graph: &ExecutableGraph, node_id: &str) -> bool {
    graph.nodes.iter().any(|node| node.id == node_id)
}

fn graph_has_transition(graph: &ExecutableGraph, from_node_id: &str, to_node_id: &str) -> bool {
    graph.edges.iter().any(|edge| {
        graph.nodes[edge.from].id == from_node_id && graph.nodes[edge.to].id == to_node_id
    })
}

const fn valid_style_counters(attempt: u32, step: u64) -> bool {
    attempt > 0 && step > 0
}

fn active_node_matches(
    active: Option<&StyleNodeEnteredEvent>,
    node_id: &str,
    attempt: u32,
    loop_iteration: u32,
    step: u64,
) -> bool {
    active.is_some_and(|entered| {
        entered.node_id == node_id
            && entered.attempt == attempt
            && entered.loop_iteration == loop_iteration
            && entered.step == step
    })
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
    /// Style execution initialization appeared more than once.
    #[error("style execution was initialized more than once")]
    DuplicateStyleExecutionInitialization,
    /// A style node or transition occurred before execution initialization.
    #[error("style execution is not initialized")]
    StyleExecutionNotInitialized,
    /// The persisted compiled graph cannot safely drive canonical execution state.
    #[error("compiled style graph is invalid")]
    InvalidCompiledStyleGraph,
    /// A style node lifecycle or graph transition is invalid.
    #[error("style execution state transition is invalid")]
    InvalidStyleExecutionTransition,
    /// Provider-reported usage exceeded the replay-safe integer bound.
    #[error("style provider token usage overflowed")]
    StyleTokenUsageOverflow,
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
    use std::{collections::BTreeSet, str::FromStr};

    use agentmod_event_model::{EventMetadata, EventOrigin};
    use agentmod_graph_engine::{CompilerLimits, GraphCacheInputs, compile as compile_graph};
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
                style_binding: None,
            }),
        )
    }

    fn compiled_graph() -> ExecutableGraph {
        compile_graph(
            r#"
format_version = 1
entry = "start"
[budget]
max_steps = 10
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[[nodes]]
id = "start"
kind = "conditional_branch"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "start"
to = "done"
"#,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: "1.0.0".into(),
                capability_set: BTreeSet::default(),
            },
            CompilerLimits::default(),
        )
        .expect("compiled graph")
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
    fn style_execution_replay_recovers_active_node_and_transitions_without_effects() {
        let graph = compiled_graph();
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph.clone()),
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: "start".into(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                    node_id: "start".into(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    result_reference: Some("result:start-1".into()),
                    artifact_reference: Some("artifact:start-1".into()),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                    from_node_id: "start".into(),
                    to_node_id: "done".into(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: "done".into(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
        ];

        let recovered = replay(&events).expect("replay");
        let execution = recovered
            .style_execution
            .as_ref()
            .expect("style execution state");
        assert_eq!(execution.graph.as_ref(), &graph);
        assert_eq!(
            execution.active_node,
            Some(StyleNodeEnteredEvent {
                node_id: "done".into(),
                attempt: 1,
                loop_iteration: 0,
                step: 2,
            })
        );
        assert_eq!(execution.completed_nodes.len(), 1);
        assert_eq!(
            execution.completed_nodes[0].result_reference.as_deref(),
            Some("result:start-1")
        );
        assert_eq!(
            execution.transitions,
            vec![StyleTransitionSelectedEvent {
                from_node_id: "start".into(),
                to_node_id: "done".into(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            }]
        );
        assert_eq!(replay(&events).expect("repeat replay"), recovered);
    }

    #[test]
    fn style_execution_failure_replays_termination_without_dispatch() {
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled_graph()),
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: "start".into(),
                    attempt: 2,
                    loop_iteration: 3,
                    step: 7,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::StyleNodeFailed(StyleNodeFailedEvent {
                    node_id: "start".into(),
                    attempt: 2,
                    loop_iteration: 3,
                    step: 7,
                    reason: "provider_unavailable".into(),
                    artifact_reference: Some("artifact:failure-1".into()),
                    termination_reason: Some("retry_budget_exhausted".into()),
                }),
            ),
        ];

        let execution = replay(&events)
            .expect("replay")
            .style_execution
            .expect("style execution state");
        assert_eq!(execution.active_node, None);
        assert_eq!(
            execution.termination_reason.as_deref(),
            Some("retry_budget_exhausted")
        );
        assert_eq!(execution.failed_nodes.len(), 1);
        assert_eq!(
            execution.failed_nodes[0].artifact_reference.as_deref(),
            Some("artifact:failure-1")
        );
    }

    #[test]
    fn style_token_usage_and_compaction_checkpoint_replay_canonically() {
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled_graph()),
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ModelResponseCompleted(ModelResponseCompletedEvent {
                    cancellation_id: String::from("provider-1"),
                    finish_reason: String::from("stop"),
                    input_tokens: 11,
                    output_tokens: 7,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                    replacement: Vec::new(),
                    provenance: ProjectionProvenance {
                        projection_id: String::from("compaction-4"),
                        source_range: None,
                        method: String::from("sliding_window"),
                        committed_at: Sequence::new(4).expect("sequence"),
                        artifact_id: None,
                    },
                }),
            ),
        ];
        let execution = replay(&events)
            .expect("replay")
            .style_execution
            .expect("style execution");
        assert_eq!(execution.input_tokens, 11);
        assert_eq!(execution.output_tokens, 7);
        assert_eq!(execution.tokens_at_last_compaction, 18);
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
            style_binding: None,
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
