//! Runtime business semantics for durable continuation creation and resolution.

use agentmod_primitives::{ContentHash, ContinuationId, TimestampMillis};
use agentmod_runtime_data::continuation::{
    ChildGraphApprovalOperationRecord, ChildGraphApprovalPayloadRecord,
    ChildMessageApprovalPayloadRecord, ContinuationDataError, ContinuationDataPort,
    ContinuationPayloadRecord, ContinuationRecord, ContinuationStateRecord,
    ContinuationTerminalStateRecord, ContinuationWakeRecord, CreateContinuationDataRequest,
    DeferredTurnPayloadRecord, FindGraphNodeWaitByCancellationDataRequest,
    GenericToolApprovalIdentityRecord, GraphNodeExecutionBoundaryRecord,
    GraphNodeExecutorSourceRecord, GraphNodeWaitPayloadRecord, GraphScheduleApprovalPayloadRecord,
    NativeAutomaticMemoryWriteApprovalPayloadRecord, PendingToolCallPayloadRecord,
    PluginAutomaticMemoryWriteApprovalPayloadRecord, PluginContextOperationApprovalPayloadRecord,
    PluginContextOperationApprovalStageRecord, PluginNodeActionApprovalIdentityRecord,
    PluginNodeInvocationApprovalPayloadRecord, ResolveContinuationDataRequest,
    StyleApprovalPayloadRecord, ToolApprovalPayloadRecord,
    TransitionContinuationTerminalDataRequest,
};
use serde_json::Value;
use thiserror::Error;

/// Logic-owned wake condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationWakeCondition {
    /// Explicit user or supervisor resolution.
    Manual,
    /// Eligible at a wall-clock threshold.
    At(TimestampMillis),
    /// Eligible after a matching committed event.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Optional constrained selector.
        selector: Option<String>,
    },
    /// Eligible after matching supervised process output.
    ProcessOutput {
        /// Runtime process identifier.
        process_id: String,
        /// Literal or constrained pattern.
        pattern: String,
    },
}

/// Business command to create a durable continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateContinuationCommand {
    /// Session containing this durable continuation.
    pub session_id: String,
    /// Opaque identifier produced by the runtime's ID source.
    pub id: ContinuationId,
    /// Wake semantics.
    pub wake_condition: ContinuationWakeCondition,
    /// Pending action reconstructed after a decision.
    pub payload: ContinuationPayload,
    /// Optional expiration.
    pub expires_at: Option<TimestampMillis>,
}

/// Business command to resolve an approval continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation.
    pub id: ContinuationId,
    /// Approval or denial.
    pub approved: bool,
}

/// Business result of an idempotent approval resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveApprovalResult {
    /// True only for the request that won the durable transition.
    pub transitioned: bool,
    /// Endpoint-safe terminal disposition.
    pub disposition: ApprovalDisposition,
    /// Pending action associated with this decision.
    pub payload: ContinuationPayload,
}

/// Business query for one durable continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadContinuationQuery {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation identifier.
    pub id: ContinuationId,
}

/// Logic-owned durable continuation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationState {
    /// Awaiting resolution.
    Pending,
    /// Approved and claimed.
    Resumed,
    /// Denied.
    Cancelled,
    /// Expired.
    Expired,
}

/// Business result for one durable continuation query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadContinuationResult {
    /// Current durable state.
    pub state: ContinuationState,
    /// Durable wake condition used for scheduler proof validation.
    pub wake_condition: ContinuationWakeCondition,
    /// Optional expiration.
    pub expires_at: Option<TimestampMillis>,
    /// Pending action associated with the continuation.
    pub payload: ContinuationPayload,
}

/// Scheduler-owned proof that a durable wake condition matched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationWakeProof {
    /// Time trigger observed at the claimed occurrence.
    At(TimestampMillis),
    /// Exact committed event type matched by the scheduler.
    RuntimeEvent {
        /// Exact committed runtime event identity.
        event_id: String,
        /// Stable event type.
        event_type: String,
        /// Scheduler observation timestamp.
        observed_at: TimestampMillis,
    },
    /// Exact process-output trigger matched by the scheduler.
    ProcessOutput {
        /// Exact process-output observation identity.
        output_id: String,
        /// Runtime process identity.
        process_id: String,
        /// Literal bounded pattern.
        pattern: String,
        /// Scheduler observation timestamp.
        observed_at: TimestampMillis,
    },
}

/// Business command to claim a nonmanual continuation exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeContinuationCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation.
    pub id: ContinuationId,
    /// Schedule presenting the claim.
    pub schedule_id: String,
    /// Authenticated trigger proof.
    pub proof: ContinuationWakeProof,
}

/// Business result for one durable wake attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeContinuationResult {
    /// True only for the request that won the resume-once transition.
    pub transitioned: bool,
    /// Deferred action associated with the continuation.
    pub payload: DeferredTurnContinuation,
}

/// Business command to wake an already-running graph node.
///
/// Unlike [`WakeContinuationCommand`], this command can never create a new
/// provider turn. It only claims an exact, persisted graph transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeGraphNodeCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Durable continuation.
    pub id: ContinuationId,
    /// Exact schedule presenting the wake claim.
    pub schedule_id: String,
    /// Authenticated scheduler proof.
    pub proof: ContinuationWakeProof,
}

/// Typed graph state required to resume after a scheduler wake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphNodeResume {
    /// Canonical owning session.
    pub session_id: String,
    /// Immutable execution-run identity.
    pub run_id: String,
    /// Stable nested branch path.
    pub branch_path: Vec<String>,
    /// Node that was waiting.
    pub node_id: String,
    /// Exact executor selected in the immutable execution plan.
    pub executor_id: String,
    /// Exact executor version selected in the immutable execution plan.
    pub executor_version: String,
    /// Exact executor source selected in the immutable execution plan.
    pub executor_source: GraphNodeExecutorSource,
    /// Exact process boundary selected in the immutable execution plan.
    pub execution_boundary: GraphNodeExecutionBoundary,
    /// Hash of the compiled adapter configuration consumed by this executor.
    pub adapter_configuration_reference: ContentHash,
    /// Hash of the complete immutable execution plan.
    pub execution_plan_hash: ContentHash,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact previously compiled transition target.
    pub transition_target_node_id: String,
    /// Canonical reference/hash for the compiled transition.
    pub compiled_transition_reference: String,
    /// Schedule whose authenticated claim won this wake.
    pub schedule_id: String,
    /// Stable cancellation token for the graph run.
    pub cancellation_token: String,
    /// Canonical cancellation grant/reference.
    pub cancellation_reference: String,
}

/// Result of a graph-node wake claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeGraphNodeResult {
    /// True only for the scheduler claim that changed durable state.
    pub transitioned: bool,
    /// Exact graph continuation to resume for the winning claim.
    ///
    /// Duplicate claims return `None`, making them a safe no-op rather than a
    /// second dispatch of the transition.
    pub resume: Option<GraphNodeResume>,
}

/// Logic-owned durable pending action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuationPayload {
    /// Final intercepted tool call plus the turn state needed to continue.
    ToolApproval(Box<ToolApprovalContinuation>),
    /// A compiled style `user_approval` node plus exact command identity.
    StyleApproval(Box<StyleApprovalContinuation>),
    /// Exact runtime-owned automatic-memory write waiting for approval.
    NativeAutomaticMemoryWriteApproval(Box<NativeAutomaticMemoryWriteApprovalContinuation>),
    /// Exact non-idempotent plugin automatic-memory write waiting for approval.
    PluginAutomaticMemoryWriteApproval(Box<PluginAutomaticMemoryWriteApprovalContinuation>),
    /// Exact plugin memory-retrieve/compaction stage waiting for approval.
    PluginContextOperationApproval(Box<PluginContextOperationApprovalContinuation>),
    /// Consequential graph schedule creation waiting for policy approval.
    GraphScheduleApproval(Box<GraphScheduleApprovalContinuation>),
    /// Generic child-message delivery waiting for policy approval.
    ChildMessageApproval(Box<ChildMessageApprovalContinuation>),
    /// Exact plugin-host node invocation waiting for policy approval.
    PluginNodeInvocationApproval(Box<PluginNodeInvocationApprovalContinuation>),
    /// Exact generic child-graph ancillary action waiting for resolution.
    ChildGraphApproval(Box<ChildGraphApprovalContinuation>),
    /// Complete provider turn deferred behind a scheduler-owned trigger.
    DeferredTurn(Box<DeferredTurnContinuation>),
    /// Exact graph node state suspended behind a scheduler-owned trigger.
    GraphNodeWait(Box<GraphNodeWaitContinuation>),
    /// Storage-only marker for callers without an executable action.
    Opaque(String),
}

/// Logic-owned restart-safe native automatic-memory approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAutomaticMemoryWriteApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Exact automatic-memory outbox identity.
    pub write_id: String,
    /// Hash of the originating turn request.
    pub request_hash: ContentHash,
    /// Digest of the exact consequential action.
    pub action_digest: ContentHash,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable session style.
    pub style: String,
    /// User input owning the turn.
    pub prompt: String,
    /// Provider retained for exact turn resume.
    pub provider: String,
    /// Model retained for exact turn resume.
    pub model: String,
    /// Provider and style options retained for resume.
    pub options: Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Logic-owned child-graph ancillary operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildGraphApprovalOperation {
    /// Authorize one exact child creation proposal.
    CreateChild,
    /// Authorize cancellation of an exact canonical child set.
    CancelChildren,
    /// Accept one exact reviewer routing evidence record.
    ReviewEvidence,
}

/// Logic-owned restart-safe child-graph ancillary approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildGraphApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Exact ancillary operation.
    pub operation: ChildGraphApprovalOperation,
    /// Immutable graph run.
    pub run_id: String,
    /// Compiled node owning the operation.
    pub node_id: String,
    /// Stable nested branch ancestry.
    pub branch_path: Vec<String>,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Exact compiled adapter configuration hash.
    pub adapter_configuration_reference: ContentHash,
    /// Hash of the complete bounded ancillary request.
    pub request_hash: ContentHash,
    /// Operation-specific action or evidence hash.
    pub subject_hash: ContentHash,
}

/// Logic-owned restart-safe generic child-message approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildMessageApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Stable business message identity.
    pub message_identity: ContentHash,
    /// Digest of the exact proposed consequential action.
    pub action_digest: ContentHash,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable session style.
    pub style: String,
    /// User input owning the graph run.
    pub prompt: String,
    /// Provider retained for exact resume identity.
    pub provider: String,
    /// Model retained for exact resume identity.
    pub model: String,
    /// Canonical graph input environment.
    pub options: Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Logic-owned restart-safe exact plugin-node invocation approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeInvocationApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Stable digest-derived plugin invocation ID.
    pub invocation_id: String,
    /// Digest of the complete isolated invocation.
    pub invocation_digest: ContentHash,
    /// Exact bounded node-input hash.
    pub input_hash: ContentHash,
    /// Exact readable-state projection hash.
    pub readable_state_hash: ContentHash,
    /// Canonical event that caused the invocation.
    pub causation_event_id: String,
    /// Canonical invocation-proposal event that owns the approval.
    pub proposal_event_id: String,
    /// Exact immutable plugin identity.
    pub plugin_id: String,
    /// Canonical graph run.
    pub run_id: String,
    /// Active compiled node.
    pub node_id: String,
    /// Stable nested branch ancestry.
    pub branch_path: Vec<String>,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact persisted executor ID.
    pub executor_id: String,
    /// Exact persisted executor version.
    pub executor_version: String,
    /// Exact persisted executor declaration hash.
    pub executor_declaration_hash: ContentHash,
    /// Exact compiled adapter configuration hash.
    pub adapter_configuration_reference: ContentHash,
    /// Digest of the exact consequential action.
    pub action_digest: ContentHash,
    /// Immutable execution-plan hash.
    pub execution_plan_hash: ContentHash,
    /// Immutable registry hash.
    pub registry_hash: ContentHash,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable style identity.
    pub style: String,
    /// User input owning the graph run.
    pub prompt: String,
    /// Provider retained for exact resume identity.
    pub provider: String,
    /// Model retained for exact resume identity.
    pub model: String,
    /// Canonical graph input environment.
    pub options: Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Logic-owned restart-safe graph schedule approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphScheduleApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Deterministic graph schedule identity.
    pub schedule_id: String,
    /// Digest of the exact proposed schedule action.
    pub action_digest: ContentHash,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable session style.
    pub style: String,
    /// User input owning the graph run.
    pub prompt: String,
    /// Provider retained for exact resume identity.
    pub provider: String,
    /// Model retained for exact resume identity.
    pub model: String,
    /// Canonical graph input environment.
    pub options: Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Logic-owned restart-safe compiled-style approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleApprovalContinuation {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// User-authored input owning the graph execution.
    pub prompt: String,
    /// Provider retained for exact command identity.
    pub provider: String,
    /// Model retained for exact command identity.
    pub model: String,
    /// Style-specific and provider options.
    pub options: Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the graph execution.
    pub cancellation_id: String,
    /// Exact compiled-style cache key selected by the session.
    pub compiled_style_cache_key: String,
    /// Active graph node requesting the decision.
    pub node_id: String,
    /// Stable nested branch path. Empty identifies a root graph node.
    pub branch_path: Vec<String>,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Canonical hash of caller-controlled graph inputs.
    pub request_reference: String,
}

/// Logic-owned restart-safe plugin automatic-memory approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginAutomaticMemoryWriteApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Exact automatic-memory outbox identity.
    pub write_id: String,
    /// Digest-backed isolated invocation identity.
    pub invocation_id: String,
    /// Digest of every exact invocation field.
    pub invocation_digest: ContentHash,
    /// Exact activated plugin.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Exact selected memory provider.
    pub provider_id: String,
    /// Exact selected provider version.
    pub provider_version: String,
    /// Hash of the selected provider declaration.
    pub declaration_hash: ContentHash,
    /// Hash of immutable provider configuration.
    pub configuration_reference: ContentHash,
    /// Hash of the originating turn request.
    pub request_hash: ContentHash,
    /// Digest of the exact consequential action.
    pub action_digest: ContentHash,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable session style.
    pub style: String,
    /// User input owning the turn.
    pub prompt: String,
    /// Provider retained for exact turn resume.
    pub provider: String,
    /// Model retained for exact turn resume.
    pub model: String,
    /// Provider and style options retained for resume.
    pub options: Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Logic-owned plugin context-operation approval stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginContextOperationApprovalStage {
    /// Approval before crossing the isolated plugin boundary.
    Invocation,
    /// Approval before applying the validated projection.
    Application,
}

/// Logic-owned restart-safe plugin context-operation approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginContextOperationApprovalContinuation {
    /// Canonical owning session.
    pub session_id: String,
    /// Digest-derived plugin operation identity.
    pub invocation_id: String,
    /// Hash of every immutable invocation field.
    pub invocation_digest: ContentHash,
    /// Exact approval stage.
    pub stage: PluginContextOperationApprovalStage,
    /// Digest of the consequential action.
    pub action_digest: ContentHash,
    /// Completed proposal hash for application approval.
    pub proposal_hash: Option<ContentHash>,
    /// Provider-projection replacement hash for application approval.
    pub replacement_hash: Option<ContentHash>,
    /// Canonical workspace.
    pub workspace: String,
    /// Immutable session style.
    pub style: String,
    /// User input owning the turn.
    pub prompt: String,
    /// Provider retained for exact resume.
    pub provider: String,
    /// Model retained for exact resume.
    pub model: String,
    /// Provider/style options retained for exact resume.
    pub options: Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Logic-owned restart-safe deferred provider turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredTurnContinuation {
    /// Canonical session containing the deferred work.
    pub session_id: String,
    /// Exact schedule allowed to claim this work.
    pub schedule_id: String,
    /// User-authored prompt.
    pub prompt: String,
    /// Workspace selected for the turn.
    pub workspace: String,
    /// Provider adapter selected for the turn.
    pub provider: String,
    /// Provider model selected for the turn.
    pub model: String,
    /// Provider-specific request options.
    pub options: Value,
    /// Session style selected for execution.
    pub style: String,
    /// Stable cancellation identifier for the deferred turn.
    pub cancellation_id: String,
}

/// Logic-owned, restart-safe graph-node wait payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphNodeWaitContinuation {
    /// Canonical session containing the graph run.
    pub session_id: String,
    /// Immutable graph-run identity.
    pub run_id: String,
    /// Stable nested branch path from the graph root.
    pub branch_path: Vec<String>,
    /// Node that created the durable wait.
    pub node_id: String,
    /// Exact persisted node executor identity.
    pub executor_id: String,
    /// Exact persisted node executor version.
    pub executor_version: String,
    /// Exact persisted executor source, including plugin identity.
    pub executor_source: GraphNodeExecutorSource,
    /// Exact persisted executor process boundary.
    pub execution_boundary: GraphNodeExecutionBoundary,
    /// Hash of the compiled adapter configuration consumed by the executor.
    pub adapter_configuration_reference: ContentHash,
    /// Hash of the complete immutable execution plan owning this wait.
    pub execution_plan_hash: ContentHash,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact compiled transition target selected before waiting.
    pub transition_target_node_id: String,
    /// Canonical reference/hash for the compiled transition.
    pub compiled_transition_reference: String,
    /// Exact scheduler identity allowed to wake this node.
    pub schedule_id: String,
    /// Stable graph cancellation token.
    pub cancellation_token: String,
    /// Canonical cancellation grant/reference.
    pub cancellation_reference: String,
}

/// Query for the unique pending graph-node wait behind a cancellation token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FindGraphNodeWaitByCancellationQuery {
    /// Stable graph-run cancellation token.
    pub cancellation_token: String,
}

/// Exact pending graph-node wait resolved for cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoundGraphNodeWait {
    /// Canonical owning session.
    pub session_id: String,
    /// Durable continuation identity.
    pub continuation_id: ContinuationId,
    /// Durable continuation state at lookup.
    pub state: ContinuationState,
    /// Exact persisted graph-node wait payload.
    pub wait: GraphNodeWaitContinuation,
}

/// Logic-owned executor source retained in a graph-node wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphNodeExecutorSource {
    /// Runtime logic owns execution.
    Runtime,
    /// An exact plugin owns execution behind the plugin-host boundary.
    Plugin {
        /// Immutable plugin identity.
        plugin_id: String,
    },
}

/// Logic-owned process boundary retained in a graph-node wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphNodeExecutionBoundary {
    /// Runtime logic owns execution.
    RuntimeLogic,
    /// Execution must travel through the isolated plugin host.
    PluginHost,
}

/// Logic-owned restart-safe tool approval payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolApprovalContinuation {
    /// Canonical session containing the pending action.
    pub session_id: String,
    /// Workspace selected for the turn.
    pub workspace: String,
    /// Provider-issued tool-call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Intercepted and policy-checked tool arguments.
    pub arguments: Value,
    /// Cancellation identifier associated with the turn.
    pub cancellation_id: String,
    /// Provider adapter selected for the turn.
    pub provider: String,
    /// Provider model selected for the turn.
    pub model: String,
    /// Provider-specific request options.
    pub options: Value,
    /// Session style selected for execution.
    pub style: String,
    /// Harness continuation that originally emitted the tool call.
    pub harness_continuation: String,
    /// Remaining sibling calls in the same provider batch.
    pub remaining_tool_calls: Vec<PendingToolCallContinuation>,
    /// Exact generic graph work when the tool belongs to a graph branch.
    ///
    /// Legacy provider and root-style approvals leave this absent.
    pub generic_graph_identity: Option<GenericToolApprovalIdentity>,
}

/// Logic-owned immutable graph identity for a tool approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenericToolApprovalIdentity {
    /// Runtime-owned graph run.
    pub run_id: String,
    /// Compiled node owning the tool call.
    pub node_id: String,
    /// Stable nested branch path. An empty path denotes a root graph node.
    pub branch_path: Vec<String>,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact persisted executor ID.
    pub executor_id: String,
    /// Exact persisted executor version.
    pub executor_version: String,
    /// Hash of the exact executor declaration.
    pub executor_declaration_hash: ContentHash,
    /// Hash of the compiled node configuration consumed by the executor.
    pub adapter_configuration_reference: ContentHash,
    /// Hash of the complete immutable execution plan.
    pub execution_plan_hash: ContentHash,
    /// Hash of the registry used to compile that plan.
    pub registry_hash: ContentHash,
    /// Exact plugin-node action owning this tool call, when applicable.
    pub plugin_node_action: Option<PluginNodeActionApprovalIdentity>,
}

/// Logic-owned immutable plugin-node action identity retained behind tool approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeActionApprovalIdentity {
    /// Exact isolated plugin invocation.
    pub invocation_id: String,
    /// Exact runtime outcome-validation marker.
    pub validation_hash: ContentHash,
    /// Stable ordered action position.
    pub action_index: u32,
    /// Exact plugin proposal hash.
    pub action_hash: ContentHash,
    /// Exact runtime-owned typed proposal hash.
    pub runtime_proposal_hash: ContentHash,
}

/// Logic-owned sibling tool call retained behind an approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingToolCallContinuation {
    /// Harness continuation alias for the batch.
    pub harness_continuation: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Provider-supplied arguments.
    pub arguments: Value,
}

/// Approval disposition after resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDisposition {
    /// Approved action is claimed for continuation.
    Approved,
    /// Action is durably denied.
    Denied,
}

/// Runtime-logic-owned terminal continuation disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationTerminalDisposition {
    /// Cancel the pending continuation.
    Cancelled,
    /// Expire the pending continuation before resumption.
    Expired,
}

/// Command for an atomic durable pending-to-terminal transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionContinuationTerminalCommand {
    /// Session containing the continuation.
    pub session_id: String,
    /// Exact durable continuation identity.
    pub id: ContinuationId,
    /// Requested terminal disposition.
    pub disposition: ContinuationTerminalDisposition,
}

/// Result of an idempotent durable terminal transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionContinuationTerminalResult {
    /// True only for the caller that changed durable state.
    pub transitioned: bool,
    /// Exact terminal disposition retained in storage.
    pub disposition: ContinuationTerminalDisposition,
    /// Validated pending action retained for recovery and audit.
    pub payload: ContinuationPayload,
}

/// Narrow continuation use-case interface consumed by runtime service.
pub trait ContinuationLogicPort {
    /// Creates a durable pending continuation before requesting approval.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] when business validation or persistence fails.
    fn create_continuation(
        &self,
        command: CreateContinuationCommand,
    ) -> Result<(), ContinuationLogicError>;

    /// Atomically resolves an approval.
    ///
    /// Exactly one caller receives `transitioned == true`; duplicate equal
    /// decisions are idempotent, while conflicting decisions are errors.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for state conflicts or persistence failure.
    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, ContinuationLogicError>;

    /// Loads one continuation without changing its state.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for invalid scope or persistence failure.
    fn load_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<LoadContinuationResult, ContinuationLogicError>;
    /// Loads a continuation when present without treating absence as failure.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for invalid scope, corrupt payload,
    /// or inaccessible continuation storage.
    fn load_optional_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<Option<LoadContinuationResult>, ContinuationLogicError>;

    /// Atomically transitions a pending continuation to cancelled or expired.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for invalid scope, state conflicts,
    /// corrupt payloads, unsupported data adapters, or persistence failure.
    fn transition_terminal(
        &self,
        _command: TransitionContinuationTerminalCommand,
    ) -> Result<TransitionContinuationTerminalResult, ContinuationLogicError> {
        Err(ContinuationLogicError::InvalidResolutionState)
    }

    /// Finds the unique pending graph wait behind an opaque cancellation token.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] for invalid, ambiguous, corrupt, or
    /// unsupported bounded lookups.
    fn find_graph_node_wait_by_cancellation(
        &self,
        _query: FindGraphNodeWaitByCancellationQuery,
    ) -> Result<Option<FoundGraphNodeWait>, ContinuationLogicError> {
        Err(ContinuationLogicError::UnsupportedCancellationLookup)
    }

    /// Claims a scheduler-owned, nonmanual continuation exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] when the proof, payload, or durable
    /// state does not authorize a wake.
    fn wake_continuation(
        &self,
        _command: WakeContinuationCommand,
    ) -> Result<WakeContinuationResult, ContinuationLogicError> {
        Err(ContinuationLogicError::InvalidWakeProof)
    }

    /// Claims a graph-node scheduler continuation exactly once without
    /// constructing a provider or user turn.
    ///
    /// This boundary rejects a durably cancelled continuation and preserves
    /// the cancellation token/reference for the caller. Canonical session-run
    /// cancellation state is owned and revalidated by runtime orchestration
    /// immediately before graph resumption; continuation storage does not own
    /// that state.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationLogicError`] when the schedule, proof, expiry,
    /// cancellation state, or payload does not authorize the exact resume.
    fn wake_graph_node(
        &self,
        _command: WakeGraphNodeCommand,
    ) -> Result<WakeGraphNodeResult, ContinuationLogicError> {
        Err(ContinuationLogicError::InvalidWakeProof)
    }
}

/// Continuation business implementation over data only.
#[derive(Clone, Debug)]
pub struct ContinuationLogic<D> {
    data: D,
}

impl<D> ContinuationLogic<D> {
    /// Creates continuation logic.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> ContinuationLogicPort for ContinuationLogic<D>
where
    D: ContinuationDataPort,
{
    fn create_continuation(
        &self,
        command: CreateContinuationCommand,
    ) -> Result<(), ContinuationLogicError> {
        if command.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        validate_wake_condition(&command.wake_condition)?;
        validate_expiration(command.expires_at, &command.wake_condition)?;
        validate_payload(&command.session_id, &command.payload)?;
        self.data
            .create(CreateContinuationDataRequest {
                record: ContinuationRecord {
                    session_id: command.session_id,
                    id: command.id.to_string(),
                    state: ContinuationStateRecord::Pending,
                    wake_condition: to_data_wake(command.wake_condition),
                    payload: to_data_payload(command.payload),
                    expires_at_millis: command.expires_at.map(TimestampMillis::get),
                },
            })
            .map_err(ContinuationLogicError::Data)
    }

    fn resolve_approval(
        &self,
        command: ResolveApprovalCommand,
    ) -> Result<ResolveApprovalResult, ContinuationLogicError> {
        let pending = self
            .data
            .load(&command.session_id, &command.id.to_string())
            .map_err(ContinuationLogicError::Data)?;
        if !matches!(pending.wake_condition, ContinuationWakeRecord::Manual) {
            return Err(ContinuationLogicError::InvalidWakeProof);
        }
        let record = self
            .data
            .resolve(ResolveContinuationDataRequest {
                session_id: command.session_id,
                id: command.id.to_string(),
                approved: command.approved,
            })
            .map_err(ContinuationLogicError::Data)?;
        let disposition = match record.state {
            ContinuationStateRecord::Resumed => ApprovalDisposition::Approved,
            ContinuationStateRecord::Cancelled => ApprovalDisposition::Denied,
            ContinuationStateRecord::Pending | ContinuationStateRecord::Expired => {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
        };
        Ok(ResolveApprovalResult {
            transitioned: record.transitioned,
            disposition,
            payload: from_data_payload(record.payload),
        })
    }

    fn load_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<LoadContinuationResult, ContinuationLogicError> {
        if query.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let record = self
            .data
            .load(&query.session_id, &query.id.to_string())
            .map_err(ContinuationLogicError::Data)?;
        let payload = from_data_payload(record.payload);
        validate_payload(&query.session_id, &payload)?;
        Ok(LoadContinuationResult {
            state: match record.state {
                ContinuationStateRecord::Pending => ContinuationState::Pending,
                ContinuationStateRecord::Resumed => ContinuationState::Resumed,
                ContinuationStateRecord::Cancelled => ContinuationState::Cancelled,
                ContinuationStateRecord::Expired => ContinuationState::Expired,
            },
            wake_condition: from_data_wake(record.wake_condition)?,
            expires_at: record.expires_at_millis.map(TimestampMillis::new),
            payload,
        })
    }

    fn load_optional_continuation(
        &self,
        query: LoadContinuationQuery,
    ) -> Result<Option<LoadContinuationResult>, ContinuationLogicError> {
        if query.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let Some(record) = self
            .data
            .load_optional(&query.session_id, &query.id.to_string())
            .map_err(ContinuationLogicError::Data)?
        else {
            return Ok(None);
        };
        let payload = from_data_payload(record.payload);
        validate_payload(&query.session_id, &payload)?;
        Ok(Some(LoadContinuationResult {
            state: match record.state {
                ContinuationStateRecord::Pending => ContinuationState::Pending,
                ContinuationStateRecord::Resumed => ContinuationState::Resumed,
                ContinuationStateRecord::Cancelled => ContinuationState::Cancelled,
                ContinuationStateRecord::Expired => ContinuationState::Expired,
            },
            wake_condition: from_data_wake(record.wake_condition)?,
            expires_at: record.expires_at_millis.map(TimestampMillis::new),
            payload,
        }))
    }

    fn transition_terminal(
        &self,
        command: TransitionContinuationTerminalCommand,
    ) -> Result<TransitionContinuationTerminalResult, ContinuationLogicError> {
        if command.session_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let target = match command.disposition {
            ContinuationTerminalDisposition::Cancelled => {
                ContinuationTerminalStateRecord::Cancelled
            }
            ContinuationTerminalDisposition::Expired => ContinuationTerminalStateRecord::Expired,
        };
        let record = self
            .data
            .transition_terminal(TransitionContinuationTerminalDataRequest {
                session_id: command.session_id.clone(),
                id: command.id.to_string(),
                target,
            })
            .map_err(ContinuationLogicError::Data)?;
        let disposition = match record.state {
            ContinuationTerminalStateRecord::Cancelled => {
                ContinuationTerminalDisposition::Cancelled
            }
            ContinuationTerminalStateRecord::Expired => ContinuationTerminalDisposition::Expired,
        };
        if disposition != command.disposition {
            return Err(ContinuationLogicError::InvalidResolutionState);
        }
        let payload = from_data_payload(record.payload);
        validate_payload(&command.session_id, &payload)?;
        Ok(TransitionContinuationTerminalResult {
            transitioned: record.transitioned,
            disposition,
            payload,
        })
    }

    fn find_graph_node_wait_by_cancellation(
        &self,
        query: FindGraphNodeWaitByCancellationQuery,
    ) -> Result<Option<FoundGraphNodeWait>, ContinuationLogicError> {
        if query.cancellation_token.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidCancellationToken);
        }
        let Some(record) = self
            .data
            .find_graph_node_wait_by_cancellation(FindGraphNodeWaitByCancellationDataRequest {
                cancellation_token: query.cancellation_token.clone(),
            })
            .map_err(ContinuationLogicError::Data)?
        else {
            return Ok(None);
        };
        let state = match record.state {
            ContinuationStateRecord::Pending => ContinuationState::Pending,
            ContinuationStateRecord::Resumed => ContinuationState::Resumed,
            ContinuationStateRecord::Cancelled => ContinuationState::Cancelled,
            ContinuationStateRecord::Expired => ContinuationState::Expired,
        };
        let payload = from_data_payload(record.payload);
        validate_payload(&record.session_id, &payload)?;
        let ContinuationPayload::GraphNodeWait(wait) = payload else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        if wait.cancellation_token != query.cancellation_token {
            return Err(ContinuationLogicError::InvalidCancellationToken);
        }
        let continuation_id = record
            .id
            .parse()
            .map_err(|_| ContinuationLogicError::InvalidPayload)?;
        Ok(Some(FoundGraphNodeWait {
            session_id: record.session_id,
            continuation_id,
            state,
            wait: *wait,
        }))
    }

    fn wake_continuation(
        &self,
        command: WakeContinuationCommand,
    ) -> Result<WakeContinuationResult, ContinuationLogicError> {
        if command.session_id.trim().is_empty() || command.schedule_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let loaded = self.load_continuation(LoadContinuationQuery {
            session_id: command.session_id.clone(),
            id: command.id,
        })?;
        let ContinuationPayload::DeferredTurn(payload) = loaded.payload else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        if payload.schedule_id != command.schedule_id
            || !wake_proof_matches(&loaded.wake_condition, &command.proof)
        {
            return Err(ContinuationLogicError::InvalidWakeProof);
        }
        if loaded
            .expires_at
            .is_some_and(|expiry| proof_timestamp(&command.proof) > expiry)
        {
            return Err(ContinuationLogicError::Expired);
        }
        match loaded.state {
            ContinuationState::Resumed => {
                return Ok(WakeContinuationResult {
                    transitioned: false,
                    payload: *payload,
                });
            }
            ContinuationState::Pending => {}
            ContinuationState::Cancelled | ContinuationState::Expired => {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
        }
        let resolved = self
            .data
            .resolve(ResolveContinuationDataRequest {
                session_id: command.session_id,
                id: command.id.to_string(),
                approved: true,
            })
            .map_err(ContinuationLogicError::Data)?;
        if resolved.state != ContinuationStateRecord::Resumed {
            return Err(ContinuationLogicError::InvalidResolutionState);
        }
        let ContinuationPayload::DeferredTurn(resolved_payload) =
            from_data_payload(resolved.payload)
        else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        Ok(WakeContinuationResult {
            transitioned: resolved.transitioned,
            payload: *resolved_payload,
        })
    }

    fn wake_graph_node(
        &self,
        command: WakeGraphNodeCommand,
    ) -> Result<WakeGraphNodeResult, ContinuationLogicError> {
        if command.session_id.trim().is_empty() || command.schedule_id.trim().is_empty() {
            return Err(ContinuationLogicError::InvalidSession);
        }
        let loaded = self.load_continuation(LoadContinuationQuery {
            session_id: command.session_id.clone(),
            id: command.id,
        })?;
        let ContinuationPayload::GraphNodeWait(payload) = loaded.payload else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        let loaded_payload = *payload;
        if loaded_payload.schedule_id != command.schedule_id
            || !wake_proof_matches(&loaded.wake_condition, &command.proof)
        {
            return Err(ContinuationLogicError::InvalidWakeProof);
        }
        if loaded
            .expires_at
            .is_some_and(|expiry| proof_timestamp(&command.proof) > expiry)
        {
            if !matches!(
                loaded.state,
                ContinuationState::Pending | ContinuationState::Expired
            ) {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
            let expired = self.transition_terminal(TransitionContinuationTerminalCommand {
                session_id: command.session_id.clone(),
                id: command.id,
                disposition: ContinuationTerminalDisposition::Expired,
            })?;
            if expired.payload
                != ContinuationPayload::GraphNodeWait(Box::new(loaded_payload.clone()))
            {
                return Err(ContinuationLogicError::InvalidPayload);
            }
            return Err(ContinuationLogicError::Expired);
        }
        match loaded.state {
            ContinuationState::Resumed => {
                return Ok(WakeGraphNodeResult {
                    transitioned: false,
                    resume: None,
                });
            }
            ContinuationState::Pending => {}
            ContinuationState::Cancelled | ContinuationState::Expired => {
                return Err(ContinuationLogicError::InvalidResolutionState);
            }
        }
        let resolved = self
            .data
            .resolve(ResolveContinuationDataRequest {
                session_id: command.session_id.clone(),
                id: command.id.to_string(),
                approved: true,
            })
            .map_err(ContinuationLogicError::Data)?;
        if resolved.state != ContinuationStateRecord::Resumed {
            return Err(ContinuationLogicError::InvalidResolutionState);
        }
        let resolved_payload = from_data_payload(resolved.payload);
        validate_payload(&command.session_id, &resolved_payload)?;
        let ContinuationPayload::GraphNodeWait(resolved_payload) = resolved_payload else {
            return Err(ContinuationLogicError::InvalidPayload);
        };
        if *resolved_payload != loaded_payload {
            return Err(ContinuationLogicError::InvalidPayload);
        }
        if resolved.transitioned {
            Ok(WakeGraphNodeResult {
                transitioned: true,
                resume: Some(graph_resume(*resolved_payload)),
            })
        } else {
            Ok(WakeGraphNodeResult {
                transitioned: false,
                resume: None,
            })
        }
    }
}

fn validate_wake_condition(
    wake_condition: &ContinuationWakeCondition,
) -> Result<(), ContinuationLogicError> {
    match wake_condition {
        ContinuationWakeCondition::RuntimeEvent { event_type, .. }
            if event_type.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidWakeCondition)
        }
        ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        } if process_id.trim().is_empty() || pattern.is_empty() => {
            Err(ContinuationLogicError::InvalidWakeCondition)
        }
        _ => Ok(()),
    }
}

fn validate_expiration(
    expires_at: Option<TimestampMillis>,
    wake_condition: &ContinuationWakeCondition,
) -> Result<(), ContinuationLogicError> {
    let Some(expires_at) = expires_at else {
        return Ok(());
    };
    if expires_at.get() < 0
        || matches!(wake_condition, ContinuationWakeCondition::At(wake_at) if wake_at > &expires_at)
    {
        return Err(ContinuationLogicError::InvalidExpiration);
    }
    Ok(())
}

fn to_data_wake(wake_condition: ContinuationWakeCondition) -> ContinuationWakeRecord {
    match wake_condition {
        ContinuationWakeCondition::Manual => ContinuationWakeRecord::Manual,
        ContinuationWakeCondition::At(timestamp) => ContinuationWakeRecord::At(timestamp.get()),
        ContinuationWakeCondition::RuntimeEvent {
            event_type,
            selector,
        } => ContinuationWakeRecord::RuntimeEvent {
            event_type,
            selector,
        },
        ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        } => ContinuationWakeRecord::ProcessOutput {
            process_id,
            pattern,
        },
    }
}

fn from_data_wake(
    wake_condition: ContinuationWakeRecord,
) -> Result<ContinuationWakeCondition, ContinuationLogicError> {
    let wake_condition = match wake_condition {
        ContinuationWakeRecord::Manual => ContinuationWakeCondition::Manual,
        ContinuationWakeRecord::At(value) => {
            ContinuationWakeCondition::At(TimestampMillis::new(value))
        }
        ContinuationWakeRecord::RuntimeEvent {
            event_type,
            selector,
        } => ContinuationWakeCondition::RuntimeEvent {
            event_type,
            selector,
        },
        ContinuationWakeRecord::ProcessOutput {
            process_id,
            pattern,
        } => ContinuationWakeCondition::ProcessOutput {
            process_id,
            pattern,
        },
    };
    validate_wake_condition(&wake_condition)?;
    Ok(wake_condition)
}

fn wake_proof_matches(
    condition: &ContinuationWakeCondition,
    proof: &ContinuationWakeProof,
) -> bool {
    match (condition, proof) {
        (ContinuationWakeCondition::At(expected), ContinuationWakeProof::At(observed)) => {
            observed >= expected
        }
        (
            ContinuationWakeCondition::RuntimeEvent {
                event_type: expected,
                selector: _,
            },
            ContinuationWakeProof::RuntimeEvent { event_type, .. },
        ) => expected == event_type,
        (
            ContinuationWakeCondition::ProcessOutput {
                process_id: expected_process,
                pattern: expected_pattern,
            },
            ContinuationWakeProof::ProcessOutput {
                process_id,
                pattern,
                ..
            },
        ) => expected_process == process_id && expected_pattern == pattern,
        _ => false,
    }
}

fn proof_timestamp(proof: &ContinuationWakeProof) -> TimestampMillis {
    match proof {
        ContinuationWakeProof::At(timestamp) => *timestamp,
        ContinuationWakeProof::RuntimeEvent { observed_at, .. }
        | ContinuationWakeProof::ProcessOutput { observed_at, .. } => *observed_at,
    }
}

fn graph_resume(payload: GraphNodeWaitContinuation) -> GraphNodeResume {
    GraphNodeResume {
        session_id: payload.session_id,
        run_id: payload.run_id,
        branch_path: payload.branch_path,
        node_id: payload.node_id,
        executor_id: payload.executor_id,
        executor_version: payload.executor_version,
        executor_source: payload.executor_source,
        execution_boundary: payload.execution_boundary,
        adapter_configuration_reference: payload.adapter_configuration_reference,
        execution_plan_hash: payload.execution_plan_hash,
        attempt: payload.attempt,
        loop_iteration: payload.loop_iteration,
        step: payload.step,
        transition_target_node_id: payload.transition_target_node_id,
        compiled_transition_reference: payload.compiled_transition_reference,
        schedule_id: payload.schedule_id,
        cancellation_token: payload.cancellation_token,
        cancellation_reference: payload.cancellation_reference,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "validation exhaustively binds every durable continuation variant at the logic boundary"
)]
fn validate_payload(
    session_id: &str,
    payload: &ContinuationPayload,
) -> Result<(), ContinuationLogicError> {
    match payload {
        ContinuationPayload::ToolApproval(tool)
            if tool.session_id != session_id
                || tool.workspace.trim().is_empty()
                || tool.call_id.trim().is_empty()
                || tool.tool.trim().is_empty()
                || !tool.arguments.is_object()
                || tool.cancellation_id.trim().is_empty()
                || tool.provider.trim().is_empty()
                || tool.model.trim().is_empty()
                || !tool.options.is_object()
                || tool.style.trim().is_empty()
                || tool.harness_continuation.trim().is_empty()
                || tool.remaining_tool_calls.len() > 64
                || tool.remaining_tool_calls.iter().any(|pending| {
                    pending.harness_continuation.trim().is_empty()
                        || pending.call_id.trim().is_empty()
                        || pending.tool.trim().is_empty()
                        || !pending.arguments.is_object()
                })
                || tool
                    .generic_graph_identity
                    .as_ref()
                    .is_some_and(|identity| {
                        identity.run_id.trim().is_empty()
                            || identity.node_id.trim().is_empty()
                            || identity.branch_path.len() > 64
                            || identity
                                .branch_path
                                .iter()
                                .any(|branch| branch.trim().is_empty())
                            || identity.attempt == 0
                            || identity.step == 0
                            || identity.executor_id.trim().is_empty()
                            || identity.executor_version.trim().is_empty()
                            || identity.executor_declaration_hash
                                == ContentHash::from_bytes([0; 32])
                            || identity.adapter_configuration_reference
                                == ContentHash::from_bytes([0; 32])
                            || identity.execution_plan_hash == ContentHash::from_bytes([0; 32])
                            || identity.registry_hash == ContentHash::from_bytes([0; 32])
                            || identity.plugin_node_action.as_ref().is_some_and(|action| {
                                action.invocation_id.trim().is_empty()
                                    || action.validation_hash == ContentHash::from_bytes([0; 32])
                                    || action.action_hash == ContentHash::from_bytes([0; 32])
                                    || action.runtime_proposal_hash
                                        == ContentHash::from_bytes([0; 32])
                            })
                    }) =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::StyleApproval(approval)
            if approval.session_id != session_id
                || approval.workspace.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.style.trim().is_empty()
                || approval.cancellation_id.trim().is_empty()
                || approval.compiled_style_cache_key.trim().is_empty()
                || approval.node_id.trim().is_empty()
                || approval.attempt == 0
                || approval.step == 0
                || approval.request_reference.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::NativeAutomaticMemoryWriteApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.write_id, 256)
                || approval.request_hash == ContentHash::from_bytes([0; 32])
                || approval.action_digest == ContentHash::from_bytes([0; 32])
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty()
                || approval.compiled_style_cache_key.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::PluginAutomaticMemoryWriteApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.write_id, 256)
                || !valid_graph_reference(&approval.invocation_id, 256)
                || approval.invocation_digest == ContentHash::from_bytes([0; 32])
                || !valid_graph_reference(&approval.plugin_id, 256)
                || !valid_graph_reference(&approval.plugin_version, 128)
                || !valid_graph_reference(&approval.provider_id, 256)
                || !valid_graph_reference(&approval.provider_version, 128)
                || approval.declaration_hash == ContentHash::from_bytes([0; 32])
                || approval.configuration_reference == ContentHash::from_bytes([0; 32])
                || approval.request_hash == ContentHash::from_bytes([0; 32])
                || approval.action_digest == ContentHash::from_bytes([0; 32])
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty()
                || approval.compiled_style_cache_key.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::PluginContextOperationApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.invocation_id, 256)
                || approval.invocation_digest == ContentHash::from_bytes([0; 32])
                || approval.action_digest == ContentHash::from_bytes([0; 32])
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty()
                || approval.compiled_style_cache_key.trim().is_empty()
                || (approval.stage == PluginContextOperationApprovalStage::Invocation
                    && (approval.proposal_hash.is_some()
                        || approval.replacement_hash.is_some()))
                || (approval.stage == PluginContextOperationApprovalStage::Application
                    && (approval.proposal_hash.is_none()
                        || approval.replacement_hash.is_none())) =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::GraphScheduleApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.schedule_id, 256)
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::ChildMessageApproval(approval)
            if approval.session_id != session_id
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::PluginNodeInvocationApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.invocation_id, 256)
                || !valid_graph_reference(&approval.causation_event_id, 64)
                || !valid_graph_reference(&approval.proposal_event_id, 64)
                || !valid_graph_reference(&approval.plugin_id, 256)
                || !valid_graph_reference(&approval.run_id, 256)
                || !valid_graph_reference(&approval.node_id, 256)
                || approval.branch_path.len() > 64
                || approval
                    .branch_path
                    .iter()
                    .any(|branch| !valid_graph_reference(branch, 128))
                || approval.attempt == 0
                || approval.step == 0
                || !valid_graph_reference(&approval.executor_id, 256)
                || !valid_graph_reference(&approval.executor_version, 128)
                || approval.executor_declaration_hash == ContentHash::from_bytes([0; 32])
                || approval.workspace.trim().is_empty()
                || approval.style.trim().is_empty()
                || approval.prompt.trim().is_empty()
                || approval.provider.trim().is_empty()
                || approval.model.trim().is_empty()
                || !approval.options.is_object()
                || approval.cancellation_id.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::ChildGraphApproval(approval)
            if approval.session_id != session_id
                || !valid_graph_reference(&approval.run_id, 256)
                || !valid_graph_reference(&approval.node_id, 256)
                || approval.branch_path.len() > 64
                || approval
                    .branch_path
                    .iter()
                    .any(|branch| !valid_graph_reference(branch, 128))
                || approval.attempt == 0
                || approval.step == 0
                || approval.execution_plan_hash == ContentHash::from_bytes([0; 32])
                || approval.adapter_configuration_reference == ContentHash::from_bytes([0; 32])
                || approval.request_hash == ContentHash::from_bytes([0; 32])
                || approval.subject_hash == ContentHash::from_bytes([0; 32]) =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::DeferredTurn(turn)
            if turn.session_id != session_id
                || turn.schedule_id.trim().is_empty()
                || turn.prompt.trim().is_empty()
                || turn.workspace.trim().is_empty()
                || turn.provider.trim().is_empty()
                || turn.model.trim().is_empty()
                || !turn.options.is_object()
                || turn.style.trim().is_empty()
                || turn.cancellation_id.trim().is_empty() =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::GraphNodeWait(wait)
            if wait.session_id != session_id
                || !valid_graph_reference(&wait.run_id, 256)
                || wait.branch_path.len() > 64
                || wait
                    .branch_path
                    .iter()
                    .any(|branch| !valid_graph_reference(branch, 128))
                || !valid_graph_reference(&wait.node_id, 256)
                || !valid_graph_reference(&wait.executor_id, 256)
                || !valid_graph_reference(&wait.executor_version, 128)
                || !valid_executor_location(&wait.executor_source, wait.execution_boundary)
                || wait.attempt == 0
                || wait.step == 0
                || !valid_graph_reference(&wait.transition_target_node_id, 256)
                || !valid_graph_reference(&wait.compiled_transition_reference, 256)
                || !valid_graph_reference(&wait.schedule_id, 256)
                || !valid_graph_reference(&wait.cancellation_token, 256)
                || !valid_graph_reference(&wait.cancellation_reference, 256) =>
        {
            Err(ContinuationLogicError::InvalidPayload)
        }
        ContinuationPayload::Opaque(label) if label.trim().is_empty() => {
            Err(ContinuationLogicError::InvalidPayload)
        }
        _ => Ok(()),
    }
}

fn valid_graph_reference(value: &str, maximum_length: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum_length && !value.contains('\0')
}

fn valid_executor_location(
    source: &GraphNodeExecutorSource,
    boundary: GraphNodeExecutionBoundary,
) -> bool {
    match (source, boundary) {
        (GraphNodeExecutorSource::Runtime, GraphNodeExecutionBoundary::RuntimeLogic) => true,
        (GraphNodeExecutorSource::Plugin { plugin_id }, GraphNodeExecutionBoundary::PluginHost) => {
            valid_graph_reference(plugin_id, 256)
        }
        _ => false,
    }
}

fn to_data_executor_source(source: GraphNodeExecutorSource) -> GraphNodeExecutorSourceRecord {
    match source {
        GraphNodeExecutorSource::Runtime => GraphNodeExecutorSourceRecord::Runtime,
        GraphNodeExecutorSource::Plugin { plugin_id } => {
            GraphNodeExecutorSourceRecord::Plugin { plugin_id }
        }
    }
}

fn from_data_executor_source(source: GraphNodeExecutorSourceRecord) -> GraphNodeExecutorSource {
    match source {
        GraphNodeExecutorSourceRecord::Runtime => GraphNodeExecutorSource::Runtime,
        GraphNodeExecutorSourceRecord::Plugin { plugin_id } => {
            GraphNodeExecutorSource::Plugin { plugin_id }
        }
    }
}

const fn to_data_execution_boundary(
    boundary: GraphNodeExecutionBoundary,
) -> GraphNodeExecutionBoundaryRecord {
    match boundary {
        GraphNodeExecutionBoundary::RuntimeLogic => GraphNodeExecutionBoundaryRecord::RuntimeLogic,
        GraphNodeExecutionBoundary::PluginHost => GraphNodeExecutionBoundaryRecord::PluginHost,
    }
}

const fn from_data_execution_boundary(
    boundary: GraphNodeExecutionBoundaryRecord,
) -> GraphNodeExecutionBoundary {
    match boundary {
        GraphNodeExecutionBoundaryRecord::RuntimeLogic => GraphNodeExecutionBoundary::RuntimeLogic,
        GraphNodeExecutionBoundaryRecord::PluginHost => GraphNodeExecutionBoundary::PluginHost,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the explicit logic-to-data mapping keeps every continuation DTO owned by its layer"
)]
fn to_data_payload(payload: ContinuationPayload) -> ContinuationPayloadRecord {
    match payload {
        ContinuationPayload::ToolApproval(tool) => {
            ContinuationPayloadRecord::ToolApproval(Box::new(ToolApprovalPayloadRecord {
                session_id: tool.session_id,
                workspace: tool.workspace,
                call_id: tool.call_id,
                tool: tool.tool,
                arguments: tool.arguments,
                cancellation_id: tool.cancellation_id,
                provider: tool.provider,
                model: tool.model,
                options: tool.options,
                style: tool.style,
                harness_continuation: tool.harness_continuation,
                remaining_tool_calls: tool
                    .remaining_tool_calls
                    .into_iter()
                    .map(|pending| PendingToolCallPayloadRecord {
                        harness_continuation: pending.harness_continuation,
                        call_id: pending.call_id,
                        tool: pending.tool,
                        arguments: pending.arguments,
                    })
                    .collect(),
                generic_graph_identity: tool.generic_graph_identity.map(|identity| {
                    GenericToolApprovalIdentityRecord {
                        run_id: identity.run_id,
                        node_id: identity.node_id,
                        branch_path: identity.branch_path,
                        attempt: identity.attempt,
                        loop_iteration: identity.loop_iteration,
                        step: identity.step,
                        executor_id: identity.executor_id,
                        executor_version: identity.executor_version,
                        executor_declaration_hash: identity.executor_declaration_hash,
                        adapter_configuration_reference: identity.adapter_configuration_reference,
                        execution_plan_hash: identity.execution_plan_hash,
                        registry_hash: identity.registry_hash,
                        plugin_node_action: identity.plugin_node_action.map(|action| {
                            PluginNodeActionApprovalIdentityRecord {
                                invocation_id: action.invocation_id,
                                validation_hash: action.validation_hash,
                                action_index: action.action_index,
                                action_hash: action.action_hash,
                                runtime_proposal_hash: action.runtime_proposal_hash,
                            }
                        }),
                    }
                }),
            }))
        }
        ContinuationPayload::StyleApproval(approval) => {
            ContinuationPayloadRecord::StyleApproval(Box::new(StyleApprovalPayloadRecord {
                session_id: approval.session_id,
                workspace: approval.workspace,
                prompt: approval.prompt,
                provider: approval.provider,
                model: approval.model,
                options: approval.options,
                style: approval.style,
                cancellation_id: approval.cancellation_id,
                compiled_style_cache_key: approval.compiled_style_cache_key,
                node_id: approval.node_id,
                branch_path: approval.branch_path,
                attempt: approval.attempt,
                loop_iteration: approval.loop_iteration,
                step: approval.step,
                request_reference: approval.request_reference,
            }))
        }
        ContinuationPayload::NativeAutomaticMemoryWriteApproval(approval) => {
            ContinuationPayloadRecord::NativeAutomaticMemoryWriteApproval(Box::new(
                NativeAutomaticMemoryWriteApprovalPayloadRecord {
                    session_id: approval.session_id,
                    write_id: approval.write_id,
                    request_hash: approval.request_hash,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayload::PluginAutomaticMemoryWriteApproval(approval) => {
            ContinuationPayloadRecord::PluginAutomaticMemoryWriteApproval(Box::new(
                PluginAutomaticMemoryWriteApprovalPayloadRecord {
                    session_id: approval.session_id,
                    write_id: approval.write_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    plugin_id: approval.plugin_id,
                    plugin_version: approval.plugin_version,
                    provider_id: approval.provider_id,
                    provider_version: approval.provider_version,
                    declaration_hash: approval.declaration_hash,
                    configuration_reference: approval.configuration_reference,
                    request_hash: approval.request_hash,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayload::PluginContextOperationApproval(approval) => {
            ContinuationPayloadRecord::PluginContextOperationApproval(Box::new(
                PluginContextOperationApprovalPayloadRecord {
                    session_id: approval.session_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    stage: match approval.stage {
                        PluginContextOperationApprovalStage::Invocation => {
                            PluginContextOperationApprovalStageRecord::Invocation
                        }
                        PluginContextOperationApprovalStage::Application => {
                            PluginContextOperationApprovalStageRecord::Application
                        }
                    },
                    action_digest: approval.action_digest,
                    proposal_hash: approval.proposal_hash,
                    replacement_hash: approval.replacement_hash,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayload::GraphScheduleApproval(approval) => {
            ContinuationPayloadRecord::GraphScheduleApproval(Box::new(
                GraphScheduleApprovalPayloadRecord {
                    session_id: approval.session_id,
                    schedule_id: approval.schedule_id,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                },
            ))
        }
        ContinuationPayload::ChildMessageApproval(approval) => {
            ContinuationPayloadRecord::ChildMessageApproval(Box::new(
                ChildMessageApprovalPayloadRecord {
                    session_id: approval.session_id,
                    message_identity: approval.message_identity,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                },
            ))
        }
        ContinuationPayload::PluginNodeInvocationApproval(approval) => {
            ContinuationPayloadRecord::PluginNodeInvocationApproval(Box::new(
                PluginNodeInvocationApprovalPayloadRecord {
                    session_id: approval.session_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    input_hash: approval.input_hash,
                    readable_state_hash: approval.readable_state_hash,
                    causation_event_id: approval.causation_event_id,
                    proposal_event_id: approval.proposal_event_id,
                    plugin_id: approval.plugin_id,
                    run_id: approval.run_id,
                    node_id: approval.node_id,
                    branch_path: approval.branch_path,
                    attempt: approval.attempt,
                    loop_iteration: approval.loop_iteration,
                    step: approval.step,
                    executor_id: approval.executor_id,
                    executor_version: approval.executor_version,
                    executor_declaration_hash: approval.executor_declaration_hash,
                    adapter_configuration_reference: approval.adapter_configuration_reference,
                    action_digest: approval.action_digest,
                    execution_plan_hash: approval.execution_plan_hash,
                    registry_hash: approval.registry_hash,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                },
            ))
        }
        ContinuationPayload::ChildGraphApproval(approval) => {
            ContinuationPayloadRecord::ChildGraphApproval(Box::new(
                ChildGraphApprovalPayloadRecord {
                    session_id: approval.session_id,
                    operation: match approval.operation {
                        ChildGraphApprovalOperation::CreateChild => {
                            ChildGraphApprovalOperationRecord::CreateChild
                        }
                        ChildGraphApprovalOperation::CancelChildren => {
                            ChildGraphApprovalOperationRecord::CancelChildren
                        }
                        ChildGraphApprovalOperation::ReviewEvidence => {
                            ChildGraphApprovalOperationRecord::ReviewEvidence
                        }
                    },
                    run_id: approval.run_id,
                    node_id: approval.node_id,
                    branch_path: approval.branch_path,
                    attempt: approval.attempt,
                    loop_iteration: approval.loop_iteration,
                    step: approval.step,
                    execution_plan_hash: approval.execution_plan_hash,
                    adapter_configuration_reference: approval.adapter_configuration_reference,
                    request_hash: approval.request_hash,
                    subject_hash: approval.subject_hash,
                },
            ))
        }
        ContinuationPayload::DeferredTurn(turn) => {
            ContinuationPayloadRecord::DeferredTurn(Box::new(DeferredTurnPayloadRecord {
                session_id: turn.session_id,
                schedule_id: turn.schedule_id,
                prompt: turn.prompt,
                workspace: turn.workspace,
                provider: turn.provider,
                model: turn.model,
                options: turn.options,
                style: turn.style,
                cancellation_id: turn.cancellation_id,
            }))
        }
        ContinuationPayload::GraphNodeWait(wait) => {
            ContinuationPayloadRecord::GraphNodeWait(Box::new(GraphNodeWaitPayloadRecord {
                session_id: wait.session_id,
                run_id: wait.run_id,
                branch_path: wait.branch_path,
                node_id: wait.node_id,
                executor_id: wait.executor_id,
                executor_version: wait.executor_version,
                executor_source: to_data_executor_source(wait.executor_source),
                execution_boundary: to_data_execution_boundary(wait.execution_boundary),
                adapter_configuration_reference: wait.adapter_configuration_reference,
                execution_plan_hash: wait.execution_plan_hash,
                attempt: wait.attempt,
                loop_iteration: wait.loop_iteration,
                step: wait.step,
                transition_target_node_id: wait.transition_target_node_id,
                compiled_transition_reference: wait.compiled_transition_reference,
                schedule_id: wait.schedule_id,
                cancellation_token: wait.cancellation_token,
                cancellation_reference: wait.cancellation_reference,
            }))
        }
        ContinuationPayload::Opaque(label) => ContinuationPayloadRecord::Opaque { label },
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the explicit data-to-logic mapping keeps every continuation DTO owned by its layer"
)]
fn from_data_payload(payload: ContinuationPayloadRecord) -> ContinuationPayload {
    match payload {
        ContinuationPayloadRecord::ToolApproval(tool) => {
            ContinuationPayload::ToolApproval(Box::new(ToolApprovalContinuation {
                session_id: tool.session_id,
                workspace: tool.workspace,
                call_id: tool.call_id,
                tool: tool.tool,
                arguments: tool.arguments,
                cancellation_id: tool.cancellation_id,
                provider: tool.provider,
                model: tool.model,
                options: tool.options,
                style: tool.style,
                harness_continuation: tool.harness_continuation,
                remaining_tool_calls: tool
                    .remaining_tool_calls
                    .into_iter()
                    .map(|pending| PendingToolCallContinuation {
                        harness_continuation: pending.harness_continuation,
                        call_id: pending.call_id,
                        tool: pending.tool,
                        arguments: pending.arguments,
                    })
                    .collect(),
                generic_graph_identity: tool.generic_graph_identity.map(|identity| {
                    GenericToolApprovalIdentity {
                        run_id: identity.run_id,
                        node_id: identity.node_id,
                        branch_path: identity.branch_path,
                        attempt: identity.attempt,
                        loop_iteration: identity.loop_iteration,
                        step: identity.step,
                        executor_id: identity.executor_id,
                        executor_version: identity.executor_version,
                        executor_declaration_hash: identity.executor_declaration_hash,
                        adapter_configuration_reference: identity.adapter_configuration_reference,
                        execution_plan_hash: identity.execution_plan_hash,
                        registry_hash: identity.registry_hash,
                        plugin_node_action: identity.plugin_node_action.map(|action| {
                            PluginNodeActionApprovalIdentity {
                                invocation_id: action.invocation_id,
                                validation_hash: action.validation_hash,
                                action_index: action.action_index,
                                action_hash: action.action_hash,
                                runtime_proposal_hash: action.runtime_proposal_hash,
                            }
                        }),
                    }
                }),
            }))
        }
        ContinuationPayloadRecord::StyleApproval(approval) => {
            ContinuationPayload::StyleApproval(Box::new(StyleApprovalContinuation {
                session_id: approval.session_id,
                workspace: approval.workspace,
                prompt: approval.prompt,
                provider: approval.provider,
                model: approval.model,
                options: approval.options,
                style: approval.style,
                cancellation_id: approval.cancellation_id,
                compiled_style_cache_key: approval.compiled_style_cache_key,
                node_id: approval.node_id,
                branch_path: approval.branch_path,
                attempt: approval.attempt,
                loop_iteration: approval.loop_iteration,
                step: approval.step,
                request_reference: approval.request_reference,
            }))
        }
        ContinuationPayloadRecord::NativeAutomaticMemoryWriteApproval(approval) => {
            ContinuationPayload::NativeAutomaticMemoryWriteApproval(Box::new(
                NativeAutomaticMemoryWriteApprovalContinuation {
                    session_id: approval.session_id,
                    write_id: approval.write_id,
                    request_hash: approval.request_hash,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayloadRecord::PluginAutomaticMemoryWriteApproval(approval) => {
            ContinuationPayload::PluginAutomaticMemoryWriteApproval(Box::new(
                PluginAutomaticMemoryWriteApprovalContinuation {
                    session_id: approval.session_id,
                    write_id: approval.write_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    plugin_id: approval.plugin_id,
                    plugin_version: approval.plugin_version,
                    provider_id: approval.provider_id,
                    provider_version: approval.provider_version,
                    declaration_hash: approval.declaration_hash,
                    configuration_reference: approval.configuration_reference,
                    request_hash: approval.request_hash,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayloadRecord::PluginContextOperationApproval(approval) => {
            ContinuationPayload::PluginContextOperationApproval(Box::new(
                PluginContextOperationApprovalContinuation {
                    session_id: approval.session_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    stage: match approval.stage {
                        PluginContextOperationApprovalStageRecord::Invocation => {
                            PluginContextOperationApprovalStage::Invocation
                        }
                        PluginContextOperationApprovalStageRecord::Application => {
                            PluginContextOperationApprovalStage::Application
                        }
                    },
                    action_digest: approval.action_digest,
                    proposal_hash: approval.proposal_hash,
                    replacement_hash: approval.replacement_hash,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                    compiled_style_cache_key: approval.compiled_style_cache_key,
                },
            ))
        }
        ContinuationPayloadRecord::GraphScheduleApproval(approval) => {
            ContinuationPayload::GraphScheduleApproval(Box::new(
                GraphScheduleApprovalContinuation {
                    session_id: approval.session_id,
                    schedule_id: approval.schedule_id,
                    action_digest: approval.action_digest,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                },
            ))
        }
        ContinuationPayloadRecord::ChildMessageApproval(approval) => {
            ContinuationPayload::ChildMessageApproval(Box::new(ChildMessageApprovalContinuation {
                session_id: approval.session_id,
                message_identity: approval.message_identity,
                action_digest: approval.action_digest,
                workspace: approval.workspace,
                style: approval.style,
                prompt: approval.prompt,
                provider: approval.provider,
                model: approval.model,
                options: approval.options,
                cancellation_id: approval.cancellation_id,
            }))
        }
        ContinuationPayloadRecord::PluginNodeInvocationApproval(approval) => {
            ContinuationPayload::PluginNodeInvocationApproval(Box::new(
                PluginNodeInvocationApprovalContinuation {
                    session_id: approval.session_id,
                    invocation_id: approval.invocation_id,
                    invocation_digest: approval.invocation_digest,
                    input_hash: approval.input_hash,
                    readable_state_hash: approval.readable_state_hash,
                    causation_event_id: approval.causation_event_id,
                    proposal_event_id: approval.proposal_event_id,
                    plugin_id: approval.plugin_id,
                    run_id: approval.run_id,
                    node_id: approval.node_id,
                    branch_path: approval.branch_path,
                    attempt: approval.attempt,
                    loop_iteration: approval.loop_iteration,
                    step: approval.step,
                    executor_id: approval.executor_id,
                    executor_version: approval.executor_version,
                    executor_declaration_hash: approval.executor_declaration_hash,
                    adapter_configuration_reference: approval.adapter_configuration_reference,
                    action_digest: approval.action_digest,
                    execution_plan_hash: approval.execution_plan_hash,
                    registry_hash: approval.registry_hash,
                    workspace: approval.workspace,
                    style: approval.style,
                    prompt: approval.prompt,
                    provider: approval.provider,
                    model: approval.model,
                    options: approval.options,
                    cancellation_id: approval.cancellation_id,
                },
            ))
        }
        ContinuationPayloadRecord::ChildGraphApproval(approval) => {
            ContinuationPayload::ChildGraphApproval(Box::new(ChildGraphApprovalContinuation {
                session_id: approval.session_id,
                operation: match approval.operation {
                    ChildGraphApprovalOperationRecord::CreateChild => {
                        ChildGraphApprovalOperation::CreateChild
                    }
                    ChildGraphApprovalOperationRecord::CancelChildren => {
                        ChildGraphApprovalOperation::CancelChildren
                    }
                    ChildGraphApprovalOperationRecord::ReviewEvidence => {
                        ChildGraphApprovalOperation::ReviewEvidence
                    }
                },
                run_id: approval.run_id,
                node_id: approval.node_id,
                branch_path: approval.branch_path,
                attempt: approval.attempt,
                loop_iteration: approval.loop_iteration,
                step: approval.step,
                execution_plan_hash: approval.execution_plan_hash,
                adapter_configuration_reference: approval.adapter_configuration_reference,
                request_hash: approval.request_hash,
                subject_hash: approval.subject_hash,
            }))
        }
        ContinuationPayloadRecord::DeferredTurn(turn) => {
            ContinuationPayload::DeferredTurn(Box::new(DeferredTurnContinuation {
                session_id: turn.session_id,
                schedule_id: turn.schedule_id,
                prompt: turn.prompt,
                workspace: turn.workspace,
                provider: turn.provider,
                model: turn.model,
                options: turn.options,
                style: turn.style,
                cancellation_id: turn.cancellation_id,
            }))
        }
        ContinuationPayloadRecord::GraphNodeWait(wait) => {
            ContinuationPayload::GraphNodeWait(Box::new(GraphNodeWaitContinuation {
                session_id: wait.session_id,
                run_id: wait.run_id,
                branch_path: wait.branch_path,
                node_id: wait.node_id,
                executor_id: wait.executor_id,
                executor_version: wait.executor_version,
                executor_source: from_data_executor_source(wait.executor_source),
                execution_boundary: from_data_execution_boundary(wait.execution_boundary),
                adapter_configuration_reference: wait.adapter_configuration_reference,
                execution_plan_hash: wait.execution_plan_hash,
                attempt: wait.attempt,
                loop_iteration: wait.loop_iteration,
                step: wait.step,
                transition_target_node_id: wait.transition_target_node_id,
                compiled_transition_reference: wait.compiled_transition_reference,
                schedule_id: wait.schedule_id,
                cancellation_token: wait.cancellation_token,
                cancellation_reference: wait.cancellation_reference,
            }))
        }
        ContinuationPayloadRecord::Opaque { label } => ContinuationPayload::Opaque(label),
    }
}

/// Continuation business failure.
#[derive(Debug, Error)]
pub enum ContinuationLogicError {
    /// Session scope is missing.
    #[error("continuation session is invalid")]
    InvalidSession,
    /// Wake condition cannot be evaluated safely.
    #[error("continuation wake condition is invalid")]
    InvalidWakeCondition,
    /// Pending action cannot be reconstructed safely.
    #[error("continuation payload is invalid")]
    InvalidPayload,
    /// Data returned a state inconsistent with a completed resolution.
    #[error("continuation resolution returned an invalid state")]
    InvalidResolutionState,
    /// Scheduler proof does not match the stored wake condition and schedule.
    #[error("continuation wake proof is invalid")]
    InvalidWakeProof,
    /// Continuation expired before the wake proof.
    #[error("continuation expired before wakeup")]
    Expired,
    /// Expiration cannot precede a time wake or the Unix epoch.
    #[error("continuation expiration is invalid")]
    InvalidExpiration,
    /// Cancellation token is empty or does not match the retained graph wait.
    #[error("graph continuation cancellation token is invalid")]
    InvalidCancellationToken,
    /// The data adapter cannot perform bounded cancellation lookup.
    #[error("graph continuation cancellation lookup is unsupported")]
    UnsupportedCancellationLookup,
    /// Continuation dataset failed.
    #[error("continuation data failed: {0}")]
    Data(#[source] ContinuationDataError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use uuid::Uuid;

    use agentmod_runtime_data::continuation::{
        ResolveContinuationDataRecord, TransitionContinuationTerminalDataRecord,
    };

    use super::*;

    struct MockData {
        creates: RefCell<Vec<CreateContinuationDataRequest>>,
        resolutions: RefCell<Vec<ResolveContinuationDataRequest>>,
        state: ContinuationStateRecord,
        transitioned: bool,
    }

    impl ContinuationDataPort for MockData {
        fn create(
            &self,
            request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            self.creates.borrow_mut().push(request);
            Ok(())
        }

        fn load(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: ContinuationStateRecord::Pending,
                wake_condition: ContinuationWakeRecord::Manual,
                payload: ContinuationPayloadRecord::Opaque {
                    label: "fixture".into(),
                },
                expires_at_millis: None,
            })
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            self.resolutions.borrow_mut().push(request);
            Ok(ResolveContinuationDataRecord {
                transitioned: self.transitioned,
                state: self.state,
                payload: ContinuationPayloadRecord::Opaque {
                    label: "fixture".into(),
                },
            })
        }
    }

    fn id() -> ContinuationId {
        ContinuationId::from_uuid(Uuid::from_u128(1))
    }

    fn deferred_payload() -> ContinuationPayloadRecord {
        ContinuationPayloadRecord::DeferredTurn(Box::new(DeferredTurnPayloadRecord {
            session_id: "session_1".into(),
            schedule_id: "schedule_1".into(),
            prompt: "continue".into(),
            workspace: "workspace".into(),
            provider: "mock".into(),
            model: "mock-model".into(),
            options: serde_json::json!({}),
            style: "persistent-chat".into(),
            cancellation_id: Uuid::from_u128(2).to_string(),
        }))
    }

    fn graph_wait_payload() -> ContinuationPayloadRecord {
        ContinuationPayloadRecord::GraphNodeWait(Box::new(GraphNodeWaitPayloadRecord {
            session_id: "session_1".into(),
            run_id: "run_immutable_1".into(),
            branch_path: vec!["root".into(), "branch_a".into()],
            node_id: "wait_for_schedule".into(),
            executor_id: "runtime.delay".into(),
            executor_version: "1.0.0".into(),
            executor_source: GraphNodeExecutorSourceRecord::Runtime,
            execution_boundary: GraphNodeExecutionBoundaryRecord::RuntimeLogic,
            adapter_configuration_reference: ContentHash::digest(b"delay-config"),
            execution_plan_hash: ContentHash::digest(b"execution-plan"),
            attempt: 2,
            loop_iteration: 3,
            step: 4,
            transition_target_node_id: "after_wait".into(),
            compiled_transition_reference: "transition_hash_1".into(),
            schedule_id: "schedule_1".into(),
            cancellation_token: "cancel_token_1".into(),
            cancellation_reference: "cancel_reference_1".into(),
        }))
    }

    fn tool_approval_payload(
        generic_graph_identity: Option<GenericToolApprovalIdentity>,
    ) -> ContinuationPayload {
        ContinuationPayload::ToolApproval(Box::new(ToolApprovalContinuation {
            session_id: String::from("session_1"),
            workspace: String::from("workspace"),
            call_id: String::from("parallel-call"),
            tool: String::from("filesystem.read"),
            arguments: serde_json::json!({"path":"README.md"}),
            cancellation_id: String::from("cancel_1"),
            provider: String::from("mock"),
            model: String::from("mock-model"),
            options: serde_json::json!({}),
            style: String::from("user-graph"),
            harness_continuation: String::from("style-owned"),
            remaining_tool_calls: Vec::new(),
            generic_graph_identity,
        }))
    }

    #[test]
    fn tool_approval_mapping_preserves_exact_parallel_graph_identity() {
        let identity = GenericToolApprovalIdentity {
            run_id: String::from("run_1"),
            node_id: String::from("branch_tool"),
            branch_path: vec![String::from("branch_a")],
            attempt: 2,
            loop_iteration: 3,
            step: 4,
            executor_id: String::from("runtime.tool_execution"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: ContentHash::digest(b"declaration"),
            adapter_configuration_reference: ContentHash::digest(b"configuration"),
            execution_plan_hash: ContentHash::digest(b"plan"),
            registry_hash: ContentHash::digest(b"registry"),
            plugin_node_action: None,
        };
        let payload = tool_approval_payload(Some(identity));
        let mapped = from_data_payload(to_data_payload(payload.clone()));
        assert_eq!(mapped, payload);
        validate_payload("session_1", &mapped).expect("valid exact branch tool approval");
    }

    #[test]
    fn tool_approval_mapping_preserves_exact_plugin_action_identity() {
        let identity = GenericToolApprovalIdentity {
            run_id: String::from("run_plugin"),
            node_id: String::from("plugin_node"),
            branch_path: Vec::new(),
            attempt: 1,
            loop_iteration: 0,
            step: 2,
            executor_id: String::from("fixture.plugin-executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: ContentHash::digest(b"plugin declaration"),
            adapter_configuration_reference: ContentHash::digest(b"plugin configuration"),
            execution_plan_hash: ContentHash::digest(b"plugin plan"),
            registry_hash: ContentHash::digest(b"plugin registry"),
            plugin_node_action: Some(PluginNodeActionApprovalIdentity {
                invocation_id: String::from("plugin-node:invocation"),
                validation_hash: ContentHash::digest(b"validation"),
                action_index: 3,
                action_hash: ContentHash::digest(b"plugin action"),
                runtime_proposal_hash: ContentHash::digest(b"runtime proposal"),
            }),
        };
        let payload = tool_approval_payload(Some(identity));
        let mapped = from_data_payload(to_data_payload(payload.clone()));
        assert_eq!(mapped, payload);
        validate_payload("session_1", &mapped).expect("valid exact plugin action approval");
    }

    #[test]
    fn legacy_tool_approval_record_defaults_generic_graph_identity_to_none() {
        let record = ToolApprovalPayloadRecord {
            session_id: String::from("session_1"),
            workspace: String::from("workspace"),
            call_id: String::from("call_1"),
            tool: String::from("filesystem.read"),
            arguments: serde_json::json!({"path":"README.md"}),
            cancellation_id: String::from("cancel_1"),
            provider: String::from("mock"),
            model: String::from("mock-model"),
            options: serde_json::json!({}),
            style: String::from("persistent-chat"),
            harness_continuation: String::from("continue_1"),
            remaining_tool_calls: Vec::new(),
            generic_graph_identity: None,
        };
        let mut encoded = serde_json::to_value(record).expect("serialize tool approval");
        encoded
            .as_object_mut()
            .expect("tool approval object")
            .remove("generic_graph_identity");
        let decoded: ToolApprovalPayloadRecord =
            serde_json::from_value(encoded).expect("deserialize legacy tool approval");
        assert!(decoded.generic_graph_identity.is_none());
        let payload = from_data_payload(ContinuationPayloadRecord::ToolApproval(Box::new(decoded)));
        let ContinuationPayload::ToolApproval(payload) = payload else {
            panic!("tool approval")
        };
        assert_eq!(payload.call_id, "call_1");
        assert_eq!(payload.style, "persistent-chat");
        assert!(payload.generic_graph_identity.is_none());
    }

    #[test]
    fn plugin_node_approval_mapping_preserves_branch_and_receipt_identity() {
        let payload = ContinuationPayload::PluginNodeInvocationApproval(Box::new(
            PluginNodeInvocationApprovalContinuation {
                session_id: String::from("session_1"),
                invocation_id: String::from("plugin-node:invocation_1"),
                invocation_digest: ContentHash::digest(b"invocation"),
                input_hash: ContentHash::digest(b"input"),
                readable_state_hash: ContentHash::digest(b"readable"),
                causation_event_id: Uuid::from_u128(2).to_string(),
                proposal_event_id: Uuid::from_u128(3).to_string(),
                plugin_id: String::from("fixture.plugin"),
                run_id: String::from("run_1"),
                node_id: String::from("plugin_node"),
                branch_path: vec![String::from("fanout"), String::from("member_a")],
                attempt: 2,
                loop_iteration: 3,
                step: 4,
                executor_id: String::from("fixture.executor"),
                executor_version: String::from("1.0.0"),
                executor_declaration_hash: ContentHash::digest(b"declaration"),
                adapter_configuration_reference: ContentHash::digest(b"configuration"),
                action_digest: ContentHash::digest(b"action"),
                execution_plan_hash: ContentHash::digest(b"plan"),
                registry_hash: ContentHash::digest(b"registry"),
                workspace: String::from("workspace"),
                style: String::from("user-graph"),
                prompt: String::from("continue"),
                provider: String::from("mock"),
                model: String::from("mock-model"),
                options: serde_json::json!({"bounded": true}),
                cancellation_id: String::from("cancel_1"),
            },
        ));
        let mapped = from_data_payload(to_data_payload(payload.clone()));
        assert_eq!(mapped, payload);
        validate_payload("session_1", &mapped).expect("valid exact plugin approval");
    }

    #[test]
    fn plugin_automatic_memory_approval_mapping_preserves_exact_invocation_and_resume() {
        let payload = ContinuationPayload::PluginAutomaticMemoryWriteApproval(Box::new(
            PluginAutomaticMemoryWriteApprovalContinuation {
                session_id: String::from("session_1"),
                write_id: String::from("automatic-memory-write:fixture"),
                invocation_id: String::from("plugin-automatic-memory:fixture"),
                invocation_digest: ContentHash::digest(b"invocation"),
                plugin_id: String::from("fixture.plugin"),
                plugin_version: String::from("2.0.0"),
                provider_id: String::from("fixture.memory"),
                provider_version: String::from("1.0.0"),
                declaration_hash: ContentHash::digest(b"declaration"),
                configuration_reference: ContentHash::digest(b"configuration"),
                request_hash: ContentHash::digest(b"request"),
                action_digest: ContentHash::digest(b"action"),
                workspace: String::from("workspace"),
                style: String::from("persistent-chat"),
                prompt: String::from("remember this"),
                provider: String::from("mock"),
                model: String::from("mock-model"),
                options: serde_json::json!({"bounded": true}),
                cancellation_id: String::from("run_1"),
                compiled_style_cache_key: String::from("cache_1"),
            },
        ));
        let mapped = from_data_payload(to_data_payload(payload.clone()));
        assert_eq!(mapped, payload);
        validate_payload("session_1", &mapped)
            .expect("valid exact plugin automatic-memory approval");
        let encoded = serde_json::to_value(to_data_payload(payload)).expect("plugin payload JSON");
        assert_eq!(
            encoded["kind"],
            serde_json::json!("plugin_automatic_memory_write_approval")
        );
    }

    #[test]
    fn native_automatic_memory_approval_mapping_is_distinct_and_exact() {
        let payload = ContinuationPayload::NativeAutomaticMemoryWriteApproval(Box::new(
            NativeAutomaticMemoryWriteApprovalContinuation {
                session_id: String::from("session_1"),
                write_id: String::from("automatic-memory-write:native-fixture"),
                request_hash: ContentHash::digest(b"request"),
                action_digest: ContentHash::digest(b"action"),
                workspace: String::from("workspace"),
                style: String::from("persistent-chat"),
                prompt: String::from("remember this"),
                provider: String::from("mock"),
                model: String::from("mock-model"),
                options: serde_json::json!({"bounded": true}),
                cancellation_id: String::from("run_1"),
                compiled_style_cache_key: String::from("cache_1"),
            },
        ));
        let data = to_data_payload(payload.clone());
        let encoded = serde_json::to_value(&data).expect("native payload JSON");
        assert_eq!(
            encoded["kind"],
            serde_json::json!("native_automatic_memory_write_approval")
        );
        let mapped = from_data_payload(data);
        assert_eq!(mapped, payload);
        validate_payload("session_1", &mapped)
            .expect("valid exact native automatic-memory approval");
    }

    #[test]
    fn style_approval_mapping_preserves_root_and_nested_branch_identity() {
        for branch_path in [Vec::new(), vec!["fanout".into(), "review".into()]] {
            let payload = ContinuationPayload::StyleApproval(Box::new(StyleApprovalContinuation {
                session_id: "session_1".into(),
                workspace: "workspace".into(),
                prompt: "continue".into(),
                provider: "mock".into(),
                model: "mock-model".into(),
                options: serde_json::json!({"bounded": true}),
                style: "user-graph".into(),
                cancellation_id: "cancel_1".into(),
                compiled_style_cache_key: "cache_1".into(),
                node_id: "approval".into(),
                branch_path,
                attempt: 1,
                loop_iteration: 2,
                step: 7,
                request_reference: "request_1".into(),
            }));
            assert_eq!(from_data_payload(to_data_payload(payload.clone())), payload);
        }
    }

    struct WakeMockData {
        resolutions: RefCell<Vec<ResolveContinuationDataRequest>>,
        wake_condition: ContinuationWakeRecord,
        payload: ContinuationPayloadRecord,
        expires_at_millis: Option<i64>,
        state: ContinuationStateRecord,
        transitioned: bool,
    }

    impl ContinuationDataPort for WakeMockData {
        fn create(
            &self,
            _request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            unreachable!("wake tests never create")
        }

        fn load(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: self.state,
                wake_condition: self.wake_condition.clone(),
                payload: self.payload.clone(),
                expires_at_millis: self.expires_at_millis,
            })
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            self.resolutions.borrow_mut().push(request);
            Ok(ResolveContinuationDataRecord {
                transitioned: self.transitioned,
                state: ContinuationStateRecord::Resumed,
                payload: self.payload.clone(),
            })
        }

        fn transition_terminal(
            &self,
            request: TransitionContinuationTerminalDataRequest,
        ) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
            Ok(TransitionContinuationTerminalDataRecord {
                transitioned: true,
                state: request.target,
                payload: self.payload.clone(),
            })
        }
    }

    struct SubstitutingWakeMockData {
        resolutions: RefCell<Vec<ResolveContinuationDataRequest>>,
        loaded_payload: ContinuationPayloadRecord,
        resolved_payload: ContinuationPayloadRecord,
    }

    impl ContinuationDataPort for SubstitutingWakeMockData {
        fn create(
            &self,
            _request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            unreachable!("substitution tests never create")
        }

        fn load(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: ContinuationStateRecord::Pending,
                wake_condition: ContinuationWakeRecord::At(100),
                payload: self.loaded_payload.clone(),
                expires_at_millis: None,
            })
        }

        fn resolve(
            &self,
            request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            self.resolutions.borrow_mut().push(request);
            Ok(ResolveContinuationDataRecord {
                transitioned: true,
                state: ContinuationStateRecord::Resumed,
                payload: self.resolved_payload.clone(),
            })
        }
    }

    struct RecordingTerminalData {
        transitions: RefCell<Vec<TransitionContinuationTerminalDataRequest>>,
        payload: ContinuationPayloadRecord,
        transitioned: bool,
    }

    impl ContinuationDataPort for RecordingTerminalData {
        fn create(
            &self,
            _request: CreateContinuationDataRequest,
        ) -> Result<(), ContinuationDataError> {
            unreachable!("terminal tests never create")
        }

        fn load(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<ContinuationRecord, ContinuationDataError> {
            Ok(ContinuationRecord {
                session_id: session_id.to_owned(),
                id: id.to_owned(),
                state: ContinuationStateRecord::Pending,
                wake_condition: ContinuationWakeRecord::At(100),
                payload: self.payload.clone(),
                expires_at_millis: Some(99),
            })
        }

        fn resolve(
            &self,
            _request: ResolveContinuationDataRequest,
        ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
            unreachable!("terminal tests never resume")
        }

        fn transition_terminal(
            &self,
            request: TransitionContinuationTerminalDataRequest,
        ) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
            self.transitions.borrow_mut().push(request.clone());
            Ok(TransitionContinuationTerminalDataRecord {
                transitioned: self.transitioned,
                state: request.target,
                payload: self.payload.clone(),
            })
        }
    }

    #[test]
    fn creates_pending_data_record() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Pending,
            transitioned: false,
        });
        logic
            .create_continuation(CreateContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                wake_condition: ContinuationWakeCondition::Manual,
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: None,
            })
            .expect("create");
        let observed = logic.data.creates.into_inner();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].record.state, ContinuationStateRecord::Pending);
        assert_eq!(observed[0].record.id, id().to_string());
    }

    #[test]
    fn returns_transition_winner_for_approved_resolution() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Resumed,
            transitioned: true,
        });
        assert_eq!(
            logic
                .resolve_approval(ResolveApprovalCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    approved: true,
                })
                .expect("resolve"),
            ResolveApprovalResult {
                transitioned: true,
                disposition: ApprovalDisposition::Approved,
                payload: ContinuationPayload::Opaque("fixture".into()),
            }
        );
        assert_eq!(
            logic.data.resolutions.into_inner(),
            vec![ResolveContinuationDataRequest {
                session_id: "session_1".into(),
                id: id().to_string(),
                approved: true,
            }]
        );
    }

    #[test]
    fn terminal_api_maps_cancelled_and_expired_and_validates_payload() {
        for (disposition, expected) in [
            (
                ContinuationTerminalDisposition::Cancelled,
                ContinuationTerminalStateRecord::Cancelled,
            ),
            (
                ContinuationTerminalDisposition::Expired,
                ContinuationTerminalStateRecord::Expired,
            ),
        ] {
            let logic = ContinuationLogic::new(RecordingTerminalData {
                transitions: RefCell::new(Vec::new()),
                payload: graph_wait_payload(),
                transitioned: false,
            });
            let result = logic
                .transition_terminal(TransitionContinuationTerminalCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    disposition,
                })
                .expect("terminal transition");
            assert!(!result.transitioned);
            assert_eq!(result.disposition, disposition);
            assert_eq!(result.payload, from_data_payload(graph_wait_payload()));
            assert_eq!(
                logic.data.transitions.borrow().as_slice(),
                &[TransitionContinuationTerminalDataRequest {
                    session_id: "session_1".into(),
                    id: id().to_string(),
                    target: expected,
                }]
            );
        }
    }

    #[test]
    fn rejects_unsafe_empty_wake_selectors_before_data() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Pending,
            transitioned: false,
        });
        assert!(matches!(
            logic.create_continuation(CreateContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                wake_condition: ContinuationWakeCondition::ProcessOutput {
                    process_id: String::new(),
                    pattern: "ready".into(),
                },
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: None,
            }),
            Err(ContinuationLogicError::InvalidWakeCondition)
        ));
        assert!(logic.data.creates.borrow().is_empty());
    }

    #[test]
    fn valid_schedule_proof_wakes_deferred_turn_once() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::RuntimeEvent {
                event_type: "task.ready".into(),
                selector: None,
            },
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        let result = logic
            .wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::RuntimeEvent {
                    event_id: "event_1".into(),
                    event_type: "task.ready".into(),
                    observed_at: TimestampMillis::new(100),
                },
            })
            .expect("wake");
        assert!(result.transitioned);
        assert_eq!(result.payload.prompt, "continue");
        assert_eq!(logic.data.resolutions.borrow().len(), 1);
    }

    #[test]
    fn rejects_schedule_or_trigger_mismatch_without_transition() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::ProcessOutput {
                process_id: "process_1".into(),
                pattern: "ready".into(),
            },
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        for (schedule_id, pattern) in [("schedule_other", "ready"), ("schedule_1", "different")] {
            assert!(matches!(
                logic.wake_continuation(WakeContinuationCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    schedule_id: schedule_id.into(),
                    proof: ContinuationWakeProof::ProcessOutput {
                        output_id: "output_1".into(),
                        process_id: "process_1".into(),
                        pattern: pattern.into(),
                        observed_at: TimestampMillis::new(100),
                    },
                }),
                Err(ContinuationLogicError::InvalidWakeProof)
            ));
        }
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn resumed_continuation_is_an_idempotent_noop() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Resumed,
            transitioned: false,
        });
        let result = logic
            .wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
            })
            .expect("duplicate wake");
        assert!(!result.transitioned);
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn expired_time_proof_fails_before_transition() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: Some(199),
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        assert!(matches!(
            logic.wake_continuation(WakeContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(200)),
            }),
            Err(ContinuationLogicError::Expired)
        ));
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn graph_wake_returns_exact_resume_only_to_the_winning_claim() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: graph_wait_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        let result = logic
            .wake_graph_node(WakeGraphNodeCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
            })
            .expect("wake graph node");
        assert!(result.transitioned);
        let resume = result.resume.expect("winner resumes graph");
        assert_eq!(resume.run_id, "run_immutable_1");
        assert_eq!(resume.branch_path, ["root", "branch_a"]);
        assert_eq!(resume.node_id, "wait_for_schedule");
        assert_eq!(resume.executor_id, "runtime.delay");
        assert_eq!(resume.executor_version, "1.0.0");
        assert_eq!(resume.executor_source, GraphNodeExecutorSource::Runtime);
        assert_eq!(
            resume.execution_boundary,
            GraphNodeExecutionBoundary::RuntimeLogic
        );
        assert_eq!(
            resume.adapter_configuration_reference,
            ContentHash::digest(b"delay-config")
        );
        assert_eq!(
            resume.execution_plan_hash,
            ContentHash::digest(b"execution-plan")
        );
        assert_eq!(resume.transition_target_node_id, "after_wait");
        assert_eq!(resume.compiled_transition_reference, "transition_hash_1");
        assert_eq!(logic.data.resolutions.borrow().len(), 1);
    }

    #[test]
    fn duplicate_graph_wake_is_an_idempotent_noop() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: graph_wait_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Resumed,
            transitioned: false,
        });
        let result = logic
            .wake_graph_node(WakeGraphNodeCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
            })
            .expect("duplicate wake");
        assert!(!result.transitioned);
        assert!(result.resume.is_none());
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn graph_wake_rejects_schedule_mismatch_expiry_and_cancellation() {
        for (schedule_id, expiry, state, expected) in [
            (
                "other_schedule",
                None,
                ContinuationStateRecord::Pending,
                "schedule",
            ),
            (
                "schedule_1",
                Some(99),
                ContinuationStateRecord::Pending,
                "expiry",
            ),
            (
                "schedule_1",
                None,
                ContinuationStateRecord::Cancelled,
                "cancellation",
            ),
        ] {
            let logic = ContinuationLogic::new(WakeMockData {
                resolutions: RefCell::new(Vec::new()),
                wake_condition: ContinuationWakeRecord::At(100),
                payload: graph_wait_payload(),
                expires_at_millis: expiry,
                state,
                transitioned: true,
            });
            let error = logic
                .wake_graph_node(WakeGraphNodeCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    schedule_id: schedule_id.into(),
                    proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
                })
                .expect_err(expected);
            assert!(matches!(
                error,
                ContinuationLogicError::InvalidWakeProof
                    | ContinuationLogicError::Expired
                    | ContinuationLogicError::InvalidResolutionState
            ));
            assert!(logic.data.resolutions.borrow().is_empty());
        }
    }

    #[test]
    fn expired_graph_wake_durably_transitions_before_returning_expired() {
        let logic = ContinuationLogic::new(RecordingTerminalData {
            transitions: RefCell::new(Vec::new()),
            payload: graph_wait_payload(),
            transitioned: true,
        });
        assert!(matches!(
            logic.wake_graph_node(WakeGraphNodeCommand {
                session_id: "session_1".into(),
                id: id(),
                schedule_id: "schedule_1".into(),
                proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
            }),
            Err(ContinuationLogicError::Expired)
        ));
        assert_eq!(
            logic.data.transitions.borrow().as_slice(),
            &[TransitionContinuationTerminalDataRequest {
                session_id: "session_1".into(),
                id: id().to_string(),
                target: ContinuationTerminalStateRecord::Expired,
            }]
        );
    }

    #[test]
    fn graph_wake_rejects_payload_substitution_after_winning_cas() {
        let original = graph_wait_payload();
        let mut substitutions = Vec::new();

        let mut changed_transition = original.clone();
        let ContinuationPayloadRecord::GraphNodeWait(wait) = &mut changed_transition else {
            unreachable!("graph fixture")
        };
        wait.transition_target_node_id = "attacker_transition".into();
        substitutions.push(changed_transition);

        let mut changed_executor = original.clone();
        let ContinuationPayloadRecord::GraphNodeWait(wait) = &mut changed_executor else {
            unreachable!("graph fixture")
        };
        wait.executor_id = "runtime.schedule".into();
        wait.executor_version = "9.9.9".into();
        wait.executor_source = GraphNodeExecutorSourceRecord::Plugin {
            plugin_id: "substituted_plugin".into(),
        };
        wait.execution_boundary = GraphNodeExecutionBoundaryRecord::PluginHost;
        substitutions.push(changed_executor);

        let mut changed_adapter = original.clone();
        let ContinuationPayloadRecord::GraphNodeWait(wait) = &mut changed_adapter else {
            unreachable!("graph fixture")
        };
        wait.adapter_configuration_reference = ContentHash::digest(b"substituted-config");
        substitutions.push(changed_adapter);

        let mut changed_plan = original.clone();
        let ContinuationPayloadRecord::GraphNodeWait(wait) = &mut changed_plan else {
            unreachable!("graph fixture")
        };
        wait.execution_plan_hash = ContentHash::digest(b"substituted-plan");
        substitutions.push(changed_plan);

        for resolved_payload in substitutions {
            let logic = ContinuationLogic::new(SubstitutingWakeMockData {
                resolutions: RefCell::new(Vec::new()),
                loaded_payload: original.clone(),
                resolved_payload,
            });
            assert!(matches!(
                logic.wake_graph_node(WakeGraphNodeCommand {
                    session_id: "session_1".into(),
                    id: id(),
                    schedule_id: "schedule_1".into(),
                    proof: ContinuationWakeProof::At(TimestampMillis::new(100)),
                }),
                Err(ContinuationLogicError::InvalidPayload)
            ));
            assert_eq!(logic.data.resolutions.borrow().len(), 1);
        }
    }

    #[test]
    fn manual_approval_cannot_resolve_scheduler_continuation() {
        let logic = ContinuationLogic::new(WakeMockData {
            resolutions: RefCell::new(Vec::new()),
            wake_condition: ContinuationWakeRecord::At(100),
            payload: deferred_payload(),
            expires_at_millis: None,
            state: ContinuationStateRecord::Pending,
            transitioned: true,
        });
        assert!(matches!(
            logic.resolve_approval(ResolveApprovalCommand {
                session_id: "session_1".into(),
                id: id(),
                approved: true,
            }),
            Err(ContinuationLogicError::InvalidWakeProof)
        ));
        assert!(logic.data.resolutions.borrow().is_empty());
    }

    #[test]
    fn rejects_expiration_before_time_wake() {
        let logic = ContinuationLogic::new(MockData {
            creates: RefCell::new(Vec::new()),
            resolutions: RefCell::new(Vec::new()),
            state: ContinuationStateRecord::Pending,
            transitioned: false,
        });
        assert!(matches!(
            logic.create_continuation(CreateContinuationCommand {
                session_id: "session_1".into(),
                id: id(),
                wake_condition: ContinuationWakeCondition::At(TimestampMillis::new(200)),
                payload: ContinuationPayload::Opaque("fixture".into()),
                expires_at: Some(TimestampMillis::new(199)),
            }),
            Err(ContinuationLogicError::InvalidExpiration)
        ));
        assert!(logic.data.creates.borrow().is_empty());
    }
}
