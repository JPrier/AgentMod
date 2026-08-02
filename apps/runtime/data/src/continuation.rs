//! Business-facing durable continuation datasets.

use std::path::PathBuf;

use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::continuation::{
    ContinuationDependencyError, ContinuationDependencyPort, DependencyContinuationRecord,
    DependencyContinuationState, DependencyCreateContinuationRequest,
    DependencyTransitionContinuationRequest, FileContinuationDependency,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Data-owned wake condition record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContinuationWakeRecord {
    /// Explicit resolution.
    Manual,
    /// Time threshold in Unix milliseconds.
    At(i64),
    /// Matching committed runtime event.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Optional constrained selector.
        selector: Option<String>,
    },
    /// Matching output from a supervised process.
    ProcessOutput {
        /// Runtime process identifier.
        process_id: String,
        /// Literal or constrained pattern.
        pattern: String,
    },
}

/// Data-owned continuation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationStateRecord {
    /// Awaiting resolution.
    Pending,
    /// Approved and claimed.
    Resumed,
    /// Denied.
    Cancelled,
    /// Expired.
    Expired,
}

/// Data-owned pending action reconstructed only after a durable decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContinuationPayloadRecord {
    /// An intercepted provider tool call waiting for user approval.
    ToolApproval(Box<ToolApprovalPayloadRecord>),
    /// A compiled style `user_approval` node waiting for resolution.
    StyleApproval(Box<StyleApprovalPayloadRecord>),
    /// An exact runtime-owned automatic-memory write waiting for approval.
    NativeAutomaticMemoryWriteApproval(Box<NativeAutomaticMemoryWriteApprovalPayloadRecord>),
    /// An exact plugin automatic-memory write waiting for user approval.
    PluginAutomaticMemoryWriteApproval(Box<PluginAutomaticMemoryWriteApprovalPayloadRecord>),
    /// An exact plugin memory-retrieve/compaction stage waiting for approval.
    PluginContextOperationApproval(Box<PluginContextOperationApprovalPayloadRecord>),
    /// A graph `schedule` creation waiting for runtime policy approval.
    GraphScheduleApproval(Box<GraphScheduleApprovalPayloadRecord>),
    /// A generic child-message delivery waiting for runtime policy approval.
    ChildMessageApproval(Box<ChildMessageApprovalPayloadRecord>),
    /// An exact plugin-host node invocation waiting for runtime policy approval.
    PluginNodeInvocationApproval(Box<PluginNodeInvocationApprovalPayloadRecord>),
    /// Exact generic child-graph ancillary action waiting for resolution.
    ChildGraphApproval(Box<ChildGraphApprovalPayloadRecord>),
    /// A complete provider turn deferred until an authenticated scheduler claim.
    DeferredTurn(Box<DeferredTurnPayloadRecord>),
    /// A graph node waiting for an authenticated scheduler-owned wake.
    ///
    /// This remains deliberately separate from a deferred provider turn: waking
    /// it resumes an already-initialized graph run and never synthesizes a new
    /// user prompt.
    GraphNodeWait(Box<GraphNodeWaitPayloadRecord>),
    /// Generic fixture payload used by storage-only callers.
    Opaque {
        /// Stable non-secret label.
        label: String,
    },
}

/// Data-owned plugin context-operation approval stage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContextOperationApprovalStageRecord {
    /// Authorization before the isolated plugin invocation.
    Invocation,
    /// Authorization before applying the validated replacement.
    Application,
}

/// Data-owned restart-safe plugin context-operation approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginContextOperationApprovalPayloadRecord {
    /// Canonical owning session.
    pub session_id: String,
    /// Digest-derived plugin operation identity.
    pub invocation_id: String,
    /// Hash of every immutable invocation field.
    pub invocation_digest: ContentHash,
    /// Exact approval stage.
    pub stage: PluginContextOperationApprovalStageRecord,
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
    pub options: serde_json::Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Data-owned child-graph ancillary operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildGraphApprovalOperationRecord {
    /// Authorize one exact child creation proposal.
    CreateChild,
    /// Authorize cancellation of an exact canonical child set.
    CancelChildren,
    /// Accept one exact reviewer routing evidence record.
    ReviewEvidence,
}

/// Data-owned restart-safe child-graph ancillary approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildGraphApprovalPayloadRecord {
    /// Canonical owning session.
    pub session_id: String,
    /// Exact ancillary operation.
    pub operation: ChildGraphApprovalOperationRecord,
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

/// Data-owned restart-safe child-message approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned restart-safe exact plugin-node invocation approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginNodeInvocationApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned restart-safe graph schedule approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraphScheduleApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Stable graph cancellation identity.
    pub cancellation_id: String,
}

/// Data-owned restart-safe compiled-style approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the graph execution.
    pub cancellation_id: String,
    /// Exact compiled-style cache key selected by the session.
    pub compiled_style_cache_key: String,
    /// Active graph node requesting the decision.
    pub node_id: String,
    /// Stable nested branch path. Empty identifies a root graph node.
    #[serde(default)]
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

/// Data-owned restart-safe native automatic-memory approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeAutomaticMemoryWriteApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Data-owned restart-safe plugin automatic-memory approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginAutomaticMemoryWriteApprovalPayloadRecord {
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
    pub options: serde_json::Value,
    /// Stable graph-run cancellation identity.
    pub cancellation_id: String,
    /// Exact immutable compiled-style cache key.
    pub compiled_style_cache_key: String,
}

/// Data-owned restart-safe deferred provider turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeferredTurnPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Schedule allowed to claim this continuation.
    pub schedule_id: String,
    /// User-authored prompt.
    pub prompt: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Provider selected for the resumed turn.
    pub provider: String,
    /// Model selected for the resumed turn.
    pub model: String,
    /// Provider-specific options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Stable cancellation identity for the deferred turn.
    pub cancellation_id: String,
}

/// Data-owned, restart-safe graph-node wait payload.
///
/// Every field is part of the immutable continuation contract.  The runtime
/// must compare it with the recovered graph state before it performs the
/// stored transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraphNodeWaitPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Immutable graph-run identity, distinct from a user turn.
    pub run_id: String,
    /// Stable nested branch path from the graph root.
    pub branch_path: Vec<String>,
    /// Node that created the wait.
    pub node_id: String,
    /// Exact persisted node executor identity.
    pub executor_id: String,
    /// Exact persisted node executor version.
    pub executor_version: String,
    /// Exact executor source, including plugin identity when applicable.
    pub executor_source: GraphNodeExecutorSourceRecord,
    /// Exact process boundary selected by the immutable execution plan.
    pub execution_boundary: GraphNodeExecutionBoundaryRecord,
    /// Hash/reference of the compiled adapter configuration consumed by the executor.
    pub adapter_configuration_reference: ContentHash,
    /// Hash of the complete immutable execution plan owning this resolution.
    pub execution_plan_hash: ContentHash,
    /// One-based attempt number for this node.
    pub attempt: u32,
    /// Zero-based bounded loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step at which the wait was created.
    pub step: u64,
    /// Exact compiled transition target selected before waiting.
    pub transition_target_node_id: String,
    /// Canonical reference/hash for that compiled transition.
    pub compiled_transition_reference: String,
    /// Exact schedule allowed to wake this continuation.
    pub schedule_id: String,
    /// Stable cancellation token associated with this graph run.
    pub cancellation_token: String,
    /// Canonical cancellation grant/reference used for recovery validation.
    pub cancellation_reference: String,
}

/// Data-owned source identity for a waiting graph node executor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GraphNodeExecutorSourceRecord {
    /// Runtime logic owns the executor.
    Runtime,
    /// An exact plugin owns the executor behind the plugin-host boundary.
    Plugin {
        /// Immutable plugin identity.
        plugin_id: String,
    },
}

/// Data-owned execution boundary for a waiting graph node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeExecutionBoundaryRecord {
    /// Runtime logic owns execution.
    RuntimeLogic,
    /// Execution must travel through the isolated plugin host.
    PluginHost,
}

/// Data-owned restart-safe tool approval payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolApprovalPayloadRecord {
    /// Session identifier used for defense-in-depth validation.
    pub session_id: String,
    /// Canonical workspace text.
    pub workspace: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Final intercepted tool identifier.
    pub tool: String,
    /// Final intercepted arguments.
    pub arguments: serde_json::Value,
    /// Turn cancellation identifier.
    pub cancellation_id: String,
    /// Provider selected for the resumed projection.
    pub provider: String,
    /// Model selected for the resumed projection.
    pub model: String,
    /// Provider-specific options.
    pub options: serde_json::Value,
    /// Explicit session style.
    pub style: String,
    /// Original in-memory harness continuation, usable before restart.
    pub harness_continuation: String,
    /// Sibling calls that must finish before the provider batch may resume.
    #[serde(default)]
    pub remaining_tool_calls: Vec<PendingToolCallPayloadRecord>,
    /// Exact generic graph work retained when the tool belongs to a graph
    /// branch. Legacy provider and root-style approvals omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generic_graph_identity: Option<GenericToolApprovalIdentityRecord>,
}

/// Data-owned immutable graph identity for a tool approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenericToolApprovalIdentityRecord {
    /// Runtime-owned graph run.
    pub run_id: String,
    /// Compiled node owning the tool call.
    pub node_id: String,
    /// Stable nested branch path. An empty path denotes a root graph node.
    #[serde(default)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_node_action: Option<PluginNodeActionApprovalIdentityRecord>,
}

/// Data-owned immutable plugin-node action identity retained behind tool approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginNodeActionApprovalIdentityRecord {
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

/// Data-owned sibling tool call retained behind an approval.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingToolCallPayloadRecord {
    /// Harness continuation alias for the batch.
    pub harness_continuation: String,
    /// Provider call identifier.
    pub call_id: String,
    /// Stable internal tool name.
    pub tool: String,
    /// Provider-supplied arguments.
    pub arguments: serde_json::Value,
}

/// Data-owned continuation record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationRecord {
    /// Session containing the continuation.
    pub session_id: String,
    /// Opaque continuation identifier.
    pub id: String,
    /// Durable state.
    pub state: ContinuationStateRecord,
    /// Durable wake condition.
    pub wake_condition: ContinuationWakeRecord,
    /// Pending action required for restart-safe resumption.
    pub payload: ContinuationPayloadRecord,
    /// Optional expiry in Unix milliseconds.
    pub expires_at_millis: Option<i64>,
}

/// Data request to persist a new continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateContinuationDataRequest {
    /// Initial pending record.
    pub record: ContinuationRecord,
}

/// Data request to resolve an approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveContinuationDataRequest {
    /// Session containing the continuation.
    pub session_id: String,
    /// Continuation identifier.
    pub id: String,
    /// Desired terminal state.
    pub approved: bool,
}

/// Data result for an idempotent resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveContinuationDataRecord {
    /// True only for the winning transition.
    pub transitioned: bool,
    /// Durable state after resolution.
    pub state: ContinuationStateRecord,
    /// Durable pending action payload.
    pub payload: ContinuationPayloadRecord,
}

/// Terminal continuation disposition owned by runtime data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationTerminalStateRecord {
    /// The pending continuation was cancelled.
    Cancelled,
    /// The pending continuation expired before it could resume.
    Expired,
}

/// Data request for an atomic pending-to-terminal transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionContinuationTerminalDataRequest {
    /// Session containing the continuation.
    pub session_id: String,
    /// Continuation identifier.
    pub id: String,
    /// Exact terminal disposition requested by logic.
    pub target: ContinuationTerminalStateRecord,
}

/// Data result for an idempotent pending-to-terminal transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionContinuationTerminalDataRecord {
    /// True only for the caller that changed durable state.
    pub transitioned: bool,
    /// Durable terminal state after the operation.
    pub state: ContinuationTerminalStateRecord,
    /// Durable pending action payload retained for validation and audit.
    pub payload: ContinuationPayloadRecord,
}

/// Bounded lookup for the exact pending graph wait behind a cancellation token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FindGraphNodeWaitByCancellationDataRequest {
    /// Stable graph-run cancellation token.
    pub cancellation_token: String,
}

/// Narrow data interface consumed by continuation logic.
pub trait ContinuationDataPort {
    /// Persists a new pending continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for invalid records or adapter failures.
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError>;

    /// Loads one durable continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for missing, corrupt, or inaccessible records.
    fn load(&self, session_id: &str, id: &str)
    -> Result<ContinuationRecord, ContinuationDataError>;

    /// Loads one continuation when present while preserving all non-absence errors.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for corrupt or inaccessible records.
    fn load_optional(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<Option<ContinuationRecord>, ContinuationDataError> {
        match self.load(session_id, id) {
            Ok(record) => Ok(Some(record)),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Resolves a pending continuation using an atomic dependency operation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for state conflicts or adapter failures.
    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError>;

    /// Atomically transitions a pending continuation to cancelled or expired.
    ///
    /// Exact duplicate transitions are idempotent. A different winner remains
    /// a state conflict at the dependency boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for unsupported adapters, state
    /// conflicts, corrupt payloads, or persistence failures.
    fn transition_terminal(
        &self,
        _request: TransitionContinuationTerminalDataRequest,
    ) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
        Err(ContinuationDataError::TerminalTransitionUnsupported)
    }

    /// Finds the unique pending graph-node wait bound to a cancellation token.
    ///
    /// Implementations fail closed when lookup is unsupported, truncated, or
    /// the token is bound to more than one pending continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDataError`] for invalid or ambiguous tokens,
    /// unsupported lookup, corrupt records, or persistence failures.
    fn find_graph_node_wait_by_cancellation(
        &self,
        _request: FindGraphNodeWaitByCancellationDataRequest,
    ) -> Result<Option<ContinuationRecord>, ContinuationDataError> {
        Err(ContinuationDataError::LookupUnsupported)
    }
}

/// Data router for continuation persistence.
#[derive(Clone, Debug)]
pub struct ContinuationData<D> {
    dependency: D,
}

impl<D> ContinuationData<D> {
    /// Creates a router over one injected dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

/// Constructs the local durable continuation data adapter without exposing its
/// dependency implementation to runtime logic.
#[must_use]
pub fn local_continuation_data(root: PathBuf) -> impl ContinuationDataPort + use<> {
    ContinuationData::new(FileContinuationDependency::new(root))
}

impl<D> ContinuationDataPort for ContinuationData<D>
where
    D: ContinuationDependencyPort,
{
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError> {
        if request.record.state != ContinuationStateRecord::Pending {
            return Err(ContinuationDataError::InvalidInitialState);
        }
        let dependency_record = to_dependency_record(request.record)?;
        self.dependency
            .create_continuation(DependencyCreateContinuationRequest {
                record: dependency_record,
            })
            .map_err(ContinuationDataError::Dependency)
    }

    fn load(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<ContinuationRecord, ContinuationDataError> {
        self.dependency
            .load_continuation(session_id, id)
            .map_err(ContinuationDataError::Dependency)
            .and_then(from_dependency_record)
    }

    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
        let target = if request.approved {
            DependencyContinuationState::Resumed
        } else {
            DependencyContinuationState::Cancelled
        };
        let response = self
            .dependency
            .transition_continuation(DependencyTransitionContinuationRequest {
                session_id: request.session_id,
                id: request.id,
                expected: DependencyContinuationState::Pending,
                target,
            })
            .map_err(ContinuationDataError::Dependency)?;
        Ok(ResolveContinuationDataRecord {
            transitioned: response.transitioned,
            state: from_dependency_state(response.current),
            payload: serde_json::from_slice(&response.payload_json)
                .map_err(ContinuationDataError::PayloadEncoding)?,
        })
    }

    fn transition_terminal(
        &self,
        request: TransitionContinuationTerminalDataRequest,
    ) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
        transition_terminal(&self.dependency, request)
    }

    fn find_graph_node_wait_by_cancellation(
        &self,
        request: FindGraphNodeWaitByCancellationDataRequest,
    ) -> Result<Option<ContinuationRecord>, ContinuationDataError> {
        find_graph_node_wait_by_cancellation(&self.dependency, &request)
    }
}

impl<D> ContinuationDataPort for super::RuntimeData<D>
where
    D: ContinuationDependencyPort,
{
    fn create(&self, request: CreateContinuationDataRequest) -> Result<(), ContinuationDataError> {
        if request.record.state != ContinuationStateRecord::Pending {
            return Err(ContinuationDataError::InvalidInitialState);
        }
        self.dependency
            .create_continuation(DependencyCreateContinuationRequest {
                record: to_dependency_record(request.record)?,
            })
            .map_err(ContinuationDataError::Dependency)
    }

    fn load(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<ContinuationRecord, ContinuationDataError> {
        self.dependency
            .load_continuation(session_id, id)
            .map_err(ContinuationDataError::Dependency)
            .and_then(from_dependency_record)
    }

    fn resolve(
        &self,
        request: ResolveContinuationDataRequest,
    ) -> Result<ResolveContinuationDataRecord, ContinuationDataError> {
        let target = if request.approved {
            DependencyContinuationState::Resumed
        } else {
            DependencyContinuationState::Cancelled
        };
        let response = self
            .dependency
            .transition_continuation(DependencyTransitionContinuationRequest {
                session_id: request.session_id,
                id: request.id,
                expected: DependencyContinuationState::Pending,
                target,
            })
            .map_err(ContinuationDataError::Dependency)?;
        Ok(ResolveContinuationDataRecord {
            transitioned: response.transitioned,
            state: from_dependency_state(response.current),
            payload: serde_json::from_slice(&response.payload_json)
                .map_err(ContinuationDataError::PayloadEncoding)?,
        })
    }

    fn transition_terminal(
        &self,
        request: TransitionContinuationTerminalDataRequest,
    ) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
        transition_terminal(&self.dependency, request)
    }

    fn find_graph_node_wait_by_cancellation(
        &self,
        request: FindGraphNodeWaitByCancellationDataRequest,
    ) -> Result<Option<ContinuationRecord>, ContinuationDataError> {
        find_graph_node_wait_by_cancellation(&self.dependency, &request)
    }
}

fn find_graph_node_wait_by_cancellation<D: ContinuationDependencyPort>(
    dependency: &D,
    request: &FindGraphNodeWaitByCancellationDataRequest,
) -> Result<Option<ContinuationRecord>, ContinuationDataError> {
    if request.cancellation_token.trim().is_empty() {
        return Err(ContinuationDataError::InvalidCancellationToken);
    }
    let mut matched = None;
    for dependency_record in dependency
        .list_continuations(4096)
        .map_err(ContinuationDataError::Dependency)?
    {
        let record = from_dependency_record(dependency_record)?;
        let ContinuationPayloadRecord::GraphNodeWait(wait) = &record.payload else {
            continue;
        };
        if wait.cancellation_token != request.cancellation_token
            || !matches!(
                record.state,
                ContinuationStateRecord::Pending | ContinuationStateRecord::Cancelled
            )
        {
            continue;
        }
        if matched.is_some() {
            return Err(ContinuationDataError::AmbiguousCancellationToken);
        }
        matched = Some(record);
    }
    Ok(matched)
}

fn transition_terminal<D: ContinuationDependencyPort>(
    dependency: &D,
    request: TransitionContinuationTerminalDataRequest,
) -> Result<TransitionContinuationTerminalDataRecord, ContinuationDataError> {
    let requested_state = request.target;
    let target = match request.target {
        ContinuationTerminalStateRecord::Cancelled => DependencyContinuationState::Cancelled,
        ContinuationTerminalStateRecord::Expired => DependencyContinuationState::Expired,
    };
    let response = dependency
        .transition_continuation(DependencyTransitionContinuationRequest {
            session_id: request.session_id,
            id: request.id,
            expected: DependencyContinuationState::Pending,
            target,
        })
        .map_err(ContinuationDataError::Dependency)?;
    let state = match response.current {
        DependencyContinuationState::Cancelled => ContinuationTerminalStateRecord::Cancelled,
        DependencyContinuationState::Expired => ContinuationTerminalStateRecord::Expired,
        DependencyContinuationState::Pending | DependencyContinuationState::Resumed => {
            return Err(ContinuationDataError::InvalidTerminalState);
        }
    };
    if state != requested_state {
        return Err(ContinuationDataError::InvalidTerminalState);
    }
    Ok(TransitionContinuationTerminalDataRecord {
        transitioned: response.transitioned,
        state,
        payload: serde_json::from_slice(&response.payload_json)
            .map_err(ContinuationDataError::PayloadEncoding)?,
    })
}

fn to_dependency_record(
    record: ContinuationRecord,
) -> Result<DependencyContinuationRecord, ContinuationDataError> {
    Ok(DependencyContinuationRecord {
        session_id: record.session_id,
        id: record.id,
        state: to_dependency_state(record.state),
        wake_condition_json: serde_json::to_vec(&record.wake_condition)
            .map_err(ContinuationDataError::WakeEncoding)?,
        payload_json: serde_json::to_vec(&record.payload)
            .map_err(ContinuationDataError::PayloadEncoding)?,
        expires_at_millis: record.expires_at_millis,
    })
}

fn from_dependency_record(
    record: DependencyContinuationRecord,
) -> Result<ContinuationRecord, ContinuationDataError> {
    Ok(ContinuationRecord {
        session_id: record.session_id,
        id: record.id,
        state: from_dependency_state(record.state),
        wake_condition: serde_json::from_slice(&record.wake_condition_json)
            .map_err(ContinuationDataError::WakeEncoding)?,
        payload: serde_json::from_slice(&record.payload_json)
            .map_err(ContinuationDataError::PayloadEncoding)?,
        expires_at_millis: record.expires_at_millis,
    })
}

const fn to_dependency_state(state: ContinuationStateRecord) -> DependencyContinuationState {
    match state {
        ContinuationStateRecord::Pending => DependencyContinuationState::Pending,
        ContinuationStateRecord::Resumed => DependencyContinuationState::Resumed,
        ContinuationStateRecord::Cancelled => DependencyContinuationState::Cancelled,
        ContinuationStateRecord::Expired => DependencyContinuationState::Expired,
    }
}

const fn from_dependency_state(state: DependencyContinuationState) -> ContinuationStateRecord {
    match state {
        DependencyContinuationState::Pending => ContinuationStateRecord::Pending,
        DependencyContinuationState::Resumed => ContinuationStateRecord::Resumed,
        DependencyContinuationState::Cancelled => ContinuationStateRecord::Cancelled,
        DependencyContinuationState::Expired => ContinuationStateRecord::Expired,
    }
}

/// Continuation dataset failure.
#[derive(Debug, Error)]
pub enum ContinuationDataError {
    /// New records must begin pending.
    #[error("new continuation must be pending")]
    InvalidInitialState,
    /// Wake-condition serialization failed inside the data boundary.
    #[error("continuation wake condition is invalid: {0}")]
    WakeEncoding(#[source] serde_json::Error),
    /// Pending action serialization failed inside the data boundary.
    #[error("continuation payload is invalid: {0}")]
    PayloadEncoding(#[source] serde_json::Error),
    /// The data adapter does not implement terminal continuation transitions.
    #[error("continuation terminal transition is unsupported")]
    TerminalTransitionUnsupported,
    /// The dependency returned a nonterminal state for a terminal transition.
    #[error("continuation terminal transition returned an invalid state")]
    InvalidTerminalState,
    /// Cancellation token cannot identify graph work.
    #[error("graph continuation cancellation token is invalid")]
    InvalidCancellationToken,
    /// More than one pending graph wait is bound to the same token.
    #[error("graph continuation cancellation token is ambiguous")]
    AmbiguousCancellationToken,
    /// This adapter cannot perform bounded continuation lookup.
    #[error("graph continuation lookup is unsupported")]
    LookupUnsupported,
    /// Persistence adapter failed.
    #[error("continuation persistence failed: {0}")]
    Dependency(#[source] ContinuationDependencyError),
}

impl ContinuationDataError {
    /// Returns whether the dependency reported an absent continuation.
    #[must_use]
    pub const fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::Dependency(ContinuationDependencyError::NotFound(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_dependency::continuation::DependencyTransitionContinuationResponse;

    use super::*;

    #[derive(Default)]
    struct MockDependency {
        created: RefCell<Vec<DependencyCreateContinuationRequest>>,
        transitioned: RefCell<Vec<DependencyTransitionContinuationRequest>>,
        listed: RefCell<Vec<DependencyContinuationRecord>>,
    }

    impl ContinuationDependencyPort for MockDependency {
        fn create_continuation(
            &self,
            request: DependencyCreateContinuationRequest,
        ) -> Result<(), ContinuationDependencyError> {
            self.created.borrow_mut().push(request);
            Ok(())
        }

        fn load_continuation(
            &self,
            session_id: &str,
            id: &str,
        ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
            Ok(DependencyContinuationRecord {
                session_id: session_id.into(),
                id: id.into(),
                state: DependencyContinuationState::Pending,
                wake_condition_json: br#"{"kind":"manual"}"#.to_vec(),
                payload_json: br#"{"kind":"opaque","value":{"label":"fixture"}}"#.to_vec(),
                expires_at_millis: None,
            })
        }

        fn transition_continuation(
            &self,
            request: DependencyTransitionContinuationRequest,
        ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError> {
            self.transitioned.borrow_mut().push(request.clone());
            Ok(DependencyTransitionContinuationResponse {
                transitioned: true,
                current: request.target,
                payload_json: br#"{"kind":"opaque","value":{"label":"fixture"}}"#.to_vec(),
            })
        }

        fn list_continuations(
            &self,
            _limit: u32,
        ) -> Result<Vec<DependencyContinuationRecord>, ContinuationDependencyError> {
            Ok(self.listed.borrow().clone())
        }
    }

    fn pending() -> ContinuationRecord {
        ContinuationRecord {
            session_id: "session_1".into(),
            id: "approval_1".into(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::Manual,
            payload: ContinuationPayloadRecord::Opaque {
                label: "fixture".into(),
            },
            expires_at_millis: None,
        }
    }

    #[test]
    fn maps_create_without_reusing_data_types() {
        let data = ContinuationData::new(MockDependency::default());
        data.create(CreateContinuationDataRequest { record: pending() })
            .expect("create");
        let observed = data.dependency.created.into_inner();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].record.id, "approval_1");
        assert_eq!(
            observed[0].record.state,
            DependencyContinuationState::Pending
        );
    }

    #[test]
    fn maps_approval_to_pending_compare_and_set() {
        let data = ContinuationData::new(MockDependency::default());
        let result = data
            .resolve(ResolveContinuationDataRequest {
                session_id: "session_1".into(),
                id: "approval_1".into(),
                approved: true,
            })
            .expect("resolve");
        assert!(result.transitioned);
        assert_eq!(result.state, ContinuationStateRecord::Resumed);
        assert_eq!(
            data.dependency.transitioned.into_inner(),
            vec![DependencyTransitionContinuationRequest {
                session_id: "session_1".into(),
                id: "approval_1".into(),
                expected: DependencyContinuationState::Pending,
                target: DependencyContinuationState::Resumed,
            }]
        );
    }

    #[test]
    fn maps_cancelled_and_expired_to_exact_terminal_compare_and_set() {
        for (target, dependency_target) in [
            (
                ContinuationTerminalStateRecord::Cancelled,
                DependencyContinuationState::Cancelled,
            ),
            (
                ContinuationTerminalStateRecord::Expired,
                DependencyContinuationState::Expired,
            ),
        ] {
            let data = ContinuationData::new(MockDependency::default());
            let result = data
                .transition_terminal(TransitionContinuationTerminalDataRequest {
                    session_id: "session_1".into(),
                    id: "approval_1".into(),
                    target,
                })
                .expect("terminal transition");
            assert!(result.transitioned);
            assert_eq!(result.state, target);
            assert_eq!(
                result.payload,
                ContinuationPayloadRecord::Opaque {
                    label: "fixture".into()
                }
            );
            assert_eq!(
                data.dependency.transitioned.into_inner(),
                vec![DependencyTransitionContinuationRequest {
                    session_id: "session_1".into(),
                    id: "approval_1".into(),
                    expected: DependencyContinuationState::Pending,
                    target: dependency_target,
                }]
            );
        }
    }

    #[test]
    fn normalizes_loaded_wake_condition() {
        let data = ContinuationData::new(MockDependency::default());
        assert_eq!(
            data.load("session_1", "approval_1")
                .expect("load")
                .wake_condition,
            ContinuationWakeRecord::Manual
        );
    }

    #[test]
    fn maps_graph_node_wait_payload_without_losing_the_resume_contract() {
        let record = ContinuationRecord {
            session_id: "session_1".into(),
            id: "graph_wait_1".into(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::At(123),
            payload: ContinuationPayloadRecord::GraphNodeWait(Box::new(
                GraphNodeWaitPayloadRecord {
                    session_id: "session_1".into(),
                    run_id: "run_1".into(),
                    branch_path: vec!["root".into(), "parallel_a".into()],
                    node_id: "delay".into(),
                    executor_id: "runtime.delay".into(),
                    executor_version: "1.0.0".into(),
                    executor_source: GraphNodeExecutorSourceRecord::Runtime,
                    execution_boundary: GraphNodeExecutionBoundaryRecord::RuntimeLogic,
                    adapter_configuration_reference: ContentHash::digest(b"delay-config"),
                    execution_plan_hash: ContentHash::digest(b"execution-plan"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 7,
                    transition_target_node_id: "after_delay".into(),
                    compiled_transition_reference: "transition_hash".into(),
                    schedule_id: "schedule_1".into(),
                    cancellation_token: "token_1".into(),
                    cancellation_reference: "cancel_ref_1".into(),
                },
            )),
            expires_at_millis: Some(124),
        };
        let dependency = to_dependency_record(record.clone()).expect("map to dependency");
        let restored = from_dependency_record(dependency).expect("map from dependency");
        assert_eq!(restored, record);
    }

    #[test]
    fn style_approval_payload_round_trips_branch_path_and_defaults_legacy_root() {
        let nested =
            ContinuationPayloadRecord::StyleApproval(Box::new(StyleApprovalPayloadRecord {
                session_id: "session_1".into(),
                workspace: "workspace".into(),
                prompt: "continue".into(),
                provider: "mock".into(),
                model: "mock-model".into(),
                options: serde_json::json!({}),
                style: "user-graph".into(),
                cancellation_id: "cancel_1".into(),
                compiled_style_cache_key: "cache_1".into(),
                node_id: "approval".into(),
                branch_path: vec!["fanout".into(), "review".into()],
                attempt: 1,
                loop_iteration: 0,
                step: 4,
                request_reference: "request_1".into(),
            }));
        let encoded = serde_json::to_vec(&nested).expect("encode nested approval");
        assert_eq!(
            serde_json::from_slice::<ContinuationPayloadRecord>(&encoded)
                .expect("decode nested approval"),
            nested
        );

        let legacy = br#"{"kind":"style_approval","value":{"session_id":"session_1","workspace":"workspace","prompt":"continue","provider":"mock","model":"mock-model","options":{},"style":"user-graph","cancellation_id":"cancel_1","compiled_style_cache_key":"cache_1","node_id":"approval","attempt":1,"loop_iteration":0,"step":4,"request_reference":"request_1"}}"#;
        let ContinuationPayloadRecord::StyleApproval(root) =
            serde_json::from_slice(legacy).expect("decode legacy root approval")
        else {
            panic!("style approval payload");
        };
        assert!(root.branch_path.is_empty());
    }

    #[test]
    fn finds_only_the_unique_cancellable_graph_wait_by_exact_token() {
        let wait = ContinuationRecord {
            session_id: "session_1".into(),
            id: "00000000-0000-0000-0000-000000000001".into(),
            state: ContinuationStateRecord::Pending,
            wake_condition: ContinuationWakeRecord::At(123),
            payload: ContinuationPayloadRecord::GraphNodeWait(Box::new(
                GraphNodeWaitPayloadRecord {
                    session_id: "session_1".into(),
                    run_id: "run_1".into(),
                    branch_path: Vec::new(),
                    node_id: "delay".into(),
                    executor_id: "runtime.delay".into(),
                    executor_version: "1.0.0".into(),
                    executor_source: GraphNodeExecutorSourceRecord::Runtime,
                    execution_boundary: GraphNodeExecutionBoundaryRecord::RuntimeLogic,
                    adapter_configuration_reference: ContentHash::digest(b"delay-config"),
                    execution_plan_hash: ContentHash::digest(b"execution-plan"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    transition_target_node_id: "done".into(),
                    compiled_transition_reference: "transition".into(),
                    schedule_id: "schedule_1".into(),
                    cancellation_token: "cancel_1".into(),
                    cancellation_reference: "reference".into(),
                },
            )),
            expires_at_millis: None,
        };
        let dependency = MockDependency::default();
        dependency
            .listed
            .borrow_mut()
            .push(to_dependency_record(wait.clone()).expect("encode wait"));
        let data = ContinuationData::new(dependency);
        assert_eq!(
            data.find_graph_node_wait_by_cancellation(FindGraphNodeWaitByCancellationDataRequest {
                cancellation_token: "cancel_1".into()
            })
            .expect("lookup"),
            Some(wait)
        );
    }
}
