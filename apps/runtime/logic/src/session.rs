//! Pure runtime session reducers and typed committed payloads.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use agentmod_event_model::{EventClassification, EventEnvelope, EventModelError, EventScope};
use agentmod_graph_engine::{ExecutableGraph, GRAPH_FORMAT_VERSION, NodeKind};
use agentmod_primitives::{ContentHash, ContinuationId, EventId, Sequence, SessionId};
use agentmod_session_style_sdk::CompiledSessionStyle;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    action::{ActionProposal, ConsequentialAction, ProposalId},
    conversation::{ConversationEntry, ConversationError, ConversationState, ProjectionProvenance},
    projection::measure_projection,
};

const MAX_REPLAY_VISIBLE_MODEL_BYTES: usize = 16 * 1024 * 1024;

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
    /// Exact harness adapter version selected at session creation.
    #[serde(default = "legacy_harness_version")]
    pub harness_version: String,
    /// Hash of the selected harness capability set.
    #[serde(default = "legacy_harness_capability_hash")]
    pub harness_capability_set_hash: ContentHash,
    /// Capabilities the compiled style requires from the harness.
    #[serde(default)]
    pub harness_required_capabilities: Vec<String>,
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

fn legacy_harness_version() -> String {
    String::from("unversioned")
}

fn legacy_harness_capability_hash() -> ContentHash {
    ContentHash::digest(b"legacy-unversioned-harness")
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

/// Immutable causal ownership and typed task input for a worker session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildSessionLinkedEvent {
    /// Runtime-managed parent session.
    pub parent_session_id: SessionId,
    /// Canonical parent creation proposal used for reconciliation.
    pub parent_action_sequence: Sequence,
    /// Parent graph node that created the worker.
    pub parent_graph_node_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Zero-based task revision.
    pub revision: u32,
    /// One-based child-session depth.
    pub depth: u32,
    /// Bounded typed task input; this is not a user message.
    pub task: String,
    /// Hash of the exact task bytes.
    pub input_hash: ContentHash,
    /// Hard worker token budget.
    pub token_budget: u64,
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
    /// Context phase atomically completed by this canonical replacement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_phase: Option<ContextPhaseIdentity>,
}

/// Stable identity for one recoverable context-composition boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBoundaryIdentity {
    /// Active compiled graph node.
    pub node_id: String,
    /// `turn_start` or `before_model_request`.
    pub boundary: String,
    /// Client/runtime run identity, normally the provider cancellation ID.
    pub run_id: String,
    /// Lifecycle path that requested this boundary.
    pub origin: ContextBoundaryOrigin,
    /// Exact provider/model/options/current-input identity.
    pub request_hash: ContentHash,
    /// Journal head before the boundary began.
    pub source_head: Sequence,
}

/// Explicit lifecycle path that owns one context boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextBoundaryOrigin {
    /// Initial model request for a user-authored turn.
    UserTurn,
    /// Initial model request for a runtime-owned typed child task.
    ChildTask,
    /// Model continuation after a non-approval tool batch.
    ToolContinuation,
    /// Model continuation after resolving a durable approval.
    ApprovalContinuation,
}

/// Stable identity for one effect-bearing phase inside a context boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextPhaseIdentity {
    /// Owning recoverable boundary.
    pub boundary: ContextBoundaryIdentity,
    /// `memory` or `compaction`.
    pub phase: String,
}

/// Canonical intent to begin context composition before invoking its pipeline.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBoundaryStartedEvent {
    /// Exact recoverable boundary identity.
    pub identity: ContextBoundaryIdentity,
}

/// Canonical completion for a phase which required no projection replacement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextPhaseCompletedEvent {
    /// Exact phase identity.
    pub identity: ContextPhaseIdentity,
}

/// Canonical intent before invoking one context phase's blocking pipeline.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextPhaseStartedEvent {
    /// Exact phase identity.
    pub identity: ContextPhaseIdentity,
}

/// Canonical completion of a provider-projection lifecycle boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBoundaryCompletedEvent {
    /// Exact recoverable boundary identity.
    pub identity: ContextBoundaryIdentity,
    /// Hash of the exact structured provider projection.
    pub projection_hash: ContentHash,
    /// Provider-independent approximate token pressure.
    pub estimated_tokens: u64,
    /// Exact serialized provider projection bytes.
    pub serialized_bytes: u64,
}

/// Auditable model request proposal before authorization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelRequestProposedEvent {
    /// Logic proposal identifier.
    pub proposal_id: String,
    /// Selected harness registry ID.
    #[serde(default = "default_native_harness")]
    pub harness: String,
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
    /// Authorized harness registry ID.
    #[serde(default = "default_native_harness")]
    pub harness: String,
    /// Final provider after interception.
    pub provider: String,
    /// Final model after interception.
    pub model: String,
    /// Digest bound into the short-lived harness grant.
    pub action_digest: ContentHash,
}

fn default_native_harness() -> String {
    String::from("native")
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Exact external plugin set activated for style execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginSetActivatedEvent {
    /// Deterministically ordered plugin identities accepted by the host.
    pub plugin_ids: Vec<String>,
    /// Plugin-set hash bound into the immutable session style.
    pub plugin_set_hash: ContentHash,
}

/// Canonical result of one blocking plugin invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginInvocationCompletedEvent {
    /// Plugin selected by the compiled style declaration.
    pub plugin_id: String,
    /// Stable interceptor declaration ID.
    pub handler: String,
    /// Consequential action boundary observed by the interceptor.
    pub action_kind: String,
    /// Stable proposal identity.
    pub proposal_id: String,
    /// Digest of the exact interceptor input.
    pub input_digest: ContentHash,
    /// Digest of a returned proposal, when present.
    pub output_digest: Option<ContentHash>,
    /// Normalized decision or failure classification.
    pub outcome: String,
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
    /// Exact proposal digest when failure occurred before a dispatch record existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
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
    /// Canonical identity of caller-controlled inputs bound before entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_reference: Option<String>,
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

/// Records a control-plane termination before a node may legally be entered.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleExecutionTerminatedEvent {
    /// Stable redacted terminal reason.
    pub reason: String,
    /// Node whose entry was refused, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused_node_id: Option<String>,
    /// Refused one-based graph step, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused_step: Option<u64>,
    /// Effective configured limit, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

/// Exact logical identity of one compiled artifact-persistence node effect.
///
/// The content store's BLAKE3 identity remains dependency-owned text. This
/// identity instead binds the canonical graph attempt and approved content
/// which recovery is allowed to reconcile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceIdentity {
    /// Stable execution identity used by the durable receipt.
    pub execution_id: String,
    /// Stable proposal identity passed through interception and policy.
    pub proposal_id: String,
    /// Active compiled graph node.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Hash of the exact approved bytes.
    pub content_hash: ContentHash,
}

/// Canonical artifact-persistence proposal before blocking policy evaluation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceProposedEvent {
    /// Exact graph/content/proposal identity.
    pub identity: ArtifactPersistenceIdentity,
    /// Stable media type requested for the immutable object.
    pub mime_type: String,
    /// Exact bounded byte count.
    pub byte_size: u64,
}

/// Final artifact-persistence action authorized before dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceApprovedEvent {
    /// Exact graph/content/proposal identity.
    pub identity: ArtifactPersistenceIdentity,
    /// Digest of the final intercepted action.
    pub action_digest: ContentHash,
}

/// Durable artifact-persistence outbox record committed before storage dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceDispatchedEvent {
    /// Exact graph/content/proposal identity.
    pub identity: ArtifactPersistenceIdentity,
    /// Digest of the exact approved action sent to data.
    pub action_digest: ContentHash,
}

/// Immutable artifact storage completed and has a durable terminal receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceCompletedEvent {
    /// Exact graph/content/proposal identity.
    pub identity: ArtifactPersistenceIdentity,
    /// Digest of the exact approved action.
    pub action_digest: ContentHash,
    /// Dependency-owned content-addressed BLAKE3 identity.
    pub artifact_id: String,
    /// Portable dependency-owned content reference.
    pub artifact_reference: String,
    /// Exact persisted media type.
    pub mime_type: String,
    /// Exact persisted byte count.
    pub byte_size: u64,
}

/// Stable identity for one child-agent creation proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentExecutionIdentity {
    /// Deterministic identity derived from the graph cursor and task.
    pub execution_id: String,
    /// Spawn node that owns this child.
    pub node_id: String,
    /// One-based spawn-node attempt.
    pub attempt: u32,
    /// Zero-based orchestration loop iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Runtime-owned task identity.
    pub task_id: String,
}

/// Canonical child-session creation intent before policy and atomic branching.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentCreationProposedEvent {
    /// Exact graph/task identity.
    pub identity: ChildAgentExecutionIdentity,
    /// Bounded task description.
    pub task: String,
    /// Explicit child style selector.
    pub child_style: String,
    /// Style-selected workspace mode.
    pub workspace_mode: String,
    /// Hard child token budget.
    pub token_budget: u64,
}

/// Final child-session creation action authorized before atomic branching.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentCreationApprovedEvent {
    /// Exact graph/task identity.
    pub identity: ChildAgentExecutionIdentity,
    /// Digest of the final intercepted action.
    pub action_digest: ContentHash,
}

/// Atomic child branch became durable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentCreatedEvent {
    /// Exact proposal identity.
    pub identity: ChildAgentExecutionIdentity,
    /// Runtime-managed child session.
    pub child_session_id: SessionId,
    /// Parent proposal sequence retained by the typed child ownership link.
    pub parent_action_sequence: Sequence,
    /// Exact selected child style.
    pub child_style: String,
}

/// Child session reached a canonical terminal result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentCompletedEvent {
    /// Exact proposal identity.
    pub identity: ChildAgentExecutionIdentity,
    /// Runtime-managed child session.
    pub child_session_id: SessionId,
    /// Verified child journal head.
    pub child_head_sequence: Sequence,
    /// Bounded structured handoff summary.
    pub summary: String,
}

/// Runtime-owned task emitted by a planner model response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlannedTask {
    /// Stable task identity within the parent session.
    pub task_id: String,
    /// Bounded typed worker assignment.
    pub description: String,
}

/// Structured planner output committed before child creation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskPlanCommittedEvent {
    /// Planner graph node.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Exact model response containing the structured plan.
    pub model_response_sequence: Sequence,
    /// Runtime-validated task records.
    pub tasks: Vec<PlannedTask>,
}

/// Exact completed child set used to release a parent join.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildJoinCompletedEvent {
    /// Wait node that owns the join.
    pub node_id: String,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// Deterministically ordered child execution IDs.
    pub child_execution_ids: Vec<String>,
}

/// Structured reviewer decision committed before revision routing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewerFindingsCommittedEvent {
    /// Review graph node.
    pub node_id: String,
    /// One-based node attempt.
    pub attempt: u32,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// One-based graph step.
    pub step: u64,
    /// Whether the current integration is accepted.
    pub approved: bool,
    /// Tasks that require another runtime-owned revision.
    pub rejected_task_ids: Vec<String>,
    /// Bounded structured findings.
    pub findings: Vec<String>,
}

/// Stable identity for one live typed-summary compaction request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryIdentity {
    /// Projection-local summary execution ID.
    pub summary_id: String,
    /// Hash of the exact provider/model/options/entries request.
    pub request_hash: ContentHash,
    /// Provider used for the summary model request.
    pub provider: String,
    /// Model used for the summary model request.
    pub model: String,
    /// Bounded summary schema version.
    pub schema_version: u16,
    /// Maximum provider-visible summary bytes.
    pub max_summary_bytes: u32,
    /// Inclusive source projection range being summarized.
    pub source_range: Option<(Sequence, Sequence)>,
}

/// Canonical intent to begin a live typed-summary model request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryProposedEvent {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
}

/// Records the final policy-approved summary action before dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryApprovedEvent {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
    /// Digest bound into the short-lived harness grant.
    pub action_digest: ContentHash,
}

/// Records provider dispatch of one summary request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryStartedEvent {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
}

/// Terminal provider evidence for one summary request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryCompletedEvent {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
    /// Hash of the exact bounded summary text.
    pub content_hash: ContentHash,
    /// Bounded provider-visible summary text.
    pub text: String,
    /// Provider-reported input tokens.
    pub input_tokens: u64,
    /// Provider-reported output tokens.
    pub output_tokens: u64,
}

/// Terminal failure for one summary request without provider evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryFailedEvent {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
    /// Stable failure code.
    pub code: String,
    /// Bounded failure detail.
    pub message: String,
}

/// Canonical identity for one automatic memory write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteIdentity {
    /// Canonical cross-restart write identity.
    pub write_id: String,
    /// Provider used for the write.
    pub provider: String,
    /// Normalized scope key.
    pub scope: String,
    /// Provenance label.
    pub source: String,
    /// Hash of the exact approved content.
    pub content_hash: ContentHash,
    /// Canonical duplicate-prevention key, when the selected policy uses one.
    pub deduplication_key: Option<String>,
}

/// Canonical intent to begin one automatic memory-write proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteProposedEvent {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Logic proposal identifier.
    pub proposal_id: String,
    /// Maximum retained bytes.
    pub max_bytes: u32,
    /// Trigger boundary that proposed the write.
    pub trigger: String,
}

/// Records the final policy-approved memory-write action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteApprovedEvent {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
}

/// Records durable dispatch intent before the memory provider is called.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteDispatchedEvent {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
}

/// Terminal provider receipt for one automatic memory write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteCompletedEvent {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
    /// Provider-local stable reference.
    pub reference: String,
    /// Whether the provider retained the content.
    pub retained: bool,
    /// Whether an identical canonical write was already retained.
    pub deduplicated: bool,
}

/// Terminal failure for one automatic memory write without a receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteFailedEvent {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Stable failure code.
    pub code: String,
    /// Bounded failure detail.
    pub message: String,
}

/// Stable identity for one artifact-handoff compaction write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactIdentity {
    /// Projection-local artifact write execution ID.
    pub execution_id: String,
    /// Logic proposal identifier.
    pub proposal_id: String,
    /// Hash of the exact serialized context payload.
    pub content_hash: ContentHash,
    /// Media type of the context artifact.
    pub mime_type: String,
    /// Inclusive source projection range captured.
    pub source_range: Option<(Sequence, Sequence)>,
}

/// Canonical intent to begin one artifact-handoff context write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactProposedEvent {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Byte size of the exact serialized payload.
    pub byte_size: u64,
}

/// Records the final policy-approved context-artifact action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactApprovedEvent {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
}

/// Records durable dispatch intent before artifact storage is called.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactDispatchedEvent {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
}

/// Terminal artifact-store receipt for one artifact-handoff write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactCompletedEvent {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Digest of the policy-approved action.
    pub action_digest: ContentHash,
    /// Content-addressed artifact ID.
    pub artifact_id: String,
    /// Portable immutable artifact reference.
    pub artifact_reference: String,
    /// Exact media type.
    pub mime_type: String,
    /// Exact byte count.
    pub byte_size: u64,
}

/// Terminal failure for one artifact-handoff write without a receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactFailedEvent {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Stable failure code.
    pub code: String,
    /// Bounded failure detail.
    pub message: String,
}

/// Typed committed events consumed by the pure session reducer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum RuntimeCommittedEvent {
    /// Establishes a new session.
    SessionCreated(SessionCreatedEvent),
    /// Establishes immutable parent/fork ancestry for a child session.
    SessionBranched(SessionBranchedEvent),
    /// Establishes immutable parent/task ownership for a worker session.
    ChildSessionLinked(ChildSessionLinkedEvent),
    /// Adds structured canonical content.
    ConversationEntryCommitted(ConversationEntryCommittedEvent),
    /// Replaces only provider-visible structured state.
    ContextProjectionReplaced(ContextProjectionReplacedEvent),
    /// Begins one recoverable context lifecycle boundary.
    ContextBoundaryStarted(ContextBoundaryStartedEvent),
    /// Begins one effect-bearing context phase.
    ContextPhaseStarted(ContextPhaseStartedEvent),
    /// Completes one context phase without a replacement.
    ContextPhaseCompleted(ContextPhaseCompletedEvent),
    /// Completes one recoverable context lifecycle boundary.
    ContextBoundaryCompleted(ContextBoundaryCompletedEvent),
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
    /// Records the exact external plugin set activated for this session.
    PluginSetActivated(PluginSetActivatedEvent),
    /// Records one completed blocking plugin invocation.
    PluginInvocationCompleted(PluginInvocationCompletedEvent),
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
    /// Records a terminal control outcome without entering another node.
    StyleExecutionTerminated(StyleExecutionTerminatedEvent),
    /// Records artifact-persistence intent before blocking policy evaluation.
    ArtifactPersistenceProposed(ArtifactPersistenceProposedEvent),
    /// Records the final approved artifact-persistence action.
    ArtifactPersistenceApproved(ArtifactPersistenceApprovedEvent),
    /// Records durable dispatch intent before artifact storage is called.
    ArtifactPersistenceDispatched(ArtifactPersistenceDispatchedEvent),
    /// Records a request-bound terminal artifact receipt.
    ArtifactPersistenceCompleted(ArtifactPersistenceCompletedEvent),
    /// Records child-session creation intent before policy and branching.
    ChildAgentCreationProposed(ChildAgentCreationProposedEvent),
    /// Records the final approved child-session creation action.
    ChildAgentCreationApproved(ChildAgentCreationApprovedEvent),
    /// Records the atomically created child session.
    ChildAgentCreated(ChildAgentCreatedEvent),
    /// Records a verified terminal child result.
    ChildAgentCompleted(ChildAgentCompletedEvent),
    /// Records structured planner tasks before child creation.
    TaskPlanCommitted(TaskPlanCommittedEvent),
    /// Records the exact terminal child set used by a join.
    ChildJoinCompleted(ChildJoinCompletedEvent),
    /// Records a structured reviewer decision.
    ReviewerFindingsCommitted(ReviewerFindingsCommittedEvent),
    /// Begins one live typed-summary model request.
    ContextSummaryProposed(ContextSummaryProposedEvent),
    /// Records the final approved summary action before dispatch.
    ContextSummaryApproved(ContextSummaryApprovedEvent),
    /// Records provider dispatch of one summary request.
    ContextSummaryStarted(ContextSummaryStartedEvent),
    /// Records terminal provider evidence for one summary request.
    ContextSummaryCompleted(ContextSummaryCompletedEvent),
    /// Records a terminal summary failure without provider evidence.
    ContextSummaryFailed(ContextSummaryFailedEvent),
    /// Begins one automatic memory-write proposal.
    MemoryWriteProposed(MemoryWriteProposedEvent),
    /// Records the final approved memory-write action.
    MemoryWriteApproved(MemoryWriteApprovedEvent),
    /// Records durable dispatch intent before the memory provider is called.
    MemoryWriteDispatched(MemoryWriteDispatchedEvent),
    /// Records a terminal memory-write receipt.
    MemoryWriteCompleted(MemoryWriteCompletedEvent),
    /// Records a terminal memory-write failure.
    MemoryWriteFailed(MemoryWriteFailedEvent),
    /// Begins one artifact-handoff context write.
    ContextArtifactProposed(ContextArtifactProposedEvent),
    /// Records the final approved context-artifact action.
    ContextArtifactApproved(ContextArtifactApprovedEvent),
    /// Records durable dispatch intent before artifact storage is called.
    ContextArtifactDispatched(ContextArtifactDispatchedEvent),
    /// Records a terminal artifact-store receipt.
    ContextArtifactCompleted(ContextArtifactCompletedEvent),
    /// Records a terminal context-artifact failure.
    ContextArtifactFailed(ContextArtifactFailedEvent),
}

impl RuntimeCommittedEvent {
    /// Returns the stable metadata event type required for this payload.
    #[must_use]
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::SessionCreated(_) => "session.created",
            Self::SessionBranched(_) => "session.branched",
            Self::ChildSessionLinked(_) => "child_session.linked",
            Self::ConversationEntryCommitted(_) => "conversation.entry_committed",
            Self::ContextProjectionReplaced(_) => "context.projection_replaced",
            Self::ContextBoundaryStarted(_) => "context.boundary_started",
            Self::ContextPhaseStarted(_) => "context.phase_started",
            Self::ContextPhaseCompleted(_) => "context.phase_completed",
            Self::ContextBoundaryCompleted(_) => "context.boundary_completed",
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
            Self::PluginSetActivated(_) => "plugin.set_activated",
            Self::PluginInvocationCompleted(_) => "plugin.invocation_completed",
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
            Self::StyleExecutionTerminated(_) => "style.execution_terminated",
            Self::ArtifactPersistenceProposed(_) => "artifact.persistence_proposed",
            Self::ArtifactPersistenceApproved(_) => "artifact.persistence_approved",
            Self::ArtifactPersistenceDispatched(_) => "artifact.persistence_dispatched",
            Self::ArtifactPersistenceCompleted(_) => "artifact.persistence_completed",
            Self::ChildAgentCreationProposed(_) => "child_agent.creation_proposed",
            Self::ChildAgentCreationApproved(_) => "child_agent.creation_approved",
            Self::ChildAgentCreated(_) => "child_agent.created",
            Self::ChildAgentCompleted(_) => "child_agent.completed",
            Self::TaskPlanCommitted(_) => "style.task_plan_committed",
            Self::ChildJoinCompleted(_) => "child_agent.join_completed",
            Self::ReviewerFindingsCommitted(_) => "style.reviewer_findings_committed",
            Self::ContextSummaryProposed(_) => "context.summary_proposed",
            Self::ContextSummaryApproved(_) => "context.summary_approved",
            Self::ContextSummaryStarted(_) => "context.summary_started",
            Self::ContextSummaryCompleted(_) => "context.summary_completed",
            Self::ContextSummaryFailed(_) => "context.summary_failed",
            Self::MemoryWriteProposed(_) => "memory.write_proposed",
            Self::MemoryWriteApproved(_) => "memory.write_approved",
            Self::MemoryWriteDispatched(_) => "memory.write_dispatched",
            Self::MemoryWriteCompleted(_) => "memory.write_completed",
            Self::MemoryWriteFailed(_) => "memory.write_failed",
            Self::ContextArtifactProposed(_) => "context.artifact_proposed",
            Self::ContextArtifactApproved(_) => "context.artifact_approved",
            Self::ContextArtifactDispatched(_) => "context.artifact_dispatched",
            Self::ContextArtifactCompleted(_) => "context.artifact_completed",
            Self::ContextArtifactFailed(_) => "context.artifact_failed",
        }
    }
}

/// Replay-owned artifact-persistence outbox state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPersistenceState {
    /// Proposal is canonical, but no policy outcome is canonical yet.
    Proposed,
    /// Policy approved the exact action; storage has not been dispatched.
    Approved,
    /// Dispatch intent is canonical; recovery must use an exact terminal receipt.
    Dispatched,
    /// A request-bound terminal receipt is canonical.
    Completed,
}

/// Replay-owned child-agent lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildAgentState {
    /// Creation proposal is canonical; no policy outcome is canonical yet.
    Proposed,
    /// Policy approved the exact action; no durable child is known yet.
    Approved,
    /// Child session exists and may be active.
    Active,
    /// Child session reached a verified canonical terminal state.
    Completed,
}

/// Replay-owned runtime-managed child session.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentRecord {
    /// Exact graph/task identity.
    pub identity: ChildAgentExecutionIdentity,
    /// Bounded task description.
    pub task: String,
    /// Selected child style.
    pub child_style: String,
    /// Style-selected workspace mode.
    pub workspace_mode: String,
    /// Hard child token budget.
    pub token_budget: u64,
    /// Current lifecycle.
    pub state: ChildAgentState,
    /// Parent sequence that proposed the child.
    pub proposed_at: Sequence,
    /// Digest of the policy-approved action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
    /// Parent sequence that approved the child action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<Sequence>,
    /// Runtime-managed child session after atomic creation.
    pub child_session_id: Option<SessionId>,
    /// Creation sequence in the parent journal.
    pub created_at: Option<Sequence>,
    /// Verified child journal head after completion.
    pub child_head_sequence: Option<Sequence>,
    /// Parent completion sequence.
    pub completed_at: Option<Sequence>,
    /// Bounded result summary.
    pub summary: Option<String>,
}

/// Replay-owned planner/worker/reviewer orchestration state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlannerWorkerState {
    /// Latest validated plan tasks keyed by stable task ID.
    pub tasks: BTreeMap<String, PlannedTask>,
    /// Sequence that committed the plan.
    pub plan_committed_at: Option<Sequence>,
    /// Exact child execution IDs in each completed join.
    pub joins: Vec<ChildJoinRecord>,
    /// Structured reviewer decisions in canonical order.
    pub reviews: Vec<ReviewerDecisionRecord>,
}

/// Replay-owned exact child set that released one wait node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildJoinRecord {
    /// Orchestration iteration joined.
    pub loop_iteration: u32,
    /// Deterministically ordered child execution IDs.
    pub child_execution_ids: Vec<String>,
    /// Canonical join commit sequence.
    pub committed_at: Sequence,
}

/// Replay-owned structured reviewer decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewerDecisionRecord {
    /// Orchestration iteration reviewed.
    pub loop_iteration: u32,
    /// Whether the integration was accepted.
    pub approved: bool,
    /// Rejected task IDs.
    pub rejected_task_ids: Vec<String>,
    /// Bounded reviewer findings.
    pub findings: Vec<String>,
    /// Canonical commit sequence.
    pub committed_at: Sequence,
}

/// Safe next action derived only from canonical artifact-persistence state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactPersistenceResumeAction {
    /// Policy evaluation has no canonical outcome and must not be inferred.
    AwaitPolicyRecovery,
    /// The exact approved request may be dispatched for the first time.
    DispatchApproved,
    /// Dispatch may already have crossed the effect boundary; reconcile only.
    ReconcileReceipt,
    /// The effect is terminal and must not be dispatched again.
    CompleteNode,
}

/// Canonical artifact-persistence record reconstructed without opening storage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceRecord {
    /// Exact graph/content/proposal identity.
    pub identity: ArtifactPersistenceIdentity,
    /// Requested and persisted media type.
    pub mime_type: String,
    /// Requested and persisted byte count.
    pub byte_size: u64,
    /// Latest durable outbox state.
    pub state: ArtifactPersistenceState,
    /// Digest of the approved action, once policy succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
    /// Canonical proposal sequence.
    pub proposed_at: Sequence,
    /// Canonical proposal event used as dependency creation provenance.
    pub proposed_event: EventId,
    /// Canonical approval sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<Sequence>,
    /// Canonical dispatch sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<Sequence>,
    /// Canonical terminal sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Dependency-owned BLAKE3 content identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    /// Portable dependency-owned immutable reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_reference: Option<String>,
}

/// Replay-owned live typed-summary compaction outbox state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSummaryState {
    /// Proposal is canonical; no policy outcome is canonical yet.
    Proposed,
    /// Policy approved the exact request; no provider call is canonical yet.
    Approved,
    /// Provider dispatch is canonical; recovery must reuse exact evidence.
    Started,
    /// Terminal provider evidence is canonical.
    Completed,
    /// Terminal failure without provider evidence is canonical.
    Failed,
}

/// Replay-owned live typed-summary compaction record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextSummaryRecord {
    /// Exact summary request identity.
    pub identity: ContextSummaryIdentity,
    /// Latest durable outbox state.
    pub state: ContextSummaryState,
    /// Digest of the approved action, once policy succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
    /// Canonical proposal sequence.
    pub proposed_at: Sequence,
    /// Canonical approval sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<Sequence>,
    /// Canonical dispatch sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Sequence>,
    /// Canonical terminal sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Hash of the exact bounded summary text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<ContentHash>,
    /// Bounded provider-visible summary text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Provider-reported input tokens.
    #[serde(default)]
    pub input_tokens: u64,
    /// Provider-reported output tokens.
    #[serde(default)]
    pub output_tokens: u64,
}

impl ContextSummaryRecord {
    /// Returns whether terminal provider evidence already exists.
    #[must_use]
    pub const fn has_terminal_evidence(&self) -> bool {
        matches!(
            self.state,
            ContextSummaryState::Completed | ContextSummaryState::Failed
        )
    }
}

/// Replay-owned automatic memory-write outbox state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryWriteState {
    /// Proposal is canonical; no policy outcome is canonical yet.
    Proposed,
    /// Policy approved the exact action; no provider call is canonical yet.
    Approved,
    /// Dispatch intent is canonical; recovery must use an exact terminal receipt.
    Dispatched,
    /// A terminal provider receipt is canonical.
    Completed,
    /// Terminal failure without a receipt is canonical.
    Failed,
}

/// Replay-owned automatic memory-write outbox record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryWriteRecord {
    /// Canonical write identity.
    pub identity: MemoryWriteIdentity,
    /// Logic proposal identifier.
    pub proposal_id: String,
    /// Maximum retained bytes.
    pub max_bytes: u32,
    /// Trigger boundary that proposed the write.
    pub trigger: String,
    /// Latest durable outbox state.
    pub state: MemoryWriteState,
    /// Digest of the approved action, once policy succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
    /// Canonical proposal sequence.
    pub proposed_at: Sequence,
    /// Canonical approval sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<Sequence>,
    /// Canonical dispatch sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<Sequence>,
    /// Canonical terminal sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Provider-local stable reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    /// Whether the provider retained the content.
    #[serde(default)]
    pub retained: bool,
    /// Whether an identical canonical write was already retained.
    #[serde(default)]
    pub deduplicated: bool,
    /// Stable failure code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_code: Option<String>,
}

impl MemoryWriteRecord {
    /// Returns whether a terminal receipt or failure already exists.
    #[must_use]
    pub const fn has_terminal_evidence(&self) -> bool {
        matches!(
            self.state,
            MemoryWriteState::Completed | MemoryWriteState::Failed
        )
    }
}

/// Replay-owned artifact-handoff compaction outbox state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextArtifactState {
    /// Proposal is canonical; no policy outcome is canonical yet.
    Proposed,
    /// Policy approved the exact action; no storage call is canonical yet.
    Approved,
    /// Dispatch intent is canonical; recovery must use an exact terminal receipt.
    Dispatched,
    /// A terminal artifact-store receipt is canonical.
    Completed,
    /// Terminal failure without a receipt is canonical.
    Failed,
}

/// Replay-owned artifact-handoff compaction outbox record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextArtifactRecord {
    /// Exact context-artifact identity.
    pub identity: ContextArtifactIdentity,
    /// Requested and persisted byte count.
    pub byte_size: u64,
    /// Latest durable outbox state.
    pub state: ContextArtifactState,
    /// Digest of the approved action, once policy succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_digest: Option<ContentHash>,
    /// Canonical proposal sequence.
    pub proposed_at: Sequence,
    /// Canonical proposal event used as dependency creation provenance.
    pub proposed_event: EventId,
    /// Canonical approval sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<Sequence>,
    /// Canonical dispatch sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<Sequence>,
    /// Canonical terminal sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Content-addressed artifact ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    /// Portable immutable artifact reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_reference: Option<String>,
}

impl ContextArtifactRecord {
    /// Returns whether a terminal receipt or failure already exists.
    #[must_use]
    pub const fn has_terminal_evidence(&self) -> bool {
        matches!(
            self.state,
            ContextArtifactState::Completed | ContextArtifactState::Failed
        )
    }
}

impl ArtifactPersistenceRecord {
    /// Returns the only restart action legal at this canonical cut.
    #[must_use]
    pub const fn resume_action(&self) -> ArtifactPersistenceResumeAction {
        match self.state {
            ArtifactPersistenceState::Proposed => {
                ArtifactPersistenceResumeAction::AwaitPolicyRecovery
            }
            ArtifactPersistenceState::Approved => ArtifactPersistenceResumeAction::DispatchApproved,
            ArtifactPersistenceState::Dispatched => {
                ArtifactPersistenceResumeAction::ReconcileReceipt
            }
            ArtifactPersistenceState::Completed => ArtifactPersistenceResumeAction::CompleteNode,
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
    /// Immutable parent/task ownership for a runtime-managed worker session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_origin: Option<ChildSessionOrigin>,
    /// Durable lifecycle.
    pub lifecycle: SessionLifecycle,
    /// Canonical content and provider projection.
    pub conversation: ConversationState,
    /// Durable approval continuations.
    pub approvals: BTreeMap<ContinuationId, ApprovalRecord>,
    /// Durable tool-dispatch outbox projection keyed by provider call ID.
    #[serde(default)]
    pub tool_executions: BTreeMap<String, ToolExecutionRecord>,
    /// Artifact-persistence outbox records keyed by stable execution identity.
    #[serde(default)]
    pub artifact_persistences: BTreeMap<String, ArtifactPersistenceRecord>,
    /// Runtime-managed child sessions keyed by deterministic execution ID.
    #[serde(default)]
    pub child_agents: BTreeMap<String, ChildAgentRecord>,
    /// Planner/worker/reviewer task, join, and review projection.
    #[serde(default)]
    pub planner_worker: PlannerWorkerState,
    /// Replay-owned plugin activation and blocking invocation projection.
    #[serde(default)]
    pub plugins: PluginExecutionState,
    /// Restart/reconnect reconciliation state keyed by provider call ID.
    #[serde(default)]
    pub process_reconciliations: BTreeMap<String, ProcessReconciliationRecord>,
    /// Live typed-summary compaction outbox keyed by summary execution ID.
    #[serde(default)]
    pub context_summaries: BTreeMap<String, ContextSummaryRecord>,
    /// Automatic memory-write outbox keyed by canonical write identity.
    #[serde(default)]
    pub memory_writes: BTreeMap<String, MemoryWriteRecord>,
    /// Artifact-handoff compaction outbox keyed by execution identity.
    #[serde(default)]
    pub context_artifacts: BTreeMap<String, ContextArtifactRecord>,
    /// Last applied sequence.
    pub last_sequence: Sequence,
    /// Integrity checksum of the last applied event.
    pub last_event_checksum: ContentHash,
}

/// Replay-owned plugin composition projection.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginExecutionState {
    /// Exact currently activated plugin set.
    pub activated_plugin_ids: Vec<String>,
    /// Latest activation sequence.
    pub activated_at: Option<Sequence>,
    /// Canonical blocking invocation records in execution order.
    pub invocations: Vec<PluginInvocationRecord>,
}

/// Replay-owned blocking plugin invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginInvocationRecord {
    /// Plugin identity.
    pub plugin_id: String,
    /// Interceptor declaration ID.
    pub handler: String,
    /// Consequential action boundary.
    pub action_kind: String,
    /// Stable proposal identity.
    pub proposal_id: String,
    /// Normalized outcome.
    pub outcome: String,
    /// Canonical event sequence.
    pub committed_at: Sequence,
}

/// Replay-derived branch ancestry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionAncestry {
    /// Immutable parent session.
    pub parent_session_id: SessionId,
    /// Inclusive parent sequence used for the branch.
    pub fork_sequence: Sequence,
}

/// Replay-derived worker ownership and typed input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildSessionOrigin {
    /// Runtime-managed parent.
    pub parent_session_id: SessionId,
    /// Canonical parent proposal sequence.
    pub parent_action_sequence: Sequence,
    /// Child journal sequence that established the typed input.
    pub linked_at: Sequence,
    /// Parent graph node that owns this worker.
    pub parent_graph_node_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Zero-based task revision.
    pub revision: u32,
    /// One-based child depth.
    pub depth: u32,
    /// Bounded typed task input.
    pub task: String,
    /// Hash of the exact task bytes.
    pub input_hash: ContentHash,
    /// Hard worker token budget.
    pub token_budget: u64,
}

/// Canonical style execution projection reconstructed without running nodes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleExecutionState {
    /// Exact compiled graph selected by the initialization event.
    pub graph: Box<ExecutableGraph>,
    /// Canonical identity of caller-controlled inputs bound before entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_reference: Option<String>,
    /// Explicit replay control position. This makes control-only crash gaps
    /// distinguishable without inferring intent from unrelated effect events.
    pub control: StyleExecutionControlState,
    /// Node currently executing, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_node: Option<StyleNodeEnteredEvent>,
    /// Canonical sequence at which the active node was entered.
    ///
    /// Recovery uses this exact journal cut to distinguish a node that has not
    /// begun its adapter work from one whose effect evidence is missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_node_entered_at: Option<Sequence>,
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
    /// Recoverable context boundaries retained in canonical execution order.
    #[serde(default)]
    pub context_boundaries: Vec<ContextBoundaryExecutionState>,
    /// Latest provider exchange reconstructed from bounded canonical deltas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_model_execution: Option<ModelExecutionEvidence>,
}

/// Bounded replay evidence for the latest started provider exchange.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelExecutionEvidence {
    /// Exact turn cancellation/run identity.
    pub cancellation_id: String,
    /// Latest canonical user input owning this exchange chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_sequence: Option<Sequence>,
    /// Canonical model-start sequence.
    pub started_at: Sequence,
    /// Concatenated visible deltas for the current turn across provider exchanges.
    pub visible_text: String,
    /// Exact structured tool proposals observed in this provider exchange
    /// chain, retained in canonical order for restart reconstruction.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_proposals: Vec<ModelToolCallProposedEvent>,
    /// Matching successful terminal completion sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Whether terminal evidence was a normal model response rather than a
    /// tool-call handoff.
    #[serde(default)]
    pub response_completed: bool,
}

/// Replay-derived progress for one context-composition boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBoundaryExecutionState {
    /// Stable boundary identity.
    pub identity: ContextBoundaryIdentity,
    /// Completed phases in canonical order.
    pub completed_phases: Vec<String>,
    /// Phases whose blocking pipeline may have been invoked.
    #[serde(default)]
    pub started_phases: Vec<String>,
    /// Latest event belonging to this boundary.
    pub last_sequence: Sequence,
    /// Terminal boundary event sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Sequence>,
    /// Final approximate token pressure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_tokens: Option<u64>,
    /// Final exact serialized bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serialized_bytes: Option<u64>,
    /// Exact projection replacement event that completed a phase, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_replacement_event: Option<EventId>,
}

/// Exact replay-derived control position for a compiled style graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum StyleExecutionControlState {
    /// The next legal event is entry into this node.
    ReadyForEntry(StyleExecutionCursor),
    /// A node has been entered and has not produced a canonical outcome.
    Active(StyleNodeEnteredEvent),
    /// A node completed and its deterministic transition is not yet selected.
    AwaitingTransition(StyleNodeCompletedEvent),
    /// A transition was selected and its destination has not yet been entered.
    AwaitingDestinationEntry(StyleTransitionSelectedEvent),
    /// Style execution ended and cannot accept another node.
    Terminal {
        /// Stable terminal reason.
        reason: String,
    },
}

/// Expected identity and counters for the next node-entry event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StyleExecutionCursor {
    /// Stable compiled graph node ID.
    pub node_id: String,
    /// One-based execution attempt.
    pub attempt: u32,
    /// Zero-based loop iteration.
    pub loop_iteration: u32,
    /// One-based, session-monotonic graph step.
    pub step: u64,
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
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolExecutionRecord {
    /// Stable execution identity, absent for policy-denial terminals.
    pub execution_id: Option<String>,
    /// Provider call identifier.
    pub call_id: String,
    /// Exact approved action digest.
    pub action_digest: Option<ContentHash>,
    /// Latest durable execution state.
    pub state: ToolExecutionState,
    /// Dispatch sequence.
    pub dispatched_at: Option<Sequence>,
    /// Terminal sequence when known.
    pub terminal_at: Option<Sequence>,
    /// Number of host lifecycle items durably projected into the journal.
    #[serde(default)]
    pub observed_event_count: u64,
    /// Exact bounded terminal payload needed to reconstruct provider context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_outcome: Option<ToolExecutionTerminalOutcome>,
}

/// Canonical terminal tool outcome retained for projection repair.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum ToolExecutionTerminalOutcome {
    /// Tool host completed with a bounded structured result.
    Completed {
        /// Bounded structured result.
        result: serde_json::Value,
        /// Optional immutable full-output artifact.
        artifact: Option<String>,
        /// Whether the host result was already truncated.
        truncated: bool,
    },
    /// Tool execution or policy failed.
    Failed {
        /// Stable failure code.
        code: String,
        /// Redacted failure message.
        message: String,
        /// Whether retry may be legal.
        retryable: bool,
    },
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
        child_origin: None,
        lifecycle: SessionLifecycle::Active,
        conversation: ConversationState::new(),
        approvals: BTreeMap::new(),
        tool_executions: BTreeMap::new(),
        artifact_persistences: BTreeMap::new(),
        child_agents: BTreeMap::new(),
        planner_worker: PlannerWorkerState::default(),
        plugins: PluginExecutionState::default(),
        process_reconciliations: BTreeMap::new(),
        context_summaries: BTreeMap::new(),
        memory_writes: BTreeMap::new(),
        context_artifacts: BTreeMap::new(),
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
        RuntimeCommittedEvent::ChildSessionLinked(linked) => {
            apply_child_session_link(state, linked, event.metadata.sequence)
        }
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
            if let Some(phase) = &replaced.context_phase {
                apply_context_phase_completed(state, phase, event.metadata.sequence)?;
                let boundary = style_execution_mut(state)?
                    .context_boundaries
                    .last_mut()
                    .filter(|boundary| boundary.identity == phase.boundary)
                    .ok_or(SessionReducerError::InvalidContextBoundaryTransition)?;
                boundary.phase_replacement_event = Some(event.metadata.event_id);
            }
            Ok(())
        }
        RuntimeCommittedEvent::ContextBoundaryStarted(started) => {
            apply_context_boundary_started(state, started, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextPhaseStarted(started) => {
            apply_context_phase_started(state, &started.identity, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextPhaseCompleted(completed) => {
            apply_context_phase_completed(state, &completed.identity, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextBoundaryCompleted(completed) => {
            apply_context_boundary_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ModelRequestProposed(_)
        | RuntimeCommittedEvent::ModelRequestApproved(_)
        | RuntimeCommittedEvent::ModelToolCallDeltaObserved(_)
        | RuntimeCommittedEvent::ToolCallProposed(_)
        | RuntimeCommittedEvent::ToolCallApproved(_)
        | RuntimeCommittedEvent::SchedulerFired(_)
        | RuntimeCommittedEvent::SchedulerDeliveryReconciled(_) => Ok(()),
        RuntimeCommittedEvent::PluginSetActivated(activated) => {
            if activated.plugin_set_hash
                != state
                    .style_binding
                    .as_ref()
                    .ok_or(SessionReducerError::MissingStyleBinding)?
                    .plugin_set_hash
                || activated.plugin_ids.iter().collect::<BTreeSet<_>>().len()
                    != activated.plugin_ids.len()
            {
                return Err(SessionReducerError::InvalidPluginActivation);
            }
            state
                .plugins
                .activated_plugin_ids
                .clone_from(&activated.plugin_ids);
            state.plugins.activated_at = Some(event.metadata.sequence);
            Ok(())
        }
        RuntimeCommittedEvent::PluginInvocationCompleted(completed) => {
            if !state
                .plugins
                .activated_plugin_ids
                .contains(&completed.plugin_id)
                || completed.handler.trim().is_empty()
                || completed.action_kind.trim().is_empty()
                || completed.proposal_id.trim().is_empty()
                || completed.outcome.trim().is_empty()
            {
                return Err(SessionReducerError::InvalidPluginInvocation);
            }
            state.plugins.invocations.push(PluginInvocationRecord {
                plugin_id: completed.plugin_id.clone(),
                handler: completed.handler.clone(),
                action_kind: completed.action_kind.clone(),
                proposal_id: completed.proposal_id.clone(),
                outcome: completed.outcome.clone(),
                committed_at: event.metadata.sequence,
            });
            Ok(())
        }
        RuntimeCommittedEvent::ModelRequestStarted(started) => {
            let user_sequence = state
                .conversation
                .provider_projection()
                .iter()
                .rev()
                .find_map(|entry| match entry {
                    ConversationEntry::UserMessage(user) => Some(user.source_sequence),
                    ConversationEntry::PendingTask(task) => Some(task.source_sequence),
                    _ => None,
                });
            if let Some(execution) = state.style_execution.as_mut() {
                if let Some(evidence) = execution.latest_model_execution.as_mut() {
                    if evidence.completed_at.is_none()
                        || evidence.cancellation_id != started.cancellation_id
                        || evidence.user_sequence != user_sequence
                    {
                        return Err(SessionReducerError::InvalidModelExecutionEvidence);
                    }
                    evidence.started_at = event.metadata.sequence;
                    evidence.completed_at = None;
                    evidence.response_completed = false;
                } else {
                    execution.latest_model_execution = Some(ModelExecutionEvidence {
                        cancellation_id: started.cancellation_id.clone(),
                        user_sequence,
                        started_at: event.metadata.sequence,
                        visible_text: String::new(),
                        tool_proposals: Vec::new(),
                        completed_at: None,
                        response_completed: false,
                    });
                }
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelOutputDeltaObserved(observed) => {
            if let Some(execution) = state.style_execution.as_mut() {
                let Some(evidence) = execution.latest_model_execution.as_mut() else {
                    return Ok(());
                };
                if evidence.cancellation_id != observed.cancellation_id
                    || evidence.completed_at.is_some()
                {
                    return Err(SessionReducerError::InvalidModelExecutionEvidence);
                }
                let next_len = evidence
                    .visible_text
                    .len()
                    .checked_add(observed.text.len())
                    .ok_or(SessionReducerError::ModelOutputEvidenceOverflow)?;
                if next_len > MAX_REPLAY_VISIBLE_MODEL_BYTES {
                    return Err(SessionReducerError::ModelOutputEvidenceOverflow);
                }
                evidence.visible_text.push_str(&observed.text);
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelRequestCancelled(cancelled) => {
            if let Some(execution) = state.style_execution.as_mut()
                && execution
                    .latest_model_execution
                    .as_ref()
                    .is_some_and(|evidence| evidence.cancellation_id == cancelled.cancellation_id)
            {
                execution.latest_model_execution = None;
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelRequestFailed(_) => {
            if let Some(execution) = state.style_execution.as_mut() {
                execution.latest_model_execution = None;
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelToolCallProposed(proposed) => {
            if let Some(execution) = state.style_execution.as_mut()
                && let Some(evidence) = execution.latest_model_execution.as_mut()
                && evidence.completed_at.is_none()
            {
                if evidence
                    .tool_proposals
                    .iter()
                    .any(|existing| existing.call_id == proposed.call_id)
                {
                    return Err(SessionReducerError::InvalidModelExecutionEvidence);
                }
                evidence.tool_proposals.push(proposed.clone());
                evidence.completed_at = Some(event.metadata.sequence);
            }
            Ok(())
        }
        RuntimeCommittedEvent::ModelResponseCompleted(completed) => {
            if let Some(execution) = state.style_execution.as_mut() {
                if let Some(evidence) = execution.latest_model_execution.as_mut() {
                    if evidence.cancellation_id != completed.cancellation_id
                        || evidence.response_completed
                    {
                        return Err(SessionReducerError::InvalidModelExecutionEvidence);
                    }
                    evidence.completed_at = Some(event.metadata.sequence);
                    evidence.response_completed = true;
                }
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
            apply_style_node_entered(state, entered, event.metadata.sequence)
        }
        RuntimeCommittedEvent::StyleNodeCompleted(completed) => {
            apply_style_node_completed(state, completed)
        }
        RuntimeCommittedEvent::StyleNodeFailed(failed) => apply_style_node_failed(state, failed),
        RuntimeCommittedEvent::StyleTransitionSelected(selected) => {
            apply_style_transition_selected(state, selected)
        }
        RuntimeCommittedEvent::StyleExecutionTerminated(terminated) => {
            apply_style_execution_terminated(state, terminated)
        }
        RuntimeCommittedEvent::ArtifactPersistenceProposed(proposed) => {
            apply_artifact_persistence_proposed(
                state,
                proposed,
                event.metadata.sequence,
                event.metadata.event_id,
            )
        }
        RuntimeCommittedEvent::ArtifactPersistenceApproved(approved) => {
            apply_artifact_persistence_approved(state, approved, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ArtifactPersistenceDispatched(dispatched) => {
            apply_artifact_persistence_dispatched(state, dispatched, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ArtifactPersistenceCompleted(completed) => {
            apply_artifact_persistence_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ChildAgentCreationProposed(proposed) => {
            apply_child_agent_creation_proposed(state, proposed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ChildAgentCreationApproved(approved) => {
            apply_child_agent_creation_approved(state, approved, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ChildAgentCreated(created) => {
            apply_child_agent_created(state, created, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ChildAgentCompleted(completed) => {
            apply_child_agent_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::TaskPlanCommitted(committed) => {
            apply_task_plan_committed(state, committed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ChildJoinCompleted(completed) => {
            apply_child_join_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ReviewerFindingsCommitted(committed) => {
            apply_reviewer_findings_committed(state, committed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextSummaryProposed(proposed) => {
            apply_context_summary_proposed(state, proposed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextSummaryApproved(approved) => {
            apply_context_summary_approved(state, approved, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextSummaryStarted(started) => {
            apply_context_summary_started(state, started, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextSummaryCompleted(completed) => {
            apply_context_summary_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextSummaryFailed(failed) => {
            apply_context_summary_failed(state, failed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::MemoryWriteProposed(proposed) => {
            apply_memory_write_proposed(state, proposed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::MemoryWriteApproved(approved) => {
            apply_memory_write_approved(state, approved, event.metadata.sequence)
        }
        RuntimeCommittedEvent::MemoryWriteDispatched(dispatched) => {
            apply_memory_write_dispatched(state, dispatched, event.metadata.sequence)
        }
        RuntimeCommittedEvent::MemoryWriteCompleted(completed) => {
            apply_memory_write_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::MemoryWriteFailed(failed) => {
            apply_memory_write_failed(state, failed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextArtifactProposed(proposed) => {
            apply_context_artifact_proposed(
                state,
                proposed,
                event.metadata.sequence,
                event.metadata.event_id,
            )
        }
        RuntimeCommittedEvent::ContextArtifactApproved(approved) => {
            apply_context_artifact_approved(state, approved, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextArtifactDispatched(dispatched) => {
            apply_context_artifact_dispatched(state, dispatched, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextArtifactCompleted(completed) => {
            apply_context_artifact_completed(state, completed, event.metadata.sequence)
        }
        RuntimeCommittedEvent::ContextArtifactFailed(failed) => {
            apply_context_artifact_failed(state, failed, event.metadata.sequence)
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
        RuntimeCommittedEvent::ToolExecutionCompleted(completed) => mark_tool_terminal(
            state,
            &completed.call_id,
            event.metadata.sequence,
            None,
            ToolExecutionTerminalOutcome::Completed {
                result: completed.result.clone(),
                artifact: completed.artifact.clone(),
                truncated: completed.truncated,
            },
        ),
        RuntimeCommittedEvent::ToolExecutionFailed(failed) => mark_tool_terminal(
            state,
            &failed.call_id,
            event.metadata.sequence,
            failed.action_digest,
            ToolExecutionTerminalOutcome::Failed {
                code: failed.code.clone(),
                message: failed.message.clone(),
                retryable: failed.retryable,
            },
        ),
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

fn apply_child_session_link(
    state: &mut SessionState,
    linked: &ChildSessionLinkedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if sequence.get() != 2
        || state.ancestry.is_some()
        || state.child_origin.is_some()
        || linked.parent_session_id == state.id
        || linked.parent_graph_node_id.trim().is_empty()
        || linked.task_id.trim().is_empty()
        || linked.task.trim().is_empty()
        || linked.task.len() > 64 * 1024
        || linked.input_hash != ContentHash::digest(linked.task.as_bytes())
        || linked.depth == 0
        || linked.token_budget == 0
    {
        return Err(SessionReducerError::InvalidChildSessionLink);
    }
    state.child_origin = Some(ChildSessionOrigin {
        parent_session_id: linked.parent_session_id,
        parent_action_sequence: linked.parent_action_sequence,
        linked_at: sequence,
        parent_graph_node_id: linked.parent_graph_node_id.clone(),
        task_id: linked.task_id.clone(),
        revision: linked.revision,
        depth: linked.depth,
        task: linked.task.clone(),
        input_hash: linked.input_hash,
        token_budget: linked.token_budget,
    });
    Ok(())
}

fn apply_context_boundary_started(
    state: &mut SessionState,
    started: &ContextBoundaryStartedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    let active = execution
        .active_node
        .as_ref()
        .ok_or(SessionReducerError::InvalidContextBoundaryTransition)?;
    if active.node_id != started.identity.node_id
        || started.identity.run_id.trim().is_empty()
        || !matches!(
            started.identity.boundary.as_str(),
            "turn_start" | "before_model_request" | "before_turn_completion"
        )
        || started
            .identity
            .source_head
            .checked_next()
            .map_err(|_| SessionReducerError::SequenceOverflow)?
            != sequence
        || execution
            .context_boundaries
            .iter()
            .any(|boundary| boundary.completed_at.is_none())
        || execution
            .context_boundaries
            .iter()
            .any(|boundary| boundary.identity == started.identity)
    {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    match (started.identity.boundary.as_str(), started.identity.origin) {
        ("turn_start", ContextBoundaryOrigin::UserTurn | ContextBoundaryOrigin::ChildTask) => {}
        (
            "before_model_request",
            ContextBoundaryOrigin::UserTurn | ContextBoundaryOrigin::ChildTask,
        ) => {
            let Some(previous) = execution.context_boundaries.last() else {
                return Err(SessionReducerError::InvalidContextBoundaryTransition);
            };
            let same_node = previous.identity.node_id == started.identity.node_id;
            let exact_context_to_model_edge =
                graph_node_kind(&execution.graph, &previous.identity.node_id)
                    == Some(NodeKind::ContextTransform)
                    && graph_node_kind(&execution.graph, &started.identity.node_id)
                        == Some(NodeKind::ModelCall)
                    && graph_has_transition(
                        &execution.graph,
                        &previous.identity.node_id,
                        &started.identity.node_id,
                    );
            if previous.completed_at.is_none()
                || previous.identity.boundary != "turn_start"
                || previous.identity.origin != started.identity.origin
                || (!same_node && !exact_context_to_model_edge)
                || previous.identity.run_id != started.identity.run_id
                || previous.identity.request_hash != started.identity.request_hash
            {
                return Err(SessionReducerError::InvalidContextBoundaryTransition);
            }
        }
        (
            "before_model_request",
            ContextBoundaryOrigin::ToolContinuation | ContextBoundaryOrigin::ApprovalContinuation,
        ) => {
            let is_tool_node =
                execution.graph.nodes.iter().any(|node| {
                    node.id == active.node_id && node.kind == NodeKind::ToolExecutionGate
                });
            if !is_tool_node {
                return Err(SessionReducerError::InvalidContextBoundaryTransition);
            }
        }
        (
            "before_turn_completion",
            ContextBoundaryOrigin::UserTurn | ContextBoundaryOrigin::ChildTask,
        ) => {
            let is_complete_turn = execution
                .graph
                .nodes
                .iter()
                .any(|node| node.id == active.node_id && node.kind == NodeKind::CompleteTurn);
            if !is_complete_turn {
                return Err(SessionReducerError::InvalidContextBoundaryTransition);
            }
        }
        _ => return Err(SessionReducerError::InvalidContextBoundaryTransition),
    }
    execution
        .context_boundaries
        .push(ContextBoundaryExecutionState {
            identity: started.identity.clone(),
            completed_phases: Vec::new(),
            started_phases: Vec::new(),
            last_sequence: sequence,
            completed_at: None,
            estimated_tokens: None,
            serialized_bytes: None,
            phase_replacement_event: None,
        });
    Ok(())
}

fn apply_context_phase_completed(
    state: &mut SessionState,
    phase: &ContextPhaseIdentity,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if !matches!(phase.phase.as_str(), "memory" | "compaction" | "discard") {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    let execution = style_execution_mut(state)?;
    let boundary = execution
        .context_boundaries
        .last_mut()
        .filter(|candidate| candidate.identity == phase.boundary)
        .ok_or(SessionReducerError::InvalidContextBoundaryTransition)?;
    let exact_phase_order = match phase.phase.as_str() {
        "memory" => {
            boundary.started_phases.as_slice() == ["memory"] && boundary.completed_phases.is_empty()
        }
        "compaction" => {
            boundary.identity.boundary == "before_model_request"
                && boundary.started_phases.as_slice() == ["memory", "compaction"]
                && boundary.completed_phases.as_slice() == ["memory"]
        }
        "discard" => {
            boundary.identity.boundary == "before_turn_completion"
                && boundary.started_phases.as_slice() == ["discard"]
                && boundary.completed_phases.is_empty()
        }
        _ => false,
    };
    if boundary.completed_at.is_some()
        || !exact_phase_order
        || boundary
            .last_sequence
            .checked_next()
            .map_err(|_| SessionReducerError::SequenceOverflow)?
            != sequence
    {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    boundary.completed_phases.push(phase.phase.clone());
    boundary.last_sequence = sequence;
    Ok(())
}

fn apply_context_phase_started(
    state: &mut SessionState,
    phase: &ContextPhaseIdentity,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if !matches!(phase.phase.as_str(), "memory" | "compaction" | "discard") {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    let execution = style_execution_mut(state)?;
    let boundary = execution
        .context_boundaries
        .last_mut()
        .filter(|candidate| candidate.identity == phase.boundary)
        .ok_or(SessionReducerError::InvalidContextBoundaryTransition)?;
    let exact_phase_order = match phase.phase.as_str() {
        "memory" => boundary.started_phases.is_empty() && boundary.completed_phases.is_empty(),
        "compaction" => {
            boundary.identity.boundary == "before_model_request"
                && boundary.started_phases.as_slice() == ["memory"]
                && boundary.completed_phases.as_slice() == ["memory"]
        }
        "discard" => {
            boundary.identity.boundary == "before_turn_completion"
                && boundary.started_phases.is_empty()
                && boundary.completed_phases.is_empty()
        }
        _ => false,
    };
    if boundary.completed_at.is_some()
        || !exact_phase_order
        || boundary
            .last_sequence
            .checked_next()
            .map_err(|_| SessionReducerError::SequenceOverflow)?
            != sequence
    {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    boundary.started_phases.push(phase.phase.clone());
    boundary.last_sequence = sequence;
    Ok(())
}

fn apply_context_boundary_completed(
    state: &mut SessionState,
    completed: &ContextBoundaryCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let measurement = measure_projection(state.conversation.provider_projection())
        .map_err(|_| SessionReducerError::InvalidContextBoundaryMeasurement)?;
    if measurement.projection_hash != completed.projection_hash
        || measurement.estimated_tokens != completed.estimated_tokens
        || measurement.serialized_bytes != completed.serialized_bytes
    {
        return Err(SessionReducerError::InvalidContextBoundaryMeasurement);
    }
    let execution = style_execution_mut(state)?;
    let boundary = execution
        .context_boundaries
        .last_mut()
        .filter(|candidate| candidate.identity == completed.identity)
        .ok_or(SessionReducerError::InvalidContextBoundaryTransition)?;
    let exact_phases = match completed.identity.boundary.as_str() {
        "turn_start" => {
            boundary.started_phases.as_slice() == ["memory"]
                && boundary.completed_phases.as_slice() == ["memory"]
        }
        "before_model_request" => {
            boundary.started_phases.as_slice() == ["memory", "compaction"]
                && boundary.completed_phases.as_slice() == ["memory", "compaction"]
        }
        "before_turn_completion" => {
            boundary.started_phases.as_slice() == ["discard"]
                && boundary.completed_phases.as_slice() == ["discard"]
        }
        _ => false,
    };
    if boundary.completed_at.is_some()
        || !exact_phases
        || boundary
            .last_sequence
            .checked_next()
            .map_err(|_| SessionReducerError::SequenceOverflow)?
            != sequence
    {
        return Err(SessionReducerError::InvalidContextBoundaryTransition);
    }
    boundary.last_sequence = sequence;
    boundary.completed_at = Some(sequence);
    boundary.estimated_tokens = Some(completed.estimated_tokens);
    boundary.serialized_bytes = Some(completed.serialized_bytes);
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
        input_reference: initialized.input_reference.clone(),
        control: StyleExecutionControlState::ReadyForEntry(StyleExecutionCursor {
            node_id: initialized.graph.nodes[initialized.graph.entry_index]
                .id
                .clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }),
        active_node: None,
        active_node_entered_at: None,
        completed_nodes: Vec::new(),
        failed_nodes: Vec::new(),
        transitions: Vec::new(),
        termination_reason: None,
        input_tokens: 0,
        output_tokens: 0,
        tokens_at_last_compaction: 0,
        context_boundaries: Vec::new(),
        latest_model_execution: None,
    });
    Ok(())
}

fn apply_style_node_entered(
    state: &mut SessionState,
    entered: &StyleNodeEnteredEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    let expected = match &execution.control {
        StyleExecutionControlState::ReadyForEntry(expected) => expected.clone(),
        StyleExecutionControlState::AwaitingDestinationEntry(selected) => StyleExecutionCursor {
            node_id: selected.to_node_id.clone(),
            attempt: selected.attempt,
            loop_iteration: if graph_node_kind(&execution.graph, &selected.from_node_id)
                == Some(NodeKind::Loop)
                && graph_node_kind(&execution.graph, &selected.to_node_id)
                    != Some(NodeKind::CompleteSession)
            {
                selected
                    .loop_iteration
                    .checked_add(1)
                    .ok_or(SessionReducerError::StyleLoopIterationOverflow)?
            } else {
                selected.loop_iteration
            },
            step: selected
                .step
                .checked_add(1)
                .ok_or(SessionReducerError::StyleStepOverflow)?,
        },
        StyleExecutionControlState::Active(_)
        | StyleExecutionControlState::AwaitingTransition(_)
        | StyleExecutionControlState::Terminal { .. } => {
            return Err(SessionReducerError::InvalidStyleExecutionTransition);
        }
    };
    if execution.active_node.is_some()
        || execution.termination_reason.is_some()
        || !entry_matches(&expected, entered)
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.active_node = Some(entered.clone());
    execution.active_node_entered_at = Some(sequence);
    if matches!(
        graph_node_kind(&execution.graph, &entered.node_id),
        Some(NodeKind::ModelCall | NodeKind::Review)
    ) {
        execution.latest_model_execution = None;
    }
    execution.control = StyleExecutionControlState::Active(entered.clone());
    Ok(())
}

fn active_artifact_persistence_identity_matches(
    state: &SessionState,
    identity: &ArtifactPersistenceIdentity,
) -> bool {
    let Some(execution) = state.style_execution.as_ref() else {
        return false;
    };
    !identity.execution_id.trim().is_empty()
        && !identity.proposal_id.trim().is_empty()
        && !identity.node_id.trim().is_empty()
        && valid_style_counters(identity.attempt, identity.step)
        && graph_node_kind(&execution.graph, &identity.node_id) == Some(NodeKind::PersistArtifact)
        && active_node_matches(
            execution.active_node.as_ref(),
            &identity.node_id,
            identity.attempt,
            identity.loop_iteration,
            identity.step,
        )
        && matches!(
            &execution.control,
            StyleExecutionControlState::Active(active)
                if active_node_matches(
                    Some(active),
                    &identity.node_id,
                    identity.attempt,
                    identity.loop_iteration,
                    identity.step
                )
        )
}

fn apply_artifact_persistence_proposed(
    state: &mut SessionState,
    proposed: &ArtifactPersistenceProposedEvent,
    sequence: Sequence,
    event_id: EventId,
) -> Result<(), SessionReducerError> {
    let identity = &proposed.identity;
    if proposed.mime_type.trim().is_empty()
        || proposed.byte_size == 0
        || !active_artifact_persistence_identity_matches(state, identity)
    {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    if state
        .artifact_persistences
        .contains_key(&identity.execution_id)
        || state.artifact_persistences.values().any(|record| {
            record.identity.proposal_id == identity.proposal_id
                || (record.identity.node_id == identity.node_id
                    && record.identity.attempt == identity.attempt
                    && record.identity.loop_iteration == identity.loop_iteration
                    && record.identity.step == identity.step)
        })
    {
        return Err(SessionReducerError::DuplicateArtifactPersistence);
    }
    state.artifact_persistences.insert(
        identity.execution_id.clone(),
        ArtifactPersistenceRecord {
            identity: identity.clone(),
            mime_type: proposed.mime_type.clone(),
            byte_size: proposed.byte_size,
            state: ArtifactPersistenceState::Proposed,
            action_digest: None,
            proposed_at: sequence,
            proposed_event: event_id,
            approved_at: None,
            dispatched_at: None,
            completed_at: None,
            artifact_id: None,
            artifact_reference: None,
        },
    );
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_artifact_persistence_approved(
    state: &mut SessionState,
    approved: &ArtifactPersistenceApprovedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if !active_artifact_persistence_identity_matches(state, &approved.identity) {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    let record = state
        .artifact_persistences
        .get_mut(&approved.identity.execution_id)
        .ok_or(SessionReducerError::InvalidArtifactPersistenceTransition)?;
    if record.state != ArtifactPersistenceState::Proposed
        || record.identity != approved.identity
        || record.action_digest.is_some()
        || record.approved_at.is_some()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    record.state = ArtifactPersistenceState::Approved;
    record.action_digest = Some(approved.action_digest);
    record.approved_at = Some(sequence);
    Ok(())
}

fn apply_artifact_persistence_dispatched(
    state: &mut SessionState,
    dispatched: &ArtifactPersistenceDispatchedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if !active_artifact_persistence_identity_matches(state, &dispatched.identity) {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    let record = state
        .artifact_persistences
        .get_mut(&dispatched.identity.execution_id)
        .ok_or(SessionReducerError::InvalidArtifactPersistenceTransition)?;
    if record.state != ArtifactPersistenceState::Approved
        || record.identity != dispatched.identity
        || record.action_digest != Some(dispatched.action_digest)
        || record.approved_at.is_none()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    record.state = ArtifactPersistenceState::Dispatched;
    record.dispatched_at = Some(sequence);
    Ok(())
}

fn apply_artifact_persistence_completed(
    state: &mut SessionState,
    completed: &ArtifactPersistenceCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if completed.artifact_id.trim().is_empty()
        || completed.artifact_reference.trim().is_empty()
        || completed.mime_type.trim().is_empty()
        || !active_artifact_persistence_identity_matches(state, &completed.identity)
    {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    let record = state
        .artifact_persistences
        .get_mut(&completed.identity.execution_id)
        .ok_or(SessionReducerError::InvalidArtifactPersistenceTransition)?;
    if record.state != ArtifactPersistenceState::Dispatched
        || record.identity != completed.identity
        || record.action_digest != Some(completed.action_digest)
        || record.mime_type != completed.mime_type
        || record.byte_size != completed.byte_size
        || record.approved_at.is_none()
        || record.dispatched_at.is_none()
        || record.completed_at.is_some()
        || record.artifact_id.is_some()
        || record.artifact_reference.is_some()
    {
        return Err(SessionReducerError::InvalidArtifactPersistenceTransition);
    }
    record.state = ArtifactPersistenceState::Completed;
    record.completed_at = Some(sequence);
    record.artifact_id = Some(completed.artifact_id.clone());
    record.artifact_reference = Some(completed.artifact_reference.clone());
    Ok(())
}

/// Advances the active open context boundary's journal head for boundary-scoped
/// outbox events (typed-summary and artifact-handoff compaction) that commit
/// between a phase start and its replacement/phase completion.
fn advance_open_boundary(
    state: &mut SessionState,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if let Some(execution) = state.style_execution.as_mut()
        && let Some(boundary) = execution.context_boundaries.last_mut()
        && boundary.completed_at.is_none()
    {
        if boundary.last_sequence.checked_next() != Ok(sequence) {
            return Err(SessionReducerError::InvalidContextBoundaryTransition);
        }
        boundary.last_sequence = sequence;
    }
    Ok(())
}

fn valid_summary_identity(identity: &ContextSummaryIdentity) -> bool {
    !identity.summary_id.trim().is_empty()
        && !identity.provider.trim().is_empty()
        && !identity.model.trim().is_empty()
        && identity.schema_version == 1
        && identity.max_summary_bytes > 0
        && identity.max_summary_bytes <= 1024 * 1024
}

fn apply_context_summary_proposed(
    state: &mut SessionState,
    proposed: &ContextSummaryProposedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let identity = &proposed.identity;
    if !valid_summary_identity(identity)
        || identity
            .source_range
            .is_some_and(|(start, end)| start > end)
        || state.context_summaries.contains_key(&identity.summary_id)
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    state.context_summaries.insert(
        identity.summary_id.clone(),
        ContextSummaryRecord {
            identity: identity.clone(),
            state: ContextSummaryState::Proposed,
            action_digest: None,
            proposed_at: sequence,
            approved_at: None,
            started_at: None,
            completed_at: None,
            content_hash: None,
            text: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    );
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn summary_record_mut<'a>(
    state: &'a mut SessionState,
    identity: &ContextSummaryIdentity,
) -> Result<&'a mut ContextSummaryRecord, SessionReducerError> {
    let record = state
        .context_summaries
        .get_mut(&identity.summary_id)
        .ok_or(SessionReducerError::InvalidSummaryTransition)?;
    if record.identity != *identity {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    Ok(record)
}

fn apply_context_summary_approved(
    state: &mut SessionState,
    approved: &ContextSummaryApprovedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = summary_record_mut(state, &approved.identity)?;
    if record.state != ContextSummaryState::Proposed
        || record.action_digest.is_some()
        || record.approved_at.is_some()
        || record.started_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    record.state = ContextSummaryState::Approved;
    record.action_digest = Some(approved.action_digest);
    record.approved_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_summary_started(
    state: &mut SessionState,
    started: &ContextSummaryStartedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = summary_record_mut(state, &started.identity)?;
    if record.state != ContextSummaryState::Approved
        || record.approved_at.is_none()
        || record.started_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    record.state = ContextSummaryState::Started;
    record.started_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_summary_completed(
    state: &mut SessionState,
    completed: &ContextSummaryCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let identity = &completed.identity;
    if completed.text.trim().is_empty()
        || u64::try_from(completed.text.len())
            .map_or(true, |len| len > u64::from(identity.max_summary_bytes))
        || completed.content_hash != ContentHash::digest(completed.text.as_bytes())
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    let record = summary_record_mut(state, identity)?;
    if record.state != ContextSummaryState::Started
        || record.started_at.is_none()
        || record.completed_at.is_some()
        || record.content_hash.is_some()
        || record.text.is_some()
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    record.state = ContextSummaryState::Completed;
    record.completed_at = Some(sequence);
    record.content_hash = Some(completed.content_hash);
    record.text = Some(completed.text.clone());
    record.input_tokens = completed.input_tokens;
    record.output_tokens = completed.output_tokens;
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
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_summary_failed(
    state: &mut SessionState,
    failed: &ContextSummaryFailedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if failed.code.trim().is_empty() {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    let record = summary_record_mut(state, &failed.identity)?;
    if record.state != ContextSummaryState::Started
        || record.started_at.is_none()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidSummaryTransition);
    }
    record.state = ContextSummaryState::Failed;
    record.completed_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn valid_memory_write_identity(identity: &MemoryWriteIdentity) -> bool {
    !identity.write_id.trim().is_empty()
        && !identity.provider.trim().is_empty()
        && !identity.scope.trim().is_empty()
        && !identity.source.trim().is_empty()
}

fn apply_memory_write_proposed(
    state: &mut SessionState,
    proposed: &MemoryWriteProposedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let identity = &proposed.identity;
    if !valid_memory_write_identity(identity)
        || proposed.proposal_id.trim().is_empty()
        || proposed.max_bytes == 0
        || proposed.trigger.trim().is_empty()
        || state.memory_writes.contains_key(&identity.write_id)
    {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    state.memory_writes.insert(
        identity.write_id.clone(),
        MemoryWriteRecord {
            identity: identity.clone(),
            proposal_id: proposed.proposal_id.clone(),
            max_bytes: proposed.max_bytes,
            trigger: proposed.trigger.clone(),
            state: MemoryWriteState::Proposed,
            action_digest: None,
            proposed_at: sequence,
            approved_at: None,
            dispatched_at: None,
            completed_at: None,
            reference: None,
            retained: false,
            deduplicated: false,
            failed_code: None,
        },
    );
    Ok(())
}

fn memory_write_record_mut<'a>(
    state: &'a mut SessionState,
    identity: &MemoryWriteIdentity,
) -> Result<&'a mut MemoryWriteRecord, SessionReducerError> {
    let record = state
        .memory_writes
        .get_mut(&identity.write_id)
        .ok_or(SessionReducerError::InvalidMemoryWriteTransition)?;
    if record.identity != *identity {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    Ok(record)
}

fn apply_memory_write_approved(
    state: &mut SessionState,
    approved: &MemoryWriteApprovedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = memory_write_record_mut(state, &approved.identity)?;
    if record.state != MemoryWriteState::Proposed
        || record.action_digest.is_some()
        || record.approved_at.is_some()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    record.state = MemoryWriteState::Approved;
    record.action_digest = Some(approved.action_digest);
    record.approved_at = Some(sequence);
    Ok(())
}

fn apply_memory_write_dispatched(
    state: &mut SessionState,
    dispatched: &MemoryWriteDispatchedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = memory_write_record_mut(state, &dispatched.identity)?;
    if record.state != MemoryWriteState::Approved
        || record.action_digest != Some(dispatched.action_digest)
        || record.approved_at.is_none()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    record.state = MemoryWriteState::Dispatched;
    record.dispatched_at = Some(sequence);
    Ok(())
}

fn apply_memory_write_completed(
    state: &mut SessionState,
    completed: &MemoryWriteCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if completed.reference.trim().is_empty() {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    let record = memory_write_record_mut(state, &completed.identity)?;
    if record.state != MemoryWriteState::Dispatched
        || record.action_digest != Some(completed.action_digest)
        || record.approved_at.is_none()
        || record.dispatched_at.is_none()
        || record.completed_at.is_some()
        || record.reference.is_some()
    {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    record.state = MemoryWriteState::Completed;
    record.completed_at = Some(sequence);
    record.reference = Some(completed.reference.clone());
    record.retained = completed.retained;
    record.deduplicated = completed.deduplicated;
    Ok(())
}

fn apply_memory_write_failed(
    state: &mut SessionState,
    failed: &MemoryWriteFailedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if failed.code.trim().is_empty() {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    let record = memory_write_record_mut(state, &failed.identity)?;
    if !matches!(
        record.state,
        MemoryWriteState::Proposed | MemoryWriteState::Approved
    ) || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidMemoryWriteTransition);
    }
    record.state = MemoryWriteState::Failed;
    record.completed_at = Some(sequence);
    record.failed_code = Some(failed.code.clone());
    Ok(())
}

fn valid_context_artifact_identity(identity: &ContextArtifactIdentity) -> bool {
    !identity.execution_id.trim().is_empty()
        && !identity.proposal_id.trim().is_empty()
        && !identity.mime_type.trim().is_empty()
        && identity
            .source_range
            .is_some_and(|(start, end)| start <= end)
}

fn apply_context_artifact_proposed(
    state: &mut SessionState,
    proposed: &ContextArtifactProposedEvent,
    sequence: Sequence,
    event_id: EventId,
) -> Result<(), SessionReducerError> {
    let identity = &proposed.identity;
    if !valid_context_artifact_identity(identity)
        || proposed.byte_size == 0
        || state.context_artifacts.contains_key(&identity.execution_id)
        || state
            .context_artifacts
            .values()
            .any(|record| record.identity.proposal_id == identity.proposal_id)
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    state.context_artifacts.insert(
        identity.execution_id.clone(),
        ContextArtifactRecord {
            identity: identity.clone(),
            byte_size: proposed.byte_size,
            state: ContextArtifactState::Proposed,
            action_digest: None,
            proposed_at: sequence,
            proposed_event: event_id,
            approved_at: None,
            dispatched_at: None,
            completed_at: None,
            artifact_id: None,
            artifact_reference: None,
        },
    );
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn context_artifact_record_mut<'a>(
    state: &'a mut SessionState,
    identity: &ContextArtifactIdentity,
) -> Result<&'a mut ContextArtifactRecord, SessionReducerError> {
    let record = state
        .context_artifacts
        .get_mut(&identity.execution_id)
        .ok_or(SessionReducerError::InvalidContextArtifactTransition)?;
    if record.identity != *identity {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    Ok(record)
}

fn apply_context_artifact_approved(
    state: &mut SessionState,
    approved: &ContextArtifactApprovedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = context_artifact_record_mut(state, &approved.identity)?;
    if record.state != ContextArtifactState::Proposed
        || record.action_digest.is_some()
        || record.approved_at.is_some()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    record.state = ContextArtifactState::Approved;
    record.action_digest = Some(approved.action_digest);
    record.approved_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_artifact_dispatched(
    state: &mut SessionState,
    dispatched: &ContextArtifactDispatchedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let record = context_artifact_record_mut(state, &dispatched.identity)?;
    if record.state != ContextArtifactState::Approved
        || record.action_digest != Some(dispatched.action_digest)
        || record.approved_at.is_none()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    record.state = ContextArtifactState::Dispatched;
    record.dispatched_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_artifact_completed(
    state: &mut SessionState,
    completed: &ContextArtifactCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if completed.artifact_id.trim().is_empty()
        || completed.artifact_reference.trim().is_empty()
        || completed.mime_type.trim().is_empty()
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    let record = context_artifact_record_mut(state, &completed.identity)?;
    if record.state != ContextArtifactState::Dispatched
        || record.action_digest != Some(completed.action_digest)
        || record.identity.mime_type != completed.mime_type
        || record.byte_size != completed.byte_size
        || record.approved_at.is_none()
        || record.dispatched_at.is_none()
        || record.completed_at.is_some()
        || record.artifact_id.is_some()
        || record.artifact_reference.is_some()
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    record.state = ContextArtifactState::Completed;
    record.completed_at = Some(sequence);
    record.artifact_id = Some(completed.artifact_id.clone());
    record.artifact_reference = Some(completed.artifact_reference.clone());
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_context_artifact_failed(
    state: &mut SessionState,
    failed: &ContextArtifactFailedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    if failed.code.trim().is_empty() {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    let record = context_artifact_record_mut(state, &failed.identity)?;
    if record.state != ContextArtifactState::Approved
        || record.approved_at.is_none()
        || record.dispatched_at.is_some()
        || record.completed_at.is_some()
    {
        return Err(SessionReducerError::InvalidContextArtifactTransition);
    }
    record.state = ContextArtifactState::Failed;
    record.completed_at = Some(sequence);
    advance_open_boundary(state, sequence)?;
    Ok(())
}

fn apply_child_agent_creation_proposed(
    state: &mut SessionState,
    proposed: &ChildAgentCreationProposedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let identity = &proposed.identity;
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    if identity.execution_id.trim().is_empty()
        || identity.task_id.trim().is_empty()
        || proposed.task.trim().is_empty()
        || proposed.task.len() > 64 * 1024
        || proposed.child_style.trim().is_empty()
        || proposed.workspace_mode.trim().is_empty()
        || proposed.token_budget == 0
        || graph_node_kind(&execution.graph, &identity.node_id) != Some(NodeKind::SpawnChildAgent)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &identity.node_id,
            identity.attempt,
            identity.loop_iteration,
            identity.step,
        )
        || state.child_agents.contains_key(&identity.execution_id)
        || state.child_agents.values().any(|record| {
            record.identity.task_id == identity.task_id
                && record.identity.attempt == identity.attempt
                && record.identity.loop_iteration == identity.loop_iteration
        })
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    state.child_agents.insert(
        identity.execution_id.clone(),
        ChildAgentRecord {
            identity: identity.clone(),
            task: proposed.task.clone(),
            child_style: proposed.child_style.clone(),
            workspace_mode: proposed.workspace_mode.clone(),
            token_budget: proposed.token_budget,
            state: ChildAgentState::Proposed,
            proposed_at: sequence,
            action_digest: None,
            approved_at: None,
            child_session_id: None,
            created_at: None,
            child_head_sequence: None,
            completed_at: None,
            summary: None,
        },
    );
    Ok(())
}

fn apply_child_agent_creation_approved(
    state: &mut SessionState,
    approved: &ChildAgentCreationApprovedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    if graph_node_kind(&execution.graph, &approved.identity.node_id)
        != Some(NodeKind::SpawnChildAgent)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &approved.identity.node_id,
            approved.identity.attempt,
            approved.identity.loop_iteration,
            approved.identity.step,
        )
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    let record = state
        .child_agents
        .get_mut(&approved.identity.execution_id)
        .ok_or(SessionReducerError::InvalidChildAgentTransition)?;
    let expected_digest = ActionProposal {
        id: ProposalId(record.identity.execution_id.clone()),
        action: ConsequentialAction::ChildAgentCreation {
            style: record.child_style.clone(),
            workspace_mode: record.workspace_mode.clone(),
            token_budget: record.token_budget,
        },
        style: state.style.clone(),
        workspace: state.workspace.clone(),
        origin: String::from("runtime"),
    }
    .digest()
    .map_err(|_| SessionReducerError::InvalidChildAgentTransition)?;
    if record.identity != approved.identity
        || record.state != ChildAgentState::Proposed
        || approved.action_digest != expected_digest
        || record.action_digest.is_some()
        || record.approved_at.is_some()
        || record.child_session_id.is_some()
        || record.created_at.is_some()
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    record.state = ChildAgentState::Approved;
    record.action_digest = Some(approved.action_digest);
    record.approved_at = Some(sequence);
    Ok(())
}

fn apply_child_agent_created(
    state: &mut SessionState,
    created: &ChildAgentCreatedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    if created.child_session_id == state.id
        || graph_node_kind(&execution.graph, &created.identity.node_id)
            != Some(NodeKind::SpawnChildAgent)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &created.identity.node_id,
            created.identity.attempt,
            created.identity.loop_iteration,
            created.identity.step,
        )
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    let record = state
        .child_agents
        .get_mut(&created.identity.execution_id)
        .ok_or(SessionReducerError::InvalidChildAgentTransition)?;
    if record.identity != created.identity
        || record.state != ChildAgentState::Approved
        || record.child_style != created.child_style
        || record.proposed_at != created.parent_action_sequence
        || record.action_digest.is_none()
        || record.approved_at.is_none()
        || record.child_session_id.is_some()
        || record.created_at.is_some()
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    record.state = ChildAgentState::Active;
    record.child_session_id = Some(created.child_session_id);
    record.created_at = Some(sequence);
    Ok(())
}

fn apply_child_agent_completed(
    state: &mut SessionState,
    completed: &ChildAgentCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    if execution
        .active_node
        .as_ref()
        .and_then(|active| graph_node_kind(&execution.graph, &active.node_id))
        != Some(NodeKind::WaitForAgents)
        || completed.summary.len() > 256 * 1024
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    let record = state
        .child_agents
        .get_mut(&completed.identity.execution_id)
        .ok_or(SessionReducerError::InvalidChildAgentTransition)?;
    if record.identity != completed.identity
        || record.state != ChildAgentState::Active
        || record.child_session_id != Some(completed.child_session_id)
        || record.created_at.is_none()
        || record.completed_at.is_some()
        || record.child_head_sequence.is_some()
        || record.summary.is_some()
    {
        return Err(SessionReducerError::InvalidChildAgentTransition);
    }
    record.state = ChildAgentState::Completed;
    record.child_head_sequence = Some(completed.child_head_sequence);
    record.completed_at = Some(sequence);
    record.summary = Some(completed.summary.clone());
    Ok(())
}

fn apply_task_plan_committed(
    state: &mut SessionState,
    committed: &TaskPlanCommittedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    let model_completed = execution
        .latest_model_execution
        .as_ref()
        .is_some_and(|evidence| {
            evidence.response_completed
                && evidence.completed_at == Some(committed.model_response_sequence)
        });
    let max_children = state
        .style_binding
        .as_ref()
        .and_then(|binding| {
            serde_json::from_str::<CompiledSessionStyle>(&binding.compiled_style_json).ok()
        })
        .map_or(0, |compiled| compiled.child_agents.max_children);
    let active_matches = active_node_matches(
        execution.active_node.as_ref(),
        &committed.node_id,
        committed.attempt,
        committed.loop_iteration,
        committed.step,
    );
    let mut tasks = BTreeMap::new();
    let valid_tasks = committed.tasks.iter().all(|task| {
        !task.task_id.trim().is_empty()
            && !task.description.trim().is_empty()
            && task.task_id.len() <= 256
            && task.description.len() <= 64 * 1024
            && tasks.insert(task.task_id.clone(), task.clone()).is_none()
    });
    if graph_node_kind(&execution.graph, &committed.node_id) != Some(NodeKind::ModelCall)
        || !active_matches
        || !model_completed
        || committed.tasks.len() < 2
        || u32::try_from(committed.tasks.len()).map_or(true, |count| count > max_children)
        || !valid_tasks
        || !state.planner_worker.tasks.is_empty()
        || state.planner_worker.plan_committed_at.is_some()
    {
        return Err(SessionReducerError::InvalidPlannerWorkerTransition);
    }
    state.planner_worker.tasks = tasks;
    state.planner_worker.plan_committed_at = Some(sequence);
    Ok(())
}

fn apply_child_join_completed(
    state: &mut SessionState,
    completed: &ChildJoinCompletedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    let mut expected = state
        .child_agents
        .values()
        .filter(|record| {
            record.identity.loop_iteration == completed.loop_iteration
                && record.state == ChildAgentState::Completed
        })
        .map(|record| record.identity.execution_id.clone())
        .collect::<Vec<_>>();
    expected.sort();
    let mut actual = completed.child_execution_ids.clone();
    actual.sort();
    actual.dedup();
    if graph_node_kind(&execution.graph, &completed.node_id) != Some(NodeKind::WaitForAgents)
        || execution.active_node.as_ref().is_none_or(|active| {
            active.node_id != completed.node_id || active.loop_iteration != completed.loop_iteration
        })
        || actual.is_empty()
        || actual != expected
        || completed.child_execution_ids.len() != actual.len()
        || state
            .planner_worker
            .joins
            .iter()
            .any(|join| join.loop_iteration == completed.loop_iteration)
    {
        return Err(SessionReducerError::InvalidPlannerWorkerTransition);
    }
    state.planner_worker.joins.push(ChildJoinRecord {
        loop_iteration: completed.loop_iteration,
        child_execution_ids: actual,
        committed_at: sequence,
    });
    Ok(())
}

fn apply_reviewer_findings_committed(
    state: &mut SessionState,
    committed: &ReviewerFindingsCommittedEvent,
    sequence: Sequence,
) -> Result<(), SessionReducerError> {
    let execution = state
        .style_execution
        .as_ref()
        .ok_or(SessionReducerError::StyleExecutionNotInitialized)?;
    let mut rejected = committed.rejected_task_ids.clone();
    rejected.sort();
    rejected.dedup();
    let known_rejections = rejected
        .iter()
        .all(|task_id| state.planner_worker.tasks.contains_key(task_id));
    let findings_bytes = committed
        .findings
        .iter()
        .try_fold(0_usize, |total, finding| total.checked_add(finding.len()))
        .unwrap_or(usize::MAX);
    if graph_node_kind(&execution.graph, &committed.node_id) != Some(NodeKind::Review)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &committed.node_id,
            committed.attempt,
            committed.loop_iteration,
            committed.step,
        )
        || committed.approved != committed.rejected_task_ids.is_empty()
        || committed.rejected_task_ids.len() != rejected.len()
        || !known_rejections
        || committed.findings.is_empty()
        || committed.findings.len() > 128
        || committed
            .findings
            .iter()
            .any(|finding| finding.trim().is_empty())
        || findings_bytes > 256 * 1024
        || state
            .planner_worker
            .reviews
            .iter()
            .any(|review| review.loop_iteration == committed.loop_iteration)
    {
        return Err(SessionReducerError::InvalidPlannerWorkerTransition);
    }
    state.planner_worker.reviews.push(ReviewerDecisionRecord {
        loop_iteration: committed.loop_iteration,
        approved: committed.approved,
        rejected_task_ids: rejected,
        findings: committed.findings.clone(),
        committed_at: sequence,
    });
    Ok(())
}

fn apply_style_node_completed(
    state: &mut SessionState,
    completed: &StyleNodeCompletedEvent,
) -> Result<(), SessionReducerError> {
    if !style_node_effect_evidence_complete(
        state
            .style_execution
            .as_ref()
            .ok_or(SessionReducerError::StyleExecutionNotInitialized)?,
        &state.conversation,
        state.style_binding.as_ref(),
        &state.artifact_persistences,
        &state.approvals,
        &state.tool_executions,
        &state.child_agents,
        &state.planner_worker,
        state.child_origin.as_ref(),
        completed,
        state.last_sequence,
    ) {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    let execution = style_execution_mut(state)?;
    if !valid_style_counters(completed.attempt, completed.step)
        || !active_node_matches(
            execution.active_node.as_ref(),
            &completed.node_id,
            completed.attempt,
            completed.loop_iteration,
            completed.step,
        )
        || !matches!(
            &execution.control,
            StyleExecutionControlState::Active(active)
                if active_node_matches(
                    Some(active),
                    &completed.node_id,
                    completed.attempt,
                    completed.loop_iteration,
                    completed.step
                )
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.active_node = None;
    execution.active_node_entered_at = None;
    execution.completed_nodes.push(completed.clone());
    execution.control = match graph_node_kind(&execution.graph, &completed.node_id) {
        Some(NodeKind::CompleteTurn) => {
            StyleExecutionControlState::ReadyForEntry(StyleExecutionCursor {
                node_id: execution.graph.nodes[execution.graph.entry_index]
                    .id
                    .clone(),
                attempt: 1,
                loop_iteration: 0,
                step: completed
                    .step
                    .checked_add(1)
                    .ok_or(SessionReducerError::StyleStepOverflow)?,
            })
        }
        Some(NodeKind::CompleteSession | NodeKind::Fail) => {
            let reason = if graph_node_kind(&execution.graph, &completed.node_id)
                == Some(NodeKind::CompleteSession)
            {
                "complete_session"
            } else {
                "style_failed"
            };
            execution.termination_reason = Some(reason.to_owned());
            StyleExecutionControlState::Terminal {
                reason: reason.to_owned(),
            }
        }
        Some(_) => StyleExecutionControlState::AwaitingTransition(completed.clone()),
        None => return Err(SessionReducerError::InvalidStyleExecutionTransition),
    };
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
    execution.active_node_entered_at = None;
    execution
        .termination_reason
        .clone_from(&failed.termination_reason);
    execution.failed_nodes.push(failed.clone());
    execution.control = if let Some(reason) = &failed.termination_reason {
        StyleExecutionControlState::Terminal {
            reason: reason.clone(),
        }
    } else {
        StyleExecutionControlState::ReadyForEntry(StyleExecutionCursor {
            node_id: execution.graph.nodes[execution.graph.entry_index]
                .id
                .clone(),
            attempt: 1,
            loop_iteration: 0,
            step: failed
                .step
                .checked_add(1)
                .ok_or(SessionReducerError::StyleStepOverflow)?,
        })
    };
    Ok(())
}

fn apply_style_transition_selected(
    state: &mut SessionState,
    selected: &StyleTransitionSelectedEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    let StyleExecutionControlState::AwaitingTransition(completed) = &execution.control else {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    };
    if execution.active_node.is_some()
        || execution.termination_reason.is_some()
        || !valid_style_counters(selected.attempt, selected.step)
        || completed.node_id != selected.from_node_id
        || completed.attempt != selected.attempt
        || completed.loop_iteration != selected.loop_iteration
        || completed.step != selected.step
        || !graph_has_transition(
            &execution.graph,
            &selected.from_node_id,
            &selected.to_node_id,
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.transitions.push(selected.clone());
    execution.control = StyleExecutionControlState::AwaitingDestinationEntry(selected.clone());
    Ok(())
}

fn apply_style_execution_terminated(
    state: &mut SessionState,
    terminated: &StyleExecutionTerminatedEvent,
) -> Result<(), SessionReducerError> {
    let execution = style_execution_mut(state)?;
    if terminated.reason.trim().is_empty()
        || terminated
            .refused_node_id
            .as_deref()
            .is_some_and(str::is_empty)
        || matches!(
            execution.control,
            StyleExecutionControlState::Active(_)
                | StyleExecutionControlState::AwaitingTransition(_)
                | StyleExecutionControlState::Terminal { .. }
        )
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    let expected = match &execution.control {
        StyleExecutionControlState::ReadyForEntry(cursor) => cursor,
        StyleExecutionControlState::AwaitingDestinationEntry(selected) => {
            if terminated.refused_node_id.as_deref() != Some(selected.to_node_id.as_str())
                || terminated.refused_step
                    != selected
                        .step
                        .checked_add(1)
                        .map(Some)
                        .ok_or(SessionReducerError::StyleStepOverflow)?
            {
                return Err(SessionReducerError::InvalidStyleExecutionTransition);
            }
            execution.termination_reason = Some(terminated.reason.clone());
            execution.control = StyleExecutionControlState::Terminal {
                reason: terminated.reason.clone(),
            };
            return Ok(());
        }
        StyleExecutionControlState::Active(_)
        | StyleExecutionControlState::AwaitingTransition(_)
        | StyleExecutionControlState::Terminal { .. } => unreachable!(),
    };
    if terminated.refused_node_id.as_deref() != Some(expected.node_id.as_str())
        || terminated.refused_step != Some(expected.step)
    {
        return Err(SessionReducerError::InvalidStyleExecutionTransition);
    }
    execution.termination_reason = Some(terminated.reason.clone());
    execution.control = StyleExecutionControlState::Terminal {
        reason: terminated.reason.clone(),
    };
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

fn graph_node_kind(graph: &ExecutableGraph, node_id: &str) -> Option<NodeKind> {
    graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .map(|node| node.kind)
}

fn graph_has_transition(graph: &ExecutableGraph, from_node_id: &str, to_node_id: &str) -> bool {
    graph.edges.iter().any(|edge| {
        graph.nodes[edge.from].id == from_node_id && graph.nodes[edge.to].id == to_node_id
    })
}

fn artifact_persistence_effect_evidence_complete(
    artifact_persistences: &BTreeMap<String, ArtifactPersistenceRecord>,
    completed: &StyleNodeCompletedEvent,
    journal_head: Sequence,
) -> bool {
    let Some(artifact_reference) = completed.artifact_reference.as_deref() else {
        return false;
    };
    artifact_persistences.values().any(|record| {
        record.state == ArtifactPersistenceState::Completed
            && record.identity.node_id == completed.node_id
            && record.identity.attempt == completed.attempt
            && record.identity.loop_iteration == completed.loop_iteration
            && record.identity.step == completed.step
            && record.completed_at == Some(journal_head)
            && record.artifact_reference.as_deref() == Some(artifact_reference)
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "replay validation keeps every independent canonical evidence projection explicit"
)]
fn style_node_effect_evidence_complete(
    execution: &StyleExecutionState,
    conversation: &ConversationState,
    binding: Option<&SessionStyleBinding>,
    artifact_persistences: &BTreeMap<String, ArtifactPersistenceRecord>,
    approvals: &BTreeMap<ContinuationId, ApprovalRecord>,
    tool_executions: &BTreeMap<String, ToolExecutionRecord>,
    child_agents: &BTreeMap<String, ChildAgentRecord>,
    planner_worker: &PlannerWorkerState,
    child_origin: Option<&ChildSessionOrigin>,
    completed: &StyleNodeCompletedEvent,
    journal_head: Sequence,
) -> bool {
    if graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::PersistArtifact) {
        return artifact_persistence_effect_evidence_complete(
            artifact_persistences,
            completed,
            journal_head,
        );
    }
    if is_declarative_graph(&execution.graph)
        && graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::UserApproval)
    {
        let Some(continuation_id) = completed
            .result_reference
            .as_deref()
            .and_then(|reference| reference.strip_prefix("declarative-approval:"))
            .and_then(|value| ContinuationId::from_str(value).ok())
        else {
            return false;
        };
        return approvals.get(&continuation_id).is_some_and(|approval| {
            approval.state == ApprovalState::Approved && approval.resolved_at == Some(journal_head)
        });
    }
    if graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::SpawnChildAgent) {
        let Some(expected) = completed
            .result_reference
            .as_deref()
            .and_then(|reference| reference.strip_prefix("children:"))
            .and_then(|value| value.parse::<usize>().ok())
        else {
            return false;
        };
        let matching = child_agents
            .values()
            .filter(|record| {
                record.identity.node_id == completed.node_id
                    && record.identity.attempt == completed.attempt
                    && record.identity.loop_iteration == completed.loop_iteration
                    && record.identity.step == completed.step
                    && matches!(
                        record.state,
                        ChildAgentState::Active | ChildAgentState::Completed
                    )
            })
            .collect::<Vec<_>>();
        let mut actual_task_ids = matching
            .iter()
            .map(|record| record.identity.task_id.clone())
            .collect::<Vec<_>>();
        actual_task_ids.sort();
        let mut expected_task_ids = if completed.loop_iteration == 0 {
            planner_worker.tasks.keys().cloned().collect::<Vec<_>>()
        } else {
            planner_worker
                .reviews
                .iter()
                .rev()
                .find(|review| {
                    review.loop_iteration.checked_add(1) == Some(completed.loop_iteration)
                })
                .map_or_else(Vec::new, |review| review.rejected_task_ids.clone())
        };
        expected_task_ids.sort();
        return expected > 0 && matching.len() == expected && actual_task_ids == expected_task_ids;
    }
    if is_planner_worker_graph(&execution.graph)
        && graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::ModelCall)
    {
        if completed.node_id == "plan" {
            return planner_worker.plan_committed_at == Some(journal_head);
        }
        return execution
            .latest_model_execution
            .as_ref()
            .is_some_and(|evidence| {
                evidence.response_completed
                    && evidence
                        .completed_at
                        .is_some_and(|sequence| sequence <= journal_head)
            });
    }
    if graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::WaitForAgents) {
        let Some(expected) = completed
            .result_reference
            .as_deref()
            .and_then(|reference| reference.strip_prefix("children-completed:"))
            .and_then(|value| value.parse::<usize>().ok())
        else {
            return false;
        };
        let matching = child_agents
            .values()
            .filter(|record| {
                record.state == ChildAgentState::Completed
                    && record.identity.loop_iteration == completed.loop_iteration
            })
            .collect::<Vec<_>>();
        let mut matching_ids = matching
            .iter()
            .map(|record| record.identity.execution_id.clone())
            .collect::<Vec<_>>();
        matching_ids.sort();
        return expected > 0
            && matching.len() == expected
            && planner_worker.joins.last().is_some_and(|join| {
                join.loop_iteration == completed.loop_iteration
                    && join.committed_at == journal_head
                    && join.child_execution_ids == matching_ids
            });
    }
    if is_planner_worker_graph(&execution.graph)
        && graph_node_kind(&execution.graph, &completed.node_id) == Some(NodeKind::Review)
    {
        return planner_worker.reviews.last().is_some_and(|review| {
            review.loop_iteration == completed.loop_iteration
                && review.committed_at == journal_head
                && completed.result_reference.as_deref()
                    == Some(if review.approved {
                        "review:approved:true"
                    } else {
                        "review:approved:false"
                    })
        });
    }
    if is_declarative_graph(&execution.graph)
        && graph_node_kind(&execution.graph, &completed.node_id)
            == Some(NodeKind::ToolExecutionGate)
    {
        let Some(call_id) = completed
            .result_reference
            .as_deref()
            .and_then(|reference| reference.strip_prefix("declarative-tool:"))
            .and_then(|reference| {
                reference
                    .split_once(":iteration:")
                    .map(|(call_id, _)| call_id)
            })
        else {
            return false;
        };
        return tool_executions.get(call_id).is_some_and(|record| {
            record.state == ToolExecutionState::Terminal
                && record
                    .terminal_at
                    .is_some_and(|sequence| sequence <= journal_head)
        });
    }
    let fresh_context_method = if is_ephemeral_turn_graph(&execution.graph) {
        Some("ephemeral_fresh_context")
    } else if is_research_loop_graph(&execution.graph) {
        Some("research_fresh_context")
    } else {
        None
    };
    if fresh_context_method.is_none() {
        return true;
    }
    match graph_node_kind(&execution.graph, &completed.node_id) {
        Some(NodeKind::ContextTransform) => fresh_context_effect_evidence_complete(
            execution,
            conversation,
            binding,
            child_origin,
            completed,
            journal_head,
            fresh_context_method.expect("fresh context method exists"),
        ),
        Some(NodeKind::CompleteTurn) if is_ephemeral_turn_graph(&execution.graph) => {
            let Some(boundary) = execution.context_boundaries.last() else {
                return false;
            };
            let Some(provenance) = conversation.projection_provenance() else {
                return false;
            };
            boundary.identity.node_id == completed.node_id
                && boundary.identity.boundary == "before_turn_completion"
                && boundary.identity.origin
                    == if child_origin.is_some() {
                        ContextBoundaryOrigin::ChildTask
                    } else {
                        ContextBoundaryOrigin::UserTurn
                    }
                && boundary.completed_phases.as_slice() == ["discard"]
                && boundary.completed_at == Some(journal_head)
                && provenance.method == "ephemeral_discard"
                && provenance.committed_at.checked_next().ok() == Some(journal_head)
                && conversation.provider_projection().is_empty()
        }
        _ => true,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "fresh-context evidence validates user and typed-child inputs plus exact memory provenance"
)]
fn fresh_context_effect_evidence_complete(
    execution: &StyleExecutionState,
    conversation: &ConversationState,
    binding: Option<&SessionStyleBinding>,
    child_origin: Option<&ChildSessionOrigin>,
    completed: &StyleNodeCompletedEvent,
    journal_head: Sequence,
    method: &str,
) -> bool {
    let Some(boundary) = execution.context_boundaries.last() else {
        return false;
    };
    let Some(provenance) = conversation.projection_provenance() else {
        return false;
    };
    let inputs = conversation
        .provider_projection()
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                ConversationEntry::UserMessage(_) | ConversationEntry::PendingTask(_)
            )
        })
        .collect::<Vec<_>>();
    let [input] = inputs.as_slice() else {
        return false;
    };
    let (source_sequence, exact_input, expected_origin) = match input {
        ConversationEntry::UserMessage(user) => {
            let canonical_user =
                conversation
                    .history()
                    .iter()
                    .rev()
                    .find_map(|entry| match entry {
                        ConversationEntry::UserMessage(candidate)
                            if candidate.source_sequence <= boundary.identity.source_head =>
                        {
                            Some(candidate)
                        }
                        _ => None,
                    });
            (
                user.source_sequence,
                canonical_user == Some(user),
                ContextBoundaryOrigin::UserTurn,
            )
        }
        ConversationEntry::PendingTask(task) => {
            let exact = child_origin.is_some_and(|origin| {
                task.task_id == origin.task_id
                    && task.description == origin.task
                    && task.state == "assigned"
                    && task.source_sequence == origin.linked_at
                    && origin.input_hash == ContentHash::digest(task.description.as_bytes())
            });
            (
                task.source_sequence,
                exact,
                ContextBoundaryOrigin::ChildTask,
            )
        }
        _ => unreachable!("filtered provider input"),
    };
    if !exact_input {
        return false;
    }
    let Some(replacement_event) = boundary.phase_replacement_event else {
        return false;
    };
    let Some(selected_memory_provider) = binding.map(|binding| binding.memory.provider.as_str())
    else {
        return false;
    };
    if conversation.provider_projection().iter().any(|entry| {
        !matches!(
            entry,
            ConversationEntry::UserMessage(_) | ConversationEntry::PendingTask(_)
        ) && !matches!(
            entry,
            ConversationEntry::RetrievedMemory(memory)
                if memory.injection_sequence == provenance.committed_at
                    && memory.injection_event == Some(replacement_event)
                    && memory.provider == selected_memory_provider
                    && selected_memory_provider != "none"
        )
    }) {
        return false;
    }
    boundary.identity.node_id == completed.node_id
        && boundary.identity.boundary == "turn_start"
        && boundary.identity.origin == expected_origin
        && boundary.completed_phases.as_slice() == ["memory"]
        && boundary.completed_at == Some(journal_head)
        && provenance.method == method
        && provenance.committed_at.checked_next().ok() == Some(journal_head)
        && provenance.source_range == Some((source_sequence, source_sequence))
        && match input {
            ConversationEntry::UserMessage(user) => {
                user.id.0.strip_prefix("user:").is_some_and(|suffix| {
                    suffix.ends_with(&format!(":{}", boundary.identity.run_id))
                })
            }
            ConversationEntry::PendingTask(task) => {
                task.id.0
                    == format!(
                        "child-task:{}:{}",
                        task.task_id,
                        child_origin.map_or(0, |origin| origin.revision)
                    )
            }
            _ => false,
        }
}

fn is_ephemeral_turn_graph(graph: &ExecutableGraph) -> bool {
    if graph.nodes.len() != 4 || graph.edges.len() != 3 {
        return false;
    }
    let Some(entry) = graph.nodes.get(graph.entry_index) else {
        return false;
    };
    let kinds = [
        NodeKind::ContextTransform,
        NodeKind::ModelCall,
        NodeKind::ToolExecutionGate,
        NodeKind::CompleteTurn,
    ];
    let mut index = entry.index;
    for (offset, kind) in kinds.into_iter().enumerate() {
        if graph.nodes.get(index).map(|node| node.kind) != Some(kind) {
            return false;
        }
        if offset + 1 == kinds.len() {
            return !graph.edges.iter().any(|edge| edge.from == index);
        }
        let mut outgoing = graph.edges.iter().filter(|edge| edge.from == index);
        let Some(edge) = outgoing.next() else {
            return false;
        };
        if outgoing.next().is_some() {
            return false;
        }
        index = edge.to;
    }
    false
}

fn is_research_loop_graph(graph: &ExecutableGraph) -> bool {
    if graph.nodes.len() != 6 || graph.edges.len() != 6 {
        return false;
    }
    let expected = [
        ("fresh-context", NodeKind::ContextTransform),
        ("research", NodeKind::ModelCall),
        ("tool", NodeKind::ToolExecutionGate),
        ("persist", NodeKind::PersistArtifact),
        ("repeat", NodeKind::Loop),
        ("done", NodeKind::CompleteSession),
    ];
    if expected.iter().any(|(id, kind)| {
        graph
            .nodes
            .iter()
            .find(|node| node.id == *id)
            .map(|node| node.kind)
            != Some(*kind)
    }) {
        return false;
    }
    graph
        .nodes
        .get(graph.entry_index)
        .is_some_and(|node| node.id == "fresh-context")
        && graph_has_transition(graph, "fresh-context", "research")
        && graph_has_transition(graph, "research", "tool")
        && graph_has_transition(graph, "tool", "persist")
        && graph_has_transition(graph, "persist", "repeat")
        && graph_has_transition(graph, "repeat", "fresh-context")
        && graph_has_transition(graph, "repeat", "done")
}

fn is_planner_worker_graph(graph: &ExecutableGraph) -> bool {
    let expected = [
        ("plan", NodeKind::ModelCall),
        ("spawn-workers", NodeKind::SpawnChildAgent),
        ("wait-workers", NodeKind::WaitForAgents),
        ("integrate", NodeKind::ModelCall),
        ("review", NodeKind::Review),
        ("revision", NodeKind::Loop),
        ("done", NodeKind::CompleteSession),
    ];
    graph.nodes.len() == expected.len()
        && graph.edges.len() == 7
        && expected.iter().all(|(id, kind)| {
            graph
                .nodes
                .iter()
                .find(|node| node.id == *id)
                .map(|node| node.kind)
                == Some(*kind)
        })
        && graph
            .nodes
            .get(graph.entry_index)
            .is_some_and(|node| node.id == "plan")
        && graph_has_transition(graph, "plan", "spawn-workers")
        && graph_has_transition(graph, "spawn-workers", "wait-workers")
        && graph_has_transition(graph, "wait-workers", "integrate")
        && graph_has_transition(graph, "integrate", "review")
        && graph_has_transition(graph, "review", "revision")
        && graph_has_transition(graph, "revision", "spawn-workers")
        && graph_has_transition(graph, "revision", "done")
}

fn is_declarative_graph(graph: &ExecutableGraph) -> bool {
    let expected = [
        ("branch", NodeKind::ConditionalBranch),
        ("approval", NodeKind::UserApproval),
        ("tool", NodeKind::ToolExecutionGate),
        ("repeat", NodeKind::Loop),
        ("done", NodeKind::CompleteSession),
    ];
    graph.nodes.len() == expected.len()
        && graph.edges.len() == 6
        && expected.iter().all(|(id, kind)| {
            graph
                .nodes
                .iter()
                .find(|node| node.id == *id)
                .map(|node| node.kind)
                == Some(*kind)
        })
        && graph
            .nodes
            .get(graph.entry_index)
            .is_some_and(|node| node.id == "branch")
        && graph_has_transition(graph, "branch", "approval")
        && graph_has_transition(graph, "branch", "tool")
        && graph_has_transition(graph, "approval", "tool")
        && graph_has_transition(graph, "tool", "repeat")
        && graph_has_transition(graph, "repeat", "tool")
        && graph_has_transition(graph, "repeat", "done")
}

const fn valid_style_counters(attempt: u32, step: u64) -> bool {
    attempt > 0 && step > 0
}

fn entry_matches(expected: &StyleExecutionCursor, entered: &StyleNodeEnteredEvent) -> bool {
    valid_style_counters(entered.attempt, entered.step)
        && entered.node_id == expected.node_id
        && entered.attempt == expected.attempt
        && entered.loop_iteration == expected.loop_iteration
        && entered.step == expected.step
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
            execution_id: Some(dispatched.execution_id.clone()),
            call_id: dispatched.call_id.clone(),
            action_digest: Some(dispatched.action_digest),
            state: ToolExecutionState::Dispatched,
            dispatched_at: Some(sequence),
            terminal_at: None,
            observed_event_count: 0,
            terminal_outcome: None,
        },
    );
    Ok(())
}

fn mark_tool_terminal(
    state: &mut SessionState,
    call_id: &str,
    sequence: Sequence,
    action_digest: Option<ContentHash>,
    outcome: ToolExecutionTerminalOutcome,
) -> Result<(), SessionReducerError> {
    if let Some(record) = state.tool_executions.get_mut(call_id) {
        if record.state == ToolExecutionState::Terminal {
            return Err(SessionReducerError::InvalidToolExecutionTransition);
        }
        if let Some(action_digest) = action_digest {
            if record
                .action_digest
                .is_some_and(|existing| existing != action_digest)
            {
                return Err(SessionReducerError::InvalidToolExecutionTransition);
            }
            record.action_digest = Some(action_digest);
        }
        record.state = ToolExecutionState::Terminal;
        record.terminal_at = Some(sequence);
        record.terminal_outcome = Some(outcome);
        record.observed_event_count = record
            .observed_event_count
            .checked_add(1)
            .ok_or(SessionReducerError::ToolEventCountOverflow)?;
    } else {
        state.tool_executions.insert(
            call_id.to_owned(),
            ToolExecutionRecord {
                execution_id: None,
                call_id: call_id.to_owned(),
                action_digest,
                state: ToolExecutionState::Terminal,
                dispatched_at: None,
                terminal_at: Some(sequence),
                observed_event_count: 1,
                terminal_outcome: Some(outcome),
            },
        );
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
    /// A worker may establish one exact parent/task link at sequence two.
    #[error("child session ownership link is invalid")]
    InvalidChildSessionLink,
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
    /// An artifact-persistence execution identity was proposed more than once.
    #[error("artifact persistence was proposed more than once")]
    DuplicateArtifactPersistence,
    /// Artifact persistence did not follow proposal, approval, dispatch, receipt ordering.
    #[error("artifact persistence state transition is invalid")]
    InvalidArtifactPersistenceTransition,
    /// A live typed-summary request violated canonical ordering or bounds.
    #[error("typed-summary compaction state transition is invalid")]
    InvalidSummaryTransition,
    /// An automatic memory write violated canonical outbox ordering or bounds.
    #[error("automatic memory-write state transition is invalid")]
    InvalidMemoryWriteTransition,
    /// An artifact-handoff context write violated canonical outbox ordering.
    #[error("artifact-handoff context write state transition is invalid")]
    InvalidContextArtifactTransition,
    /// Child sessions did not follow proposal, atomic creation, and terminal ordering.
    #[error("child-agent state transition is invalid")]
    InvalidChildAgentTransition,
    /// Planner tasks, joins, or reviewer findings violated canonical ordering.
    #[error("planner-worker-reviewer state transition is invalid")]
    InvalidPlannerWorkerTransition,
    /// Plugin activation requires an immutable style binding.
    #[error("plugin activation requires a session style binding")]
    MissingStyleBinding,
    /// Activated plugin identity or plugin-set hash was invalid.
    #[error("plugin activation state is invalid")]
    InvalidPluginActivation,
    /// A plugin invocation did not match the active plugin set or typed boundary.
    #[error("plugin invocation state is invalid")]
    InvalidPluginInvocation,
    /// Context composition did not follow start, phase, completion ordering.
    #[error("context boundary state transition is invalid")]
    InvalidContextBoundaryTransition,
    /// Context completion did not describe the exact replayed projection.
    #[error("context boundary projection measurement is invalid")]
    InvalidContextBoundaryMeasurement,
    /// Provider lifecycle evidence did not match the latest canonical start.
    #[error("model execution evidence is invalid")]
    InvalidModelExecutionEvidence,
    /// Bounded visible model evidence exceeded the protocol frame ceiling.
    #[error("model output replay evidence exceeded its byte limit")]
    ModelOutputEvidenceOverflow,
    /// Provider-reported usage exceeded the replay-safe integer bound.
    #[error("style provider token usage overflowed")]
    StyleTokenUsageOverflow,
    /// Style graph step arithmetic overflowed.
    #[error("style graph step counter overflowed")]
    StyleStepOverflow,
    /// Style loop iteration arithmetic overflowed.
    #[error("style graph loop iteration overflowed")]
    StyleLoopIterationOverflow,
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
    use agentmod_session_style_sdk::{BuiltInStyle, CompiledSessionStyle};
    use uuid::Uuid;

    use crate::conversation::{ContextSummaryEntry, ConversationEntryId, TextEntry};
    use crate::style_executor::tests::binding;

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

    #[test]
    fn plugin_activation_and_invocation_replay_into_inspectable_state() {
        let style_binding = binding(BuiltInStyle::PersistentChat);
        let plugin_set_hash = style_binding.plugin_set_hash;
        let events = vec![
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding)),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
                    plugin_ids: vec![String::from("fixture.rewriter")],
                    plugin_set_hash,
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::PluginInvocationCompleted(PluginInvocationCompletedEvent {
                    plugin_id: String::from("fixture.rewriter"),
                    handler: String::from("rewrite-tool"),
                    action_kind: String::from("tool_call"),
                    proposal_id: String::from("tool-call:fixture"),
                    input_digest: ContentHash::digest(b"input"),
                    output_digest: Some(ContentHash::digest(b"output")),
                    outcome: String::from("replace"),
                }),
            ),
        ];
        let state = replay(&events).expect("plugin replay");
        assert_eq!(
            state.plugins.activated_plugin_ids,
            vec![String::from("fixture.rewriter")]
        );
        assert_eq!(
            state.plugins.activated_at,
            Some(Sequence::new(2).expect("sequence"))
        );
        assert_eq!(state.plugins.invocations.len(), 1);
        assert_eq!(state.plugins.invocations[0].outcome, "replace");
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture retains the complete canonical plan-to-child proposal sequence"
    )]
    fn proposed_child_creation_fixture() -> (
        ChildAgentExecutionIdentity,
        ContentHash,
        Vec<EventEnvelope<RuntimeCommittedEvent>>,
    ) {
        let style_binding = binding(BuiltInStyle::PlannerWorker);
        let graph: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled style");
        let identity = ChildAgentExecutionIdentity {
            execution_id: String::from("child:spawn-workers:task-1:0"),
            node_id: String::from("spawn-workers"),
            attempt: 1,
            loop_iteration: 0,
            step: 2,
            task_id: String::from("task-1"),
        };
        let proposal = ChildAgentCreationProposedEvent {
            identity: identity.clone(),
            task: String::from("inspect scheduler recovery"),
            child_style: String::from("ephemeral-turn@1.1.0"),
            workspace_mode: String::from("shared_read_only"),
            token_budget: 10_000,
        };
        let digest = ActionProposal {
            id: ProposalId(identity.execution_id.clone()),
            action: ConsequentialAction::ChildAgentCreation {
                style: proposal.child_style.clone(),
                workspace_mode: proposal.workspace_mode.clone(),
                token_budget: proposal.token_budget,
            },
            style: style_binding.id.clone(),
            workspace: String::from("fixture"),
            origin: String::from("runtime"),
        }
        .digest()
        .expect("digest");
        let events = vec![
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding)),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(graph.graph),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("plan"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ModelRequestStarted(ModelRequestStartedEvent {
                    cancellation_id: String::from("planner"),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ModelOutputDeltaObserved(ModelOutputDeltaObservedEvent {
                    cancellation_id: String::from("planner"),
                    text: String::from("structured plan"),
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::ModelResponseCompleted(ModelResponseCompletedEvent {
                    cancellation_id: String::from("planner"),
                    finish_reason: String::from("stop"),
                    input_tokens: 1,
                    output_tokens: 1,
                }),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::TaskPlanCommitted(TaskPlanCommittedEvent {
                    node_id: String::from("plan"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    model_response_sequence: Sequence::new(6).expect("response"),
                    tasks: vec![
                        PlannedTask {
                            task_id: String::from("task-1"),
                            description: String::from("inspect scheduler recovery"),
                        },
                        PlannedTask {
                            task_id: String::from("task-2"),
                            description: String::from("inspect tool recovery"),
                        },
                    ],
                }),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                    node_id: String::from("plan"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    result_reference: Some(String::from("plan-artifact")),
                    artifact_reference: None,
                }),
            ),
            envelope(
                9,
                RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                    from_node_id: String::from("plan"),
                    to_node_id: String::from("spawn-workers"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                10,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("spawn-workers"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
            envelope(
                11,
                RuntimeCommittedEvent::ChildAgentCreationProposed(proposal),
            ),
        ];
        (identity, digest, events)
    }

    #[test]
    fn child_session_link_replays_typed_task_without_conversation_input() {
        let parent = SessionId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000099"));
        let task = String::from("inspect the scheduler recovery invariant");
        let linked = envelope(
            2,
            RuntimeCommittedEvent::ChildSessionLinked(ChildSessionLinkedEvent {
                parent_session_id: parent,
                parent_action_sequence: Sequence::new(17).expect("sequence"),
                parent_graph_node_id: String::from("spawn-workers"),
                task_id: String::from("task-1"),
                revision: 0,
                depth: 1,
                input_hash: ContentHash::digest(task.as_bytes()),
                task: task.clone(),
                token_budget: 10_000,
            }),
        );

        let events = [created(), linked];
        let state = replay(&events).expect("child replay");

        let origin = state.child_origin.expect("typed child origin");
        assert_eq!(origin.parent_session_id, parent);
        assert_eq!(origin.task, task);
        assert!(state.ancestry.is_none());
        assert!(state.conversation.history().is_empty());
    }

    #[test]
    fn child_session_link_rejects_mismatched_task_hash() {
        let linked = envelope(
            2,
            RuntimeCommittedEvent::ChildSessionLinked(ChildSessionLinkedEvent {
                parent_session_id: SessionId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000099",
                )),
                parent_action_sequence: Sequence::new(17).expect("sequence"),
                parent_graph_node_id: String::from("spawn-workers"),
                task_id: String::from("task-1"),
                revision: 0,
                depth: 1,
                task: String::from("exact task"),
                input_hash: ContentHash::digest(b"different task"),
                token_budget: 10_000,
            }),
        );

        let events = [created(), linked];
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidChildSessionLink)
        ));
    }

    #[test]
    fn child_creation_requires_exact_policy_digest_before_atomic_receipt() {
        let (identity, digest, mut events) = proposed_child_creation_fixture();
        let mut wrong = events.clone();
        wrong.push(envelope(
            12,
            RuntimeCommittedEvent::ChildAgentCreationApproved(ChildAgentCreationApprovedEvent {
                identity: identity.clone(),
                action_digest: ContentHash::digest(b"wrong"),
            }),
        ));
        assert!(matches!(
            replay(&wrong),
            Err(SessionReducerError::InvalidChildAgentTransition)
        ));

        events.push(envelope(
            12,
            RuntimeCommittedEvent::ChildAgentCreationApproved(ChildAgentCreationApprovedEvent {
                identity: identity.clone(),
                action_digest: digest,
            }),
        ));
        events.push(envelope(
            13,
            RuntimeCommittedEvent::ChildAgentCreated(ChildAgentCreatedEvent {
                identity: identity.clone(),
                child_session_id: SessionId::from_uuid(Uuid::from_u128(999)),
                parent_action_sequence: Sequence::new(11).expect("proposal sequence"),
                child_style: String::from("ephemeral-turn@1.1.0"),
            }),
        ));

        let state = replay(&events).expect("approved child receipt");
        assert_eq!(
            state
                .child_agents
                .get(&identity.execution_id)
                .expect("child")
                .state,
            ChildAgentState::Active
        );
    }

    #[test]
    fn planner_task_plan_rejects_duplicate_runtime_task_identity() {
        let (_, _, mut events) = proposed_child_creation_fixture();
        events.truncate(7);
        let RuntimeCommittedEvent::TaskPlanCommitted(plan) =
            &mut events.last_mut().expect("plan event").payload
        else {
            panic!("expected task plan");
        };
        plan.tasks[1].task_id = plan.tasks[0].task_id.clone();
        let resealed = EventEnvelope::seal(
            events.last().expect("plan event").metadata.clone(),
            events.last().expect("plan event").payload.clone(),
        )
        .expect("reseal changed plan");
        *events.last_mut().expect("plan event") = resealed;

        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidPlannerWorkerTransition)
        ));
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
                capability_set: BTreeSet::from([String::from("model")]),
            },
            CompilerLimits::default(),
        )
        .expect("compiled graph")
    }

    fn context_graph() -> ExecutableGraph {
        compile_graph(
            r#"
format_version = 1
entry = "respond"
[budget]
max_steps = 10
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["model"]
providers = ["mock"]
[[nodes]]
id = "respond"
kind = "model_call"
provider = "mock"
[[nodes]]
id = "done"
kind = "complete_turn"
[[edges]]
from = "respond"
to = "done"
"#,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: "1.0.0".into(),
                capability_set: BTreeSet::from([String::from("model")]),
            },
            CompilerLimits::default(),
        )
        .expect("context graph")
    }

    fn ephemeral_context_graph() -> ExecutableGraph {
        compile_graph(
            r#"
format_version = 1
entry = "fresh-context"
[budget]
max_steps = 10
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["context", "model"]
providers = ["mock"]
[[nodes]]
id = "fresh-context"
kind = "context_transform"
[[nodes]]
id = "respond"
kind = "model_call"
provider = "mock"
[[nodes]]
id = "done"
kind = "complete_turn"
[[edges]]
from = "fresh-context"
to = "respond"
[[edges]]
from = "respond"
to = "done"
"#,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: "1.0.0".into(),
                capability_set: BTreeSet::from([String::from("context"), String::from("model")]),
            },
            CompilerLimits::default(),
        )
        .expect("ephemeral context graph")
    }

    fn artifact_graph() -> ExecutableGraph {
        compile_graph(
            r#"
format_version = 1
entry = "persist"
[budget]
max_steps = 10
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["artifacts"]
[[nodes]]
id = "persist"
kind = "persist_artifact"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "persist"
to = "done"
"#,
            &GraphCacheInputs {
                plugin_set_hash: ContentHash::digest(b"plugins"),
                runtime_api_version: "1.0.0".into(),
                capability_set: BTreeSet::from([String::from("artifacts")]),
            },
            CompilerLimits::default(),
        )
        .expect("artifact graph")
    }

    fn artifact_identity() -> ArtifactPersistenceIdentity {
        ArtifactPersistenceIdentity {
            execution_id: String::from("artifact:persist:1:0:1"),
            proposal_id: String::from("artifact-proposal:persist:1:0:1"),
            node_id: String::from("persist"),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            content_hash: ContentHash::digest(br#"{"finding":"bounded"}"#),
        }
    }

    fn active_artifact_events() -> Vec<EventEnvelope<RuntimeCommittedEvent>> {
        vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(artifact_graph()),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("persist"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
        ]
    }

    fn context_identity(
        boundary: &str,
        source_head: u64,
        origin: ContextBoundaryOrigin,
    ) -> ContextBoundaryIdentity {
        ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: boundary.into(),
            run_id: String::from("run-1"),
            origin,
            request_hash: ContentHash::digest(b"request-1"),
            source_head: Sequence::new(source_head).expect("source head"),
        }
    }

    fn active_context_state() -> SessionState {
        replay(&[
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(context_graph()),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
        ])
        .expect("active context state")
    }

    #[test]
    fn model_execution_evidence_rejects_overwrite_and_mismatched_terminal() {
        let started = reduce(
            Some(active_context_state()),
            &envelope(
                4,
                RuntimeCommittedEvent::ModelRequestStarted(ModelRequestStartedEvent {
                    cancellation_id: String::from("run-1"),
                }),
            ),
        )
        .expect("first model start");
        assert!(matches!(
            reduce(
                Some(started.clone()),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ModelRequestStarted(ModelRequestStartedEvent {
                        cancellation_id: String::from("run-2"),
                    }),
                ),
            ),
            Err(SessionReducerError::InvalidModelExecutionEvidence)
        ));
        assert!(matches!(
            reduce(
                Some(started),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ModelResponseCompleted(ModelResponseCompletedEvent {
                        cancellation_id: String::from("run-2"),
                        finish_reason: String::from("stop"),
                        input_tokens: 1,
                        output_tokens: 1,
                    },),
                ),
            ),
            Err(SessionReducerError::InvalidModelExecutionEvidence)
        ));
    }

    #[test]
    fn context_reducer_rejects_overlapping_and_reversed_boundaries() {
        let turn = context_identity("turn_start", 3, ContextBoundaryOrigin::UserTurn);
        let state = reduce(
            Some(active_context_state()),
            &envelope(
                4,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn.clone(),
                }),
            ),
        )
        .expect("turn boundary");
        let overlap = ContextBoundaryIdentity {
            source_head: Sequence::new(4).expect("source head"),
            ..turn.clone()
        };
        assert!(matches!(
            reduce(
                Some(state.clone()),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                        identity: overlap,
                    })
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryTransition)
        ));
        assert!(matches!(
            reduce(
                Some(state),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                        identity: ContextPhaseIdentity {
                            boundary: turn,
                            phase: String::from("compaction"),
                        },
                    })
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryTransition)
        ));
    }

    #[test]
    fn context_reducer_rejects_incomplete_phase_and_measurement_mismatch() {
        let turn = context_identity("turn_start", 3, ContextBoundaryOrigin::UserTurn);
        let phase = ContextPhaseIdentity {
            boundary: turn.clone(),
            phase: String::from("memory"),
        };
        let state = replay(&[
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(context_graph()),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn.clone(),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: phase.clone(),
                }),
            ),
        ])
        .expect("started phase");
        let empty = measure_projection(&[]).expect("empty measurement");
        assert!(matches!(
            reduce(
                Some(state.clone()),
                &envelope(
                    6,
                    RuntimeCommittedEvent::ContextBoundaryCompleted(
                        ContextBoundaryCompletedEvent {
                            identity: turn.clone(),
                            projection_hash: empty.projection_hash,
                            estimated_tokens: empty.estimated_tokens,
                            serialized_bytes: empty.serialized_bytes,
                        },
                    )
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryTransition)
        ));
        let state = reduce(
            Some(state),
            &envelope(
                6,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: phase,
                }),
            ),
        )
        .expect("completed phase");
        assert!(matches!(
            reduce(
                Some(state),
                &envelope(
                    7,
                    RuntimeCommittedEvent::ContextBoundaryCompleted(
                        ContextBoundaryCompletedEvent {
                            identity: turn,
                            projection_hash: ContentHash::digest(b"wrong"),
                            estimated_tokens: empty.estimated_tokens,
                            serialized_bytes: empty.serialized_bytes,
                        },
                    )
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryMeasurement)
        ));
    }

    #[test]
    fn context_reducer_requires_memory_before_model_compaction_and_latest_boundary() {
        let turn = context_identity("turn_start", 3, ContextBoundaryOrigin::UserTurn);
        let turn_phase = ContextPhaseIdentity {
            boundary: turn.clone(),
            phase: String::from("memory"),
        };
        let empty = measure_projection(&[]).expect("empty measurement");
        let before = context_identity("before_model_request", 7, ContextBoundaryOrigin::UserTurn);
        let state = replay(&[
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(context_graph()),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn.clone(),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: turn_phase.clone(),
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: turn_phase.clone(),
                }),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                    identity: turn.clone(),
                    projection_hash: empty.projection_hash,
                    estimated_tokens: empty.estimated_tokens,
                    serialized_bytes: empty.serialized_bytes,
                }),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: before.clone(),
                }),
            ),
        ])
        .expect("before-model boundary");
        assert!(matches!(
            reduce(
                Some(state.clone()),
                &envelope(
                    9,
                    RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                        identity: ContextPhaseIdentity {
                            boundary: before,
                            phase: String::from("compaction"),
                        },
                    })
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryTransition)
        ));
        assert!(matches!(
            reduce(
                Some(state),
                &envelope(
                    9,
                    RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                        identity: turn_phase,
                    })
                )
            ),
            Err(SessionReducerError::InvalidContextBoundaryTransition)
        ));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the replay fixture spells out the exact cross-node boundary journal"
    )]
    fn context_reducer_allows_only_exact_context_to_model_boundary_identity() {
        let request_hash = ContentHash::digest(b"ephemeral-request");
        let turn = ContextBoundaryIdentity {
            node_id: String::from("fresh-context"),
            boundary: String::from("turn_start"),
            run_id: String::from("run-ephemeral"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(3).expect("source head"),
        };
        let phase = ContextPhaseIdentity {
            boundary: turn.clone(),
            phase: String::from("memory"),
        };
        let empty = measure_projection(&[]).expect("empty measurement");
        let state = replay(&[
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(ephemeral_context_graph()),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("fresh-context"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: turn.clone(),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: phase.clone(),
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
                    identity: phase,
                }),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                    identity: turn,
                    projection_hash: empty.projection_hash,
                    estimated_tokens: empty.estimated_tokens,
                    serialized_bytes: empty.serialized_bytes,
                }),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                    node_id: String::from("fresh-context"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    result_reference: None,
                    artifact_reference: None,
                }),
            ),
            envelope(
                9,
                RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                    from_node_id: String::from("fresh-context"),
                    to_node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                10,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("respond"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 2,
                }),
            ),
        ])
        .expect("context-to-model state");
        let before = ContextBoundaryIdentity {
            node_id: String::from("respond"),
            boundary: String::from("before_model_request"),
            run_id: String::from("run-ephemeral"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(10).expect("source head"),
        };
        reduce(
            Some(state.clone()),
            &envelope(
                11,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: before.clone(),
                }),
            ),
        )
        .expect("exact compiled context-to-model edge");

        for invalid in [
            ContextBoundaryIdentity {
                run_id: String::from("other-run"),
                ..before.clone()
            },
            ContextBoundaryIdentity {
                request_hash: ContentHash::digest(b"other-request"),
                ..before.clone()
            },
            ContextBoundaryIdentity {
                origin: ContextBoundaryOrigin::ApprovalContinuation,
                ..before
            },
        ] {
            assert!(matches!(
                reduce(
                    Some(state.clone()),
                    &envelope(
                        11,
                        RuntimeCommittedEvent::ContextBoundaryStarted(
                            ContextBoundaryStartedEvent { identity: invalid }
                        ),
                    ),
                ),
                Err(SessionReducerError::InvalidContextBoundaryTransition)
            ));
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the reducer fixture spells out the exact fresh-context journal evidence"
    )]
    fn ephemeral_fresh_completion_state(
        replacement: Vec<ConversationEntry>,
        source_sequence: Sequence,
    ) -> SessionState {
        let mut style_binding = binding(BuiltInStyle::EphemeralTurn);
        style_binding.memory.provider = String::from("file");
        let compiled: CompiledSessionStyle =
            serde_json::from_str(&style_binding.compiled_style_json).expect("compiled style");
        let request_hash = ContentHash::digest(b"fresh-request");
        let boundary = ContextBoundaryIdentity {
            node_id: String::from("fresh-context"),
            boundary: String::from("turn_start"),
            run_id: String::from("run-current"),
            origin: ContextBoundaryOrigin::UserTurn,
            request_hash,
            source_head: Sequence::new(5).expect("source head"),
        };
        let phase = ContextPhaseIdentity {
            boundary: boundary.clone(),
            phase: String::from("memory"),
        };
        let measurement = measure_projection(&replacement).expect("replacement measurement");
        replay(&[
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style_binding.id.clone(),
                    style_binding: Some(Box::new(style_binding)),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: ConversationEntry::UserMessage(TextEntry {
                            id: ConversationEntryId(String::from("user:2:run-old")),
                            text: String::from("old"),
                            source_sequence: Sequence::new(2).expect("sequence"),
                        }),
                    },
                ),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ConversationEntryCommitted(
                    ConversationEntryCommittedEvent {
                        entry: ConversationEntry::UserMessage(TextEntry {
                            id: ConversationEntryId(String::from("user:3:run-current")),
                            text: String::from("current"),
                            source_sequence: Sequence::new(3).expect("sequence"),
                        }),
                    },
                ),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                    StyleExecutionInitializedEvent {
                        graph: Box::new(compiled.graph),
                        input_reference: None,
                    },
                )),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                    node_id: String::from("fresh-context"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                }),
            ),
            envelope(
                6,
                RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
                    identity: boundary.clone(),
                }),
            ),
            envelope(
                7,
                RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
                    identity: phase.clone(),
                }),
            ),
            envelope(
                8,
                RuntimeCommittedEvent::ContextProjectionReplaced(ContextProjectionReplacedEvent {
                    replacement,
                    provenance: ProjectionProvenance {
                        projection_id: String::from("fresh"),
                        source_range: Some((source_sequence, source_sequence)),
                        method: String::from("ephemeral_fresh_context"),
                        committed_at: Sequence::new(8).expect("sequence"),
                        artifact_id: None,
                    },
                    context_phase: Some(phase),
                }),
            ),
            envelope(
                9,
                RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
                    identity: boundary,
                    projection_hash: measurement.projection_hash,
                    estimated_tokens: measurement.estimated_tokens,
                    serialized_bytes: measurement.serialized_bytes,
                }),
            ),
        ])
        .expect("fresh completion state")
    }

    fn fresh_memory(
        provider: &str,
        injection_sequence: u64,
        injection_event: EventId,
    ) -> ConversationEntry {
        ConversationEntry::RetrievedMemory(crate::conversation::RetrievedMemoryEntry {
            id: ConversationEntryId(String::from("memory:fresh")),
            provider: provider.to_owned(),
            query: String::from("current"),
            scope: String::from("session:fixture"),
            source: String::from("fixture"),
            reference: String::from("memory-1"),
            score: Some(1.0),
            content: String::from("selected memory"),
            injection_sequence: Sequence::new(injection_sequence).expect("sequence"),
            injection_event: Some(injection_event),
            created_at_millis: 1,
            size_bytes: 15,
        })
    }

    #[test]
    fn ephemeral_context_completion_rejects_stale_user_and_memory_provenance() {
        let old = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user:2:run-old")),
            text: String::from("old"),
            source_sequence: Sequence::new(2).expect("sequence"),
        });
        let current = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user:3:run-current")),
            text: String::from("current"),
            source_sequence: Sequence::new(3).expect("sequence"),
        });
        let replacement_event = EventId::from_uuid(Uuid::from_u128(108));
        let wrong_event = EventId::from_uuid(Uuid::from_u128(999));
        let fabricated = ConversationEntry::UserMessage(TextEntry {
            id: ConversationEntryId(String::from("user:3:run-current")),
            text: String::from("fabricated"),
            source_sequence: Sequence::new(3).expect("sequence"),
        });
        let cases = [
            ephemeral_fresh_completion_state(vec![old], Sequence::new(2).expect("sequence")),
            ephemeral_fresh_completion_state(vec![fabricated], Sequence::new(3).expect("sequence")),
            ephemeral_fresh_completion_state(
                vec![fresh_memory("file", 7, replacement_event), current.clone()],
                Sequence::new(3).expect("sequence"),
            ),
            ephemeral_fresh_completion_state(
                vec![fresh_memory("file", 8, wrong_event), current.clone()],
                Sequence::new(3).expect("sequence"),
            ),
            ephemeral_fresh_completion_state(
                vec![
                    fresh_memory("sqlite-fts", 8, replacement_event),
                    current.clone(),
                ],
                Sequence::new(3).expect("sequence"),
            ),
        ];
        for state in cases {
            assert!(matches!(
                reduce(
                    Some(state),
                    &envelope(
                        10,
                        RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                            node_id: String::from("fresh-context"),
                            attempt: 1,
                            loop_iteration: 0,
                            step: 1,
                            result_reference: None,
                            artifact_reference: None,
                        }),
                    ),
                ),
                Err(SessionReducerError::InvalidStyleExecutionTransition)
            ));
        }

        reduce(
            Some(ephemeral_fresh_completion_state(
                vec![fresh_memory("file", 8, replacement_event), current],
                Sequence::new(3).expect("sequence"),
            )),
            &envelope(
                10,
                RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                    node_id: String::from("fresh-context"),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
                    result_reference: None,
                    artifact_reference: None,
                }),
            ),
        )
        .expect("current selected memory remains supported");
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
                    context_phase: None,
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
        assert_eq!(execution.execution_id.as_deref(), Some("execution-1"));
        assert_eq!(execution.action_digest, Some(digest));
        assert_eq!(execution.state, ToolExecutionState::Terminal);
        assert_eq!(execution.observed_event_count, 2);
        assert_eq!(
            execution.terminal_at,
            Some(Sequence::new(4).expect("sequence"))
        );
        assert!(matches!(
            execution.terminal_outcome,
            Some(ToolExecutionTerminalOutcome::Completed { .. })
        ));
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
                        input_reference: None,
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
        assert!(matches!(
            execution.control,
            StyleExecutionControlState::Active(StyleNodeEnteredEvent {
                ref node_id,
                step: 2,
                ..
            }) if node_id == "done"
        ));
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
                        input_reference: None,
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
                RuntimeCommittedEvent::StyleNodeFailed(StyleNodeFailedEvent {
                    node_id: "start".into(),
                    attempt: 1,
                    loop_iteration: 0,
                    step: 1,
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
        assert!(matches!(
            execution.control,
            StyleExecutionControlState::Terminal { ref reason }
                if reason == "retry_budget_exhausted"
        ));
    }

    fn style_initialized(graph: &ExecutableGraph) -> EventEnvelope<RuntimeCommittedEvent> {
        envelope(
            2,
            RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                StyleExecutionInitializedEvent {
                    graph: Box::new(graph.clone()),
                    input_reference: None,
                },
            )),
        )
    }

    fn style_entered(
        sequence: u64,
        node_id: &str,
        step: u64,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        envelope(
            sequence,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: node_id.into(),
                attempt: 1,
                loop_iteration: 0,
                step,
            }),
        )
    }

    fn style_completed(
        sequence: u64,
        node_id: &str,
        step: u64,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        envelope(
            sequence,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: node_id.into(),
                attempt: 1,
                loop_iteration: 0,
                step,
                result_reference: None,
                artifact_reference: None,
            }),
        )
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the replay test spells out each durable crash cut and terminal evidence"
    )]
    fn artifact_persistence_replays_explicit_crash_cuts_and_content_store_identity() {
        let identity = artifact_identity();
        let action_digest = ContentHash::digest(b"approved-artifact-action");
        let mut events = active_artifact_events();
        events.push(envelope(
            4,
            RuntimeCommittedEvent::ArtifactPersistenceProposed(ArtifactPersistenceProposedEvent {
                identity: identity.clone(),
                mime_type: String::from("application/vnd.agentmod.research-finding+json"),
                byte_size: 21,
            }),
        ));
        events.push(envelope(
            5,
            RuntimeCommittedEvent::ArtifactPersistenceApproved(ArtifactPersistenceApprovedEvent {
                identity: identity.clone(),
                action_digest,
            }),
        ));

        let approved = replay(&events).expect("approved crash cut replays");
        let record = approved
            .artifact_persistences
            .get(&identity.execution_id)
            .expect("approved outbox record");
        assert_eq!(record.identity, identity);
        assert_eq!(record.state, ArtifactPersistenceState::Approved);
        assert_eq!(
            record.resume_action(),
            ArtifactPersistenceResumeAction::DispatchApproved
        );
        assert_eq!(record.action_digest, Some(action_digest));
        assert_eq!(
            record.approved_at,
            Some(Sequence::new(5).expect("sequence"))
        );
        assert_eq!(record.dispatched_at, None);
        assert_eq!(record.completed_at, None);

        events.push(envelope(
            6,
            RuntimeCommittedEvent::ArtifactPersistenceDispatched(
                ArtifactPersistenceDispatchedEvent {
                    identity: identity.clone(),
                    action_digest,
                },
            ),
        ));
        let dispatched = replay(&events).expect("dispatched crash cut replays");
        let record = dispatched
            .artifact_persistences
            .get(&identity.execution_id)
            .expect("dispatched outbox record");
        assert_eq!(record.state, ArtifactPersistenceState::Dispatched);
        assert_eq!(
            record.resume_action(),
            ArtifactPersistenceResumeAction::ReconcileReceipt
        );
        assert_eq!(
            record.dispatched_at,
            Some(Sequence::new(6).expect("sequence"))
        );

        let artifact_id = format!("blake3:{}", "a".repeat(64));
        let artifact_reference = format!("artifact:{artifact_id}");
        events.push(envelope(
            7,
            RuntimeCommittedEvent::ArtifactPersistenceCompleted(
                ArtifactPersistenceCompletedEvent {
                    identity: identity.clone(),
                    action_digest,
                    artifact_id: artifact_id.clone(),
                    artifact_reference: artifact_reference.clone(),
                    mime_type: String::from("application/vnd.agentmod.research-finding+json"),
                    byte_size: 21,
                },
            ),
        ));
        let completed = replay(&events).expect("terminal receipt replays");
        let record = completed
            .artifact_persistences
            .get(&identity.execution_id)
            .expect("completed outbox record");
        assert_eq!(record.state, ArtifactPersistenceState::Completed);
        assert_eq!(
            record.resume_action(),
            ArtifactPersistenceResumeAction::CompleteNode
        );
        assert_eq!(record.artifact_id.as_deref(), Some(artifact_id.as_str()));
        assert_eq!(
            record.artifact_reference.as_deref(),
            Some(artifact_reference.as_str())
        );

        assert!(matches!(
            reduce(
                Some(completed.clone()),
                &envelope(
                    8,
                    RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                        node_id: String::from("persist"),
                        attempt: 1,
                        loop_iteration: 0,
                        step: 1,
                        result_reference: None,
                        artifact_reference: Some(String::from("artifact:blake3:mismatched")),
                    }),
                ),
            ),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));

        events.push(envelope(
            8,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: String::from("persist"),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
                result_reference: None,
                artifact_reference: Some(artifact_reference),
            }),
        ));
        let state = replay(&events).expect("receipt authorizes node completion");
        assert!(matches!(
            state.style_execution.expect("style execution").control,
            StyleExecutionControlState::AwaitingTransition(_)
        ));
    }

    #[test]
    fn persist_artifact_node_rejects_completion_without_terminal_evidence() {
        let mut events = active_artifact_events();
        events.push(envelope(
            4,
            RuntimeCommittedEvent::StyleNodeCompleted(StyleNodeCompletedEvent {
                node_id: String::from("persist"),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
                result_reference: None,
                artifact_reference: Some(String::from("artifact:blake3:missing")),
            }),
        ));
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the rejection test keeps each invalid ordering and identity case explicit"
    )]
    fn artifact_persistence_rejects_out_of_order_and_mismatched_requests() {
        let identity = artifact_identity();
        let action_digest = ContentHash::digest(b"approved-artifact-action");
        let mut proposed_events = active_artifact_events();
        proposed_events.push(envelope(
            4,
            RuntimeCommittedEvent::ArtifactPersistenceProposed(ArtifactPersistenceProposedEvent {
                identity: identity.clone(),
                mime_type: String::from("application/vnd.agentmod.research-finding+json"),
                byte_size: 21,
            }),
        ));
        let proposed = replay(&proposed_events).expect("proposal replays");

        assert!(matches!(
            reduce(
                Some(proposed.clone()),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ArtifactPersistenceDispatched(
                        ArtifactPersistenceDispatchedEvent {
                            identity: identity.clone(),
                            action_digest,
                        },
                    ),
                ),
            ),
            Err(SessionReducerError::InvalidArtifactPersistenceTransition)
        ));

        let mut wrong_identity = identity.clone();
        wrong_identity.content_hash = ContentHash::digest(b"different-content");
        assert!(matches!(
            reduce(
                Some(proposed.clone()),
                &envelope(
                    5,
                    RuntimeCommittedEvent::ArtifactPersistenceApproved(
                        ArtifactPersistenceApprovedEvent {
                            identity: wrong_identity,
                            action_digest,
                        },
                    ),
                ),
            ),
            Err(SessionReducerError::InvalidArtifactPersistenceTransition)
        ));

        let approved = reduce(
            Some(proposed),
            &envelope(
                5,
                RuntimeCommittedEvent::ArtifactPersistenceApproved(
                    ArtifactPersistenceApprovedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                ),
            ),
        )
        .expect("approval replays");
        let dispatched = reduce(
            Some(approved),
            &envelope(
                6,
                RuntimeCommittedEvent::ArtifactPersistenceDispatched(
                    ArtifactPersistenceDispatchedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                ),
            ),
        )
        .expect("dispatch replays");
        assert!(matches!(
            reduce(
                Some(dispatched),
                &envelope(
                    7,
                    RuntimeCommittedEvent::ArtifactPersistenceCompleted(
                        ArtifactPersistenceCompletedEvent {
                            identity,
                            action_digest,
                            artifact_id: format!("blake3:{}", "b".repeat(64)),
                            artifact_reference: format!("artifact:blake3:{}", "b".repeat(64)),
                            mime_type: String::from("application/json"),
                            byte_size: 21,
                        },
                    ),
                ),
            ),
            Err(SessionReducerError::InvalidArtifactPersistenceTransition)
        ));
    }

    fn style_transition(
        sequence: u64,
        from: &str,
        to: &str,
        step: u64,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        envelope(
            sequence,
            RuntimeCommittedEvent::StyleTransitionSelected(StyleTransitionSelectedEvent {
                from_node_id: from.into(),
                to_node_id: to.into(),
                attempt: 1,
                loop_iteration: 0,
                step,
            }),
        )
    }

    #[test]
    fn style_replay_rejects_skipped_entry_node() {
        let graph = compiled_graph();
        let events = vec![
            created(),
            style_initialized(&graph),
            style_entered(3, "done", 1),
        ];
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[test]
    fn style_replay_rejects_transition_from_unexecuted_node() {
        let graph = compiled_graph();
        let events = vec![
            created(),
            style_initialized(&graph),
            style_transition(3, "start", "done", 1),
        ];
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[test]
    fn style_replay_rejects_duplicate_transition() {
        let graph = compiled_graph();
        let events = vec![
            created(),
            style_initialized(&graph),
            style_entered(3, "start", 1),
            style_completed(4, "start", 1),
            style_transition(5, "start", "done", 1),
            style_transition(6, "start", "done", 1),
        ];
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[test]
    fn style_replay_rejects_wrong_transition_destination() {
        let graph = compiled_graph();
        let events = vec![
            created(),
            style_initialized(&graph),
            style_entered(3, "start", 1),
            style_completed(4, "start", 1),
            style_transition(5, "start", "start", 1),
        ];
        assert!(matches!(
            replay(&events),
            Err(SessionReducerError::InvalidStyleExecutionTransition)
        ));
    }

    #[test]
    fn style_replay_rejects_decreasing_or_skipped_counters() {
        let graph = compiled_graph();
        for destination_step in [1, 3] {
            let events = vec![
                created(),
                style_initialized(&graph),
                style_entered(3, "start", 1),
                style_completed(4, "start", 1),
                style_transition(5, "start", "done", 1),
                style_entered(6, "done", destination_step),
            ];
            assert!(matches!(
                replay(&events),
                Err(SessionReducerError::InvalidStyleExecutionTransition)
            ));
        }
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
                        input_reference: None,
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
                    context_phase: None,
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

    fn sequence(value: u64) -> Sequence {
        Sequence::new(value).expect("sequence")
    }

    fn summary_identity() -> ContextSummaryIdentity {
        ContextSummaryIdentity {
            summary_id: String::from("summary:run:1"),
            request_hash: ContentHash::digest(b"summary-request"),
            provider: String::from("mock"),
            model: String::from("deterministic-mock"),
            schema_version: 1,
            max_summary_bytes: 64 * 1024,
            source_range: Some((Sequence::FIRST, sequence(8))),
        }
    }

    fn reduce_all(events: Vec<EventEnvelope<RuntimeCommittedEvent>>) -> SessionState {
        let mut state: Option<SessionState> = None;
        for event in events {
            state = Some(reduce(state, &event).expect("reduce"));
        }
        state.expect("initialized")
    }

    #[test]
    fn summary_outbox_follows_proposal_approval_start_completion_ordering() {
        let identity = summary_identity();
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::ContextSummaryProposed(ContextSummaryProposedEvent {
                    identity: identity.clone(),
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::ContextSummaryApproved(ContextSummaryApprovedEvent {
                    identity: identity.clone(),
                    action_digest: ContentHash::digest(b"approved-summary"),
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::ContextSummaryStarted(ContextSummaryStartedEvent {
                    identity: identity.clone(),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::ContextSummaryCompleted(ContextSummaryCompletedEvent {
                    identity: identity.clone(),
                    content_hash: ContentHash::digest(b"bounded summary"),
                    text: String::from("bounded summary"),
                    input_tokens: 12,
                    output_tokens: 3,
                }),
            ),
        ];
        let state = reduce_all(events);
        let record = state
            .context_summaries
            .get(&identity.summary_id)
            .expect("record");
        assert_eq!(record.state, ContextSummaryState::Completed);
        assert!(record.has_terminal_evidence());
        assert_eq!(record.text.as_deref(), Some("bounded summary"));
        assert_eq!(record.input_tokens, 12);
    }

    #[test]
    fn summary_evidence_hash_must_match_text_and_completion_requires_start() {
        let identity = summary_identity();
        let bad_hash = envelope(
            2,
            RuntimeCommittedEvent::ContextSummaryProposed(ContextSummaryProposedEvent {
                identity: identity.clone(),
            }),
        );
        let approved = envelope(
            3,
            RuntimeCommittedEvent::ContextSummaryApproved(ContextSummaryApprovedEvent {
                identity: identity.clone(),
                action_digest: ContentHash::digest(b"approved"),
            }),
        );
        let completed_without_start = envelope(
            4,
            RuntimeCommittedEvent::ContextSummaryCompleted(ContextSummaryCompletedEvent {
                identity: identity.clone(),
                content_hash: ContentHash::digest(b"summary"),
                text: String::from("summary"),
                input_tokens: 1,
                output_tokens: 1,
            }),
        );
        assert!(matches!(
            reduce_all(vec![created(), bad_hash.clone()])
                .context_summaries
                .get("summary:run:1")
                .map(|r| r.state),
            Some(ContextSummaryState::Proposed)
        ));
        assert!(matches!(
            reduce(Some(reduce_all(vec![created(), bad_hash])), &approved)
                .and_then(|state| reduce(Some(state), &completed_without_start))
                .map(|_| ()),
            Err(SessionReducerError::InvalidSummaryTransition)
        ));

        let hash_mismatch = envelope(
            2,
            RuntimeCommittedEvent::ContextSummaryProposed(ContextSummaryProposedEvent {
                identity: summary_identity(),
            }),
        );
        let approved = envelope(
            3,
            RuntimeCommittedEvent::ContextSummaryApproved(ContextSummaryApprovedEvent {
                identity: summary_identity(),
                action_digest: ContentHash::digest(b"approved"),
            }),
        );
        let started = envelope(
            4,
            RuntimeCommittedEvent::ContextSummaryStarted(ContextSummaryStartedEvent {
                identity: summary_identity(),
            }),
        );
        let mismatched = envelope(
            5,
            RuntimeCommittedEvent::ContextSummaryCompleted(ContextSummaryCompletedEvent {
                identity: summary_identity(),
                content_hash: ContentHash::digest(b"different"),
                text: String::from("summary"),
                input_tokens: 1,
                output_tokens: 1,
            }),
        );
        let state = reduce_all(vec![created(), hash_mismatch, approved, started]);
        assert!(matches!(
            reduce(Some(state), &mismatched),
            Err(SessionReducerError::InvalidSummaryTransition)
        ));
    }

    fn memory_write_identity() -> MemoryWriteIdentity {
        MemoryWriteIdentity {
            write_id: String::from("write:turn:1:abc"),
            provider: String::from("file"),
            scope: String::from("session:s1"),
            source: String::from("auto:turn_completion"),
            content_hash: ContentHash::digest(b"durable fact"),
            deduplication_key: Some(String::from("canonical:write:turn:1")),
        }
    }

    #[test]
    fn memory_write_outbox_follows_proposal_approval_dispatch_receipt_ordering() {
        let identity = memory_write_identity();
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::MemoryWriteProposed(MemoryWriteProposedEvent {
                    identity: identity.clone(),
                    proposal_id: String::from("memory-write:1"),
                    max_bytes: 4096,
                    trigger: String::from("turn_completion"),
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::MemoryWriteApproved(MemoryWriteApprovedEvent {
                    identity: identity.clone(),
                    action_digest: ContentHash::digest(b"approved-write"),
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::MemoryWriteDispatched(MemoryWriteDispatchedEvent {
                    identity: identity.clone(),
                    action_digest: ContentHash::digest(b"approved-write"),
                }),
            ),
            envelope(
                5,
                RuntimeCommittedEvent::MemoryWriteCompleted(MemoryWriteCompletedEvent {
                    identity: identity.clone(),
                    action_digest: ContentHash::digest(b"approved-write"),
                    reference: String::from("memory-ref-1"),
                    retained: true,
                    deduplicated: false,
                }),
            ),
        ];
        let state = reduce_all(events);
        let record = state.memory_writes.get(&identity.write_id).expect("record");
        assert_eq!(record.state, MemoryWriteState::Completed);
        assert_eq!(record.reference.as_deref(), Some("memory-ref-1"));
        assert!(record.has_terminal_evidence());
    }

    #[test]
    fn memory_write_dispatch_without_approval_and_duplicate_proposal_are_rejected() {
        let identity = memory_write_identity();
        let proposed = envelope(
            2,
            RuntimeCommittedEvent::MemoryWriteProposed(MemoryWriteProposedEvent {
                identity: identity.clone(),
                proposal_id: String::from("memory-write:1"),
                max_bytes: 4096,
                trigger: String::from("turn_completion"),
            }),
        );
        let dispatched_without_approval = envelope(
            3,
            RuntimeCommittedEvent::MemoryWriteDispatched(MemoryWriteDispatchedEvent {
                identity: identity.clone(),
                action_digest: ContentHash::digest(b"approved-write"),
            }),
        );
        let state = reduce_all(vec![created(), proposed]);
        assert!(matches!(
            reduce(Some(state.clone()), &dispatched_without_approval),
            Err(SessionReducerError::InvalidMemoryWriteTransition)
        ));
        let duplicate = envelope(
            3,
            RuntimeCommittedEvent::MemoryWriteProposed(MemoryWriteProposedEvent {
                identity: memory_write_identity(),
                proposal_id: String::from("memory-write:2"),
                max_bytes: 4096,
                trigger: String::from("turn_completion"),
            }),
        );
        assert!(matches!(
            reduce(Some(state), &duplicate),
            Err(SessionReducerError::InvalidMemoryWriteTransition)
        ));
    }

    #[test]
    fn memory_write_failure_is_terminal_without_dispatch() {
        let identity = memory_write_identity();
        let events = vec![
            created(),
            envelope(
                2,
                RuntimeCommittedEvent::MemoryWriteProposed(MemoryWriteProposedEvent {
                    identity: identity.clone(),
                    proposal_id: String::from("memory-write:1"),
                    max_bytes: 4096,
                    trigger: String::from("turn_completion"),
                }),
            ),
            envelope(
                3,
                RuntimeCommittedEvent::MemoryWriteApproved(MemoryWriteApprovedEvent {
                    identity: identity.clone(),
                    action_digest: ContentHash::digest(b"approved-write"),
                }),
            ),
            envelope(
                4,
                RuntimeCommittedEvent::MemoryWriteFailed(MemoryWriteFailedEvent {
                    identity: identity.clone(),
                    code: String::from("approval_required"),
                    message: String::from("user policy requires approval"),
                }),
            ),
        ];
        let state = reduce_all(events);
        let record = state.memory_writes.get(&identity.write_id).expect("record");
        assert_eq!(record.state, MemoryWriteState::Failed);
        assert_eq!(record.failed_code.as_deref(), Some("approval_required"));
        assert!(record.has_terminal_evidence());
    }
}
