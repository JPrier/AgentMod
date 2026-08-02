use std::collections::BTreeMap;

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};

/// Complete versioned session-style manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionStyleManifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Stable identity and runtime compatibility.
    pub identity: StyleIdentity,
    /// Built-in or custom style.
    pub kind: StyleKind,
    /// Required built-in semantic when `kind` is `built_in`.
    pub built_in_semantic: Option<BuiltInStyle>,
    /// Declarative graph source or reference.
    pub graph: GraphSource,
    /// Ordered blocking interceptors.
    #[serde(default)]
    pub interceptors: Vec<InterceptorDeclaration>,
    /// Runtime capabilities required by this style.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Tool groups visible to the style.
    #[serde(default)]
    pub allowed_tool_groups: Vec<String>,
    /// Provider IDs usable by graph nodes.
    #[serde(default)]
    pub allowed_providers: Vec<String>,
    /// Plugin IDs permitted in the style.
    #[serde(default)]
    pub allowed_plugins: Vec<String>,
    /// Ordered exact plugin context transforms applied before model requests.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_transforms: Vec<ContextTransformSelection>,
    /// Harness selection and capability requirements.
    #[serde(default)]
    pub harness: HarnessSelection,
    /// Memory provider selection.
    pub memory: MemorySelection,
    /// Compaction strategy selection.
    pub compaction: CompactionSelection,
    /// Default approval behavior by action group.
    pub approvals: ApprovalDefaults,
    /// Hard execution budgets.
    pub budgets: ExecutionBudgets,
    /// Child-agent bounds.
    pub child_agents: ChildAgentLimits,
    /// Business retry policy.
    pub retry: RetryPolicy,
    /// Explicit terminal outcomes and hard-limit behavior.
    pub termination: TerminationPolicy,
    /// Top-level style selection policy.
    pub selection: TopLevelSelection,
}

/// Immutable selection of one plugin-provided context transform.
///
/// Vector order in [`SessionStyleManifest::context_transforms`] is execution
/// order and is retained byte-for-byte in the compiled style contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextTransformSelection {
    /// Exact allowed and activated plugin identity.
    pub plugin_id: String,
    /// Exact transform declaration identity within the plugin.
    pub transform_id: String,
    /// Exact selected semantic version; compatible substitution is prohibited.
    pub version: String,
    /// Hash of the exact authoritative plugin declaration.
    pub declaration_hash: ContentHash,
    /// Runtime lifecycle boundary at which the transform executes.
    pub lifecycle: ContextTransformLifecycle,
    /// Hash of the exact immutable adapter configuration.
    pub configuration_reference: ContentHash,
}

/// Supported context-transform lifecycle boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformLifecycle {
    /// Apply after canonical context construction and before a model request.
    BeforeModelRequest,
}

/// Harness selected by a style and the capabilities execution requires.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessSelection {
    /// Stable harness registry ID.
    pub id: String,
    /// Harness capabilities required in addition to runtime capabilities.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
}

impl Default for HarnessSelection {
    fn default() -> Self {
        Self {
            id: String::from("native"),
            required_capabilities: Vec::new(),
        }
    }
}

/// Stable style identity and runtime compatibility.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StyleIdentity {
    /// Lowercase stable style ID.
    pub id: String,
    /// Semantic style version.
    pub version: String,
    /// Semantic runtime API version requirement.
    pub runtime_api: String,
}

/// Style provenance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StyleKind {
    /// Shipped semantic descriptor.
    BuiltIn,
    /// User or plugin supplied descriptor.
    Custom,
}

/// Required built-in execution semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltInStyle {
    /// Persistent conversation with tools, memory, and compaction.
    PersistentChat,
    /// Fresh provider projection for each turn.
    EphemeralTurn,
    /// Bounded repeated research executions.
    ResearchLoop,
    /// Planner, workers, integrator, and reviewer.
    PlannerWorker,
    /// User-authored compiled graph.
    DeclarativeGraph,
}

/// Declarative graph source without filesystem access in this SDK.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum GraphSource {
    /// Inline versioned graph TOML.
    Inline {
        /// Complete graph source.
        source: String,
    },
    /// Content-addressed graph resolved by the composition root.
    Reference {
        /// Stable graph reference.
        id: String,
        /// Expected lowercase BLAKE3 digest.
        content_hash: String,
    },
}

/// Blocking interceptor registration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterceptorDeclaration {
    /// Unique handler ID within the style.
    pub id: String,
    /// Owning built-in component or plugin ID.
    pub owner: String,
    /// Proposal event handled by this interceptor.
    pub event: String,
    /// Broad stage, ascending.
    #[serde(default)]
    pub stage: u16,
    /// Priority within a stage, descending.
    #[serde(default)]
    pub priority: i32,
    /// Handlers which execute after this handler.
    #[serde(default)]
    pub before: Vec<String>,
    /// Handlers which execute before this handler.
    #[serde(default)]
    pub after: Vec<String>,
    /// Decision kinds the handler may emit.
    pub supported_decisions: Vec<DecisionCapability>,
    /// Capabilities required by this interceptor.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
}

/// Typed interceptor decision capability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionCapability {
    /// Continue with the proposal.
    Continue,
    /// Replace the proposal.
    Replace,
    /// Reject the proposal.
    Reject,
    /// Require durable approval.
    RequireApproval,
    /// Defer using a continuation.
    Defer,
    /// Cancel the proposal.
    Cancel,
    /// Fork supported execution.
    Fork,
}

/// Replaceable memory configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySelection {
    /// Provider ID, including `none`.
    pub provider: String,
    /// Exact plugin-provided implementation, when the provider is external.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<PluginMemorySelection>,
    /// Searchable scopes.
    #[serde(default)]
    pub scopes: Vec<MemoryScope>,
    /// Lifecycle boundary at which retrieval is requested.
    #[serde(default)]
    pub retrieval_timing: MemoryRetrievalTiming,
    /// Deterministic inputs used to construct the retrieval query.
    #[serde(default)]
    pub query: MemoryQueryConstruction,
    /// Maximum injected records.
    pub max_items: u32,
    /// Maximum injected bytes.
    pub max_injected_bytes: u64,
    /// Lifecycle boundary at which runtime-owned memory writes are requested.
    #[serde(default)]
    pub write_policy: MemoryWritePolicy,
    /// Provider-projection location for approved retrieved records.
    #[serde(default)]
    pub injection_location: MemoryInjectionLocation,
}

/// Immutable selection of one plugin-provided memory implementation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemorySelection {
    /// Exact allowed and activated plugin identity.
    pub plugin_id: String,
    /// Exact selected plugin semantic version.
    pub plugin_version: String,
    /// Memory-provider declaration identity within the plugin.
    pub provider_id: String,
    /// Exact selected provider semantic version.
    pub provider_version: String,
    /// Hash of the exact authoritative provider declaration.
    pub declaration_hash: ContentHash,
    /// Hash of the exact immutable adapter configuration.
    pub configuration_reference: ContentHash,
}

/// Memory retrieval lifecycle boundary.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryRetrievalTiming {
    /// Never retrieve memory.
    #[default]
    Never,
    /// Retrieve once when a user turn starts.
    TurnStart,
    /// Retrieve once when a bounded graph iteration starts.
    IterationStart,
    /// Retrieve whenever a context-transform graph node executes.
    ContextNode,
    /// Retrieve immediately before each provider request.
    BeforeModelRequest,
}

/// Deterministic memory-query construction settings.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryQueryConstruction {
    /// Runtime-owned source text for the query.
    pub source: MemoryQuerySource,
    /// Include descriptions of active artifacts in the query.
    pub include_active_artifacts: bool,
    /// Include the selected style ID and active graph-node ID.
    pub include_style_context: bool,
    /// Maximum UTF-8 bytes in the constructed query.
    pub max_query_bytes: u32,
}

impl Default for MemoryQueryConstruction {
    fn default() -> Self {
        Self {
            source: MemoryQuerySource::CurrentInput,
            include_active_artifacts: false,
            include_style_context: false,
            max_query_bytes: 16 * 1024,
        }
    }
}

/// Runtime-owned primary source for a memory query.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryQuerySource {
    /// The current user input.
    #[default]
    CurrentInput,
    /// The session goal supplied to the active execution.
    SessionGoal,
    /// The current input followed by the session goal.
    CurrentInputAndGoal,
    /// An explicit query supplied by a graph node or calling client.
    Explicit,
}

/// Runtime-owned memory write boundary.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryWritePolicy {
    /// Never request an automatic memory write.
    #[default]
    Never,
    /// Write only through an explicit graph node or client request.
    ExplicitOnly,
    /// Propose a write after successful turn completion.
    TurnCompletion,
    /// Propose a write after successful bounded-iteration completion.
    IterationCompletion,
    /// Propose a write only when the session reaches a terminal success.
    SessionCompletion,
}

/// Location of approved memory records in a provider projection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryInjectionLocation {
    /// Do not inject retrieved records.
    #[default]
    None,
    /// Insert records before provider-visible conversation history.
    BeforeConversation,
    /// Insert records after provider-visible conversation history.
    AfterConversation,
    /// Insert records immediately before the current user input.
    BeforeCurrentInput,
    /// Expose records through a typed context artifact reference.
    ContextArtifact,
}

/// Memory scope.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    /// Current session.
    Session,
    /// Current project.
    Project,
    /// Current user.
    User,
    /// Entire runtime.
    Runtime,
}

/// Replaceable compaction configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionSelection {
    /// Selected strategy.
    pub strategy: CompactionStrategy,
    /// Exact plugin-provided implementation, when the strategy is `plugin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<PluginCompactorSelection>,
    /// Token threshold, when the strategy is automatic.
    pub trigger_tokens: Option<u64>,
    /// Tokens reserved for provider output and runtime-required context.
    #[serde(default)]
    pub reserved_context_tokens: u64,
    /// Maximum provider-visible projection after compaction.
    ///
    /// Zero retains schema-v1 behavior and delegates the hard projection limit
    /// to the runtime/provider boundary.
    #[serde(default)]
    pub max_provider_projection_tokens: u64,
    /// Preserve unresolved tasks.
    pub preserve_unresolved_tasks: bool,
    /// Preserve active process state.
    pub preserve_active_processes: bool,
    /// Typed provider-projection records a compactor must retain.
    #[serde(default = "default_compaction_preservation_requirements")]
    pub preservation_requirements: Vec<CompactionPreservationRequirement>,
}

/// Immutable selection of one plugin-provided compactor implementation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompactorSelection {
    /// Exact allowed and activated plugin identity.
    pub plugin_id: String,
    /// Exact selected plugin semantic version.
    pub plugin_version: String,
    /// Compactor declaration identity within the plugin.
    pub compactor_id: String,
    /// Exact selected compactor semantic version.
    pub compactor_version: String,
    /// Hash of the exact authoritative compactor declaration.
    pub declaration_hash: ContentHash,
    /// Hash of the exact immutable adapter configuration.
    pub configuration_reference: ContentHash,
}

/// Provider-projection records which a compaction strategy must retain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPreservationRequirement {
    /// Effective system and developer instructions.
    SystemInstructions,
    /// Current user input.
    CurrentInput,
    /// Pending approval and continuation references.
    PendingControlState,
    /// References to immutable artifacts selected for the projection.
    ArtifactReferences,
    /// Provenance for injected memory records.
    MemoryProvenance,
    /// Active style graph and node identity.
    ActiveGraphState,
    /// Tool-call/result correlation required for the next provider request.
    ToolCallCorrelation,
}

fn default_compaction_preservation_requirements() -> Vec<CompactionPreservationRequirement> {
    vec![
        CompactionPreservationRequirement::SystemInstructions,
        CompactionPreservationRequirement::CurrentInput,
        CompactionPreservationRequirement::PendingControlState,
        CompactionPreservationRequirement::ArtifactReferences,
        CompactionPreservationRequirement::MemoryProvenance,
        CompactionPreservationRequirement::ActiveGraphState,
        CompactionPreservationRequirement::ToolCallCorrelation,
    ]
}

/// Built-in compaction strategy identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStrategy {
    /// Sliding provider-visible window.
    SlidingWindow,
    /// Structured summary.
    Summary,
    /// Immutable artifact handoff.
    ArtifactHandoff,
    /// Evict large tool output projections.
    ToolOutputEviction,
    /// Invoke the exact plugin compactor selected by the immutable style.
    Plugin,
    /// Disable compaction.
    None,
}

/// Permission default.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Automatically allow after mandatory policy.
    Allow,
    /// Require user approval.
    Ask,
    /// Deny the action.
    Deny,
}

/// Default approval map; mandatory runtime security remains authoritative.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDefaults {
    /// Fallback decision.
    pub default: ApprovalDecision,
    /// Overrides by stable action or tool group.
    #[serde(default)]
    pub groups: BTreeMap<String, ApprovalDecision>,
}

/// Hard style-wide execution budgets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBudgets {
    /// Maximum loop/research iterations.
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

/// Child-session resource bounds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildAgentLimits {
    /// Total children created by one session.
    pub max_children: u32,
    /// Maximum concurrently active children.
    pub max_concurrent: u32,
    /// Recursive spawn depth.
    pub max_depth: u16,
    /// Token budget for one child.
    pub per_child_token_budget: u64,
    /// Exact child style selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_style: Option<String>,
    /// Workspace isolation selected for every child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_mode: Option<ChildWorkspaceMode>,
    /// Explicit merge policy required by branch workspaces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_merge_policy: Option<ChildWorkspaceMergePolicy>,
    /// Explicit custom workspace locator when that mode is selected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_workspace: Option<String>,
    /// Whether a child inherits the parent provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_provider: Option<bool>,
    /// Whether a child inherits the parent model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_model: Option<bool>,
    /// Whether a child inherits the parent's exact authenticated MCP bootstrap.
    ///
    /// An omitted value is semantically `false`, preserving historical style
    /// manifests and their canonical hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_mcp: Option<bool>,
    /// Maximum provider-context contribution for one child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_budget_tokens: Option<u64>,
    /// Maximum cost for one child in micros.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_child_cost_budget_micros: Option<u64>,
    /// Tool groups available to children.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_groups: Vec<String>,
    /// Child access to parent/session memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_access: Option<ChildMemoryAccess>,
    /// Parent wait/join behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_behavior: Option<ChildJoinBehavior>,
    /// Parent cancellation propagation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_behavior: Option<ChildCancellationBehavior>,
    /// Maximum reviewer attempts involving child revisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_max_attempts: Option<u32>,
}

/// Workspace isolation for runtime-managed child sessions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWorkspaceMode {
    /// Children may read the shared workspace but may not mutate it.
    SharedReadOnly,
    /// Children share one workspace with runtime-serialized writes.
    SharedSerializedWrites,
    /// Each child receives an independent Git worktree.
    IndependentGitWorktree,
    /// Each child receives a temporary workspace copy.
    TemporaryCopy,
    /// Each child receives a bounded runtime-owned filesystem copy.
    IsolatedCopy,
    /// Each child receives an owned Git worktree with explicit merge policy.
    BranchWorkspace,
    /// The style provides an explicit custom workspace locator.
    ExplicitCustomWorkspace,
}

/// Immutable integration policy for a branch child workspace.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWorkspaceMergePolicy {
    /// No implicit merge; an explicit reviewed action is required.
    ManualReview,
    /// A reviewed fast-forward action may be proposed separately.
    ReviewedFastForward,
    /// A reviewed three-way merge action may be proposed separately.
    ReviewedThreeWay,
}

/// Memory visibility granted to a child.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMemoryAccess {
    /// Child receives no memory access.
    None,
    /// Child may retrieve but not write memory.
    ReadOnly,
    /// Child may retrieve and write within selected scopes.
    ReadWrite,
}

/// Parent join semantics for a child set.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildJoinBehavior {
    /// Wait for every expected child.
    All,
    /// Continue after the first successful child.
    FirstSuccess,
    /// Continue after any child reaches a terminal state.
    AnyTerminal,
}

/// Parent cancellation propagation for active children.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildCancellationBehavior {
    /// Parent cancellation propagates to every active child.
    Cascade,
    /// Children remain independently active.
    Detach,
    /// Parent cancellation waits for child terminal states.
    Wait,
}

/// Bounded business retry policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Total attempts including the first.
    pub max_attempts: u32,
    /// Initial backoff.
    pub initial_backoff_ms: u64,
    /// Maximum backoff.
    pub max_backoff_ms: u64,
    /// Stable retryable failure classes.
    #[serde(default)]
    pub retryable_failures: Vec<String>,
}

/// Explicit terminal behavior.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminationPolicy {
    /// Outcomes graph terminal nodes may produce.
    pub allowed_outcomes: Vec<TerminationOutcome>,
    /// Outcome committed when any hard limit is reached.
    pub on_hard_limit: TerminationOutcome,
    /// Require a graph terminal node on every path.
    pub require_explicit_terminal_node: bool,
}

/// Terminal session outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationOutcome {
    /// Complete the current turn.
    CompleteTurn,
    /// Complete the session.
    CompleteSession,
    /// Fail with a bounded structured reason.
    Fail,
    /// Cancel the session.
    Cancel,
}

/// Top-level selection constraints.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopLevelSelection {
    /// A frontend, CLI, RPC, or configuration must select the style.
    pub requires_explicit_selection: bool,
    /// Whether a model may select this top-level style.
    pub model_may_select: bool,
}
