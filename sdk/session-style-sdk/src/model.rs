use std::collections::BTreeMap;

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
    /// Searchable scopes.
    #[serde(default)]
    pub scopes: Vec<MemoryScope>,
    /// Maximum injected records.
    pub max_items: u32,
    /// Maximum injected bytes.
    pub max_injected_bytes: u64,
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
    /// Token threshold, when the strategy is automatic.
    pub trigger_tokens: Option<u64>,
    /// Preserve unresolved tasks.
    pub preserve_unresolved_tasks: bool,
    /// Preserve active process state.
    pub preserve_active_processes: bool,
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
