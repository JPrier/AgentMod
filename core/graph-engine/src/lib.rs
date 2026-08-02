//! Versioned, deterministic compilation of generic `AgentMod` execution graphs.
//!
//! This crate validates structure and compiles inspectable execution data. It
//! deliberately assigns no runtime-specific behavior to graph node kinds.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use agentmod_expression_engine::{Expression, ExpressionLimits, Operand, ParseError, PathSegment};
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current human-editable graph format version.
pub const GRAPH_FORMAT_VERSION: u16 = 1;

/// Bounds applied while parsing and compiling an untrusted graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompilerLimits {
    /// Maximum TOML source bytes.
    pub max_source_bytes: usize,
    /// Maximum nodes.
    pub max_nodes: usize,
    /// Maximum edges.
    pub max_edges: usize,
    /// Maximum graph variable declarations.
    pub max_variables: usize,
    /// Maximum entries in one configuration collection.
    pub max_configuration_items: usize,
    /// Maximum serialized bytes in one node configuration.
    pub max_configuration_bytes: usize,
    /// Maximum nesting depth in a typed variable or configuration value.
    pub max_value_depth: usize,
    /// Maximum configured native parallelism.
    pub max_parallelism: u32,
    /// Maximum UTF-8 bytes in an identifier or declaration.
    pub max_name_bytes: usize,
    /// Maximum retry count on one node.
    pub max_retry_limit: u32,
    /// Maximum static loop iterations.
    pub max_loop_iterations: u32,
    /// Maximum graph step budget.
    pub max_steps: u64,
    /// Maximum graph token budget.
    pub max_tokens: u64,
    /// Maximum graph cost budget in micros.
    pub max_cost_micros: u64,
    /// Maximum graph duration budget in milliseconds.
    pub max_duration_ms: u64,
    /// Allows an exact historical graph to retain an unconfigured artifact
    /// persistence node while its immutable compiled identity is recovered.
    ///
    /// This compatibility switch is false for every untrusted/current graph.
    /// Callers may enable it only after matching a frozen versioned descriptor.
    pub allow_legacy_unconfigured_artifact_persistence: bool,
    /// Limits for every embedded condition.
    pub expression: ExpressionLimits,
}

impl Default for CompilerLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 1024 * 1024,
            max_nodes: 10_000,
            max_edges: 50_000,
            max_variables: 10_000,
            max_configuration_items: 1_024,
            max_configuration_bytes: 256 * 1_024,
            max_value_depth: 32,
            max_parallelism: 256,
            max_name_bytes: 256,
            max_retry_limit: 32,
            max_loop_iterations: 10_000,
            max_steps: 10_000_000,
            max_tokens: 10_000_000_000,
            max_cost_micros: 1_000_000_000_000,
            max_duration_ms: 365 * 24 * 60 * 60 * 1_000,
            allow_legacy_unconfigured_artifact_persistence: false,
            expression: ExpressionLimits::default(),
        }
    }
}

/// Inputs that bind compiled graph cache entries to runtime compatibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCacheInputs {
    /// Hash of the validated plugin set.
    pub plugin_set_hash: ContentHash,
    /// Runtime API version used to interpret nodes.
    pub runtime_api_version: String,
    /// Actual runtime capability set.
    pub capability_set: BTreeSet<String>,
}

/// Parsed versioned graph source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDefinition {
    /// Source format version.
    pub format_version: u16,
    /// Stable entry node ID.
    pub entry: String,
    /// Hard execution budgets.
    pub budget: GraphBudget,
    /// Capabilities and implementations the graph declares.
    #[serde(default)]
    pub declarations: GraphDeclarations,
    /// Canonical typed variable declarations.
    #[serde(default)]
    pub variables: Vec<VariableDeclaration>,
    /// Generic node definitions.
    pub nodes: Vec<NodeDefinition>,
    /// Directed transitions.
    pub edges: Vec<EdgeDefinition>,
}

impl GraphDefinition {
    /// Parses versioned TOML while applying source-size bounds.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError`] when the source is too large or is not valid
    /// versioned graph TOML.
    pub fn parse(source: &str, limits: CompilerLimits) -> Result<Self, GraphError> {
        if source.len() > limits.max_source_bytes {
            return Err(GraphError::SourceTooLarge {
                actual: source.len(),
                maximum: limits.max_source_bytes,
            });
        }
        toml::from_str(source).map_err(|error| GraphError::InvalidToml {
            detail: error.message().to_owned(),
        })
    }
}

/// Hard execution budgets declared by a graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphBudget {
    /// Maximum node transitions.
    pub max_steps: u64,
    /// Maximum provider tokens.
    pub max_tokens: u64,
    /// Maximum provider cost in micros of the configured currency.
    pub max_cost_micros: u64,
    /// Maximum wall-clock execution duration.
    pub max_duration_ms: u64,
}

/// Graph-level implementation declarations.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDeclarations {
    /// Capability names.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Tool names.
    #[serde(default)]
    pub tools: BTreeSet<String>,
    /// Provider names.
    #[serde(default)]
    pub providers: BTreeSet<String>,
    /// User-space event types the graph may emit.
    #[serde(default)]
    pub events: BTreeSet<String>,
    /// Plugin identities the graph may invoke.
    #[serde(default)]
    pub plugins: BTreeSet<String>,
}

/// Canonical graph variable declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariableDeclaration {
    /// Stable variable name.
    pub name: String,
    /// Bounded value type.
    #[serde(rename = "type")]
    pub value_type: VariableValueType,
    /// Lifetime and visibility scope.
    pub scope: VariableScope,
    /// Producing node ID, or the reserved `runtime` producer.
    pub producer: String,
    /// Additional graph nodes authorized to contribute to this variable
    /// through one compiler-proven parallel merge region.
    #[serde(default)]
    pub merge_contributors: BTreeSet<String>,
    /// Nodes allowed to consume the value.
    pub consumers: BTreeSet<String>,
    /// Whether the producer may assign more than once.
    pub mutability: VariableMutability,
    /// Deterministic merge policy for shared writes.
    #[serde(default)]
    pub merge_policy: Option<VariableMergePolicy>,
    /// Maximum canonical serialized value bytes.
    pub max_size_bytes: u64,
    /// Information-flow classification.
    pub security_classification: SecurityClassification,
}

/// Recursive, bounded canonical variable type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VariableValueType {
    /// Boolean.
    Boolean,
    /// Signed 64-bit integer.
    Integer,
    /// Canonical decimal string.
    Decimal,
    /// UTF-8 string.
    String,
    /// Closed set of tags.
    Enum {
        /// Allowed tag values.
        values: BTreeSet<String>,
    },
    /// Homogeneous bounded list.
    List {
        /// Element type.
        item_type: Box<Self>,
        /// Maximum element count.
        max_items: u32,
    },
    /// Homogeneous bounded map.
    Map {
        /// Value type.
        value_type: Box<Self>,
        /// Maximum entry count.
        max_entries: u32,
    },
    /// Session identity.
    SessionId,
    /// Child-session identity.
    ChildId,
    /// Task identity.
    TaskId,
    /// Immutable artifact reference.
    ArtifactReference,
    /// Opaque reference to secret material held outside the graph environment.
    SecretReference,
    /// Tool-result reference.
    ToolResultReference,
    /// Approval result.
    ApprovalResult,
    /// Node-result reference.
    NodeResultReference,
    /// Runtime-recorded timestamp.
    Timestamp,
    /// Runtime-recorded duration.
    Duration,
}

/// Canonical variable lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariableScope {
    /// Current node only.
    Node,
    /// Current parallel branch.
    Branch,
    /// Current graph run.
    Run,
    /// Current session.
    Session,
}

/// Canonical variable assignment policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariableMutability {
    /// Exactly one assignment.
    Immutable,
    /// Versioned reassignment is allowed.
    Mutable,
}

/// Deterministic shared-write merge operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariableMergePolicy {
    /// Append branch values in stable branch order.
    Append,
    /// Set union in canonical value order.
    Union,
    /// Recursively merge maps and reject conflicting leaves.
    DeepMerge,
    /// Select the first stable branch value.
    FirstBranch,
}

/// Information-flow classification for variables and messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityClassification {
    /// Safe for ordinary provider and user projections.
    Public,
    /// Session-internal, non-secret data.
    Internal,
    /// Sensitive data requiring explicit projection policy.
    Confidential,
    /// Secret reference only; inline secret values remain prohibited.
    SecretReference,
}

/// Generic graph node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDefinition {
    /// Stable node ID.
    pub id: String,
    /// Generic node kind.
    pub kind: NodeKind,
    /// Kind-specific bounded execution configuration.
    #[serde(default)]
    pub configuration: Option<NodeConfiguration>,
    /// Optional condition evaluated before runtime execution.
    #[serde(default)]
    pub condition: Option<String>,
    /// Tool selected by tool-execution nodes.
    #[serde(default)]
    pub tool: Option<String>,
    /// Provider selected by model/review nodes.
    #[serde(default)]
    pub provider: Option<String>,
    /// Additional capabilities required by this node.
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    /// State scopes read by the node.
    #[serde(default)]
    pub read_scopes: BTreeSet<String>,
    /// State scopes proposed for writing by the node.
    #[serde(default)]
    pub write_scopes: BTreeSet<String>,
    /// Declared canonical variables read by this node.
    #[serde(default)]
    pub read_variables: BTreeSet<String>,
    /// Declared canonical variables written by this node.
    #[serde(default)]
    pub write_variables: BTreeSet<String>,
    /// Business retry limit.
    #[serde(default)]
    pub retry_limit: u32,
    /// Static iteration bound; required only for loop nodes.
    #[serde(default)]
    pub max_iterations: Option<u32>,
}

/// Kind-specific, bounded node configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub enum NodeConfiguration {
    /// Style-selected provider-context composition.
    ContextTransform {
        /// Whether history is preserved or replaced with the current input.
        strategy: ContextTransformStrategy,
    },
    /// Runtime-owned provider request with canonical bounded outputs.
    ModelRequest {
        /// Declared enum/tag variable receiving the provider disposition.
        disposition_output: String,
        /// Declared node-result reference receiving the exact provider result.
        result_output: String,
        /// Bounded provider-specific string options retained in stable key order.
        #[serde(default)]
        provider_options: BTreeMap<String, String>,
        /// Declared ordinary output variable mapped to an RFC 6901 JSON Pointer.
        #[serde(default)]
        json_outputs: BTreeMap<String, String>,
        /// Declared bounded canonical inputs supplied to the provider request.
        #[serde(default)]
        inputs: BTreeMap<String, NodeValueSource>,
    },
    /// Exact arguments supplied to the declared tool.
    ToolExecution {
        /// Bounded static value or declared canonical variable.
        arguments: NodeValueSource,
    },
    /// Runtime-owned execution of the canonical tool batch proposed by a model result.
    ProviderToolBatchExecution {
        /// Declared node-result reference produced by the model request.
        request_reference_variable: String,
        /// Declared model-disposition variable paired with the request reference.
        disposition_variable: String,
        /// Maximum canonical tool proposals accepted from the provider result.
        maximum_calls: u32,
        /// Exact graph-declared tools that the provider may propose.
        allowed_tools: BTreeSet<String>,
    },
    /// Runtime-owned manual approval gate.
    UserApproval {
        /// Bounded static summary or declared string variable.
        action_summary: NodeTextSource,
    },
    /// Runtime-owned child-session creation proposal.
    SpawnChildAgent {
        /// One task, a bounded list of task strings, or a bounded map keyed by task ID.
        task_input: NodeValueSource,
        /// Stable prefix used when the task input does not supply task IDs.
        task_id_prefix: String,
        /// Exact child style selector.
        child_style: String,
        /// Tool groups granted to every proposed child.
        #[serde(default)]
        tool_groups: BTreeSet<String>,
        /// Maximum children this node may propose.
        maximum_children: u32,
        /// Maximum recursive child depth.
        maximum_depth: u32,
        /// Hard token budget per child.
        token_budget: u64,
        /// Maximum provider-context contribution per child.
        context_budget_tokens: u64,
        /// Hard cost budget per child.
        cost_budget_micros: u64,
        /// Workspace isolation.
        workspace: ChildWorkspaceConfiguration,
        /// Declared immutable artifact references available to each child task.
        #[serde(default)]
        artifact_references: BTreeSet<String>,
        /// Canonical artifact-reference variables resolved when the node runs.
        #[serde(default)]
        artifact_reference_variables: BTreeSet<String>,
        /// Information-flow classification of the task input.
        security_classification: SecurityClassification,
        /// Child creation is consequential and must always require approval.
        approval_required: bool,
    },
    /// Replay-derived wait over one exact child set.
    WaitForAgents {
        /// Exact child IDs or a canonical list-of-child-ID variable.
        children: ChildSetSource,
        /// Maximum child count when the set is resolved from a variable.
        maximum_children: u32,
        /// Minimum successful children required to continue.
        minimum_successes: u32,
        /// Durable timeout from the canonical wait start.
        timeout_ms: u64,
        /// Parent cancellation behavior.
        cancellation: ChildWaitCancellation,
    },
    /// Runtime-validated structured reviewer result.
    Review {
        /// Bounded integration/evidence input.
        input: NodeValueSource,
        /// Declared artifact evidence supplied to the reviewer.
        #[serde(default)]
        artifact_references: BTreeSet<String>,
        /// Canonical artifact-reference variables resolved for this review.
        #[serde(default)]
        artifact_reference_variables: BTreeSet<String>,
        /// Exact bounded result contract.
        result_schema: ReviewResultSchema,
        /// Explicit graph destinations for every reviewer disposition.
        routes: ReviewRoutes,
        /// Maximum revision index accepted by this node.
        maximum_revisions: u32,
    },
    /// Runtime-owned immutable artifact persistence.
    PersistArtifact {
        /// Bounded static content or declared canonical variable.
        content: ArtifactContentSource,
        /// Valid bounded media type.
        mime_type: String,
        /// Artifact information-flow handling.
        security: ArtifactSecurityClassification,
        /// Artifact retention policy.
        retention: ArtifactRetentionPolicy,
    },
    /// Canonical child-session message.
    SendChildAgentMessage {
        /// Exact child or a canonical child-ID variable.
        child: ChildSelector,
        /// Bounded typed payload.
        payload: serde_json::Value,
        /// Declared artifact references attached to the message.
        #[serde(default)]
        artifact_references: BTreeSet<String>,
        /// Payload information-flow classification.
        security_classification: SecurityClassification,
        /// Maximum canonical message bytes.
        max_message_bytes: u64,
        /// Delivery behavior when cancellation has begun.
        cancellation: ChildMessageCancellation,
    },
    /// Replayable child or branch result join.
    JoinResults {
        /// Required child or branch references.
        required: BTreeSet<String>,
        /// Optional child or branch references.
        #[serde(default)]
        optional: BTreeSet<String>,
        /// Minimum successful results.
        minimum_successes: u32,
        /// Failure handling.
        failure_policy: JoinFailurePolicy,
        /// Result ordering.
        ordering_policy: JoinOrderingPolicy,
        /// Durable timeout in milliseconds.
        timeout_ms: u64,
        /// Whether cancellation propagates to incomplete members.
        cancellation_propagates: bool,
        /// Projection into the canonical graph environment.
        result_projection: JoinResultProjection,
        /// Artifact collection behavior.
        artifact_collection: JoinArtifactCollection,
    },
    /// Bounded parallel graph fan-out.
    ParallelBranch {
        /// Maximum concurrently executing branches.
        max_parallelism: u32,
        /// Maximum queued branches.
        max_queue_depth: u32,
        /// Exact join node ID.
        join_target: String,
        /// Readiness/failure policy.
        join_policy: ParallelJoinPolicy,
        /// Per-variable merge overrides.
        #[serde(default)]
        variable_merge_policies: BTreeMap<String, VariableMergePolicy>,
        /// Optional stable serialization policy for shared resources.
        #[serde(default)]
        serialization_policy: Option<ParallelSerializationPolicy>,
    },
    /// Durable delay.
    Delay {
        /// Exact duration or wake timestamp selected at compilation.
        resolution: DelayResolution,
        /// Optional expiration timestamp.
        #[serde(default)]
        expiration_timestamp: Option<String>,
        /// Cancellation behavior.
        cancellation: DelayCancellation,
    },
    /// Consequential scheduler operation.
    Schedule {
        /// Trigger to create or await.
        trigger: ScheduleTrigger,
        /// Whether this node waits through the runtime-owned durable continuation
        /// created for the trigger registration.
        wait_for_trigger: bool,
        /// Cancellation behavior.
        cancellation: ScheduleCancellation,
    },
    /// Constrained user-space event.
    EmitEvent {
        /// Declared user-space event type.
        event_type: String,
        /// Bounded typed payload.
        payload: serde_json::Value,
        /// Declared artifact references.
        #[serde(default)]
        artifact_references: BTreeSet<String>,
        /// Non-secret metadata.
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
    /// Runtime-owned completion of one canonical turn result.
    CompleteTurn {
        /// Declared node-result reference finalized into the assistant turn.
        result_reference_variable: String,
        /// Provider-projection handling after canonical assistant commitment.
        cleanup: CompleteTurnCleanup,
    },
    /// Plugin-host-backed implementation of an existing serialized node kind.
    Plugin {
        /// Exact allowed plugin identity.
        plugin_id: String,
        /// Exact executor identity.
        executor_id: String,
        /// Exact executor version.
        executor_version: String,
        /// Serialized graph kind implemented by the plugin.
        node_kind: NodeKind,
        /// Declared input schema reference.
        input_schema: String,
        /// Declared output schema reference.
        output_schema: String,
        /// Exact immutable configuration reference.
        configuration_reference: String,
        /// Bounded instance input.
        #[serde(default)]
        input: serde_json::Value,
    },
}

/// Context projection semantics declared by a graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformStrategy {
    /// Preserve canonical conversation history and apply style-selected composition.
    PreserveHistory,
    /// Isolate the provider projection to the current canonical input.
    Fresh,
}

/// Provider-projection handling performed by the complete-turn executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompleteTurnCleanup {
    /// Retain the provider projection after canonical assistant commitment.
    PreserveProjection,
    /// Durably discard the provider projection after canonical assistant commitment.
    DiscardProjection,
}

/// Bounded node value source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum NodeValueSource {
    /// Static typed JSON retained in the immutable graph.
    Static {
        /// Exact bounded value.
        value: serde_json::Value,
    },
    /// Value read from one declared canonical variable.
    Variable {
        /// Exact variable name.
        variable: String,
    },
}

/// Bounded approval-summary source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum NodeTextSource {
    /// Static bounded text.
    Static {
        /// Exact text.
        value: String,
    },
    /// Text read from one declared string variable.
    Variable {
        /// Exact variable name.
        variable: String,
    },
}

/// Workspace isolation retained in an immutable child-spawn node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum ChildWorkspaceConfiguration {
    /// Child may inspect but not mutate the shared workspace.
    SharedReadOnly,
    /// Writes are serialized under one stable graph-owned key.
    SharedSerializedWrites {
        /// Stable serialization key.
        serialization_key: String,
    },
    /// Child receives an independent Git worktree.
    IndependentGitWorktree,
    /// Child receives a temporary workspace copy.
    TemporaryCopy,
    /// Child receives a bounded runtime-owned filesystem copy.
    IsolatedCopy,
    /// Child receives an owned Git worktree with an explicit merge policy.
    BranchWorkspace {
        /// Policy retained by the lease. Materialization never performs a merge.
        merge_policy: ChildWorkspaceMergePolicy,
    },
    /// Graph supplies an explicit bounded workspace locator.
    ExplicitCustomWorkspace {
        /// Static path or declared canonical string variable.
        path: NodeTextSource,
    },
}

/// Explicit integration policy retained by a branch workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWorkspaceMergePolicy {
    /// No implicit merge; an explicit reviewed action is required.
    ManualReview,
    /// A separately reviewed fast-forward action may be proposed.
    ReviewedFastForward,
    /// A separately reviewed three-way merge action may be proposed.
    ReviewedThreeWay,
}

/// Source of the exact child set consumed by a wait node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ChildSetSource {
    /// Exact child session IDs retained by the compiled graph.
    Exact {
        /// Stable, deduplicated child identities.
        child_ids: BTreeSet<String>,
    },
    /// Canonical list-of-child-ID variable.
    Variable {
        /// Exact authorized variable name.
        variable: String,
    },
}

/// Parent cancellation behavior while waiting for children.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWaitCancellation {
    /// Propose cancellation for every incomplete child.
    Cascade,
    /// Leave children independently active and stop waiting.
    Detach,
    /// Continue waiting for terminal child state without later effects.
    Wait,
}

/// Bounded schema for one reviewer result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewResultSchema {
    /// Maximum structured findings.
    pub maximum_findings: u32,
    /// Maximum UTF-8 bytes per finding.
    pub maximum_finding_bytes: u32,
    /// Maximum rejected task or child identities.
    pub maximum_rejections: u32,
    /// Whether every finding must carry immutable artifact evidence.
    pub require_artifact_evidence: bool,
}

/// Exact graph destinations for reviewer routing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewRoutes {
    /// Destination when the reviewer accepts the integration.
    pub approved: String,
    /// Destination when bounded revision remains available.
    pub revision: String,
    /// Structured failure destination.
    pub failure: String,
}

/// Bounded artifact content source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ArtifactContentSource {
    /// Canonical JSON bytes.
    StaticJson {
        /// Exact bounded value.
        value: serde_json::Value,
    },
    /// Exact bounded UTF-8 bytes.
    StaticText {
        /// Exact text.
        value: String,
    },
    /// Value read from one declared canonical variable.
    Variable {
        /// Exact variable name.
        variable: String,
    },
    /// Visible UTF-8 response bytes recovered from one canonical provider-result
    /// receipt. The referenced variable remains an opaque node-result handle;
    /// runtime logic validates the handle against durable provider evidence
    /// before projecting its bounded visible text.
    ProviderResultText {
        /// Declared node-result-reference variable produced by a model or
        /// provider-tool-batch node.
        reference_variable: String,
    },
    /// Bounded canonical projection of one node-result receipt.
    NodeResultProjection {
        /// Declared node-result-reference variable.
        reference_variable: String,
    },
}

/// Artifact security handling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactSecurityClassification {
    /// Ordinary workspace content.
    Standard,
    /// User-private content.
    Private,
}

/// Artifact retention selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRetentionPolicy {
    /// Retain until explicit removal.
    Permanent,
    /// Retain with the session.
    Session,
}

/// Child identity source for a message node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ChildSelector {
    /// Exact child identity.
    Exact {
        /// Canonical child ID.
        child_id: String,
    },
    /// Child identity read from a declared canonical variable.
    Variable {
        /// Variable name.
        variable: String,
    },
}

/// Message behavior after cancellation starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildMessageCancellation {
    /// Reject delivery after cancellation begins.
    Reject,
    /// Deliver only if the child is still running.
    DeliverIfRunning,
}

/// Join member failure behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinFailurePolicy {
    /// Fail immediately when the success threshold becomes impossible.
    FailFast,
    /// Wait for every required member before evaluating.
    WaitRequired,
    /// Continue when the minimum-success threshold is met.
    MinimumSuccess,
}

/// Deterministic join output order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinOrderingPolicy {
    /// Sort by stable member identity.
    MemberId,
    /// Use canonical completion sequence.
    CompletionSequence,
}

/// Join result projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinResultProjection {
    /// Bounded inline canonical values.
    Inline,
    /// Node-result references.
    NodeReferences,
    /// Artifact references only.
    ArtifactReferences,
}

/// Artifact collection at a join.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinArtifactCollection {
    /// Do not collect artifacts.
    None,
    /// Collect explicitly declared references.
    Declared,
    /// Collect every result artifact reference.
    All,
}

/// Parallel readiness policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelJoinPolicy {
    /// Require all branches.
    All,
    /// Use the join node's minimum-success threshold.
    MinimumSuccess,
}

/// Explicit shared-resource serialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelSerializationPolicy {
    /// Dispatch and commit conflicting branches by stable branch ID.
    StableBranchOrder,
}

/// Durable delay resolution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum DelayResolution {
    /// Relative duration resolved by the runtime once and recorded.
    Duration {
        /// Positive duration in milliseconds.
        duration_ms: u64,
    },
    /// Exact RFC 3339 wake timestamp.
    WakeTimestamp {
        /// Timestamp text retained exactly.
        timestamp: String,
    },
}

/// Durable delay cancellation policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelayCancellation {
    /// Cancel the durable continuation.
    CancelContinuation,
    /// Leave the continuation pending but suppress later effects.
    SuppressEffects,
}

/// Scheduler trigger type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ScheduleTrigger {
    /// One-time wake.
    At {
        /// Exact RFC 3339 timestamp.
        timestamp: String,
    },
    /// Recurring interval.
    Interval {
        /// Positive interval milliseconds.
        interval_ms: u64,
        /// Optional exact initial timestamp.
        #[serde(default)]
        start_timestamp: Option<String>,
    },
    /// Declared runtime user-space event.
    RuntimeEvent {
        /// Event type.
        event_type: String,
    },
    /// Bounded process-output match.
    ProcessOutput {
        /// Canonical process reference.
        process_reference: String,
        /// Bounded literal match text.
        pattern: String,
    },
}

/// Schedule cancellation behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleCancellation {
    /// Cancel trigger registration.
    CancelTrigger,
    /// Preserve recurring registration while cancelling this wait.
    CancelWaitOnly,
}

/// Generic graph node kinds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// Transform provider-visible context.
    ContextTransform,
    /// Request model execution.
    ModelCall,
    /// Gate and request a tool operation.
    ToolExecutionGate,
    /// Wait for user approval.
    UserApproval,
    /// Create a child session.
    SpawnChildAgent,
    /// Send a child-session message.
    SendChildAgentMessage,
    /// Wait for child sessions.
    WaitForAgents,
    /// Join child-session results.
    JoinResults,
    /// Review structured work.
    Review,
    /// Statically bounded loop control.
    Loop,
    /// Conditional branch.
    ConditionalBranch,
    /// Parallel branch.
    ParallelBranch,
    /// Delay execution.
    Delay,
    /// Create or wait for a schedule.
    Schedule,
    /// Emit a typed runtime event.
    EmitEvent,
    /// Persist an immutable artifact.
    PersistArtifact,
    /// Complete the current turn.
    CompleteTurn,
    /// Complete the session.
    CompleteSession,
    /// Fail with a structured reason.
    Fail,
}

impl NodeKind {
    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::CompleteTurn | Self::CompleteSession | Self::Fail
        )
    }

    const fn implied_capability(self) -> Option<&'static str> {
        match self {
            Self::ContextTransform => Some("context"),
            Self::ModelCall | Self::Review => Some("model"),
            Self::ToolExecutionGate => Some("tools"),
            Self::UserApproval => Some("approval"),
            Self::SpawnChildAgent
            | Self::SendChildAgentMessage
            | Self::WaitForAgents
            | Self::JoinResults => Some("agents"),
            Self::Delay | Self::Schedule => Some("scheduling"),
            Self::EmitEvent => Some("events"),
            Self::PersistArtifact => Some("artifacts"),
            Self::Loop
            | Self::ConditionalBranch
            | Self::ParallelBranch
            | Self::CompleteTurn
            | Self::CompleteSession
            | Self::Fail => None,
        }
    }
}

/// Directed graph transition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeDefinition {
    /// Source node ID.
    pub from: String,
    /// Destination node ID.
    pub to: String,
    /// Optional constrained transition condition.
    #[serde(default)]
    pub condition: Option<String>,
    /// Optional stable inspection label.
    #[serde(default)]
    pub label: Option<String>,
}

/// Deterministic executable graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableGraph {
    /// Source format version.
    #[serde(deserialize_with = "deserialize_supported_format_version")]
    pub format_version: u16,
    /// Index of the entry node.
    pub entry_index: usize,
    /// Validated hard budgets.
    pub budget: GraphBudget,
    /// Sorted declarations.
    pub declarations: GraphDeclarations,
    /// Variable declarations sorted by name.
    #[serde(default)]
    pub variables: Vec<VariableDeclaration>,
    /// Nodes sorted by ID.
    pub nodes: Vec<ExecutableNode>,
    /// Edges sorted by source, destination, and label.
    pub edges: Vec<ExecutableEdge>,
    /// Complete deterministic cache identity.
    pub cache_key: GraphCacheKey,
}

fn deserialize_supported_format_version<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let format_version = u16::deserialize(deserializer)?;
    if format_version != GRAPH_FORMAT_VERSION {
        return Err(serde::de::Error::custom(format!(
            "unsupported graph format version {format_version}; supported version is {GRAPH_FORMAT_VERSION}"
        )));
    }
    Ok(format_version)
}

impl ExecutableGraph {
    /// Returns deterministic JSON suitable for graph inspection and golden tests.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError`] only if the fully owned executable representation
    /// cannot be serialized.
    pub fn inspect_json(&self) -> Result<String, GraphError> {
        serde_json::to_string_pretty(self).map_err(|error| GraphError::Inspection {
            detail: error.to_string(),
        })
    }
}

/// Compiled generic node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableNode {
    /// Deterministic node index.
    pub index: usize,
    /// Stable source ID.
    pub id: String,
    /// Generic kind.
    pub kind: NodeKind,
    /// Exact validated kind-specific configuration.
    #[serde(default)]
    pub configuration: Option<NodeConfiguration>,
    /// Parsed constrained condition.
    pub condition: Option<Expression>,
    /// Declared tool.
    pub tool: Option<String>,
    /// Declared provider.
    pub provider: Option<String>,
    /// Required capabilities.
    pub required_capabilities: BTreeSet<String>,
    /// Read scopes.
    pub read_scopes: BTreeSet<String>,
    /// Proposed write scopes.
    pub write_scopes: BTreeSet<String>,
    /// Declared canonical variable reads.
    #[serde(default)]
    pub read_variables: BTreeSet<String>,
    /// Declared canonical variable writes.
    #[serde(default)]
    pub write_variables: BTreeSet<String>,
    /// Retry limit.
    pub retry_limit: u32,
    /// Static loop bound.
    pub max_iterations: Option<u32>,
}

/// Compiled directed transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableEdge {
    /// Source node index.
    pub from: usize,
    /// Destination node index.
    pub to: usize,
    /// Parsed constrained condition.
    pub condition: Option<Expression>,
    /// Optional stable label.
    pub label: Option<String>,
}

/// Cache identity with inspectable constituent hashes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphCacheKey {
    /// Exact graph source hash.
    pub graph_content_hash: ContentHash,
    /// Validated plugin-set hash.
    pub plugin_set_hash: ContentHash,
    /// Sorted runtime capability-set hash.
    pub capability_set_hash: ContentHash,
    /// Runtime API version hash.
    pub runtime_api_hash: ContentHash,
    /// Hash binding all constituents.
    pub combined_hash: ContentHash,
}

/// Parses, validates, and deterministically compiles a graph.
///
/// # Errors
///
/// Returns [`GraphError`] for malformed source, structural invalidity, missing
/// declarations, unsafe parallel writes, unbounded cycles, or exceeded limits.
pub fn compile(
    source: &str,
    cache_inputs: &GraphCacheInputs,
    limits: CompilerLimits,
) -> Result<ExecutableGraph, GraphError> {
    let definition = GraphDefinition::parse(source, limits)?;
    validate_version_and_bounds(&definition, limits)?;
    validate_names(&definition, limits)?;

    let node_map = collect_nodes(&definition)?;
    let variable_map = collect_variables(&definition)?;
    validate_edges(&definition, &node_map, limits)?;
    validate_entry_and_reachability(&definition, &node_map)?;
    validate_termination(&definition, &node_map)?;
    validate_node_contracts(&definition, &node_map, cache_inputs, limits)?;
    validate_variables(&definition, &node_map, &variable_map, limits)?;
    validate_effect_output_writes(&node_map, &variable_map)?;
    validate_cycles(&definition, &node_map)?;
    validate_parallel_writes(&definition, &node_map, &variable_map, limits)?;

    let mut sorted_nodes: Vec<_> = definition.nodes.iter().collect();
    sorted_nodes.sort_by(|left, right| left.id.cmp(&right.id));
    let indices: BTreeMap<_, _> = sorted_nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.as_str(), index))
        .collect();

    let mut nodes = Vec::with_capacity(sorted_nodes.len());
    for (index, node) in sorted_nodes.into_iter().enumerate() {
        let mut configuration = effective_node_configuration(node);
        if let Some(configuration) = &mut configuration {
            canonicalize_node_configuration(configuration);
        }
        nodes.push(ExecutableNode {
            index,
            id: node.id.clone(),
            kind: node.kind,
            configuration,
            condition: parse_condition(node.condition.as_deref(), &node.id, limits.expression)?,
            tool: node.tool.clone(),
            provider: node.provider.clone(),
            required_capabilities: node.required_capabilities.clone(),
            read_scopes: node.read_scopes.clone(),
            write_scopes: node.write_scopes.clone(),
            read_variables: node.read_variables.clone(),
            write_variables: node.write_variables.clone(),
            retry_limit: node.retry_limit,
            max_iterations: node.max_iterations,
        });
    }

    let mut sorted_edges: Vec<_> = definition.edges.iter().collect();
    sorted_edges.sort_by(|left, right| {
        left.from
            .cmp(&right.from)
            .then_with(|| left.to.cmp(&right.to))
            .then_with(|| left.label.cmp(&right.label))
            .then_with(|| left.condition.cmp(&right.condition))
    });
    let mut edges = Vec::with_capacity(sorted_edges.len());
    for edge in sorted_edges {
        edges.push(ExecutableEdge {
            from: indices[edge.from.as_str()],
            to: indices[edge.to.as_str()],
            condition: parse_condition(
                edge.condition.as_deref(),
                &format!("{} -> {}", edge.from, edge.to),
                limits.expression,
            )?,
            label: edge.label.clone(),
        });
    }

    let mut variables = definition.variables;
    variables.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(ExecutableGraph {
        format_version: definition.format_version,
        entry_index: indices[definition.entry.as_str()],
        budget: definition.budget,
        declarations: definition.declarations,
        variables,
        nodes,
        edges,
        cache_key: build_cache_key(source, cache_inputs),
    })
}

fn validate_version_and_bounds(
    definition: &GraphDefinition,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    if definition.format_version != GRAPH_FORMAT_VERSION {
        return Err(GraphError::UnsupportedVersion {
            actual: definition.format_version,
            supported: GRAPH_FORMAT_VERSION,
        });
    }
    if definition.nodes.len() > limits.max_nodes {
        return Err(GraphError::TooManyNodes {
            actual: definition.nodes.len(),
            maximum: limits.max_nodes,
        });
    }
    if definition.edges.len() > limits.max_edges {
        return Err(GraphError::TooManyEdges {
            actual: definition.edges.len(),
            maximum: limits.max_edges,
        });
    }
    for (name, actual, maximum) in [
        ("max_steps", definition.budget.max_steps, limits.max_steps),
        (
            "max_tokens",
            definition.budget.max_tokens,
            limits.max_tokens,
        ),
        (
            "max_cost_micros",
            definition.budget.max_cost_micros,
            limits.max_cost_micros,
        ),
        (
            "max_duration_ms",
            definition.budget.max_duration_ms,
            limits.max_duration_ms,
        ),
    ] {
        if actual == 0 || actual > maximum {
            return Err(GraphError::InvalidBudget {
                name,
                actual,
                maximum,
            });
        }
    }
    Ok(())
}

fn validate_names(definition: &GraphDefinition, limits: CompilerLimits) -> Result<(), GraphError> {
    validate_name("entry", &definition.entry, limits.max_name_bytes)?;
    for node in &definition.nodes {
        validate_name("node", &node.id, limits.max_name_bytes)?;
        for value in node
            .required_capabilities
            .iter()
            .chain(&node.read_scopes)
            .chain(&node.write_scopes)
            .chain(&node.read_variables)
            .chain(&node.write_variables)
        {
            validate_name("node declaration", value, limits.max_name_bytes)?;
        }
    }
    for value in definition
        .declarations
        .capabilities
        .iter()
        .chain(&definition.declarations.tools)
        .chain(&definition.declarations.providers)
        .chain(&definition.declarations.events)
        .chain(&definition.declarations.plugins)
    {
        validate_name("graph declaration", value, limits.max_name_bytes)?;
    }
    Ok(())
}

fn validate_name(kind: &'static str, value: &str, maximum: usize) -> Result<(), GraphError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.:/-".contains(character))
    {
        Err(GraphError::InvalidName {
            kind,
            value: value.to_owned(),
            maximum,
        })
    } else {
        Ok(())
    }
}

fn collect_nodes(
    definition: &GraphDefinition,
) -> Result<BTreeMap<&str, &NodeDefinition>, GraphError> {
    let mut nodes = BTreeMap::new();
    for node in &definition.nodes {
        if nodes.insert(node.id.as_str(), node).is_some() {
            return Err(GraphError::DuplicateNode {
                node: node.id.clone(),
            });
        }
    }
    Ok(nodes)
}

fn collect_variables(
    definition: &GraphDefinition,
) -> Result<BTreeMap<&str, &VariableDeclaration>, GraphError> {
    let mut variables = BTreeMap::new();
    for variable in &definition.variables {
        if variables.insert(variable.name.as_str(), variable).is_some() {
            return Err(GraphError::DuplicateVariable {
                variable: variable.name.clone(),
            });
        }
    }
    Ok(variables)
}

#[allow(clippy::too_many_lines)] // The declaration and access invariants are checked together.
fn validate_variables(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    variables: &BTreeMap<&str, &VariableDeclaration>,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    if definition.variables.len() > limits.max_variables {
        return Err(GraphError::TooManyVariables {
            actual: definition.variables.len(),
            maximum: limits.max_variables,
        });
    }

    for variable in &definition.variables {
        validate_name("variable", &variable.name, limits.max_name_bytes)?;
        if variable.producer != "runtime" {
            validate_name(
                "variable producer",
                &variable.producer,
                limits.max_name_bytes,
            )?;
        }
        if variable.max_size_bytes == 0
            || variable.max_size_bytes > limits.max_configuration_bytes as u64
        {
            return Err(GraphError::InvalidVariableSize {
                variable: variable.name.clone(),
                actual: variable.max_size_bytes,
                maximum: limits.max_configuration_bytes as u64,
            });
        }
        if variable.merge_contributors.len() > limits.max_configuration_items {
            return Err(GraphError::TooManyVariableMergeContributors {
                variable: variable.name.clone(),
                actual: variable.merge_contributors.len(),
                maximum: limits.max_configuration_items,
            });
        }
        validate_variable_type(&variable.name, &variable.value_type, limits, 1)?;
        validate_variable_security(variable)?;
        validate_variable_merge_policy(variable)?;
        if variable.producer != "runtime" && !nodes.contains_key(variable.producer.as_str()) {
            return Err(GraphError::UnknownVariableProducer {
                variable: variable.name.clone(),
                producer: variable.producer.clone(),
            });
        }
        match nodes.get(variable.producer.as_str()) {
            Some(producer) if !producer.write_variables.contains(variable.name.as_str()) => {
                return Err(GraphError::VariableProducerDoesNotWrite {
                    variable: variable.name.clone(),
                    producer: variable.producer.clone(),
                });
            }
            Some(_) | None => {}
        }
        if !variable.merge_contributors.is_empty() {
            if variable.producer == "runtime" {
                return Err(GraphError::InvalidVariableMergeContributors {
                    variable: variable.name.clone(),
                    detail: "parallel merge contributors require one graph-node producer"
                        .to_owned(),
                });
            }
            if variable.mutability != VariableMutability::Mutable {
                return Err(GraphError::InvalidVariableMergeContributors {
                    variable: variable.name.clone(),
                    detail: "parallel merge contributors require a mutable variable".to_owned(),
                });
            }
            if !matches!(variable.scope, VariableScope::Run | VariableScope::Session) {
                return Err(GraphError::InvalidVariableMergeContributors {
                    variable: variable.name.clone(),
                    detail: "parallel merge contributors require run or session variable scope"
                        .to_owned(),
                });
            }
            if variable.merge_policy.is_none() {
                return Err(GraphError::InvalidVariableMergeContributors {
                    variable: variable.name.clone(),
                    detail: "parallel merge contributors require a declared merge policy"
                        .to_owned(),
                });
            }
        }
        for contributor in &variable.merge_contributors {
            validate_name(
                "variable merge contributor",
                contributor,
                limits.max_name_bytes,
            )?;
            if contributor == &variable.producer {
                return Err(GraphError::InvalidVariableMergeContributors {
                    variable: variable.name.clone(),
                    detail: "the singular producer cannot also be an additional merge contributor"
                        .to_owned(),
                });
            }
            let Some(node) = nodes.get(contributor.as_str()) else {
                return Err(GraphError::UnknownVariableMergeContributor {
                    variable: variable.name.clone(),
                    contributor: contributor.clone(),
                });
            };
            if !node.write_variables.contains(variable.name.as_str()) {
                return Err(GraphError::VariableMergeContributorDoesNotWrite {
                    variable: variable.name.clone(),
                    contributor: contributor.clone(),
                });
            }
        }
        for consumer in &variable.consumers {
            validate_name("variable consumer", consumer, limits.max_name_bytes)?;
            if !nodes.contains_key(consumer.as_str()) {
                return Err(GraphError::UnknownVariableConsumer {
                    variable: variable.name.clone(),
                    consumer: consumer.clone(),
                });
            }
        }
        if matches!(variable.scope, VariableScope::Branch) && variable.merge_policy.is_some() {
            return Err(GraphError::InvalidVariableMergePolicy {
                variable: variable.name.clone(),
                detail: "branch-scoped values cannot declare a shared merge policy".to_owned(),
            });
        }
        validate_variable_scope(definition, nodes, variable)?;
    }

    let strict_variable_access = !definition.variables.is_empty()
        || definition
            .nodes
            .iter()
            .any(|node| !node.read_variables.is_empty() || !node.write_variables.is_empty());
    for node in &definition.nodes {
        for variable in &node.read_variables {
            let declaration = variables.get(variable.as_str()).ok_or_else(|| {
                GraphError::UndeclaredVariableRead {
                    node: node.id.clone(),
                    variable: variable.clone(),
                }
            })?;
            if !declaration.consumers.contains(node.id.as_str()) {
                return Err(GraphError::UnauthorizedVariableConsumer {
                    node: node.id.clone(),
                    variable: variable.clone(),
                });
            }
        }
        for variable in &node.write_variables {
            let Some(declaration) = variables.get(variable.as_str()) else {
                return Err(GraphError::UndeclaredVariableWrite {
                    node: node.id.clone(),
                    variable: variable.clone(),
                });
            };
            if declaration.producer != node.id
                && !declaration.merge_contributors.contains(node.id.as_str())
            {
                return Err(GraphError::UnauthorizedVariableWriter {
                    node: node.id.clone(),
                    variable: variable.clone(),
                    producer: declaration.producer.clone(),
                });
            }
        }
        validate_configuration_variable_access(node, variables)?;
        validate_expression_variable_access(
            node.condition.as_deref(),
            &node.id,
            &node.read_variables,
            variables,
            limits.expression,
            strict_variable_access,
        )?;
    }
    for edge in &definition.edges {
        let source = nodes
            .get(edge.from.as_str())
            .expect("edges were validated before variable access");
        validate_expression_variable_access(
            edge.condition.as_deref(),
            &format!("{} -> {}", edge.from, edge.to),
            &source.read_variables,
            variables,
            limits.expression,
            strict_variable_access,
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EffectOutputSlot {
    ToolResult,
    ApprovalResult,
    ArtifactReference,
    ChildId,
    ChildIds,
    NodeResult,
    Timestamp,
    Duration,
}

impl EffectOutputSlot {
    const fn name(self) -> &'static str {
        match self {
            Self::ToolResult => "tool_result_reference",
            Self::ApprovalResult => "approval_result",
            Self::ArtifactReference => "artifact_reference",
            Self::ChildId => "child_id",
            Self::ChildIds => "child_ids",
            Self::NodeResult => "node_result_reference",
            Self::Timestamp => "timestamp",
            Self::Duration => "duration",
        }
    }
}

fn validate_effect_output_writes(
    nodes: &BTreeMap<&str, &NodeDefinition>,
    variables: &BTreeMap<&str, &VariableDeclaration>,
) -> Result<(), GraphError> {
    for node in nodes.values() {
        if node.write_variables.is_empty() {
            continue;
        }
        let mut consumers = BTreeMap::<EffectOutputSlot, Vec<String>>::new();
        for variable in &node.write_variables {
            let declaration = variables
                .get(variable.as_str())
                .expect("declared writes were validated before effect outputs");
            let Some(slot) = effect_output_slot(&declaration.value_type).map_err(|detail| {
                GraphError::InvalidEffectOutputType {
                    node: node.id.clone(),
                    variable: variable.clone(),
                    detail,
                }
            })?
            else {
                continue;
            };
            if !native_effect_slot_available(node.kind, slot) {
                return Err(GraphError::EffectOutputSlotUnavailable {
                    node: node.id.clone(),
                    variable: variable.clone(),
                    slot: slot.name(),
                });
            }
            consumers.entry(slot).or_default().push(variable.clone());
        }
        for (slot, mut variables) in consumers.clone() {
            if variables.len() > 1 {
                variables.sort();
                return Err(GraphError::DuplicateEffectOutputSlot {
                    node: node.id.clone(),
                    slot: slot.name(),
                    variables,
                });
            }
        }
        if consumers.contains_key(&EffectOutputSlot::ChildId)
            && consumers.contains_key(&EffectOutputSlot::ChildIds)
        {
            let mut variables = consumers.into_values().flatten().collect::<Vec<_>>();
            variables.sort();
            return Err(GraphError::AmbiguousChildEffectOutput {
                node: node.id.clone(),
                variables,
            });
        }
    }
    // Graphs without declared writes never enter this validation loop and
    // retain the legacy schema-free output contract.
    Ok(())
}

fn effect_output_slot(
    value_type: &VariableValueType,
) -> Result<Option<EffectOutputSlot>, &'static str> {
    match value_type {
        VariableValueType::Boolean
        | VariableValueType::Integer
        | VariableValueType::Decimal
        | VariableValueType::String
        | VariableValueType::Enum { .. } => Ok(None),
        VariableValueType::List { item_type, .. } if **item_type == VariableValueType::ChildId => {
            Ok(Some(EffectOutputSlot::ChildIds))
        }
        VariableValueType::List { item_type, .. }
        | VariableValueType::Map {
            value_type: item_type,
            ..
        } if ordinary_effect_output_type(item_type) => Ok(None),
        VariableValueType::ArtifactReference => Ok(Some(EffectOutputSlot::ArtifactReference)),
        VariableValueType::ChildId => Ok(Some(EffectOutputSlot::ChildId)),
        VariableValueType::ToolResultReference => Ok(Some(EffectOutputSlot::ToolResult)),
        VariableValueType::ApprovalResult => Ok(Some(EffectOutputSlot::ApprovalResult)),
        VariableValueType::NodeResultReference => Ok(Some(EffectOutputSlot::NodeResult)),
        VariableValueType::Timestamp => Ok(Some(EffectOutputSlot::Timestamp)),
        VariableValueType::Duration => Ok(Some(EffectOutputSlot::Duration)),
        VariableValueType::SessionId
        | VariableValueType::TaskId
        | VariableValueType::SecretReference => {
            Err("session/task handles and secret references cannot be effect outputs")
        }
        VariableValueType::List { .. } | VariableValueType::Map { .. } => {
            Err("ordinary effect-output collections cannot contain runtime-owned slot types")
        }
    }
}

fn ordinary_effect_output_type(value_type: &VariableValueType) -> bool {
    match value_type {
        VariableValueType::Boolean
        | VariableValueType::Integer
        | VariableValueType::Decimal
        | VariableValueType::String
        | VariableValueType::Enum { .. } => true,
        VariableValueType::List { item_type, .. } => ordinary_effect_output_type(item_type),
        VariableValueType::Map { value_type, .. } => ordinary_effect_output_type(value_type),
        VariableValueType::SessionId
        | VariableValueType::ChildId
        | VariableValueType::TaskId
        | VariableValueType::ArtifactReference
        | VariableValueType::SecretReference
        | VariableValueType::ToolResultReference
        | VariableValueType::ApprovalResult
        | VariableValueType::NodeResultReference
        | VariableValueType::Timestamp
        | VariableValueType::Duration => false,
    }
}

const fn native_effect_slot_available(kind: NodeKind, slot: EffectOutputSlot) -> bool {
    match slot {
        EffectOutputSlot::ToolResult => matches!(kind, NodeKind::ToolExecutionGate),
        EffectOutputSlot::ApprovalResult => matches!(kind, NodeKind::UserApproval),
        EffectOutputSlot::ArtifactReference => matches!(kind, NodeKind::PersistArtifact),
        EffectOutputSlot::ChildId | EffectOutputSlot::ChildIds => {
            matches!(kind, NodeKind::SpawnChildAgent)
        }
        EffectOutputSlot::NodeResult | EffectOutputSlot::Timestamp | EffectOutputSlot::Duration => {
            kind.implied_capability().is_some()
        }
    }
}

fn validate_variable_type(
    variable: &str,
    value_type: &VariableValueType,
    limits: CompilerLimits,
    depth: usize,
) -> Result<(), GraphError> {
    if depth > limits.max_value_depth {
        return Err(GraphError::VariableTypeTooDeep {
            variable: variable.to_owned(),
            maximum: limits.max_value_depth,
        });
    }
    match value_type {
        VariableValueType::Enum { values } => {
            if values.is_empty() || values.len() > limits.max_configuration_items {
                return Err(GraphError::InvalidVariableType {
                    variable: variable.to_owned(),
                    detail: "enum values must be non-empty and within the collection bound"
                        .to_owned(),
                });
            }
            for value in values {
                validate_name("enum value", value, limits.max_name_bytes)?;
            }
        }
        VariableValueType::List {
            item_type,
            max_items,
        } => {
            if *max_items == 0 || *max_items as usize > limits.max_configuration_items {
                return Err(GraphError::InvalidVariableType {
                    variable: variable.to_owned(),
                    detail: "list max_items must be within the collection bound".to_owned(),
                });
            }
            validate_variable_type(variable, item_type, limits, depth + 1)?;
        }
        VariableValueType::Map {
            value_type,
            max_entries,
        } => {
            if *max_entries == 0 || *max_entries as usize > limits.max_configuration_items {
                return Err(GraphError::InvalidVariableType {
                    variable: variable.to_owned(),
                    detail: "map max_entries must be within the collection bound".to_owned(),
                });
            }
            validate_variable_type(variable, value_type, limits, depth + 1)?;
        }
        VariableValueType::Boolean
        | VariableValueType::Integer
        | VariableValueType::Decimal
        | VariableValueType::String
        | VariableValueType::SessionId
        | VariableValueType::ChildId
        | VariableValueType::TaskId
        | VariableValueType::ArtifactReference
        | VariableValueType::SecretReference
        | VariableValueType::ToolResultReference
        | VariableValueType::ApprovalResult
        | VariableValueType::NodeResultReference
        | VariableValueType::Timestamp
        | VariableValueType::Duration => {}
    }
    Ok(())
}

fn validate_variable_security(variable: &VariableDeclaration) -> Result<(), GraphError> {
    let is_secret_reference = matches!(&variable.value_type, VariableValueType::SecretReference);
    let is_secret_classification = matches!(
        variable.security_classification,
        SecurityClassification::SecretReference
    );
    if is_secret_reference == is_secret_classification {
        Ok(())
    } else {
        Err(GraphError::InvalidVariableSecurityClassification {
            variable: variable.name.clone(),
        })
    }
}

fn validate_variable_merge_policy(variable: &VariableDeclaration) -> Result<(), GraphError> {
    let Some(policy) = variable.merge_policy else {
        return Ok(());
    };
    if variable.mutability != VariableMutability::Mutable {
        return Err(GraphError::InvalidVariableMergePolicy {
            variable: variable.name.clone(),
            detail: "a merged variable must be mutable".to_owned(),
        });
    }
    let compatible = match policy {
        VariableMergePolicy::Append | VariableMergePolicy::Union => {
            matches!(&variable.value_type, VariableValueType::List { .. })
        }
        VariableMergePolicy::DeepMerge => {
            matches!(&variable.value_type, VariableValueType::Map { .. })
        }
        VariableMergePolicy::FirstBranch => true,
    };
    if compatible {
        Ok(())
    } else {
        Err(GraphError::InvalidVariableMergePolicy {
            variable: variable.name.clone(),
            detail: format!(
                "merge policy `{policy:?}` is incompatible with the declared value type"
            ),
        })
    }
}

fn validate_variable_scope(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    variable: &VariableDeclaration,
) -> Result<(), GraphError> {
    match variable.scope {
        VariableScope::Run | VariableScope::Session => Ok(()),
        VariableScope::Node => {
            let valid = if variable.producer == "runtime" {
                variable.consumers.len() == 1
            } else {
                variable
                    .consumers
                    .iter()
                    .all(|consumer| consumer == &variable.producer)
            };
            if valid {
                Ok(())
            } else {
                Err(GraphError::InvalidVariableScope {
                    variable: variable.name.clone(),
                    detail: "node-scoped values may be consumed only by their producing node"
                        .to_owned(),
                })
            }
        }
        VariableScope::Branch => {
            if variable.producer == "runtime" {
                return Err(GraphError::InvalidVariableScope {
                    variable: variable.name.clone(),
                    detail: "branch-scoped values require a graph-node producer".to_owned(),
                });
            }
            let graph = adjacency(definition);
            let mut regions = Vec::new();
            for parallel in nodes
                .values()
                .filter(|node| node.kind == NodeKind::ParallelBranch)
            {
                let branches = graph.get(parallel.id.as_str()).cloned().unwrap_or_default();
                let Some(join) = common_join(&branches, &graph, nodes) else {
                    continue;
                };
                for branch in branches {
                    let members = branch_members(branch, join, &graph);
                    if members.contains(variable.producer.as_str()) {
                        regions.push(members);
                    }
                }
            }
            regions.sort_by_key(BTreeSet::len);
            if regions.first().is_some_and(|region| {
                variable
                    .consumers
                    .iter()
                    .all(|consumer| region.contains(consumer.as_str()))
            }) {
                Ok(())
            } else {
                Err(GraphError::InvalidVariableScope {
                    variable: variable.name.clone(),
                    detail: "branch-scoped values cannot escape their innermost parallel branch"
                        .to_owned(),
                })
            }
        }
    }
}

fn branch_members<'a>(
    branch: &'a str,
    join: &str,
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
) -> BTreeSet<&'a str> {
    let mut members = BTreeSet::new();
    let mut queue = VecDeque::from([branch]);
    while let Some(node) = queue.pop_front() {
        if node == join || !members.insert(node) {
            continue;
        }
        if let Some(targets) = graph.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    members
}

fn validate_expression_variable_access(
    source: Option<&str>,
    owner: &str,
    declared_reads: &BTreeSet<String>,
    variables: &BTreeMap<&str, &VariableDeclaration>,
    limits: ExpressionLimits,
    strict_variable_access: bool,
) -> Result<(), GraphError> {
    let Some(expression) = parse_condition(source, owner, limits)? else {
        return Ok(());
    };
    if !strict_variable_access {
        // Graph format v1 originally exposed provider and runtime projections
        // directly to conditions. A graph opts into canonical variable access
        // by declaring variables or node variable reads/writes; only then do
        // undeclared roots become a compile-time rejection.
        return Ok(());
    }
    let mut roots = BTreeSet::new();
    collect_expression_roots(&expression, &mut roots);
    for root in roots {
        if is_runtime_condition_root(&root) {
            continue;
        }
        if !variables.contains_key(root.as_str()) {
            return Err(GraphError::UndeclaredConditionVariable {
                owner: owner.to_owned(),
                variable: root,
            });
        }
        if !declared_reads.contains(root.as_str()) {
            return Err(GraphError::ConditionVariableNotDeclaredRead {
                owner: owner.to_owned(),
                variable: root,
            });
        }
    }
    Ok(())
}

fn collect_expression_roots(expression: &Expression, roots: &mut BTreeSet<String>) {
    match expression {
        Expression::Value(operand) => collect_operand_root(operand, roots),
        Expression::Not(inner) => collect_expression_roots(inner, roots),
        Expression::And(left, right) | Expression::Or(left, right) => {
            collect_expression_roots(left, roots);
            collect_expression_roots(right, roots);
        }
        Expression::Compare { left, right, .. } => {
            collect_operand_root(left, roots);
            collect_operand_root(right, roots);
        }
        Expression::Exists(path) => collect_path_root(path.segments(), roots),
    }
}

fn collect_operand_root(operand: &Operand, roots: &mut BTreeSet<String>) {
    if let Operand::Path(path) = operand {
        collect_path_root(path.segments(), roots);
    }
}

fn collect_path_root(path: &[PathSegment], roots: &mut BTreeSet<String>) {
    if let Some(PathSegment::Key(root)) = path.first() {
        roots.insert(root.clone());
    }
}

fn is_runtime_condition_root(root: &str) -> bool {
    matches!(
        root,
        "session" | "model" | "runtime" | "budget" | "node" | "loop"
    )
}

fn validate_configuration_variable_access(
    node: &NodeDefinition,
    variables: &BTreeMap<&str, &VariableDeclaration>,
) -> Result<(), GraphError> {
    let variable = match &node.configuration {
        Some(NodeConfiguration::SendChildAgentMessage {
            child: ChildSelector::Variable { variable },
            ..
        }) => Some((
            variable.as_str(),
            Some(VariableValueTypeDiscriminant::ChildId),
        )),
        _ => None,
    };
    let Some((variable, expected_type)) = variable else {
        return Ok(());
    };
    let declaration =
        variables
            .get(variable)
            .ok_or_else(|| GraphError::UndeclaredVariableRead {
                node: node.id.clone(),
                variable: variable.to_owned(),
            })?;
    if !node.read_variables.contains(variable) {
        return Err(GraphError::ConfigurationVariableNotDeclaredRead {
            node: node.id.clone(),
            variable: variable.to_owned(),
        });
    }
    if !declaration.consumers.contains(node.id.as_str()) {
        return Err(GraphError::UnauthorizedVariableConsumer {
            node: node.id.clone(),
            variable: variable.to_owned(),
        });
    }
    if expected_type.is_some_and(|expected_type| {
        VariableValueTypeDiscriminant::of(&declaration.value_type) != Some(expected_type)
    }) {
        let expected_type = expected_type.expect("is_some_and confirmed expected type");
        return Err(GraphError::ConfigurationVariableTypeMismatch {
            node: node.id.clone(),
            variable: variable.to_owned(),
            expected: expected_type.name(),
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum VariableValueTypeDiscriminant {
    ChildId,
}

impl VariableValueTypeDiscriminant {
    const fn of(value_type: &VariableValueType) -> Option<Self> {
        match value_type {
            VariableValueType::ChildId => Some(Self::ChildId),
            _ => None,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ChildId => "child_id",
        }
    }
}

fn validate_edges(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let mut seen = BTreeSet::new();
    for edge in &definition.edges {
        validate_name("edge source", &edge.from, limits.max_name_bytes)?;
        validate_name("edge destination", &edge.to, limits.max_name_bytes)?;
        if !nodes.contains_key(edge.from.as_str()) {
            return Err(GraphError::UnknownEdgeNode {
                edge: format!("{} -> {}", edge.from, edge.to),
                node: edge.from.clone(),
            });
        }
        if !nodes.contains_key(edge.to.as_str()) {
            return Err(GraphError::UnknownEdgeNode {
                edge: format!("{} -> {}", edge.from, edge.to),
                node: edge.to.clone(),
            });
        }
        let identity = (
            edge.from.as_str(),
            edge.to.as_str(),
            edge.label.as_deref(),
            edge.condition.as_deref(),
        );
        if let Some(label) = &edge.label {
            validate_name("edge label", label, limits.max_name_bytes)?;
        }
        if !seen.insert(identity) {
            return Err(GraphError::DuplicateEdge {
                edge: format!("{} -> {}", edge.from, edge.to),
            });
        }
    }
    Ok(())
}

fn adjacency(definition: &GraphDefinition) -> BTreeMap<&str, Vec<&str>> {
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for node in &definition.nodes {
        result.entry(node.id.as_str()).or_default();
    }
    for edge in &definition.edges {
        result
            .entry(edge.from.as_str())
            .or_default()
            .push(edge.to.as_str());
    }
    for targets in result.values_mut() {
        targets.sort_unstable();
        targets.dedup();
    }
    result
}

fn reverse_adjacency(definition: &GraphDefinition) -> BTreeMap<&str, Vec<&str>> {
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for node in &definition.nodes {
        result.entry(node.id.as_str()).or_default();
    }
    for edge in &definition.edges {
        result
            .entry(edge.to.as_str())
            .or_default()
            .push(edge.from.as_str());
    }
    result
}

fn reachable_from<'a>(
    start: &'a str,
    adjacency: &BTreeMap<&'a str, Vec<&'a str>>,
) -> BTreeSet<&'a str> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        if !reached.insert(node) {
            continue;
        }
        if let Some(targets) = adjacency.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    reached
}

fn validate_entry_and_reachability(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    if !nodes.contains_key(definition.entry.as_str()) {
        return Err(GraphError::UnknownEntry {
            entry: definition.entry.clone(),
        });
    }
    let graph = adjacency(definition);
    let reached = reachable_from(&definition.entry, &graph);
    let unreachable: Vec<_> = nodes
        .keys()
        .filter(|node| !reached.contains(**node))
        .map(|node| (*node).to_owned())
        .collect();
    if unreachable.is_empty() {
        Ok(())
    } else {
        Err(GraphError::UnreachableNodes { nodes: unreachable })
    }
}

fn validate_termination(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    let terminals: Vec<_> = nodes
        .values()
        .filter(|node| node.kind.is_terminal())
        .map(|node| node.id.as_str())
        .collect();
    if terminals.is_empty() {
        return Err(GraphError::MissingTermination);
    }
    let graph = adjacency(definition);
    for terminal in &terminals {
        if graph
            .get(terminal)
            .is_some_and(|targets| !targets.is_empty())
        {
            return Err(GraphError::TerminalHasOutgoingEdge {
                node: (*terminal).to_owned(),
            });
        }
    }
    let reverse = reverse_adjacency(definition);
    let mut can_terminate = BTreeSet::new();
    for terminal in terminals {
        can_terminate.extend(reachable_from(terminal, &reverse));
    }
    let trapped: Vec<_> = nodes
        .keys()
        .filter(|node| !can_terminate.contains(**node))
        .map(|node| (*node).to_owned())
        .collect();
    if trapped.is_empty() {
        Ok(())
    } else {
        Err(GraphError::NoTerminationPath { nodes: trapped })
    }
}

fn validate_node_contracts(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    cache_inputs: &GraphCacheInputs,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    for capability in &definition.declarations.capabilities {
        if !cache_inputs.capability_set.contains(capability) {
            return Err(GraphError::RuntimeCapabilityUnavailable {
                capability: capability.clone(),
            });
        }
    }
    for node in &definition.nodes {
        if node.retry_limit > limits.max_retry_limit {
            return Err(GraphError::RetryLimitExceeded {
                node: node.id.clone(),
                actual: node.retry_limit,
                maximum: limits.max_retry_limit,
            });
        }
        match (node.kind, node.max_iterations) {
            (NodeKind::Loop, Some(value)) if value > 0 && value <= limits.max_loop_iterations => {}
            (NodeKind::Loop, value) => {
                return Err(GraphError::InvalidLoopBound {
                    node: node.id.clone(),
                    actual: value,
                    maximum: limits.max_loop_iterations,
                });
            }
            (_, Some(_)) => {
                return Err(GraphError::LoopBoundOnNonLoop {
                    node: node.id.clone(),
                });
            }
            (_, None) => {}
        }
        let required = node
            .required_capabilities
            .iter()
            .map(String::as_str)
            .chain(node.kind.implied_capability());
        for capability in required {
            if !definition.declarations.capabilities.contains(capability) {
                return Err(GraphError::UndeclaredCapability {
                    node: node.id.clone(),
                    capability: capability.to_owned(),
                });
            }
        }
        match node.kind {
            NodeKind::ToolExecutionGate => {
                if matches!(
                    node.configuration.as_ref(),
                    Some(NodeConfiguration::ProviderToolBatchExecution { .. })
                ) {
                    if node.tool.is_some() {
                        return Err(GraphError::InvalidNodeConfiguration {
                            node: node.id.clone(),
                            detail: "provider tool-batch execution selects tools only from its declared allowlist"
                                .to_owned(),
                        });
                    }
                } else {
                    let tool = node
                        .tool
                        .as_deref()
                        .ok_or_else(|| GraphError::MissingTool {
                            node: node.id.clone(),
                        })?;
                    if !definition.declarations.tools.contains(tool) {
                        return Err(GraphError::UndeclaredTool {
                            node: node.id.clone(),
                            tool: tool.to_owned(),
                        });
                    }
                }
            }
            NodeKind::ModelCall | NodeKind::Review => {
                let provider =
                    node.provider
                        .as_deref()
                        .ok_or_else(|| GraphError::MissingProvider {
                            node: node.id.clone(),
                        })?;
                if !definition.declarations.providers.contains(provider) {
                    return Err(GraphError::UndeclaredProvider {
                        node: node.id.clone(),
                        provider: provider.to_owned(),
                    });
                }
            }
            _ => {}
        }
        validate_node_configuration(node, nodes, definition, limits)?;
        parse_condition(node.condition.as_deref(), &node.id, limits.expression)?;
    }
    for edge in &definition.edges {
        parse_condition(
            edge.condition.as_deref(),
            &format!("{} -> {}", edge.from, edge.to),
            limits.expression,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // Exhaustive typed configuration dispatch is intentionally colocated.
fn validate_node_configuration(
    node: &NodeDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    definition: &GraphDefinition,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let Some(configuration) = &node.configuration else {
        if node.kind == NodeKind::PersistArtifact
            && !limits.allow_legacy_unconfigured_artifact_persistence
        {
            return Err(GraphError::InvalidNodeConfiguration {
                node: node.id.clone(),
                detail: "artifact persistence requires explicit bounded content configuration"
                    .to_owned(),
            });
        }
        return Ok(());
    };
    let serialized_bytes = serde_json::to_vec(configuration)
        .map_err(|error| GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: error.to_string(),
        })?
        .len();
    if serialized_bytes > limits.max_configuration_bytes {
        return Err(GraphError::NodeConfigurationTooLarge {
            node: node.id.clone(),
            actual: serialized_bytes,
            maximum: limits.max_configuration_bytes,
        });
    }
    match configuration {
        NodeConfiguration::ContextTransform { .. } => {
            require_configuration_kind(node, NodeKind::ContextTransform)?;
        }
        NodeConfiguration::ModelRequest {
            disposition_output,
            result_output,
            provider_options,
            json_outputs,
            inputs,
        } => {
            require_configuration_kind(node, NodeKind::ModelCall)?;
            let mut exact_outputs = json_outputs.keys().cloned().collect::<BTreeSet<_>>();
            let outputs_are_distinct = disposition_output != result_output
                && exact_outputs.insert(disposition_output.clone())
                && exact_outputs.insert(result_output.clone());
            if !outputs_are_distinct || node.write_variables != exact_outputs {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "model request disposition, result, and JSON outputs must have distinct names and exactly match declared node writes".to_owned(),
                });
            }
            validate_collection_len(
                &node.id,
                "model provider options",
                provider_options.len(),
                limits,
            )?;
            validate_collection_len(&node.id, "model JSON outputs", json_outputs.len(), limits)?;
            validate_collection_len(&node.id, "model inputs", inputs.len(), limits)?;
            for (name, value) in provider_options {
                validate_name("model provider option", name, limits.max_name_bytes)?;
                validate_bounded_configuration_string(
                    node,
                    "model provider option value",
                    value,
                    limits.max_name_bytes,
                )?;
            }
            validate_configuration_output_variable_type(
                node,
                definition,
                disposition_output,
                "enum(response_complete, tool_requests)",
                is_model_disposition_type,
                limits.max_name_bytes,
            )?;
            validate_configuration_output_variable_type(
                node,
                definition,
                result_output,
                "node_result_reference",
                |value_type| *value_type == VariableValueType::NodeResultReference,
                limits.max_name_bytes,
            )?;
            for (variable, pointer) in json_outputs {
                validate_configuration_output_variable_type(
                    node,
                    definition,
                    variable,
                    "ordinary bounded canonical value",
                    ordinary_effect_output_type,
                    limits.max_name_bytes,
                )?;
                validate_json_pointer(node, variable, pointer, limits.max_name_bytes)?;
            }
            for (name, source) in inputs {
                validate_name("model input", name, limits.max_name_bytes)?;
                match source {
                    NodeValueSource::Static { value } => {
                        validate_json_value(&node.id, "model input", value, limits)?;
                    }
                    NodeValueSource::Variable { variable } => {
                        validate_configuration_variable(
                            node,
                            definition,
                            variable,
                            false,
                            false,
                            limits.max_name_bytes,
                        )?;
                    }
                }
            }
        }
        NodeConfiguration::ToolExecution { arguments } => {
            require_configuration_kind(node, NodeKind::ToolExecutionGate)?;
            match arguments {
                NodeValueSource::Static { value } => {
                    validate_json_value(&node.id, "tool arguments", value, limits)?;
                }
                NodeValueSource::Variable { variable } => {
                    validate_configuration_variable(
                        node,
                        definition,
                        variable,
                        false,
                        false,
                        limits.max_name_bytes,
                    )?;
                }
            }
        }
        NodeConfiguration::ProviderToolBatchExecution {
            request_reference_variable,
            disposition_variable,
            maximum_calls,
            allowed_tools,
        } => {
            require_configuration_kind(node, NodeKind::ToolExecutionGate)?;
            validate_configuration_variable_type(
                node,
                definition,
                request_reference_variable,
                "node_result_reference",
                |value_type| *value_type == VariableValueType::NodeResultReference,
                limits.max_name_bytes,
            )?;
            validate_configuration_variable_type(
                node,
                definition,
                disposition_variable,
                "enum(response_complete, tool_requests)",
                is_model_disposition_type,
                limits.max_name_bytes,
            )?;
            if *maximum_calls == 0
                || usize::try_from(*maximum_calls)
                    .map_or(true, |value| value > limits.max_configuration_items)
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "provider tool-batch maximum_calls must be nonzero and bounded"
                        .to_owned(),
                });
            }
            if allowed_tools.is_empty() || allowed_tools.len() > limits.max_configuration_items {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "provider tool-batch allowed_tools must be nonempty and bounded"
                        .to_owned(),
                });
            }
            for tool in allowed_tools {
                validate_name(
                    "provider tool-batch allowed tool",
                    tool,
                    limits.max_name_bytes,
                )?;
                if !definition.declarations.tools.contains(tool) {
                    return Err(GraphError::UndeclaredTool {
                        node: node.id.clone(),
                        tool: tool.clone(),
                    });
                }
            }
            let Some(result_output) = node.write_variables.iter().next() else {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "provider tool-batch execution must declare one terminal result output"
                        .to_owned(),
                });
            };
            if node.write_variables.len() != 1 {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "provider tool-batch execution must declare exactly one terminal result output"
                        .to_owned(),
                });
            }
            validate_configuration_output_variable_type(
                node,
                definition,
                result_output,
                "node_result_reference",
                |value_type| *value_type == VariableValueType::NodeResultReference,
                limits.max_name_bytes,
            )?;
        }
        NodeConfiguration::UserApproval { action_summary } => {
            require_configuration_kind(node, NodeKind::UserApproval)?;
            match action_summary {
                NodeTextSource::Static { value }
                    if !value.trim().is_empty()
                        && value.len() <= limits.max_configuration_bytes => {}
                NodeTextSource::Static { .. } => {
                    return Err(GraphError::InvalidNodeConfiguration {
                        node: node.id.clone(),
                        detail: "approval summary must be non-empty and bounded".to_owned(),
                    });
                }
                NodeTextSource::Variable { variable } => {
                    validate_configuration_variable(
                        node,
                        definition,
                        variable,
                        true,
                        false,
                        limits.max_name_bytes,
                    )?;
                }
            }
        }
        NodeConfiguration::SpawnChildAgent {
            task_input,
            task_id_prefix,
            child_style,
            tool_groups,
            maximum_children,
            maximum_depth,
            token_budget,
            context_budget_tokens,
            cost_budget_micros,
            workspace,
            artifact_references,
            artifact_reference_variables,
            security_classification,
            approval_required,
        } => {
            require_configuration_kind(node, NodeKind::SpawnChildAgent)?;
            validate_name(
                "child task id prefix",
                task_id_prefix,
                limits.max_name_bytes,
            )?;
            if child_style.trim().is_empty()
                || child_style.len() > limits.max_name_bytes
                || child_style.chars().any(char::is_control)
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "child style selector must be non-empty and bounded".to_owned(),
                });
            }
            validate_collection_len(&node.id, "child tool groups", tool_groups.len(), limits)?;
            validate_collection_len(
                &node.id,
                "child artifact references",
                artifact_references.len(),
                limits,
            )?;
            validate_collection_len(
                &node.id,
                "child artifact reference variables",
                artifact_reference_variables.len(),
                limits,
            )?;
            if *maximum_children == 0
                || usize::try_from(*maximum_children)
                    .map_or(true, |value| value > limits.max_configuration_items)
                || *maximum_depth == 0
                || *maximum_depth > 64
                || *token_budget == 0
                || *token_budget > definition.budget.max_tokens
                || *context_budget_tokens == 0
                || *context_budget_tokens > *token_budget
                || *cost_budget_micros == 0
                || *cost_budget_micros > definition.budget.max_cost_micros
                || !*approval_required
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "child count, depth, token/context/cost budgets, and mandatory approval must be positive and within graph bounds".to_owned(),
                });
            }
            for tool_group in tool_groups {
                validate_name("child tool group", tool_group, limits.max_name_bytes)?;
            }
            for artifact in artifact_references {
                validate_name("child artifact reference", artifact, limits.max_name_bytes)?;
            }
            for variable in artifact_reference_variables {
                validate_configuration_variable_type(
                    node,
                    definition,
                    variable,
                    "artifact_reference",
                    |value_type| *value_type == VariableValueType::ArtifactReference,
                    limits.max_name_bytes,
                )?;
            }
            validate_child_task_source(
                node,
                definition,
                task_input,
                *maximum_children,
                *security_classification,
                limits,
            )?;
            validate_child_workspace(node, definition, workspace, limits)?;
        }
        NodeConfiguration::WaitForAgents {
            children,
            maximum_children,
            minimum_successes,
            timeout_ms,
            ..
        } => {
            require_configuration_kind(node, NodeKind::WaitForAgents)?;
            if *maximum_children == 0
                || usize::try_from(*maximum_children)
                    .map_or(true, |value| value > limits.max_configuration_items)
                || *minimum_successes == 0
                || *minimum_successes > *maximum_children
                || *timeout_ms == 0
                || *timeout_ms > definition.budget.max_duration_ms
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "child wait count, success threshold, and timeout must be positive and within graph bounds".to_owned(),
                });
            }
            match children {
                ChildSetSource::Exact { child_ids } => {
                    validate_collection_len(&node.id, "wait child ids", child_ids.len(), limits)?;
                    if child_ids.is_empty()
                        || child_ids.len() > *maximum_children as usize
                        || *minimum_successes as usize > child_ids.len()
                    {
                        return Err(GraphError::InvalidNodeConfiguration {
                            node: node.id.clone(),
                            detail:
                                "exact child set must be non-empty and satisfy configured bounds"
                                    .to_owned(),
                        });
                    }
                    for child_id in child_ids {
                        validate_name("wait child id", child_id, limits.max_name_bytes)?;
                    }
                }
                ChildSetSource::Variable { variable } => {
                    validate_configuration_variable_type(
                        node,
                        definition,
                        variable,
                        "child_id or list<child_id>",
                        |value_type| {
                            *value_type == VariableValueType::ChildId
                                || matches!(
                                    value_type,
                                    VariableValueType::List { item_type, .. }
                                        if **item_type == VariableValueType::ChildId
                                )
                        },
                        limits.max_name_bytes,
                    )?;
                }
            }
        }
        NodeConfiguration::Review {
            input,
            artifact_references,
            artifact_reference_variables,
            result_schema,
            routes,
            maximum_revisions,
        } => {
            require_configuration_kind(node, NodeKind::Review)?;
            validate_collection_len(
                &node.id,
                "review artifact references",
                artifact_references.len(),
                limits,
            )?;
            validate_collection_len(
                &node.id,
                "review artifact reference variables",
                artifact_reference_variables.len(),
                limits,
            )?;
            if result_schema.maximum_findings == 0
                || result_schema.maximum_findings as usize > limits.max_configuration_items
                || result_schema.maximum_finding_bytes == 0
                || result_schema.maximum_finding_bytes as usize > limits.max_configuration_bytes
                || result_schema.maximum_rejections == 0
                || result_schema.maximum_rejections as usize > limits.max_configuration_items
                || *maximum_revisions == 0
                || *maximum_revisions > limits.max_loop_iterations
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail:
                        "review schema and revision limits must be positive and compiler-bounded"
                            .to_owned(),
                });
            }
            for artifact in artifact_references {
                validate_name("review artifact reference", artifact, limits.max_name_bytes)?;
            }
            for variable in artifact_reference_variables {
                validate_configuration_variable_type(
                    node,
                    definition,
                    variable,
                    "artifact_reference",
                    |value_type| *value_type == VariableValueType::ArtifactReference,
                    limits.max_name_bytes,
                )?;
            }
            validate_review_input(node, definition, input, limits)?;
            validate_review_routes(node, nodes, definition, routes, limits)?;
        }
        NodeConfiguration::PersistArtifact {
            content, mime_type, ..
        } => {
            require_configuration_kind(node, NodeKind::PersistArtifact)?;
            if !valid_mime_type(mime_type, limits.max_name_bytes) {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "artifact mime_type must be a bounded ASCII type/subtype".to_owned(),
                });
            }
            match content {
                ArtifactContentSource::StaticJson { value } => {
                    validate_json_value(&node.id, "artifact content", value, limits)?;
                }
                ArtifactContentSource::StaticText { value }
                    if value.len() <= limits.max_configuration_bytes => {}
                ArtifactContentSource::StaticText { .. } => {
                    return Err(GraphError::InvalidNodeConfiguration {
                        node: node.id.clone(),
                        detail: "artifact text exceeds the compiler bound".to_owned(),
                    });
                }
                ArtifactContentSource::Variable { variable } => {
                    validate_configuration_variable(
                        node,
                        definition,
                        variable,
                        false,
                        true,
                        limits.max_name_bytes,
                    )?;
                }
                ArtifactContentSource::ProviderResultText { reference_variable }
                | ArtifactContentSource::NodeResultProjection { reference_variable } => {
                    validate_configuration_variable_type(
                        node,
                        definition,
                        reference_variable,
                        "node_result_reference",
                        |value_type| *value_type == VariableValueType::NodeResultReference,
                        limits.max_name_bytes,
                    )?;
                }
            }
        }
        NodeConfiguration::SendChildAgentMessage {
            child,
            payload,
            artifact_references,
            max_message_bytes,
            ..
        } => {
            require_configuration_kind(node, NodeKind::SendChildAgentMessage)?;
            validate_collection_len(
                &node.id,
                "artifact_references",
                artifact_references.len(),
                limits,
            )?;
            validate_json_value(&node.id, "payload", payload, limits)?;
            let payload_bytes = canonical_json_bytes(payload)?;
            if *max_message_bytes == 0
                || *max_message_bytes > limits.max_configuration_bytes as u64
                || u64::try_from(payload_bytes.len())
                    .map_or(true, |payload_len| payload_len > *max_message_bytes)
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail:
                        "max_message_bytes must bound the canonical payload and compiler maximum"
                            .to_owned(),
                });
            }
            match child {
                ChildSelector::Exact { child_id } => {
                    validate_name("child id", child_id, limits.max_name_bytes)?;
                }
                ChildSelector::Variable { variable } => {
                    validate_name("child variable", variable, limits.max_name_bytes)?;
                }
            }
            for artifact in artifact_references {
                validate_name("artifact reference", artifact, limits.max_name_bytes)?;
            }
        }
        NodeConfiguration::JoinResults {
            required,
            optional,
            minimum_successes,
            timeout_ms,
            ..
        } => {
            require_configuration_kind(node, NodeKind::JoinResults)?;
            validate_collection_len(&node.id, "required", required.len(), limits)?;
            validate_collection_len(&node.id, "optional", optional.len(), limits)?;
            validate_collection_len(
                &node.id,
                "members",
                required.len().saturating_add(optional.len()),
                limits,
            )?;
            if required.intersection(optional).next().is_some()
                || *minimum_successes == 0
                || *minimum_successes as usize > required.len() + optional.len()
                || *timeout_ms == 0
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "join members must be disjoint, minimum_successes must be feasible, and timeout_ms must be positive".to_owned(),
                });
            }
            for member in required.iter().chain(optional) {
                validate_name("join member", member, limits.max_name_bytes)?;
            }
        }
        NodeConfiguration::ParallelBranch {
            max_parallelism,
            max_queue_depth,
            join_target,
            variable_merge_policies,
            ..
        } => {
            require_configuration_kind(node, NodeKind::ParallelBranch)?;
            if *max_parallelism == 0
                || *max_parallelism > limits.max_parallelism
                || *max_queue_depth == 0
                || *max_queue_depth as usize > limits.max_configuration_items
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail:
                        "parallelism and queue depth must be positive and within compiler bounds"
                            .to_owned(),
                });
            }
            validate_name("parallel join target", join_target, limits.max_name_bytes)?;
            if nodes
                .get(join_target.as_str())
                .is_none_or(|target| target.kind != NodeKind::JoinResults)
            {
                return Err(GraphError::InvalidParallelJoinTarget {
                    node: node.id.clone(),
                    join_target: join_target.clone(),
                });
            }
            validate_collection_len(
                &node.id,
                "variable_merge_policies",
                variable_merge_policies.len(),
                limits,
            )?;
            for variable in variable_merge_policies.keys() {
                validate_name("parallel merge variable", variable, limits.max_name_bytes)?;
            }
        }
        NodeConfiguration::Delay {
            resolution,
            expiration_timestamp,
            ..
        } => {
            require_configuration_kind(node, NodeKind::Delay)?;
            match resolution {
                DelayResolution::Duration { duration_ms } if *duration_ms > 0 => {}
                DelayResolution::Duration { .. } => {
                    return Err(GraphError::InvalidNodeConfiguration {
                        node: node.id.clone(),
                        detail: "delay duration_ms must be positive".to_owned(),
                    });
                }
                DelayResolution::WakeTimestamp { timestamp } => {
                    validate_timestamp("delay wake timestamp", timestamp, limits)?;
                }
            }
            if let Some(timestamp) = expiration_timestamp {
                validate_timestamp("delay expiration timestamp", timestamp, limits)?;
            }
        }
        NodeConfiguration::Schedule { trigger, .. } => {
            require_configuration_kind(node, NodeKind::Schedule)?;
            match trigger {
                ScheduleTrigger::At { timestamp } => {
                    validate_timestamp("schedule timestamp", timestamp, limits)?;
                }
                ScheduleTrigger::Interval {
                    interval_ms,
                    start_timestamp,
                } => {
                    if *interval_ms == 0 {
                        return Err(GraphError::InvalidNodeConfiguration {
                            node: node.id.clone(),
                            detail: "schedule interval_ms must be positive".to_owned(),
                        });
                    }
                    if let Some(timestamp) = start_timestamp {
                        validate_timestamp("schedule start timestamp", timestamp, limits)?;
                    }
                }
                ScheduleTrigger::RuntimeEvent { event_type } => {
                    validate_name("runtime event", event_type, limits.max_name_bytes)?;
                    if !definition.declarations.events.contains(event_type) {
                        return Err(GraphError::UndeclaredEventType {
                            node: node.id.clone(),
                            event_type: event_type.clone(),
                        });
                    }
                }
                ScheduleTrigger::ProcessOutput {
                    process_reference,
                    pattern,
                } => {
                    validate_name(
                        "process reference",
                        process_reference,
                        limits.max_name_bytes,
                    )?;
                    if pattern.is_empty() || pattern.len() > limits.max_name_bytes {
                        return Err(GraphError::InvalidNodeConfiguration {
                            node: node.id.clone(),
                            detail: "process output pattern must be non-empty and bounded"
                                .to_owned(),
                        });
                    }
                }
            }
        }
        NodeConfiguration::EmitEvent {
            event_type,
            payload,
            artifact_references,
            metadata,
        } => {
            require_configuration_kind(node, NodeKind::EmitEvent)?;
            validate_name("event type", event_type, limits.max_name_bytes)?;
            if !definition.declarations.events.contains(event_type) {
                return Err(GraphError::UndeclaredEventType {
                    node: node.id.clone(),
                    event_type: event_type.clone(),
                });
            }
            validate_json_value(&node.id, "event payload", payload, limits)?;
            validate_collection_len(
                &node.id,
                "event artifact references",
                artifact_references.len(),
                limits,
            )?;
            validate_collection_len(&node.id, "event metadata", metadata.len(), limits)?;
            for artifact in artifact_references {
                validate_name("artifact reference", artifact, limits.max_name_bytes)?;
            }
            for (key, value) in metadata {
                validate_name("event metadata key", key, limits.max_name_bytes)?;
                if value.len() > limits.max_name_bytes {
                    return Err(GraphError::InvalidNodeConfiguration {
                        node: node.id.clone(),
                        detail: "event metadata value exceeds the bounded size".to_owned(),
                    });
                }
            }
        }
        NodeConfiguration::CompleteTurn {
            result_reference_variable,
            ..
        } => {
            require_configuration_kind(node, NodeKind::CompleteTurn)?;
            if !node.write_variables.is_empty() {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "complete-turn configuration cannot declare node writes".to_owned(),
                });
            }
            validate_configuration_variable_type(
                node,
                definition,
                result_reference_variable,
                "node_result_reference",
                |value_type| *value_type == VariableValueType::NodeResultReference,
                limits.max_name_bytes,
            )?;
        }
        NodeConfiguration::Plugin {
            plugin_id,
            executor_id,
            executor_version,
            node_kind,
            input_schema,
            output_schema,
            configuration_reference,
            input,
        } => {
            if *node_kind != node.kind {
                return Err(GraphError::InvalidPluginNodeKind {
                    node: node.id.clone(),
                    configured_kind: *node_kind,
                    actual_kind: node.kind,
                });
            }
            if !definition.declarations.plugins.contains(plugin_id) {
                return Err(GraphError::UndeclaredPlugin {
                    node: node.id.clone(),
                    plugin_id: plugin_id.clone(),
                });
            }
            for (kind, value) in [
                ("plugin id", plugin_id.as_str()),
                ("plugin executor id", executor_id.as_str()),
                ("plugin executor version", executor_version.as_str()),
                ("plugin input schema", input_schema.as_str()),
                ("plugin output schema", output_schema.as_str()),
                (
                    "plugin configuration reference",
                    configuration_reference.as_str(),
                ),
            ] {
                validate_name(kind, value, limits.max_name_bytes)?;
            }
            validate_json_value(&node.id, "plugin input", input, limits)?;
        }
    }
    Ok(())
}

fn effective_node_configuration(node: &NodeDefinition) -> Option<NodeConfiguration> {
    node.configuration.clone().or_else(|| match node.kind {
        NodeKind::ContextTransform => Some(NodeConfiguration::ContextTransform {
            strategy: ContextTransformStrategy::PreserveHistory,
        }),
        NodeKind::ToolExecutionGate => Some(NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Static {
                value: serde_json::Value::Object(serde_json::Map::new()),
            },
        }),
        NodeKind::UserApproval => Some(NodeConfiguration::UserApproval {
            action_summary: NodeTextSource::Static {
                value: String::from("graph requested user approval"),
            },
        }),
        _ => None,
    })
}

fn validate_child_task_source(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    source: &NodeValueSource,
    maximum_children: u32,
    security_classification: SecurityClassification,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    match source {
        NodeValueSource::Static { value } => {
            validate_json_value(&node.id, "child task input", value, limits)?;
            if security_classification == SecurityClassification::SecretReference
                || !valid_static_child_tasks(value, maximum_children as usize)
                || contains_forbidden_inline_secret(value)
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "static child tasks must be bounded strings/list/map values without inline secret fields".to_owned(),
                });
            }
        }
        NodeValueSource::Variable { variable } => {
            validate_configuration_variable_type(
                node,
                definition,
                variable,
                "string, list<string>, map<string>, or secret_reference",
                |value_type| match value_type {
                    VariableValueType::String | VariableValueType::SecretReference => true,
                    VariableValueType::List { item_type, .. } => {
                        **item_type == VariableValueType::String
                    }
                    VariableValueType::Map { value_type, .. } => {
                        **value_type == VariableValueType::String
                    }
                    _ => false,
                },
                limits.max_name_bytes,
            )?;
            let declaration = configuration_variable(node, definition, variable)?;
            if (security_classification == SecurityClassification::SecretReference)
                != (declaration.value_type == VariableValueType::SecretReference)
            {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail:
                        "child task secret-reference classification must match its variable type"
                            .to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn valid_static_child_tasks(value: &serde_json::Value, maximum_children: usize) -> bool {
    match value {
        serde_json::Value::String(value) => !value.trim().is_empty() && maximum_children >= 1,
        serde_json::Value::Array(values) => {
            !values.is_empty()
                && values.len() <= maximum_children
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty()))
        }
        serde_json::Value::Object(values) => {
            !values.is_empty()
                && values.len() <= maximum_children
                && values.iter().all(|(task_id, value)| {
                    !task_id.trim().is_empty()
                        && value.as_str().is_some_and(|value| !value.trim().is_empty())
                })
        }
        _ => false,
    }
}

fn contains_forbidden_inline_secret(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(values) => values.iter().any(|(key, value)| {
            matches!(
                key.to_ascii_lowercase().as_str(),
                "secret" | "password" | "token" | "api_key" | "private_key"
            ) || contains_forbidden_inline_secret(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(contains_forbidden_inline_secret),
        _ => false,
    }
}

fn validate_child_workspace(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    workspace: &ChildWorkspaceConfiguration,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    match workspace {
        ChildWorkspaceConfiguration::SharedSerializedWrites { serialization_key } => validate_name(
            "child workspace serialization key",
            serialization_key,
            limits.max_name_bytes,
        ),
        ChildWorkspaceConfiguration::ExplicitCustomWorkspace { path } => match path {
            NodeTextSource::Static { value }
                if !value.trim().is_empty() && value.len() <= limits.max_configuration_bytes =>
            {
                Ok(())
            }
            NodeTextSource::Static { .. } => Err(GraphError::InvalidNodeConfiguration {
                node: node.id.clone(),
                detail: "custom child workspace must be non-empty and bounded".to_owned(),
            }),
            NodeTextSource::Variable { variable } => validate_configuration_variable_type(
                node,
                definition,
                variable,
                "string",
                |value_type| *value_type == VariableValueType::String,
                limits.max_name_bytes,
            ),
        },
        ChildWorkspaceConfiguration::SharedReadOnly
        | ChildWorkspaceConfiguration::IndependentGitWorktree
        | ChildWorkspaceConfiguration::TemporaryCopy
        | ChildWorkspaceConfiguration::IsolatedCopy
        | ChildWorkspaceConfiguration::BranchWorkspace { .. } => Ok(()),
    }
}

fn validate_review_input(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    input: &NodeValueSource,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    match input {
        NodeValueSource::Static { value } => {
            validate_json_value(&node.id, "review input", value, limits)?;
            if contains_forbidden_inline_secret(value) {
                return Err(GraphError::InvalidNodeConfiguration {
                    node: node.id.clone(),
                    detail: "review input cannot contain inline secret fields".to_owned(),
                });
            }
            Ok(())
        }
        NodeValueSource::Variable { variable } => validate_configuration_variable(
            node,
            definition,
            variable,
            false,
            false,
            limits.max_name_bytes,
        ),
    }
}

fn validate_review_routes(
    node: &NodeDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    definition: &GraphDefinition,
    routes: &ReviewRoutes,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let destinations = [&routes.approved, &routes.revision, &routes.failure];
    for destination in destinations {
        validate_name("review route", destination, limits.max_name_bytes)?;
        if !definition
            .edges
            .iter()
            .any(|edge| edge.from == node.id && edge.to == *destination)
            || !nodes.contains_key(destination.as_str())
        {
            return Err(GraphError::InvalidNodeConfiguration {
                node: node.id.clone(),
                detail: format!("review route `{destination}` is not an exact outgoing edge"),
            });
        }
    }
    if routes.approved == routes.revision
        || routes.approved == routes.failure
        || routes.revision == routes.failure
        || nodes
            .get(routes.failure.as_str())
            .is_none_or(|failure| failure.kind != NodeKind::Fail)
    {
        return Err(GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: "review routes must be distinct and failure must target a fail node".to_owned(),
        });
    }
    Ok(())
}

fn configuration_variable<'a>(
    node: &NodeDefinition,
    definition: &'a GraphDefinition,
    variable: &str,
) -> Result<&'a VariableDeclaration, GraphError> {
    definition
        .variables
        .iter()
        .find(|declaration| declaration.name == variable)
        .ok_or_else(|| GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!("configuration variable `{variable}` is not declared"),
        })
}

fn is_model_disposition_type(value_type: &VariableValueType) -> bool {
    matches!(
        value_type,
        VariableValueType::Enum { values }
            if values.len() == 2
                && values.contains("response_complete")
                && values.contains("tool_requests")
    )
}

fn validate_configuration_output_variable_type(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    variable: &str,
    expected: &'static str,
    compatible: impl FnOnce(&VariableValueType) -> bool,
    max_name_bytes: usize,
) -> Result<(), GraphError> {
    validate_name("configuration output variable", variable, max_name_bytes)?;
    let declaration = configuration_variable(node, definition, variable)?;
    if !node.write_variables.contains(variable) || declaration.producer != node.id {
        return Err(GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!(
                "configuration output variable `{variable}` is not an exact declared node write"
            ),
        });
    }
    if compatible(&declaration.value_type) {
        Ok(())
    } else {
        Err(GraphError::ConfigurationVariableTypeMismatch {
            node: node.id.clone(),
            variable: variable.to_owned(),
            expected,
        })
    }
}

fn validate_configuration_variable_type(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    variable: &str,
    expected: &'static str,
    compatible: impl FnOnce(&VariableValueType) -> bool,
    max_name_bytes: usize,
) -> Result<(), GraphError> {
    validate_configuration_variable(node, definition, variable, false, false, max_name_bytes)?;
    if compatible(&configuration_variable(node, definition, variable)?.value_type) {
        Ok(())
    } else {
        Err(GraphError::ConfigurationVariableTypeMismatch {
            node: node.id.clone(),
            variable: variable.to_owned(),
            expected,
        })
    }
}

fn validate_configuration_variable(
    node: &NodeDefinition,
    definition: &GraphDefinition,
    variable: &str,
    require_string: bool,
    require_inline_artifact_value: bool,
    max_name_bytes: usize,
) -> Result<(), GraphError> {
    validate_name("configuration variable", variable, max_name_bytes)?;
    let declaration = definition
        .variables
        .iter()
        .find(|declaration| declaration.name == variable)
        .ok_or_else(|| GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!("configuration variable `{variable}` is not declared"),
        })?;
    if !node.read_variables.contains(variable)
        || !declaration.consumers.contains(&node.id)
        || (require_string && declaration.value_type != VariableValueType::String)
        || (require_inline_artifact_value
            && matches!(
                &declaration.value_type,
                VariableValueType::SessionId
                    | VariableValueType::ChildId
                    | VariableValueType::TaskId
                    | VariableValueType::ArtifactReference
                    | VariableValueType::SecretReference
                    | VariableValueType::ToolResultReference
                    | VariableValueType::NodeResultReference
            ))
    {
        return Err(GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!(
                "configuration variable `{variable}` is not an authorized compatible read"
            ),
        });
    }
    Ok(())
}

fn valid_mime_type(value: &str, maximum: usize) -> bool {
    let Some((type_name, subtype)) = value.split_once('/') else {
        return false;
    };
    value.len() <= maximum
        && !type_name.is_empty()
        && !subtype.is_empty()
        && !subtype.contains('/')
        && type_name.bytes().all(is_mime_token_byte)
        && subtype.bytes().all(is_mime_token_byte)
}

const fn is_mime_token_byte(value: u8) -> bool {
    matches!(
        value,
        b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
            | b'!'
            | b'#'
            | b'$'
            | b'&'
            | b'^'
            | b'_'
            | b'.'
            | b'+'
            | b'-'
    )
}

fn require_configuration_kind(node: &NodeDefinition, expected: NodeKind) -> Result<(), GraphError> {
    if node.kind == expected {
        Ok(())
    } else {
        Err(GraphError::ConfigurationKindMismatch {
            node: node.id.clone(),
            expected,
            actual: node.kind,
        })
    }
}

fn validate_collection_len(
    node: &str,
    field: &str,
    actual: usize,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    if actual > limits.max_configuration_items {
        Err(GraphError::ConfigurationCollectionTooLarge {
            node: node.to_owned(),
            field: field.to_owned(),
            actual,
            maximum: limits.max_configuration_items,
        })
    } else {
        Ok(())
    }
}

fn validate_bounded_configuration_string(
    node: &NodeDefinition,
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), GraphError> {
    if value.len() <= maximum && !value.chars().any(char::is_control) {
        Ok(())
    } else {
        Err(GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!("{field} must be bounded and contain no control characters"),
        })
    }
}

fn validate_json_pointer(
    node: &NodeDefinition,
    variable: &str,
    pointer: &str,
    maximum: usize,
) -> Result<(), GraphError> {
    // The empty RFC 6901 pointer selects the complete provider-visible JSON.
    // Its output declaration was already proven to be an ordinary bounded
    // canonical type, so runtime type validation can safely accept or reject
    // the complete value.
    let valid = pointer.is_empty()
        || (pointer.starts_with('/')
            && pointer.len() <= maximum
            && !pointer.chars().any(char::is_control)
            && valid_json_pointer_escapes(pointer));
    if valid {
        Ok(())
    } else {
        Err(GraphError::InvalidNodeConfiguration {
            node: node.id.clone(),
            detail: format!(
                "model JSON output `{variable}` must use a bounded RFC 6901 JSON Pointer"
            ),
        })
    }
}

fn valid_json_pointer_escapes(pointer: &str) -> bool {
    let mut characters = pointer.chars();
    while let Some(character) = characters.next() {
        if character == '~' && !matches!(characters.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

fn validate_timestamp(
    kind: &'static str,
    timestamp: &str,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    if timestamp.len() > limits.max_name_bytes || !is_rfc3339_timestamp(timestamp) {
        Err(GraphError::InvalidTimestamp {
            kind,
            value: timestamp.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn is_rfc3339_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let Some(year) = decimal_component(bytes, 0, 4) else {
        return false;
    };
    let Some(month) = decimal_component(bytes, 5, 7) else {
        return false;
    };
    let Some(day) = decimal_component(bytes, 8, 10) else {
        return false;
    };
    let Some(hour) = decimal_component(bytes, 11, 13) else {
        return false;
    };
    let Some(minute) = decimal_component(bytes, 14, 16) else {
        return false;
    };
    let Some(second) = decimal_component(bytes, 17, 19) else {
        return false;
    };
    if month == 0
        || month > 12
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return false;
    }
    let mut index = 19;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == fraction_start {
            return false;
        }
    }
    match bytes.get(index) {
        Some(b'Z') => index + 1 == bytes.len(),
        Some(b'+' | b'-') => {
            decimal_component(bytes, index + 1, index + 3)
                .is_some_and(|offset_hour| offset_hour <= 23)
                && bytes.get(index + 3) == Some(&b':')
                && decimal_component(bytes, index + 4, index + 6)
                    .is_some_and(|offset_minute| offset_minute <= 59)
                && index + 6 == bytes.len()
        }
        _ => false,
    }
}

fn decimal_component(bytes: &[u8], start: usize, end: usize) -> Option<u32> {
    let component = bytes.get(start..end)?;
    if component.iter().all(u8::is_ascii_digit) {
        std::str::from_utf8(component).ok()?.parse().ok()
    } else {
        None
    }
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

fn validate_json_value(
    node: &str,
    field: &str,
    value: &serde_json::Value,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let serialized = canonical_json_bytes(value)?;
    if serialized.len() > limits.max_configuration_bytes {
        return Err(GraphError::NodeConfigurationTooLarge {
            node: node.to_owned(),
            actual: serialized.len(),
            maximum: limits.max_configuration_bytes,
        });
    }
    if json_depth(value) > limits.max_value_depth {
        return Err(GraphError::NodeConfigurationTooDeep {
            node: node.to_owned(),
            field: field.to_owned(),
            maximum: limits.max_value_depth,
        });
    }
    Ok(())
}

fn json_depth(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => 1,
    }
}

fn canonical_json_bytes(value: &serde_json::Value) -> Result<Vec<u8>, GraphError> {
    let mut canonical = value.clone();
    canonicalize_json(&mut canonical);
    serde_json::to_vec(&canonical).map_err(|error| GraphError::Inspection {
        detail: error.to_string(),
    })
}

fn canonicalize_node_configuration(configuration: &mut NodeConfiguration) {
    match configuration {
        NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Static { value },
        }
        | NodeConfiguration::SpawnChildAgent {
            task_input: NodeValueSource::Static { value },
            ..
        }
        | NodeConfiguration::Review {
            input: NodeValueSource::Static { value },
            ..
        }
        | NodeConfiguration::PersistArtifact {
            content: ArtifactContentSource::StaticJson { value },
            ..
        }
        | NodeConfiguration::SendChildAgentMessage { payload: value, .. }
        | NodeConfiguration::EmitEvent { payload: value, .. }
        | NodeConfiguration::Plugin { input: value, .. } => canonicalize_json(value),
        NodeConfiguration::ContextTransform { .. }
        | NodeConfiguration::ModelRequest { .. }
        | NodeConfiguration::ToolExecution {
            arguments: NodeValueSource::Variable { .. },
        }
        | NodeConfiguration::ProviderToolBatchExecution { .. }
        | NodeConfiguration::SpawnChildAgent {
            task_input: NodeValueSource::Variable { .. },
            ..
        }
        | NodeConfiguration::Review {
            input: NodeValueSource::Variable { .. },
            ..
        }
        | NodeConfiguration::UserApproval { .. }
        | NodeConfiguration::WaitForAgents { .. }
        | NodeConfiguration::PersistArtifact { .. }
        | NodeConfiguration::JoinResults { .. }
        | NodeConfiguration::ParallelBranch { .. }
        | NodeConfiguration::Delay { .. }
        | NodeConfiguration::Schedule { .. }
        | NodeConfiguration::CompleteTurn { .. } => {}
    }
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for item in values {
                canonicalize_json(item);
            }
        }
        serde_json::Value::Object(values) => {
            let mut sorted = BTreeMap::new();
            for (key, mut item) in std::mem::take(values) {
                canonicalize_json(&mut item);
                sorted.insert(key, item);
            }
            values.extend(sorted);
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn parse_condition(
    source: Option<&str>,
    owner: &str,
    limits: ExpressionLimits,
) -> Result<Option<Expression>, GraphError> {
    source
        .map(|condition| {
            Expression::parse(condition, limits).map_err(|error| GraphError::InvalidCondition {
                owner: owner.to_owned(),
                error,
            })
        })
        .transpose()
}

fn validate_cycles(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    let graph = adjacency(definition);
    let loop_nodes: BTreeSet<_> = nodes
        .values()
        .filter(|node| node.kind == NodeKind::Loop)
        .map(|node| node.id.as_str())
        .collect();
    let mut incoming: BTreeMap<_, usize> = nodes
        .keys()
        .copied()
        .filter(|node| !loop_nodes.contains(node))
        .map(|node| (node, 0))
        .collect();
    for (source, targets) in &graph {
        if loop_nodes.contains(source) {
            continue;
        }
        for target in targets {
            if !loop_nodes.contains(target) {
                *incoming
                    .get_mut(target)
                    .expect("validated target exists in non-loop node map") += 1;
            }
        }
    }
    let mut ready: BTreeSet<_> = incoming
        .iter()
        .filter_map(|(node, count)| (*count == 0).then_some(*node))
        .collect();
    let mut processed = 0;
    while let Some(node) = ready.pop_first() {
        processed += 1;
        for target in graph.get(node).into_iter().flatten().copied() {
            if loop_nodes.contains(target) {
                continue;
            }
            let count = incoming
                .get_mut(target)
                .expect("validated target exists in non-loop node map");
            *count -= 1;
            if *count == 0 {
                ready.insert(target);
            }
        }
    }
    if processed == incoming.len() {
        Ok(())
    } else {
        Err(GraphError::IllegalCycle {
            nodes: incoming
                .into_iter()
                .filter_map(|(node, count)| (count > 0).then_some(node.to_owned()))
                .collect(),
        })
    }
}

fn validate_parallel_writes(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    variables: &BTreeMap<&str, &VariableDeclaration>,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let graph = adjacency(definition);
    let reverse = reverse_adjacency(definition);
    for parallel in nodes
        .values()
        .filter(|node| node.kind == NodeKind::ParallelBranch)
    {
        let outgoing = definition
            .edges
            .iter()
            .filter(|edge| edge.from == parallel.id)
            .collect::<Vec<_>>();
        let branches =
            validate_parallel_topology(parallel, &outgoing, &graph, &reverse, nodes, limits)?;
        if branches.len() < 2 {
            return Err(GraphError::ParallelNeedsBranches {
                node: parallel.id.clone(),
            });
        }
        let Some(NodeConfiguration::ParallelBranch {
            join_target,
            variable_merge_policies,
            serialization_policy,
            ..
        }) = &parallel.configuration
        else {
            return Err(GraphError::InvalidNodeConfiguration {
                node: parallel.id.clone(),
                detail: "parallel_branch requires typed configuration".to_owned(),
            });
        };
        let join = join_target.as_str();
        let configuration = Some((variable_merge_policies, serialization_policy));
        let scopes: Vec<_> = branches
            .iter()
            .map(|branch| branch_write_scopes(branch, join, &graph, nodes))
            .collect();
        for left in 0..branches.len() {
            for right in (left + 1)..branches.len() {
                let conflicting_scope = (!configuration
                    .is_some_and(|(_, policy)| policy.is_some()))
                .then(|| scopes[left].intersection(&scopes[right]).next())
                .flatten();
                if let Some(scope) = conflicting_scope {
                    return Err(GraphError::ConflictingParallelWrites {
                        node: parallel.id.clone(),
                        scope: (*scope).to_owned(),
                        branches: vec![branches[left].to_owned(), branches[right].to_owned()],
                    });
                }
            }
        }
        let written_variables: Vec<_> = branches
            .iter()
            .map(|branch| branch_write_variables(branch, join, &graph, nodes))
            .collect();
        for left in 0..branches.len() {
            for right in (left + 1)..branches.len() {
                for variable in written_variables[left].intersection(&written_variables[right]) {
                    let declaration = variables
                        .get(variable)
                        .expect("node variable writes were validated before parallel writes");
                    let configured_policy =
                        configuration.and_then(|(policies, _)| policies.get(*variable));
                    let serialized = configuration.is_some_and(|(_, policy)| policy.is_some());
                    let has_merge =
                        declaration.merge_policy.is_some() || configured_policy.is_some();
                    if declaration.scope != VariableScope::Branch && !has_merge && !serialized {
                        return Err(GraphError::ConflictingParallelVariableWrites {
                            node: parallel.id.clone(),
                            variable: (*variable).to_owned(),
                            branches: vec![branches[left].to_owned(), branches[right].to_owned()],
                        });
                    }
                }
            }
        }
        if let Some((policies, _)) = configuration {
            for variable in policies.keys() {
                if !variables.contains_key(variable.as_str()) {
                    return Err(GraphError::UnknownParallelMergeVariable {
                        node: parallel.id.clone(),
                        variable: variable.clone(),
                    });
                }
            }
        }
    }
    validate_parallel_merge_contributor_ownership(definition, nodes, variables, &graph)?;
    Ok(())
}

fn validate_parallel_merge_contributor_ownership(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    variables: &BTreeMap<&str, &VariableDeclaration>,
    graph: &BTreeMap<&str, Vec<&str>>,
) -> Result<(), GraphError> {
    let mut contributor_owners: BTreeMap<&str, Vec<(String, Option<VariableMergePolicy>)>> =
        variables
            .values()
            .filter(|variable| !variable.merge_contributors.is_empty())
            .map(|variable| (variable.name.as_str(), Vec::new()))
            .collect();
    for parallel in nodes
        .values()
        .filter(|node| node.kind == NodeKind::ParallelBranch)
    {
        let Some(NodeConfiguration::ParallelBranch {
            join_target,
            variable_merge_policies,
            ..
        }) = &parallel.configuration
        else {
            continue;
        };
        let branches = definition
            .edges
            .iter()
            .filter(|edge| edge.from == parallel.id)
            .map(|edge| edge.to.as_str())
            .collect::<Vec<_>>();
        let regions = branches
            .iter()
            .map(|branch| {
                parallel_branch_region(parallel.id.as_str(), branch, join_target, graph, nodes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for variable in variables
            .values()
            .filter(|variable| !variable.merge_contributors.is_empty())
        {
            let writers = std::iter::once(variable.producer.as_str())
                .chain(variable.merge_contributors.iter().map(String::as_str))
                .collect::<BTreeSet<_>>();
            let writer_regions = writers
                .iter()
                .filter_map(|writer| regions.iter().position(|region| region.contains(*writer)))
                .collect::<BTreeSet<_>>();
            if writer_regions.len() == writers.len() {
                contributor_owners
                    .get_mut(variable.name.as_str())
                    .expect("declared contributor variable has an ownership accumulator")
                    .push((
                        parallel.id.clone(),
                        variable_merge_policies.get(variable.name.as_str()).copied(),
                    ));
            }
        }
    }
    for variable in variables
        .values()
        .filter(|variable| !variable.merge_contributors.is_empty())
    {
        let owners = contributor_owners
            .get(variable.name.as_str())
            .expect("declared contributor variable has an ownership accumulator");
        let [(owner, configured_policy)] = owners.as_slice() else {
            let detail = if owners.is_empty() {
                "producer and contributors must occupy distinct branches of one exact parallel region before its configured join"
            } else {
                "producer and contributors match more than one parallel region"
            };
            return Err(GraphError::InvalidParallelMergeContributorOwnership {
                variable: variable.name.clone(),
                detail: detail.to_owned(),
            });
        };
        if *configured_policy != variable.merge_policy {
            return Err(GraphError::ParallelMergeContributorPolicyMismatch {
                variable: variable.name.clone(),
                node: owner.clone(),
                declared: variable
                    .merge_policy
                    .expect("merge contributors require a declaration policy"),
                configured: *configured_policy,
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // The fail-closed topology invariants are easier to audit together.
fn validate_parallel_topology<'a>(
    parallel: &NodeDefinition,
    outgoing: &[&'a EdgeDefinition],
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    reverse: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
    limits: CompilerLimits,
) -> Result<Vec<&'a str>, GraphError> {
    if outgoing.len() < 2 {
        return Err(GraphError::ParallelNeedsBranches {
            node: parallel.id.clone(),
        });
    }
    let Some(NodeConfiguration::ParallelBranch {
        max_parallelism,
        max_queue_depth,
        join_target,
        join_policy,
        serialization_policy,
        ..
    }) = &parallel.configuration
    else {
        return Err(GraphError::InvalidNodeConfiguration {
            node: parallel.id.clone(),
            detail: "parallel_branch requires typed configuration".to_owned(),
        });
    };
    let active_capacity = if serialization_policy.is_some() {
        1
    } else {
        usize::try_from(*max_parallelism).unwrap_or(usize::MAX)
    };
    let total_capacity =
        active_capacity.saturating_add(usize::try_from(*max_queue_depth).unwrap_or(usize::MAX));
    if outgoing.len() > total_capacity {
        return Err(GraphError::InvalidNodeConfiguration {
            node: parallel.id.clone(),
            detail: "parallel fan-out exceeds active plus queued branch capacity".to_owned(),
        });
    }
    let join =
        nodes
            .get(join_target.as_str())
            .ok_or_else(|| GraphError::InvalidParallelJoinTarget {
                node: parallel.id.clone(),
                join_target: join_target.clone(),
            })?;
    let Some(NodeConfiguration::JoinResults {
        required,
        optional,
        minimum_successes,
        failure_policy,
        ..
    }) = &join.configuration
    else {
        return Err(GraphError::InvalidNodeConfiguration {
            node: join.id.clone(),
            detail: "configured parallel join requires typed join_results configuration".to_owned(),
        });
    };

    let mut labels = BTreeSet::new();
    let mut targets = BTreeSet::new();
    for edge in outgoing {
        if edge.condition.is_some() {
            return Err(GraphError::ConditionalParallelFanout {
                node: parallel.id.clone(),
                target: edge.to.clone(),
            });
        }
        let label = edge
            .label
            .as_deref()
            .ok_or_else(|| GraphError::UnlabeledParallelFanout {
                node: parallel.id.clone(),
                target: edge.to.clone(),
            })?;
        validate_name("parallel member reference", label, limits.max_name_bytes)?;
        if !labels.insert(label) {
            return Err(GraphError::DuplicateParallelMemberReference {
                node: parallel.id.clone(),
                reference: label.to_owned(),
            });
        }
        if edge.to == *join_target {
            return Err(GraphError::InvalidParallelJoinTarget {
                node: parallel.id.clone(),
                join_target: join_target.clone(),
            });
        }
        if !targets.insert(edge.to.as_str()) {
            return Err(GraphError::DuplicateParallelBranchTarget {
                node: parallel.id.clone(),
                target: edge.to.clone(),
            });
        }
    }

    let configured_members = required.union(optional).map(String::as_str).collect();
    if labels != configured_members {
        return Err(GraphError::ParallelJoinMemberMismatch {
            node: parallel.id.clone(),
            join: join.id.clone(),
            fanout_members: labels.into_iter().map(str::to_owned).collect(),
            join_members: configured_members.into_iter().map(str::to_owned).collect(),
        });
    }
    match join_policy {
        ParallelJoinPolicy::All
            if !optional.is_empty()
                || required.len() != targets.len()
                || usize::try_from(*minimum_successes).unwrap_or(usize::MAX) != targets.len() =>
        {
            return Err(GraphError::ParallelJoinPolicyMismatch {
                node: parallel.id.clone(),
                join: join.id.clone(),
            });
        }
        ParallelJoinPolicy::MinimumSuccess
            if *failure_policy != JoinFailurePolicy::MinimumSuccess =>
        {
            return Err(GraphError::ParallelJoinPolicyMismatch {
                node: parallel.id.clone(),
                join: join.id.clone(),
            });
        }
        _ => {}
    }

    let branches = outgoing
        .iter()
        .map(|edge| edge.to.as_str())
        .collect::<Vec<_>>();
    let mut regions = Vec::with_capacity(branches.len());
    for branch in &branches {
        regions.push(parallel_branch_region(
            parallel.id.as_str(),
            branch,
            join_target,
            graph,
            nodes,
        )?);
    }
    for left in 0..regions.len() {
        for right in (left + 1)..regions.len() {
            if let Some(shared) = regions[left].intersection(&regions[right]).next() {
                return Err(GraphError::OverlappingParallelBranchRegions {
                    node: parallel.id.clone(),
                    branches: vec![branches[left].to_owned(), branches[right].to_owned()],
                    shared_node: (*shared).to_owned(),
                });
            }
        }
    }

    let all_members = regions
        .iter()
        .flat_map(BTreeSet::iter)
        .copied()
        .collect::<BTreeSet<_>>();
    for (branch, region) in branches.iter().zip(&regions) {
        for member in region {
            for source in reverse.get(member).into_iter().flatten() {
                let is_fanout_edge = *member == *branch && *source == parallel.id;
                if !is_fanout_edge && !region.contains(source) {
                    return Err(GraphError::ExternalParallelBranchEntry {
                        node: parallel.id.clone(),
                        branch: (*branch).to_owned(),
                        edge_source: (*source).to_owned(),
                        target: (*member).to_owned(),
                    });
                }
            }
        }
    }
    for source in reverse.get(join_target.as_str()).into_iter().flatten() {
        if !all_members.contains(source) {
            return Err(GraphError::ExternalParallelJoinEntry {
                node: parallel.id.clone(),
                join: join.id.clone(),
                edge_source: (*source).to_owned(),
            });
        }
    }
    Ok(branches)
}

fn parallel_branch_region<'a>(
    parallel: &str,
    branch: &'a str,
    join: &str,
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> Result<BTreeSet<&'a str>, GraphError> {
    let mut members = BTreeSet::new();
    let mut queue = VecDeque::from([branch]);
    while let Some(node) = queue.pop_front() {
        if node == join || !members.insert(node) {
            continue;
        }
        let definition = nodes
            .get(node)
            .expect("parallel topology uses already-validated graph nodes");
        match definition.kind {
            NodeKind::Fail => continue,
            NodeKind::CompleteTurn | NodeKind::CompleteSession => {
                return Err(GraphError::ParallelBranchTerminalCompletion {
                    node: parallel.to_owned(),
                    branch: branch.to_owned(),
                    terminal: node.to_owned(),
                });
            }
            _ => {}
        }
        let targets = graph.get(node).map(Vec::as_slice).unwrap_or_default();
        if targets.is_empty() {
            return Err(GraphError::ParallelBranchBypassesJoin {
                node: parallel.to_owned(),
                branch: branch.to_owned(),
                path_end: node.to_owned(),
            });
        }
        queue.extend(targets.iter().copied());
    }
    Ok(members)
}

fn common_join<'a>(
    branches: &[&'a str],
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> Option<&'a str> {
    let mut common: Option<BTreeSet<&str>> = None;
    for branch in branches {
        let reachable = reachable_from(branch, graph);
        let joins: BTreeSet<_> = reachable
            .into_iter()
            .filter(|node| {
                nodes
                    .get(node)
                    .is_some_and(|item| item.kind == NodeKind::JoinResults)
            })
            .collect();
        common = Some(match common {
            None => joins,
            Some(current) => current.intersection(&joins).copied().collect(),
        });
    }
    common?.into_iter().next()
}

fn branch_write_scopes<'a>(
    branch: &'a str,
    join: &str,
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> BTreeSet<&'a str> {
    let mut scopes = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([branch]);
    while let Some(node) = queue.pop_front() {
        if node == join || !visited.insert(node) {
            continue;
        }
        if let Some(definition) = nodes.get(node) {
            scopes.extend(definition.write_scopes.iter().map(String::as_str));
        }
        if let Some(targets) = graph.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    scopes
}

fn branch_write_variables<'a>(
    branch: &'a str,
    join: &str,
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> BTreeSet<&'a str> {
    let mut variables = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([branch]);
    while let Some(node) = queue.pop_front() {
        if node == join || !visited.insert(node) {
            continue;
        }
        if let Some(definition) = nodes.get(node) {
            variables.extend(definition.write_variables.iter().map(String::as_str));
        }
        if let Some(targets) = graph.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    variables
}

fn build_cache_key(source: &str, inputs: &GraphCacheInputs) -> GraphCacheKey {
    let graph_content_hash = ContentHash::digest(source.as_bytes());
    let plugin_set_hash = inputs.plugin_set_hash;
    let capability_set_hash = ContentHash::digest(&encode_strings(
        inputs.capability_set.iter().map(String::as_str),
    ));
    let runtime_api_hash = ContentHash::digest(inputs.runtime_api_version.as_bytes());
    let mut combined = Vec::with_capacity(128);
    combined.extend_from_slice(graph_content_hash.as_bytes());
    combined.extend_from_slice(plugin_set_hash.as_bytes());
    combined.extend_from_slice(capability_set_hash.as_bytes());
    combined.extend_from_slice(runtime_api_hash.as_bytes());
    GraphCacheKey {
        graph_content_hash,
        plugin_set_hash,
        capability_set_hash,
        runtime_api_hash,
        combined_hash: ContentHash::digest(&combined),
    }
}

fn encode_strings<'a>(values: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
        encoded.extend_from_slice(value.as_bytes());
    }
    encoded
}

/// Deterministic graph rejection.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphError {
    /// Source exceeds the configured byte bound.
    #[error("graph source is {actual} bytes; maximum is {maximum}")]
    SourceTooLarge {
        /// Actual bytes.
        actual: usize,
        /// Maximum bytes.
        maximum: usize,
    },
    /// TOML syntax or schema is invalid.
    #[error("invalid graph TOML: {detail}")]
    InvalidToml {
        /// Parser-owned stable message.
        detail: String,
    },
    /// Source format is unsupported.
    #[error("graph format version {actual} is unsupported; expected {supported}")]
    UnsupportedVersion {
        /// Actual version.
        actual: u16,
        /// Supported version.
        supported: u16,
    },
    /// Node bound exceeded.
    #[error("graph has {actual} nodes; maximum is {maximum}")]
    TooManyNodes {
        /// Actual count.
        actual: usize,
        /// Maximum count.
        maximum: usize,
    },
    /// Edge bound exceeded.
    #[error("graph has {actual} edges; maximum is {maximum}")]
    TooManyEdges {
        /// Actual count.
        actual: usize,
        /// Maximum count.
        maximum: usize,
    },
    /// Budget is zero or exceeds compiler policy.
    #[error("budget `{name}` is {actual}; valid range is 1..={maximum}")]
    InvalidBudget {
        /// Budget field.
        name: &'static str,
        /// Actual value.
        actual: u64,
        /// Maximum.
        maximum: u64,
    },
    /// Identifier or declaration is invalid.
    #[error("invalid {kind} `{value}`; maximum is {maximum} ASCII name bytes")]
    InvalidName {
        /// Name category.
        kind: &'static str,
        /// Invalid value.
        value: String,
        /// Maximum bytes.
        maximum: usize,
    },
    /// Duplicate node ID.
    #[error("duplicate node `{node}`")]
    DuplicateNode {
        /// Duplicate ID.
        node: String,
    },
    /// Entry does not exist.
    #[error("entry node `{entry}` does not exist")]
    UnknownEntry {
        /// Entry ID.
        entry: String,
    },
    /// Edge references an unknown node.
    #[error("edge `{edge}` references unknown node `{node}`")]
    UnknownEdgeNode {
        /// Edge label.
        edge: String,
        /// Unknown node.
        node: String,
    },
    /// Exact duplicate edge.
    #[error("duplicate edge `{edge}`")]
    DuplicateEdge {
        /// Edge label.
        edge: String,
    },
    /// Nodes cannot be reached from entry.
    #[error("unreachable nodes: {nodes:?}")]
    UnreachableNodes {
        /// Sorted IDs.
        nodes: Vec<String>,
    },
    /// No terminal node exists.
    #[error("graph has no complete_turn, complete_session, or fail node")]
    MissingTermination,
    /// Terminal node has an outgoing transition.
    #[error("terminal node `{node}` has an outgoing edge")]
    TerminalHasOutgoingEdge {
        /// Terminal ID.
        node: String,
    },
    /// Reachable nodes cannot reach termination.
    #[error("nodes have no termination path: {nodes:?}")]
    NoTerminationPath {
        /// Sorted IDs.
        nodes: Vec<String>,
    },
    /// Node retry limit exceeds compiler policy.
    #[error("node `{node}` retry limit {actual} exceeds {maximum}")]
    RetryLimitExceeded {
        /// Node ID.
        node: String,
        /// Actual limit.
        actual: u32,
        /// Maximum.
        maximum: u32,
    },
    /// Loop is missing a valid static bound.
    #[error("loop `{node}` has invalid bound {actual:?}; valid range is 1..={maximum}")]
    InvalidLoopBound {
        /// Loop ID.
        node: String,
        /// Actual optional value.
        actual: Option<u32>,
        /// Maximum.
        maximum: u32,
    },
    /// Non-loop node declares a loop bound.
    #[error("non-loop node `{node}` declares max_iterations")]
    LoopBoundOnNonLoop {
        /// Node ID.
        node: String,
    },
    /// Node requires a capability omitted from declarations.
    #[error("node `{node}` requires undeclared capability `{capability}`")]
    UndeclaredCapability {
        /// Node ID.
        node: String,
        /// Capability.
        capability: String,
    },
    /// Graph declares a capability absent from the runtime cache context.
    #[error("runtime does not provide declared capability `{capability}`")]
    RuntimeCapabilityUnavailable {
        /// Missing capability.
        capability: String,
    },
    /// Tool node supplies no tool.
    #[error("tool node `{node}` does not select a tool")]
    MissingTool {
        /// Node ID.
        node: String,
    },
    /// Selected tool was not declared.
    #[error("node `{node}` selects undeclared tool `{tool}`")]
    UndeclaredTool {
        /// Node ID.
        node: String,
        /// Tool name.
        tool: String,
    },
    /// Model/review node supplies no provider.
    #[error("model node `{node}` does not select a provider")]
    MissingProvider {
        /// Node ID.
        node: String,
    },
    /// Selected provider was not declared.
    #[error("node `{node}` selects undeclared provider `{provider}`")]
    UndeclaredProvider {
        /// Node ID.
        node: String,
        /// Provider name.
        provider: String,
    },
    /// Embedded condition is invalid.
    #[error("invalid condition on `{owner}`: {error}")]
    InvalidCondition {
        /// Node or edge owner.
        owner: String,
        /// Expression parse failure.
        error: ParseError,
    },
    /// A cycle exists that does not traverse a bounded loop node.
    #[error("cycle does not traverse a bounded loop node: {nodes:?}")]
    IllegalCycle {
        /// Deterministic cycle nodes.
        nodes: Vec<String>,
    },
    /// Parallel node has fewer than two branches.
    #[error("parallel node `{node}` requires at least two outgoing branches")]
    ParallelNeedsBranches {
        /// Parallel node.
        node: String,
    },
    /// Parallel branches have no common join.
    #[error("parallel node `{node}` has no common join_results node")]
    ParallelMissingJoin {
        /// Parallel node.
        node: String,
    },
    /// Parallel fan-out uses a condition instead of dispatching every declared member.
    #[error("parallel node `{node}` has conditional fan-out to `{target}`")]
    ConditionalParallelFanout {
        /// Parallel node.
        node: String,
        /// Conditional branch target.
        target: String,
    },
    /// Parallel fan-out omits its stable join-member reference.
    #[error("parallel node `{node}` has unlabeled fan-out to `{target}`")]
    UnlabeledParallelFanout {
        /// Parallel node.
        node: String,
        /// Unlabeled branch target.
        target: String,
    },
    /// Parallel fan-out repeats a graph-owned member reference.
    #[error("parallel node `{node}` repeats member reference `{reference}`")]
    DuplicateParallelMemberReference {
        /// Parallel node.
        node: String,
        /// Duplicate configured reference.
        reference: String,
    },
    /// Parallel fan-out repeats a branch entry target.
    #[error("parallel node `{node}` repeats branch target `{target}`")]
    DuplicateParallelBranchTarget {
        /// Parallel node.
        node: String,
        /// Duplicate target node.
        target: String,
    },
    /// Fan-out labels and immutable join membership are not identical.
    #[error(
        "parallel node `{node}` members {fanout_members:?} do not match join `{join}` members {join_members:?}"
    )]
    ParallelJoinMemberMismatch {
        /// Parallel node.
        node: String,
        /// Configured join node.
        join: String,
        /// Stable fan-out labels.
        fanout_members: Vec<String>,
        /// Configured join members.
        join_members: Vec<String>,
    },
    /// Fan-out readiness policy and join threshold/failure policy disagree.
    #[error("parallel node `{node}` policy does not match join `{join}`")]
    ParallelJoinPolicyMismatch {
        /// Parallel node.
        node: String,
        /// Configured join node.
        join: String,
    },
    /// Two branch regions share canonical graph nodes.
    #[error("parallel node `{node}` branches {branches:?} overlap at `{shared_node}`")]
    OverlappingParallelBranchRegions {
        /// Parallel node.
        node: String,
        /// Conflicting branch entries.
        branches: Vec<String>,
        /// Shared graph node.
        shared_node: String,
    },
    /// A branch region can be entered without its owning fan-out.
    #[error(
        "parallel node `{node}` branch `{branch}` has outside edge `{edge_source}` -> `{target}`"
    )]
    ExternalParallelBranchEntry {
        /// Parallel node.
        node: String,
        /// Owned branch entry.
        branch: String,
        /// Outside edge source.
        edge_source: String,
        /// Branch-region target.
        target: String,
    },
    /// A configured join can be entered outside its owning branch regions.
    #[error("parallel node `{node}` join `{join}` has outside edge from `{edge_source}`")]
    ExternalParallelJoinEntry {
        /// Parallel node.
        node: String,
        /// Configured join.
        join: String,
        /// Outside edge source.
        edge_source: String,
    },
    /// A non-failure branch terminates the enclosing run before its join.
    #[error(
        "parallel node `{node}` branch `{branch}` reaches completion `{terminal}` before its join"
    )]
    ParallelBranchTerminalCompletion {
        /// Parallel node.
        node: String,
        /// Branch entry.
        branch: String,
        /// Premature completion node.
        terminal: String,
    },
    /// A non-failure branch path ends without reaching the configured join.
    #[error(
        "parallel node `{node}` branch `{branch}` ends at `{path_end}` without reaching its join"
    )]
    ParallelBranchBypassesJoin {
        /// Parallel node.
        node: String,
        /// Branch entry.
        branch: String,
        /// Path end.
        path_end: String,
    },
    /// Parallel branches propose writes to the same scope.
    #[error("parallel node `{node}` branches {branches:?} both write `{scope}`")]
    ConflictingParallelWrites {
        /// Parallel node.
        node: String,
        /// Conflicting scope.
        scope: String,
        /// Conflicting branch entry IDs.
        branches: Vec<String>,
    },
    /// Graph exceeds the typed variable declaration bound.
    #[error("graph has {actual} variable declarations; maximum is {maximum}")]
    TooManyVariables {
        /// Actual number of declarations.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// A variable exceeds the bound for explicitly authorized parallel merge
    /// contributors.
    #[error("variable `{variable}` has {actual} merge contributors; maximum is {maximum}")]
    TooManyVariableMergeContributors {
        /// Variable name.
        variable: String,
        /// Actual contributor count.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// Duplicate variable declaration.
    #[error("duplicate variable `{variable}`")]
    DuplicateVariable {
        /// Duplicate variable name.
        variable: String,
    },
    /// Variable value maximum is invalid.
    #[error("variable `{variable}` maximum size {actual} is invalid; maximum is {maximum}")]
    InvalidVariableSize {
        /// Variable name.
        variable: String,
        /// Configured size.
        actual: u64,
        /// Compiler maximum.
        maximum: u64,
    },
    /// Variable type declaration is invalid.
    #[error("variable `{variable}` has invalid type: {detail}")]
    InvalidVariableType {
        /// Variable name.
        variable: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// Variable type nesting exceeds the compiler bound.
    #[error("variable `{variable}` type exceeds maximum nesting depth {maximum}")]
    VariableTypeTooDeep {
        /// Variable name.
        variable: String,
        /// Maximum nesting depth.
        maximum: usize,
    },
    /// Variable declares an unknown producer.
    #[error("variable `{variable}` declares unknown producer `{producer}`")]
    UnknownVariableProducer {
        /// Variable name.
        variable: String,
        /// Producer node identity.
        producer: String,
    },
    /// Variable declares an unknown additional parallel merge contributor.
    #[error("variable `{variable}` declares unknown merge contributor `{contributor}`")]
    UnknownVariableMergeContributor {
        /// Variable name.
        variable: String,
        /// Contributor node identity.
        contributor: String,
    },
    /// Variable declares an unknown consumer.
    #[error("variable `{variable}` declares unknown consumer `{consumer}`")]
    UnknownVariableConsumer {
        /// Variable name.
        variable: String,
        /// Consumer node identity.
        consumer: String,
    },
    /// Variable merge policy is not compatible with its scope.
    #[error("variable `{variable}` has invalid merge policy: {detail}")]
    InvalidVariableMergePolicy {
        /// Variable name.
        variable: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// Variable visibility is incompatible with its declared graph scope.
    #[error("variable `{variable}` has invalid scope: {detail}")]
    InvalidVariableScope {
        /// Variable name.
        variable: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// Secret-reference classification and value type do not agree.
    #[error("variable `{variable}` must use secret_reference type and classification together")]
    InvalidVariableSecurityClassification {
        /// Variable name.
        variable: String,
    },
    /// Node reads an undeclared variable.
    #[error("node `{node}` reads undeclared variable `{variable}`")]
    UndeclaredVariableRead {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
    },
    /// Node writes an undeclared variable.
    #[error("node `{node}` writes undeclared variable `{variable}`")]
    UndeclaredVariableWrite {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
    },
    /// Node is not an authorized consumer of a declared variable.
    #[error("node `{node}` is not a declared consumer of variable `{variable}`")]
    UnauthorizedVariableConsumer {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
    },
    /// Variable producer omitted the variable from its declared writes.
    #[error("variable `{variable}` producer `{producer}` does not declare the write")]
    VariableProducerDoesNotWrite {
        /// Variable name.
        variable: String,
        /// Producer node identity.
        producer: String,
    },
    /// A declared additional merge contributor omitted the variable from its
    /// writes.
    #[error("variable `{variable}` merge contributor `{contributor}` does not declare the write")]
    VariableMergeContributorDoesNotWrite {
        /// Variable name.
        variable: String,
        /// Contributor node identity.
        contributor: String,
    },
    /// A variable's additional merge-contributor declaration is structurally
    /// invalid.
    #[error("variable `{variable}` has invalid merge contributors: {detail}")]
    InvalidVariableMergeContributors {
        /// Variable name.
        variable: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// A node attempts to write a variable owned by another producer.
    #[error("node `{node}` writes variable `{variable}` owned by producer `{producer}`")]
    UnauthorizedVariableWriter {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
        /// Declared producer identity.
        producer: String,
    },
    /// A declared effect output cannot be represented by an ordinary value or
    /// one runtime-owned result slot.
    #[error("node `{node}` variable `{variable}` has invalid effect-output type: {detail}")]
    InvalidEffectOutputType {
        /// Producing node identity.
        node: String,
        /// Declared output variable.
        variable: String,
        /// Stable incompatibility detail.
        detail: &'static str,
    },
    /// A native node kind cannot produce the requested runtime-owned slot.
    #[error("node `{node}` variable `{variable}` requests unavailable effect slot `{slot}`")]
    EffectOutputSlotUnavailable {
        /// Producing node identity.
        node: String,
        /// Declared output variable.
        variable: String,
        /// Stable runtime slot name.
        slot: &'static str,
    },
    /// More than one variable consumes the same single-value result slot.
    #[error("node `{node}` variables {variables:?} duplicate effect slot `{slot}`")]
    DuplicateEffectOutputSlot {
        /// Producing node identity.
        node: String,
        /// Stable runtime slot name.
        slot: &'static str,
        /// Conflicting variables in stable name order.
        variables: Vec<String>,
    },
    /// A child-spawn result cannot project both singular and plural child slots.
    #[error("node `{node}` variables {variables:?} ambiguously request child and child-list slots")]
    AmbiguousChildEffectOutput {
        /// Producing child-spawn node.
        node: String,
        /// Conflicting variables in stable name order.
        variables: Vec<String>,
    },
    /// A configuration references a variable not declared in the node read set.
    #[error(
        "node `{node}` configuration reads variable `{variable}` without declaring it in read_variables"
    )]
    ConfigurationVariableNotDeclaredRead {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
    },
    /// A configuration variable has the wrong canonical type.
    #[error("node `{node}` configuration variable `{variable}` must have type `{expected}`")]
    ConfigurationVariableTypeMismatch {
        /// Node identity.
        node: String,
        /// Variable name.
        variable: String,
        /// Required type name.
        expected: &'static str,
    },
    /// Condition reads an undeclared canonical variable.
    #[error("condition `{owner}` reads undeclared variable `{variable}`")]
    UndeclaredConditionVariable {
        /// Node or edge identity.
        owner: String,
        /// Variable name.
        variable: String,
    },
    /// Condition variable is not declared in the source node's read set.
    #[error(
        "condition `{owner}` reads variable `{variable}` without declaring it in read_variables"
    )]
    ConditionVariableNotDeclaredRead {
        /// Node or edge identity.
        owner: String,
        /// Variable name.
        variable: String,
    },
    /// Configuration is incompatible with its node kind.
    #[error("node `{node}` configuration requires `{expected:?}` but node kind is `{actual:?}`")]
    ConfigurationKindMismatch {
        /// Node identity.
        node: String,
        /// Configuration's native node kind.
        expected: NodeKind,
        /// Actual node kind.
        actual: NodeKind,
    },
    /// Node configuration cannot be serialized canonically.
    #[error("node `{node}` has invalid configuration: {detail}")]
    InvalidNodeConfiguration {
        /// Node identity.
        node: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// Node configuration exceeds its total byte limit.
    #[error("node `{node}` configuration is {actual} bytes; maximum is {maximum}")]
    NodeConfigurationTooLarge {
        /// Node identity.
        node: String,
        /// Actual canonical byte size.
        actual: usize,
        /// Compiler maximum.
        maximum: usize,
    },
    /// A configuration collection exceeds its item bound.
    #[error("node `{node}` configuration field `{field}` has {actual} items; maximum is {maximum}")]
    ConfigurationCollectionTooLarge {
        /// Node identity.
        node: String,
        /// Field identity.
        field: String,
        /// Actual number of items.
        actual: usize,
        /// Compiler maximum.
        maximum: usize,
    },
    /// JSON configuration value exceeds its nesting bound.
    #[error("node `{node}` configuration field `{field}` exceeds maximum depth {maximum}")]
    NodeConfigurationTooDeep {
        /// Node identity.
        node: String,
        /// Field identity.
        field: String,
        /// Compiler maximum.
        maximum: usize,
    },
    /// Configured timestamp is not a bounded timestamp representation.
    #[error("invalid {kind} `{value}`")]
    InvalidTimestamp {
        /// Timestamp role.
        kind: &'static str,
        /// Invalid configured value.
        value: String,
    },
    /// Parallel configuration points to a non-join or unreachable join node.
    #[error("parallel node `{node}` has invalid join target `{join_target}`")]
    InvalidParallelJoinTarget {
        /// Parallel node identity.
        node: String,
        /// Configured target.
        join_target: String,
    },
    /// Parallel branches write the same canonical variable without a merge or serialization policy.
    #[error("parallel node `{node}` branches {branches:?} both write variable `{variable}`")]
    ConflictingParallelVariableWrites {
        /// Parallel node identity.
        node: String,
        /// Variable identity.
        variable: String,
        /// Conflicting branch entry IDs.
        branches: Vec<String>,
    },
    /// Parallel configuration names a variable that is not declared.
    #[error("parallel node `{node}` configures a merge for unknown variable `{variable}`")]
    UnknownParallelMergeVariable {
        /// Parallel node identity.
        node: String,
        /// Variable identity.
        variable: String,
    },
    /// Explicit variable merge contributors do not have one unambiguous
    /// compiler-proven parallel owner.
    #[error("variable `{variable}` has invalid parallel merge ownership: {detail}")]
    InvalidParallelMergeContributorOwnership {
        /// Variable identity.
        variable: String,
        /// Stable diagnostic detail.
        detail: String,
    },
    /// The variable declaration and its owning parallel node disagree about
    /// the deterministic merge operation.
    #[error(
        "variable `{variable}` merge policy `{declared:?}` does not match parallel node `{node}` policy {configured:?}"
    )]
    ParallelMergeContributorPolicyMismatch {
        /// Variable identity.
        variable: String,
        /// Owning parallel node identity.
        node: String,
        /// Variable declaration policy.
        declared: VariableMergePolicy,
        /// Parallel configuration policy, if declared.
        configured: Option<VariableMergePolicy>,
    },
    /// User-space event type is absent from graph declarations.
    #[error("node `{node}` uses undeclared event type `{event_type}`")]
    UndeclaredEventType {
        /// Node identity.
        node: String,
        /// Event type.
        event_type: String,
    },
    /// Plugin configuration names a kind different from the enclosing node.
    #[error(
        "plugin node `{node}` declares kind `{configured_kind:?}` but enclosing kind is `{actual_kind:?}`"
    )]
    InvalidPluginNodeKind {
        /// Node identity.
        node: String,
        /// Configured kind.
        configured_kind: NodeKind,
        /// Enclosing node kind.
        actual_kind: NodeKind,
    },
    /// Plugin is not explicitly allowed by the immutable graph declaration.
    #[error("node `{node}` invokes undeclared plugin `{plugin_id}`")]
    UndeclaredPlugin {
        /// Node identity.
        node: String,
        /// Plugin identity.
        plugin_id: String,
    },
    /// Inspection serialization failed.
    #[error("graph inspection serialization failed: {detail}")]
    Inspection {
        /// Serialization diagnostic.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
format_version = 1
entry = "plan"

[budget]
max_steps = 100
max_tokens = 10000
max_cost_micros = 500000
max_duration_ms = 60000

[declarations]
capabilities = ["model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "plan"
kind = "model_call"
provider = "mock"
condition = "session.ready == true"
retry_limit = 2

[[nodes]]
id = "read"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "plan"
to = "read"
condition = "model.tool_requested"

[[edges]]
from = "read"
to = "done"
"#;

    fn cache_inputs() -> GraphCacheInputs {
        GraphCacheInputs {
            plugin_set_hash: ContentHash::digest(b"plugins"),
            runtime_api_version: "1.0".into(),
            capability_set: ["context".to_owned(), "model".to_owned(), "tools".to_owned()]
                .into_iter()
                .collect(),
        }
    }

    fn compile_valid(source: &str) -> Result<ExecutableGraph, GraphError> {
        compile(source, &cache_inputs(), CompilerLimits::default())
    }

    fn configured_parallel_source() -> String {
        r#"
format_version = 1
entry = "parallel"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1000
[declarations]
capabilities = ["agents"]

[[nodes]]
id = "parallel"
kind = "parallel_branch"
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join", join_policy = "all" }
[[nodes]]
id = "left_entry"
kind = "conditional_branch"
[[nodes]]
id = "left_work"
kind = "wait_for_agents"
[[nodes]]
id = "right_entry"
kind = "wait_for_agents"
[[nodes]]
id = "join"
kind = "join_results"
configuration = { type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }
[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "parallel"
to = "left_entry"
label = "left-result"
[[edges]]
from = "parallel"
to = "right_entry"
label = "right-result"
[[edges]]
from = "left_entry"
to = "left_work"
[[edges]]
from = "left_work"
to = "join"
[[edges]]
from = "right_entry"
to = "join"
[[edges]]
from = "join"
to = "done"
"#
        .to_owned()
    }

    fn configured_parallel_merge_source() -> String {
        configured_parallel_source()
            .replace(
                "[[nodes]]\nid = \"parallel\"",
                r#"[[variables]]
name = "shared"
type = { kind = "list", item_type = { kind = "string" }, max_items = 8 }
scope = "run"
producer = "left_entry"
merge_contributors = ["right_entry"]
consumers = ["join"]
mutability = "mutable"
merge_policy = "append"
max_size_bytes = 2048
security_classification = "internal"

[[nodes]]
id = "parallel""#,
            )
            .replace(
                "join_target = \"join\", join_policy = \"all\" }",
                "join_target = \"join\", join_policy = \"all\", variable_merge_policies = { shared = \"append\" } }",
            )
            .replace(
                "id = \"left_entry\"\nkind = \"conditional_branch\"",
                "id = \"left_entry\"\nkind = \"conditional_branch\"\nwrite_variables = [\"shared\"]",
            )
            .replace(
                "id = \"right_entry\"\nkind = \"wait_for_agents\"",
                "id = \"right_entry\"\nkind = \"wait_for_agents\"\nwrite_variables = [\"shared\"]",
            )
    }

    fn compile_configured_parallel(source: &str) -> Result<ExecutableGraph, GraphError> {
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("agents".into());
        compile(source, &inputs, CompilerLimits::default())
    }

    fn configured_child_graph() -> String {
        r#"
format_version = 1
entry = "commission"
[budget]
max_steps = 100
max_tokens = 10000
max_cost_micros = 500000
max_duration_ms = 60000
[declarations]
capabilities = ["agents", "model"]
providers = ["mock"]

[[variables]]
name = "assignments"
type = { kind = "map", value_type = { kind = "string" }, max_entries = 4 }
scope = "run"
producer = "runtime"
consumers = ["commission"]
mutability = "immutable"
max_size_bytes = 4096
security_classification = "internal"

[[variables]]
name = "worker_ids"
type = { kind = "list", item_type = { kind = "child_id" }, max_items = 4 }
scope = "run"
producer = "runtime"
consumers = ["rendezvous"]
mutability = "mutable"
max_size_bytes = 4096
security_classification = "internal"

[[variables]]
name = "integration"
type = { kind = "node_result_reference" }
scope = "run"
producer = "runtime"
consumers = ["quality-gate"]
mutability = "mutable"
max_size_bytes = 1024
security_classification = "internal"

[[nodes]]
id = "commission"
kind = "spawn_child_agent"
read_variables = ["assignments"]
configuration = { type = "spawn_child_agent", task_input = { kind = "variable", variable = "assignments" }, task_id_prefix = "work", child_style = "worker@1.0.0", tool_groups = ["filesystem.read"], maximum_children = 4, maximum_depth = 2, token_budget = 1000, context_budget_tokens = 500, cost_budget_micros = 10000, workspace = { mode = "shared_read_only" }, artifact_references = ["artifact-brief"], security_classification = "internal", approval_required = true }

[[nodes]]
id = "rendezvous"
kind = "wait_for_agents"
read_variables = ["worker_ids"]
configuration = { type = "wait_for_agents", children = { kind = "variable", variable = "worker_ids" }, maximum_children = 4, minimum_successes = 2, timeout_ms = 30000, cancellation = "cascade" }

[[nodes]]
id = "quality-gate"
kind = "review"
provider = "mock"
read_variables = ["integration"]
configuration = { type = "review", input = { kind = "variable", variable = "integration" }, artifact_references = ["artifact-evidence"], result_schema = { maximum_findings = 8, maximum_finding_bytes = 512, maximum_rejections = 4, require_artifact_evidence = true }, routes = { approved = "accepted", revision = "revise", failure = "rejected" }, maximum_revisions = 3 }

[[nodes]]
id = "revise"
kind = "loop"
max_iterations = 3

[[nodes]]
id = "accepted"
kind = "complete_session"

[[nodes]]
id = "rejected"
kind = "fail"

[[edges]]
from = "commission"
to = "rendezvous"
[[edges]]
from = "rendezvous"
to = "quality-gate"
[[edges]]
from = "quality-gate"
to = "accepted"
[[edges]]
from = "quality-gate"
to = "revise"
[[edges]]
from = "quality-gate"
to = "rejected"
[[edges]]
from = "revise"
to = "commission"
"#
        .to_owned()
    }

    fn compile_configured_child_graph(source: &str) -> Result<ExecutableGraph, GraphError> {
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("agents".into());
        compile(source, &inputs, CompilerLimits::default())
    }

    #[test]
    fn valid_graph_compiles_to_stable_sorted_representation() {
        let graph = compile_valid(VALID).expect("compile");
        assert_eq!(
            graph
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["done", "plan", "read"]
        );
        assert_eq!(graph.entry_index, 1);
        let first = graph.inspect_json().expect("inspection");
        let second = compile_valid(VALID)
            .expect("compile again")
            .inspect_json()
            .expect("inspection");
        assert_eq!(first, second);
        assert!(first.contains(r#""graph_content_hash""#));
        assert!(first.contains(r#""expression": "compare""#));
    }

    #[test]
    fn cache_key_changes_for_each_compatibility_input() {
        let baseline = compile_valid(VALID).expect("baseline").cache_key;
        let changed_source = compile_valid(&VALID.replace("max_steps = 100", "max_steps = 101"))
            .expect("source")
            .cache_key;
        assert_ne!(baseline.combined_hash, changed_source.combined_hash);

        let mut inputs = cache_inputs();
        inputs.plugin_set_hash = ContentHash::digest(b"other plugins");
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("plugins")
                .cache_key
                .combined_hash
        );

        let mut inputs = cache_inputs();
        inputs.runtime_api_version = "1.1".into();
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("runtime")
                .cache_key
                .combined_hash
        );

        let mut inputs = cache_inputs();
        inputs.capability_set.insert("extra".into());
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("capabilities")
                .cache_key
                .combined_hash
        );
    }

    #[test]
    fn rejects_version_and_size_bounds() {
        assert!(matches!(
            compile_valid(&VALID.replace("format_version = 1", "format_version = 2")),
            Err(GraphError::UnsupportedVersion { .. })
        ));
        let limits = CompilerLimits {
            max_source_bytes: 4,
            ..CompilerLimits::default()
        };
        assert!(matches!(
            compile(VALID, &cache_inputs(), limits),
            Err(GraphError::SourceTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_and_unknown_node_families() {
        let duplicate = VALID.replace(
            "[[nodes]]\nid = \"done\"",
            "[[nodes]]\nid = \"plan\"\nkind = \"complete_session\"\n\n[[nodes]]\nid = \"done\"",
        );
        assert!(matches!(
            compile_valid(&duplicate),
            Err(GraphError::DuplicateNode { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("entry = \"plan\"", "entry = \"missing\"")),
            Err(GraphError::UnknownEntry { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("to = \"read\"", "to = \"missing\"")),
            Err(GraphError::UnknownEdgeNode { .. })
        ));
    }

    #[test]
    fn rejects_unreachable_and_termination_families() {
        let unreachable = VALID.replace(
            "[[nodes]]\nid = \"done\"",
            "[[nodes]]\nid = \"orphan\"\nkind = \"complete_turn\"\n\n[[nodes]]\nid = \"done\"",
        );
        assert!(matches!(
            compile_valid(&unreachable),
            Err(GraphError::UnreachableNodes { .. })
        ));
        let no_terminal = VALID.replace("kind = \"complete_session\"", "kind = \"emit_event\"");
        assert!(matches!(
            compile_valid(&no_terminal),
            Err(GraphError::MissingTermination)
        ));
        let terminal_edge = format!("{VALID}\n[[edges]]\nfrom = \"done\"\nto = \"plan\"\n");
        assert!(matches!(
            compile_valid(&terminal_edge),
            Err(GraphError::TerminalHasOutgoingEdge { .. })
        ));
    }

    #[test]
    fn rejects_retry_and_budget_families() {
        assert!(matches!(
            compile_valid(&VALID.replace("retry_limit = 2", "retry_limit = 99")),
            Err(GraphError::RetryLimitExceeded { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("max_steps = 100", "max_steps = 0")),
            Err(GraphError::InvalidBudget { .. })
        ));
    }

    #[test]
    fn rejects_capability_tool_and_provider_families() {
        let capability = VALID.replace(
            "condition = \"session.ready == true\"",
            "required_capabilities = [\"browser\"]\ncondition = \"session.ready == true\"",
        );
        assert!(matches!(
            compile_valid(&capability),
            Err(GraphError::UndeclaredCapability { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("tool = \"filesystem.read\"", "tool = \"other\"")),
            Err(GraphError::UndeclaredTool { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("provider = \"mock\"", "provider = \"other\"")),
            Err(GraphError::UndeclaredProvider { .. })
        ));
        let mut inputs = cache_inputs();
        inputs.capability_set.remove("model");
        assert!(matches!(
            compile(VALID, &inputs, CompilerLimits::default()),
            Err(GraphError::RuntimeCapabilityUnavailable { .. })
        ));
    }

    #[test]
    fn rejects_invalid_embedded_condition() {
        assert!(matches!(
            compile_valid(&VALID.replace("session.ready == true", "session.ready = true")),
            Err(GraphError::InvalidCondition { .. })
        ));
    }

    #[test]
    fn legacy_provider_conditions_remain_valid_until_a_graph_opts_into_variables() {
        let legacy = VALID.replace("session.ready == true", "request.ready == true");
        assert!(compile_valid(&legacy).is_ok());
    }

    #[test]
    fn rfc3339_schedule_and_delay_timestamps_are_strictly_bounded() {
        let limits = CompilerLimits::default();
        assert!(validate_timestamp("test", "2024-02-29T23:59:60.1+05:30", limits).is_ok());
        assert!(matches!(
            validate_timestamp("test", "2023-02-29T23:59:00Z", limits),
            Err(GraphError::InvalidTimestamp { .. })
        ));
        assert!(matches!(
            validate_timestamp("test", "2024-01-01 00:00:00Z", limits),
            Err(GraphError::InvalidTimestamp { .. })
        ));
    }

    #[test]
    fn cycles_require_a_statically_bounded_loop_node() {
        let illegal = VALID.replace(
            "[[edges]]\nfrom = \"read\"\nto = \"done\"",
            "[[edges]]\nfrom = \"read\"\nto = \"plan\"\n\n[[edges]]\nfrom = \"read\"\nto = \"done\"",
        );
        assert!(matches!(
            compile_valid(&illegal),
            Err(GraphError::IllegalCycle { .. })
        ));

        let bounded = r#"
format_version = 1
entry = "loop"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[[nodes]]
id = "loop"
kind = "loop"
max_iterations = 3
[[nodes]]
id = "work"
kind = "conditional_branch"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "loop"
to = "work"
[[edges]]
from = "work"
to = "loop"
[[edges]]
from = "loop"
to = "done"
"#;
        assert!(compile_valid(bounded).is_ok());
        assert!(matches!(
            compile_valid(&bounded.replace("max_iterations = 3", "max_iterations = 0")),
            Err(GraphError::InvalidLoopBound { .. })
        ));
    }

    #[test]
    fn rejects_conflicting_parallel_write_scopes() {
        let parallel = r#"
format_version = 1
entry = "parallel"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[declarations]
capabilities = ["agents"]
[[nodes]]
id = "parallel"
kind = "parallel_branch"
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join", join_policy = "all" }
[[nodes]]
id = "left"
kind = "wait_for_agents"
write_scopes = ["workspace"]
[[nodes]]
id = "right"
kind = "wait_for_agents"
write_scopes = ["workspace"]
[[nodes]]
id = "join"
kind = "join_results"
configuration = { type = "join_results", required = ["left-result", "right-result"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "parallel"
to = "left"
label = "left-result"
[[edges]]
from = "parallel"
to = "right"
label = "right-result"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("agents".into());
        assert!(matches!(
            compile(parallel, &inputs, CompilerLimits::default()),
            Err(GraphError::ConflictingParallelWrites { .. })
        ));
        assert!(
            compile(
                &parallel.replacen(
                    "write_scopes = [\"workspace\"]",
                    "write_scopes = [\"context\"]",
                    1
                ),
                &inputs,
                CompilerLimits::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn configured_parallel_topology_binds_labels_to_join_members() {
        let source = configured_parallel_source();
        assert!(compile_configured_parallel(&source).is_ok());

        let mismatched = source.replace(
            r#"required = ["left-result", "right-result"]"#,
            r#"required = ["left-result", "substituted"]"#,
        );
        assert!(matches!(
            compile_configured_parallel(&mismatched),
            Err(GraphError::ParallelJoinMemberMismatch { .. })
        ));

        let duplicate_reference =
            source.replacen(r#"label = "right-result""#, r#"label = "left-result""#, 1);
        assert!(matches!(
            compile_configured_parallel(&duplicate_reference),
            Err(GraphError::DuplicateParallelMemberReference { .. })
        ));
    }

    #[test]
    fn configured_parallel_fanout_must_be_unconditional_labeled_and_unique() {
        let source = configured_parallel_source();
        let unlabeled = source.replacen("label = \"left-result\"\n", "", 1);
        assert!(matches!(
            compile_configured_parallel(&unlabeled),
            Err(GraphError::UnlabeledParallelFanout { .. })
        ));

        let conditional = source.replacen(
            "label = \"left-result\"",
            "label = \"left-result\"\ncondition = \"session.ready == true\"",
            1,
        );
        assert!(matches!(
            compile_configured_parallel(&conditional),
            Err(GraphError::ConditionalParallelFanout { .. })
        ));

        let duplicate_target = source
            .replace(
                "from = \"parallel\"\nto = \"right_entry\"\nlabel = \"right-result\"",
                "from = \"parallel\"\nto = \"left_entry\"\nlabel = \"right-result\"",
            )
            .replace(
                "from = \"left_entry\"\nto = \"left_work\"",
                "from = \"left_entry\"\nto = \"left_work\"\n\n[[edges]]\nfrom = \"left_entry\"\nto = \"right_entry\"",
            );
        assert!(matches!(
            compile_configured_parallel(&duplicate_target),
            Err(GraphError::DuplicateParallelBranchTarget { .. })
        ));
    }

    #[test]
    fn configured_parallel_regions_reject_overlap_and_cross_branch_edges() {
        let source = configured_parallel_source();
        let overlap = source.replace(
            "from = \"left_work\"\nto = \"join\"",
            "from = \"left_work\"\nto = \"right_entry\"",
        );
        assert!(matches!(
            compile_configured_parallel(&overlap),
            Err(GraphError::OverlappingParallelBranchRegions { .. })
        ));
    }

    #[test]
    fn configured_parallel_regions_reject_outside_entry() {
        let source = configured_parallel_source()
            .replace("entry = \"parallel\"", "entry = \"start\"")
            .replace(
                "[[nodes]]\nid = \"parallel\"",
                "[[nodes]]\nid = \"start\"\nkind = \"conditional_branch\"\n\n[[nodes]]\nid = \"parallel\"",
            )
            .replacen(
                "[[edges]]\nfrom = \"parallel\"",
                "[[edges]]\nfrom = \"start\"\nto = \"parallel\"\n\n[[edges]]\nfrom = \"start\"\nto = \"left_work\"\n\n[[edges]]\nfrom = \"parallel\"",
                1,
            );
        let error = compile_configured_parallel(&source).expect_err("outside entry must fail");
        assert!(
            matches!(error, GraphError::ExternalParallelBranchEntry { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn configured_parallel_regions_reject_join_bypass_and_terminal_completion() {
        let source = configured_parallel_source()
            .replace(
                "[[nodes]]\nid = \"done\"",
                "[[nodes]]\nid = \"left_done\"\nkind = \"complete_session\"\n\n[[nodes]]\nid = \"done\"",
            )
            .replace(
                "from = \"left_work\"\nto = \"join\"",
                "from = \"left_work\"\nto = \"left_done\"",
            );
        assert!(matches!(
            compile_configured_parallel(&source),
            Err(GraphError::ParallelBranchTerminalCompletion { .. })
        ));
    }

    #[test]
    fn configured_parallel_join_policy_and_threshold_must_agree() {
        let source = configured_parallel_source()
            .replace("join_policy = \"all\"", "join_policy = \"minimum_success\"");
        assert!(matches!(
            compile_configured_parallel(&source),
            Err(GraphError::ParallelJoinPolicyMismatch { .. })
        ));
        let compatible = source.replace(
            "failure_policy = \"wait_required\"",
            "failure_policy = \"minimum_success\"",
        );
        assert!(compile_configured_parallel(&compatible).is_ok());
    }

    #[test]
    fn explicit_parallel_merge_contributors_compile_canonically() {
        let source = configured_parallel_merge_source();
        let graph = compile_configured_parallel(&source).expect("explicit contributors compile");
        let declaration = graph
            .variables
            .iter()
            .find(|variable| variable.name == "shared")
            .expect("shared variable");
        assert_eq!(declaration.producer, "left_entry");
        assert_eq!(
            declaration.merge_contributors,
            BTreeSet::from(["right_entry".to_owned()])
        );
        assert_eq!(declaration.merge_policy, Some(VariableMergePolicy::Append));
        let first = graph.inspect_json().expect("inspect");
        let second = compile_configured_parallel(&source)
            .expect("recompile")
            .inspect_json()
            .expect("inspect again");
        assert_eq!(first, second);
    }

    #[test]
    fn parallel_merge_writers_require_explicit_contributor_authorization() {
        let source = configured_parallel_merge_source()
            .replace("merge_contributors = [\"right_entry\"]\n", "");
        assert!(matches!(
            compile_configured_parallel(&source),
            Err(GraphError::UnauthorizedVariableWriter {
                ref node,
                ref variable,
                ..
            }) if node == "right_entry" && variable == "shared"
        ));

        let source = configured_parallel_merge_source();
        let producer_does_not_write = source.replace(
            "id = \"left_entry\"\nkind = \"conditional_branch\"\nwrite_variables = [\"shared\"]",
            "id = \"left_entry\"\nkind = \"conditional_branch\"",
        );
        assert!(matches!(
            compile_configured_parallel(&producer_does_not_write),
            Err(GraphError::VariableProducerDoesNotWrite {
                ref variable,
                ref producer,
            }) if variable == "shared" && producer == "left_entry"
        ));

        let contributor_does_not_write = source.replace(
            "id = \"right_entry\"\nkind = \"wait_for_agents\"\nwrite_variables = [\"shared\"]",
            "id = \"right_entry\"\nkind = \"wait_for_agents\"",
        );
        assert!(matches!(
            compile_configured_parallel(&contributor_does_not_write),
            Err(GraphError::VariableMergeContributorDoesNotWrite {
                ref variable,
                ref contributor,
            }) if variable == "shared" && contributor == "right_entry"
        ));
    }

    #[test]
    fn parallel_merge_contributors_must_share_one_exact_parallel_owner() {
        let source = configured_parallel_merge_source()
            .replace("entry = \"parallel\"", "entry = \"outside\"")
            .replace(
                "merge_contributors = [\"right_entry\"]",
                "merge_contributors = [\"outside\"]",
            )
            .replace(
                "[[nodes]]\nid = \"parallel\"",
                "[[nodes]]\nid = \"outside\"\nkind = \"conditional_branch\"\nwrite_variables = [\"shared\"]\n\n[[nodes]]\nid = \"parallel\"",
            )
            .replace(
                "id = \"right_entry\"\nkind = \"wait_for_agents\"\nwrite_variables = [\"shared\"]",
                "id = \"right_entry\"\nkind = \"wait_for_agents\"",
            )
            .replacen(
                "[[edges]]\nfrom = \"parallel\"",
                "[[edges]]\nfrom = \"outside\"\nto = \"parallel\"\n\n[[edges]]\nfrom = \"parallel\"",
                1,
            );
        assert!(matches!(
            compile_configured_parallel(&source),
            Err(GraphError::InvalidParallelMergeContributorOwnership {
                ref variable,
                ..
            }) if variable == "shared"
        ));
    }

    #[test]
    fn parallel_merge_contributors_require_exact_owner_policy() {
        let source = configured_parallel_merge_source();
        let missing = source.replace(", variable_merge_policies = { shared = \"append\" }", "");
        assert!(matches!(
            compile_configured_parallel(&missing),
            Err(GraphError::ParallelMergeContributorPolicyMismatch {
                ref variable,
                configured: None,
                ..
            }) if variable == "shared"
        ));

        let mismatched = source.replace(
            "variable_merge_policies = { shared = \"append\" }",
            "variable_merge_policies = { shared = \"first_branch\" }",
        );
        assert!(matches!(
            compile_configured_parallel(&mismatched),
            Err(GraphError::ParallelMergeContributorPolicyMismatch {
                ref variable,
                configured: Some(VariableMergePolicy::FirstBranch),
                ..
            }) if variable == "shared"
        ));
    }

    #[test]
    fn node_and_branch_scoped_parallel_merge_contributors_are_rejected() {
        for scope in ["node", "branch"] {
            let source = configured_parallel_merge_source()
                .replace("scope = \"run\"", &format!("scope = \"{scope}\""));
            assert!(matches!(
                compile_configured_parallel(&source),
                Err(GraphError::InvalidVariableMergeContributors {
                    ref variable,
                    ..
                }) if variable == "shared"
            ));
        }
    }

    #[test]
    fn parallel_merge_contributor_names_and_collection_are_bounded() {
        let invalid_name = configured_parallel_merge_source().replace(
            "merge_contributors = [\"right_entry\"]",
            "merge_contributors = [\"invalid contributor\"]",
        );
        assert!(matches!(
            compile_configured_parallel(&invalid_name),
            Err(GraphError::InvalidName {
                kind: "variable merge contributor",
                ..
            })
        ));

        let source = configured_parallel_merge_source();
        let definition =
            GraphDefinition::parse(&source, CompilerLimits::default()).expect("parse graph");
        let nodes = collect_nodes(&definition).expect("nodes");
        let variables = collect_variables(&definition).expect("variables");
        let limits = CompilerLimits {
            max_configuration_items: 0,
            ..CompilerLimits::default()
        };
        assert!(matches!(
            validate_variables(&definition, &nodes, &variables, limits),
            Err(GraphError::TooManyVariableMergeContributors {
                ref variable,
                actual: 1,
                maximum: 0,
            }) if variable == "shared"
        ));
    }

    #[test]
    fn typed_variables_and_node_configuration_are_canonical_and_enforced() {
        let source = r#"
format_version = 1
entry = "produce"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[declarations]
capabilities = ["agents"]

[[variables]]
name = "child"
type = { kind = "child_id" }
scope = "run"
producer = "produce"
consumers = ["message"]
mutability = "immutable"
max_size_bytes = 64
security_classification = "internal"

[[nodes]]
id = "produce"
kind = "spawn_child_agent"
write_variables = ["child"]
configuration = { type = "spawn_child_agent", task_input = { kind = "static", value = "task" }, task_id_prefix = "work", child_style = "worker@1.0.0", maximum_children = 1, maximum_depth = 1, token_budget = 1, context_budget_tokens = 1, cost_budget_micros = 1, workspace = { mode = "shared_read_only" }, security_classification = "internal", approval_required = true }

[[nodes]]
id = "message"
kind = "send_child_agent_message"
read_variables = ["child"]
configuration = { type = "send_child_agent_message", child = { kind = "variable", variable = "child" }, payload = { z = 1, a = 2 }, security_classification = "internal", max_message_bytes = 64, cancellation = "reject" }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "produce"
to = "message"
[[edges]]
from = "message"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("agents".into());
        let graph =
            compile(source, &inputs, CompilerLimits::default()).expect("compile configured graph");
        assert_eq!(graph.variables[0].name, "child");
        let message = graph
            .nodes
            .iter()
            .find(|node| node.id == "message")
            .expect("message node");
        let configuration = message.configuration.as_ref().expect("configuration");
        let NodeConfiguration::SendChildAgentMessage { payload, .. } = configuration else {
            panic!("wrong configuration");
        };
        assert_eq!(
            canonical_json_bytes(payload).expect("payload"),
            br#"{"a":2,"z":1}"#
        );

        let wrong_writer = source.replace("write_variables = [\"child\"]", "write_variables = []");
        assert!(matches!(
            compile(&wrong_writer, &inputs, CompilerLimits::default()),
            Err(GraphError::VariableProducerDoesNotWrite { .. })
        ));
        let unauthorized_writer = source.replace(
            "[[nodes]]\nid = \"message\"\nkind = \"send_child_agent_message\"",
            "[[nodes]]\nid = \"message\"\nkind = \"send_child_agent_message\"\nwrite_variables = [\"child\"]",
        );
        assert!(matches!(
            compile(&unauthorized_writer, &inputs, CompilerLimits::default()),
            Err(GraphError::UnauthorizedVariableWriter { .. })
        ));

        let mismatched = source
            .replace(
                "kind = \"send_child_agent_message\"\nread_variables",
                "kind = \"emit_event\"\nread_variables",
            )
            .replace(
                "capabilities = [\"agents\"]",
                "capabilities = [\"agents\", \"events\"]\nevents = [\"graph.notice\"]",
            );
        inputs.capability_set.insert("events".into());
        assert!(matches!(
            compile(&mismatched, &inputs, CompilerLimits::default()),
            Err(GraphError::ConfigurationKindMismatch { .. })
        ));
    }

    #[test]
    fn native_effect_output_slots_reject_duplicates_and_incompatible_types() {
        let source = r#"
format_version = 1
entry = "effect"
[budget]
max_steps = 10
max_tokens = 10
max_cost_micros = 10
max_duration_ms = 1000
[declarations]
capabilities = ["tools"]
tools = ["filesystem.read"]

[[variables]]
name = "tool_result"
type = { kind = "tool_result_reference" }
scope = "run"
producer = "effect"
consumers = ["done"]
mutability = "immutable"
max_size_bytes = 256
security_classification = "internal"

[[variables]]
name = "label"
type = { kind = "string" }
scope = "run"
producer = "effect"
consumers = ["done"]
mutability = "immutable"
max_size_bytes = 256
security_classification = "internal"

[[nodes]]
id = "effect"
kind = "tool_execution_gate"
tool = "filesystem.read"
write_variables = ["label", "tool_result"]

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "effect"
to = "done"
"#;
        assert!(compile_valid(source).is_ok());

        let duplicate = source
            .replace(
                "[[nodes]]\nid = \"effect\"",
                r#"[[variables]]
name = "second_tool_result"
type = { kind = "tool_result_reference" }
scope = "run"
producer = "effect"
consumers = ["done"]
mutability = "immutable"
max_size_bytes = 256
security_classification = "internal"

[[nodes]]
id = "effect""#,
            )
            .replace(
                "write_variables = [\"label\", \"tool_result\"]",
                "write_variables = [\"label\", \"second_tool_result\", \"tool_result\"]",
            );
        assert!(matches!(
            compile_valid(&duplicate),
            Err(GraphError::DuplicateEffectOutputSlot {
                slot: "tool_result_reference",
                ..
            })
        ));

        let incompatible = source
            .replace(
                "type = { kind = \"tool_result_reference\" }",
                "type = { kind = \"approval_result\" }",
            )
            .replace("name = \"tool_result\"", "name = \"approval\"")
            .replace("\"tool_result\"]", "\"approval\"]");
        assert!(matches!(
            compile_valid(&incompatible),
            Err(GraphError::EffectOutputSlotUnavailable {
                slot: "approval_result",
                ..
            })
        ));
    }

    #[test]
    fn child_effect_output_rejects_singular_plural_ambiguity() {
        let source = r#"
format_version = 1
entry = "spawn"
[budget]
max_steps = 10
max_tokens = 10
max_cost_micros = 10
max_duration_ms = 1000
[declarations]
capabilities = ["agents"]

[[variables]]
name = "child"
type = { kind = "child_id" }
scope = "run"
producer = "spawn"
consumers = ["done"]
mutability = "immutable"
max_size_bytes = 256
security_classification = "internal"

[[variables]]
name = "children"
type = { kind = "list", item_type = { kind = "child_id" }, max_items = 4 }
scope = "run"
producer = "spawn"
consumers = ["done"]
mutability = "immutable"
max_size_bytes = 1024
security_classification = "internal"

[[nodes]]
id = "spawn"
kind = "spawn_child_agent"
write_variables = ["child", "children"]
configuration = { type = "spawn_child_agent", task_input = { kind = "static", value = "task" }, task_id_prefix = "work", child_style = "worker@1.0.0", maximum_children = 4, maximum_depth = 1, token_budget = 1, context_budget_tokens = 1, cost_budget_micros = 1, workspace = { mode = "shared_read_only" }, security_classification = "internal", approval_required = true }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "spawn"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs.capability_set.insert(String::from("agents"));
        assert!(matches!(
            compile(source, &inputs, CompilerLimits::default()),
            Err(GraphError::AmbiguousChildEffectOutput { .. })
        ));
    }

    #[test]
    fn parallel_branch_ordinary_output_preserves_declared_merge_compatibility() {
        let source = configured_parallel_source()
            .replace(
                "[[nodes]]\nid = \"parallel\"",
                r#"[[variables]]
name = "left_batch"
type = { kind = "list", item_type = { kind = "string" }, max_items = 4 }
scope = "run"
producer = "left_entry"
consumers = ["join"]
mutability = "mutable"
merge_policy = "append"
max_size_bytes = 1024
security_classification = "internal"

[[nodes]]
id = "parallel""#,
            )
            .replace(
                "id = \"left_entry\"\nkind = \"conditional_branch\"",
                "id = \"left_entry\"\nkind = \"conditional_branch\"\nwrite_variables = [\"left_batch\"]",
            );
        let graph = compile_configured_parallel(&source).expect("parallel output graph");
        let left = graph
            .nodes
            .iter()
            .find(|node| node.id == "left_entry")
            .expect("left branch");
        assert_eq!(left.write_variables, BTreeSet::from(["left_batch".into()]));
        assert_eq!(
            graph
                .variables
                .iter()
                .find(|variable| variable.name == "left_batch")
                .and_then(|variable| variable.merge_policy),
            Some(VariableMergePolicy::Append)
        );
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table-shaped test keeps every artifact configuration rejection and legacy compatibility assertion adjacent"
    )]
    fn artifact_configuration_rejects_implicit_content_secret_references_and_invalid_mime_types() {
        let source = r#"
format_version = 1
entry = "produce"
[budget]
max_steps = 10
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[declarations]
capabilities = ["artifacts"]

[[variables]]
name = "secret"
type = { kind = "secret_reference" }
scope = "run"
producer = "produce"
consumers = ["persist"]
mutability = "immutable"
max_size_bytes = 128
security_classification = "secret_reference"

[[nodes]]
id = "produce"
kind = "conditional_branch"
write_variables = ["secret"]
[[nodes]]
id = "persist"
kind = "persist_artifact"
read_variables = ["secret"]
configuration = { type = "persist_artifact", content = { kind = "variable", variable = "secret" }, mime_type = "application/json", security = "private", retention = "session" }
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "produce"
to = "persist"
[[edges]]
from = "persist"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("artifacts".into());
        assert!(matches!(
            compile(source, &inputs, CompilerLimits::default()),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let static_content = source
            .replace(
                r#"content = { kind = "variable", variable = "secret" }"#,
                r#"content = { kind = "static_text", value = "bounded" }"#,
            )
            .replace(
                "mime_type = \"application/json\"",
                "mime_type = \"text/plain\\r\\nforged\"",
            );
        assert!(matches!(
            compile(&static_content, &inputs, CompilerLimits::default()),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let missing_configuration = source.replace(
            "configuration = { type = \"persist_artifact\", content = { kind = \"variable\", variable = \"secret\" }, mime_type = \"application/json\", security = \"private\", retention = \"session\" }\n",
            "",
        );
        assert!(matches!(
            compile(
                &missing_configuration,
                &inputs,
                CompilerLimits::default()
            ),
            Err(GraphError::InvalidNodeConfiguration { ref detail, .. })
                if detail.contains("explicit bounded content")
        ));
        let legacy_limits = CompilerLimits {
            allow_legacy_unconfigured_artifact_persistence: true,
            ..CompilerLimits::default()
        };
        let legacy_source = r#"
format_version = 1
entry = "persist"
[budget]
max_steps = 2
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
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
"#;
        let legacy = compile(legacy_source, &inputs, legacy_limits)
            .expect("exact historical graph compatibility");
        assert!(
            legacy
                .nodes
                .iter()
                .find(|node| node.id == "persist")
                .expect("persist node")
                .configuration
                .is_none()
        );
    }

    #[test]
    fn provider_result_artifact_source_requires_an_exact_node_result_reference() {
        let source = r#"
format_version = 1
entry = "model"
[budget]
max_steps = 3
max_tokens = 10
max_cost_micros = 10
max_duration_ms = 10
[declarations]
capabilities = ["artifacts", "model"]
providers = ["mock"]

[[variables]]
name = "disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "model"
consumers = []
mutability = "immutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "receipt"
type = { kind = "node_result_reference" }
scope = "run"
producer = "model"
consumers = ["persist"]
mutability = "immutable"
max_size_bytes = 512
security_classification = "internal"

[[nodes]]
id = "model"
kind = "model_call"
provider = "mock"
write_variables = ["disposition", "receipt"]
configuration = { type = "model_request", disposition_output = "disposition", result_output = "receipt" }

[[nodes]]
id = "persist"
kind = "persist_artifact"
read_variables = ["receipt"]
configuration = { type = "persist_artifact", content = { kind = "provider_result_text", reference_variable = "receipt" }, mime_type = "text/markdown", security = "private", retention = "session" }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "model"
to = "persist"
[[edges]]
from = "persist"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs
            .capability_set
            .extend(["artifacts".into(), "model".into()]);
        let compiled =
            compile(source, &inputs, CompilerLimits::default()).expect("provider-result artifact");
        assert!(matches!(
            compiled
                .nodes
                .iter()
                .find(|node| node.id == "persist")
                .and_then(|node| node.configuration.as_ref()),
            Some(NodeConfiguration::PersistArtifact {
                content: ArtifactContentSource::ProviderResultText { reference_variable },
                ..
            }) if reference_variable == "receipt"
        ));

        let mistyped = source.replace(
            r#"name = "receipt"
type = { kind = "node_result_reference" }"#,
            r#"name = "receipt"
type = { kind = "string" }"#,
        );
        let error = compile(&mistyped, &inputs, CompilerLimits::default())
            .expect_err("provider-result content must reject a mistyped reference");
        assert!(
            matches!(error, GraphError::ConfigurationVariableTypeMismatch { .. }),
            "unexpected compiler error: {error:?}"
        );
    }

    #[test]
    fn variable_scope_merge_and_secret_reference_contracts_are_enforced() {
        let node_scoped = r#"
format_version = 1
entry = "produce"
[budget]
max_steps = 10
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[declarations]

[[variables]]
name = "private"
type = { kind = "string" }
scope = "node"
producer = "produce"
consumers = ["consume"]
mutability = "immutable"
max_size_bytes = 64
security_classification = "internal"

[[nodes]]
id = "produce"
kind = "conditional_branch"
write_variables = ["private"]
[[nodes]]
id = "consume"
kind = "conditional_branch"
read_variables = ["private"]
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "produce"
to = "consume"
[[edges]]
from = "consume"
to = "done"
"#;
        assert!(matches!(
            compile_valid(node_scoped),
            Err(GraphError::InvalidVariableScope { .. })
        ));

        let mut variable = VariableDeclaration {
            name: "merged".into(),
            value_type: VariableValueType::String,
            scope: VariableScope::Run,
            producer: "runtime".into(),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: VariableMutability::Mutable,
            merge_policy: Some(VariableMergePolicy::Append),
            max_size_bytes: 64,
            security_classification: SecurityClassification::Internal,
        };
        assert!(matches!(
            validate_variable_merge_policy(&variable),
            Err(GraphError::InvalidVariableMergePolicy { .. })
        ));

        variable.merge_policy = None;
        variable.value_type = VariableValueType::SecretReference;
        assert!(matches!(
            validate_variable_security(&variable),
            Err(GraphError::InvalidVariableSecurityClassification { .. })
        ));
        variable.security_classification = SecurityClassification::SecretReference;
        assert!(validate_variable_security(&variable).is_ok());
    }

    #[test]
    fn native_and_plugin_configuration_schema_round_trip() {
        let configurations = vec![
            NodeConfiguration::SendChildAgentMessage {
                child: ChildSelector::Exact {
                    child_id: "child-1".into(),
                },
                payload: serde_json::json!({"message": "hello"}),
                artifact_references: ["artifact-1".to_owned()].into_iter().collect(),
                security_classification: SecurityClassification::Internal,
                max_message_bytes: 128,
                cancellation: ChildMessageCancellation::DeliverIfRunning,
            },
            NodeConfiguration::JoinResults {
                required: ["child-1".to_owned()].into_iter().collect(),
                optional: ["child-2".to_owned()].into_iter().collect(),
                minimum_successes: 1,
                failure_policy: JoinFailurePolicy::MinimumSuccess,
                ordering_policy: JoinOrderingPolicy::MemberId,
                timeout_ms: 10,
                cancellation_propagates: true,
                result_projection: JoinResultProjection::ArtifactReferences,
                artifact_collection: JoinArtifactCollection::All,
            },
            NodeConfiguration::ParallelBranch {
                max_parallelism: 2,
                max_queue_depth: 4,
                join_target: "join".into(),
                join_policy: ParallelJoinPolicy::All,
                variable_merge_policies: [("result".to_owned(), VariableMergePolicy::Append)]
                    .into_iter()
                    .collect(),
                serialization_policy: Some(ParallelSerializationPolicy::StableBranchOrder),
            },
            NodeConfiguration::Delay {
                resolution: DelayResolution::Duration { duration_ms: 10 },
                expiration_timestamp: Some("2025-01-01T00:00:00Z".into()),
                cancellation: DelayCancellation::CancelContinuation,
            },
            NodeConfiguration::Schedule {
                trigger: ScheduleTrigger::Interval {
                    interval_ms: 10,
                    start_timestamp: Some("2025-01-01T00:00:00Z".into()),
                },
                wait_for_trigger: true,
                cancellation: ScheduleCancellation::CancelWaitOnly,
            },
            NodeConfiguration::EmitEvent {
                event_type: "graph.notice".into(),
                payload: serde_json::json!({"safe": true}),
                artifact_references: BTreeSet::new(),
                metadata: [("origin".to_owned(), "graph".to_owned())]
                    .into_iter()
                    .collect(),
            },
            NodeConfiguration::Plugin {
                plugin_id: "plugin.example".into(),
                executor_id: "example.node".into(),
                executor_version: "1.0.0".into(),
                node_kind: NodeKind::Review,
                input_schema: "input.v1".into(),
                output_schema: "output.v1".into(),
                configuration_reference: "config-1".into(),
                input: serde_json::json!({"key": "value"}),
            },
        ];
        for configuration in configurations {
            let encoded = serde_json::to_string(&configuration).expect("serialize");
            let decoded: NodeConfiguration = serde_json::from_str(&encoded).expect("deserialize");
            assert_eq!(decoded, configuration);
        }
    }

    #[test]
    fn child_graph_configuration_schema_round_trip() {
        let configurations = [
            NodeConfiguration::SpawnChildAgent {
                task_input: NodeValueSource::Static {
                    value: serde_json::json!({"task-a": "inspect"}),
                },
                task_id_prefix: "task".into(),
                child_style: "worker-v1".into(),
                tool_groups: ["filesystem.read".to_owned()].into_iter().collect(),
                maximum_children: 2,
                maximum_depth: 2,
                token_budget: 100,
                context_budget_tokens: 50,
                cost_budget_micros: 100,
                workspace: ChildWorkspaceConfiguration::SharedReadOnly,
                artifact_references: ["artifact-brief".to_owned()].into_iter().collect(),
                artifact_reference_variables: BTreeSet::new(),
                security_classification: SecurityClassification::Internal,
                approval_required: true,
            },
            NodeConfiguration::WaitForAgents {
                children: ChildSetSource::Variable {
                    variable: "children".into(),
                },
                maximum_children: 2,
                minimum_successes: 1,
                timeout_ms: 10,
                cancellation: ChildWaitCancellation::Cascade,
            },
            NodeConfiguration::Review {
                input: NodeValueSource::Static {
                    value: serde_json::json!({"result": "node:1"}),
                },
                artifact_references: ["artifact-evidence".to_owned()].into_iter().collect(),
                artifact_reference_variables: BTreeSet::new(),
                result_schema: ReviewResultSchema {
                    maximum_findings: 4,
                    maximum_finding_bytes: 256,
                    maximum_rejections: 2,
                    require_artifact_evidence: true,
                },
                routes: ReviewRoutes {
                    approved: "done".into(),
                    revision: "revise".into(),
                    failure: "failed".into(),
                },
                maximum_revisions: 2,
            },
        ];
        for configuration in configurations {
            let encoded = serde_json::to_string(&configuration).expect("serialize");
            let decoded: NodeConfiguration = serde_json::from_str(&encoded).expect("deserialize");
            assert_eq!(decoded, configuration);
        }
    }

    #[test]
    fn renamed_arbitrary_child_graph_compiles_exact_typed_contracts() {
        let graph = compile_configured_child_graph(&configured_child_graph())
            .expect("arbitrary child graph compiles");
        let kinds = graph
            .nodes
            .iter()
            .map(|node| (&node.id, node.kind, node.configuration.is_some()))
            .collect::<Vec<_>>();
        assert!(kinds.contains(&(&"commission".to_owned(), NodeKind::SpawnChildAgent, true)));
        assert!(kinds.contains(&(&"rendezvous".to_owned(), NodeKind::WaitForAgents, true)));
        assert!(kinds.contains(&(&"quality-gate".to_owned(), NodeKind::Review, true)));
    }

    #[test]
    fn child_wait_variable_accepts_a_single_child_id_declaration() {
        let source = configured_child_graph()
            .replace(
                "type = { kind = \"list\", item_type = { kind = \"child_id\" }, max_items = 4 }",
                "type = { kind = \"child_id\" }",
            )
            .replace("minimum_successes = 2", "minimum_successes = 1");
        let graph =
            compile_configured_child_graph(&source).expect("singleton child id wait compiles");
        let wait = graph
            .nodes
            .iter()
            .find(|node| node.id == "rendezvous")
            .expect("wait node");
        assert_eq!(wait.kind, NodeKind::WaitForAgents);
    }

    #[test]
    fn child_graph_configuration_rejects_approval_type_route_and_size_substitution() {
        let source = configured_child_graph();
        let no_approval = source.replace("approval_required = true", "approval_required = false");
        assert!(matches!(
            compile_configured_child_graph(&no_approval),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let wrong_child_type = source.replace(
            "type = { kind = \"list\", item_type = { kind = \"child_id\" }, max_items = 4 }",
            "type = { kind = \"list\", item_type = { kind = \"string\" }, max_items = 4 }",
        );
        assert!(matches!(
            compile_configured_child_graph(&wrong_child_type),
            Err(GraphError::ConfigurationVariableTypeMismatch { .. })
        ));

        let forged_failure = source.replace("failure = \"rejected\"", "failure = \"accepted\"");
        assert!(matches!(
            compile_configured_child_graph(&forged_failure),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let oversized = source.replace(
            "task_id_prefix = \"work\"",
            &format!(
                "task_id_prefix = \"{}\"",
                "x".repeat(CompilerLimits::default().max_name_bytes + 1)
            ),
        );
        assert!(compile_configured_child_graph(&oversized).is_err());
    }

    #[test]
    fn legacy_planless_child_nodes_compile_but_require_versioned_runtime_migration() {
        let source = r#"
format_version = 1
entry = "legacy-spawn"
[budget]
max_steps = 4
max_tokens = 10
max_cost_micros = 10
max_duration_ms = 10
[declarations]
capabilities = ["agents"]
[[nodes]]
id = "legacy-spawn"
kind = "spawn_child_agent"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "legacy-spawn"
to = "done"
"#;
        let graph = compile_configured_child_graph(source).expect("legacy graph remains loadable");
        assert!(
            graph
                .nodes
                .iter()
                .find(|node| node.id == "legacy-spawn")
                .expect("spawn")
                .configuration
                .is_none(),
            "pure generic dispatch must reject this node until an explicit branch migration recompiles it"
        );
    }

    const GENERIC_EPHEMERAL_TURN: &str = r#"
format_version = 1
entry = "fresh"
[budget]
max_steps = 16
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["context", "model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[variables]]
name = "disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "model"
consumers = ["tools"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "model"
consumers = ["tools"]
mutability = "mutable"
max_size_bytes = 256
security_classification = "internal"

[[variables]]
name = "turn_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tools"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 256
security_classification = "internal"

[[nodes]]
id = "fresh"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }
[[nodes]]
id = "model"
kind = "model_call"
provider = "mock"
write_variables = ["disposition", "model_result"]
configuration = { type = "model_request", disposition_output = "disposition", result_output = "model_result" }
[[nodes]]
id = "tools"
kind = "tool_execution_gate"
read_variables = ["disposition", "model_result"]
write_variables = ["turn_result"]
configuration = { type = "provider_tool_batch_execution", request_reference_variable = "model_result", disposition_variable = "disposition", maximum_calls = 8, allowed_tools = ["filesystem.read"] }
[[nodes]]
id = "done"
kind = "complete_turn"
read_variables = ["turn_result"]
configuration = { type = "complete_turn", result_reference_variable = "turn_result", cleanup = "discard_projection" }
[[edges]]
from = "fresh"
to = "model"
[[edges]]
from = "model"
to = "tools"
[[edges]]
from = "tools"
to = "done"
"#;

    const MODEL_JSON_OUTPUT_GRAPH: &str = r#"
format_version = 1
entry = "model"
[budget]
max_steps = 4
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000
[declarations]
capabilities = ["model"]
providers = ["mock"]

[[variables]]
name = "disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "model"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "receipt"
type = { kind = "node_result_reference" }
scope = "run"
producer = "model"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 256
security_classification = "internal"

[[variables]]
name = "summary"
type = { kind = "string" }
scope = "run"
producer = "model"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 1024
security_classification = "internal"

[[variables]]
name = "document"
type = { kind = "map", value_type = { kind = "integer" }, max_entries = 8 }
scope = "run"
producer = "model"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 1024
security_classification = "internal"

[[nodes]]
id = "model"
kind = "model_call"
provider = "mock"
write_variables = ["disposition", "receipt", "summary", "document"]
configuration = { type = "model_request", disposition_output = "disposition", result_output = "receipt", provider_options = { response_format = "json", temperature = "0" }, json_outputs = { document = "", summary = "/response/~0summary~1text" } }

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "model"
to = "done"
"#;

    #[test]
    fn generic_model_tool_batch_and_complete_turn_configuration_is_exact_and_bounded() {
        let graph = compile_valid(GENERIC_EPHEMERAL_TURN).expect("generic ephemeral graph");
        let node = |id: &str| {
            graph
                .nodes
                .iter()
                .find(|node| node.id == id)
                .unwrap_or_else(|| panic!("missing node {id}"))
        };
        assert_eq!(
            node("model").configuration,
            Some(NodeConfiguration::ModelRequest {
                disposition_output: "disposition".into(),
                result_output: "model_result".into(),
                provider_options: BTreeMap::new(),
                json_outputs: BTreeMap::new(),
                inputs: BTreeMap::new(),
            })
        );
        assert_eq!(
            node("tools").configuration,
            Some(NodeConfiguration::ProviderToolBatchExecution {
                request_reference_variable: "model_result".into(),
                disposition_variable: "disposition".into(),
                maximum_calls: 8,
                allowed_tools: BTreeSet::from(["filesystem.read".into()]),
            })
        );
        assert_eq!(node("tools").tool, None);
        assert_eq!(
            node("done").configuration,
            Some(NodeConfiguration::CompleteTurn {
                result_reference_variable: "turn_result".into(),
                cleanup: CompleteTurnCleanup::DiscardProjection,
            })
        );

        let parsed = GraphDefinition::parse(GENERIC_EPHEMERAL_TURN, CompilerLimits::default())
            .expect("parse configuration");
        let json = serde_json::to_string(&parsed).expect("configuration JSON");
        let decoded: GraphDefinition = serde_json::from_str(&json).expect("configuration JSON");
        assert_eq!(decoded, parsed);
    }

    #[test]
    fn model_request_options_and_json_outputs_compile_exactly() {
        let graph = compile_valid(MODEL_JSON_OUTPUT_GRAPH).expect("model JSON output graph");
        let configuration = graph
            .nodes
            .iter()
            .find(|node| node.id == "model")
            .and_then(|node| node.configuration.as_ref())
            .expect("model configuration");
        assert_eq!(
            configuration,
            &NodeConfiguration::ModelRequest {
                disposition_output: "disposition".into(),
                result_output: "receipt".into(),
                provider_options: BTreeMap::from([
                    ("response_format".into(), "json".into()),
                    ("temperature".into(), "0".into()),
                ]),
                json_outputs: BTreeMap::from([
                    ("document".into(), String::new()),
                    ("summary".into(), "/response/~0summary~1text".into()),
                ]),
                inputs: BTreeMap::new(),
            }
        );
    }

    #[test]
    fn model_request_schema_defaults_legacy_fields_and_round_trips_new_fields() {
        let legacy = r#"{
            "type":"model_request",
            "disposition_output":"disposition",
            "result_output":"receipt"
        }"#;
        let decoded: NodeConfiguration =
            serde_json::from_str(legacy).expect("legacy model configuration");
        assert_eq!(
            decoded,
            NodeConfiguration::ModelRequest {
                disposition_output: "disposition".into(),
                result_output: "receipt".into(),
                provider_options: BTreeMap::new(),
                json_outputs: BTreeMap::new(),
                inputs: BTreeMap::new(),
            }
        );

        let configuration = NodeConfiguration::ModelRequest {
            disposition_output: "disposition".into(),
            result_output: "receipt".into(),
            provider_options: BTreeMap::from([("temperature".into(), "0".into())]),
            json_outputs: BTreeMap::from([("summary".into(), "/summary".into())]),
            inputs: BTreeMap::new(),
        };
        let encoded = serde_json::to_string(&configuration).expect("serialize");
        let round_trip: NodeConfiguration = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(round_trip, configuration);
    }

    #[test]
    fn model_request_outputs_must_be_distinct_and_exact_declared_writes() {
        for invalid in [
            MODEL_JSON_OUTPUT_GRAPH.replace(
                r#"json_outputs = { document = "", summary = "/response/~0summary~1text" }"#,
                r#"json_outputs = { disposition = "/disposition", document = "", summary = "/response/~0summary~1text" }"#,
            ),
            MODEL_JSON_OUTPUT_GRAPH.replace(
                r#"write_variables = ["disposition", "receipt", "summary", "document"]"#,
                r#"write_variables = ["disposition", "receipt", "summary"]"#,
            ),
            MODEL_JSON_OUTPUT_GRAPH.replace(
                r#"write_variables = ["disposition", "receipt", "summary", "document"]"#,
                r#"write_variables = ["disposition", "receipt", "summary", "document", "extra"]"#,
            ),
        ] {
            assert!(matches!(
                compile_valid(&invalid),
                Err(GraphError::InvalidNodeConfiguration { .. }
                    | GraphError::VariableProducerDoesNotWrite { .. }
                    | GraphError::UndeclaredVariableWrite { .. })
            ));
        }
    }

    #[test]
    fn model_json_outputs_reject_runtime_owned_and_nested_reference_types() {
        for value_type in [
            r#"{ kind = "session_id" }"#,
            r#"{ kind = "child_id" }"#,
            r#"{ kind = "task_id" }"#,
            r#"{ kind = "artifact_reference" }"#,
            r#"{ kind = "secret_reference" }"#,
            r#"{ kind = "tool_result_reference" }"#,
            r#"{ kind = "approval_result" }"#,
            r#"{ kind = "node_result_reference" }"#,
            r#"{ kind = "timestamp" }"#,
            r#"{ kind = "duration" }"#,
            r#"{ kind = "map", value_type = { kind = "child_id" }, max_entries = 8 }"#,
        ] {
            let invalid = MODEL_JSON_OUTPUT_GRAPH.replace(
                r#"type = { kind = "string" }"#,
                &format!("type = {value_type}"),
            );
            assert!(matches!(
                compile_valid(&invalid),
                Err(GraphError::ConfigurationVariableTypeMismatch {
                    ref variable,
                    expected: "ordinary bounded canonical value",
                    ..
                }) if variable == "summary"
            ));
        }
    }

    #[test]
    fn model_json_pointers_require_bounded_rfc6901_syntax() {
        for pointer in [
            "response/summary",
            "/invalid~",
            "/invalid~2escape",
            "/control\\u0001character",
        ] {
            let invalid = MODEL_JSON_OUTPUT_GRAPH.replace(
                r#"summary = "/response/~0summary~1text""#,
                &format!(r#"summary = "{pointer}""#),
            );
            assert!(matches!(
                compile_valid(&invalid),
                Err(GraphError::InvalidNodeConfiguration { ref detail, .. })
                    if detail.contains("RFC 6901")
            ));
        }

        let oversized = MODEL_JSON_OUTPUT_GRAPH.replace(
            r#"summary = "/response/~0summary~1text""#,
            &format!(
                r#"summary = "/{}""#,
                "x".repeat(CompilerLimits::default().max_name_bytes)
            ),
        );
        assert!(matches!(
            compile_valid(&oversized),
            Err(GraphError::InvalidNodeConfiguration { ref detail, .. })
                if detail.contains("RFC 6901")
        ));
    }

    #[test]
    fn model_provider_options_are_bounded_and_control_free() {
        let invalid_name = MODEL_JSON_OUTPUT_GRAPH.replace(
            r#"response_format = "json""#,
            r#""response format" = "json""#,
        );
        assert!(matches!(
            compile_valid(&invalid_name),
            Err(GraphError::InvalidName {
                kind: "model provider option",
                ..
            })
        ));

        let invalid_value = MODEL_JSON_OUTPUT_GRAPH.replace(
            r#"response_format = "json""#,
            r#"response_format = "json\u0001""#,
        );
        assert!(matches!(
            compile_valid(&invalid_value),
            Err(GraphError::InvalidNodeConfiguration { ref detail, .. })
                if detail.contains("control characters")
        ));

        let oversized_value = MODEL_JSON_OUTPUT_GRAPH.replace(
            r#"response_format = "json""#,
            &format!(
                r#"response_format = "{}""#,
                "x".repeat(CompilerLimits::default().max_name_bytes + 1)
            ),
        );
        assert!(matches!(
            compile_valid(&oversized_value),
            Err(GraphError::InvalidNodeConfiguration { ref detail, .. })
                if detail.contains("bounded")
        ));

        let one_item_limit = CompilerLimits {
            max_configuration_items: 1,
            ..CompilerLimits::default()
        };
        assert!(matches!(
            compile(MODEL_JSON_OUTPUT_GRAPH, &cache_inputs(), one_item_limit),
            Err(GraphError::ConfigurationCollectionTooLarge {
                ref field,
                actual: 2,
                maximum: 1,
                ..
            }) if field == "model provider options"
        ));
        let one_provider_option = MODEL_JSON_OUTPUT_GRAPH.replace(r#", temperature = "0""#, "");
        assert!(matches!(
            compile(&one_provider_option, &cache_inputs(), one_item_limit),
            Err(GraphError::ConfigurationCollectionTooLarge {
                ref field,
                actual: 2,
                maximum: 1,
                ..
            }) if field == "model JSON outputs"
        ));
    }

    #[test]
    fn generic_provider_tool_batch_rejects_unbounded_forged_or_mistyped_contracts() {
        let zero_calls = GENERIC_EPHEMERAL_TURN.replace("maximum_calls = 8", "maximum_calls = 0");
        assert!(matches!(
            compile_valid(&zero_calls),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let undeclared_tool = GENERIC_EPHEMERAL_TURN.replace(
            "allowed_tools = [\"filesystem.read\"]",
            "allowed_tools = [\"filesystem.write\"]",
        );
        assert!(matches!(
            compile_valid(&undeclared_tool),
            Err(GraphError::UndeclaredTool { .. })
        ));

        let wrong_disposition = GENERIC_EPHEMERAL_TURN.replace(
            r#"type = { kind = "enum", values = ["response_complete", "tool_requests"] }"#,
            r#"type = { kind = "string" }"#,
        );
        assert!(matches!(
            compile_valid(&wrong_disposition),
            Err(GraphError::ConfigurationVariableTypeMismatch { .. })
        ));

        let undeclared_read = GENERIC_EPHEMERAL_TURN
            .replace("read_variables = [\"disposition\", \"model_result\"]\n", "");
        assert!(matches!(
            compile_valid(&undeclared_read),
            Err(GraphError::InvalidNodeConfiguration { .. })
        ));

        let forged_output = GENERIC_EPHEMERAL_TURN.replace(
            "write_variables = [\"turn_result\"]",
            "write_variables = [\"turn_result\", \"model_result\"]",
        );
        assert!(matches!(
            compile_valid(&forged_output),
            Err(GraphError::InvalidNodeConfiguration { .. }
                | GraphError::UnauthorizedVariableWriter { .. })
        ));
    }
}
