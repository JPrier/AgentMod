//! Versioned wire contracts between the runtime and isolated plugin hosts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current plugin-host wire protocol.
pub const CURRENT_PROTOCOL_VERSION: u16 = 2;

/// Canonical plugin invocation audit outcome codes.
///
/// These constants are the wire-stable outcome vocabulary. The runtime maps
/// them into canonical audit events; the plugin host records them in its
/// bounded audit ring.
pub mod audit_outcome {
    /// An invocation was proposed but has not started.
    pub const PROPOSED: &str = "proposed";
    /// An invocation started inside the plugin host.
    pub const STARTED: &str = "started";
    /// An invocation completed with a valid response.
    pub const COMPLETED: &str = "completed";
    /// The plugin explicitly rejected the operation.
    pub const REJECTED_BY_PLUGIN: &str = "rejected_by_plugin";
    /// Runtime validation rejected the returned result.
    pub const REJECTED_BY_RUNTIME: &str = "rejected_by_runtime";
    /// The invocation exceeded its deadline.
    pub const TIMED_OUT: &str = "timed_out";
    /// The invocation was cancelled.
    pub const CANCELLED: &str = "cancelled";
    /// The plugin worker process crashed.
    pub const CRASHED: &str = "crashed";
    /// The plugin returned an unparseable or out-of-schema response.
    pub const INVALID_RESPONSE: &str = "invalid_response";
    /// The plugin was placed in quarantine.
    pub const QUARANTINED: &str = "quarantined";
    /// An observer delivery attempt was made.
    pub const OBSERVER_DELIVERY_ATTEMPTED: &str = "observer_delivery_attempted";
    /// An observer delivery completed.
    pub const OBSERVER_DELIVERY_COMPLETED: &str = "observer_delivery_completed";
    /// An observer delivery failed.
    pub const OBSERVER_DELIVERY_FAILED: &str = "observer_delivery_failed";
    /// An observer delivery was dropped by the bounded queue.
    pub const OBSERVER_DELIVERY_DROPPED: &str = "observer_delivery_dropped";
}

/// Plugin execution classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginClass {
    /// Synchronous proposal interceptor.
    Blocking,
    /// Non-authoritative committed-event observer.
    Observer,
    /// Dynamically provided tool.
    Tool,
    /// Plugin-provided graph node executor.
    GraphNode,
    /// Plugin-provided memory backend.
    Memory,
    /// Plugin-provided compaction strategy.
    Compaction,
    /// Plugin-provided context transform.
    ContextTransform,
    /// Other declared extension category.
    Extension,
}

/// Out-of-process entrypoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginEntrypoint {
    /// Executable path or name selected by the composition root policy.
    pub program: String,
    /// Fixed launch arguments.
    #[serde(default)]
    pub arguments: Vec<String>,
}

/// Configuration-schema declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfigurationSchema {
    /// Stable schema ID.
    pub id: String,
    /// Positive schema version.
    pub version: u32,
    /// Whether configuration is mandatory.
    pub required: bool,
    /// Bounded inline JSON Schema object.
    pub inline_json: String,
}

/// Declared plugin-provided graph node executor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeExecutorDeclaration {
    /// Stable executor ID.
    pub executor_id: String,
    /// Executor semantic version.
    pub version: String,
    /// Serialized graph node kind executed by this executor.
    pub node_kind: String,
    /// Runtime API requirement of this executor.
    pub runtime_api: String,
    /// Business capabilities required from the runtime.
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    /// Bounded inline JSON Schema for node input.
    pub input_schema: String,
    /// Bounded inline JSON Schema for node output.
    pub output_schema: String,
    /// Per-node execution timeout in milliseconds.
    pub timeout_ms: u64,
    /// Node failure policy: reject, cancel, disable, continue, or retry.
    pub failure_policy: String,
    /// Whether repeated execution with identical input is safe.
    pub idempotent: bool,
    /// Whether execution performs a declared external effect.
    pub external_effect: bool,
    /// Readable state scopes for node input.
    #[serde(default)]
    pub read_authority: BTreeSet<String>,
    /// State scope the node may propose to modify.
    pub state_scope: String,
}

/// Plugin-provided memory backend declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryDeclaration {
    /// Supported memory scopes.
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    /// Memory capabilities.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Hard retained-byte bound.
    pub bounded_bytes: u64,
}

/// Plugin-provided compaction strategy declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompactionDeclaration {
    /// Stable strategy ID.
    pub strategy_id: String,
    /// Whether committing the identical replacement twice is safe.
    pub idempotent: bool,
    /// Maximum replacement byte size.
    pub bounded_bytes: u64,
}

/// Context transform lifecycle boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContextTransformBoundary {
    /// Immediately before memory retrieval.
    BeforeMemoryRetrieval,
    /// Immediately after memory retrieval.
    AfterMemoryRetrieval,
    /// Immediately before compaction.
    BeforeCompaction,
    /// Immediately after compaction.
    AfterCompaction,
    /// Before the provider projection is finalized.
    BeforeProviderProjection,
    /// Before the turn completes.
    BeforeTurnCompletion,
}

/// Plugin-provided context transform declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginContextTransformDeclaration {
    /// Stable transform ID.
    pub transform_id: String,
    /// Lifecycle boundary at which the transform runs.
    pub boundary: PluginContextTransformBoundary,
    /// Ordering stage.
    #[serde(default)]
    pub stage: u16,
    /// Priority within the stage.
    #[serde(default)]
    pub priority: i32,
    /// Transform IDs that must execute after this transform.
    #[serde(default)]
    pub before: BTreeSet<String>,
    /// Transform IDs that must execute before this transform.
    #[serde(default)]
    pub after: BTreeSet<String>,
}

/// Observer delivery semantics declared by a plugin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum PluginObserverDelivery {
    /// Bounded fire-and-forget queue; drops are counted and audited.
    BestEffort,
    /// At most once; duplicate deliveries are dropped.
    AtMostOnce,
    /// At least once with a runtime-issued idempotency key and bounded retries.
    AtLeastOnce {
        /// Maximum delivery attempts including the first.
        max_attempts: u8,
        /// Delay between delivery attempts.
        retry_backoff_ms: u64,
    },
}

/// Wire form of a plugin manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Globally unique plugin ID.
    pub id: String,
    /// Plugin semantic version.
    pub version: String,
    /// Compatible runtime API requirement.
    pub runtime_api: String,
    /// Extension category.
    pub category: String,
    /// Invocation/model/turn/session/project/user/runtime scope.
    pub scope: String,
    /// Blocking/observer/tool/graph-node/memory/compaction/context-transform class.
    pub class: PluginClass,
    /// Isolated executable.
    pub entrypoint: PluginEntrypoint,
    /// Requested and provided capabilities.
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    /// Capabilities made available.
    #[serde(default)]
    pub provided_capabilities: BTreeSet<String>,
    /// Canonical event or proposal names.
    #[serde(default)]
    pub subscribed_events: BTreeSet<String>,
    /// Readable state scopes.
    #[serde(default)]
    pub read_authority: BTreeSet<String>,
    /// Proposed state writes; observers must leave canonical writes absent.
    #[serde(default)]
    pub proposed_write_authority: BTreeSet<String>,
    /// Allowed tool or tool-group names.
    #[serde(default)]
    pub tool_permissions: BTreeSet<String>,
    /// Allowed exact domains or wildcard subdomains.
    #[serde(default)]
    pub network_permissions: BTreeSet<String>,
    /// Stable handlers that must precede this plugin.
    #[serde(default)]
    pub after: BTreeSet<String>,
    /// Stable handlers that must follow this plugin.
    #[serde(default)]
    pub before: BTreeSet<String>,
    /// Ordering stage.
    #[serde(default)]
    pub stage: u16,
    /// Priority within the stage.
    #[serde(default)]
    pub priority: i32,
    /// Execution deadline.
    pub timeout_ms: u64,
    /// Failure policy: reject, cancel, disable, continue, or retry.
    pub failure_policy: String,
    /// Maximum attempts for retry policy.
    #[serde(default = "one")]
    pub max_attempts: u8,
    /// Delay between retries.
    #[serde(default)]
    pub retry_backoff_ms: u64,
    /// Plugin-owned state migration version.
    pub state_migration_version: u32,
    /// Configuration schema.
    pub configuration_schema: PluginConfigurationSchema,
    /// Declared graph node executors.
    #[serde(default)]
    pub node_executors: Vec<PluginNodeExecutorDeclaration>,
    /// Declared plugin memory backend, if any.
    #[serde(default)]
    pub memory: Option<PluginMemoryDeclaration>,
    /// Declared plugin compaction strategy, if any.
    #[serde(default)]
    pub compaction: Option<PluginCompactionDeclaration>,
    /// Declared context transforms.
    #[serde(default)]
    pub context_transforms: Vec<PluginContextTransformDeclaration>,
    /// Declared observer delivery semantics.
    #[serde(default = "default_observer_delivery")]
    pub observer_delivery: PluginObserverDelivery,
}

const fn one() -> u8 {
    1
}

fn default_observer_delivery() -> PluginObserverDelivery {
    PluginObserverDelivery::BestEffort
}

/// Short-lived authorization attached to consequential calls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginAuthorization {
    /// Authenticated local owner.
    pub owner_id: String,
    /// Runtime session.
    pub session_id: String,
    /// Unique call ID.
    pub call_id: String,
    /// Digest of the exact normalized operation.
    pub normalized_digest: String,
    /// Shared-key authorization grant.
    pub grant: String,
    /// Opaque cancellation ID.
    pub cancellation_id: String,
}

/// Bounded plugin memory item returned by retrieval.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryItem {
    /// Provider-local reference.
    pub reference: String,
    /// Bounded content.
    pub content: String,
    /// Provider relevance score.
    pub score: Option<f64>,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: i64,
}

/// Runtime/plugin-host command.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "command", content = "value", rename_all = "snake_case")]
pub enum PluginCommand {
    /// Negotiate protocol/API capabilities before activation.
    Negotiate {
        /// Requested wire version.
        protocol_version: u16,
        /// Runtime plugin API version.
        runtime_api_version: String,
        /// Runtime capabilities.
        capabilities: BTreeSet<String>,
    },
    /// Validate an activation set, including ordering constraints.
    ValidateSet {
        /// Complete candidate set.
        manifests: Vec<PluginManifest>,
    },
    /// Validate, migrate, and load a plugin.
    Load {
        /// Declared plugin authority and compatibility.
        manifest: Box<PluginManifest>,
        /// Schema-validated configuration.
        configuration: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Run one declared interceptor.
    Intercept {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable handler ID.
        handler: String,
        /// Stable proposal class.
        proposal_type: String,
        /// Current proposal.
        proposal: Value,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Deliver an event to an observer queue.
    Observe {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable observer handler ID.
        handler: String,
        /// Stable committed event type.
        event_type: String,
        /// Bounded committed event projection.
        event: Value,
        /// First canonical sequence in the delivered range.
        event_range_start: u64,
        /// Last canonical sequence in the delivered range.
        event_range_end: u64,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Invoke a declared plugin tool.
    InvokeTool {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Stable tool name.
        tool: String,
        /// Normalized tool arguments.
        arguments: Value,
        /// Explicit readable state.
        readable_state: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Execute one declared plugin graph node.
    ExecuteNode {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Declared executor ID.
        executor_id: String,
        /// Compiled graph node ID.
        node_id: String,
        /// Serialized graph node kind.
        node_kind: String,
        /// Normalized node input.
        input: Value,
        /// Bounded runtime variable environment.
        variables: Value,
        /// Explicit readable state.
        readable_state: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Describe plugin memory scopes and capabilities.
    MemoryDescribe {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Retrieve bounded plugin memory.
    MemoryRetrieve {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Memory scope.
        scope: String,
        /// Normalized query.
        query: String,
        /// Maximum items.
        limit: usize,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Commit an already-approved plugin memory write.
    MemoryCommitWrite {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Memory scope.
        scope: String,
        /// Bounded entries to commit.
        entries: Vec<PluginMemoryItem>,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Report plugin memory health.
    MemoryHealth {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Propose a replacement projection.
    CompactionPropose {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Inclusive canonical source sequence range start.
        source_range_start: u64,
        /// Inclusive canonical source sequence range end.
        source_range_end: u64,
        /// Content hash of the exact source range.
        source_range_hash: String,
        /// Current provider-visible entries.
        current_entries: Value,
        /// Structured proposal supplied by the runtime.
        proposal: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Run one context transform at a lifecycle boundary.
    ContextTransform {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Declared transform ID.
        transform_id: String,
        /// Lifecycle boundary.
        boundary: PluginContextTransformBoundary,
        /// Bounded transform payload.
        payload: Value,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Cancel a running plugin invocation.
    Cancel {
        /// Plugin invocation to stop.
        invocation_id: String,
    },
    /// Disable without deleting persisted state.
    Disable {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Quarantine a plugin after a policy or crash finding.
    Quarantine {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Redacted reason code.
        reason_code: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Reload a plugin after an upgrade, preserving persisted state.
    Reload {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Return a quarantined plugin to active service under policy.
    Unquarantine {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Read a bounded audit slice.
    AuditList {
        /// Optional cursor: only audits after this invocation ID.
        since_invocation_id: Option<String>,
        /// Maximum entries.
        limit: u16,
    },
    /// Report plugin-host health and bounded audit state.
    Health,
}

/// Auditable invocation metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginAudit {
    /// Plugin ID.
    pub plugin_id: String,
    /// Invocation ID, if any.
    pub invocation_id: Option<String>,
    /// Stable operation name.
    pub operation: String,
    /// Stable outcome code (see [`audit_outcome`]).
    pub outcome: String,
    /// Attempt count.
    pub attempts: u8,
}

/// Plugin-host response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum PluginResponse {
    /// Negotiation succeeded.
    Negotiated {
        /// Selected protocol.
        protocol_version: u16,
        /// Host API version.
        runtime_api_version: String,
        /// Mutually available capabilities.
        capabilities: BTreeSet<String>,
    },
    /// Candidate activation set is valid.
    SetValidated {
        /// Deterministically ordered plugin IDs.
        plugin_ids: Vec<String>,
    },
    /// Manifest/configuration accepted.
    Loaded {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Active state version.
        state_version: u32,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal is unchanged.
    Continue {
        /// Unchanged or explicitly normalized proposal.
        proposal: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal was replaced.
    Replace {
        /// Replacement proposal.
        proposal: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Proposal was rejected.
    Reject {
        /// Safe rejection explanation.
        reason: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin tool completed.
    ToolResult {
        /// Bounded normalized result.
        value: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin graph node completed.
    NodeResult {
        /// Bounded normalized node output.
        value: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Observation was accepted or dropped by the bounded queue.
    Observation {
        /// Whether it entered the queue.
        accepted: bool,
        /// Current bounded queue depth.
        queue_depth: usize,
        /// Total dropped events for this plugin.
        dropped: u64,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Memory scopes and capabilities.
    MemoryDescribed {
        /// Supported scopes.
        scopes: BTreeSet<String>,
        /// Capabilities.
        capabilities: BTreeSet<String>,
        /// Hard retained-byte bound.
        bounded_bytes: u64,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Bounded memory retrieval result.
    MemoryRetrieved {
        /// Items.
        items: Vec<PluginMemoryItem>,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Approved memory write was committed.
    MemoryWriteCommitted {
        /// Whether the provider retained the entries.
        retained: bool,
        /// Provider-local references.
        references: Vec<String>,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin memory health projection.
    MemoryHealthResult {
        /// Whether the backend is healthy.
        healthy: bool,
        /// Retained item count.
        item_count: u64,
        /// Retained bytes.
        retained_bytes: u64,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Replacement projection was accepted by the plugin.
    CompactionProposalAccepted {
        /// Structured replacement entries.
        replacement: Value,
        /// Measured replacement byte size.
        size_bytes: u64,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Context transform completed.
    TransformResult {
        /// Bounded transformed payload.
        value: Value,
        /// Audit result.
        audit: PluginAudit,
    },
    /// A plugin invocation was cancelled.
    Cancelled {
        /// Cancelled invocation ID.
        invocation_id: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin was disabled, quarantined, reloaded, or unquarantined.
    StateChanged {
        /// Plugin ID.
        plugin_id: String,
        /// `disabled`, `quarantined`, `reloaded`, or `active`.
        state: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Bounded audit slice.
    AuditListed {
        /// Audits in stable append order.
        audits: Vec<PluginAudit>,
        /// Whether older entries were truncated.
        truncated: bool,
    },
    /// Health projection.
    Health {
        /// Loaded plugin count.
        loaded: usize,
        /// Running invocation count.
        running: usize,
        /// Observer drops.
        observer_dropped: u64,
    },
    /// Structured plugin failure.
    Failed {
        /// Stable failure class.
        code: String,
        /// Redacted diagnostic.
        message: String,
        /// Whether runtime policy may retry.
        retryable: bool,
    },
}
