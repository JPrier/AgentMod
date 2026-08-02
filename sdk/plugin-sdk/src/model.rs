use serde::{Deserialize, Serialize};

/// Complete versioned plugin manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Plugin identity and compatibility.
    pub identity: PluginIdentity,
    /// Functional plugin category.
    pub category: PluginCategory,
    /// Maximum state scope visible to the plugin.
    pub scope: PluginScope,
    /// Blocking or asynchronous observer classification.
    pub classification: PluginClassification,
    /// Execution entrypoint declaration.
    pub entrypoint: Entrypoint,
    /// Trust assigned by distribution/configuration.
    pub trust: TrustLevel,
    /// Required isolation boundary.
    pub isolation: IsolationMode,
    /// Capabilities required from the runtime or plugin set.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Capabilities exported by this plugin.
    #[serde(default)]
    pub provided_capabilities: Vec<String>,
    /// Canonical event names subscribed by the plugin.
    #[serde(default)]
    pub subscribed_events: Vec<String>,
    /// Declared read and proposed-write authority.
    pub authorities: AuthorityManifest,
    /// Tool and network permissions requested by the plugin.
    pub permissions: PermissionManifest,
    /// Deterministic ordering constraints.
    #[serde(default)]
    pub ordering: OrderingManifest,
    /// Configuration schema metadata.
    pub configuration: ConfigurationSchemaMetadata,
    /// Handler failure behavior.
    pub failure_policy: FailurePolicy,
    /// Per-invocation timeout in milliseconds.
    pub timeout_ms: u64,
    /// Plugin-owned state migration version.
    pub state_migration_version: u32,
    /// Exact graph-node executors exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_executors: Vec<NodeExecutorManifest>,
    /// Exact provider-projection context transforms exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_transforms: Vec<ContextTransformManifest>,
    /// Exact memory-provider implementations exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memory_providers: Vec<MemoryProviderManifest>,
    /// Exact provider-projection compactors exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compactors: Vec<CompactorManifest>,
}

/// Recovery declaration shared by isolated memory and compaction operations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginOperationIdempotency {
    /// The exact operation may be safely repeated with the same invocation ID.
    Idempotent,
    /// An ambiguous operation must not be automatically repeated.
    NonIdempotent,
}

/// Exact retrieval operation exported by one plugin memory provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryRetrieveManifest {
    /// Stable isolated-process handler name.
    pub handler: String,
    /// Bounded inline JSON Schema for the runtime-owned retrieval request.
    pub input_schema: String,
    /// Bounded inline JSON Schema for the proposed memory-item collection.
    pub output_schema: String,
    /// Per-invocation timeout, bounded by the containing plugin timeout.
    pub timeout_ms: u64,
    /// Retrieval-specific failure behavior.
    pub failure_policy: FailurePolicy,
    /// Whether an ambiguous retrieval may be repeated.
    pub idempotency: PluginOperationIdempotency,
    /// Permissions required by retrieval, bounded by the plugin declaration.
    pub required_permissions: PermissionManifest,
    /// Maximum state scope readable by retrieval.
    pub state_scope: PluginScope,
    /// Whether retrieval can perform external effects.
    pub external_effects: bool,
}

/// Exact write operation exported by one plugin memory provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryWriteManifest {
    /// Stable isolated-process handler name.
    pub handler: String,
    /// Bounded inline JSON Schema for the approved runtime write request.
    pub input_schema: String,
    /// Bounded inline JSON Schema for the terminal provider receipt.
    pub output_schema: String,
    /// Per-invocation timeout, bounded by the containing plugin timeout.
    pub timeout_ms: u64,
    /// Write-specific failure behavior.
    pub failure_policy: FailurePolicy,
    /// Whether an ambiguous write may be repeated.
    pub idempotency: PluginOperationIdempotency,
    /// Permissions required by the write, bounded by the plugin declaration.
    pub required_permissions: PermissionManifest,
    /// Maximum state scope readable by the write.
    pub state_scope: PluginScope,
    /// Whether this explicitly declared write can perform external effects.
    pub external_effects: bool,
}

/// Exact plugin-provided memory implementation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryProviderManifest {
    /// Stable provider implementation ID.
    pub provider_id: String,
    /// Exact provider semantic version.
    pub version: String,
    /// Semantic runtime API requirement for this provider.
    pub runtime_api: String,
    /// Capabilities resolved by this provider.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Required pure retrieval operation.
    pub retrieve: MemoryRetrieveManifest,
    /// Optional explicitly declared consequential write operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<MemoryWriteManifest>,
}

impl MemoryProviderManifest {
    /// Returns the deterministic complete declaration bytes a registry hashes.
    ///
    /// Every declaration field participates because serialization starts at
    /// the complete strongly typed provider declaration.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if canonical JSON encoding fails.
    pub fn declaration_hash_input(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// Exact plugin-provided provider-projection compactor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactorManifest {
    /// Stable compactor implementation ID.
    pub compactor_id: String,
    /// Exact compactor semantic version.
    pub version: String,
    /// Semantic runtime API requirement for this compactor.
    pub runtime_api: String,
    /// Stable isolated-process handler name.
    pub handler: String,
    /// Capabilities resolved by this compactor.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Bounded inline JSON Schema for canonical projection input.
    pub input_schema: String,
    /// Bounded inline JSON Schema for the replacement proposal.
    pub output_schema: String,
    /// Per-invocation timeout, bounded by the containing plugin timeout.
    pub timeout_ms: u64,
    /// Compactor-specific failure behavior.
    pub failure_policy: FailurePolicy,
    /// Whether an ambiguous compaction may be repeated.
    pub idempotency: PluginOperationIdempotency,
    /// Permissions required by compaction, bounded by the plugin declaration.
    pub required_permissions: PermissionManifest,
    /// Maximum state scope readable by compaction.
    pub state_scope: PluginScope,
    /// Whether this compactor can perform external effects.
    pub external_effects: bool,
}

impl CompactorManifest {
    /// Returns the deterministic complete declaration bytes a registry hashes.
    ///
    /// Every declaration field participates because serialization starts at
    /// the complete strongly typed compactor declaration.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if canonical JSON encoding fails.
    pub fn declaration_hash_input(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// Lifecycle boundary supported by an exact plugin context transform.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformLifecycle {
    /// Transform the bounded provider projection immediately before a model request.
    BeforeModelRequest,
}

/// Recovery declaration for a plugin context-transform invocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformIdempotency {
    /// The exact pure invocation may be safely repeated with the same invocation ID.
    Idempotent,
    /// An ambiguous invocation must not be automatically repeated.
    NonIdempotent,
}

/// Exact plugin-provided provider-projection transform declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextTransformManifest {
    /// Stable transform implementation ID.
    pub transform_id: String,
    /// Exact transform semantic version.
    pub version: String,
    /// Semantic runtime API requirement for this transform.
    pub runtime_api: String,
    /// Stable isolated-process handler name.
    pub handler: String,
    /// Exact lifecycle boundary at which the transform may run.
    pub lifecycle: ContextTransformLifecycle,
    /// Capabilities resolved by this transform.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Bounded inline JSON Schema for the transform input.
    pub input_schema: String,
    /// Bounded inline JSON Schema for the proposed provider projection.
    pub output_schema: String,
    /// Per-invocation timeout, bounded by the containing plugin timeout.
    pub timeout_ms: u64,
    /// Transform-specific failure behavior.
    pub failure_policy: FailurePolicy,
    /// Whether an ambiguous invocation may be repeated.
    pub idempotency: ContextTransformIdempotency,
    /// Permissions required by this transform, bounded by the plugin declaration.
    pub required_permissions: PermissionManifest,
    /// Maximum state scope readable by this transform.
    pub state_scope: PluginScope,
    /// Whether the transform may perform or propose external effects.
    pub external_effects: bool,
}

/// Exact plugin-provided graph-node executor declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExecutorManifest {
    /// Stable executor implementation ID.
    pub executor_id: String,
    /// Exact executor semantic version.
    pub version: String,
    /// Semantic runtime API requirement for this executor.
    pub runtime_api: String,
    /// Serialized graph node kind handled by this executor.
    pub node_kind: String,
    /// Stable isolated-process handler name.
    pub handler: String,
    /// Capabilities resolved by this executor.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Bounded inline JSON Schema for node input.
    pub input_schema: String,
    /// Bounded inline JSON Schema for the proposed outcome.
    pub output_schema: String,
    /// Per-invocation timeout, bounded by the plugin timeout.
    pub timeout_ms: u64,
    /// Executor-specific failure behavior.
    pub failure_policy: FailurePolicy,
    /// Whether an invocation may be safely repeated after an ambiguous transport result.
    pub idempotency: NodeExecutorIdempotency,
    /// Permissions required by this executor, bounded by the plugin declaration.
    pub required_permissions: PermissionManifest,
    /// Maximum state scope readable by this executor.
    pub state_scope: PluginScope,
    /// Whether the executor can propose externally consequential actions.
    pub external_effects: bool,
}

/// Recovery declaration for a plugin node invocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutorIdempotency {
    /// The exact invocation may be safely repeated with the same invocation ID.
    Idempotent,
    /// An ambiguous invocation must not be automatically repeated.
    NonIdempotent,
}

/// Stable plugin identity and version compatibility declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginIdentity {
    /// Globally stable lowercase plugin ID.
    pub id: String,
    /// Semantic plugin version.
    pub version: String,
    /// Semantic version requirement for the runtime plugin API.
    pub runtime_api: String,
}

/// Plugin extension category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginCategory {
    /// Blocking interceptor.
    Interceptor,
    /// Asynchronous event observer.
    Observer,
    /// Tool provider.
    Tool,
    /// Model provider.
    Provider,
    /// Memory implementation.
    Memory,
    /// Context transform.
    ContextTransform,
    /// Compaction implementation.
    Compaction,
    /// Session execution style.
    SessionStyle,
    /// Declarative graph node.
    GraphNode,
    /// Permission policy.
    PermissionPolicy,
    /// Scheduler.
    Scheduler,
    /// Frontend.
    Frontend,
    /// Artifact processor.
    ArtifactProcessor,
}

/// Plugin lifetime and authority scope.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginScope {
    /// One action invocation.
    Invocation,
    /// One model request.
    ModelCall,
    /// One user turn.
    Turn,
    /// One session.
    Session,
    /// One project.
    Project,
    /// One user.
    User,
    /// Entire runtime.
    Runtime,
}

/// Whether a plugin blocks proposals or observes committed events.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginClassification {
    /// Ordered blocking interceptor.
    Blocking,
    /// Asynchronous observer without canonical write authority.
    Observer,
}

/// Versioned plugin entrypoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Entrypoint {
    /// Statically linked trusted Rust entrypoint.
    RustBuiltin {
        /// Composition-root symbol name.
        symbol: String,
    },
    /// Out-of-process executable entrypoint.
    Process {
        /// Executable or validated executable path.
        program: String,
        /// Fixed launch arguments.
        #[serde(default)]
        args: Vec<String>,
    },
    /// Sandboxed WASI component entrypoint.
    WasiComponent {
        /// Project/user-relative component path.
        component: String,
    },
}

/// Assigned plugin trust.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    /// Shipped and statically reviewed first-party code.
    FirstParty,
    /// Explicitly approved third-party process plugin.
    ApprovedThirdParty,
    /// Sandboxed third-party component.
    Sandboxed,
    /// Untrusted and not eligible for activation.
    Untrusted,
}

/// Requested isolation mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationMode {
    /// Trusted in-process execution.
    TrustedInProcess,
    /// Isolated process execution.
    Process,
    /// WASI component sandbox.
    Wasi,
}

/// State access declaration.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityManifest {
    /// State categories the plugin may read.
    #[serde(default)]
    pub read: Vec<AuthorityTarget>,
    /// State changes the plugin may propose, never apply directly.
    #[serde(default)]
    pub proposed_write: Vec<AuthorityTarget>,
}

/// Authority target independent from runtime implementation types.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityTarget {
    /// Invocation-local state.
    InvocationState,
    /// Model-call-local state.
    ModelCallState,
    /// Turn state.
    TurnState,
    /// Session state.
    SessionState,
    /// Project state.
    ProjectState,
    /// User state.
    UserState,
    /// Runtime state.
    RuntimeState,
    /// Canonical session state, available only as a proposal target.
    CanonicalState,
    /// Rebuildable derived index.
    DerivedIndex,
    /// Plugin-owned state.
    PluginState,
    /// External notification sink.
    ExternalNotification,
}

/// Requested tool and network access.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionManifest {
    /// Stable tool or tool-group permission names.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Exact domains or `*.` subdomain patterns.
    #[serde(default)]
    pub network: Vec<String>,
}

/// Deterministic plugin ordering declaration.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrderingManifest {
    /// Broad ordering stage.
    #[serde(default)]
    pub stage: u16,
    /// Priority within the stage.
    #[serde(default)]
    pub priority: i32,
    /// Plugin IDs which must execute after this plugin.
    #[serde(default)]
    pub before: Vec<String>,
    /// Plugin IDs which must execute before this plugin.
    #[serde(default)]
    pub after: Vec<String>,
}

/// Configuration schema identity and source metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationSchemaMetadata {
    /// Stable schema ID.
    pub schema_id: String,
    /// Plugin-specific schema version.
    pub schema_version: u32,
    /// Whether configuration must be supplied.
    pub required: bool,
    /// Schema document source.
    pub source: ConfigurationSchemaSource,
}

/// Configuration JSON Schema source.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ConfigurationSchemaSource {
    /// Inline JSON Schema document.
    InlineJson {
        /// Complete JSON Schema text.
        document: String,
    },
    /// Plugin-package-relative JSON Schema file.
    File {
        /// Safe relative path.
        relative_path: String,
    },
}

/// Failure behavior declared by a plugin handler.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum FailurePolicy {
    /// Reject the blocked proposal.
    Reject,
    /// Cancel the blocked proposal.
    Cancel,
    /// Disable the failed plugin.
    Disable,
    /// Continue without the observer result.
    Continue,
    /// Retry within bounded attempts and timeout.
    Retry {
        /// Total attempts including the first.
        max_attempts: u8,
        /// Delay between attempts.
        backoff_ms: u64,
    },
}
