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
    /// Declared plugin-provided graph node executors.
    #[serde(default)]
    pub node_executors: Vec<PluginNodeExecutor>,
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
    #[serde(default)]
    pub observer_delivery: PluginObserverDelivery,
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

/// Declared plugin-provided graph node executor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeExecutor {
    /// Stable executor ID.
    pub executor_id: String,
    /// Executor semantic version.
    pub version: String,
    /// Serialized graph node kind.
    pub node_kind: String,
    /// Runtime API requirement of this executor.
    pub runtime_api: String,
    /// Business capabilities required from the runtime.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
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
    pub read_authority: Vec<String>,
    /// State scope the node may propose to modify.
    pub state_scope: String,
}

/// Plugin-provided memory backend declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryDeclaration {
    /// Supported memory scopes.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Memory capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,
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
    pub before: Vec<String>,
    /// Transform IDs that must execute before this transform.
    #[serde(default)]
    pub after: Vec<String>,
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

impl Default for PluginObserverDelivery {
    fn default() -> Self {
        Self::BestEffort
    }
}
