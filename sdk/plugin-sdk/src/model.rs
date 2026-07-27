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
