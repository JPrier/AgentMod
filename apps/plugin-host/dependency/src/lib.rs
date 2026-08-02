//! External process, persistence, authorization, and plugin-SDK adapters.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_plugin_sdk as sdk;
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, RwLock, mpsc},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Canonical plugin audit outcome codes (dependency-local mirror of the wire
/// contract vocabulary).
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

/// Dependency-owned plugin classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyPluginClass {
    /// Blocking interceptor.
    Blocking,
    /// Observer.
    Observer,
    /// Tool.
    Tool,
    /// Plugin-provided graph node executor.
    GraphNode,
    /// Plugin-provided memory backend.
    Memory,
    /// Plugin-provided compaction strategy.
    Compaction,
    /// Plugin-provided context transform.
    ContextTransform,
    /// Other extension.
    Extension,
}

/// Dependency-owned entrypoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyEntrypoint {
    /// Executable.
    pub program: String,
    /// Fixed arguments.
    pub arguments: Vec<String>,
}

/// Dependency-owned configuration schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyConfigurationSchema {
    /// ID.
    pub id: String,
    /// Version.
    pub version: u32,
    /// Required.
    pub required: bool,
    /// Inline JSON schema.
    pub inline_json: String,
}

/// Dependency-owned graph node executor declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyNodeExecutor {
    /// Executor ID.
    pub executor_id: String,
    /// Executor version.
    pub version: String,
    /// Serialized graph node kind.
    pub node_kind: String,
    /// Runtime API requirement.
    pub runtime_api: String,
    /// Required capabilities.
    pub required_capabilities: BTreeSet<String>,
    /// Input schema.
    pub input_schema: String,
    /// Output schema.
    pub output_schema: String,
    /// Timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: String,
    /// Idempotent.
    pub idempotent: bool,
    /// External effect.
    pub external_effect: bool,
    /// Read authority.
    pub read_authority: BTreeSet<String>,
    /// State scope.
    pub state_scope: String,
}

/// Dependency-owned memory declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyMemoryDeclaration {
    /// Scopes.
    pub scopes: BTreeSet<String>,
    /// Capabilities.
    pub capabilities: BTreeSet<String>,
    /// Byte bound.
    pub bounded_bytes: u64,
}

/// Dependency-owned compaction declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyCompactionDeclaration {
    /// Strategy ID.
    pub strategy_id: String,
    /// Idempotent.
    pub idempotent: bool,
    /// Byte bound.
    pub bounded_bytes: u64,
}

/// Dependency-owned context transform boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyContextTransformBoundary {
    /// Before memory retrieval.
    BeforeMemoryRetrieval,
    /// After memory retrieval.
    AfterMemoryRetrieval,
    /// Before compaction.
    BeforeCompaction,
    /// After compaction.
    AfterCompaction,
    /// Before provider projection.
    BeforeProviderProjection,
    /// Before turn completion.
    BeforeTurnCompletion,
}

/// Dependency-owned context transform declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyContextTransform {
    /// Transform ID.
    pub transform_id: String,
    /// Boundary.
    pub boundary: DependencyContextTransformBoundary,
    /// Stage.
    pub stage: u16,
    /// Priority.
    pub priority: i32,
    /// Before constraints.
    pub before: BTreeSet<String>,
    /// After constraints.
    pub after: BTreeSet<String>,
}

/// Dependency-owned observer delivery semantics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DependencyObserverDelivery {
    /// Best effort.
    BestEffort,
    /// At most once.
    AtMostOnce,
    /// At least once with idempotency key.
    AtLeastOnce {
        /// Maximum attempts.
        max_attempts: u8,
        /// Retry backoff.
        retry_backoff_ms: u64,
    },
}

/// Dependency-owned manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyManifest {
    /// Schema version.
    pub schema_version: u16,
    /// Plugin ID.
    pub id: String,
    /// Plugin version.
    pub version: String,
    /// Runtime API requirement.
    pub runtime_api: String,
    /// Category.
    pub category: String,
    /// Scope.
    pub scope: String,
    /// Class.
    pub class: DependencyPluginClass,
    /// Entrypoint.
    pub entrypoint: DependencyEntrypoint,
    /// Required capabilities.
    pub required_capabilities: BTreeSet<String>,
    /// Provided capabilities.
    pub provided_capabilities: BTreeSet<String>,
    /// Events.
    pub subscribed_events: BTreeSet<String>,
    /// Read authority.
    pub read_authority: BTreeSet<String>,
    /// Proposed writes.
    pub proposed_write_authority: BTreeSet<String>,
    /// Tool permissions.
    pub tool_permissions: BTreeSet<String>,
    /// Network permissions.
    pub network_permissions: BTreeSet<String>,
    /// After constraints.
    pub after: BTreeSet<String>,
    /// Before constraints.
    pub before: BTreeSet<String>,
    /// Stage.
    pub stage: u16,
    /// Priority.
    pub priority: i32,
    /// Timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: String,
    /// Attempts.
    pub max_attempts: u8,
    /// Retry backoff.
    pub retry_backoff_ms: u64,
    /// State version.
    pub state_migration_version: u32,
    /// Config schema.
    pub configuration_schema: DependencyConfigurationSchema,
    /// Graph node executors.
    pub node_executors: Vec<DependencyNodeExecutor>,
    /// Memory declaration.
    pub memory: Option<DependencyMemoryDeclaration>,
    /// Compaction declaration.
    pub compaction: Option<DependencyCompactionDeclaration>,
    /// Context transforms.
    pub context_transforms: Vec<DependencyContextTransform>,
    /// Observer delivery semantics.
    pub observer_delivery: DependencyObserverDelivery,
}

/// Dependency-owned authorization envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Call.
    pub call_id: String,
    /// Digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Cancellation.
    pub cancellation_id: String,
}

/// Load request.
#[derive(Clone, Debug)]
pub struct DependencyLoadRequest {
    /// Manifest.
    pub manifest: DependencyManifest,
    /// Configuration.
    pub configuration: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Invocation request.
#[derive(Clone, Debug)]
pub struct DependencyInvocationRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Handler/tool.
    pub handler: String,
    /// Invocation operation.
    pub operation: String,
    /// Proposal/tool payload kind.
    pub kind: String,
    /// Payload.
    pub payload: Value,
    /// Readable state.
    pub readable_state: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Graph node execution request.
#[derive(Clone, Debug)]
pub struct DependencyNodeExecutionRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Executor.
    pub executor_id: String,
    /// Node ID.
    pub node_id: String,
    /// Node kind.
    pub node_kind: String,
    /// Input.
    pub input: Value,
    /// Variables.
    pub variables: Value,
    /// Readable state.
    pub readable_state: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Memory request (describe/retrieve/commit/health).
#[derive(Clone, Debug)]
pub struct DependencyMemoryRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Scope.
    pub scope: String,
    /// Query.
    pub query: String,
    /// Limit.
    pub limit: usize,
    /// Entries to commit.
    pub entries: Vec<DependencyMemoryItem>,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Memory item.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DependencyMemoryItem {
    /// Reference.
    pub reference: String,
    /// Content.
    pub content: String,
    /// Score.
    pub score: Option<f64>,
    /// Created at millis.
    pub created_at_ms: i64,
}

/// Compaction proposal request.
#[derive(Clone, Debug)]
pub struct DependencyCompactionRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Source range start.
    pub source_range_start: u64,
    /// Source range end.
    pub source_range_end: u64,
    /// Source range hash.
    pub source_range_hash: String,
    /// Current entries.
    pub current_entries: Value,
    /// Proposal.
    pub proposal: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Context transform request.
#[derive(Clone, Debug)]
pub struct DependencyContextTransformRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Transform.
    pub transform_id: String,
    /// Boundary.
    pub boundary: DependencyContextTransformBoundary,
    /// Payload.
    pub payload: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Observer request.
#[derive(Clone, Debug)]
pub struct DependencyObservationRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation (also the idempotency key).
    pub invocation_id: String,
    /// Handler.
    pub handler: String,
    /// Event type.
    pub event_type: String,
    /// Event.
    pub event: Value,
    /// First canonical sequence in the delivered range.
    pub event_range_start: u64,
    /// Last canonical sequence in the delivered range.
    pub event_range_end: u64,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// State-change request.
#[derive(Clone, Debug)]
pub struct DependencyStateChangeRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Reason.
    pub reason: Option<String>,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// Invocation decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyDecision {
    /// Continue.
    Continue(Value),
    /// Replace.
    Replace(Value),
    /// Reject.
    Reject(String),
    /// Tool result.
    ToolResult(Value),
    /// Graph node result.
    NodeResult(Value),
}

/// Memory result.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyMemoryResult {
    /// Describe.
    Describe {
        /// Scopes.
        scopes: BTreeSet<String>,
        /// Capabilities.
        capabilities: BTreeSet<String>,
        /// Byte bound.
        bounded_bytes: u64,
    },
    /// Retrieve.
    Retrieve {
        /// Items.
        items: Vec<DependencyMemoryItem>,
    },
    /// Commit.
    Commit {
        /// Retained.
        retained: bool,
        /// References.
        references: Vec<String>,
    },
    /// Health.
    Health {
        /// Healthy.
        healthy: bool,
        /// Item count.
        item_count: u64,
        /// Retained bytes.
        retained_bytes: u64,
    },
}

/// Load result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLoadResult {
    /// Plugin ID.
    pub plugin_id: String,
    /// State version.
    pub state_version: u32,
    /// Attempts.
    pub attempts: u8,
}

/// Observer enqueue result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyObservationResult {
    /// Accepted.
    pub accepted: bool,
    /// Queue depth.
    pub queue_depth: usize,
    /// Drop count.
    pub dropped: u64,
}

/// Plugin status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginStatus {
    /// Active.
    Active,
    /// Disabled.
    Disabled,
    /// Quarantined.
    Quarantined,
}

/// Loaded plugin record.
#[derive(Clone, Debug)]
pub struct DependencyPluginRecord {
    /// Manifest.
    pub manifest: DependencyManifest,
    /// Status.
    pub status: DependencyPluginStatus,
    /// Drops.
    pub observer_dropped: u64,
    /// Pending durable deliveries.
    pub pending_deliveries: usize,
}

/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHealth {
    /// Loaded.
    pub loaded: usize,
    /// Running.
    pub running: usize,
    /// Drops.
    pub observer_dropped: u64,
    /// Pending durable deliveries.
    pub pending_deliveries: usize,
}

/// Audit entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAudit {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: Option<String>,
    /// Operation.
    pub operation: String,
    /// Outcome.
    pub outcome: String,
    /// Attempts.
    pub attempts: u8,
}

/// Durable observer delivery record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableDeliveryRecord {
    /// Delivery ID (runtime idempotency key).
    pub delivery_id: String,
    /// Plugin ID.
    pub plugin_id: String,
    /// Handler.
    pub handler: String,
    /// Event type.
    pub event_type: String,
    /// Bounded event projection.
    pub event: Value,
    /// First canonical sequence in the delivered range.
    pub event_range_start: u64,
    /// Last canonical sequence in the delivered range.
    pub event_range_end: u64,
    /// Attempts so far.
    pub attempts: u8,
    /// Maximum attempts.
    pub max_attempts: u8,
    /// Retry backoff.
    pub retry_backoff_ms: u64,
    /// Next retry at millis.
    pub next_retry_at_ms: i64,
    /// Terminal outcome when delivered: completed/failed, otherwise pending.
    pub terminal: Option<String>,
}

/// Hard dependency configuration.
#[derive(Clone, Debug)]
pub struct PluginDependencyConfig {
    /// Runtime API.
    pub runtime_api_version: String,
    /// Protocol version.
    pub protocol_version: u16,
    /// Available capabilities.
    pub available_capabilities: BTreeSet<String>,
    /// Authenticated owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Authorization key.
    pub authorization_key_hex: String,
    /// Durable state root.
    pub state_root: PathBuf,
    /// Approved executable roots.
    pub executable_roots: Vec<PathBuf>,
    /// Observer queue.
    pub observer_queue_capacity: usize,
    /// Response limit.
    pub max_response_bytes: usize,
    /// Calls per minute per plugin.
    pub rate_limit_per_minute: usize,
    /// Restart bound.
    pub max_restarts: u8,
    /// Audit ring bound.
    pub audit_capacity: usize,
}

/// Dependency interface.
#[async_trait]
pub trait PluginDependencyPort: Send + Sync {
    /// Negotiates protocol and capabilities.
    async fn negotiate(
        &self,
        protocol_version: u16,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDependencyError>;
    /// Validates a complete set.
    async fn validate_set(
        &self,
        manifests: Vec<DependencyManifest>,
    ) -> Result<Vec<String>, PluginDependencyError>;
    /// Loads and migrates.
    async fn load(
        &self,
        request: DependencyLoadRequest,
    ) -> Result<DependencyLoadResult, PluginDependencyError>;
    /// Gets a loaded record.
    async fn get(&self, plugin_id: String)
    -> Result<DependencyPluginRecord, PluginDependencyError>;
    /// Invokes a blocking handler or tool.
    async fn invoke(
        &self,
        request: DependencyInvocationRequest,
    ) -> Result<(DependencyDecision, u8), PluginDependencyError>;
    /// Executes a declared graph node.
    async fn execute_node(
        &self,
        request: DependencyNodeExecutionRequest,
    ) -> Result<(Value, u8), PluginDependencyError>;
    /// Runs a plugin memory operation.
    async fn memory(
        &self,
        operation: String,
        request: DependencyMemoryRequest,
    ) -> Result<(DependencyMemoryResult, u8), PluginDependencyError>;
    /// Proposes a replacement projection.
    async fn compaction_propose(
        &self,
        request: DependencyCompactionRequest,
    ) -> Result<(Value, u64, u8), PluginDependencyError>;
    /// Runs one context transform.
    async fn context_transform(
        &self,
        request: DependencyContextTransformRequest,
    ) -> Result<(Value, u8), PluginDependencyError>;
    /// Enqueues an observation.
    async fn observe(
        &self,
        request: DependencyObservationRequest,
    ) -> Result<DependencyObservationResult, PluginDependencyError>;
    /// Cancels an invocation.
    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDependencyError>;
    /// Disables.
    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Quarantines.
    async fn quarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Reloads an upgraded plugin.
    async fn reload(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Returns a quarantined plugin to active service.
    async fn unquarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Health.
    async fn health(&self) -> DependencyHealth;
    /// Recent audit entries.
    async fn audits(&self) -> Vec<DependencyAudit>;
    /// Durable delivery records.
    async fn deliveries(&self) -> Vec<DurableDeliveryRecord>;
    /// Number of currently running invocations.
    async fn active_invocations(&self) -> usize;
    /// Number of pending (non-terminal) durable deliveries.
    async fn pending_deliveries(&self) -> usize;
    /// Flushes durable delivery state.
    async fn flush(&self) -> Result<(), PluginDependencyError>;
}

#[derive(Clone)]
struct LoadedPlugin {
    manifest: DependencyManifest,
    configuration: Value,
    status: Arc<RwLock<DependencyPluginStatus>>,
    observer: Option<mpsc::Sender<ObserverWork>>,
    observer_depth: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ObserverWork {
    invocation_id: String,
    handler: String,
    event_type: String,
    event: Value,
    durable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedState {
    version: u32,
    value: Value,
}

#[derive(Debug, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum WorkerRequest<'a> {
    Initialize {
        configuration: &'a Value,
        state_version: u32,
    },
    Migrate {
        from: u32,
        to: u32,
        state: &'a Value,
    },
    Intercept {
        handler: &'a str,
        proposal_type: &'a str,
        proposal: &'a Value,
        readable_state: &'a Value,
    },
    Observe {
        handler: &'a str,
        event_type: &'a str,
        event: &'a Value,
        idempotency_key: &'a str,
    },
    Tool {
        tool: &'a str,
        arguments: &'a Value,
        readable_state: &'a Value,
    },
    ExecuteNode {
        executor_id: &'a str,
        node_id: &'a str,
        node_kind: &'a str,
        input: &'a Value,
        variables: &'a Value,
        readable_state: &'a Value,
    },
    MemoryDescribe,
    MemoryRetrieve {
        scope: &'a str,
        query: &'a str,
        limit: usize,
    },
    MemoryCommitWrite {
        scope: &'a str,
        entries: &'a [DependencyMemoryItem],
    },
    MemoryHealth,
    CompactionPropose {
        source_range_start: u64,
        source_range_end: u64,
        source_range_hash: &'a str,
        current_entries: &'a Value,
        proposal: &'a Value,
    },
    ContextTransform {
        transform_id: &'a str,
        boundary: DependencyContextTransformBoundary,
        payload: &'a Value,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum WorkerResponse {
    Ready,
    State {
        state: Value,
    },
    Continue {
        proposal: Value,
    },
    Replace {
        proposal: Value,
    },
    Reject {
        reason: String,
    },
    ToolResult {
        value: Value,
    },
    NodeResult {
        value: Value,
    },
    Observed,
    MemoryDescribe {
        scopes: BTreeSet<String>,
        capabilities: BTreeSet<String>,
        bounded_bytes: u64,
    },
    MemoryRetrieve {
        items: Vec<DependencyMemoryItem>,
    },
    MemoryCommitWrite {
        retained: bool,
        references: Vec<String>,
    },
    MemoryHealth {
        healthy: bool,
        item_count: u64,
        retained_bytes: u64,
    },
    CompactionProposalAccepted {
        replacement: Value,
        size_bytes: u64,
    },
    TransformResult {
        value: Value,
    },
}

/// Isolated implementation.
#[derive(Clone)]
pub struct IsolatedPluginDependency {
    config: Arc<PluginDependencyConfig>,
    key: Arc<AuthorizationKey>,
    plugins: Arc<Mutex<BTreeMap<String, LoadedPlugin>>>,
    invocations: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    nonces: Arc<Mutex<BTreeMap<String, i64>>>,
    rates: Arc<Mutex<BTreeMap<String, VecDeque<Instant>>>>,
    audits: Arc<Mutex<VecDeque<DependencyAudit>>>,
    deliveries: Arc<Mutex<Vec<DurableDeliveryRecord>>>,
    at_most_once: Arc<Mutex<BTreeMap<String, VecDeque<String>>>>,
    last_activity: Arc<Mutex<i64>>,
}

impl IsolatedPluginDependency {
    /// Constructs the dependency and loads durable replay state.
    ///
    /// # Errors
    ///
    /// Rejects incomplete security or resource configuration.
    pub async fn new(mut config: PluginDependencyConfig) -> Result<Self, PluginDependencyError> {
        if config.owner_id.is_empty()
            || config.session_id.is_empty()
            || config.authorization_key_hex.is_empty()
            || config.state_root.as_os_str().is_empty()
            || config.executable_roots.is_empty()
            || config.observer_queue_capacity == 0
            || config.max_response_bytes == 0
            || config.rate_limit_per_minute == 0
            || config.audit_capacity == 0
        {
            return Err(PluginDependencyError::InvalidConfiguration);
        }
        let key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        fs::create_dir_all(&config.state_root)
            .await
            .map_err(redacted_io)?;
        let state_root = fs::canonicalize(&config.state_root)
            .await
            .map_err(redacted_io)?;
        config.state_root = state_root;
        let mut roots = Vec::with_capacity(config.executable_roots.len());
        for root in &config.executable_roots {
            roots.push(fs::canonicalize(root).await.map_err(redacted_io)?);
        }
        config.executable_roots = roots;
        let nonces = load_json::<BTreeMap<String, i64>>(&config.state_root.join("nonces.json"))
            .await?
            .unwrap_or_default();
        let deliveries =
            load_json::<Vec<DurableDeliveryRecord>>(&config.state_root.join("deliveries.json"))
                .await?
                .unwrap_or_default();
        Ok(Self {
            config: Arc::new(config),
            key: Arc::new(key),
            plugins: Arc::new(Mutex::new(BTreeMap::new())),
            invocations: Arc::new(Mutex::new(BTreeMap::new())),
            nonces: Arc::new(Mutex::new(nonces)),
            rates: Arc::new(Mutex::new(BTreeMap::new())),
            audits: Arc::new(Mutex::new(VecDeque::new())),
            deliveries: Arc::new(Mutex::new(deliveries)),
            at_most_once: Arc::new(Mutex::new(BTreeMap::new())),
            last_activity: Arc::new(Mutex::new(now_millis()?)),
        })
    }

    /// Recovers durable deliveries after a restart.
    ///
    /// Pending at-least-once deliveries whose retry is due are requeued (safe
    /// because the runtime idempotency key deduplicates at the worker). Pending
    /// deliveries past their retry budget or with an ambiguous in-flight
    /// attempt are marked failed without redispatch (fail closed).
    pub async fn recover_deliveries(&self) -> Result<usize, PluginDependencyError> {
        let now = now_millis()?;
        let mut requeued: usize = 0;
        let mut updated = Vec::new();
        let deliveries = self.deliveries.lock().await;
        for mut record in deliveries.iter().cloned() {
            if record.terminal.is_some() {
                continue;
            }
            if record.attempts >= record.max_attempts.max(1) {
                record.terminal = Some(audit_outcome::OBSERVER_DELIVERY_FAILED.to_owned());
            } else if record.next_retry_at_ms <= now {
                if let Some(plugin) = self.plugins.lock().await.get(&record.plugin_id).cloned() {
                    if let Some(sender) = plugin.observer {
                        let work = ObserverWork {
                            invocation_id: record.delivery_id.clone(),
                            handler: record.handler.clone(),
                            event_type: record.event_type.clone(),
                            event: record.event.clone(),
                            durable: true,
                        };
                        if sender.try_send(work).is_ok() {
                            plugin.observer_depth.fetch_add(1, Ordering::AcqRel);
                            requeued = requeued.saturating_add(1);
                            continue;
                        }
                    }
                }
                record.terminal = Some(audit_outcome::OBSERVER_DELIVERY_FAILED.to_owned());
            }
            updated.push(record);
        }
        drop(deliveries);
        if !updated.is_empty() {
            let mut current = self.deliveries.lock().await;
            for record in updated {
                if let Some(slot) = current
                    .iter_mut()
                    .find(|candidate| candidate.delivery_id == record.delivery_id)
                {
                    *slot = record;
                }
            }
            persist_json(&self.config.state_root.join("deliveries.json"), &*current).await?;
        }
        Ok(requeued)
    }

    async fn authorize<T: Serialize>(
        &self,
        action: &str,
        operation: &T,
        authorization: &DependencyAuthorization,
    ) -> Result<(), PluginDependencyError> {
        let canonical =
            serde_json::to_vec(operation).map_err(|_| PluginDependencyError::Invalid)?;
        let digest = ContentHash::digest(&canonical);
        if authorization.owner_id != self.config.owner_id
            || authorization.session_id != self.config.session_id
            || authorization.normalized_digest != digest.to_hex()
        {
            return Err(PluginDependencyError::Authorization);
        }
        let now = now_millis()?;
        let claims = verify_authorization(
            &authorization.grant,
            &self.key,
            ExpectedAuthorization {
                owner: &self.config.owner_id,
                session: &self.config.session_id,
                call_id: &authorization.call_id,
                action,
                normalized_digest: digest,
            },
            TimestampMillis::new(now),
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        let mut nonces = self.nonces.lock().await;
        nonces.retain(|_, expiry| *expiry >= now);
        let nonce = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        if nonces.contains_key(&nonce) {
            return Err(PluginDependencyError::Replay);
        }
        nonces.insert(nonce, claims.expires_at.get());
        persist_json(&self.config.state_root.join("nonces.json"), &*nonces).await
    }

    async fn entry(&self, id: &str) -> Result<LoadedPlugin, PluginDependencyError> {
        self.plugins
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or(PluginDependencyError::NotLoaded)
    }

    async fn audit(&self, audit: DependencyAudit) {
        let mut entries = self.audits.lock().await;
        if entries.len() == self.config.audit_capacity {
            entries.pop_front();
        }
        entries.push_back(audit);
    }

    async fn enforce_rate(&self, plugin_id: &str) -> Result<(), PluginDependencyError> {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(60);
        let mut rates = self.rates.lock().await;
        let entries = rates.entry(plugin_id.to_owned()).or_default();
        while entries.front().is_some_and(|entry| *entry < cutoff) {
            entries.pop_front();
        }
        if entries.len() >= self.config.rate_limit_per_minute {
            return Err(PluginDependencyError::RateLimited);
        }
        entries.push_back(now);
        Ok(())
    }

    async fn touch(&self) {
        if let Ok(mut last) = self.last_activity.try_lock() {
            *last = now_millis().unwrap_or(0);
        }
    }

    async fn invoke_worker(
        &self,
        plugin: &LoadedPlugin,
        invocation_id: &str,
        request: &WorkerRequest<'_>,
    ) -> Result<(WorkerResponse, u8), PluginDependencyError> {
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        self.enforce_rate(&plugin.manifest.id).await?;
        self.touch().await;
        let cancellation = CancellationToken::new();
        {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(invocation_id.to_owned(), cancellation.clone())
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        let configured_attempts = plugin.manifest.max_attempts.max(1);
        let maximum = configured_attempts.min(self.config.max_restarts.saturating_add(1).max(1));
        let mut attempt = 0_u8;
        let result = loop {
            attempt = attempt.saturating_add(1);
            let result = run_once(
                &plugin.manifest,
                request,
                cancellation.clone(),
                self.config.max_response_bytes,
            )
            .await;
            if result.is_ok() || attempt >= maximum || cancellation.is_cancelled() {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(
                plugin.manifest.retry_backoff_ms.min(5_000),
            ))
            .await;
        };
        self.invocations.lock().await.remove(invocation_id);
        result.map(|response| (response, attempt))
    }

    async fn record_delivery(
        &self,
        record: DurableDeliveryRecord,
    ) -> Result<(), PluginDependencyError> {
        let mut deliveries = self.deliveries.lock().await;
        if let Some(slot) = deliveries
            .iter_mut()
            .find(|candidate| candidate.delivery_id == record.delivery_id)
        {
            *slot = record;
        } else {
            deliveries.push(record);
        }
        persist_json(
            &self.config.state_root.join("deliveries.json"),
            &*deliveries,
        )
        .await
    }
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "the dependency trait implementation keeps each isolated operation mapping explicit"
)]
impl PluginDependencyPort for IsolatedPluginDependency {
    async fn negotiate(
        &self,
        protocol_version: u16,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDependencyError> {
        if protocol_version != self.config.protocol_version
            || runtime_api_version != self.config.runtime_api_version
        {
            return Err(PluginDependencyError::Incompatible);
        }
        Ok((
            self.config.protocol_version,
            self.config.runtime_api_version.clone(),
            capabilities
                .intersection(&self.config.available_capabilities)
                .cloned()
                .collect(),
        ))
    }

    async fn validate_set(
        &self,
        manifests: Vec<DependencyManifest>,
    ) -> Result<Vec<String>, PluginDependencyError> {
        let sdk_manifests = manifests
            .iter()
            .map(to_sdk_manifest)
            .collect::<Result<Vec<_>, _>>()?;
        let context = validation_context(&self.config);
        let validated = sdk::validate_plugin_set(&sdk_manifests, &context)
            .map_err(|report| PluginDependencyError::Validation(report.to_string()))?;
        Ok(validated
            .into_iter()
            .map(|plugin| plugin.manifest().identity.id.clone())
            .collect())
    }

    async fn load(
        &self,
        request: DependencyLoadRequest,
    ) -> Result<DependencyLoadResult, PluginDependencyError> {
        self.authorize(
            "plugin.load",
            &(&request.manifest, &request.configuration),
            &request.authorization,
        )
        .await?;
        sdk::validate_manifest(
            &to_sdk_manifest(&request.manifest)?,
            &validation_context(&self.config),
        )
        .map_err(|report| PluginDependencyError::Validation(report.to_string()))?;
        validate_configuration(
            &request.manifest.configuration_schema,
            &request.configuration,
        )?;
        validate_executable(&request.manifest.entrypoint.program, &self.config).await?;
        let state_path = state_path(&self.config.state_root, &request.manifest.id)?;
        let existing = load_json::<PersistedState>(&state_path).await?;
        let mut attempts = 1;
        let state = if let Some(existing) = existing {
            if existing.version > request.manifest.state_migration_version {
                return Err(PluginDependencyError::StateVersion);
            }
            if existing.version < request.manifest.state_migration_version {
                let temporary = LoadedPlugin {
                    manifest: request.manifest.clone(),
                    configuration: request.configuration.clone(),
                    status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                    observer: None,
                    observer_depth: Arc::new(AtomicU64::new(0)),
                    dropped: Arc::new(AtomicU64::new(0)),
                };
                let (response, used) = self
                    .invoke_worker(
                        &temporary,
                        &format!("migration-{}", request.authorization.call_id),
                        &WorkerRequest::Migrate {
                            from: existing.version,
                            to: request.manifest.state_migration_version,
                            state: &existing.value,
                        },
                    )
                    .await?;
                attempts = used;
                match response {
                    WorkerResponse::State { state } => PersistedState {
                        version: request.manifest.state_migration_version,
                        value: state,
                    },
                    _ => return Err(PluginDependencyError::MalformedResponse),
                }
            } else {
                existing
            }
        } else {
            let temporary = LoadedPlugin {
                manifest: request.manifest.clone(),
                configuration: request.configuration.clone(),
                status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                observer: None,
                observer_depth: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
            };
            let (response, used) = self
                .invoke_worker(
                    &temporary,
                    &format!("initialize-{}", request.authorization.call_id),
                    &WorkerRequest::Initialize {
                        configuration: &request.configuration,
                        state_version: request.manifest.state_migration_version,
                    },
                )
                .await?;
            attempts = used;
            if !matches!(response, WorkerResponse::Ready) {
                return Err(PluginDependencyError::MalformedResponse);
            }
            PersistedState {
                version: request.manifest.state_migration_version,
                value: Value::Object(serde_json::Map::new()),
            }
        };
        persist_json(&state_path, &state).await?;
        let status = Arc::new(RwLock::new(DependencyPluginStatus::Active));
        let depth = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let observer = if request.manifest.class == DependencyPluginClass::Observer {
            let (sender, receiver) = mpsc::channel(self.config.observer_queue_capacity);
            tokio::spawn(observer_worker(
                request.manifest.clone(),
                receiver,
                Arc::clone(&depth),
                self.config.max_response_bytes,
                self.clone(),
            ));
            Some(sender)
        } else {
            None
        };
        self.plugins.lock().await.insert(
            request.manifest.id.clone(),
            LoadedPlugin {
                manifest: request.manifest.clone(),
                configuration: request.configuration.clone(),
                status,
                observer,
                observer_depth: depth,
                dropped,
            },
        );
        let audit = DependencyAudit {
            plugin_id: request.manifest.id.clone(),
            invocation_id: None,
            operation: "load".to_owned(),
            outcome: "loaded".to_owned(),
            attempts,
        };
        self.audit(audit).await;
        Ok(DependencyLoadResult {
            plugin_id: request.manifest.id,
            state_version: state.version,
            attempts,
        })
    }

    async fn get(
        &self,
        plugin_id: String,
    ) -> Result<DependencyPluginRecord, PluginDependencyError> {
        let plugin = self.entry(&plugin_id).await?;
        let status = *plugin.status.read().await;
        Ok(DependencyPluginRecord {
            manifest: plugin.manifest,
            status,
            observer_dropped: plugin.dropped.load(Ordering::Acquire),
            pending_deliveries: self
                .deliveries
                .lock()
                .await
                .iter()
                .filter(|record| record.plugin_id == plugin_id && record.terminal.is_none())
                .count(),
        })
    }

    async fn invoke(
        &self,
        request: DependencyInvocationRequest,
    ) -> Result<(DependencyDecision, u8), PluginDependencyError> {
        self.authorize(
            &format!("plugin.{}", request.operation),
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.handler,
                &request.operation,
                &request.kind,
                &request.payload,
                &request.readable_state,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        let worker_request = if request.operation == "intercept" {
            WorkerRequest::Intercept {
                handler: &request.handler,
                proposal_type: &request.kind,
                proposal: &request.payload,
                readable_state: &request.readable_state,
            }
        } else {
            WorkerRequest::Tool {
                tool: &request.handler,
                arguments: &request.payload,
                readable_state: &request.readable_state,
            }
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id.clone(),
            invocation_id: Some(request.invocation_id.clone()),
            operation: request.operation.clone(),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        })
        .await;
        let (response, attempts) = self
            .invoke_worker(&plugin, &request.invocation_id, &worker_request)
            .await?;
        let (decision, outcome) = match response {
            WorkerResponse::Continue { proposal } => (
                DependencyDecision::Continue(proposal),
                audit_outcome::COMPLETED,
            ),
            WorkerResponse::Replace { proposal } => (
                DependencyDecision::Replace(proposal),
                audit_outcome::COMPLETED,
            ),
            WorkerResponse::Reject { reason } => (
                DependencyDecision::Reject(reason),
                audit_outcome::REJECTED_BY_PLUGIN,
            ),
            WorkerResponse::ToolResult { value } => (
                DependencyDecision::ToolResult(value),
                audit_outcome::COMPLETED,
            ),
            _ => return Err(PluginDependencyError::MalformedResponse),
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: request.operation,
            outcome: outcome.to_owned(),
            attempts,
        })
        .await;
        Ok((decision, attempts))
    }

    async fn execute_node(
        &self,
        request: DependencyNodeExecutionRequest,
    ) -> Result<(Value, u8), PluginDependencyError> {
        self.authorize(
            "plugin.execute_node",
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.executor_id,
                &request.node_id,
                &request.node_kind,
                &request.input,
                &request.variables,
                &request.readable_state,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if plugin.manifest.class != DependencyPluginClass::GraphNode {
            return Err(PluginDependencyError::WrongClass);
        }
        let executor = plugin
            .manifest
            .node_executors
            .iter()
            .find(|executor| executor.executor_id == request.executor_id)
            .ok_or(PluginDependencyError::NotLoaded)?;
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id.clone(),
            invocation_id: Some(request.invocation_id.clone()),
            operation: "execute_node".to_owned(),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        })
        .await;
        let (response, attempts) = self
            .invoke_worker(
                &plugin,
                &request.invocation_id,
                &WorkerRequest::ExecuteNode {
                    executor_id: &request.executor_id,
                    node_id: &request.node_id,
                    node_kind: &request.node_kind,
                    input: &request.input,
                    variables: &request.variables,
                    readable_state: &request.readable_state,
                },
            )
            .await?;
        let value = match response {
            WorkerResponse::NodeResult { value } => value,
            _ => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: "execute_node".to_owned(),
                    outcome: audit_outcome::INVALID_RESPONSE.to_owned(),
                    attempts,
                })
                .await;
                return Err(PluginDependencyError::MalformedResponse);
            }
        };
        let timeout_ceiling = executor.timeout_ms;
        if timeout_ceiling == 0 || timeout_ceiling > 300_000 {
            self.audit(DependencyAudit {
                plugin_id: request.plugin_id,
                invocation_id: Some(request.invocation_id),
                operation: "execute_node".to_owned(),
                outcome: audit_outcome::REJECTED_BY_RUNTIME.to_owned(),
                attempts,
            })
            .await;
            return Err(PluginDependencyError::Invalid);
        }
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: "execute_node".to_owned(),
            outcome: audit_outcome::COMPLETED.to_owned(),
            attempts,
        })
        .await;
        Ok((value, attempts))
    }

    async fn memory(
        &self,
        operation: String,
        request: DependencyMemoryRequest,
    ) -> Result<(DependencyMemoryResult, u8), PluginDependencyError> {
        self.authorize(
            &format!("plugin.memory_{operation}"),
            &(
                &request.plugin_id,
                &request.invocation_id,
                &operation,
                &request.scope,
                &request.query,
                &request.limit,
                &request.entries,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if plugin.manifest.class != DependencyPluginClass::Memory {
            return Err(PluginDependencyError::WrongClass);
        }
        let worker_request = match operation.as_str() {
            "describe" => WorkerRequest::MemoryDescribe,
            "retrieve" => WorkerRequest::MemoryRetrieve {
                scope: &request.scope,
                query: &request.query,
                limit: request.limit,
            },
            "commit_write" => WorkerRequest::MemoryCommitWrite {
                scope: &request.scope,
                entries: &request.entries,
            },
            "health" => WorkerRequest::MemoryHealth,
            _ => return Err(PluginDependencyError::Invalid),
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id.clone(),
            invocation_id: Some(request.invocation_id.clone()),
            operation: format!("memory_{operation}"),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        })
        .await;
        let (response, attempts) = self
            .invoke_worker(&plugin, &request.invocation_id, &worker_request)
            .await?;
        let result = match response {
            WorkerResponse::MemoryDescribe {
                scopes,
                capabilities,
                bounded_bytes,
            } => DependencyMemoryResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            },
            WorkerResponse::MemoryRetrieve { items } => DependencyMemoryResult::Retrieve { items },
            WorkerResponse::MemoryCommitWrite {
                retained,
                references,
            } => DependencyMemoryResult::Commit {
                retained,
                references,
            },
            WorkerResponse::MemoryHealth {
                healthy,
                item_count,
                retained_bytes,
            } => DependencyMemoryResult::Health {
                healthy,
                item_count,
                retained_bytes,
            },
            _ => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: format!("memory_{operation}"),
                    outcome: audit_outcome::INVALID_RESPONSE.to_owned(),
                    attempts,
                })
                .await;
                return Err(PluginDependencyError::MalformedResponse);
            }
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: format!("memory_{operation}"),
            outcome: audit_outcome::COMPLETED.to_owned(),
            attempts,
        })
        .await;
        Ok((result, attempts))
    }

    async fn compaction_propose(
        &self,
        request: DependencyCompactionRequest,
    ) -> Result<(Value, u64, u8), PluginDependencyError> {
        self.authorize(
            "plugin.compaction_propose",
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.source_range_start,
                &request.source_range_end,
                &request.source_range_hash,
                &request.current_entries,
                &request.proposal,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if plugin.manifest.class != DependencyPluginClass::Compaction {
            return Err(PluginDependencyError::WrongClass);
        }
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id.clone(),
            invocation_id: Some(request.invocation_id.clone()),
            operation: "compaction_propose".to_owned(),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        })
        .await;
        let (response, attempts) = self
            .invoke_worker(
                &plugin,
                &request.invocation_id,
                &WorkerRequest::CompactionPropose {
                    source_range_start: request.source_range_start,
                    source_range_end: request.source_range_end,
                    source_range_hash: &request.source_range_hash,
                    current_entries: &request.current_entries,
                    proposal: &request.proposal,
                },
            )
            .await?;
        match response {
            WorkerResponse::CompactionProposalAccepted {
                replacement,
                size_bytes,
            } => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: "compaction_propose".to_owned(),
                    outcome: audit_outcome::COMPLETED.to_owned(),
                    attempts,
                })
                .await;
                Ok((replacement, size_bytes, attempts))
            }
            _ => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: "compaction_propose".to_owned(),
                    outcome: audit_outcome::INVALID_RESPONSE.to_owned(),
                    attempts,
                })
                .await;
                Err(PluginDependencyError::MalformedResponse)
            }
        }
    }

    async fn context_transform(
        &self,
        request: DependencyContextTransformRequest,
    ) -> Result<(Value, u8), PluginDependencyError> {
        self.authorize(
            "plugin.context_transform",
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.transform_id,
                &request.boundary,
                &request.payload,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if plugin.manifest.class != DependencyPluginClass::ContextTransform {
            return Err(PluginDependencyError::WrongClass);
        }
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id.clone(),
            invocation_id: Some(request.invocation_id.clone()),
            operation: "context_transform".to_owned(),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        })
        .await;
        let (response, attempts) = self
            .invoke_worker(
                &plugin,
                &request.invocation_id,
                &WorkerRequest::ContextTransform {
                    transform_id: &request.transform_id,
                    boundary: request.boundary,
                    payload: &request.payload,
                },
            )
            .await?;
        match response {
            WorkerResponse::TransformResult { value } => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: "context_transform".to_owned(),
                    outcome: audit_outcome::COMPLETED.to_owned(),
                    attempts,
                })
                .await;
                Ok((value, attempts))
            }
            _ => {
                self.audit(DependencyAudit {
                    plugin_id: request.plugin_id,
                    invocation_id: Some(request.invocation_id),
                    operation: "context_transform".to_owned(),
                    outcome: audit_outcome::INVALID_RESPONSE.to_owned(),
                    attempts,
                })
                .await;
                Err(PluginDependencyError::MalformedResponse)
            }
        }
    }

    async fn observe(
        &self,
        request: DependencyObservationRequest,
    ) -> Result<DependencyObservationResult, PluginDependencyError> {
        self.authorize(
            "plugin.observe",
            &(
                &request.plugin_id,
                &request.invocation_id,
                &request.handler,
                &request.event_type,
                &request.event,
                &request.event_range_start,
                &request.event_range_end,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        let sender = plugin
            .observer
            .as_ref()
            .ok_or(PluginDependencyError::WrongClass)?;
        let delivery = plugin.manifest.observer_delivery.clone();
        match delivery {
            DependencyObserverDelivery::AtMostOnce => {
                let mut seen = self.at_most_once.lock().await;
                let window = seen
                    .entry(request.plugin_id.clone())
                    .or_insert_with(VecDeque::new);
                if window.contains(&request.invocation_id) {
                    let dropped = plugin.dropped.fetch_add(1, Ordering::AcqRel) + 1;
                    self.audit(DependencyAudit {
                        plugin_id: request.plugin_id,
                        invocation_id: Some(request.invocation_id),
                        operation: "observe".to_owned(),
                        outcome: audit_outcome::OBSERVER_DELIVERY_DROPPED.to_owned(),
                        attempts: 1,
                    })
                    .await;
                    return Ok(DependencyObservationResult {
                        accepted: false,
                        queue_depth: usize::try_from(plugin.observer_depth.load(Ordering::Acquire))
                            .unwrap_or(usize::MAX),
                        dropped,
                    });
                }
                window.push_back(request.invocation_id.clone());
                while window.len() > 4096 {
                    window.pop_front();
                }
                drop(seen);
            }
            DependencyObserverDelivery::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            } => {
                let record = DurableDeliveryRecord {
                    delivery_id: request.invocation_id.clone(),
                    plugin_id: request.plugin_id.clone(),
                    handler: request.handler.clone(),
                    event_type: request.event_type.clone(),
                    event: request.event.clone(),
                    event_range_start: request.event_range_start,
                    event_range_end: request.event_range_end,
                    attempts: 0,
                    max_attempts,
                    retry_backoff_ms,
                    next_retry_at_ms: now_millis()?,
                    terminal: None,
                };
                self.record_delivery(record).await?;
            }
            DependencyObserverDelivery::BestEffort => {}
        }
        let work = ObserverWork {
            invocation_id: request.invocation_id,
            handler: request.handler,
            event_type: request.event_type,
            event: request.event,
            durable: matches!(delivery, DependencyObserverDelivery::AtLeastOnce { .. }),
        };
        let accepted = sender.try_send(work).is_ok();
        if accepted {
            plugin.observer_depth.fetch_add(1, Ordering::AcqRel);
        } else {
            plugin.dropped.fetch_add(1, Ordering::AcqRel);
        }
        Ok(DependencyObservationResult {
            accepted,
            queue_depth: usize::try_from(plugin.observer_depth.load(Ordering::Acquire))
                .unwrap_or(usize::MAX),
            dropped: plugin.dropped.load(Ordering::Acquire),
        })
    }

    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDependencyError> {
        let token = self
            .invocations
            .lock()
            .await
            .get(&invocation_id)
            .cloned()
            .ok_or(PluginDependencyError::InvocationNotFound)?;
        token.cancel();
        self.audit(DependencyAudit {
            plugin_id: String::new(),
            invocation_id: Some(invocation_id),
            operation: "cancel".to_owned(),
            outcome: audit_outcome::CANCELLED.to_owned(),
            attempts: 1,
        })
        .await;
        Ok(())
    }

    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize("plugin.disable", &request.plugin_id, &request.authorization)
            .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        *plugin.status.write().await = DependencyPluginStatus::Disabled;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "disable".to_owned(),
            outcome: "disabled".to_owned(),
            attempts: 1,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn quarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize(
            "plugin.quarantine",
            &(&request.plugin_id, &request.reason),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        *plugin.status.write().await = DependencyPluginStatus::Quarantined;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "quarantine".to_owned(),
            outcome: request
                .reason
                .clone()
                .unwrap_or_else(|| audit_outcome::QUARANTINED.to_owned()),
            attempts: 1,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn reload(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize("plugin.reload", &request.plugin_id, &request.authorization)
            .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        let state_path = state_path(&self.config.state_root, &request.plugin_id)?;
        let existing = load_json::<PersistedState>(&state_path)
            .await?
            .ok_or(PluginDependencyError::StateVersion)?;
        if existing.version != plugin.manifest.state_migration_version {
            return Err(PluginDependencyError::StateVersion);
        }
        let temporary = LoadedPlugin {
            manifest: plugin.manifest.clone(),
            configuration: plugin.configuration.clone(),
            status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
            observer: None,
            observer_depth: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        };
        let (response, used) = self
            .invoke_worker(
                &temporary,
                &format!("reload-{}", request.authorization.call_id),
                &WorkerRequest::Initialize {
                    configuration: &plugin.configuration,
                    state_version: plugin.manifest.state_migration_version,
                },
            )
            .await?;
        if !matches!(response, WorkerResponse::Ready) {
            return Err(PluginDependencyError::MalformedResponse);
        }
        *plugin.status.write().await = DependencyPluginStatus::Active;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "reload".to_owned(),
            outcome: "reloaded".to_owned(),
            attempts: used,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn unquarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize(
            "plugin.unquarantine",
            &request.plugin_id,
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        let current = *plugin.status.read().await;
        if current != DependencyPluginStatus::Quarantined {
            return Err(PluginDependencyError::Invalid);
        }
        *plugin.status.write().await = DependencyPluginStatus::Active;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "unquarantine".to_owned(),
            outcome: "active".to_owned(),
            attempts: 1,
        };
        self.audit(audit.clone()).await;
        Ok(audit)
    }

    async fn health(&self) -> DependencyHealth {
        DependencyHealth {
            loaded: self.plugins.lock().await.len(),
            running: self.invocations.lock().await.len(),
            observer_dropped: self
                .plugins
                .lock()
                .await
                .values()
                .map(|plugin| plugin.dropped.load(Ordering::Acquire))
                .sum(),
            pending_deliveries: self
                .deliveries
                .lock()
                .await
                .iter()
                .filter(|record| record.terminal.is_none())
                .count(),
        }
    }

    async fn audits(&self) -> Vec<DependencyAudit> {
        self.audits.lock().await.iter().cloned().collect()
    }

    async fn deliveries(&self) -> Vec<DurableDeliveryRecord> {
        self.deliveries.lock().await.clone()
    }

    async fn active_invocations(&self) -> usize {
        self.invocations.lock().await.len()
    }

    async fn pending_deliveries(&self) -> usize {
        self.deliveries
            .lock()
            .await
            .iter()
            .filter(|record| record.terminal.is_none())
            .count()
    }

    async fn flush(&self) -> Result<(), PluginDependencyError> {
        let deliveries = self.deliveries.lock().await;
        persist_json(
            &self.config.state_root.join("deliveries.json"),
            &*deliveries,
        )
        .await
    }
}

async fn observer_worker(
    manifest: DependencyManifest,
    mut receiver: mpsc::Receiver<ObserverWork>,
    depth: Arc<AtomicU64>,
    maximum: usize,
    dependency: IsolatedPluginDependency,
) {
    while let Some(work) = receiver.recv().await {
        depth.fetch_sub(1, Ordering::AcqRel);
        let idempotency_key = work.invocation_id.clone();
        if work.durable {
            let mut record = dependency
                .deliveries
                .lock()
                .await
                .iter()
                .find(|record| record.delivery_id == idempotency_key)
                .cloned();
            if let Some(mut record) = record.take() {
                if record.terminal.is_some() {
                    continue;
                }
                let outcome = deliver_once(&manifest, &work, maximum, &idempotency_key).await;
                record.attempts = record.attempts.saturating_add(1);
                match outcome {
                    Ok(()) => {
                        record.terminal =
                            Some(audit_outcome::OBSERVER_DELIVERY_COMPLETED.to_owned());
                        let _ = dependency.record_delivery(record.clone()).await;
                        let _ = dependency
                            .audit(DependencyAudit {
                                plugin_id: record.plugin_id.clone(),
                                invocation_id: Some(record.delivery_id.clone()),
                                operation: "observe".to_owned(),
                                outcome: audit_outcome::OBSERVER_DELIVERY_COMPLETED.to_owned(),
                                attempts: record.attempts,
                            })
                            .await;
                    }
                    Err(_) if record.attempts < record.max_attempts.max(1) => {
                        record.next_retry_at_ms = now_millis()
                            .unwrap_or(0)
                            .saturating_add(i64::try_from(record.retry_backoff_ms).unwrap_or(0));
                        let _ = dependency.record_delivery(record.clone()).await;
                        let _ = dependency
                            .audit(DependencyAudit {
                                plugin_id: record.plugin_id.clone(),
                                invocation_id: Some(record.delivery_id.clone()),
                                operation: "observe".to_owned(),
                                outcome: audit_outcome::OBSERVER_DELIVERY_ATTEMPTED.to_owned(),
                                attempts: record.attempts,
                            })
                            .await;
                        tokio::time::sleep(Duration::from_millis(
                            record.retry_backoff_ms.min(5_000),
                        ))
                        .await;
                        if let Some(sender) = dependency
                            .plugins
                            .lock()
                            .await
                            .get(&record.plugin_id)
                            .and_then(|plugin| plugin.observer.clone())
                        {
                            let retried = ObserverWork {
                                invocation_id: record.delivery_id.clone(),
                                handler: record.handler.clone(),
                                event_type: record.event_type.clone(),
                                event: record.event.clone(),
                                durable: true,
                            };
                            if sender.try_send(retried).is_ok() {
                                let plugin = dependency
                                    .plugins
                                    .lock()
                                    .await
                                    .get(&record.plugin_id)
                                    .cloned();
                                if let Some(plugin) = plugin {
                                    plugin.observer_depth.fetch_add(1, Ordering::AcqRel);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        record.terminal = Some(audit_outcome::OBSERVER_DELIVERY_FAILED.to_owned());
                        let _ = dependency.record_delivery(record.clone()).await;
                        let _ = dependency
                            .audit(DependencyAudit {
                                plugin_id: record.plugin_id.clone(),
                                invocation_id: Some(record.delivery_id.clone()),
                                operation: "observe".to_owned(),
                                outcome: audit_outcome::OBSERVER_DELIVERY_FAILED.to_owned(),
                                attempts: record.attempts,
                            })
                            .await;
                    }
                }
            }
        } else {
            let outcome = deliver_once(&manifest, &work, maximum, &idempotency_key).await;
            let outcome = match outcome {
                Ok(()) => audit_outcome::OBSERVER_DELIVERY_COMPLETED,
                Err(_) => audit_outcome::OBSERVER_DELIVERY_FAILED,
            };
            let _ = dependency
                .audit(DependencyAudit {
                    plugin_id: manifest.id.clone(),
                    invocation_id: Some(idempotency_key),
                    operation: "observe".to_owned(),
                    outcome: outcome.to_owned(),
                    attempts: 1,
                })
                .await;
        }
    }
}

async fn deliver_once(
    manifest: &DependencyManifest,
    work: &ObserverWork,
    maximum: usize,
    idempotency_key: &str,
) -> Result<(), PluginDependencyError> {
    let cancellation = CancellationToken::new();
    run_once(
        manifest,
        &WorkerRequest::Observe {
            handler: &work.handler,
            event_type: &work.event_type,
            event: &work.event,
            idempotency_key,
        },
        cancellation,
        maximum,
    )
    .await
    .map(|_| ())
}

async fn run_once(
    manifest: &DependencyManifest,
    request: &WorkerRequest<'_>,
    cancellation: CancellationToken,
    maximum: usize,
) -> Result<WorkerResponse, PluginDependencyError> {
    let mut child = Command::new(&manifest.entrypoint.program);
    child
        .args(&manifest.entrypoint.arguments)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = child.spawn().map_err(redacted_io)?;
    let mut stdin = child.stdin.take().ok_or(PluginDependencyError::Process)?;
    let stdout = child.stdout.take().ok_or(PluginDependencyError::Process)?;
    let mut encoded = serde_json::to_vec(request).map_err(|_| PluginDependencyError::Invalid)?;
    encoded.push(b'\n');
    stdin.write_all(&encoded).await.map_err(redacted_io)?;
    drop(stdin);
    let reader = tokio::spawn(async move {
        let limit = u64::try_from(maximum.saturating_add(1)).unwrap_or(u64::MAX);
        let mut bytes = Vec::new();
        stdout
            .take(limit)
            .read_to_end(&mut bytes)
            .await
            .map_err(redacted_io)?;
        Ok::<Vec<u8>, PluginDependencyError>(bytes)
    });
    let deadline = Instant::now() + Duration::from_millis(manifest.timeout_ms);
    let status = tokio::select! {
        () = cancellation.cancelled() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(PluginDependencyError::Cancelled);
        }
        result = timeout_at(deadline, child.wait()) => {
            match result {
                Ok(Ok(status)) => status,
                Ok(Err(error)) => return Err(redacted_io(error)),
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(PluginDependencyError::Timeout);
                }
            }
        }
    };
    if !status.success() {
        return Err(PluginDependencyError::Crashed);
    }
    let bytes = reader.await.map_err(|_| PluginDependencyError::Process)??;
    if bytes.len() > maximum {
        return Err(PluginDependencyError::ResponseTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| PluginDependencyError::MalformedResponse)
}

fn validation_context(config: &PluginDependencyConfig) -> sdk::ValidationContext {
    sdk::ValidationContext {
        runtime_api_version: config.runtime_api_version.clone(),
        available_capabilities: config.available_capabilities.iter().cloned().collect(),
        maximum_timeout_ms: 300_000,
    }
}

#[allow(clippy::too_many_lines)]
fn to_sdk_manifest(
    manifest: &DependencyManifest,
) -> Result<sdk::PluginManifest, PluginDependencyError> {
    Ok(sdk::PluginManifest {
        schema_version: manifest.schema_version,
        identity: sdk::PluginIdentity {
            id: manifest.id.clone(),
            version: manifest.version.clone(),
            runtime_api: manifest.runtime_api.clone(),
        },
        category: match manifest.category.as_str() {
            "interceptor" => sdk::PluginCategory::Interceptor,
            "observer" => sdk::PluginCategory::Observer,
            "tool" => sdk::PluginCategory::Tool,
            "provider" => sdk::PluginCategory::Provider,
            "memory" => sdk::PluginCategory::Memory,
            "context_transform" => sdk::PluginCategory::ContextTransform,
            "compaction" => sdk::PluginCategory::Compaction,
            "session_style" => sdk::PluginCategory::SessionStyle,
            "graph_node" => sdk::PluginCategory::GraphNode,
            "permission_policy" => sdk::PluginCategory::PermissionPolicy,
            "scheduler" => sdk::PluginCategory::Scheduler,
            "frontend" => sdk::PluginCategory::Frontend,
            "artifact_processor" => sdk::PluginCategory::ArtifactProcessor,
            _ => return Err(PluginDependencyError::Invalid),
        },
        scope: match manifest.scope.as_str() {
            "invocation" => sdk::PluginScope::Invocation,
            "model_call" => sdk::PluginScope::ModelCall,
            "turn" => sdk::PluginScope::Turn,
            "session" => sdk::PluginScope::Session,
            "project" => sdk::PluginScope::Project,
            "user" => sdk::PluginScope::User,
            "runtime" => sdk::PluginScope::Runtime,
            _ => return Err(PluginDependencyError::Invalid),
        },
        classification: match manifest.class {
            DependencyPluginClass::Observer => sdk::PluginClassification::Observer,
            _ => sdk::PluginClassification::Blocking,
        },
        entrypoint: sdk::Entrypoint::Process {
            program: manifest.entrypoint.program.clone(),
            args: manifest.entrypoint.arguments.clone(),
        },
        trust: sdk::TrustLevel::ApprovedThirdParty,
        isolation: sdk::IsolationMode::Process,
        required_capabilities: manifest.required_capabilities.iter().cloned().collect(),
        provided_capabilities: manifest.provided_capabilities.iter().cloned().collect(),
        subscribed_events: manifest.subscribed_events.iter().cloned().collect(),
        authorities: sdk::AuthorityManifest {
            read: manifest
                .read_authority
                .iter()
                .map(|value| parse_authority(value))
                .collect::<Result<Vec<_>, _>>()?,
            proposed_write: manifest
                .proposed_write_authority
                .iter()
                .map(|value| parse_authority(value))
                .collect::<Result<Vec<_>, _>>()?,
        },
        permissions: sdk::PermissionManifest {
            tools: manifest.tool_permissions.iter().cloned().collect(),
            network: manifest.network_permissions.iter().cloned().collect(),
        },
        ordering: sdk::OrderingManifest {
            stage: manifest.stage,
            priority: manifest.priority,
            before: manifest.before.iter().cloned().collect(),
            after: manifest.after.iter().cloned().collect(),
        },
        configuration: sdk::ConfigurationSchemaMetadata {
            schema_id: manifest.configuration_schema.id.clone(),
            schema_version: manifest.configuration_schema.version,
            required: manifest.configuration_schema.required,
            source: sdk::ConfigurationSchemaSource::InlineJson {
                document: manifest.configuration_schema.inline_json.clone(),
            },
        },
        failure_policy: match manifest.failure_policy.as_str() {
            "reject" => sdk::FailurePolicy::Reject,
            "cancel" => sdk::FailurePolicy::Cancel,
            "disable" => sdk::FailurePolicy::Disable,
            "continue" => sdk::FailurePolicy::Continue,
            "retry" => sdk::FailurePolicy::Retry {
                max_attempts: manifest.max_attempts,
                backoff_ms: manifest.retry_backoff_ms,
            },
            _ => return Err(PluginDependencyError::Invalid),
        },
        timeout_ms: manifest.timeout_ms,
        state_migration_version: manifest.state_migration_version,
        node_executors: manifest
            .node_executors
            .iter()
            .map(|executor| sdk::PluginNodeExecutor {
                executor_id: executor.executor_id.clone(),
                version: executor.version.clone(),
                node_kind: executor.node_kind.clone(),
                runtime_api: executor.runtime_api.clone(),
                required_capabilities: executor.required_capabilities.iter().cloned().collect(),
                input_schema: executor.input_schema.clone(),
                output_schema: executor.output_schema.clone(),
                timeout_ms: executor.timeout_ms,
                failure_policy: executor.failure_policy.clone(),
                idempotent: executor.idempotent,
                external_effect: executor.external_effect,
                read_authority: executor.read_authority.iter().cloned().collect(),
                state_scope: executor.state_scope.clone(),
            })
            .collect(),
        memory: manifest
            .memory
            .as_ref()
            .map(|memory| sdk::PluginMemoryDeclaration {
                scopes: memory.scopes.iter().cloned().collect(),
                capabilities: memory.capabilities.iter().cloned().collect(),
                bounded_bytes: memory.bounded_bytes,
            }),
        compaction: manifest.compaction.as_ref().map(|compaction| {
            sdk::PluginCompactionDeclaration {
                strategy_id: compaction.strategy_id.clone(),
                idempotent: compaction.idempotent,
                bounded_bytes: compaction.bounded_bytes,
            }
        }),
        context_transforms: manifest
            .context_transforms
            .iter()
            .map(|transform| sdk::PluginContextTransformDeclaration {
                transform_id: transform.transform_id.clone(),
                boundary: match transform.boundary {
                    DependencyContextTransformBoundary::BeforeMemoryRetrieval => {
                        sdk::PluginContextTransformBoundary::BeforeMemoryRetrieval
                    }
                    DependencyContextTransformBoundary::AfterMemoryRetrieval => {
                        sdk::PluginContextTransformBoundary::AfterMemoryRetrieval
                    }
                    DependencyContextTransformBoundary::BeforeCompaction => {
                        sdk::PluginContextTransformBoundary::BeforeCompaction
                    }
                    DependencyContextTransformBoundary::AfterCompaction => {
                        sdk::PluginContextTransformBoundary::AfterCompaction
                    }
                    DependencyContextTransformBoundary::BeforeProviderProjection => {
                        sdk::PluginContextTransformBoundary::BeforeProviderProjection
                    }
                    DependencyContextTransformBoundary::BeforeTurnCompletion => {
                        sdk::PluginContextTransformBoundary::BeforeTurnCompletion
                    }
                },
                stage: transform.stage,
                priority: transform.priority,
                before: transform.before.iter().cloned().collect(),
                after: transform.after.iter().cloned().collect(),
            })
            .collect(),
        observer_delivery: match &manifest.observer_delivery {
            DependencyObserverDelivery::BestEffort => sdk::PluginObserverDelivery::BestEffort,
            DependencyObserverDelivery::AtMostOnce => sdk::PluginObserverDelivery::AtMostOnce,
            DependencyObserverDelivery::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            } => sdk::PluginObserverDelivery::AtLeastOnce {
                max_attempts: *max_attempts,
                retry_backoff_ms: *retry_backoff_ms,
            },
        },
    })
}

fn parse_authority(value: &str) -> Result<sdk::AuthorityTarget, PluginDependencyError> {
    match value {
        "invocation_state" => Ok(sdk::AuthorityTarget::InvocationState),
        "model_call_state" => Ok(sdk::AuthorityTarget::ModelCallState),
        "turn_state" => Ok(sdk::AuthorityTarget::TurnState),
        "session_state" => Ok(sdk::AuthorityTarget::SessionState),
        "project_state" => Ok(sdk::AuthorityTarget::ProjectState),
        "user_state" => Ok(sdk::AuthorityTarget::UserState),
        "runtime_state" => Ok(sdk::AuthorityTarget::RuntimeState),
        "canonical_state" => Ok(sdk::AuthorityTarget::CanonicalState),
        "derived_index" => Ok(sdk::AuthorityTarget::DerivedIndex),
        "plugin_state" => Ok(sdk::AuthorityTarget::PluginState),
        "external_notification" => Ok(sdk::AuthorityTarget::ExternalNotification),
        _ => Err(PluginDependencyError::Invalid),
    }
}

fn validate_configuration(
    schema: &DependencyConfigurationSchema,
    configuration: &Value,
) -> Result<(), PluginDependencyError> {
    let document: Value = serde_json::from_str(&schema.inline_json)
        .map_err(|_| PluginDependencyError::Configuration)?;
    if schema.required && configuration.is_null() {
        return Err(PluginDependencyError::Configuration);
    }
    if !configuration.is_object() && !configuration.is_null() {
        return Err(PluginDependencyError::Configuration);
    }
    let object = configuration.as_object();
    if let Some(required) = document.get("required").and_then(Value::as_array) {
        for field in required.iter().filter_map(Value::as_str) {
            if object.is_none_or(|values| !values.contains_key(field)) {
                return Err(PluginDependencyError::Configuration);
            }
        }
    }
    if document.get("additionalProperties") == Some(&Value::Bool(false))
        && let (Some(properties), Some(values)) = (
            document.get("properties").and_then(Value::as_object),
            object,
        )
        && values.keys().any(|key| !properties.contains_key(key))
    {
        return Err(PluginDependencyError::Configuration);
    }
    Ok(())
}

async fn validate_executable(
    program: &str,
    config: &PluginDependencyConfig,
) -> Result<(), PluginDependencyError> {
    let path = fs::canonicalize(program)
        .await
        .map_err(|_| PluginDependencyError::Executable)?;
    if !path.is_file()
        || !config
            .executable_roots
            .iter()
            .any(|root| path.starts_with(root))
    {
        return Err(PluginDependencyError::Executable);
    }
    Ok(())
}

fn state_path(root: &Path, plugin_id: &str) -> Result<PathBuf, PluginDependencyError> {
    if plugin_id.is_empty()
        || plugin_id.len() > 128
        || !plugin_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
    {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(root.join(format!("{plugin_id}.state.json")))
}

async fn load_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, PluginDependencyError> {
    let candidates = generation_paths(path).await?;
    let selected = candidates
        .last()
        .cloned()
        .unwrap_or_else(|| path.to_path_buf());
    match fs::read(selected).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| PluginDependencyError::StateCorrupt),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(redacted_io(error)),
    }
}

async fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), PluginDependencyError> {
    let bytes = serde_json::to_vec(value).map_err(|_| PluginDependencyError::Invalid)?;
    let parent = path.parent().ok_or(PluginDependencyError::External)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(PluginDependencyError::External)?;
    let committed = parent.join(format!("{file_name}.gen-{}.json", Uuid::now_v7()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&committed)
        .await
        .map_err(redacted_io)?;
    file.write_all(&bytes).await.map_err(redacted_io)?;
    file.sync_all().await.map_err(redacted_io)?;
    drop(file);
    for old in generation_paths(path).await? {
        if old != committed {
            let _ = fs::remove_file(old).await;
        }
    }
    Ok(())
}

async fn generation_paths(path: &Path) -> Result<Vec<PathBuf>, PluginDependencyError> {
    let parent = path.parent().ok_or(PluginDependencyError::External)?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(PluginDependencyError::External)?;
    let prefix = format!("{file_name}.gen-");
    let mut entries = fs::read_dir(parent).await.map_err(redacted_io)?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(redacted_io)? {
        let candidate = entry.path();
        if candidate.file_name().is_some_and(|value| {
            let value = value.to_string_lossy();
            value.starts_with(&prefix) && value.ends_with(".json")
        }) {
            paths.push(candidate);
        }
    }
    paths.sort();
    Ok(paths)
}

fn now_millis() -> Result<i64, PluginDependencyError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PluginDependencyError::Authorization)?
        .as_millis();
    i64::try_from(millis).map_err(|_| PluginDependencyError::Authorization)
}

fn redacted_io(_error: std::io::Error) -> PluginDependencyError {
    PluginDependencyError::External
}

/// Redacted dependency error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginDependencyError {
    /// Configuration.
    #[error("plugin dependency configuration is invalid")]
    InvalidConfiguration,
    /// Input.
    #[error("plugin request is invalid")]
    Invalid,
    /// Version.
    #[error("plugin protocol or API is incompatible")]
    Incompatible,
    /// Validation.
    #[error("plugin validation failed: {0}")]
    Validation(String),
    /// Configuration schema.
    #[error("plugin configuration is invalid")]
    Configuration,
    /// Authorization.
    #[error("plugin authorization denied")]
    Authorization,
    /// Replay.
    #[error("plugin authorization replay denied")]
    Replay,
    /// Executable.
    #[error("plugin executable is unavailable or outside approved roots")]
    Executable,
    /// Not loaded.
    #[error("plugin is not loaded")]
    NotLoaded,
    /// Inactive.
    #[error("plugin is disabled or quarantined")]
    Inactive,
    /// Wrong class.
    #[error("plugin operation is incompatible with its class")]
    WrongClass,
    /// Duplicate.
    #[error("plugin invocation ID is already active")]
    DuplicateInvocation,
    /// Missing invocation.
    #[error("plugin invocation was not found")]
    InvocationNotFound,
    /// Rate.
    #[error("plugin invocation rate exceeded")]
    RateLimited,
    /// Timeout.
    #[error("plugin invocation timed out")]
    Timeout,
    /// Cancelled.
    #[error("plugin invocation was cancelled")]
    Cancelled,
    /// Crash.
    #[error("plugin process crashed")]
    Crashed,
    /// Process.
    #[error("plugin process failed")]
    Process,
    /// Response.
    #[error("plugin response was malformed")]
    MalformedResponse,
    /// Bound.
    #[error("plugin response exceeded its bound")]
    ResponseTooLarge,
    /// State version.
    #[error("plugin state version is incompatible")]
    StateVersion,
    /// State.
    #[error("plugin state is corrupt")]
    StateCorrupt,
    /// External.
    #[error("plugin dependency operation failed")]
    External,
}
