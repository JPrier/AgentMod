//! Supervised runtime-to-plugin-host process transport.
#![allow(
    missing_docs,
    reason = "dependency-local transport records are self-describing"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_plugin_protocol as wire;
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};

#[derive(Clone, Debug)]
pub struct ProcessPluginDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub owner_id: String,
    pub runtime_api_version: String,
    pub sessions_root: PathBuf,
    pub executable_roots: Vec<PathBuf>,
    pub authorization_key: [u8; 32],
    pub maximum_frame_bytes: usize,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginLoadRequest {
    pub session_id: String,
    pub manifest_json: String,
    pub configuration: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginInvocationRequest {
    pub cancellation_target: DependencyPluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginObservationRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginNodeInvocationRequest {
    pub cancellation_target: DependencyPluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub executor_id: String,
    pub executor_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: ContentHash,
    pub node_kind: String,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyPluginContextTransformLifecycle {
    BeforeModelRequest,
}

#[derive(Clone, Debug)]
pub struct DependencyPluginContextTransformInvocationRequest {
    pub cancellation_target: DependencyPluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub transform_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: ContentHash,
    pub lifecycle: DependencyPluginContextTransformLifecycle,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyPluginNodeStateScope {
    Invocation,
    ModelCall,
    Turn,
    Session,
    Project,
    User,
    Runtime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPersistPluginNodeStateRequest {
    pub cancellation_target: DependencyPluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: DependencyPluginNodeStateScope,
    pub prior_generation: u64,
    pub prior_state_hash: Option<ContentHash>,
    pub state: Value,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginNodeStateReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: DependencyPluginNodeStateScope,
    pub prior_generation: u64,
    pub generation: u64,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyLoadPluginNodeStateRequest {
    pub cancellation_target: DependencyPluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: DependencyPluginNodeStateScope,
    pub expected_generation: u64,
    pub expected_state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginNodeStateReadReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: DependencyPluginNodeStateScope,
    pub generation: u64,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyLoadedPluginNodeState {
    pub state: Value,
    pub receipt: DependencyPluginNodeStateReadReceipt,
}

/// Hashes the exact terminal plugin-node state receipt identity.
///
/// # Errors
///
/// Returns [`PluginDependencyError::InvalidResponse`] if the identity cannot
/// be encoded.
pub fn plugin_node_state_receipt_digest(
    receipt: &DependencyPluginNodeStateReceipt,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        &receipt.plugin_id,
        &receipt.invocation_id,
        receipt.invocation_digest,
        &receipt.executor_id,
        &receipt.executor_version,
        receipt.executor_declaration_hash,
        receipt.state_scope,
        receipt.prior_generation,
        receipt.generation,
        receipt.state_hash,
        receipt.action_digest,
        receipt.authorization_digest,
        &receipt.idempotency_key,
        &receipt.receipt_id,
    ))
    .map(|encoded| ContentHash::digest(&encoded))
    .map_err(|_| PluginDependencyError::InvalidResponse)
}

/// Hashes the immutable identity of one terminal plugin-node state read.
///
/// # Errors
///
/// Returns [`PluginDependencyError::InvalidResponse`] when encoding fails.
pub fn plugin_node_state_read_receipt_digest(
    receipt: &DependencyPluginNodeStateReadReceipt,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        &receipt.plugin_id,
        &receipt.invocation_id,
        receipt.invocation_digest,
        &receipt.executor_id,
        &receipt.executor_version,
        receipt.executor_declaration_hash,
        receipt.state_scope,
        receipt.generation,
        receipt.state_hash,
        receipt.action_digest,
        receipt.authorization_digest,
        &receipt.idempotency_key,
        &receipt.receipt_id,
    ))
    .map(|encoded| ContentHash::digest(&encoded))
    .map_err(|_| PluginDependencyError::InvalidResponse)
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginNodeActionProposal {
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginNodeOutcome {
    pub output: Value,
    pub preserved_state: Value,
    pub proposed_actions: Vec<DependencyPluginNodeActionProposal>,
    pub attempts: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginContextTransformProposal {
    pub replacement: Value,
    pub attempts: u8,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyPluginMemoryScope {
    Session,
    Project,
    User,
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginSecurityClassification {
    Public,
    Internal,
    Private,
    Confidential,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginCanonicalReferenceKind {
    Artifact,
    NodeResult,
    ToolResult,
    ApprovalResult,
    Continuation,
    ChildSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginCanonicalReference {
    pub kind: DependencyPluginCanonicalReferenceKind,
    pub id: String,
    pub content_hash: Option<ContentHash>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginArtifactReference {
    pub artifact_id: String,
    pub content_hash: ContentHash,
    pub media_type: String,
    pub size_bytes: u64,
    pub security_classification: DependencyPluginSecurityClassification,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginOperationBinding {
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub operation_id: String,
    pub session_id: String,
    pub run_id: String,
    pub node_id: Option<String>,
    pub declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub request_hash: ContentHash,
    pub idempotency_key: String,
    pub attempt: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyPluginInvocationCancellationTarget {
    pub session_id: String,
    pub run_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub operation_id: String,
    pub declaration_hash: ContentHash,
    pub request_hash: ContentHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginInvocationCancellationStatus {
    Signalled,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginInvocationCancellationReceipt {
    pub target: DependencyPluginInvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: ContentHash,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: DependencyPluginInvocationCancellationStatus,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCancelPluginInvocationRequest {
    pub target: DependencyPluginInvocationCancellationTarget,
    pub reason_code: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginLifecycleAction {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLifecycleRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub configuration_reference: ContentHash,
    pub action: DependencyPluginLifecycleAction,
    pub reason_code: Option<String>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLifecycleResult {
    pub plugin_id: String,
    pub state: String,
    pub audit_operation: String,
    pub audit_outcome: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryRetrieveInput {
    pub query: String,
    pub scopes: BTreeSet<DependencyPluginMemoryScope>,
    pub max_items: u32,
    pub max_bytes: u64,
    pub artifacts: Vec<DependencyPluginArtifactReference>,
    pub references: Vec<DependencyPluginCanonicalReference>,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryRetrieveRequest {
    pub binding: DependencyPluginOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub max_attempts: u8,
    pub retry_backoff: Duration,
    pub timeout: Duration,
    pub input: DependencyPluginMemoryRetrieveInput,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryItemProposal {
    pub item_id: String,
    pub scope: DependencyPluginMemoryScope,
    pub value: Value,
    pub value_hash: ContentHash,
    pub artifacts: Vec<DependencyPluginArtifactReference>,
    pub references: Vec<DependencyPluginCanonicalReference>,
    pub security_classification: DependencyPluginSecurityClassification,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryRetrieveProposal {
    pub binding: DependencyPluginOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub items: Vec<DependencyPluginMemoryItemProposal>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginMemoryWriteBoundary {
    Explicit,
    TurnCompletion,
    IterationCompletion,
    SessionCompletion,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryWriteInput {
    pub scope: DependencyPluginMemoryScope,
    pub boundary: DependencyPluginMemoryWriteBoundary,
    pub value: Value,
    pub value_hash: ContentHash,
    pub artifacts: Vec<DependencyPluginArtifactReference>,
    pub references: Vec<DependencyPluginCanonicalReference>,
    pub security_classification: DependencyPluginSecurityClassification,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryWriteRequest {
    pub binding: DependencyPluginOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout: Duration,
    pub input: DependencyPluginMemoryWriteInput,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginMemoryWriteReceipt {
    pub binding: DependencyPluginOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub provider_record_id: String,
    pub value_hash: ContentHash,
    pub receipt: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginCompactionInput {
    pub projection: Value,
    pub projection_hash: ContentHash,
    pub required_references: Vec<DependencyPluginCanonicalReference>,
    pub required_artifacts: Vec<DependencyPluginArtifactReference>,
    pub preservation_requirements: BTreeSet<String>,
    pub max_replacement_bytes: u64,
    pub max_projection_tokens: u64,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginCompactionRequest {
    pub binding: DependencyPluginOperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub handler: String,
    pub max_attempts: u8,
    pub retry_backoff: Duration,
    pub timeout: Duration,
    pub input: DependencyPluginCompactionInput,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyPluginCompactionProposal {
    pub binding: DependencyPluginOperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub replacement: Value,
    pub replacement_hash: ContentHash,
    pub preserved_references: Vec<DependencyPluginCanonicalReference>,
    pub preserved_artifacts: Vec<DependencyPluginArtifactReference>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyPluginDecision {
    Continue(Value),
    Replace(Value),
    Reject(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginLoadResult {
    pub plugin_id: String,
    pub state_version: u32,
    pub attempts: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginObservationResult {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
    pub status: DependencyPluginObserverDeliveryStatus,
    pub request_hash: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyPluginObserverDeliveryStatus {
    Completed,
    Rejected,
    Failed,
    Ambiguous,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPluginManifestSource {
    pub locator: String,
    pub format: String,
    pub contents: String,
}

/// Reads exact bounded plugin manifest files without interpreting them.
///
/// # Errors
///
/// Returns a classified dependency error for an unsupported extension,
/// non-file source, size/count violation, or unavailable file.
pub async fn read_plugin_manifest_sources(
    paths: &[PathBuf],
) -> Result<Vec<DependencyPluginManifestSource>, PluginDependencyError> {
    if paths.len() > 256 {
        return Err(PluginDependencyError::InvalidRequest);
    }
    let mut sources = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::metadata(path)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        if !metadata.is_file() || metadata.len() > 1024 * 1024 {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let format = match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => "toml",
            Some("json") => "json",
            _ => return Err(PluginDependencyError::InvalidRequest),
        };
        let contents = fs::read_to_string(path)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        sources.push(DependencyPluginManifestSource {
            locator: path.to_string_lossy().into_owned(),
            format: format.to_owned(),
            contents,
        });
    }
    Ok(sources)
}

#[async_trait]
pub trait RuntimePluginDependencyPort: Send + Sync {
    async fn negotiate(
        &self,
        session_id: String,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, PluginDependencyError>;

    async fn validate_set(
        &self,
        session_id: String,
        manifests_json: Vec<String>,
    ) -> Result<Vec<String>, PluginDependencyError>;

    async fn load(
        &self,
        request: DependencyPluginLoadRequest,
    ) -> Result<DependencyPluginLoadResult, PluginDependencyError>;

    async fn invoke(
        &self,
        request: DependencyPluginInvocationRequest,
    ) -> Result<(DependencyPluginDecision, u8), PluginDependencyError>;

    async fn observe(
        &self,
        request: DependencyPluginObservationRequest,
    ) -> Result<DependencyPluginObservationResult, PluginDependencyError>;

    async fn invoke_node_executor(
        &self,
        request: DependencyPluginNodeInvocationRequest,
    ) -> Result<DependencyPluginNodeOutcome, PluginDependencyError>;

    async fn invoke_context_transform(
        &self,
        _request: DependencyPluginContextTransformInvocationRequest,
    ) -> Result<DependencyPluginContextTransformProposal, PluginDependencyError> {
        Err(PluginDependencyError::ContextTransformUnsupported)
    }

    async fn retrieve_memory(
        &self,
        _request: DependencyPluginMemoryRetrieveRequest,
    ) -> Result<DependencyPluginMemoryRetrieveProposal, PluginDependencyError> {
        Err(PluginDependencyError::MemoryOperationUnsupported)
    }

    async fn write_memory(
        &self,
        _request: DependencyPluginMemoryWriteRequest,
    ) -> Result<DependencyPluginMemoryWriteReceipt, PluginDependencyError> {
        Err(PluginDependencyError::MemoryOperationUnsupported)
    }

    async fn compact_context(
        &self,
        _request: DependencyPluginCompactionRequest,
    ) -> Result<DependencyPluginCompactionProposal, PluginDependencyError> {
        Err(PluginDependencyError::MemoryOperationUnsupported)
    }

    /// Authenticates and signals one exact plugin invocation.
    ///
    /// Implementations without protocol-v7-or-newer exact cancellation fail closed.
    ///
    /// The current newline request/response process transport is serialized per
    /// session. This method is authority-correct and validates exact receipts,
    /// but cannot preempt another in-flight exchange until a correlated
    /// multiplexed control channel is introduced. Callers must not treat a
    /// timeout here as proof that the original invocation did or did not stop.
    async fn cancel_plugin_invocation(
        &self,
        _request: DependencyCancelPluginInvocationRequest,
    ) -> Result<DependencyPluginInvocationCancellationReceipt, PluginDependencyError> {
        Err(PluginDependencyError::CancellationUnsupported)
    }

    async fn change_plugin_lifecycle(
        &self,
        _request: DependencyPluginLifecycleRequest,
    ) -> Result<DependencyPluginLifecycleResult, PluginDependencyError> {
        Err(PluginDependencyError::LifecycleManagementUnsupported)
    }

    /// Persists runtime-validated plugin-node state through the plugin host.
    ///
    /// Implementations without protocol-v3 CAS support must fail closed.
    /// Lifecycle `StateChanged` responses are not state receipts.
    async fn persist_plugin_node_state(
        &self,
        _request: DependencyPersistPluginNodeStateRequest,
    ) -> Result<DependencyPluginNodeStateReceipt, PluginDependencyError> {
        Err(PluginDependencyError::StatePersistenceUnsupported)
    }

    /// Loads one exact bounded plugin-node state generation.
    ///
    /// Implementations without protocol-v4 authenticated reads fail closed.
    async fn load_plugin_node_state(
        &self,
        _request: DependencyLoadPluginNodeStateRequest,
    ) -> Result<DependencyLoadedPluginNodeState, PluginDependencyError> {
        Err(PluginDependencyError::StateReadUnsupported)
    }

    /// Tears down one session host only after runtime and host both prove idle.
    async fn teardown_session_if_idle(
        &self,
        _session_id: &str,
        _active_continuations: usize,
        _pending_observer_deliveries: usize,
    ) -> Result<bool, PluginDependencyError> {
        Ok(false)
    }

    async fn shutdown(&self);
}

const MAX_PLUGIN_PENDING_REQUESTS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PluginTransportFailure {
    Unavailable,
    InvalidResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingPluginOperation {
    NodeInvocation,
    ContextTransform,
    MemoryRetrieve,
    MemoryWrite,
    Compaction,
    StateCas,
    StateRead,
    Interceptor,
    Observer,
    Lifecycle,
    OtherTransport,
}

impl PendingPluginOperation {
    const fn blocks_teardown(self) -> bool {
        matches!(
            self,
            Self::NodeInvocation
                | Self::ContextTransform
                | Self::MemoryRetrieve
                | Self::MemoryWrite
                | Self::Compaction
                | Self::StateCas
                | Self::StateRead
                | Self::Interceptor
                | Self::Observer
                | Self::Lifecycle
                | Self::OtherTransport
        )
    }
}

struct PendingPluginResponse {
    operation: PendingPluginOperation,
    sender: oneshot::Sender<Result<wire::PluginResponse, PluginTransportFailure>>,
}

#[derive(Clone)]
struct Connection {
    child: Arc<Mutex<Child>>,
    outbound: mpsc::Sender<Vec<u8>>,
    pending: Arc<Mutex<BTreeMap<String, PendingPluginResponse>>>,
    failed: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct ProcessPluginDependency {
    config: Arc<ProcessPluginDependencyConfig>,
    key: Arc<AuthorizationKey>,
    connections: Arc<Mutex<BTreeMap<String, Connection>>>,
}

impl ProcessPluginDependency {
    /// Creates a validated supervised plugin-host transport.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDependencyError::InvalidConfiguration`] when a required
    /// executable, root, key, frame bound, or timeout is absent.
    pub fn new(config: ProcessPluginDependencyConfig) -> Result<Self, PluginDependencyError> {
        if config.program.trim().is_empty()
            || config.owner_id.trim().is_empty()
            || config.runtime_api_version.trim().is_empty()
            || config.sessions_root.as_os_str().is_empty()
            || config.executable_roots.is_empty()
            || config.authorization_key == [0; 32]
            || config.maximum_frame_bytes == 0
            || config.request_timeout.is_zero()
        {
            return Err(PluginDependencyError::InvalidConfiguration);
        }
        Ok(Self {
            key: Arc::new(AuthorizationKey::from_bytes(config.authorization_key)),
            config: Arc::new(config),
            connections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    #[must_use]
    pub fn derive_authorization_key(seed: &[u8]) -> [u8; 32] {
        *blake3::hash(seed).as_bytes()
    }

    async fn start(&self, session_id: &str) -> Result<Connection, PluginDependencyError> {
        validate_id(session_id)?;
        let working_directory = self.config.sessions_root.join(session_id);
        fs::create_dir_all(&working_directory)
            .await
            .map_err(|_| PluginDependencyError::Unavailable)?;
        let executable_roots = self
            .config
            .executable_roots
            .iter()
            .map(|root| root.to_string_lossy())
            .collect::<Vec<_>>()
            .join(";");
        let mut child = Command::new(&self.config.program);
        child
            .args(&self.config.arguments)
            .current_dir(working_directory)
            .env_clear()
            .env("AGENTMOD_PLUGIN_OWNER", &self.config.owner_id)
            .env("AGENTMOD_PLUGIN_SESSION", session_id)
            .env(
                "AGENTMOD_PLUGIN_RUNTIME_API_VERSION",
                &self.config.runtime_api_version,
            )
            .env(
                "AGENTMOD_PLUGIN_AUTH_KEY",
                encode_hex(&self.config.authorization_key),
            )
            .env("AGENTMOD_PLUGIN_EXECUTABLE_ROOTS", executable_roots)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = child
            .spawn()
            .map_err(|_| PluginDependencyError::Unavailable)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(PluginDependencyError::Unavailable)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(PluginDependencyError::Unavailable)?;
        Ok(start_plugin_transport(
            child,
            stdin,
            stdout,
            self.config.maximum_frame_bytes,
        ))
    }

    async fn exchange(
        &self,
        session_id: &str,
        command: wire::PluginCommand,
    ) -> Result<wire::PluginResponse, PluginDependencyError> {
        self.exchange_with_timeout(session_id, command, self.config.request_timeout)
            .await
    }

    async fn exchange_with_timeout(
        &self,
        session_id: &str,
        command: wire::PluginCommand,
        operation_timeout: Duration,
    ) -> Result<wire::PluginResponse, PluginDependencyError> {
        if operation_timeout.is_zero() {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let correlation_id = uuid::Uuid::now_v7().to_string();
        let operation = pending_plugin_operation(&command);
        let frame = wire::PluginRequestFrame {
            correlation_id: correlation_id.clone(),
            command,
        };
        frame
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let bytes =
            serde_json::to_vec(&frame).map_err(|_| PluginDependencyError::InvalidResponse)?;
        if bytes.len() > self.config.maximum_frame_bytes {
            return Err(PluginDependencyError::FrameTooLarge);
        }
        let connection = {
            let mut connections = self.connections.lock().await;
            if connections
                .get(session_id)
                .is_some_and(|connection| connection.failed.load(Ordering::Acquire))
            {
                connections.remove(session_id);
            }
            if !connections.contains_key(session_id) {
                let connection = self.start(session_id).await?;
                connections.insert(session_id.to_owned(), connection);
            }
            connections
                .get(session_id)
                .cloned()
                .ok_or(PluginDependencyError::Unavailable)?
        };
        if connection.failed.load(Ordering::Acquire) || connection.closing.load(Ordering::Acquire) {
            return Err(PluginDependencyError::Unavailable);
        }
        let (sender, receiver) = oneshot::channel();
        {
            let connections = self.connections.lock().await;
            if connection.closing.load(Ordering::Acquire)
                || connections
                    .get(session_id)
                    .is_none_or(|registered| !Arc::ptr_eq(&registered.child, &connection.child))
            {
                return Err(PluginDependencyError::Unavailable);
            }
            let mut pending = connection.pending.lock().await;
            if pending.len() >= MAX_PLUGIN_PENDING_REQUESTS {
                return Err(PluginDependencyError::PendingRequestLimit);
            }
            if pending
                .insert(
                    correlation_id.clone(),
                    PendingPluginResponse { operation, sender },
                )
                .is_some()
            {
                return Err(PluginDependencyError::InvalidResponse);
            }
            drop(connections);
        }
        let exchange = async {
            if connection.outbound.send(bytes).await.is_err() {
                connection.pending.lock().await.remove(&correlation_id);
                fail_plugin_connection(
                    &connection.child,
                    &connection.pending,
                    &connection.failed,
                    PluginTransportFailure::Unavailable,
                )
                .await;
                return Err(PluginDependencyError::Unavailable);
            }
            match receiver.await {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(PluginTransportFailure::Unavailable)) | Err(_) => {
                    Err(PluginDependencyError::Unavailable)
                }
                Ok(Err(PluginTransportFailure::InvalidResponse)) => {
                    Err(PluginDependencyError::InvalidResponse)
                }
            }
        };
        timeout(operation_timeout.min(self.config.request_timeout), exchange)
            .await
            .map_err(|_| PluginDependencyError::Timeout)?
    }

    fn authorization<T: Serialize>(
        &self,
        session_id: &str,
        call_id: String,
        cancellation_id: String,
        action: &str,
        operation: &T,
    ) -> Result<wire::PluginAuthorization, PluginDependencyError> {
        let bytes =
            serde_json::to_vec(operation).map_err(|_| PluginDependencyError::InvalidRequest)?;
        let digest = ContentHash::digest(&bytes);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PluginDependencyError::Clock)?;
        let issued_at = i64::try_from(now.as_millis()).map_err(|_| PluginDependencyError::Clock)?;
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: self.config.owner_id.clone(),
                session: session_id.to_owned(),
                call_id: call_id.clone(),
                action: action.to_owned(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(issued_at),
                expires_at: TimestampMillis::new(issued_at.saturating_add(30_000)),
                nonce: uuid::Uuid::now_v7().to_string(),
            },
            &self.key,
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        Ok(wire::PluginAuthorization {
            owner_id: self.config.owner_id.clone(),
            session_id: session_id.to_owned(),
            call_id,
            normalized_digest: digest.to_hex(),
            grant,
            cancellation_id,
        })
    }

    fn cancellation_authorization(
        &self,
        target: &DependencyPluginInvocationCancellationTarget,
        call_id: String,
        cancellation_id: String,
        explicit_nonce: &str,
        action_digest: ContentHash,
    ) -> Result<wire::PluginAuthorization, PluginDependencyError> {
        if target.session_id.is_empty()
            || explicit_nonce.is_empty()
            || cancellation_id.is_empty()
            || call_id.is_empty()
        {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PluginDependencyError::Clock)?;
        let issued_at = i64::try_from(now.as_millis()).map_err(|_| PluginDependencyError::Clock)?;
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: self.config.owner_id.clone(),
                session: target.session_id.clone(),
                call_id: call_id.clone(),
                action: String::from("plugin.invocation.cancel"),
                normalized_digest: action_digest,
                issued_at: TimestampMillis::new(issued_at),
                expires_at: TimestampMillis::new(issued_at.saturating_add(30_000)),
                nonce: explicit_nonce.to_owned(),
            },
            &self.key,
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        Ok(wire::PluginAuthorization {
            owner_id: self.config.owner_id.clone(),
            session_id: target.session_id.clone(),
            call_id,
            normalized_digest: action_digest.to_hex(),
            grant,
            cancellation_id,
        })
    }
}

#[async_trait]
impl RuntimePluginDependencyPort for ProcessPluginDependency {
    async fn negotiate(
        &self,
        session_id: String,
        runtime_api_version: String,
        capabilities: BTreeSet<String>,
    ) -> Result<BTreeSet<String>, PluginDependencyError> {
        match self
            .exchange(
                &session_id,
                wire::PluginCommand::Negotiate {
                    protocol_version: wire::CURRENT_PROTOCOL_VERSION,
                    runtime_api_version,
                    capabilities,
                },
            )
            .await?
        {
            wire::PluginResponse::Negotiated { capabilities, .. } => Ok(capabilities),
            response => Err(map_failure(response)),
        }
    }

    async fn validate_set(
        &self,
        session_id: String,
        manifests_json: Vec<String>,
    ) -> Result<Vec<String>, PluginDependencyError> {
        let manifests = manifests_json
            .iter()
            .map(|manifest| {
                serde_json::from_str(manifest).map_err(|_| PluginDependencyError::InvalidRequest)
            })
            .collect::<Result<Vec<wire::PluginManifest>, _>>()?;
        match self
            .exchange(&session_id, wire::PluginCommand::ValidateSet { manifests })
            .await?
        {
            wire::PluginResponse::SetValidated { plugin_ids } => Ok(plugin_ids),
            response => Err(map_failure(response)),
        }
    }

    async fn load(
        &self,
        request: DependencyPluginLoadRequest,
    ) -> Result<DependencyPluginLoadResult, PluginDependencyError> {
        let manifest: wire::PluginManifest = serde_json::from_str(&request.manifest_json)
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let call_id = uuid::Uuid::now_v7().to_string();
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.load",
            &(&manifest, &request.configuration),
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Load {
                    manifest: Box::new(manifest),
                    configuration: request.configuration,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Loaded {
                plugin_id,
                state_version,
                audit,
            } => Ok(DependencyPluginLoadResult {
                plugin_id,
                state_version,
                attempts: audit.attempts,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn invoke(
        &self,
        request: DependencyPluginInvocationRequest,
    ) -> Result<(DependencyPluginDecision, u8), PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let cancellation_target = map_cancellation_target(&request.cancellation_target);
        let operation = (
            &cancellation_target,
            &request.plugin_id,
            &request.invocation_id,
            &request.handler,
            "intercept",
            &request.kind,
            &request.payload,
            &request.readable_state,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.intercept",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Intercept {
                    cancellation_target,
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    handler: request.handler,
                    proposal_type: request.kind,
                    proposal: request.payload,
                    readable_state: request.readable_state,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Continue { proposal, audit } => {
                Ok((DependencyPluginDecision::Continue(proposal), audit.attempts))
            }
            wire::PluginResponse::Replace { proposal, audit } => {
                Ok((DependencyPluginDecision::Replace(proposal), audit.attempts))
            }
            wire::PluginResponse::Reject { reason, audit } => {
                Ok((DependencyPluginDecision::Reject(reason), audit.attempts))
            }
            response => Err(map_failure(response)),
        }
    }

    async fn observe(
        &self,
        request: DependencyPluginObservationRequest,
    ) -> Result<DependencyPluginObservationResult, PluginDependencyError> {
        let call_id = uuid::Uuid::now_v7().to_string();
        let operation = (
            &request.plugin_id,
            &request.invocation_id,
            &request.handler,
            &request.event_type,
            &request.event,
        );
        let authorization = self.authorization(
            &request.session_id,
            call_id,
            request.cancellation_id,
            "plugin.observe",
            &operation,
        )?;
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::Observe {
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    handler: request.handler,
                    event_type: request.event_type,
                    event: request.event,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::Observation {
                accepted,
                queue_depth,
                dropped,
                status,
                request_hash,
                receipt_id,
                receipt_digest,
                replayed,
                ..
            } => Ok(DependencyPluginObservationResult {
                accepted,
                queue_depth,
                dropped,
                status: match status {
                    wire::PluginObserverDeliveryStatus::Completed => {
                        DependencyPluginObserverDeliveryStatus::Completed
                    }
                    wire::PluginObserverDeliveryStatus::Rejected => {
                        DependencyPluginObserverDeliveryStatus::Rejected
                    }
                    wire::PluginObserverDeliveryStatus::Failed => {
                        DependencyPluginObserverDeliveryStatus::Failed
                    }
                    wire::PluginObserverDeliveryStatus::Ambiguous => {
                        DependencyPluginObserverDeliveryStatus::Ambiguous
                    }
                },
                request_hash,
                receipt_id,
                receipt_digest,
                replayed,
            }),
            response => Err(map_failure(response)),
        }
    }

    async fn invoke_node_executor(
        &self,
        request: DependencyPluginNodeInvocationRequest,
    ) -> Result<DependencyPluginNodeOutcome, PluginDependencyError> {
        let cancellation_target = map_cancellation_target(&request.cancellation_target);
        let operation = (
            &cancellation_target,
            &request.plugin_id,
            &request.invocation_id,
            Some(request.executor_id.as_str()),
            Some(request.executor_version.as_str()),
            request.timeout_ms,
            request.configuration_reference,
            &request.node_kind,
            &request.handler,
            &request.input,
            &request.readable_state,
        );
        let authorization = self.authorization(
            &request.session_id,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id,
            "plugin.node_executor.invoke",
            &operation,
        )?;
        let expected_plugin = request.plugin_id.clone();
        let expected_invocation = request.invocation_id.clone();
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::InvokeNodeExecutor {
                    cancellation_target,
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    executor_id: request.executor_id,
                    executor_version: request.executor_version,
                    timeout_ms: request.timeout_ms,
                    configuration_reference: request.configuration_reference.to_hex(),
                    node_kind: request.node_kind,
                    handler: request.handler,
                    input: request.input,
                    readable_state: request.readable_state,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::NodeOutcome { proposal, audit }
                if audit.plugin_id == expected_plugin
                    && audit.invocation_id.as_deref() == Some(expected_invocation.as_str())
                    && audit.operation == "node_executor"
                    && audit.outcome == "completed"
                    && audit.attempts > 0 =>
            {
                Ok(DependencyPluginNodeOutcome {
                    output: proposal.output,
                    preserved_state: proposal.preserved_state,
                    proposed_actions: proposal
                        .proposed_actions
                        .into_iter()
                        .map(|action| DependencyPluginNodeActionProposal {
                            kind: action.kind,
                            payload: action.payload,
                        })
                        .collect(),
                    attempts: audit.attempts,
                })
            }
            response => Err(map_failure(response)),
        }
    }

    async fn invoke_context_transform(
        &self,
        request: DependencyPluginContextTransformInvocationRequest,
    ) -> Result<DependencyPluginContextTransformProposal, PluginDependencyError> {
        let lifecycle = match request.lifecycle {
            DependencyPluginContextTransformLifecycle::BeforeModelRequest => {
                wire::ContextTransformLifecycle::BeforeModelRequest
            }
        };
        let cancellation_target = map_cancellation_target(&request.cancellation_target);
        let operation = (
            &cancellation_target,
            &request.plugin_id,
            &request.invocation_id,
            &request.transform_id,
            &request.transform_version,
            request.timeout_ms,
            request.configuration_reference,
            lifecycle,
            &request.handler,
            &request.input,
            &request.readable_state,
        );
        let authorization = self.authorization(
            &request.session_id,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id,
            "plugin.context_transform.invoke",
            &operation,
        )?;
        let expected_plugin = request.plugin_id.clone();
        let expected_invocation = request.invocation_id.clone();
        match self
            .exchange(
                &request.session_id,
                wire::PluginCommand::InvokeContextTransform {
                    cancellation_target,
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    transform_id: request.transform_id,
                    transform_version: request.transform_version,
                    timeout_ms: request.timeout_ms,
                    configuration_reference: request.configuration_reference.to_hex(),
                    lifecycle,
                    handler: request.handler,
                    input: request.input,
                    readable_state: request.readable_state,
                    authorization,
                },
            )
            .await?
        {
            wire::PluginResponse::ContextTransformProposal { proposal, audit }
                if audit.plugin_id == expected_plugin
                    && audit.invocation_id.as_deref() == Some(expected_invocation.as_str())
                    && audit.operation == "context_transform"
                    && audit.outcome == "completed"
                    && audit.attempts > 0 =>
            {
                Ok(DependencyPluginContextTransformProposal {
                    replacement: proposal.replacement,
                    attempts: audit.attempts,
                })
            }
            wire::PluginResponse::Failed { code, .. } if code == "ambiguous_execution" => {
                Err(PluginDependencyError::AmbiguousContextTransform)
            }
            response => Err(map_failure(response)),
        }
    }

    async fn retrieve_memory(
        &self,
        request: DependencyPluginMemoryRetrieveRequest,
    ) -> Result<DependencyPluginMemoryRetrieveProposal, PluginDependencyError> {
        validate_pure_attempt_bound(
            request.binding.attempt,
            request.max_attempts,
            request.retry_backoff,
        )?;
        if request.timeout.is_zero() {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let timeout_ms = u64::try_from(request.timeout.as_millis())
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let binding = request.binding.clone();
        let wire_binding = map_operation_binding(&binding);
        let wire_input = map_memory_retrieve_input(&request.input);
        let normalized_input =
            serde_json::to_value(&wire_input).map_err(|_| PluginDependencyError::InvalidRequest)?;
        let operation = (
            &wire_binding,
            &request.provider_id,
            &request.provider_version,
            &request.handler,
            timeout_ms,
            wire::PluginOperationIdempotency::Idempotent,
            &normalized_input,
            &request.readable_state,
            &request.cancellation_id,
        );
        let authorization = self.authorization(
            &binding.session_id,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id.clone(),
            "plugin.memory.retrieve.invoke",
            &operation,
        )?;
        let command = wire::PluginCommand::InvokeMemoryRetrieve {
            binding: wire_binding,
            provider_id: request.provider_id.clone(),
            provider_version: request.provider_version.clone(),
            handler: request.handler.clone(),
            timeout_ms,
            idempotency: wire::PluginOperationIdempotency::Idempotent,
            request: wire_input,
            readable_state: request.readable_state.clone(),
            authorization,
        };
        command
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let response = self
            .exchange_with_timeout(&binding.session_id, command, request.timeout)
            .await?;
        response
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidResponse)?;
        map_memory_retrieve_response(response, &binding, &request)
    }

    async fn write_memory(
        &self,
        request: DependencyPluginMemoryWriteRequest,
    ) -> Result<DependencyPluginMemoryWriteReceipt, PluginDependencyError> {
        if request.binding.attempt != 1 {
            return Err(PluginDependencyError::InvalidRequest);
        }
        if request.timeout.is_zero() {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let timeout_ms = u64::try_from(request.timeout.as_millis())
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let wire_binding = map_operation_binding(&request.binding);
        let wire_input = map_memory_write_input(&request.input);
        let normalized_input =
            serde_json::to_value(&wire_input).map_err(|_| PluginDependencyError::InvalidRequest)?;
        let operation = (
            &wire_binding,
            &request.provider_id,
            &request.provider_version,
            &request.handler,
            timeout_ms,
            wire::PluginOperationIdempotency::NonIdempotent,
            &normalized_input,
            &request.readable_state,
            &request.cancellation_id,
        );
        let authorization = self.authorization(
            &request.binding.session_id,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id.clone(),
            "plugin.memory.write.invoke",
            &operation,
        )?;
        let command = wire::PluginCommand::InvokeMemoryWrite {
            binding: wire_binding,
            provider_id: request.provider_id.clone(),
            provider_version: request.provider_version.clone(),
            handler: request.handler.clone(),
            timeout_ms,
            idempotency: wire::PluginOperationIdempotency::NonIdempotent,
            request: wire_input,
            readable_state: request.readable_state.clone(),
            authorization,
        };
        command
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let response = self
            .exchange_with_timeout(&request.binding.session_id, command, request.timeout)
            .await
            .map_err(map_memory_write_transport_error)?;
        response
            .validate_contract()
            .map_err(|_| PluginDependencyError::AmbiguousMemoryWrite)?;
        map_memory_write_response(response, &request)
    }

    async fn compact_context(
        &self,
        request: DependencyPluginCompactionRequest,
    ) -> Result<DependencyPluginCompactionProposal, PluginDependencyError> {
        validate_pure_attempt_bound(
            request.binding.attempt,
            request.max_attempts,
            request.retry_backoff,
        )?;
        if request.timeout.is_zero() {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let timeout_ms = u64::try_from(request.timeout.as_millis())
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let binding = request.binding.clone();
        let wire_binding = map_operation_binding(&binding);
        let wire_input = map_compaction_input(&request.input);
        let normalized_input =
            serde_json::to_value(&wire_input).map_err(|_| PluginDependencyError::InvalidRequest)?;
        let operation = (
            &wire_binding,
            &request.compactor_id,
            &request.compactor_version,
            &request.handler,
            timeout_ms,
            wire::PluginOperationIdempotency::Idempotent,
            &normalized_input,
            &request.readable_state,
            &request.cancellation_id,
        );
        let authorization = self.authorization(
            &binding.session_id,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id.clone(),
            "plugin.compaction.invoke",
            &operation,
        )?;
        let command = wire::PluginCommand::InvokeCompaction {
            binding: wire_binding,
            compactor_id: request.compactor_id.clone(),
            compactor_version: request.compactor_version.clone(),
            handler: request.handler.clone(),
            timeout_ms,
            idempotency: wire::PluginOperationIdempotency::Idempotent,
            request: wire_input,
            readable_state: request.readable_state.clone(),
            authorization,
        };
        command
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let response = self
            .exchange_with_timeout(&binding.session_id, command, request.timeout)
            .await?;
        response
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidResponse)?;
        map_compaction_response(response, &binding, &request)
    }

    async fn cancel_plugin_invocation(
        &self,
        request: DependencyCancelPluginInvocationRequest,
    ) -> Result<DependencyPluginInvocationCancellationReceipt, PluginDependencyError> {
        validate_plugin_cancellation_request(&request)?;
        let wire_target = map_cancellation_target(&request.target);
        let action_digest = wire::plugin_invocation_cancellation_action_digest(
            &wire_target,
            &request.reason_code,
            &request.nonce,
            &request.idempotency_key,
            &request.cancellation_id,
        )
        .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let authorization = self.cancellation_authorization(
            &request.target,
            uuid::Uuid::now_v7().to_string(),
            request.cancellation_id.clone(),
            &request.nonce,
            action_digest,
        )?;
        let command = wire::PluginCommand::CancelInvocation {
            target: wire_target,
            reason_code: request.reason_code.clone(),
            action_digest,
            nonce: request.nonce.clone(),
            idempotency_key: request.idempotency_key.clone(),
            authorization,
        };
        command
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        let response = self.exchange(&request.target.session_id, command).await?;
        response
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidResponse)?;
        map_cancellation_response(response, &request, action_digest)
    }

    async fn change_plugin_lifecycle(
        &self,
        request: DependencyPluginLifecycleRequest,
    ) -> Result<DependencyPluginLifecycleResult, PluginDependencyError> {
        validate_lifecycle_request(&request)?;
        let command = build_lifecycle_command(self, &request)?;
        command
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        lifecycle_pre_dispatch_test_cut(&request).await?;
        let response = self.exchange(&request.session_id, command).await?;
        response
            .validate_contract()
            .map_err(|_| PluginDependencyError::InvalidResponse)?;
        let result = validate_lifecycle_response(&request, response)?;
        lifecycle_post_receipt_test_cut(&request).await?;
        Ok(result)
    }

    async fn persist_plugin_node_state(
        &self,
        request: DependencyPersistPluginNodeStateRequest,
    ) -> Result<DependencyPluginNodeStateReceipt, PluginDependencyError> {
        let encoded_state = serde_json::to_vec(&request.state)
            .map_err(|_| PluginDependencyError::InvalidRequest)?;
        if encoded_state.len() > self.config.maximum_frame_bytes
            || ContentHash::digest(&encoded_state) != request.state_hash
            || request.session_id.is_empty()
            || request.plugin_id.is_empty()
            || request.invocation_id.is_empty()
            || request.executor_id.is_empty()
            || request.executor_version.is_empty()
            || request.nonce.is_empty()
            || request.cancellation_id.is_empty()
            || request.idempotency_key.is_empty()
        {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let operation = (
            &request.cancellation_target,
            request.action_digest,
            &request.nonce,
            &request.cancellation_id,
            &request.idempotency_key,
        );
        let authorization = self.authorization(
            &request.session_id,
            request.idempotency_key.clone(),
            request.cancellation_id.clone(),
            "plugin.node_executor.persist_state",
            &operation,
        )?;
        if authorization.normalized_digest != request.authorization_digest.to_hex() {
            return Err(PluginDependencyError::Authorization);
        }
        let expected = request.clone();
        let response = self
            .exchange(
                &request.session_id,
                wire::PluginCommand::PersistNodeState {
                    cancellation_target: map_cancellation_target(&request.cancellation_target),
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    invocation_digest: request.invocation_digest.to_hex(),
                    executor_id: request.executor_id,
                    executor_version: request.executor_version,
                    executor_declaration_hash: request.executor_declaration_hash.to_hex(),
                    configuration_reference: request.configuration_reference.to_hex(),
                    state_scope: map_wire_node_state_scope(request.state_scope),
                    prior_generation: request.prior_generation,
                    prior_state_hash: request.prior_state_hash.map(ContentHash::to_hex),
                    state: request.state,
                    state_hash: request.state_hash.to_hex(),
                    action_digest: request.action_digest.to_hex(),
                    authorization_digest: request.authorization_digest.to_hex(),
                    nonce: request.nonce,
                    idempotency_key: request.idempotency_key,
                    authorization,
                },
            )
            .await
            .map_err(|error| match error {
                PluginDependencyError::Timeout
                | PluginDependencyError::Unavailable
                | PluginDependencyError::InvalidResponse => {
                    PluginDependencyError::AmbiguousStatePersistence
                }
                other => other,
            })?;
        map_node_state_response(&expected, response)
    }

    async fn load_plugin_node_state(
        &self,
        request: DependencyLoadPluginNodeStateRequest,
    ) -> Result<DependencyLoadedPluginNodeState, PluginDependencyError> {
        if !matches!(
            request.state_scope,
            DependencyPluginNodeStateScope::Invocation | DependencyPluginNodeStateScope::Session
        ) || request.expected_generation == 0
            || request.session_id.is_empty()
            || request.plugin_id.is_empty()
            || request.invocation_id.is_empty()
            || request.executor_id.is_empty()
            || request.executor_version.is_empty()
            || request.nonce.is_empty()
            || request.cancellation_id.is_empty()
            || request.idempotency_key.is_empty()
        {
            return Err(PluginDependencyError::InvalidRequest);
        }
        let operation = (
            &request.cancellation_target,
            request.action_digest,
            &request.nonce,
            &request.cancellation_id,
            &request.idempotency_key,
        );
        let authorization = self.authorization(
            &request.session_id,
            request.idempotency_key.clone(),
            request.cancellation_id.clone(),
            "plugin.node_executor.load_state",
            &operation,
        )?;
        if authorization.normalized_digest != request.authorization_digest.to_hex() {
            return Err(PluginDependencyError::Authorization);
        }
        let expected = request.clone();
        let response = self
            .exchange(
                &request.session_id,
                wire::PluginCommand::LoadNodeState {
                    cancellation_target: map_cancellation_target(&request.cancellation_target),
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    invocation_digest: request.invocation_digest.to_hex(),
                    executor_id: request.executor_id,
                    executor_version: request.executor_version,
                    executor_declaration_hash: request.executor_declaration_hash.to_hex(),
                    configuration_reference: request.configuration_reference.to_hex(),
                    state_scope: map_wire_node_state_scope(request.state_scope),
                    expected_generation: request.expected_generation,
                    expected_state_hash: request.expected_state_hash.to_hex(),
                    action_digest: request.action_digest.to_hex(),
                    authorization_digest: request.authorization_digest.to_hex(),
                    nonce: request.nonce,
                    idempotency_key: request.idempotency_key,
                    authorization,
                },
            )
            .await
            .map_err(|error| match error {
                PluginDependencyError::Timeout
                | PluginDependencyError::Unavailable
                | PluginDependencyError::InvalidResponse => {
                    PluginDependencyError::AmbiguousStateRead
                }
                other => other,
            })?;
        map_node_state_read_response(&expected, response, self.config.maximum_frame_bytes)
    }

    async fn shutdown(&self) {
        let connections = std::mem::take(&mut *self.connections.lock().await);
        for (_, connection) in connections {
            fail_plugin_connection(
                &connection.child,
                &connection.pending,
                &connection.failed,
                PluginTransportFailure::Unavailable,
            )
            .await;
        }
    }

    async fn teardown_session_if_idle(
        &self,
        session_id: &str,
        active_continuations: usize,
        pending_observer_deliveries: usize,
    ) -> Result<bool, PluginDependencyError> {
        if active_continuations != 0 || pending_observer_deliveries != 0 {
            return Ok(false);
        }
        let response = self
            .exchange(session_id, wire::PluginCommand::Health)
            .await?;
        let wire::PluginResponse::Health {
            running,
            observer_pending,
            state_flushed,
            ..
        } = response
        else {
            return Err(PluginDependencyError::InvalidResponse);
        };
        if running != 0 || observer_pending != 0 || !state_flushed {
            return Ok(false);
        }
        let connection = {
            let mut connections = self.connections.lock().await;
            let Some(connection) = connections.get(session_id).cloned() else {
                return Ok(true);
            };
            connection.closing.store(true, Ordering::Release);
            if has_pending_plugin_operation(&*connection.pending.lock().await) {
                connection.closing.store(false, Ordering::Release);
                return Ok(false);
            }
            connections.remove(session_id);
            connection
        };
        fail_plugin_connection(
            &connection.child,
            &connection.pending,
            &connection.failed,
            PluginTransportFailure::Unavailable,
        )
        .await;
        Ok(true)
    }
}

fn start_plugin_transport(
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    maximum_frame_bytes: usize,
) -> Connection {
    let child = Arc::new(Mutex::new(child));
    let pending = Arc::new(Mutex::new(BTreeMap::<String, PendingPluginResponse>::new()));
    let failed = Arc::new(AtomicBool::new(false));
    let closing = Arc::new(AtomicBool::new(false));
    let (outbound, outbound_receiver) = mpsc::channel(MAX_PLUGIN_PENDING_REQUESTS);

    tokio::spawn(plugin_writer_loop(
        Arc::clone(&child),
        Arc::clone(&pending),
        Arc::clone(&failed),
        stdin,
        outbound_receiver,
    ));
    tokio::spawn(plugin_reader_loop(
        Arc::clone(&child),
        Arc::clone(&pending),
        Arc::clone(&failed),
        stdout,
        maximum_frame_bytes,
    ));

    Connection {
        child,
        outbound,
        pending,
        failed,
        closing,
    }
}

const fn pending_plugin_operation(command: &wire::PluginCommand) -> PendingPluginOperation {
    match command {
        wire::PluginCommand::InvokeNodeExecutor { .. } => PendingPluginOperation::NodeInvocation,
        wire::PluginCommand::InvokeContextTransform { .. } => {
            PendingPluginOperation::ContextTransform
        }
        wire::PluginCommand::InvokeMemoryRetrieve { .. } => PendingPluginOperation::MemoryRetrieve,
        wire::PluginCommand::InvokeMemoryWrite { .. } => PendingPluginOperation::MemoryWrite,
        wire::PluginCommand::InvokeCompaction { .. } => PendingPluginOperation::Compaction,
        wire::PluginCommand::PersistNodeState { .. } => PendingPluginOperation::StateCas,
        wire::PluginCommand::LoadNodeState { .. } => PendingPluginOperation::StateRead,
        wire::PluginCommand::Intercept { .. } => PendingPluginOperation::Interceptor,
        wire::PluginCommand::Observe { .. } => PendingPluginOperation::Observer,
        wire::PluginCommand::CancelInvocation { .. }
        | wire::PluginCommand::Disable { .. }
        | wire::PluginCommand::Enable { .. }
        | wire::PluginCommand::Quarantine { .. }
        | wire::PluginCommand::Unquarantine { .. } => PendingPluginOperation::Lifecycle,
        wire::PluginCommand::Negotiate { .. }
        | wire::PluginCommand::ValidateSet { .. }
        | wire::PluginCommand::Load { .. }
        | wire::PluginCommand::InvokeTool { .. }
        | wire::PluginCommand::Health => PendingPluginOperation::OtherTransport,
    }
}

fn has_pending_plugin_operation(pending: &BTreeMap<String, PendingPluginResponse>) -> bool {
    pending
        .values()
        .any(|request| request.operation.blocks_teardown())
}

async fn plugin_writer_loop(
    child: Arc<Mutex<Child>>,
    pending: Arc<Mutex<BTreeMap<String, PendingPluginResponse>>>,
    failed: Arc<AtomicBool>,
    mut stdin: ChildStdin,
    mut outbound: mpsc::Receiver<Vec<u8>>,
) {
    while let Some(bytes) = outbound.recv().await {
        if stdin.write_all(&bytes).await.is_err()
            || stdin.write_all(b"\n").await.is_err()
            || stdin.flush().await.is_err()
        {
            fail_plugin_connection(
                &child,
                &pending,
                &failed,
                PluginTransportFailure::Unavailable,
            )
            .await;
            return;
        }
    }
}

async fn plugin_reader_loop(
    child: Arc<Mutex<Child>>,
    pending: Arc<Mutex<BTreeMap<String, PendingPluginResponse>>>,
    failed: Arc<AtomicBool>,
    stdout: ChildStdout,
    maximum_frame_bytes: usize,
) {
    let mut stdout = BufReader::new(stdout);
    loop {
        let bytes = match read_bounded_plugin_frame(&mut stdout, maximum_frame_bytes).await {
            Ok(Some(Ok(bytes))) => bytes,
            Ok(Some(Err(()))) => {
                fail_plugin_connection(
                    &child,
                    &pending,
                    &failed,
                    PluginTransportFailure::InvalidResponse,
                )
                .await;
                return;
            }
            Ok(None) | Err(_) => {
                fail_plugin_connection(
                    &child,
                    &pending,
                    &failed,
                    PluginTransportFailure::Unavailable,
                )
                .await;
                return;
            }
        };
        let Ok(frame) = wire::decode_bounded_response_frame(&bytes) else {
            fail_plugin_connection(
                &child,
                &pending,
                &failed,
                PluginTransportFailure::InvalidResponse,
            )
            .await;
            return;
        };
        let Some(pending_response) = pending.lock().await.remove(&frame.correlation_id) else {
            fail_plugin_connection(
                &child,
                &pending,
                &failed,
                PluginTransportFailure::InvalidResponse,
            )
            .await;
            return;
        };
        let _ = pending_response.sender.send(Ok(frame.response));
    }
}

async fn fail_plugin_connection(
    child: &Arc<Mutex<Child>>,
    pending: &Arc<Mutex<BTreeMap<String, PendingPluginResponse>>>,
    failed: &Arc<AtomicBool>,
    failure: PluginTransportFailure,
) {
    if failed.swap(true, Ordering::AcqRel) {
        return;
    }
    fail_plugin_waiters(pending, failure).await;
    let mut child = child.lock().await;
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn fail_plugin_waiters(
    pending: &Arc<Mutex<BTreeMap<String, PendingPluginResponse>>>,
    failure: PluginTransportFailure,
) {
    let waiters = std::mem::take(&mut *pending.lock().await);
    for (_, waiter) in waiters {
        let _ = waiter.sender.send(Err(failure));
    }
}

async fn read_bounded_plugin_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> std::io::Result<Option<Result<Vec<u8>, ()>>> {
    let mut frame = Vec::with_capacity(maximum.min(8 * 1024).saturating_add(1));
    let mut oversized = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if frame.is_empty() && !oversized {
                return Ok(None);
            }
            return Ok(Some(if oversized { Err(()) } else { Ok(frame) }));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content = newline.map_or(available, |index| &available[..index]);
        if !oversized {
            let remaining = maximum.saturating_add(1).saturating_sub(frame.len());
            frame.extend_from_slice(&content[..content.len().min(remaining)]);
            oversized = frame.len() > maximum || content.len() > remaining;
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(if oversized { Err(()) } else { Ok(frame) }));
        }
    }
}

fn map_node_state_read_response(
    expected: &DependencyLoadPluginNodeStateRequest,
    response: wire::PluginResponse,
    maximum: usize,
) -> Result<DependencyLoadedPluginNodeState, PluginDependencyError> {
    let wire::PluginResponse::NodeStateLoaded {
        state,
        receipt,
        audit,
    } = response
    else {
        return Err(map_state_failure(response));
    };
    if audit.plugin_id != expected.plugin_id
        || audit.invocation_id.as_deref() != Some(expected.invocation_id.as_str())
        || audit.operation != "load_node_state"
        || !matches!(audit.outcome.as_str(), "loaded" | "reconciled")
        || audit.attempts != 1
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    let encoded = serde_json::to_vec(&state).map_err(|_| PluginDependencyError::InvalidResponse)?;
    if encoded.len() > maximum || ContentHash::digest(&encoded) != expected.expected_state_hash {
        return Err(PluginDependencyError::InvalidResponse);
    }
    let receipt = DependencyPluginNodeStateReadReceipt {
        plugin_id: receipt.plugin_id,
        invocation_id: receipt.invocation_id,
        invocation_digest: parse_wire_hash(&receipt.invocation_digest)?,
        executor_id: receipt.executor_id,
        executor_version: receipt.executor_version,
        executor_declaration_hash: parse_wire_hash(&receipt.executor_declaration_hash)?,
        state_scope: unmap_wire_node_state_scope(receipt.state_scope),
        generation: receipt.generation,
        state_hash: parse_wire_hash(&receipt.state_hash)?,
        action_digest: parse_wire_hash(&receipt.action_digest)?,
        authorization_digest: parse_wire_hash(&receipt.authorization_digest)?,
        idempotency_key: receipt.idempotency_key,
        receipt_id: receipt.receipt_id,
        receipt_digest: parse_wire_hash(&receipt.receipt_digest)?,
        replayed: receipt.replayed,
    };
    if receipt.plugin_id != expected.plugin_id
        || receipt.invocation_id != expected.invocation_id
        || receipt.invocation_digest != expected.invocation_digest
        || receipt.executor_id != expected.executor_id
        || receipt.executor_version != expected.executor_version
        || receipt.executor_declaration_hash != expected.executor_declaration_hash
        || receipt.state_scope != expected.state_scope
        || receipt.generation != expected.expected_generation
        || receipt.state_hash != expected.expected_state_hash
        || receipt.action_digest != expected.action_digest
        || receipt.authorization_digest != expected.authorization_digest
        || receipt.idempotency_key != expected.idempotency_key
        || receipt.receipt_id.is_empty()
        || plugin_node_state_read_receipt_digest(&receipt)? != receipt.receipt_digest
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(DependencyLoadedPluginNodeState { state, receipt })
}

fn map_node_state_response(
    expected: &DependencyPersistPluginNodeStateRequest,
    response: wire::PluginResponse,
) -> Result<DependencyPluginNodeStateReceipt, PluginDependencyError> {
    let wire::PluginResponse::NodeStatePersisted { receipt, audit } = response else {
        return Err(map_state_failure(response));
    };
    if audit.plugin_id != expected.plugin_id
        || audit.invocation_id.as_deref() != Some(expected.invocation_id.as_str())
        || audit.operation != "persist_node_state"
        || !matches!(audit.outcome.as_str(), "committed" | "reconciled")
        || audit.attempts != 1
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    let receipt = DependencyPluginNodeStateReceipt {
        plugin_id: receipt.plugin_id,
        invocation_id: receipt.invocation_id,
        invocation_digest: parse_wire_hash(&receipt.invocation_digest)?,
        executor_id: receipt.executor_id,
        executor_version: receipt.executor_version,
        executor_declaration_hash: parse_wire_hash(&receipt.executor_declaration_hash)?,
        state_scope: unmap_wire_node_state_scope(receipt.state_scope),
        prior_generation: receipt.prior_generation,
        generation: receipt.generation,
        state_hash: parse_wire_hash(&receipt.state_hash)?,
        action_digest: parse_wire_hash(&receipt.action_digest)?,
        authorization_digest: parse_wire_hash(&receipt.authorization_digest)?,
        idempotency_key: receipt.idempotency_key,
        receipt_id: receipt.receipt_id,
        receipt_digest: parse_wire_hash(&receipt.receipt_digest)?,
        replayed: receipt.replayed,
    };
    if receipt.plugin_id != expected.plugin_id
        || receipt.invocation_id != expected.invocation_id
        || receipt.invocation_digest != expected.invocation_digest
        || receipt.executor_id != expected.executor_id
        || receipt.executor_version != expected.executor_version
        || receipt.executor_declaration_hash != expected.executor_declaration_hash
        || receipt.state_scope != expected.state_scope
        || receipt.prior_generation != expected.prior_generation
        || receipt.generation != expected.prior_generation.saturating_add(1)
        || receipt.state_hash != expected.state_hash
        || receipt.action_digest != expected.action_digest
        || receipt.authorization_digest != expected.authorization_digest
        || receipt.idempotency_key != expected.idempotency_key
        || receipt.receipt_id.is_empty()
        || plugin_node_state_receipt_digest(&receipt)? != receipt.receipt_digest
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(receipt)
}

const fn map_wire_node_state_scope(
    scope: DependencyPluginNodeStateScope,
) -> wire::PluginNodeStateScope {
    match scope {
        DependencyPluginNodeStateScope::Invocation => wire::PluginNodeStateScope::Invocation,
        DependencyPluginNodeStateScope::ModelCall => wire::PluginNodeStateScope::ModelCall,
        DependencyPluginNodeStateScope::Turn => wire::PluginNodeStateScope::Turn,
        DependencyPluginNodeStateScope::Session => wire::PluginNodeStateScope::Session,
        DependencyPluginNodeStateScope::Project => wire::PluginNodeStateScope::Project,
        DependencyPluginNodeStateScope::User => wire::PluginNodeStateScope::User,
        DependencyPluginNodeStateScope::Runtime => wire::PluginNodeStateScope::Runtime,
    }
}

const fn unmap_wire_node_state_scope(
    scope: wire::PluginNodeStateScope,
) -> DependencyPluginNodeStateScope {
    match scope {
        wire::PluginNodeStateScope::Invocation => DependencyPluginNodeStateScope::Invocation,
        wire::PluginNodeStateScope::ModelCall => DependencyPluginNodeStateScope::ModelCall,
        wire::PluginNodeStateScope::Turn => DependencyPluginNodeStateScope::Turn,
        wire::PluginNodeStateScope::Session => DependencyPluginNodeStateScope::Session,
        wire::PluginNodeStateScope::Project => DependencyPluginNodeStateScope::Project,
        wire::PluginNodeStateScope::User => DependencyPluginNodeStateScope::User,
        wire::PluginNodeStateScope::Runtime => DependencyPluginNodeStateScope::Runtime,
    }
}

fn parse_wire_hash(value: &str) -> Result<ContentHash, PluginDependencyError> {
    value
        .parse()
        .map_err(|_| PluginDependencyError::InvalidResponse)
}

fn validate_pure_attempt_bound(
    attempt: u8,
    max_attempts: u8,
    retry_backoff: Duration,
) -> Result<(), PluginDependencyError> {
    if attempt == 0
        || max_attempts == 0
        || attempt > max_attempts
        || max_attempts > 16
        || retry_backoff > Duration::from_secs(60)
    {
        Err(PluginDependencyError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn validate_plugin_cancellation_request(
    request: &DependencyCancelPluginInvocationRequest,
) -> Result<(), PluginDependencyError> {
    for value in [
        request.target.session_id.as_str(),
        request.target.run_id.as_str(),
        request.target.plugin_id.as_str(),
        request.target.plugin_version.as_str(),
        request.target.invocation_id.as_str(),
        request.target.operation_id.as_str(),
        request.reason_code.as_str(),
        request.nonce.as_str(),
        request.idempotency_key.as_str(),
        request.cancellation_id.as_str(),
    ] {
        if value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
        {
            return Err(PluginDependencyError::InvalidRequest);
        }
    }
    Ok(())
}

fn map_cancellation_target(
    target: &DependencyPluginInvocationCancellationTarget,
) -> wire::PluginInvocationCancellationTarget {
    wire::PluginInvocationCancellationTarget {
        session_id: target.session_id.clone(),
        run_id: target.run_id.clone(),
        plugin_id: target.plugin_id.clone(),
        plugin_version: target.plugin_version.clone(),
        invocation_id: target.invocation_id.clone(),
        invocation_digest: target.invocation_digest,
        operation_id: target.operation_id.clone(),
        declaration_hash: target.declaration_hash,
        request_hash: target.request_hash,
    }
}

fn unmap_cancellation_target(
    target: wire::PluginInvocationCancellationTarget,
) -> DependencyPluginInvocationCancellationTarget {
    DependencyPluginInvocationCancellationTarget {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: target.invocation_digest,
        operation_id: target.operation_id,
        declaration_hash: target.declaration_hash,
        request_hash: target.request_hash,
    }
}

fn map_cancellation_response(
    response: wire::PluginResponse,
    expected: &DependencyCancelPluginInvocationRequest,
    expected_action_digest: ContentHash,
) -> Result<DependencyPluginInvocationCancellationReceipt, PluginDependencyError> {
    let wire::PluginResponse::InvocationCancelled { receipt } = response else {
        return Err(map_failure(response));
    };
    let expected_target = map_cancellation_target(&expected.target);
    if receipt.target != expected_target
        || receipt.reason_code != expected.reason_code
        || receipt.action_digest != expected_action_digest
        || receipt.nonce != expected.nonce
        || receipt.idempotency_key != expected.idempotency_key
        || receipt.cancellation_id != expected.cancellation_id
        || receipt.receipt_id.is_empty()
        || wire::plugin_invocation_cancellation_receipt_digest(&receipt)
            .map_err(|_| PluginDependencyError::InvalidResponse)?
            != receipt.receipt_digest
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(DependencyPluginInvocationCancellationReceipt {
        target: unmap_cancellation_target(receipt.target),
        reason_code: receipt.reason_code,
        action_digest: receipt.action_digest,
        nonce: receipt.nonce,
        idempotency_key: receipt.idempotency_key,
        cancellation_id: receipt.cancellation_id,
        status: match receipt.status {
            wire::PluginInvocationCancellationStatus::Signalled => {
                DependencyPluginInvocationCancellationStatus::Signalled
            }
            wire::PluginInvocationCancellationStatus::AlreadyTerminal => {
                DependencyPluginInvocationCancellationStatus::AlreadyTerminal
            }
        },
        receipt_id: receipt.receipt_id,
        receipt_digest: receipt.receipt_digest,
    })
}

async fn lifecycle_post_receipt_test_cut(
    request: &DependencyPluginLifecycleRequest,
) -> Result<(), PluginDependencyError> {
    let delay_ms = std::env::var("AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_DELAY_MS")
        .ok()
        .map_or(Ok(0_u64), |value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|delay| *delay <= 60_000)
                .ok_or(PluginDependencyError::InvalidConfiguration)
        })?;
    let Some(marker_path) = std::env::var_os("AGENTMOD_PLUGIN_LIFECYCLE_POST_RECEIPT_MARKER")
    else {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        return Ok(());
    };
    let action = match request.action {
        DependencyPluginLifecycleAction::Disable => "disable",
        DependencyPluginLifecycleAction::Enable => "enable",
        DependencyPluginLifecycleAction::Quarantine => "quarantine",
        DependencyPluginLifecycleAction::Unquarantine => "unquarantine",
    };
    let mut marker = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(PathBuf::from(marker_path))
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
    marker
        .write_all(
            format!(
                "{}|{}|{}|{}\n",
                request.session_id, request.plugin_id, action, request.cancellation_id
            )
            .as_bytes(),
        )
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
    marker
        .sync_data()
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    Ok(())
}

fn validate_lifecycle_request(
    request: &DependencyPluginLifecycleRequest,
) -> Result<(), PluginDependencyError> {
    if request.session_id.is_empty()
        || request.plugin_id.is_empty()
        || request.plugin_version.is_empty()
        || request.configuration_reference == ContentHash::from_bytes([0; 32])
        || request.cancellation_id.is_empty()
        || request.plugin_id.len() > 256
        || request
            .reason_code
            .as_ref()
            .is_some_and(|reason| reason.is_empty() || reason.len() > 256)
        || matches!(
            request.action,
            DependencyPluginLifecycleAction::Disable
                | DependencyPluginLifecycleAction::Enable
                | DependencyPluginLifecycleAction::Unquarantine
        ) && request.reason_code.is_some()
        || matches!(request.action, DependencyPluginLifecycleAction::Quarantine)
            && request.reason_code.is_none()
    {
        return Err(PluginDependencyError::InvalidRequest);
    }
    Ok(())
}

fn build_lifecycle_command(
    dependency: &ProcessPluginDependency,
    request: &DependencyPluginLifecycleRequest,
) -> Result<wire::PluginCommand, PluginDependencyError> {
    let call_id = format!("lifecycle:{}", request.cancellation_id);
    let configuration_reference = request.configuration_reference.to_hex();
    let identity = (
        &request.plugin_id,
        &request.plugin_version,
        request.configuration_reference,
    );
    Ok(match request.action {
        DependencyPluginLifecycleAction::Disable => wire::PluginCommand::Disable {
            plugin_id: request.plugin_id.clone(),
            plugin_version: request.plugin_version.clone(),
            configuration_reference,
            authorization: dependency.authorization(
                &request.session_id,
                call_id,
                request.cancellation_id.clone(),
                "plugin.disable",
                &identity,
            )?,
        },
        DependencyPluginLifecycleAction::Enable => wire::PluginCommand::Enable {
            plugin_id: request.plugin_id.clone(),
            plugin_version: request.plugin_version.clone(),
            configuration_reference,
            authorization: dependency.authorization(
                &request.session_id,
                call_id,
                request.cancellation_id.clone(),
                "plugin.enable",
                &identity,
            )?,
        },
        DependencyPluginLifecycleAction::Quarantine => wire::PluginCommand::Quarantine {
            plugin_id: request.plugin_id.clone(),
            plugin_version: request.plugin_version.clone(),
            configuration_reference,
            reason_code: request
                .reason_code
                .clone()
                .ok_or(PluginDependencyError::InvalidRequest)?,
            authorization: dependency.authorization(
                &request.session_id,
                call_id,
                request.cancellation_id.clone(),
                "plugin.quarantine",
                &(
                    &request.plugin_id,
                    &request.plugin_version,
                    request.configuration_reference,
                    &request.reason_code,
                ),
            )?,
        },
        DependencyPluginLifecycleAction::Unquarantine => wire::PluginCommand::Unquarantine {
            plugin_id: request.plugin_id.clone(),
            plugin_version: request.plugin_version.clone(),
            configuration_reference,
            authorization: dependency.authorization(
                &request.session_id,
                call_id,
                request.cancellation_id.clone(),
                "plugin.unquarantine",
                &identity,
            )?,
        },
    })
}

fn validate_lifecycle_response(
    request: &DependencyPluginLifecycleRequest,
    response: wire::PluginResponse,
) -> Result<DependencyPluginLifecycleResult, PluginDependencyError> {
    let wire::PluginResponse::StateChanged {
        plugin_id,
        state,
        audit,
    } = response
    else {
        return Err(map_failure(response));
    };
    let expected = match request.action {
        DependencyPluginLifecycleAction::Disable => ("disabled", "disable"),
        DependencyPluginLifecycleAction::Enable => ("active", "enable"),
        DependencyPluginLifecycleAction::Quarantine => ("quarantined", "quarantine"),
        DependencyPluginLifecycleAction::Unquarantine => ("active", "unquarantine"),
    };
    if plugin_id != request.plugin_id
        || state != expected.0
        || audit.plugin_id != request.plugin_id
        || audit.invocation_id.is_some()
        || audit.operation != expected.1
        || audit.attempts != 1
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(DependencyPluginLifecycleResult {
        plugin_id,
        state,
        audit_operation: audit.operation,
        audit_outcome: audit.outcome,
    })
}

async fn lifecycle_pre_dispatch_test_cut(
    request: &DependencyPluginLifecycleRequest,
) -> Result<(), PluginDependencyError> {
    let delay_ms = std::env::var("AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_DELAY_MS")
        .ok()
        .map_or(Ok(0_u64), |value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|delay| *delay <= 60_000)
                .ok_or(PluginDependencyError::InvalidConfiguration)
        })?;
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    let Some(marker_path) = std::env::var_os("AGENTMOD_PLUGIN_LIFECYCLE_PRE_DISPATCH_MARKER")
    else {
        return Ok(());
    };
    let action = match request.action {
        DependencyPluginLifecycleAction::Disable => "disable",
        DependencyPluginLifecycleAction::Enable => "enable",
        DependencyPluginLifecycleAction::Quarantine => "quarantine",
        DependencyPluginLifecycleAction::Unquarantine => "unquarantine",
    };
    let mut marker = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(PathBuf::from(marker_path))
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
    marker
        .write_all(
            format!(
                "{}|{}|{}|{}\n",
                request.session_id, request.plugin_id, action, request.cancellation_id
            )
            .as_bytes(),
        )
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)?;
    marker
        .sync_data()
        .await
        .map_err(|_| PluginDependencyError::InvalidConfiguration)
}

fn map_operation_binding(
    binding: &DependencyPluginOperationBinding,
) -> wire::PluginOperationBinding {
    wire::PluginOperationBinding {
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        operation_id: binding.operation_id.clone(),
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        node_id: binding.node_id.clone(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key.clone(),
        attempt: binding.attempt,
    }
}

fn unmap_operation_binding(
    binding: wire::PluginOperationBinding,
) -> DependencyPluginOperationBinding {
    DependencyPluginOperationBinding {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    }
}

fn map_memory_scope(scope: DependencyPluginMemoryScope) -> wire::PluginMemoryScope {
    match scope {
        DependencyPluginMemoryScope::Session => wire::PluginMemoryScope::Session,
        DependencyPluginMemoryScope::Project => wire::PluginMemoryScope::Project,
        DependencyPluginMemoryScope::User => wire::PluginMemoryScope::User,
        DependencyPluginMemoryScope::Runtime => wire::PluginMemoryScope::Runtime,
    }
}

fn unmap_memory_scope(scope: wire::PluginMemoryScope) -> DependencyPluginMemoryScope {
    match scope {
        wire::PluginMemoryScope::Session => DependencyPluginMemoryScope::Session,
        wire::PluginMemoryScope::Project => DependencyPluginMemoryScope::Project,
        wire::PluginMemoryScope::User => DependencyPluginMemoryScope::User,
        wire::PluginMemoryScope::Runtime => DependencyPluginMemoryScope::Runtime,
    }
}

fn map_security(
    classification: DependencyPluginSecurityClassification,
) -> wire::PluginSecurityClassification {
    match classification {
        DependencyPluginSecurityClassification::Public => {
            wire::PluginSecurityClassification::Public
        }
        DependencyPluginSecurityClassification::Internal => {
            wire::PluginSecurityClassification::Internal
        }
        DependencyPluginSecurityClassification::Private => {
            wire::PluginSecurityClassification::Private
        }
        DependencyPluginSecurityClassification::Confidential => {
            wire::PluginSecurityClassification::Confidential
        }
    }
}

fn unmap_security(
    classification: wire::PluginSecurityClassification,
) -> DependencyPluginSecurityClassification {
    match classification {
        wire::PluginSecurityClassification::Public => {
            DependencyPluginSecurityClassification::Public
        }
        wire::PluginSecurityClassification::Internal => {
            DependencyPluginSecurityClassification::Internal
        }
        wire::PluginSecurityClassification::Private => {
            DependencyPluginSecurityClassification::Private
        }
        wire::PluginSecurityClassification::Confidential => {
            DependencyPluginSecurityClassification::Confidential
        }
    }
}

fn map_reference(reference: &DependencyPluginCanonicalReference) -> wire::PluginCanonicalReference {
    wire::PluginCanonicalReference {
        kind: match reference.kind {
            DependencyPluginCanonicalReferenceKind::Artifact => {
                wire::PluginCanonicalReferenceKind::Artifact
            }
            DependencyPluginCanonicalReferenceKind::NodeResult => {
                wire::PluginCanonicalReferenceKind::NodeResult
            }
            DependencyPluginCanonicalReferenceKind::ToolResult => {
                wire::PluginCanonicalReferenceKind::ToolResult
            }
            DependencyPluginCanonicalReferenceKind::ApprovalResult => {
                wire::PluginCanonicalReferenceKind::ApprovalResult
            }
            DependencyPluginCanonicalReferenceKind::Continuation => {
                wire::PluginCanonicalReferenceKind::Continuation
            }
            DependencyPluginCanonicalReferenceKind::ChildSession => {
                wire::PluginCanonicalReferenceKind::ChildSession
            }
        },
        id: reference.id.clone(),
        content_hash: reference.content_hash,
    }
}

fn unmap_reference(
    reference: wire::PluginCanonicalReference,
) -> DependencyPluginCanonicalReference {
    DependencyPluginCanonicalReference {
        kind: match reference.kind {
            wire::PluginCanonicalReferenceKind::Artifact => {
                DependencyPluginCanonicalReferenceKind::Artifact
            }
            wire::PluginCanonicalReferenceKind::NodeResult => {
                DependencyPluginCanonicalReferenceKind::NodeResult
            }
            wire::PluginCanonicalReferenceKind::ToolResult => {
                DependencyPluginCanonicalReferenceKind::ToolResult
            }
            wire::PluginCanonicalReferenceKind::ApprovalResult => {
                DependencyPluginCanonicalReferenceKind::ApprovalResult
            }
            wire::PluginCanonicalReferenceKind::Continuation => {
                DependencyPluginCanonicalReferenceKind::Continuation
            }
            wire::PluginCanonicalReferenceKind::ChildSession => {
                DependencyPluginCanonicalReferenceKind::ChildSession
            }
        },
        id: reference.id,
        content_hash: reference.content_hash,
    }
}

fn map_artifact(artifact: &DependencyPluginArtifactReference) -> wire::PluginArtifactReference {
    wire::PluginArtifactReference {
        artifact_id: artifact.artifact_id.clone(),
        content_hash: artifact.content_hash,
        media_type: artifact.media_type.clone(),
        size_bytes: artifact.size_bytes,
        security_classification: map_security(artifact.security_classification),
    }
}

fn unmap_artifact(artifact: wire::PluginArtifactReference) -> DependencyPluginArtifactReference {
    DependencyPluginArtifactReference {
        artifact_id: artifact.artifact_id,
        content_hash: artifact.content_hash,
        media_type: artifact.media_type,
        size_bytes: artifact.size_bytes,
        security_classification: unmap_security(artifact.security_classification),
    }
}

fn map_memory_retrieve_input(
    input: &DependencyPluginMemoryRetrieveInput,
) -> wire::PluginMemoryRetrieveRequest {
    wire::PluginMemoryRetrieveRequest {
        query: input.query.clone(),
        scopes: input.scopes.iter().copied().map(map_memory_scope).collect(),
        max_items: input.max_items,
        max_bytes: input.max_bytes,
        artifacts: input.artifacts.iter().map(map_artifact).collect(),
        references: input.references.iter().map(map_reference).collect(),
        parameters: input.parameters.clone(),
    }
}

/// Hashes the exact typed memory-retrieval request at the plugin protocol boundary.
///
/// # Errors
///
/// Returns [`PluginDependencyError::InvalidRequest`] when the request cannot be
/// represented by the bounded protocol contract.
pub fn plugin_memory_retrieve_request_hash(
    request: &DependencyPluginMemoryRetrieveRequest,
) -> Result<ContentHash, PluginDependencyError> {
    let timeout_ms = u64::try_from(request.timeout.as_millis())
        .map_err(|_| PluginDependencyError::InvalidRequest)?;
    wire::plugin_memory_retrieve_request_hash(
        &map_operation_binding(&request.binding),
        &request.provider_id,
        &request.provider_version,
        &request.handler,
        timeout_ms,
        wire::PluginOperationIdempotency::Idempotent,
        &map_memory_retrieve_input(&request.input),
        &request.readable_state,
    )
    .map_err(|_| PluginDependencyError::InvalidRequest)
}

fn map_memory_write_input(
    input: &DependencyPluginMemoryWriteInput,
) -> wire::PluginMemoryWriteRequest {
    wire::PluginMemoryWriteRequest {
        scope: map_memory_scope(input.scope),
        boundary: match input.boundary {
            DependencyPluginMemoryWriteBoundary::Explicit => {
                wire::PluginMemoryWriteBoundary::Explicit
            }
            DependencyPluginMemoryWriteBoundary::TurnCompletion => {
                wire::PluginMemoryWriteBoundary::TurnCompletion
            }
            DependencyPluginMemoryWriteBoundary::IterationCompletion => {
                wire::PluginMemoryWriteBoundary::IterationCompletion
            }
            DependencyPluginMemoryWriteBoundary::SessionCompletion => {
                wire::PluginMemoryWriteBoundary::SessionCompletion
            }
        },
        value: input.value.clone(),
        value_hash: input.value_hash,
        artifacts: input.artifacts.iter().map(map_artifact).collect(),
        references: input.references.iter().map(map_reference).collect(),
        security_classification: map_security(input.security_classification),
        parameters: input.parameters.clone(),
    }
}

fn map_compaction_input(input: &DependencyPluginCompactionInput) -> wire::PluginCompactionRequest {
    wire::PluginCompactionRequest {
        projection: input.projection.clone(),
        projection_hash: input.projection_hash,
        required_references: input
            .required_references
            .iter()
            .map(map_reference)
            .collect(),
        required_artifacts: input.required_artifacts.iter().map(map_artifact).collect(),
        preservation_requirements: input.preservation_requirements.clone(),
        max_replacement_bytes: input.max_replacement_bytes,
        max_projection_tokens: input.max_projection_tokens,
        parameters: input.parameters.clone(),
    }
}

/// Hashes the exact typed compaction request at the plugin protocol boundary.
///
/// # Errors
///
/// Returns [`PluginDependencyError::InvalidRequest`] when the request cannot be
/// represented by the bounded protocol contract.
pub fn plugin_compaction_request_hash(
    request: &DependencyPluginCompactionRequest,
) -> Result<ContentHash, PluginDependencyError> {
    let timeout_ms = u64::try_from(request.timeout.as_millis())
        .map_err(|_| PluginDependencyError::InvalidRequest)?;
    wire::plugin_compaction_request_hash(
        &map_operation_binding(&request.binding),
        &request.compactor_id,
        &request.compactor_version,
        &request.handler,
        timeout_ms,
        wire::PluginOperationIdempotency::Idempotent,
        &map_compaction_input(&request.input),
        &request.readable_state,
    )
    .map_err(|_| PluginDependencyError::InvalidRequest)
}

fn map_memory_retrieve_response(
    response: wire::PluginResponse,
    expected_binding: &DependencyPluginOperationBinding,
    request: &DependencyPluginMemoryRetrieveRequest,
) -> Result<DependencyPluginMemoryRetrieveProposal, PluginDependencyError> {
    let wire::PluginResponse::MemoryRetrieved { proposal, audit } = response else {
        return Err(map_failure(response));
    };
    let actual_binding = unmap_operation_binding(proposal.binding);
    if actual_binding != *expected_binding
        || proposal.provider_id != request.provider_id
        || proposal.provider_version != request.provider_version
        || audit.outcome != "completed"
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(DependencyPluginMemoryRetrieveProposal {
        binding: actual_binding,
        provider_id: proposal.provider_id,
        provider_version: proposal.provider_version,
        items: proposal
            .items
            .into_iter()
            .map(|item| DependencyPluginMemoryItemProposal {
                item_id: item.item_id,
                scope: unmap_memory_scope(item.scope),
                value: item.value,
                value_hash: item.value_hash,
                artifacts: item.artifacts.into_iter().map(unmap_artifact).collect(),
                references: item.references.into_iter().map(unmap_reference).collect(),
                security_classification: unmap_security(item.security_classification),
                metadata: item.metadata,
            })
            .collect(),
    })
}

fn map_memory_write_response(
    response: wire::PluginResponse,
    request: &DependencyPluginMemoryWriteRequest,
) -> Result<DependencyPluginMemoryWriteReceipt, PluginDependencyError> {
    let wire::PluginResponse::MemoryWritten { receipt, audit } = response else {
        return Err(PluginDependencyError::AmbiguousMemoryWrite);
    };
    let actual_binding = unmap_operation_binding(receipt.binding);
    if actual_binding != request.binding
        || receipt.provider_id != request.provider_id
        || receipt.provider_version != request.provider_version
        || receipt.value_hash != request.input.value_hash
        || audit.outcome != "completed"
    {
        return Err(PluginDependencyError::AmbiguousMemoryWrite);
    }
    Ok(DependencyPluginMemoryWriteReceipt {
        binding: actual_binding,
        provider_id: receipt.provider_id,
        provider_version: receipt.provider_version,
        provider_record_id: receipt.provider_record_id,
        value_hash: receipt.value_hash,
        receipt: receipt.receipt,
    })
}

fn map_memory_write_transport_error(error: PluginDependencyError) -> PluginDependencyError {
    match error {
        PluginDependencyError::Timeout
        | PluginDependencyError::Unavailable
        | PluginDependencyError::InvalidResponse => PluginDependencyError::AmbiguousMemoryWrite,
        other => other,
    }
}

fn map_compaction_response(
    response: wire::PluginResponse,
    expected_binding: &DependencyPluginOperationBinding,
    request: &DependencyPluginCompactionRequest,
) -> Result<DependencyPluginCompactionProposal, PluginDependencyError> {
    let wire::PluginResponse::CompactionProposed { proposal, audit } = response else {
        return Err(map_failure(response));
    };
    let actual_binding = unmap_operation_binding(proposal.binding);
    if actual_binding != *expected_binding
        || proposal.compactor_id != request.compactor_id
        || proposal.compactor_version != request.compactor_version
        || audit.outcome != "completed"
    {
        return Err(PluginDependencyError::InvalidResponse);
    }
    Ok(DependencyPluginCompactionProposal {
        binding: actual_binding,
        compactor_id: proposal.compactor_id,
        compactor_version: proposal.compactor_version,
        replacement: proposal.replacement,
        replacement_hash: proposal.replacement_hash,
        preserved_references: proposal
            .preserved_references
            .into_iter()
            .map(unmap_reference)
            .collect(),
        preserved_artifacts: proposal
            .preserved_artifacts
            .into_iter()
            .map(unmap_artifact)
            .collect(),
    })
}

fn map_state_failure(response: wire::PluginResponse) -> PluginDependencyError {
    match response {
        wire::PluginResponse::Failed { code, .. } if code == "stale_state_generation" => {
            PluginDependencyError::StaleStateGeneration
        }
        wire::PluginResponse::Failed { code, .. } if code == "state_conflict" => {
            PluginDependencyError::StateConflict
        }
        wire::PluginResponse::Failed { code, .. } if code == "cancelled" => {
            PluginDependencyError::Cancelled
        }
        wire::PluginResponse::Failed { code, .. } if code == "ambiguous_execution" => {
            PluginDependencyError::AmbiguousStatePersistence
        }
        other => map_failure(other),
    }
}

fn map_failure(response: wire::PluginResponse) -> PluginDependencyError {
    match response {
        wire::PluginResponse::Failed {
            code, retryable, ..
        } => PluginDependencyError::Rejected { code, retryable },
        _ => PluginDependencyError::InvalidResponse,
    }
}

fn validate_id(value: &str) -> Result<(), PluginDependencyError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        Err(PluginDependencyError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginDependencyError {
    #[error("invalid plugin dependency configuration")]
    InvalidConfiguration,
    #[error("invalid plugin request")]
    InvalidRequest,
    #[error("plugin host is unavailable")]
    Unavailable,
    #[error("plugin host request timed out")]
    Timeout,
    #[error("plugin host frame is too large")]
    FrameTooLarge,
    #[error("plugin host has reached its bounded pending-request limit")]
    PendingRequestLimit,
    #[error("plugin host returned an invalid response")]
    InvalidResponse,
    #[error("plugin host protocol does not support durable plugin-node state receipts")]
    StatePersistenceUnsupported,
    #[error("plugin host protocol does not support authenticated plugin-node state reads")]
    StateReadUnsupported,
    #[error("plugin host protocol does not support context-transform invocation")]
    ContextTransformUnsupported,
    #[error("plugin host protocol does not support memory or compaction invocation")]
    MemoryOperationUnsupported,
    #[error("plugin host protocol does not support authenticated invocation cancellation")]
    CancellationUnsupported,
    #[error("plugin host protocol does not support lifecycle management")]
    LifecycleManagementUnsupported,
    #[error("plugin-node state generation is stale")]
    StaleStateGeneration,
    #[error("plugin-node state identity conflicts with an existing receipt")]
    StateConflict,
    #[error("plugin-node state persistence was cancelled")]
    Cancelled,
    #[error("plugin-node state persistence may have completed without a terminal receipt")]
    AmbiguousStatePersistence,
    #[error("plugin-node state read did not return an unambiguous terminal receipt")]
    AmbiguousStateRead,
    #[error("plugin context-transform invocation is ambiguous")]
    AmbiguousContextTransform,
    #[error("approved plugin memory write may have completed without a terminal receipt")]
    AmbiguousMemoryWrite,
    #[error("plugin authorization failed")]
    Authorization,
    #[error("plugin host rejected the request with `{code}`")]
    Rejected { code: String, retryable: bool },
    #[error("system clock is invalid")]
    Clock,
}

#[cfg(test)]
mod memory_transport_tests {
    use super::*;
    use agentmod_protocol_support::authorization::{ExpectedAuthorization, verify_authorization};
    use serde_json::json;

    fn binding(request_hash: ContentHash) -> DependencyPluginOperationBinding {
        DependencyPluginOperationBinding {
            plugin_id: String::from("fixture.memory"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("session-1:run-1:memory-1"),
            operation_id: String::from("memory-operation-1"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: Some(String::from("memory-node")),
            declaration_hash: ContentHash::digest(b"declaration"),
            configuration_reference: ContentHash::digest(b"configuration"),
            request_hash,
            idempotency_key: String::from("memory-idempotency-1"),
            attempt: 1,
        }
    }

    fn retrieve_input() -> DependencyPluginMemoryRetrieveInput {
        DependencyPluginMemoryRetrieveInput {
            query: String::from("current goal"),
            scopes: BTreeSet::from([DependencyPluginMemoryScope::Session]),
            max_items: 4,
            max_bytes: 4096,
            artifacts: Vec::new(),
            references: Vec::new(),
            parameters: json!({}),
        }
    }

    fn retrieve_request() -> DependencyPluginMemoryRetrieveRequest {
        let input = retrieve_input();
        let request_hash = map_memory_retrieve_input(&input)
            .content_hash()
            .expect("request hash");
        DependencyPluginMemoryRetrieveRequest {
            binding: binding(request_hash),
            provider_id: String::from("fixture.provider"),
            provider_version: String::from("2.0.0"),
            handler: String::from("retrieve"),
            max_attempts: 2,
            retry_backoff: Duration::ZERO,
            timeout: Duration::from_millis(500),
            input,
            readable_state: json!({"session": "session-1"}),
            cancellation_id: String::from("cancel-1"),
        }
    }

    fn cancellation_request() -> DependencyCancelPluginInvocationRequest {
        let declaration_hash = ContentHash::digest(b"declaration");
        let request_hash = ContentHash::digest(b"request");
        DependencyCancelPluginInvocationRequest {
            target: DependencyPluginInvocationCancellationTarget {
                session_id: String::from("session-1"),
                run_id: String::from("run-1"),
                plugin_id: String::from("fixture.memory"),
                plugin_version: String::from("1.0.0"),
                invocation_id: String::from("session-1:run-1:memory-1"),
                invocation_digest: wire::plugin_invocation_identity_digest(
                    "session-1",
                    "run-1",
                    "fixture.memory",
                    "1.0.0",
                    "session-1:run-1:memory-1",
                    "memory-operation-1",
                    declaration_hash,
                    request_hash,
                )
                .expect("invocation digest"),
                operation_id: String::from("memory-operation-1"),
                declaration_hash,
                request_hash,
            },
            reason_code: String::from("user_cancelled"),
            nonce: String::from("explicit-nonce-1"),
            idempotency_key: String::from("cancel-key-1"),
            cancellation_id: String::from("cancellation-1"),
        }
    }

    fn dependency() -> ProcessPluginDependency {
        ProcessPluginDependency::new(ProcessPluginDependencyConfig {
            program: String::from("fixture-plugin-host"),
            arguments: Vec::new(),
            owner_id: String::from("owner"),
            runtime_api_version: String::from("1.0.0"),
            sessions_root: PathBuf::from("fixture-sessions"),
            executable_roots: vec![PathBuf::from("fixture-bin")],
            authorization_key: [9; 32],
            maximum_frame_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(1),
        })
        .expect("dependency")
    }

    fn audit(binding: &DependencyPluginOperationBinding, operation: &str) -> wire::PluginAudit {
        wire::PluginAudit {
            plugin_id: binding.plugin_id.clone(),
            invocation_id: Some(binding.invocation_id.clone()),
            operation: operation.to_owned(),
            outcome: String::from("completed"),
            attempts: binding.attempt,
        }
    }

    #[test]
    fn memory_operations_use_distinct_protocol_commands_and_exact_bindings() {
        let retrieve = retrieve_request();
        let retrieve_request = map_memory_retrieve_input(&retrieve.input);
        let retrieve_readable_state = retrieve.readable_state.clone();
        let mut retrieve_binding = map_operation_binding(&retrieve.binding);
        retrieve_binding.request_hash = wire::plugin_memory_retrieve_request_hash(
            &retrieve_binding,
            &retrieve.provider_id,
            &retrieve.provider_version,
            &retrieve.handler,
            1_000,
            wire::PluginOperationIdempotency::Idempotent,
            &retrieve_request,
            &retrieve_readable_state,
        )
        .expect("complete retrieve request hash");
        let retrieve_command = wire::PluginCommand::InvokeMemoryRetrieve {
            binding: retrieve_binding,
            provider_id: retrieve.provider_id.clone(),
            provider_version: retrieve.provider_version.clone(),
            handler: retrieve.handler.clone(),
            timeout_ms: 1_000,
            idempotency: wire::PluginOperationIdempotency::Idempotent,
            request: retrieve_request,
            readable_state: retrieve_readable_state,
            authorization: wire::PluginAuthorization {
                owner_id: String::from("owner"),
                session_id: retrieve.binding.session_id.clone(),
                call_id: String::from("call-1"),
                normalized_digest: String::from("digest"),
                grant: String::from("grant"),
                cancellation_id: retrieve.cancellation_id.clone(),
            },
        };
        retrieve_command
            .validate_contract()
            .expect("valid exact retrieve command");
        let tag = serde_json::to_value(&retrieve_command).expect("command JSON");
        assert_eq!(tag["command"], "invoke_memory_retrieve");

        let value = json!({"fact": "approved"});
        let write_input = DependencyPluginMemoryWriteInput {
            scope: DependencyPluginMemoryScope::Session,
            boundary: DependencyPluginMemoryWriteBoundary::IterationCompletion,
            value: value.clone(),
            value_hash: ContentHash::digest(&serde_json::to_vec(&value).expect("canonical value")),
            artifacts: Vec::new(),
            references: Vec::new(),
            security_classification: DependencyPluginSecurityClassification::Private,
            parameters: json!({}),
        };
        let write_wire = map_memory_write_input(&write_input);
        assert_ne!(
            serde_json::to_value(write_wire).expect("write JSON"),
            tag["request"]
        );
        assert_eq!(
            serde_json::to_value(wire::PluginCommand::InvokeCompaction {
                binding: map_operation_binding(&retrieve.binding),
                compactor_id: String::from("fixture.compactor"),
                compactor_version: String::from("1.0.0"),
                handler: String::from("compact"),
                timeout_ms: 1_000,
                idempotency: wire::PluginOperationIdempotency::Idempotent,
                request: wire::PluginCompactionRequest {
                    projection: json!([]),
                    projection_hash: ContentHash::digest(b"[]"),
                    required_references: Vec::new(),
                    required_artifacts: Vec::new(),
                    preservation_requirements: BTreeSet::new(),
                    max_replacement_bytes: 1024,
                    max_projection_tokens: 64,
                    parameters: json!({}),
                },
                readable_state: json!({}),
                authorization: match retrieve_command {
                    wire::PluginCommand::InvokeMemoryRetrieve { authorization, .. } =>
                        authorization,
                    _ => unreachable!("retrieve command"),
                },
            })
            .expect("compaction JSON")["command"],
            "invoke_compaction"
        );
    }

    #[test]
    fn authorization_digest_binds_the_exact_cancellation_identity() {
        let request = retrieve_request();
        let wire_binding = map_operation_binding(&request.binding);
        let wire_input = map_memory_retrieve_input(&request.input);
        let normalized_input = serde_json::to_value(&wire_input).expect("normalized input");
        let timeout_ms =
            u64::try_from(request.timeout.as_millis()).expect("bounded fixture timeout");
        let operation = (
            &wire_binding,
            &request.provider_id,
            &request.provider_version,
            &request.handler,
            timeout_ms,
            wire::PluginOperationIdempotency::Idempotent,
            &normalized_input,
            &request.readable_state,
            &request.cancellation_id,
        );
        let authorization = dependency()
            .authorization(
                &request.binding.session_id,
                String::from("call-1"),
                request.cancellation_id.clone(),
                "plugin.memory.retrieve.invoke",
                &operation,
            )
            .expect("authorization");
        assert_eq!(
            authorization.normalized_digest,
            ContentHash::digest(
                &serde_json::to_vec(&operation).expect("normalized operation tuple")
            )
            .to_hex()
        );

        let substituted_cancellation = String::from("cancel-substituted");
        let substituted = (
            &wire_binding,
            &request.provider_id,
            &request.provider_version,
            &request.handler,
            timeout_ms,
            wire::PluginOperationIdempotency::Idempotent,
            &normalized_input,
            &request.readable_state,
            &substituted_cancellation,
        );
        assert_ne!(
            authorization.normalized_digest,
            ContentHash::digest(
                &serde_json::to_vec(&substituted).expect("substituted operation tuple")
            )
            .to_hex()
        );
        let substituted_timeout = (
            &wire_binding,
            &request.provider_id,
            &request.provider_version,
            &request.handler,
            timeout_ms + 1,
            wire::PluginOperationIdempotency::Idempotent,
            &normalized_input,
            &request.readable_state,
            &request.cancellation_id,
        );
        assert_ne!(
            authorization.normalized_digest,
            ContentHash::digest(
                &serde_json::to_vec(&substituted_timeout)
                    .expect("substituted timeout operation tuple")
            )
            .to_hex()
        );
    }

    #[test]
    fn retrieve_response_requires_exact_echoed_identity_and_valid_payload_hashes() {
        let request = retrieve_request();
        let item_value = json!({"fact": "remember"});
        let response = wire::PluginResponse::MemoryRetrieved {
            proposal: wire::PluginMemoryRetrieveProposal {
                binding: map_operation_binding(&request.binding),
                provider_id: request.provider_id.clone(),
                provider_version: request.provider_version.clone(),
                items: vec![wire::PluginMemoryItemProposal {
                    item_id: String::from("item-1"),
                    scope: wire::PluginMemoryScope::Session,
                    value: item_value.clone(),
                    value_hash: ContentHash::digest(
                        &serde_json::to_vec(&item_value).expect("item value"),
                    ),
                    artifacts: Vec::new(),
                    references: Vec::new(),
                    security_classification: wire::PluginSecurityClassification::Private,
                    metadata: BTreeMap::new(),
                }],
            },
            audit: audit(&request.binding, "memory_retrieve"),
        };
        response.validate_contract().expect("valid typed response");
        assert_eq!(
            map_memory_retrieve_response(response, &request.binding, &request)
                .expect("exact proposal")
                .items
                .len(),
            1
        );

        let mut substituted = request.binding.clone();
        substituted.declaration_hash = ContentHash::digest(b"substitution");
        let response = wire::PluginResponse::MemoryRetrieved {
            proposal: wire::PluginMemoryRetrieveProposal {
                binding: map_operation_binding(&substituted),
                provider_id: request.provider_id.clone(),
                provider_version: request.provider_version.clone(),
                items: Vec::new(),
            },
            audit: audit(&substituted, "memory_retrieve"),
        };
        assert_eq!(
            map_memory_retrieve_response(response, &request.binding, &request),
            Err(PluginDependencyError::InvalidResponse)
        );
    }

    #[test]
    fn attempt_bounds_and_non_idempotent_write_recovery_are_fail_closed() {
        assert_eq!(validate_pure_attempt_bound(1, 2, Duration::ZERO), Ok(()));
        assert_eq!(
            validate_pure_attempt_bound(1, 17, Duration::ZERO),
            Err(PluginDependencyError::InvalidRequest)
        );
        assert_eq!(
            validate_pure_attempt_bound(2, 1, Duration::ZERO),
            Err(PluginDependencyError::InvalidRequest)
        );
        for error in [
            PluginDependencyError::Timeout,
            PluginDependencyError::Unavailable,
            PluginDependencyError::InvalidResponse,
        ] {
            assert_eq!(
                map_memory_write_transport_error(error),
                PluginDependencyError::AmbiguousMemoryWrite
            );
        }
    }

    #[test]
    fn cancellation_authorization_binds_exact_action_and_explicit_nonce() {
        let dependency = dependency();
        let request = cancellation_request();
        let target = map_cancellation_target(&request.target);
        let digest = wire::plugin_invocation_cancellation_action_digest(
            &target,
            &request.reason_code,
            &request.nonce,
            &request.idempotency_key,
            &request.cancellation_id,
        )
        .expect("action digest");
        let authorization = dependency
            .cancellation_authorization(
                &request.target,
                String::from("call-1"),
                request.cancellation_id.clone(),
                &request.nonce,
                digest,
            )
            .expect("authorization");
        assert_eq!(authorization.normalized_digest, digest.to_hex());
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock");
        let now = i64::try_from(now.as_millis()).expect("timestamp");
        let claims = verify_authorization(
            &authorization.grant,
            &AuthorizationKey::from_bytes([9; 32]),
            ExpectedAuthorization {
                owner: "owner",
                session: "session-1",
                call_id: "call-1",
                action: "plugin.invocation.cancel",
                normalized_digest: digest,
            },
            TimestampMillis::new(now),
        )
        .expect("verified grant");
        assert_eq!(claims.nonce, request.nonce);

        let substituted_digest = wire::plugin_invocation_cancellation_action_digest(
            &wire::PluginInvocationCancellationTarget {
                run_id: String::from("run-2"),
                ..target
            },
            &request.reason_code,
            &request.nonce,
            &request.idempotency_key,
            &request.cancellation_id,
        )
        .expect("substitution digest");
        assert_ne!(digest, substituted_digest);
    }

    #[test]
    fn cancellation_response_validation_rejects_every_target_substitution() {
        let request = cancellation_request();
        let target = map_cancellation_target(&request.target);
        let action_digest = wire::plugin_invocation_cancellation_action_digest(
            &target,
            &request.reason_code,
            &request.nonce,
            &request.idempotency_key,
            &request.cancellation_id,
        )
        .expect("action digest");
        let mut receipt = wire::PluginInvocationCancellationReceipt {
            target,
            reason_code: request.reason_code.clone(),
            action_digest,
            nonce: request.nonce.clone(),
            idempotency_key: request.idempotency_key.clone(),
            cancellation_id: request.cancellation_id.clone(),
            status: wire::PluginInvocationCancellationStatus::AlreadyTerminal,
            receipt_id: String::from("receipt-1"),
            receipt_digest: ContentHash::digest(b"pending"),
        };
        receipt.receipt_digest =
            wire::plugin_invocation_cancellation_receipt_digest(&receipt).expect("receipt digest");
        assert_eq!(
            map_cancellation_response(
                wire::PluginResponse::InvocationCancelled {
                    receipt: Box::new(receipt.clone())
                },
                &request,
                action_digest,
            )
            .expect("exact receipt")
            .status,
            DependencyPluginInvocationCancellationStatus::AlreadyTerminal
        );

        receipt.target.request_hash = ContentHash::digest(b"substitution");
        receipt.receipt_digest =
            wire::plugin_invocation_cancellation_receipt_digest(&receipt).expect("receipt digest");
        assert_eq!(
            map_cancellation_response(
                wire::PluginResponse::InvocationCancelled {
                    receipt: Box::new(receipt)
                },
                &request,
                action_digest,
            ),
            Err(PluginDependencyError::InvalidResponse)
        );
    }

    #[tokio::test]
    async fn transport_failure_closes_every_correlated_waiter() {
        let pending = Arc::new(Mutex::new(BTreeMap::new()));
        let (first_sender, first_receiver) = oneshot::channel();
        let (second_sender, second_receiver) = oneshot::channel();
        {
            let mut waiters = pending.lock().await;
            waiters.insert(
                String::from("correlation-1"),
                PendingPluginResponse {
                    operation: PendingPluginOperation::Interceptor,
                    sender: first_sender,
                },
            );
            waiters.insert(
                String::from("correlation-2"),
                PendingPluginResponse {
                    operation: PendingPluginOperation::Observer,
                    sender: second_sender,
                },
            );
        }

        fail_plugin_waiters(&pending, PluginTransportFailure::InvalidResponse).await;

        assert!(pending.lock().await.is_empty());
        assert!(matches!(
            first_receiver.await,
            Ok(Err(PluginTransportFailure::InvalidResponse))
        ));
        assert!(matches!(
            second_receiver.await,
            Ok(Err(PluginTransportFailure::InvalidResponse))
        ));
    }

    #[test]
    fn every_plugin_operation_class_blocks_idle_transport_teardown() {
        let operations = [
            PendingPluginOperation::NodeInvocation,
            PendingPluginOperation::ContextTransform,
            PendingPluginOperation::MemoryRetrieve,
            PendingPluginOperation::MemoryWrite,
            PendingPluginOperation::Compaction,
            PendingPluginOperation::StateCas,
            PendingPluginOperation::StateRead,
            PendingPluginOperation::Interceptor,
            PendingPluginOperation::Observer,
            PendingPluginOperation::Lifecycle,
            PendingPluginOperation::OtherTransport,
        ];
        for operation in operations {
            let (sender, _receiver) = oneshot::channel();
            let pending = BTreeMap::from([(
                String::from("correlation"),
                PendingPluginResponse { operation, sender },
            )]);
            assert!(
                has_pending_plugin_operation(&pending),
                "{operation:?} must block teardown"
            );
        }
    }
}

#[cfg(test)]
mod state_persistence_tests {
    use super::*;
    use serde_json::json;

    fn dependency() -> ProcessPluginDependency {
        ProcessPluginDependency::new(ProcessPluginDependencyConfig {
            program: String::from("fixture-plugin-host"),
            arguments: Vec::new(),
            owner_id: String::from("owner"),
            runtime_api_version: String::from("1.0.0"),
            sessions_root: PathBuf::from("fixture-sessions"),
            executable_roots: vec![PathBuf::from("fixture-bin")],
            authorization_key: [7; 32],
            maximum_frame_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(1),
        })
        .expect("dependency")
    }

    fn request() -> DependencyPersistPluginNodeStateRequest {
        let state = json!({"cursor": 2});
        let state_hash =
            ContentHash::digest(&serde_json::to_vec(&state).expect("bounded fixture state"));
        let declaration_hash = ContentHash::digest(b"declaration");
        let cancellation_target = DependencyPluginInvocationCancellationTarget {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest: wire::plugin_invocation_identity_digest(
                "session-1",
                "run-1",
                "fixture.plugin",
                "1.0.0",
                "plugin-node:invocation",
                "fixture.executor:state-write",
                declaration_hash,
                state_hash,
            )
            .expect("invocation digest"),
            operation_id: String::from("fixture.executor:state-write"),
            declaration_hash,
            request_hash: state_hash,
        };
        DependencyPersistPluginNodeStateRequest {
            cancellation_target,
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest: ContentHash::digest(b"invocation"),
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference: ContentHash::digest(b"configuration"),
            state_scope: DependencyPluginNodeStateScope::Invocation,
            prior_generation: 1,
            prior_state_hash: Some(ContentHash::digest(b"prior")),
            state_hash,
            state,
            action_digest: ContentHash::digest(b"action"),
            authorization_digest: ContentHash::digest(b"authorization"),
            nonce: String::from("nonce-1"),
            cancellation_id: String::from("cancel-1"),
            idempotency_key: String::from("state-write-1"),
        }
    }

    fn read_request() -> DependencyLoadPluginNodeStateRequest {
        let declaration_hash = ContentHash::digest(b"declaration");
        let expected_state_hash =
            ContentHash::digest(&serde_json::to_vec(&json!({"cursor": 2})).expect("state"));
        let cancellation_target = DependencyPluginInvocationCancellationTarget {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("plugin-node:later"),
            invocation_digest: wire::plugin_invocation_identity_digest(
                "session-1",
                "run-1",
                "fixture.plugin",
                "1.0.0",
                "plugin-node:later",
                "fixture.executor:state-read",
                declaration_hash,
                expected_state_hash,
            )
            .expect("invocation digest"),
            operation_id: String::from("fixture.executor:state-read"),
            declaration_hash,
            request_hash: expected_state_hash,
        };
        let mut request = DependencyLoadPluginNodeStateRequest {
            cancellation_target,
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("plugin-node:later"),
            invocation_digest: ContentHash::digest(b"later"),
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference: ContentHash::digest(b"configuration"),
            state_scope: DependencyPluginNodeStateScope::Session,
            expected_generation: 2,
            expected_state_hash,
            action_digest: ContentHash::digest(b"pending"),
            authorization_digest: ContentHash::digest(b"pending"),
            nonce: String::from("read-nonce-1"),
            cancellation_id: String::from("read-cancel-1"),
            idempotency_key: String::from("read-1"),
        };
        request.action_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                &request.session_id,
                &request.plugin_id,
                &request.invocation_id,
                request.invocation_digest,
                &request.executor_id,
                &request.executor_version,
                request.executor_declaration_hash,
                request.state_scope,
                request.expected_generation,
                request.expected_state_hash,
                &request.idempotency_key,
            ))
            .expect("action"),
        );
        request.authorization_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                request.action_digest,
                &request.nonce,
                &request.cancellation_id,
                &request.idempotency_key,
            ))
            .expect("authorization"),
        );
        request
    }

    #[tokio::test]
    async fn state_persistence_rejects_substituted_hash_before_process_exchange() {
        let mut request = request();
        request.state_hash = ContentHash::digest(b"substituted");
        assert_eq!(
            dependency().persist_plugin_node_state(request).await,
            Err(PluginDependencyError::InvalidRequest)
        );
    }

    #[test]
    fn state_read_response_rejects_raw_state_or_receipt_substitution() {
        let request = read_request();
        let mut receipt = DependencyPluginNodeStateReadReceipt {
            plugin_id: request.plugin_id.clone(),
            invocation_id: request.invocation_id.clone(),
            invocation_digest: request.invocation_digest,
            executor_id: request.executor_id.clone(),
            executor_version: request.executor_version.clone(),
            executor_declaration_hash: request.executor_declaration_hash,
            state_scope: request.state_scope,
            generation: request.expected_generation,
            state_hash: request.expected_state_hash,
            action_digest: request.action_digest,
            authorization_digest: request.authorization_digest,
            idempotency_key: request.idempotency_key.clone(),
            receipt_id: String::from("read-receipt-1"),
            receipt_digest: ContentHash::digest(b"pending"),
            replayed: false,
        };
        receipt.receipt_digest =
            plugin_node_state_read_receipt_digest(&receipt).expect("receipt digest");
        let response = wire::PluginResponse::NodeStateLoaded {
            state: json!({"cursor": 2}),
            receipt: Box::new(wire::PluginNodeStateReadReceipt {
                plugin_id: receipt.plugin_id,
                invocation_id: receipt.invocation_id,
                invocation_digest: receipt.invocation_digest.to_hex(),
                executor_id: receipt.executor_id,
                executor_version: receipt.executor_version,
                executor_declaration_hash: receipt.executor_declaration_hash.to_hex(),
                state_scope: wire::PluginNodeStateScope::Session,
                generation: receipt.generation,
                state_hash: receipt.state_hash.to_hex(),
                action_digest: receipt.action_digest.to_hex(),
                authorization_digest: receipt.authorization_digest.to_hex(),
                idempotency_key: receipt.idempotency_key,
                receipt_id: receipt.receipt_id,
                receipt_digest: receipt.receipt_digest.to_hex(),
                replayed: false,
            }),
            audit: wire::PluginAudit {
                plugin_id: request.plugin_id.clone(),
                invocation_id: Some(request.invocation_id.clone()),
                operation: String::from("load_node_state"),
                outcome: String::from("loaded"),
                attempts: 1,
            },
        };
        assert!(
            map_node_state_read_response(&request, response.clone(), 1024)
                .expect("exact response")
                .state
                .is_object()
        );
        let wire::PluginResponse::NodeStateLoaded { receipt, audit, .. } = response else {
            panic!("node state response")
        };
        assert_eq!(
            map_node_state_read_response(
                &request,
                wire::PluginResponse::NodeStateLoaded {
                    state: json!({"cursor": 3}),
                    receipt,
                    audit,
                },
                1024,
            ),
            Err(PluginDependencyError::InvalidResponse)
        );
    }
}

#[cfg(test)]
mod idle_teardown_process_tests {
    use super::*;
    use serde_json::json;

    fn observer_manifest(worker: &str) -> wire::PluginManifest {
        wire::PluginManifest {
            schema_version: 1,
            id: String::from("fixture.observer"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            category: String::from("observer"),
            scope: String::from("session"),
            class: wire::PluginClass::Observer,
            entrypoint: wire::PluginEntrypoint {
                program: worker.to_owned(),
                arguments: Vec::new(),
            },
            required_capabilities: BTreeSet::new(),
            provided_capabilities: BTreeSet::new(),
            subscribed_events: BTreeSet::from([String::from("tool.execution_completed")]),
            read_authority: BTreeSet::from([String::from("session_state")]),
            proposed_write_authority: BTreeSet::new(),
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            after: BTreeSet::new(),
            before: BTreeSet::new(),
            stage: 0,
            priority: 0,
            timeout_ms: 2_000,
            failure_policy: String::from("continue"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            state_migration_version: 1,
            configuration_schema: wire::PluginConfigurationSchema {
                id: String::from("fixture.observer.config"),
                version: 1,
                required: false,
                inline_json: String::from(r#"{"type":"object","additionalProperties":false}"#),
            },
            node_executors: Vec::new(),
            context_transforms: Vec::new(),
            memory_providers: Vec::new(),
            compactors: Vec::new(),
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the real process proof keeps setup, quiescence gates, receipt, and OS-exit assertion together"
    )]
    async fn pending_runtime_delivery_blocks_process_teardown_and_terminal_receipt_allows_it() {
        let Ok(host) = std::env::var("AGENTMOD_TEST_PLUGIN_HOST") else {
            return;
        };
        let worker = std::env::var("AGENTMOD_TEST_PLUGIN_WORKER")
            .expect("fixture worker path accompanies plugin host");
        let worker_path = PathBuf::from(&worker);
        let executable_root = worker_path
            .parent()
            .expect("fixture worker parent")
            .to_path_buf();
        let root = std::env::temp_dir().join(format!(
            "agentmod-plugin-idle-process-{}",
            uuid::Uuid::now_v7()
        ));
        let sessions_root = root.join("sessions");
        let dependency = ProcessPluginDependency::new(ProcessPluginDependencyConfig {
            program: host,
            arguments: Vec::new(),
            owner_id: String::from("runtime"),
            runtime_api_version: String::from("1.0.0"),
            sessions_root: sessions_root.clone(),
            executable_roots: vec![executable_root],
            authorization_key: [17; 32],
            maximum_frame_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(5),
        })
        .expect("process dependency");
        let session_id = String::from("idle-process-session");
        dependency
            .negotiate(session_id.clone(), String::from("1.0.0"), BTreeSet::new())
            .await
            .expect("negotiate");
        let manifest = observer_manifest(&worker);
        dependency
            .load(DependencyPluginLoadRequest {
                session_id: session_id.clone(),
                manifest_json: serde_json::to_string(&manifest).expect("manifest"),
                configuration: json!({}),
                cancellation_id: String::from("load-observer"),
            })
            .await
            .expect("load observer");
        assert!(
            !dependency
                .teardown_session_if_idle(&session_id, 0, 1)
                .await
                .expect("pending delivery guard")
        );
        let terminal = dependency
            .observe(DependencyPluginObservationRequest {
                session_id: session_id.clone(),
                plugin_id: manifest.id,
                invocation_id: String::from("observer-delivery-1"),
                handler: String::from("observe:tool.execution_completed"),
                event_type: String::from("tool.execution_completed"),
                event: json!({
                    "event_id":"fixture-event-1",
                    "sequence":1,
                    "event_type":"tool.execution_completed",
                    "payload":{"ok":true}
                }),
                cancellation_id: String::from("observer-cancel-1"),
            })
            .await
            .expect("terminal observer receipt");
        assert_eq!(
            terminal.status,
            DependencyPluginObserverDeliveryStatus::Completed
        );
        assert!(!terminal.receipt_id.is_empty());
        assert!(
            !dependency
                .teardown_session_if_idle(&session_id, 1, 0)
                .await
                .expect("continuation guard")
        );
        let connection = dependency
            .connections
            .lock()
            .await
            .get(&session_id)
            .cloned()
            .expect("live host connection");
        assert!(
            dependency
                .teardown_session_if_idle(&session_id, 0, 0)
                .await
                .expect("idle teardown")
        );
        assert!(
            !dependency
                .connections
                .lock()
                .await
                .contains_key(&session_id)
        );
        let mut exited = false;
        for _ in 0..50 {
            if connection
                .child
                .lock()
                .await
                .try_wait()
                .expect("host process status")
                .is_some()
            {
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(exited, "idle plugin-host process must be terminated");
        let _ = fs::remove_dir_all(root).await;
    }
}
