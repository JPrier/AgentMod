//! External process, persistence, authorization, and plugin-SDK adapters.
#![allow(
    missing_docs,
    reason = "dependency-local node executor mapping records remain boundary-specific"
)]

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
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, RwLock, mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_NODE_STATE_READ_RECEIPTS: usize = 4_096;
const MAX_CANCELLATION_RECEIPTS: usize = 4_096;

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
    /// Exact isolated graph-node executors.
    pub node_executors: Vec<DependencyNodeExecutorDeclaration>,
    /// Exact isolated context transforms.
    pub context_transforms: Vec<DependencyContextTransformDeclaration>,
    /// Exact isolated memory providers.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub memory_providers: Vec<DependencyMemoryProviderDeclaration>,
    /// Exact isolated provider-projection compactors.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub compactors: Vec<DependencyCompactorDeclaration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyOperationIdempotency {
    Idempotent,
    NonIdempotent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyOperationDeclaration {
    pub handler: String,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: DependencyOperationIdempotency,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyMemoryProviderDeclaration {
    pub provider_id: String,
    pub version: String,
    pub runtime_api: String,
    pub capabilities: Vec<String>,
    pub retrieve: DependencyOperationDeclaration,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write: Option<DependencyOperationDeclaration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCompactorDeclaration {
    pub compactor_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub capabilities: Vec<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: DependencyOperationIdempotency,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SerializedOperationFailurePolicy {
    Reject,
    Cancel,
    Disable,
    Continue,
    Retry { max_attempts: u8, backoff_ms: u64 },
}

#[derive(Serialize)]
struct SerializedOperationPermissions<'a> {
    tools: &'a [String],
    network: &'a [String],
}

fn serialized_failure_policy(
    policy: &str,
    max_attempts: u8,
    backoff_ms: u64,
) -> Result<SerializedOperationFailurePolicy, &'static str> {
    match policy {
        "reject" => Ok(SerializedOperationFailurePolicy::Reject),
        "cancel" => Ok(SerializedOperationFailurePolicy::Cancel),
        "disable" => Ok(SerializedOperationFailurePolicy::Disable),
        "continue" => Ok(SerializedOperationFailurePolicy::Continue),
        "retry" => Ok(SerializedOperationFailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        }),
        _ => Err("invalid operation failure policy"),
    }
}

impl Serialize for DependencyOperationDeclaration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let failure = serialized_failure_policy(
            &self.failure_policy,
            self.max_attempts,
            self.retry_backoff_ms,
        )
        .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("PluginOperationDeclaration", 10)?;
        state.serialize_field("handler", &self.handler)?;
        state.serialize_field("input_schema", &self.input_schema)?;
        state.serialize_field("output_schema", &self.output_schema)?;
        state.serialize_field("timeout_ms", &self.timeout_ms)?;
        state.serialize_field("failure_policy", &failure)?;
        state.serialize_field("idempotency", &self.idempotency)?;
        state.serialize_field(
            "required_permissions",
            &SerializedOperationPermissions {
                tools: &self.tool_permissions,
                network: &self.network_permissions,
            },
        )?;
        state.serialize_field("state_scope", &self.state_scope)?;
        state.serialize_field("external_effects", &self.external_effects)?;
        state.end()
    }
}

impl Serialize for DependencyCompactorDeclaration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let failure = serialized_failure_policy(
            &self.failure_policy,
            self.max_attempts,
            self.retry_backoff_ms,
        )
        .map_err(serde::ser::Error::custom)?;
        let mut state = serializer.serialize_struct("PluginCompactorDeclaration", 15)?;
        state.serialize_field("compactor_id", &self.compactor_id)?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("runtime_api", &self.runtime_api)?;
        state.serialize_field("handler", &self.handler)?;
        state.serialize_field("capabilities", &self.capabilities)?;
        state.serialize_field("input_schema", &self.input_schema)?;
        state.serialize_field("output_schema", &self.output_schema)?;
        state.serialize_field("timeout_ms", &self.timeout_ms)?;
        state.serialize_field("failure_policy", &failure)?;
        state.serialize_field("idempotency", &self.idempotency)?;
        state.serialize_field(
            "required_permissions",
            &SerializedOperationPermissions {
                tools: &self.tool_permissions,
                network: &self.network_permissions,
            },
        )?;
        state.serialize_field("state_scope", &self.state_scope)?;
        state.serialize_field("external_effects", &self.external_effects)?;
        state.end()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyContextTransformLifecycle {
    BeforeModelRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyContextTransformIdempotency {
    Idempotent,
    NonIdempotent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyContextTransformDeclaration {
    pub transform_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub lifecycle: DependencyContextTransformLifecycle,
    pub capabilities: BTreeSet<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: DependencyContextTransformIdempotency,
    pub tool_permissions: BTreeSet<String>,
    pub network_permissions: BTreeSet<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

/// Dependency-owned exact node-executor declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyNodeExecutorDeclaration {
    pub executor_id: String,
    pub version: String,
    pub runtime_api: String,
    pub node_kind: String,
    pub handler: String,
    pub capabilities: BTreeSet<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: DependencyNodeExecutorIdempotency,
    pub tool_permissions: BTreeSet<String>,
    pub network_permissions: BTreeSet<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyNodeExecutorIdempotency {
    Idempotent,
    NonIdempotent,
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
    pub cancellation_target: Option<DependencyInvocationCancellationTarget>,
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Handler/tool.
    pub handler: String,
    /// Exact executor ID for node invocation.
    pub executor_id: Option<String>,
    /// Exact executor version for node invocation.
    pub executor_version: Option<String>,
    /// Exact selected operation timeout for node execution.
    pub timeout_ms: Option<u64>,
    /// Exact activated-plugin configuration for node execution.
    pub configuration_reference: Option<ContentHash>,
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

#[derive(Clone, Debug)]
pub struct DependencyContextTransformRequest {
    pub cancellation_target: DependencyInvocationCancellationTarget,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub transform_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: ContentHash,
    pub lifecycle: DependencyContextTransformLifecycle,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub authorization: DependencyAuthorization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyContextTransformProposal {
    pub replacement: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DependencyOperationBinding {
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub operation_id: String,
    pub session_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub request_hash: ContentHash,
    pub idempotency_key: String,
    pub attempt: u8,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DependencyInvocationCancellationTarget {
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyInvocationCancellationStatus {
    Signalled,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DependencyInvocationCancellationReceipt {
    pub target: DependencyInvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: ContentHash,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: DependencyInvocationCancellationStatus,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
}

#[derive(Clone, Debug)]
pub struct DependencyCancelInvocationRequest {
    pub target: DependencyInvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: ContentHash,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: DependencyAuthorization,
}

#[derive(Clone, Debug)]
pub struct DependencyMemoryRetrieveRequest {
    pub binding: DependencyOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: DependencyOperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: DependencyAuthorization,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyMemoryRetrieveProposal {
    pub binding: DependencyOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub items: Value,
}
#[derive(Clone, Debug)]
pub struct DependencyMemoryWriteRequest {
    pub binding: DependencyOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: DependencyOperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: DependencyAuthorization,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyMemoryWriteReceipt {
    pub binding: DependencyOperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub provider_record_id: String,
    pub value_hash: ContentHash,
    pub receipt: Value,
}
#[derive(Clone, Debug)]
pub struct DependencyCompactionRequest {
    pub binding: DependencyOperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: DependencyOperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: DependencyAuthorization,
}
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyCompactionProposal {
    pub binding: DependencyOperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub replacement: Value,
    pub replacement_hash: ContentHash,
    pub preserved_references: Value,
    pub preserved_artifacts: Value,
}

/// Observer request.
#[derive(Clone, Debug)]
pub struct DependencyObservationRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Invocation.
    pub invocation_id: String,
    /// Handler.
    pub handler: String,
    /// Event type.
    pub event_type: String,
    /// Event.
    pub event: Value,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

/// State-change request.
#[derive(Clone, Debug)]
pub struct DependencyStateChangeRequest {
    /// Plugin.
    pub plugin_id: String,
    /// Exact immutable plugin version.
    pub plugin_version: String,
    /// Exact immutable configuration reference.
    pub configuration_reference: ContentHash,
    /// Reason.
    pub reason: Option<String>,
    /// Authorization.
    pub authorization: DependencyAuthorization,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug)]
pub struct DependencyPersistNodeStateRequest {
    pub cancellation_target: DependencyInvocationCancellationTarget,
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
    pub idempotency_key: String,
    pub authorization: DependencyAuthorization,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug)]
pub struct DependencyLoadNodeStateRequest {
    pub cancellation_target: DependencyInvocationCancellationTarget,
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
    pub idempotency_key: String,
    pub authorization: DependencyAuthorization,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
pub struct DependencyLoadedNodeState {
    pub state: Value,
    pub receipt: DependencyPluginNodeStateReadReceipt,
}

/// Hashes the immutable identity of one terminal state receipt.
///
/// # Errors
///
/// Returns [`PluginDependencyError::Invalid`] when encoding fails.
pub fn node_state_receipt_digest(
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
    .map_err(|_| PluginDependencyError::Invalid)
}

/// Hashes the immutable identity of one terminal state-read receipt.
///
/// # Errors
///
/// Returns [`PluginDependencyError::Invalid`] when encoding fails.
pub fn node_state_read_receipt_digest(
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
    .map_err(|_| PluginDependencyError::Invalid)
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
    /// Non-authoritative node outcome proposal.
    NodeOutcome(DependencyNodeOutcomeProposal),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyNodeActionProposal {
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DependencyNodeOutcomeProposal {
    pub output: Value,
    pub preserved_state: Value,
    pub proposed_actions: Vec<DependencyNodeActionProposal>,
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
    /// Terminal delivery classification.
    pub status: DependencyObserverDeliveryStatus,
    /// Exact request hash.
    pub request_hash: ContentHash,
    /// Stable terminal receipt identity.
    pub receipt_id: String,
    /// Digest of the exact terminal receipt.
    pub receipt_digest: ContentHash,
    /// Whether an existing exact receipt was returned.
    pub replayed: bool,
}

/// Terminal observer delivery classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyObserverDeliveryStatus {
    Completed,
    Rejected,
    Failed,
    Ambiguous,
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
}

/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHealth {
    /// Loaded.
    pub loaded: usize,
    /// Running.
    pub running: usize,
    /// Queued or actively executing observer workers.
    pub observer_pending: u64,
    /// Drops.
    pub observer_dropped: u64,
    /// Whether durable state has no unterminated observer delivery.
    pub state_flushed: bool,
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
    /// Invokes one exact pure context transform.
    async fn invoke_context_transform(
        &self,
        request: DependencyContextTransformRequest,
    ) -> Result<(DependencyContextTransformProposal, u8), PluginDependencyError>;
    /// Invokes one exact pure memory retrieval.
    async fn invoke_memory_retrieve(
        &self,
        request: DependencyMemoryRetrieveRequest,
    ) -> Result<(DependencyMemoryRetrieveProposal, u8), PluginDependencyError>;
    /// Invokes one exact approved memory write.
    async fn invoke_memory_write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<(DependencyMemoryWriteReceipt, u8), PluginDependencyError>;
    /// Invokes one exact pure provider-projection compactor.
    async fn invoke_compaction(
        &self,
        request: DependencyCompactionRequest,
    ) -> Result<(DependencyCompactionProposal, u8), PluginDependencyError>;
    /// Enqueues an observation.
    async fn observe(
        &self,
        request: DependencyObservationRequest,
    ) -> Result<DependencyObservationResult, PluginDependencyError>;
    /// Persists one exact runtime-validated plugin-node state generation.
    async fn persist_node_state(
        &self,
        request: DependencyPersistNodeStateRequest,
    ) -> Result<DependencyPluginNodeStateReceipt, PluginDependencyError>;
    /// Loads one exact bounded state generation.
    async fn load_node_state(
        &self,
        request: DependencyLoadNodeStateRequest,
    ) -> Result<DependencyLoadedNodeState, PluginDependencyError>;
    /// Authenticates and signals one exact invocation identity.
    async fn cancel_invocation(
        &self,
        request: DependencyCancelInvocationRequest,
    ) -> Result<DependencyInvocationCancellationReceipt, PluginDependencyError>;
    /// Disables.
    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Re-enables a disabled plugin.
    async fn enable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Quarantines.
    async fn quarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Releases a quarantined plugin.
    async fn unquarantine(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError>;
    /// Health.
    async fn health(&self) -> DependencyHealth;
    /// Recent audit entries.
    async fn audits(&self) -> Vec<DependencyAudit>;
}

#[derive(Clone)]
struct LoadedPlugin {
    manifest: DependencyManifest,
    configuration: Value,
    configuration_reference: ContentHash,
    status: Arc<RwLock<DependencyPluginStatus>>,
    observer: Option<mpsc::Sender<ObserverWork>>,
    observer_depth: Arc<AtomicU64>,
    observer_active: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

struct ObserverWork {
    invocation_id: String,
    handler: String,
    event_type: String,
    event: Value,
    completion: oneshot::Sender<Result<(), PluginDependencyError>>,
}

fn validate_lifecycle_binding(
    plugin: &LoadedPlugin,
    request: &DependencyStateChangeRequest,
) -> Result<(), PluginDependencyError> {
    if plugin.manifest.version != request.plugin_version
        || plugin.configuration_reference != request.configuration_reference
    {
        return Err(PluginDependencyError::ConfigurationDrift);
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedState {
    version: u32,
    value: Value,
    #[serde(default = "default_lifecycle_state")]
    lifecycle_state: String,
    #[serde(default)]
    lifecycle_receipt: Option<PersistedLifecycleReceipt>,
    #[serde(default)]
    observer_deliveries: BTreeMap<String, PersistedObserverDelivery>,
    #[serde(default)]
    node_states: BTreeMap<String, PersistedNodeState>,
    #[serde(default)]
    node_state_reads: BTreeMap<String, PersistedNodeStateRead>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PersistedObserverDelivery {
    request_hash: ContentHash,
    cancellation_id: String,
    accepted: bool,
    status: Option<DependencyObserverDeliveryStatus>,
    receipt_id: Option<String>,
    receipt_digest: Option<ContentHash>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PersistedLifecycleReceipt {
    action: String,
    plugin_version: String,
    configuration_reference: ContentHash,
    reason: Option<String>,
    cancellation_id: String,
    state: String,
    audit_outcome: String,
}

fn default_lifecycle_state() -> String {
    String::from("active")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedNodeState {
    state: Value,
    prior_state_hash: Option<ContentHash>,
    configuration_reference: ContentHash,
    cancellation_target: DependencyInvocationCancellationTarget,
    nonce: String,
    receipt: DependencyPluginNodeStateReceipt,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedNodeStateRead {
    configuration_reference: ContentHash,
    cancellation_target: DependencyInvocationCancellationTarget,
    nonce: String,
    receipt: DependencyPluginNodeStateReadReceipt,
}

#[derive(Clone)]
struct ActiveInvocation {
    target: DependencyInvocationCancellationTarget,
    cancellation: CancellationToken,
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
    },
    Tool {
        tool: &'a str,
        arguments: &'a Value,
        readable_state: &'a Value,
    },
    NodeExecutor {
        executor_id: &'a str,
        executor_version: &'a str,
        node_kind: &'a str,
        handler: &'a str,
        input: &'a Value,
        readable_state: &'a Value,
    },
    ContextTransform {
        transform_id: &'a str,
        transform_version: &'a str,
        lifecycle: DependencyContextTransformLifecycle,
        handler: &'a str,
        input: &'a Value,
        readable_state: &'a Value,
    },
    MemoryRetrieve {
        binding: &'a DependencyOperationBinding,
        configuration: &'a Value,
        provider_id: &'a str,
        provider_version: &'a str,
        handler: &'a str,
        request: &'a Value,
        readable_state: &'a Value,
    },
    MemoryWrite {
        binding: &'a DependencyOperationBinding,
        configuration: &'a Value,
        provider_id: &'a str,
        provider_version: &'a str,
        handler: &'a str,
        request: &'a Value,
        readable_state: &'a Value,
    },
    Compaction {
        binding: &'a DependencyOperationBinding,
        configuration: &'a Value,
        compactor_id: &'a str,
        compactor_version: &'a str,
        handler: &'a str,
        request: &'a Value,
        readable_state: &'a Value,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
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
    NodeOutcome {
        output: Value,
        preserved_state: Value,
        #[serde(default)]
        proposed_actions: Vec<WorkerNodeActionProposal>,
    },
    ContextTransformProposal {
        replacement: Value,
    },
    MemoryRetrieved {
        binding: DependencyOperationBinding,
        provider_id: String,
        provider_version: String,
        items: Value,
    },
    MemoryWritten {
        binding: DependencyOperationBinding,
        provider_id: String,
        provider_version: String,
        provider_record_id: String,
        value_hash: ContentHash,
        receipt: Value,
    },
    CompactionProposed {
        binding: DependencyOperationBinding,
        compactor_id: String,
        compactor_version: String,
        replacement: Value,
        replacement_hash: ContentHash,
        preserved_references: Value,
        preserved_artifacts: Value,
    },
    Observed,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerNodeActionProposal {
    kind: String,
    payload: Value,
}

/// Isolated implementation.
#[derive(Clone)]
pub struct IsolatedPluginDependency {
    config: Arc<PluginDependencyConfig>,
    key: Arc<AuthorizationKey>,
    plugins: Arc<Mutex<BTreeMap<String, LoadedPlugin>>>,
    invocations: Arc<Mutex<BTreeMap<String, ActiveInvocation>>>,
    cancellation_receipts: Arc<Mutex<BTreeMap<String, DependencyInvocationCancellationReceipt>>>,
    nonces: Arc<Mutex<BTreeMap<String, i64>>>,
    rates: Arc<Mutex<BTreeMap<String, VecDeque<Instant>>>>,
    audits: Arc<Mutex<VecDeque<DependencyAudit>>>,
    state_writes: Arc<Mutex<()>>,
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
        let cancellation_receipts = load_json::<
            BTreeMap<String, DependencyInvocationCancellationReceipt>,
        >(&config.state_root.join("cancellation-receipts.json"))
        .await?
        .unwrap_or_default();
        if cancellation_receipts.len() > MAX_CANCELLATION_RECEIPTS {
            return Err(PluginDependencyError::StateCorrupt);
        }
        Ok(Self {
            config: Arc::new(config),
            key: Arc::new(key),
            plugins: Arc::new(Mutex::new(BTreeMap::new())),
            invocations: Arc::new(Mutex::new(BTreeMap::new())),
            cancellation_receipts: Arc::new(Mutex::new(cancellation_receipts)),
            nonces: Arc::new(Mutex::new(nonces)),
            rates: Arc::new(Mutex::new(BTreeMap::new())),
            audits: Arc::new(Mutex::new(VecDeque::new())),
            state_writes: Arc::new(Mutex::new(())),
        })
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

    async fn authorize_cancellation(
        &self,
        request: &DependencyCancelInvocationRequest,
        allow_exact_nonce_reconciliation: bool,
    ) -> Result<(), PluginDependencyError> {
        if request.target.session_id != self.config.session_id
            || request.authorization.owner_id != self.config.owner_id
            || request.authorization.session_id != request.target.session_id
            || request.authorization.normalized_digest != request.action_digest.to_hex()
            || cancellation_action_digest(
                &request.target,
                &request.reason_code,
                &request.nonce,
                &request.idempotency_key,
                &request.authorization.cancellation_id,
            )? != request.action_digest
        {
            return Err(PluginDependencyError::Authorization);
        }
        let now = now_millis()?;
        let claims = verify_authorization(
            &request.authorization.grant,
            &self.key,
            ExpectedAuthorization {
                owner: &self.config.owner_id,
                session: &self.config.session_id,
                call_id: &request.authorization.call_id,
                action: "plugin.invocation.cancel",
                normalized_digest: request.action_digest,
            },
            TimestampMillis::new(now),
        )
        .map_err(|_| PluginDependencyError::Authorization)?;
        if claims.nonce != request.nonce {
            return Err(PluginDependencyError::Authorization);
        }
        let mut nonces = self.nonces.lock().await;
        nonces.retain(|_, expiry| *expiry >= now);
        let nonce_key = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        if nonces.contains_key(&nonce_key) {
            if allow_exact_nonce_reconciliation {
                return Ok(());
            }
            return Err(PluginDependencyError::Replay);
        }
        nonces.insert(nonce_key, claims.expires_at.get());
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

    async fn audit_operation(
        &self,
        binding: &DependencyOperationBinding,
        operation: &str,
        outcome: &str,
        attempts: u8,
    ) {
        self.audit(DependencyAudit {
            plugin_id: binding.plugin_id.clone(),
            invocation_id: Some(binding.invocation_id.clone()),
            operation: operation.to_owned(),
            outcome: outcome.to_owned(),
            attempts,
        })
        .await;
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

    async fn invoke_worker(
        &self,
        plugin: &LoadedPlugin,
        cancellation_target: Option<DependencyInvocationCancellationTarget>,
        request: &WorkerRequest<'_>,
    ) -> Result<(WorkerResponse, u8), PluginDependencyError> {
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        self.enforce_rate(&plugin.manifest.id).await?;
        let cancellation = CancellationToken::new();
        let invocation_id = cancellation_target
            .as_ref()
            .map(|target| target.invocation_id.clone());
        if let Some(target) = cancellation_target {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(
                    target.invocation_id.clone(),
                    ActiveInvocation {
                        target,
                        cancellation: cancellation.clone(),
                    },
                )
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            cancellation.cancel();
            if let Some(invocation_id) = &invocation_id {
                self.invocations.lock().await.remove(invocation_id);
            }
            return Err(PluginDependencyError::Inactive);
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
        if let Some(invocation_id) = invocation_id {
            self.invocations.lock().await.remove(&invocation_id);
        }
        result.map(|response| (response, attempt))
    }

    async fn invoke_worker_once(
        &self,
        plugin: &LoadedPlugin,
        cancellation_target: Option<DependencyInvocationCancellationTarget>,
        request: &WorkerRequest<'_>,
    ) -> Result<(WorkerResponse, u8), PluginDependencyError> {
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        self.enforce_rate(&plugin.manifest.id).await?;
        let cancellation = CancellationToken::new();
        let invocation_id = cancellation_target
            .as_ref()
            .map(|target| target.invocation_id.clone());
        if let Some(target) = cancellation_target {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(
                    target.invocation_id.clone(),
                    ActiveInvocation {
                        target,
                        cancellation: cancellation.clone(),
                    },
                )
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            cancellation.cancel();
            if let Some(invocation_id) = &invocation_id {
                self.invocations.lock().await.remove(invocation_id);
            }
            return Err(PluginDependencyError::Inactive);
        }
        let result = run_once(
            &plugin.manifest,
            request,
            cancellation,
            self.config.max_response_bytes,
        )
        .await;
        if let Some(invocation_id) = invocation_id {
            self.invocations.lock().await.remove(&invocation_id);
        }
        result.map(|response| (response, 1))
    }

    async fn persist_lifecycle_receipt(
        &self,
        request: &DependencyStateChangeRequest,
        action: &str,
        state: &str,
        audit_outcome: &str,
    ) -> Result<bool, PluginDependencyError> {
        let _guard = self.state_writes.lock().await;
        let path = state_path(&self.config.state_root, &request.plugin_id)?;
        let mut persisted = load_json::<PersistedState>(&path)
            .await?
            .ok_or(PluginDependencyError::StateCorrupt)?;
        let receipt = PersistedLifecycleReceipt {
            action: action.to_owned(),
            plugin_version: request.plugin_version.clone(),
            configuration_reference: request.configuration_reference,
            reason: request.reason.clone(),
            cancellation_id: request.authorization.cancellation_id.clone(),
            state: state.to_owned(),
            audit_outcome: audit_outcome.to_owned(),
        };
        if persisted.lifecycle_receipt.as_ref() == Some(&receipt) {
            return Ok(true);
        }
        let valid_transition = matches!(
            (persisted.lifecycle_state.as_str(), action),
            ("active", "disable" | "quarantine")
                | ("disabled", "enable")
                | ("quarantined", "unquarantine")
        );
        if !valid_transition {
            return Err(PluginDependencyError::Invalid);
        }
        state.clone_into(&mut persisted.lifecycle_state);
        persisted.lifecycle_receipt = Some(receipt);
        persist_json(&path, &persisted).await?;
        Ok(false)
    }

    async fn begin_observer_delivery(
        &self,
        request: &DependencyObservationRequest,
        request_hash: ContentHash,
    ) -> Result<Option<PersistedObserverDelivery>, PluginDependencyError> {
        let _guard = self.state_writes.lock().await;
        let path = state_path(&self.config.state_root, &request.plugin_id)?;
        let mut persisted = load_json::<PersistedState>(&path)
            .await?
            .ok_or(PluginDependencyError::StateCorrupt)?;
        if let Some(existing) = persisted
            .observer_deliveries
            .get_mut(&request.invocation_id)
        {
            if existing.request_hash != request_hash
                || existing.cancellation_id != request.authorization.cancellation_id
            {
                return Err(PluginDependencyError::StateConflict);
            }
            if existing.status.is_none() {
                let status = DependencyObserverDeliveryStatus::Ambiguous;
                let receipt_id = observer_delivery_receipt_id(
                    &request.plugin_id,
                    &request.invocation_id,
                    request_hash,
                )?;
                let receipt_digest = observer_delivery_receipt_digest(
                    &request.plugin_id,
                    &request.invocation_id,
                    request_hash,
                    status,
                    &receipt_id,
                )?;
                existing.status = Some(status);
                existing.receipt_id = Some(receipt_id);
                existing.receipt_digest = Some(receipt_digest);
                let terminal = existing.clone();
                persist_json(&path, &persisted).await?;
                return Ok(Some(terminal));
            }
            return Ok(Some(existing.clone()));
        }
        persisted.observer_deliveries.insert(
            request.invocation_id.clone(),
            PersistedObserverDelivery {
                request_hash,
                cancellation_id: request.authorization.cancellation_id.clone(),
                accepted: false,
                status: None,
                receipt_id: None,
                receipt_digest: None,
            },
        );
        persist_json(&path, &persisted).await?;
        Ok(None)
    }

    async fn finish_observer_delivery(
        &self,
        request: &DependencyObservationRequest,
        request_hash: ContentHash,
        accepted: bool,
        status: DependencyObserverDeliveryStatus,
    ) -> Result<PersistedObserverDelivery, PluginDependencyError> {
        let _guard = self.state_writes.lock().await;
        let path = state_path(&self.config.state_root, &request.plugin_id)?;
        let mut persisted = load_json::<PersistedState>(&path)
            .await?
            .ok_or(PluginDependencyError::StateCorrupt)?;
        let existing = persisted
            .observer_deliveries
            .get_mut(&request.invocation_id)
            .ok_or(PluginDependencyError::StateCorrupt)?;
        if existing.request_hash != request_hash
            || existing.cancellation_id != request.authorization.cancellation_id
            || existing.status.is_some()
        {
            return Err(PluginDependencyError::StateConflict);
        }
        let receipt_id =
            observer_delivery_receipt_id(&request.plugin_id, &request.invocation_id, request_hash)?;
        let receipt_digest = observer_delivery_receipt_digest(
            &request.plugin_id,
            &request.invocation_id,
            request_hash,
            status,
            &receipt_id,
        )?;
        existing.accepted = accepted;
        existing.status = Some(status);
        existing.receipt_id = Some(receipt_id);
        existing.receipt_digest = Some(receipt_digest);
        let completed = existing.clone();
        persist_json(&path, &persisted).await?;
        Ok(completed)
    }

    async fn durable_state_flushed(&self, plugin_ids: &[String]) -> bool {
        let _guard = self.state_writes.lock().await;
        for plugin_id in plugin_ids {
            let Ok(path) = state_path(&self.config.state_root, plugin_id) else {
                return false;
            };
            let Ok(Some(state)) = load_json::<PersistedState>(&path).await else {
                return false;
            };
            if state
                .observer_deliveries
                .values()
                .any(|delivery| delivery.status.is_none())
            {
                return false;
            }
        }
        true
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
        let configuration_reference = configuration_reference(&request.configuration)?;
        if let Some(loaded) = self.plugins.lock().await.get(&request.manifest.id).cloned() {
            if loaded.manifest != request.manifest
                || loaded.configuration_reference != configuration_reference
                || loaded.configuration != request.configuration
            {
                return Err(PluginDependencyError::ConfigurationDrift);
            }
            let state = load_json::<PersistedState>(&state_path(
                &self.config.state_root,
                &request.manifest.id,
            )?)
            .await?
            .ok_or(PluginDependencyError::StateCorrupt)?;
            self.audit(DependencyAudit {
                plugin_id: request.manifest.id.clone(),
                invocation_id: None,
                operation: String::from("load"),
                outcome: String::from("already_loaded"),
                attempts: 0,
            })
            .await;
            return Ok(DependencyLoadResult {
                plugin_id: request.manifest.id,
                state_version: state.version,
                attempts: 0,
            });
        }
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
                    configuration_reference,
                    status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                    observer: None,
                    observer_depth: Arc::new(AtomicU64::new(0)),
                    observer_active: Arc::new(AtomicU64::new(0)),
                    dropped: Arc::new(AtomicU64::new(0)),
                };
                let worker_request = WorkerRequest::Migrate {
                    from: existing.version,
                    to: request.manifest.state_migration_version,
                    state: &existing.value,
                };
                let (response, used) = self
                    .invoke_worker(&temporary, None, &worker_request)
                    .await?;
                attempts = used;
                match response {
                    WorkerResponse::State { state } => PersistedState {
                        version: request.manifest.state_migration_version,
                        value: state,
                        lifecycle_state: existing.lifecycle_state,
                        lifecycle_receipt: existing.lifecycle_receipt,
                        observer_deliveries: existing.observer_deliveries,
                        node_states: existing.node_states,
                        node_state_reads: existing.node_state_reads,
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
                configuration_reference,
                status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                observer: None,
                observer_depth: Arc::new(AtomicU64::new(0)),
                observer_active: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
            };
            let worker_request = WorkerRequest::Initialize {
                configuration: &request.configuration,
                state_version: request.manifest.state_migration_version,
            };
            let (response, used) = self
                .invoke_worker(&temporary, None, &worker_request)
                .await?;
            attempts = used;
            if !matches!(response, WorkerResponse::Ready) {
                return Err(PluginDependencyError::MalformedResponse);
            }
            PersistedState {
                version: request.manifest.state_migration_version,
                value: Value::Object(serde_json::Map::new()),
                lifecycle_state: default_lifecycle_state(),
                lifecycle_receipt: None,
                observer_deliveries: BTreeMap::new(),
                node_states: BTreeMap::new(),
                node_state_reads: BTreeMap::new(),
            }
        };
        persist_json(&state_path, &state).await?;
        let lifecycle_status = match state.lifecycle_state.as_str() {
            "active" => DependencyPluginStatus::Active,
            "disabled" => DependencyPluginStatus::Disabled,
            "quarantined" => DependencyPluginStatus::Quarantined,
            _ => return Err(PluginDependencyError::StateCorrupt),
        };
        let status = Arc::new(RwLock::new(lifecycle_status));
        let depth = Arc::new(AtomicU64::new(0));
        let active = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let observer = if request.manifest.class == DependencyPluginClass::Observer {
            let (sender, receiver) = mpsc::channel(self.config.observer_queue_capacity);
            tokio::spawn(observer_worker(
                request.manifest.clone(),
                receiver,
                Arc::clone(&depth),
                Arc::clone(&active),
                self.config.max_response_bytes,
            ));
            Some(sender)
        } else {
            None
        };
        self.plugins.lock().await.insert(
            request.manifest.id.clone(),
            LoadedPlugin {
                manifest: request.manifest.clone(),
                configuration: request.configuration,
                configuration_reference,
                status,
                observer,
                observer_depth: depth,
                observer_active: active,
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
        })
    }

    async fn invoke(
        &self,
        request: DependencyInvocationRequest,
    ) -> Result<(DependencyDecision, u8), PluginDependencyError> {
        let cancellation_target = match request.operation.as_str() {
            "intercept" | "node_executor" => Some(
                request
                    .cancellation_target
                    .clone()
                    .ok_or(PluginDependencyError::CancellationTargetMismatch)?,
            ),
            "tool" if request.cancellation_target.is_none() => None,
            _ => return Err(PluginDependencyError::CancellationTargetMismatch),
        };
        if request.operation == "node_executor" {
            self.authorize(
                "plugin.node_executor.invoke",
                &(
                    cancellation_target
                        .as_ref()
                        .ok_or(PluginDependencyError::CancellationTargetMismatch)?,
                    &request.plugin_id,
                    &request.invocation_id,
                    request.executor_id.as_deref(),
                    request.executor_version.as_deref(),
                    request.timeout_ms,
                    request.configuration_reference,
                    &request.kind,
                    &request.handler,
                    &request.payload,
                    &request.readable_state,
                ),
                &request.authorization,
            )
            .await?;
        } else if request.operation == "intercept" {
            self.authorize(
                &format!("plugin.{}", request.operation),
                &(
                    cancellation_target
                        .as_ref()
                        .ok_or(PluginDependencyError::CancellationTargetMismatch)?,
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
        } else {
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
        }
        let plugin = self.entry(&request.plugin_id).await?;
        let mut execution_plugin = plugin.clone();
        let mut ambiguous_on_cancel = false;
        let worker_request = if request.operation == "intercept" {
            WorkerRequest::Intercept {
                handler: &request.handler,
                proposal_type: &request.kind,
                proposal: &request.payload,
                readable_state: &request.readable_state,
            }
        } else if request.operation == "tool" {
            WorkerRequest::Tool {
                tool: &request.handler,
                arguments: &request.payload,
                readable_state: &request.readable_state,
            }
        } else if request.operation == "node_executor" {
            let executor_id = request
                .executor_id
                .as_deref()
                .ok_or(PluginDependencyError::Invalid)?;
            let executor_version = request
                .executor_version
                .as_deref()
                .ok_or(PluginDependencyError::Invalid)?;
            let declaration = plugin
                .manifest
                .node_executors
                .iter()
                .find(|declaration| {
                    declaration.executor_id == executor_id
                        && declaration.version == executor_version
                        && declaration.node_kind == request.kind
                        && declaration.handler == request.handler
                })
                .ok_or(PluginDependencyError::Invalid)?;
            execution_plugin.manifest.timeout_ms = declaration.timeout_ms;
            execution_plugin.manifest.max_attempts = if declaration.idempotency
                == DependencyNodeExecutorIdempotency::Idempotent
                && declaration.failure_policy == "retry"
            {
                declaration.max_attempts
            } else {
                1
            };
            execution_plugin.manifest.retry_backoff_ms = declaration.retry_backoff_ms;
            ambiguous_on_cancel = declaration.external_effects
                || declaration.idempotency == DependencyNodeExecutorIdempotency::NonIdempotent;
            WorkerRequest::NodeExecutor {
                executor_id,
                executor_version,
                node_kind: &request.kind,
                handler: &request.handler,
                input: &request.payload,
                readable_state: &request.readable_state,
            }
        } else {
            return Err(PluginDependencyError::Invalid);
        };
        let registered_target = if request.operation == "tool" {
            None
        } else {
            let declaration_hash = if request.operation == "node_executor" {
                let executor_id = request
                    .executor_id
                    .as_deref()
                    .ok_or(PluginDependencyError::Invalid)?;
                let executor_version = request
                    .executor_version
                    .as_deref()
                    .ok_or(PluginDependencyError::Invalid)?;
                let declaration = plugin
                    .manifest
                    .node_executors
                    .iter()
                    .find(|candidate| {
                        candidate.executor_id == executor_id
                            && candidate.version == executor_version
                            && candidate.node_kind == request.kind
                            && candidate.handler == request.handler
                    })
                    .ok_or(PluginDependencyError::Invalid)?;
                Some(ContentHash::digest(
                    &serde_json::to_vec(&to_sdk_node_executor(declaration)?)
                        .map_err(|_| PluginDependencyError::Invalid)?,
                ))
            } else {
                None
            };
            let cancellation_target =
                cancellation_target.ok_or(PluginDependencyError::CancellationTargetMismatch)?;
            let request_hash = if request.operation == "node_executor" {
                plugin_node_executor_request_hash(
                    &request.plugin_id,
                    &request.invocation_id,
                    request
                        .executor_id
                        .as_deref()
                        .ok_or(PluginDependencyError::Invalid)?,
                    request
                        .executor_version
                        .as_deref()
                        .ok_or(PluginDependencyError::Invalid)?,
                    &request.kind,
                    &request.handler,
                    request.timeout_ms.ok_or(PluginDependencyError::Invalid)?,
                    request
                        .configuration_reference
                        .ok_or(PluginDependencyError::Invalid)?,
                    &request.payload,
                    &request.readable_state,
                )?
            } else {
                plugin_interceptor_request_hash(
                    &request.plugin_id,
                    &request.invocation_id,
                    &request.handler,
                    &request.kind,
                    &request.payload,
                    &request.readable_state,
                )?
            };
            if request.operation == "node_executor"
                && (request.configuration_reference != Some(plugin.configuration_reference)
                    || request.timeout_ms != Some(execution_plugin.manifest.timeout_ms))
            {
                return Err(PluginDependencyError::Invalid);
            }
            validate_invocation_target(
                &cancellation_target,
                &plugin,
                &request.invocation_id,
                if request.operation == "node_executor" {
                    request
                        .executor_id
                        .as_deref()
                        .ok_or(PluginDependencyError::Invalid)?
                } else {
                    &request.handler
                },
                request_hash,
                declaration_hash,
                &request.authorization,
            )?;
            Some(cancellation_target)
        };
        let (response, attempts) = self
            .invoke_worker(&execution_plugin, registered_target, &worker_request)
            .await
            .map_err(|error| {
                if ambiguous_on_cancel && error == PluginDependencyError::Cancelled {
                    PluginDependencyError::Ambiguous
                } else {
                    error
                }
            })?;
        let decision = match response {
            WorkerResponse::Continue { proposal } => DependencyDecision::Continue(proposal),
            WorkerResponse::Replace { proposal } => DependencyDecision::Replace(proposal),
            WorkerResponse::Reject { reason } => DependencyDecision::Reject(reason),
            WorkerResponse::ToolResult { value } => DependencyDecision::ToolResult(value),
            WorkerResponse::NodeOutcome {
                output,
                preserved_state,
                proposed_actions,
            } => DependencyDecision::NodeOutcome(DependencyNodeOutcomeProposal {
                output,
                preserved_state,
                proposed_actions: proposed_actions
                    .into_iter()
                    .map(|action| DependencyNodeActionProposal {
                        kind: action.kind,
                        payload: action.payload,
                    })
                    .collect(),
            }),
            _ => return Err(PluginDependencyError::MalformedResponse),
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: request.operation,
            outcome: "completed".to_owned(),
            attempts,
        })
        .await;
        Ok((decision, attempts))
    }

    async fn invoke_context_transform(
        &self,
        request: DependencyContextTransformRequest,
    ) -> Result<(DependencyContextTransformProposal, u8), PluginDependencyError> {
        self.authorize(
            "plugin.context_transform.invoke",
            &(
                &request.cancellation_target,
                &request.plugin_id,
                &request.invocation_id,
                &request.transform_id,
                &request.transform_version,
                request.timeout_ms,
                request.configuration_reference,
                request.lifecycle,
                &request.handler,
                &request.input,
                &request.readable_state,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        let declaration = plugin
            .manifest
            .context_transforms
            .iter()
            .find(|declaration| {
                declaration.transform_id == request.transform_id
                    && declaration.version == request.transform_version
                    && declaration.lifecycle == request.lifecycle
                    && declaration.handler == request.handler
            })
            .ok_or(PluginDependencyError::Invalid)?;
        if declaration.idempotency != DependencyContextTransformIdempotency::Idempotent
            || declaration.external_effects
            || request.timeout_ms != declaration.timeout_ms
            || request.configuration_reference != plugin.configuration_reference
        {
            return Err(PluginDependencyError::Invalid);
        }
        let declaration_hash = ContentHash::digest(
            &serde_json::to_vec(&to_sdk_context_transform(declaration)?)
                .map_err(|_| PluginDependencyError::Invalid)?,
        );
        validate_invocation_target(
            &request.cancellation_target,
            &plugin,
            &request.invocation_id,
            &request.transform_id,
            plugin_context_transform_request_hash(
                &request.plugin_id,
                &request.invocation_id,
                &request.transform_id,
                &request.transform_version,
                request.lifecycle,
                &request.handler,
                request.timeout_ms,
                request.configuration_reference,
                &request.input,
                &request.readable_state,
            )?,
            Some(declaration_hash),
            &request.authorization,
        )?;
        let mut execution_plugin = plugin.clone();
        execution_plugin.manifest.timeout_ms = declaration.timeout_ms;
        execution_plugin.manifest.max_attempts = if declaration.failure_policy == "retry" {
            declaration.max_attempts
        } else {
            1
        };
        execution_plugin.manifest.retry_backoff_ms = declaration.retry_backoff_ms;
        let worker_request = WorkerRequest::ContextTransform {
            transform_id: &request.transform_id,
            transform_version: &request.transform_version,
            lifecycle: request.lifecycle,
            handler: &request.handler,
            input: &request.input,
            readable_state: &request.readable_state,
        };
        let (response, attempts) = self
            .invoke_worker(
                &execution_plugin,
                Some(request.cancellation_target.clone()),
                &worker_request,
            )
            .await?;
        let WorkerResponse::ContextTransformProposal { replacement } = response else {
            return Err(PluginDependencyError::MalformedResponse);
        };
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: String::from("context_transform"),
            outcome: String::from("completed"),
            attempts,
        })
        .await;
        Ok((DependencyContextTransformProposal { replacement }, attempts))
    }

    async fn invoke_memory_retrieve(
        &self,
        request: DependencyMemoryRetrieveRequest,
    ) -> Result<(DependencyMemoryRetrieveProposal, u8), PluginDependencyError> {
        let prepared = async {
            if request.binding.request_hash != memory_retrieve_request_hash(&request)? {
                return Err(PluginDependencyError::Invalid);
            }
            authorize_memory_operation(
                self,
                "plugin.memory.retrieve.invoke",
                &request.binding,
                &request.provider_id,
                &request.provider_version,
                &request.handler,
                request.timeout_ms,
                request.idempotency,
                &request.request,
                &request.readable_state,
                &request.authorization,
            )
            .await?;
            if request.idempotency != DependencyOperationIdempotency::Idempotent {
                return Err(PluginDependencyError::Invalid);
            }
            let plugin = self.entry(&request.binding.plugin_id).await?;
            validate_plugin_binding(&plugin, &request.binding)?;
            let provider = plugin
                .manifest
                .memory_providers
                .iter()
                .find(|provider| {
                    provider.provider_id == request.provider_id
                        && provider.version == request.provider_version
                        && provider.retrieve.handler == request.handler
                })
                .ok_or(PluginDependencyError::Invalid)?;
            validate_provider_declaration_hash(provider, request.binding.declaration_hash)?;
            if provider.retrieve.idempotency != request.idempotency
                || provider.retrieve.external_effects
                || provider.retrieve.timeout_ms != request.timeout_ms
            {
                return Err(PluginDependencyError::Invalid);
            }
            let declaration = provider.retrieve.clone();
            Ok::<_, PluginDependencyError>((plugin, declaration))
        }
        .await;
        let (plugin, declaration) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.audit_operation(&request.binding, "memory_retrieve", "rejected", 0)
                    .await;
                return Err(error);
            }
        };
        let mut execution_plugin = plugin.clone();
        apply_operation_execution(&mut execution_plugin, &declaration);
        let cancellation_target = operation_cancellation_target(&request.binding)?;
        let worker = self
            .invoke_worker_once(
                &execution_plugin,
                Some(cancellation_target),
                &WorkerRequest::MemoryRetrieve {
                    binding: &request.binding,
                    configuration: &plugin.configuration,
                    provider_id: &request.provider_id,
                    provider_version: &request.provider_version,
                    handler: &request.handler,
                    request: &request.request,
                    readable_state: &request.readable_state,
                },
            )
            .await;
        let (response, attempts) = match worker {
            Ok(result) => result,
            Err(error) => {
                let attempts = worker_error_attempts(&error);
                self.audit_operation(
                    &request.binding,
                    "memory_retrieve",
                    worker_error_outcome(&error),
                    attempts,
                )
                .await;
                return Err(error);
            }
        };
        let WorkerResponse::MemoryRetrieved {
            binding,
            provider_id,
            provider_version,
            items,
        } = response
        else {
            self.audit_operation(
                &request.binding,
                "memory_retrieve",
                "malformed_result",
                attempts,
            )
            .await;
            return Err(PluginDependencyError::MalformedResponse);
        };
        if binding != request.binding
            || provider_id != request.provider_id
            || provider_version != request.provider_version
        {
            self.audit_operation(
                &request.binding,
                "memory_retrieve",
                "malformed_result",
                attempts,
            )
            .await;
            return Err(PluginDependencyError::MalformedResponse);
        }
        self.audit_operation(&request.binding, "memory_retrieve", "completed", attempts)
            .await;
        Ok((
            DependencyMemoryRetrieveProposal {
                binding,
                provider_id,
                provider_version,
                items,
            },
            attempts,
        ))
    }

    async fn invoke_memory_write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<(DependencyMemoryWriteReceipt, u8), PluginDependencyError> {
        let prepared = async {
            if request.binding.request_hash != memory_write_request_hash(&request)? {
                return Err(PluginDependencyError::Invalid);
            }
            authorize_memory_operation(
                self,
                "plugin.memory.write.invoke",
                &request.binding,
                &request.provider_id,
                &request.provider_version,
                &request.handler,
                request.timeout_ms,
                request.idempotency,
                &request.request,
                &request.readable_state,
                &request.authorization,
            )
            .await?;
            if request.idempotency == DependencyOperationIdempotency::NonIdempotent
                && request.binding.attempt != 1
            {
                return Err(PluginDependencyError::Invalid);
            }
            let plugin = self.entry(&request.binding.plugin_id).await?;
            validate_plugin_binding(&plugin, &request.binding)?;
            let provider = plugin
                .manifest
                .memory_providers
                .iter()
                .find(|provider| {
                    provider.provider_id == request.provider_id
                        && provider.version == request.provider_version
                        && provider
                            .write
                            .as_ref()
                            .is_some_and(|write| write.handler == request.handler)
                })
                .ok_or(PluginDependencyError::Invalid)?;
            validate_provider_declaration_hash(provider, request.binding.declaration_hash)?;
            let write = provider
                .write
                .as_ref()
                .ok_or(PluginDependencyError::Invalid)?;
            if write.idempotency != request.idempotency || write.timeout_ms != request.timeout_ms {
                return Err(PluginDependencyError::Invalid);
            }
            let declaration = write.clone();
            Ok::<_, PluginDependencyError>((plugin, declaration))
        }
        .await;
        let (plugin, declaration) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.audit_operation(&request.binding, "memory_write", "rejected", 0)
                    .await;
                return Err(error);
            }
        };
        let mut execution_plugin = plugin.clone();
        apply_operation_execution(&mut execution_plugin, &declaration);
        let cancellation_target = operation_cancellation_target(&request.binding)?;
        let worker = self
            .invoke_worker_once(
                &execution_plugin,
                Some(cancellation_target),
                &WorkerRequest::MemoryWrite {
                    binding: &request.binding,
                    configuration: &plugin.configuration,
                    provider_id: &request.provider_id,
                    provider_version: &request.provider_version,
                    handler: &request.handler,
                    request: &request.request,
                    readable_state: &request.readable_state,
                },
            )
            .await;
        let (response, attempts) = match worker {
            Ok(result) => result,
            Err(error) => {
                let attempts = worker_error_attempts(&error);
                let dispatched = attempts == 1;
                self.audit_operation(
                    &request.binding,
                    "memory_write",
                    if dispatched {
                        "ambiguous_write"
                    } else {
                        "rejected"
                    },
                    attempts,
                )
                .await;
                return Err(if dispatched {
                    PluginDependencyError::Ambiguous
                } else {
                    error
                });
            }
        };
        let WorkerResponse::MemoryWritten {
            binding,
            provider_id,
            provider_version,
            provider_record_id,
            value_hash,
            receipt,
        } = response
        else {
            self.audit_operation(
                &request.binding,
                "memory_write",
                "ambiguous_write",
                attempts,
            )
            .await;
            return Err(PluginDependencyError::Ambiguous);
        };
        let terminal_receipt = DependencyMemoryWriteReceipt {
            binding,
            provider_id,
            provider_version,
            provider_record_id,
            value_hash,
            receipt,
        };
        if validate_memory_write_receipt(&request, &declaration, &terminal_receipt).is_err() {
            self.audit_operation(
                &request.binding,
                "memory_write",
                "ambiguous_write",
                attempts,
            )
            .await;
            return Err(PluginDependencyError::Ambiguous);
        }
        self.audit_operation(&request.binding, "memory_write", "completed", attempts)
            .await;
        Ok((terminal_receipt, attempts))
    }

    async fn invoke_compaction(
        &self,
        request: DependencyCompactionRequest,
    ) -> Result<(DependencyCompactionProposal, u8), PluginDependencyError> {
        let prepared = async {
            if request.binding.request_hash != compaction_request_hash(&request)? {
                return Err(PluginDependencyError::Invalid);
            }
            authorize_memory_operation(
                self,
                "plugin.compaction.invoke",
                &request.binding,
                &request.compactor_id,
                &request.compactor_version,
                &request.handler,
                request.timeout_ms,
                request.idempotency,
                &request.request,
                &request.readable_state,
                &request.authorization,
            )
            .await?;
            if request.idempotency != DependencyOperationIdempotency::Idempotent {
                return Err(PluginDependencyError::Invalid);
            }
            let plugin = self.entry(&request.binding.plugin_id).await?;
            validate_plugin_binding(&plugin, &request.binding)?;
            let compactor = plugin
                .manifest
                .compactors
                .iter()
                .find(|compactor| {
                    compactor.compactor_id == request.compactor_id
                        && compactor.version == request.compactor_version
                        && compactor.handler == request.handler
                })
                .ok_or(PluginDependencyError::Invalid)?;
            validate_compactor_declaration_hash(compactor, request.binding.declaration_hash)?;
            if compactor.idempotency != request.idempotency
                || compactor.external_effects
                || compactor.timeout_ms != request.timeout_ms
            {
                return Err(PluginDependencyError::Invalid);
            }
            let declaration = compactor.clone();
            Ok::<_, PluginDependencyError>((plugin, declaration))
        }
        .await;
        let (plugin, declaration) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.audit_operation(&request.binding, "compaction", "rejected", 0)
                    .await;
                return Err(error);
            }
        };
        let mut execution_plugin = plugin.clone();
        apply_compactor_execution(&mut execution_plugin, &declaration);
        let cancellation_target = operation_cancellation_target(&request.binding)?;
        let worker = self
            .invoke_worker_once(
                &execution_plugin,
                Some(cancellation_target),
                &WorkerRequest::Compaction {
                    binding: &request.binding,
                    configuration: &plugin.configuration,
                    compactor_id: &request.compactor_id,
                    compactor_version: &request.compactor_version,
                    handler: &request.handler,
                    request: &request.request,
                    readable_state: &request.readable_state,
                },
            )
            .await;
        let (response, attempts) = match worker {
            Ok(result) => result,
            Err(error) => {
                let attempts = worker_error_attempts(&error);
                self.audit_operation(
                    &request.binding,
                    "compaction",
                    worker_error_outcome(&error),
                    attempts,
                )
                .await;
                return Err(error);
            }
        };
        let WorkerResponse::CompactionProposed {
            binding,
            compactor_id,
            compactor_version,
            replacement,
            replacement_hash,
            preserved_references,
            preserved_artifacts,
        } = response
        else {
            self.audit_operation(&request.binding, "compaction", "malformed_result", attempts)
                .await;
            return Err(PluginDependencyError::MalformedResponse);
        };
        if binding != request.binding
            || compactor_id != request.compactor_id
            || compactor_version != request.compactor_version
        {
            self.audit_operation(&request.binding, "compaction", "malformed_result", attempts)
                .await;
            return Err(PluginDependencyError::MalformedResponse);
        }
        self.audit_operation(&request.binding, "compaction", "completed", attempts)
            .await;
        Ok((
            DependencyCompactionProposal {
                binding,
                compactor_id,
                compactor_version,
                replacement,
                replacement_hash,
                preserved_references,
                preserved_artifacts,
            },
            attempts,
        ))
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
        let request_hash = observer_delivery_request_hash(&request)?;
        if let Some(existing) = self.begin_observer_delivery(&request, request_hash).await? {
            return persisted_observer_result(&plugin, existing, true);
        }
        let (completion, completed) = oneshot::channel();
        let work = ObserverWork {
            invocation_id: request.invocation_id.clone(),
            handler: request.handler.clone(),
            event_type: request.event_type.clone(),
            event: request.event.clone(),
            completion,
        };
        let accepted = sender.try_send(work).is_ok();
        if accepted {
            plugin.observer_depth.fetch_add(1, Ordering::AcqRel);
        } else {
            plugin.dropped.fetch_add(1, Ordering::AcqRel);
        }
        let status = if accepted {
            match timeout(
                Duration::from_millis(plugin.manifest.timeout_ms.max(1)),
                completed,
            )
            .await
            {
                Ok(Ok(Ok(()))) => DependencyObserverDeliveryStatus::Completed,
                Ok(Ok(Err(error))) if observer_failure_is_definite(&error) => {
                    DependencyObserverDeliveryStatus::Failed
                }
                Ok(Ok(Err(_)) | Err(_)) | Err(_) => DependencyObserverDeliveryStatus::Ambiguous,
            }
        } else {
            DependencyObserverDeliveryStatus::Rejected
        };
        let persisted = self
            .finish_observer_delivery(&request, request_hash, accepted, status)
            .await?;
        self.audit(DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: Some(request.invocation_id),
            operation: String::from("observe"),
            outcome: observer_delivery_status_name(status).to_owned(),
            attempts: u8::from(accepted),
        })
        .await;
        persisted_observer_result(&plugin, persisted, false)
    }

    async fn persist_node_state(
        &self,
        request: DependencyPersistNodeStateRequest,
    ) -> Result<DependencyPluginNodeStateReceipt, PluginDependencyError> {
        validate_node_state_request(&request, self.config.max_response_bytes).map_err(|error| {
            eprintln!("plugin state rejected by request validation: {error:?}");
            error
        })?;
        let expected_authorization_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                &request.cancellation_target,
                request.action_digest,
                &request.nonce,
                &request.authorization.cancellation_id,
                &request.idempotency_key,
            ))
            .map_err(|_| PluginDependencyError::Invalid)?,
        );
        if request.authorization_digest != expected_authorization_digest {
            return Err(PluginDependencyError::Authorization);
        }
        self.authorize(
            "plugin.node_executor.persist_state",
            &(
                &request.cancellation_target,
                request.action_digest,
                &request.nonce,
                &request.authorization.cancellation_id,
                &request.idempotency_key,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        let declaration = plugin
            .manifest
            .node_executors
            .iter()
            .find(|candidate| {
                candidate.executor_id == request.executor_id
                    && candidate.version == request.executor_version
                    && candidate.state_scope == node_state_scope_name(request.state_scope)
            })
            .ok_or(PluginDependencyError::Invalid)?;
        let declaration_hash = ContentHash::digest(
            &serde_json::to_vec(&to_sdk_node_executor(declaration)?)
                .map_err(|_| PluginDependencyError::Invalid)?,
        );
        if declaration_hash != request.executor_declaration_hash {
            return Err(PluginDependencyError::Invalid);
        }
        if request.configuration_reference != plugin.configuration_reference {
            return Err(PluginDependencyError::Invalid);
        }
        validate_invocation_target(
            &request.cancellation_target,
            &plugin,
            &request.invocation_id,
            &format!("{}:state-write", request.executor_id),
            plugin_node_state_persist_request_hash(&request)?,
            Some(declaration_hash),
            &request.authorization,
        )?;
        let cancellation = CancellationToken::new();
        {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(
                    request.cancellation_target.invocation_id.clone(),
                    ActiveInvocation {
                        target: request.cancellation_target.clone(),
                        cancellation: cancellation.clone(),
                    },
                )
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            cancellation.cancel();
            self.invocations
                .lock()
                .await
                .remove(&request.cancellation_target.invocation_id);
            return Err(PluginDependencyError::Inactive);
        }
        let result = async {
            let _state_guard = self.state_writes.lock().await;
            if cancellation.is_cancelled() {
                return Err(PluginDependencyError::Cancelled);
            }
            let path = state_path(&self.config.state_root, &request.plugin_id)?;
            let mut persisted = load_json::<PersistedState>(&path)
                .await?
                .ok_or(PluginDependencyError::StateCorrupt)?;
            let key = node_state_key(request.state_scope, &request.invocation_id);
            if let Some(existing) = persisted
                .node_states
                .values()
                .find(|existing| existing.receipt.idempotency_key == request.idempotency_key)
            {
                if exact_state_replay(existing, &request) {
                    let mut receipt = existing.receipt.clone();
                    receipt.replayed = true;
                    return Ok(receipt);
                }
                return Err(PluginDependencyError::StateConflict);
            }
            match persisted.node_states.get(&key) {
                Some(existing)
                    if request.prior_generation != existing.receipt.generation
                        || request.prior_state_hash != Some(existing.receipt.state_hash) =>
                {
                    return Err(PluginDependencyError::StaleStateGeneration);
                }
                None if request.prior_generation != 0 || request.prior_state_hash.is_some() => {
                    return Err(PluginDependencyError::StaleStateGeneration);
                }
                _ => {}
            }
            if cancellation.is_cancelled() {
                return Err(PluginDependencyError::Cancelled);
            }
            let generation = request
                .prior_generation
                .checked_add(1)
                .ok_or(PluginDependencyError::StateConflict)?;
            let receipt_identity = ContentHash::digest(
                &serde_json::to_vec(&(request.action_digest, &request.idempotency_key, generation))
                    .map_err(|_| PluginDependencyError::Invalid)?,
            );
            let mut receipt = DependencyPluginNodeStateReceipt {
                plugin_id: request.plugin_id.clone(),
                invocation_id: request.invocation_id.clone(),
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id.clone(),
                executor_version: request.executor_version.clone(),
                executor_declaration_hash: request.executor_declaration_hash,
                state_scope: request.state_scope,
                prior_generation: request.prior_generation,
                generation,
                state_hash: request.state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                idempotency_key: request.idempotency_key.clone(),
                receipt_id: format!("plugin-state:{}", receipt_identity.to_hex()),
                receipt_digest: ContentHash::digest(b"pending"),
                replayed: false,
            };
            receipt.receipt_digest = node_state_receipt_digest(&receipt)?;
            persisted.node_states.insert(
                key,
                PersistedNodeState {
                    state: request.state.clone(),
                    prior_state_hash: request.prior_state_hash,
                    configuration_reference: request.configuration_reference,
                    cancellation_target: request.cancellation_target.clone(),
                    nonce: request.nonce.clone(),
                    receipt: receipt.clone(),
                },
            );
            persist_json(&path, &persisted).await?;
            Ok(receipt)
        }
        .await;
        self.invocations
            .lock()
            .await
            .remove(&request.cancellation_target.invocation_id);
        if let Ok(receipt) = &result {
            self.audit(DependencyAudit {
                plugin_id: receipt.plugin_id.clone(),
                invocation_id: Some(receipt.invocation_id.clone()),
                operation: String::from("persist_node_state"),
                outcome: if receipt.replayed {
                    String::from("reconciled")
                } else {
                    String::from("committed")
                },
                attempts: 1,
            })
            .await;
        }
        result
    }

    async fn load_node_state(
        &self,
        request: DependencyLoadNodeStateRequest,
    ) -> Result<DependencyLoadedNodeState, PluginDependencyError> {
        validate_node_state_read_request(&request)?;
        let expected_authorization_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                &request.cancellation_target,
                request.action_digest,
                &request.nonce,
                &request.authorization.cancellation_id,
                &request.idempotency_key,
            ))
            .map_err(|_| PluginDependencyError::Invalid)?,
        );
        if request.authorization_digest != expected_authorization_digest {
            return Err(PluginDependencyError::Authorization);
        }
        self.authorize(
            "plugin.node_executor.load_state",
            &(
                &request.cancellation_target,
                request.action_digest,
                &request.nonce,
                &request.authorization.cancellation_id,
                &request.idempotency_key,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            return Err(PluginDependencyError::Inactive);
        }
        let declaration = plugin
            .manifest
            .node_executors
            .iter()
            .find(|candidate| {
                candidate.executor_id == request.executor_id
                    && candidate.version == request.executor_version
                    && candidate.state_scope == node_state_scope_name(request.state_scope)
            })
            .ok_or(PluginDependencyError::Invalid)?;
        let declaration_hash = ContentHash::digest(
            &serde_json::to_vec(&to_sdk_node_executor(declaration)?)
                .map_err(|_| PluginDependencyError::Invalid)?,
        );
        if declaration_hash != request.executor_declaration_hash {
            return Err(PluginDependencyError::Invalid);
        }
        if request.configuration_reference != plugin.configuration_reference {
            return Err(PluginDependencyError::Invalid);
        }
        validate_invocation_target(
            &request.cancellation_target,
            &plugin,
            &request.invocation_id,
            &format!("{}:state-read", request.executor_id),
            plugin_node_state_load_request_hash(&request)?,
            Some(declaration_hash),
            &request.authorization,
        )?;
        let cancellation = CancellationToken::new();
        {
            let mut invocations = self.invocations.lock().await;
            if invocations
                .insert(
                    request.cancellation_target.invocation_id.clone(),
                    ActiveInvocation {
                        target: request.cancellation_target.clone(),
                        cancellation: cancellation.clone(),
                    },
                )
                .is_some()
            {
                return Err(PluginDependencyError::DuplicateInvocation);
            }
        }
        if *plugin.status.read().await != DependencyPluginStatus::Active {
            cancellation.cancel();
            self.invocations
                .lock()
                .await
                .remove(&request.cancellation_target.invocation_id);
            return Err(PluginDependencyError::Inactive);
        }
        let result = async {
            let _state_guard = self.state_writes.lock().await;
            if cancellation.is_cancelled() {
                return Err(PluginDependencyError::Cancelled);
            }
            let path = state_path(&self.config.state_root, &request.plugin_id)?;
            let mut persisted = load_json::<PersistedState>(&path)
                .await?
                .ok_or(PluginDependencyError::StateCorrupt)?;
            let key = node_state_key(request.state_scope, &request.invocation_id);
            let state = persisted
                .node_states
                .get(&key)
                .ok_or(PluginDependencyError::StaleStateGeneration)?;
            if state.receipt.plugin_id != request.plugin_id
                || state.receipt.executor_id != request.executor_id
                || state.receipt.executor_version != request.executor_version
                || state.receipt.executor_declaration_hash != request.executor_declaration_hash
                || state.receipt.state_scope != request.state_scope
                || state.receipt.generation != request.expected_generation
                || state.receipt.state_hash != request.expected_state_hash
            {
                return Err(PluginDependencyError::StaleStateGeneration);
            }
            if serde_json::to_vec(&state.state)
                .map(|encoded| ContentHash::digest(&encoded))
                .map_err(|_| PluginDependencyError::StateCorrupt)?
                != request.expected_state_hash
            {
                return Err(PluginDependencyError::StateCorrupt);
            }
            if let Some(existing) = persisted.node_state_reads.get(&request.idempotency_key) {
                if exact_state_read_replay(existing, &request) {
                    let mut receipt = existing.receipt.clone();
                    receipt.replayed = true;
                    return Ok(DependencyLoadedNodeState {
                        state: state.state.clone(),
                        receipt,
                    });
                }
                return Err(PluginDependencyError::StateConflict);
            }
            if persisted.node_state_reads.len() >= MAX_NODE_STATE_READ_RECEIPTS {
                return Err(PluginDependencyError::RateLimited);
            }
            if cancellation.is_cancelled() {
                return Err(PluginDependencyError::Cancelled);
            }
            let receipt_identity = ContentHash::digest(
                &serde_json::to_vec(&(request.action_digest, &request.idempotency_key))
                    .map_err(|_| PluginDependencyError::Invalid)?,
            );
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
                receipt_id: format!("plugin-state-read:{}", receipt_identity.to_hex()),
                receipt_digest: ContentHash::digest(b"pending"),
                replayed: false,
            };
            receipt.receipt_digest = node_state_read_receipt_digest(&receipt)?;
            persisted.node_state_reads.insert(
                request.idempotency_key.clone(),
                PersistedNodeStateRead {
                    configuration_reference: request.configuration_reference,
                    cancellation_target: request.cancellation_target.clone(),
                    nonce: request.nonce.clone(),
                    receipt: receipt.clone(),
                },
            );
            persist_json(&path, &persisted).await?;
            Ok(DependencyLoadedNodeState {
                state: state.state.clone(),
                receipt,
            })
        }
        .await;
        self.invocations
            .lock()
            .await
            .remove(&request.cancellation_target.invocation_id);
        if let Ok(loaded) = &result {
            self.audit(DependencyAudit {
                plugin_id: loaded.receipt.plugin_id.clone(),
                invocation_id: Some(loaded.receipt.invocation_id.clone()),
                operation: String::from("load_node_state"),
                outcome: if loaded.receipt.replayed {
                    String::from("reconciled")
                } else {
                    String::from("loaded")
                },
                attempts: 1,
            })
            .await;
        }
        result
    }

    async fn cancel_invocation(
        &self,
        request: DependencyCancelInvocationRequest,
    ) -> Result<DependencyInvocationCancellationReceipt, PluginDependencyError> {
        validate_cancellation_request(&request)?;
        let existing = self
            .cancellation_receipts
            .lock()
            .await
            .get(&request.idempotency_key)
            .cloned();
        let exact_replay = existing
            .as_ref()
            .is_some_and(|receipt| exact_cancellation_replay(receipt, &request));
        self.authorize_cancellation(&request, exact_replay).await?;
        if let Some(receipt) = existing {
            return if exact_replay {
                Ok(receipt)
            } else {
                Err(PluginDependencyError::IdempotencyConflict)
            };
        }
        if self.cancellation_receipts.lock().await.len() >= MAX_CANCELLATION_RECEIPTS {
            return Err(PluginDependencyError::RateLimited);
        }
        let plugin = self.entry(&request.target.plugin_id).await?;
        if plugin.manifest.version != request.target.plugin_version {
            return Err(PluginDependencyError::CancellationTargetMismatch);
        }
        let status = {
            let invocations = self.invocations.lock().await;
            match invocations.get(&request.target.invocation_id) {
                Some(active) if active.target == request.target => {
                    active.cancellation.cancel();
                    DependencyInvocationCancellationStatus::Signalled
                }
                Some(_) => return Err(PluginDependencyError::CancellationTargetMismatch),
                None => DependencyInvocationCancellationStatus::AlreadyTerminal,
            }
        };
        let receipt_identity = ContentHash::digest(
            &serde_json::to_vec(&(request.action_digest, &request.idempotency_key, status))
                .map_err(|_| PluginDependencyError::Invalid)?,
        );
        let mut receipt = DependencyInvocationCancellationReceipt {
            target: request.target,
            reason_code: request.reason_code,
            action_digest: request.action_digest,
            nonce: request.nonce,
            idempotency_key: request.idempotency_key,
            cancellation_id: request.authorization.cancellation_id,
            status,
            receipt_id: format!("plugin-cancel:{}", receipt_identity.to_hex()),
            receipt_digest: ContentHash::digest(b"pending"),
        };
        receipt.receipt_digest = cancellation_receipt_digest(&receipt)?;
        let mut receipts = self.cancellation_receipts.lock().await;
        if receipts
            .insert(receipt.idempotency_key.clone(), receipt.clone())
            .is_some()
        {
            return Err(PluginDependencyError::IdempotencyConflict);
        }
        if let Err(error) = persist_json(
            &self.config.state_root.join("cancellation-receipts.json"),
            &*receipts,
        )
        .await
        {
            receipts.remove(&receipt.idempotency_key);
            return Err(error);
        }
        drop(receipts);
        self.audit(DependencyAudit {
            plugin_id: receipt.target.plugin_id.clone(),
            invocation_id: Some(receipt.target.invocation_id.clone()),
            operation: String::from("cancel_invocation"),
            outcome: match receipt.status {
                DependencyInvocationCancellationStatus::Signalled => String::from("signalled"),
                DependencyInvocationCancellationStatus::AlreadyTerminal => {
                    String::from("already_terminal")
                }
            },
            attempts: 1,
        })
        .await;
        Ok(receipt)
    }

    async fn disable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize(
            "plugin.disable",
            &(
                &request.plugin_id,
                &request.plugin_version,
                request.configuration_reference,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        validate_lifecycle_binding(&plugin, &request)?;
        if !matches!(
            *plugin.status.read().await,
            DependencyPluginStatus::Active | DependencyPluginStatus::Disabled
        ) {
            return Err(PluginDependencyError::Invalid);
        }
        self.persist_lifecycle_receipt(&request, "disable", "disabled", "disabled")
            .await?;
        *plugin.status.write().await = DependencyPluginStatus::Disabled;
        let invocations = self.invocations.lock().await;
        for active in invocations
            .values()
            .filter(|active| active.target.plugin_id == request.plugin_id)
        {
            active.cancellation.cancel();
        }
        drop(invocations);
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

    async fn enable(
        &self,
        request: DependencyStateChangeRequest,
    ) -> Result<DependencyAudit, PluginDependencyError> {
        self.authorize(
            "plugin.enable",
            &(
                &request.plugin_id,
                &request.plugin_version,
                request.configuration_reference,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        validate_lifecycle_binding(&plugin, &request)?;
        if !matches!(
            *plugin.status.read().await,
            DependencyPluginStatus::Disabled | DependencyPluginStatus::Active
        ) {
            return Err(PluginDependencyError::Invalid);
        }
        self.persist_lifecycle_receipt(&request, "enable", "active", "active")
            .await?;
        *plugin.status.write().await = DependencyPluginStatus::Active;
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "enable".to_owned(),
            outcome: "active".to_owned(),
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
            &(
                &request.plugin_id,
                &request.plugin_version,
                request.configuration_reference,
                &request.reason,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        validate_lifecycle_binding(&plugin, &request)?;
        if !matches!(
            *plugin.status.read().await,
            DependencyPluginStatus::Active | DependencyPluginStatus::Quarantined
        ) {
            return Err(PluginDependencyError::Invalid);
        }
        let audit_outcome = request
            .reason
            .clone()
            .unwrap_or_else(|| String::from("quarantined"));
        self.persist_lifecycle_receipt(&request, "quarantine", "quarantined", &audit_outcome)
            .await?;
        *plugin.status.write().await = DependencyPluginStatus::Quarantined;
        let invocations = self.invocations.lock().await;
        for active in invocations
            .values()
            .filter(|active| active.target.plugin_id == request.plugin_id)
        {
            active.cancellation.cancel();
        }
        drop(invocations);
        let audit = DependencyAudit {
            plugin_id: request.plugin_id,
            invocation_id: None,
            operation: "quarantine".to_owned(),
            outcome: audit_outcome,
            attempts: 1,
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
            &(
                &request.plugin_id,
                &request.plugin_version,
                request.configuration_reference,
            ),
            &request.authorization,
        )
        .await?;
        let plugin = self.entry(&request.plugin_id).await?;
        validate_lifecycle_binding(&plugin, &request)?;
        if !matches!(
            *plugin.status.read().await,
            DependencyPluginStatus::Quarantined | DependencyPluginStatus::Active
        ) {
            return Err(PluginDependencyError::Invalid);
        }
        self.persist_lifecycle_receipt(&request, "unquarantine", "active", "active")
            .await?;
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
        let plugins = self.plugins.lock().await;
        let plugin_ids = plugins.keys().cloned().collect::<Vec<_>>();
        let observer_pending = plugins
            .values()
            .map(|plugin| {
                plugin
                    .observer_depth
                    .load(Ordering::Acquire)
                    .saturating_add(plugin.observer_active.load(Ordering::Acquire))
            })
            .sum();
        let observer_dropped = plugins
            .values()
            .map(|plugin| plugin.dropped.load(Ordering::Acquire))
            .sum();
        drop(plugins);
        DependencyHealth {
            loaded: plugin_ids.len(),
            running: self.invocations.lock().await.len(),
            observer_pending,
            observer_dropped,
            state_flushed: self.durable_state_flushed(&plugin_ids).await,
        }
    }

    async fn audits(&self) -> Vec<DependencyAudit> {
        self.audits.lock().await.iter().cloned().collect()
    }
}

fn validate_node_state_request(
    request: &DependencyPersistNodeStateRequest,
    maximum: usize,
) -> Result<(), PluginDependencyError> {
    if [
        request.plugin_id.as_str(),
        request.invocation_id.as_str(),
        request.executor_id.as_str(),
        request.executor_version.as_str(),
        request.nonce.as_str(),
        request.idempotency_key.as_str(),
    ]
    .iter()
    .any(|value| {
        value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    }) {
        return Err(PluginDependencyError::Invalid);
    }
    let state = serde_json::to_vec(&request.state).map_err(|_| PluginDependencyError::Invalid)?;
    if state.len() > maximum || ContentHash::digest(&state) != request.state_hash {
        return Err(PluginDependencyError::Invalid);
    }
    let action_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            &request.authorization.session_id,
            &request.plugin_id,
            &request.invocation_id,
            request.invocation_digest,
            &request.executor_id,
            &request.executor_version,
            request.executor_declaration_hash,
            request.configuration_reference,
            request.state_scope,
            request.prior_generation,
            request.prior_state_hash,
            request.state_hash,
            &request.idempotency_key,
        ))
        .map_err(|_| PluginDependencyError::Invalid)?,
    );
    if action_digest != request.action_digest {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn plugin_node_state_persist_request_hash(
    request: &DependencyPersistNodeStateRequest,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.persist.request.v1",
        &request.plugin_id,
        &request.invocation_id,
        request.invocation_digest,
        &request.executor_id,
        &request.executor_version,
        request.executor_declaration_hash,
        request.configuration_reference,
        node_state_scope_name(request.state_scope),
        request.prior_generation,
        request.prior_state_hash,
        &request.state,
        request.state_hash,
        &request.idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn validate_node_state_read_request(
    request: &DependencyLoadNodeStateRequest,
) -> Result<(), PluginDependencyError> {
    if !matches!(
        request.state_scope,
        DependencyPluginNodeStateScope::Invocation | DependencyPluginNodeStateScope::Session
    ) || request.expected_generation == 0
        || [
            request.plugin_id.as_str(),
            request.invocation_id.as_str(),
            request.executor_id.as_str(),
            request.executor_version.as_str(),
            request.nonce.as_str(),
            request.idempotency_key.as_str(),
        ]
        .iter()
        .any(|value| {
            value.is_empty()
                || value.len() > 256
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        })
    {
        return Err(PluginDependencyError::Invalid);
    }
    let action_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            &request.authorization.session_id,
            &request.plugin_id,
            &request.invocation_id,
            request.invocation_digest,
            &request.executor_id,
            &request.executor_version,
            request.executor_declaration_hash,
            request.configuration_reference,
            request.state_scope,
            request.expected_generation,
            request.expected_state_hash,
            &request.idempotency_key,
        ))
        .map_err(|_| PluginDependencyError::Invalid)?,
    );
    if action_digest != request.action_digest {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn plugin_node_state_load_request_hash(
    request: &DependencyLoadNodeStateRequest,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.load.request.v1",
        &request.plugin_id,
        &request.invocation_id,
        request.invocation_digest,
        &request.executor_id,
        &request.executor_version,
        request.executor_declaration_hash,
        request.configuration_reference,
        node_state_scope_name(request.state_scope),
        request.expected_generation,
        request.expected_state_hash,
        &request.idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

const fn node_state_scope_name(scope: DependencyPluginNodeStateScope) -> &'static str {
    match scope {
        DependencyPluginNodeStateScope::Invocation => "invocation",
        DependencyPluginNodeStateScope::ModelCall => "model_call",
        DependencyPluginNodeStateScope::Turn => "turn",
        DependencyPluginNodeStateScope::Session => "session",
        DependencyPluginNodeStateScope::Project => "project",
        DependencyPluginNodeStateScope::User => "user",
        DependencyPluginNodeStateScope::Runtime => "runtime",
    }
}

fn node_state_key(scope: DependencyPluginNodeStateScope, invocation_id: &str) -> String {
    match scope {
        DependencyPluginNodeStateScope::Invocation
        | DependencyPluginNodeStateScope::ModelCall
        | DependencyPluginNodeStateScope::Turn => {
            format!("{}:{invocation_id}", node_state_scope_name(scope))
        }
        DependencyPluginNodeStateScope::Session
        | DependencyPluginNodeStateScope::Project
        | DependencyPluginNodeStateScope::User
        | DependencyPluginNodeStateScope::Runtime => node_state_scope_name(scope).to_owned(),
    }
}

fn exact_state_replay(
    existing: &PersistedNodeState,
    request: &DependencyPersistNodeStateRequest,
) -> bool {
    existing.receipt.plugin_id == request.plugin_id
        && existing.receipt.invocation_id == request.invocation_id
        && existing.receipt.invocation_digest == request.invocation_digest
        && existing.receipt.executor_id == request.executor_id
        && existing.receipt.executor_version == request.executor_version
        && existing.receipt.executor_declaration_hash == request.executor_declaration_hash
        && existing.configuration_reference == request.configuration_reference
        && existing.cancellation_target == request.cancellation_target
        && existing.receipt.state_scope == request.state_scope
        && existing.receipt.prior_generation == request.prior_generation
        && existing.prior_state_hash == request.prior_state_hash
        && existing.receipt.state_hash == request.state_hash
        && existing.receipt.action_digest == request.action_digest
        && existing.receipt.authorization_digest == request.authorization_digest
        && existing.receipt.idempotency_key == request.idempotency_key
        && existing.nonce == request.nonce
        && existing.state == request.state
}

fn exact_state_read_replay(
    existing: &PersistedNodeStateRead,
    request: &DependencyLoadNodeStateRequest,
) -> bool {
    existing.receipt.plugin_id == request.plugin_id
        && existing.receipt.invocation_id == request.invocation_id
        && existing.receipt.invocation_digest == request.invocation_digest
        && existing.receipt.executor_id == request.executor_id
        && existing.receipt.executor_version == request.executor_version
        && existing.receipt.executor_declaration_hash == request.executor_declaration_hash
        && existing.configuration_reference == request.configuration_reference
        && existing.cancellation_target == request.cancellation_target
        && existing.receipt.state_scope == request.state_scope
        && existing.receipt.generation == request.expected_generation
        && existing.receipt.state_hash == request.expected_state_hash
        && existing.receipt.action_digest == request.action_digest
        && existing.receipt.authorization_digest == request.authorization_digest
        && existing.receipt.idempotency_key == request.idempotency_key
        && existing.nonce == request.nonce
}

async fn observer_worker(
    manifest: DependencyManifest,
    mut receiver: mpsc::Receiver<ObserverWork>,
    depth: Arc<AtomicU64>,
    active: Arc<AtomicU64>,
    maximum: usize,
) {
    while let Some(work) = receiver.recv().await {
        depth.fetch_sub(1, Ordering::AcqRel);
        active.fetch_add(1, Ordering::AcqRel);
        let cancellation = CancellationToken::new();
        let result = run_once(
            &manifest,
            &WorkerRequest::Observe {
                handler: &work.handler,
                event_type: &work.event_type,
                event: &work.event,
            },
            cancellation,
            maximum,
        )
        .await;
        let terminal = match result {
            Ok(WorkerResponse::Observed) => Ok(()),
            Ok(_) => Err(PluginDependencyError::MalformedResponse),
            Err(error) => Err(error),
        };
        let _ = work.completion.send(terminal);
        active.fetch_sub(1, Ordering::AcqRel);
        let _ = work.invocation_id;
    }
}

#[allow(clippy::too_many_arguments)]
async fn authorize_memory_operation(
    dependency: &IsolatedPluginDependency,
    action: &str,
    binding: &DependencyOperationBinding,
    implementation_id: &str,
    implementation_version: &str,
    handler: &str,
    timeout_ms: u64,
    idempotency: DependencyOperationIdempotency,
    request: &Value,
    readable_state: &Value,
    authorization: &DependencyAuthorization,
) -> Result<(), PluginDependencyError> {
    if binding.session_id != dependency.config.session_id {
        return Err(PluginDependencyError::Authorization);
    }
    dependency
        .authorize(
            action,
            &(
                binding,
                implementation_id,
                implementation_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                &authorization.cancellation_id,
            ),
            authorization,
        )
        .await
}

fn validate_plugin_binding(
    plugin: &LoadedPlugin,
    binding: &DependencyOperationBinding,
) -> Result<(), PluginDependencyError> {
    if plugin.manifest.id != binding.plugin_id
        || plugin.manifest.version != binding.plugin_version
        || plugin.configuration_reference != binding.configuration_reference
        || binding.attempt == 0
        || binding.attempt > 16
    {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn validate_provider_declaration_hash(
    provider: &DependencyMemoryProviderDeclaration,
    expected: ContentHash,
) -> Result<(), PluginDependencyError> {
    let sdk_provider = to_sdk_memory_provider(provider)?;
    let bytes = sdk_provider
        .declaration_hash_input()
        .map_err(|_| PluginDependencyError::Invalid)?;
    if ContentHash::digest(&bytes) != expected {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn validate_compactor_declaration_hash(
    compactor: &DependencyCompactorDeclaration,
    expected: ContentHash,
) -> Result<(), PluginDependencyError> {
    let sdk_compactor = to_sdk_compactor(compactor)?;
    let bytes = sdk_compactor
        .declaration_hash_input()
        .map_err(|_| PluginDependencyError::Invalid)?;
    if ContentHash::digest(&bytes) != expected {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn apply_operation_execution(
    plugin: &mut LoadedPlugin,
    operation: &DependencyOperationDeclaration,
) {
    plugin.manifest.timeout_ms = operation.timeout_ms;
    plugin.manifest.max_attempts = 1;
    plugin.manifest.retry_backoff_ms = 0;
}

fn apply_compactor_execution(
    plugin: &mut LoadedPlugin,
    compactor: &DependencyCompactorDeclaration,
) {
    plugin.manifest.timeout_ms = compactor.timeout_ms;
    plugin.manifest.max_attempts = 1;
    plugin.manifest.retry_backoff_ms = 0;
}

const fn worker_error_outcome(error: &PluginDependencyError) -> &'static str {
    match error {
        PluginDependencyError::Cancelled => "cancelled",
        PluginDependencyError::Timeout => "timeout",
        PluginDependencyError::Crashed
        | PluginDependencyError::Process
        | PluginDependencyError::External => "crashed",
        PluginDependencyError::MalformedResponse | PluginDependencyError::ResponseTooLarge => {
            "malformed_result"
        }
        _ => "rejected",
    }
}

const fn worker_error_attempts(error: &PluginDependencyError) -> u8 {
    match error {
        PluginDependencyError::Inactive
        | PluginDependencyError::RateLimited
        | PluginDependencyError::DuplicateInvocation => 0,
        _ => 1,
    }
}

fn validate_memory_write_receipt(
    request: &DependencyMemoryWriteRequest,
    declaration: &DependencyOperationDeclaration,
    receipt: &DependencyMemoryWriteReceipt,
) -> Result<(), PluginDependencyError> {
    if receipt.binding != request.binding
        || receipt.provider_id != request.provider_id
        || receipt.provider_version != request.provider_version
        || !valid_identifier(&receipt.provider_record_id)
        || request
            .request
            .get("value_hash")
            .and_then(Value::as_str)
            .and_then(|hash| hash.parse::<ContentHash>().ok())
            != Some(receipt.value_hash)
    {
        return Err(PluginDependencyError::MalformedResponse);
    }
    let encoded = serde_json::to_vec(&receipt.receipt)
        .map_err(|_| PluginDependencyError::MalformedResponse)?;
    if encoded.len() > 512 * 1024
        || !value_matches_schema(&declaration.output_schema, &receipt.receipt, 0)?
    {
        return Err(PluginDependencyError::MalformedResponse);
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
}

fn value_matches_schema(
    schema_json: &str,
    value: &Value,
    depth: usize,
) -> Result<bool, PluginDependencyError> {
    if depth > 64 {
        return Ok(false);
    }
    let schema: Value =
        serde_json::from_str(schema_json).map_err(|_| PluginDependencyError::Invalid)?;
    value_matches_schema_value(&schema, value, depth)
}

fn value_matches_schema_value(
    schema: &Value,
    value: &Value,
    depth: usize,
) -> Result<bool, PluginDependencyError> {
    if depth > 64 {
        return Ok(false);
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Ok(false);
    }
    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        let matches = match kind {
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "string" => value.is_string(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array)
            && required
                .iter()
                .filter_map(Value::as_str)
                .any(|field| !object.contains_key(field))
        {
            return Ok(false);
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        if schema.get("additionalProperties") == Some(&Value::Bool(false))
            && object
                .keys()
                .any(|field| properties.is_none_or(|items| !items.contains_key(field)))
        {
            return Ok(false);
        }
        if let Some(properties) = properties {
            for (field, field_schema) in properties {
                if let Some(field_value) = object.get(field)
                    && !value_matches_schema_value(field_schema, field_value, depth + 1)?
                {
                    return Ok(false);
                }
            }
        }
    }
    if let (Some(items_schema), Some(items)) = (schema.get("items"), value.as_array()) {
        for item in items {
            if !value_matches_schema_value(items_schema, item, depth + 1)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
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
            .map(to_sdk_node_executor)
            .collect::<Result<Vec<_>, _>>()?,
        context_transforms: manifest
            .context_transforms
            .iter()
            .map(to_sdk_context_transform)
            .collect::<Result<Vec<_>, _>>()?,
        memory_providers: manifest
            .memory_providers
            .iter()
            .map(to_sdk_memory_provider)
            .collect::<Result<Vec<_>, _>>()?,
        compactors: manifest
            .compactors
            .iter()
            .map(to_sdk_compactor)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn to_sdk_memory_provider(
    provider: &DependencyMemoryProviderDeclaration,
) -> Result<sdk::MemoryProviderManifest, PluginDependencyError> {
    Ok(sdk::MemoryProviderManifest {
        provider_id: provider.provider_id.clone(),
        version: provider.version.clone(),
        runtime_api: provider.runtime_api.clone(),
        capabilities: provider.capabilities.clone(),
        retrieve: to_sdk_memory_retrieve(&provider.retrieve)?,
        write: provider
            .write
            .as_ref()
            .map(to_sdk_memory_write)
            .transpose()?,
    })
}

fn to_sdk_memory_retrieve(
    operation: &DependencyOperationDeclaration,
) -> Result<sdk::MemoryRetrieveManifest, PluginDependencyError> {
    Ok(sdk::MemoryRetrieveManifest {
        handler: operation.handler.clone(),
        input_schema: operation.input_schema.clone(),
        output_schema: operation.output_schema.clone(),
        timeout_ms: operation.timeout_ms,
        failure_policy: parse_operation_failure(operation)?,
        idempotency: map_sdk_operation_idempotency(operation.idempotency),
        required_permissions: operation_permissions(operation),
        state_scope: parse_scope(&operation.state_scope)?,
        external_effects: operation.external_effects,
    })
}

fn to_sdk_memory_write(
    operation: &DependencyOperationDeclaration,
) -> Result<sdk::MemoryWriteManifest, PluginDependencyError> {
    Ok(sdk::MemoryWriteManifest {
        handler: operation.handler.clone(),
        input_schema: operation.input_schema.clone(),
        output_schema: operation.output_schema.clone(),
        timeout_ms: operation.timeout_ms,
        failure_policy: parse_operation_failure(operation)?,
        idempotency: map_sdk_operation_idempotency(operation.idempotency),
        required_permissions: operation_permissions(operation),
        state_scope: parse_scope(&operation.state_scope)?,
        external_effects: operation.external_effects,
    })
}

fn to_sdk_compactor(
    compactor: &DependencyCompactorDeclaration,
) -> Result<sdk::CompactorManifest, PluginDependencyError> {
    Ok(sdk::CompactorManifest {
        compactor_id: compactor.compactor_id.clone(),
        version: compactor.version.clone(),
        runtime_api: compactor.runtime_api.clone(),
        handler: compactor.handler.clone(),
        capabilities: compactor.capabilities.clone(),
        input_schema: compactor.input_schema.clone(),
        output_schema: compactor.output_schema.clone(),
        timeout_ms: compactor.timeout_ms,
        failure_policy: parse_failure(
            &compactor.failure_policy,
            compactor.max_attempts,
            compactor.retry_backoff_ms,
        )?,
        idempotency: map_sdk_operation_idempotency(compactor.idempotency),
        required_permissions: sdk::PermissionManifest {
            tools: compactor.tool_permissions.clone(),
            network: compactor.network_permissions.clone(),
        },
        state_scope: parse_scope(&compactor.state_scope)?,
        external_effects: compactor.external_effects,
    })
}

fn parse_operation_failure(
    operation: &DependencyOperationDeclaration,
) -> Result<sdk::FailurePolicy, PluginDependencyError> {
    parse_failure(
        &operation.failure_policy,
        operation.max_attempts,
        operation.retry_backoff_ms,
    )
}

fn parse_failure(
    policy: &str,
    max_attempts: u8,
    retry_backoff_ms: u64,
) -> Result<sdk::FailurePolicy, PluginDependencyError> {
    match policy {
        "reject" => Ok(sdk::FailurePolicy::Reject),
        "cancel" => Ok(sdk::FailurePolicy::Cancel),
        "disable" => Ok(sdk::FailurePolicy::Disable),
        "continue" => Ok(sdk::FailurePolicy::Continue),
        "retry" => Ok(sdk::FailurePolicy::Retry {
            max_attempts,
            backoff_ms: retry_backoff_ms,
        }),
        _ => Err(PluginDependencyError::Invalid),
    }
}

const fn map_sdk_operation_idempotency(
    idempotency: DependencyOperationIdempotency,
) -> sdk::PluginOperationIdempotency {
    match idempotency {
        DependencyOperationIdempotency::Idempotent => sdk::PluginOperationIdempotency::Idempotent,
        DependencyOperationIdempotency::NonIdempotent => {
            sdk::PluginOperationIdempotency::NonIdempotent
        }
    }
}

fn operation_permissions(operation: &DependencyOperationDeclaration) -> sdk::PermissionManifest {
    sdk::PermissionManifest {
        tools: operation.tool_permissions.clone(),
        network: operation.network_permissions.clone(),
    }
}

fn to_sdk_context_transform(
    transform: &DependencyContextTransformDeclaration,
) -> Result<sdk::ContextTransformManifest, PluginDependencyError> {
    Ok(sdk::ContextTransformManifest {
        transform_id: transform.transform_id.clone(),
        version: transform.version.clone(),
        runtime_api: transform.runtime_api.clone(),
        handler: transform.handler.clone(),
        lifecycle: match transform.lifecycle {
            DependencyContextTransformLifecycle::BeforeModelRequest => {
                sdk::ContextTransformLifecycle::BeforeModelRequest
            }
        },
        capabilities: transform.capabilities.iter().cloned().collect(),
        input_schema: transform.input_schema.clone(),
        output_schema: transform.output_schema.clone(),
        timeout_ms: transform.timeout_ms,
        failure_policy: match transform.failure_policy.as_str() {
            "reject" => sdk::FailurePolicy::Reject,
            "cancel" => sdk::FailurePolicy::Cancel,
            "disable" => sdk::FailurePolicy::Disable,
            "continue" => sdk::FailurePolicy::Continue,
            "retry" => sdk::FailurePolicy::Retry {
                max_attempts: transform.max_attempts,
                backoff_ms: transform.retry_backoff_ms,
            },
            _ => return Err(PluginDependencyError::Invalid),
        },
        idempotency: match transform.idempotency {
            DependencyContextTransformIdempotency::Idempotent => {
                sdk::ContextTransformIdempotency::Idempotent
            }
            DependencyContextTransformIdempotency::NonIdempotent => {
                sdk::ContextTransformIdempotency::NonIdempotent
            }
        },
        required_permissions: sdk::PermissionManifest {
            tools: transform.tool_permissions.iter().cloned().collect(),
            network: transform.network_permissions.iter().cloned().collect(),
        },
        state_scope: parse_scope(&transform.state_scope)?,
        external_effects: transform.external_effects,
    })
}

fn to_sdk_node_executor(
    executor: &DependencyNodeExecutorDeclaration,
) -> Result<sdk::NodeExecutorManifest, PluginDependencyError> {
    Ok(sdk::NodeExecutorManifest {
        executor_id: executor.executor_id.clone(),
        version: executor.version.clone(),
        runtime_api: executor.runtime_api.clone(),
        node_kind: executor.node_kind.clone(),
        handler: executor.handler.clone(),
        capabilities: executor.capabilities.iter().cloned().collect(),
        input_schema: executor.input_schema.clone(),
        output_schema: executor.output_schema.clone(),
        timeout_ms: executor.timeout_ms,
        failure_policy: match executor.failure_policy.as_str() {
            "reject" => sdk::FailurePolicy::Reject,
            "cancel" => sdk::FailurePolicy::Cancel,
            "disable" => sdk::FailurePolicy::Disable,
            "continue" => sdk::FailurePolicy::Continue,
            "retry" => sdk::FailurePolicy::Retry {
                max_attempts: executor.max_attempts,
                backoff_ms: executor.retry_backoff_ms,
            },
            _ => return Err(PluginDependencyError::Invalid),
        },
        idempotency: if executor.idempotency == DependencyNodeExecutorIdempotency::Idempotent {
            sdk::NodeExecutorIdempotency::Idempotent
        } else {
            sdk::NodeExecutorIdempotency::NonIdempotent
        },
        required_permissions: sdk::PermissionManifest {
            tools: executor.tool_permissions.iter().cloned().collect(),
            network: executor.network_permissions.iter().cloned().collect(),
        },
        state_scope: parse_scope(&executor.state_scope)?,
        external_effects: executor.external_effects,
    })
}

fn parse_scope(value: &str) -> Result<sdk::PluginScope, PluginDependencyError> {
    match value {
        "invocation" => Ok(sdk::PluginScope::Invocation),
        "model_call" => Ok(sdk::PluginScope::ModelCall),
        "turn" => Ok(sdk::PluginScope::Turn),
        "session" => Ok(sdk::PluginScope::Session),
        "project" => Ok(sdk::PluginScope::Project),
        "user" => Ok(sdk::PluginScope::User),
        "runtime" => Ok(sdk::PluginScope::Runtime),
        _ => Err(PluginDependencyError::Invalid),
    }
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

fn configuration_reference(configuration: &Value) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(configuration)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginDependencyError::Configuration)
}

fn observer_delivery_request_hash(
    request: &DependencyObservationRequest,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.observer.delivery.request.v1",
        &request.plugin_id,
        &request.invocation_id,
        &request.handler,
        &request.event_type,
        &request.event,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

const fn observer_delivery_status_name(status: DependencyObserverDeliveryStatus) -> &'static str {
    match status {
        DependencyObserverDeliveryStatus::Completed => "completed",
        DependencyObserverDeliveryStatus::Rejected => "rejected",
        DependencyObserverDeliveryStatus::Failed => "failed",
        DependencyObserverDeliveryStatus::Ambiguous => "ambiguous",
    }
}

fn observer_delivery_receipt_id(
    plugin_id: &str,
    invocation_id: &str,
    request_hash: ContentHash,
) -> Result<String, PluginDependencyError> {
    serde_json::to_vec(&(plugin_id, invocation_id, request_hash))
        .map(|bytes| format!("observer:{}", ContentHash::digest(&bytes).to_hex()))
        .map_err(|_| PluginDependencyError::Invalid)
}

fn observer_delivery_receipt_digest(
    plugin_id: &str,
    invocation_id: &str,
    request_hash: ContentHash,
    status: DependencyObserverDeliveryStatus,
    receipt_id: &str,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.observer.delivery.receipt.v1",
        plugin_id,
        invocation_id,
        request_hash,
        observer_delivery_status_name(status),
        receipt_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn persisted_observer_result(
    plugin: &LoadedPlugin,
    persisted: PersistedObserverDelivery,
    replayed: bool,
) -> Result<DependencyObservationResult, PluginDependencyError> {
    Ok(DependencyObservationResult {
        accepted: persisted.accepted,
        queue_depth: usize::try_from(plugin.observer_depth.load(Ordering::Acquire))
            .unwrap_or(usize::MAX),
        dropped: plugin.dropped.load(Ordering::Acquire),
        status: persisted.status.ok_or(PluginDependencyError::Ambiguous)?,
        request_hash: persisted.request_hash,
        receipt_id: persisted
            .receipt_id
            .ok_or(PluginDependencyError::StateCorrupt)?,
        receipt_digest: persisted
            .receipt_digest
            .ok_or(PluginDependencyError::StateCorrupt)?,
        replayed,
    })
}

const fn observer_failure_is_definite(error: &PluginDependencyError) -> bool {
    matches!(
        error,
        PluginDependencyError::Executable
            | PluginDependencyError::Invalid
            | PluginDependencyError::Configuration
            | PluginDependencyError::ConfigurationDrift
            | PluginDependencyError::NotLoaded
            | PluginDependencyError::Inactive
            | PluginDependencyError::WrongClass
    )
}

/// Hashes one complete dependency-owned invocation cancellation target.
///
/// # Errors
///
/// Returns [`PluginDependencyError::Invalid`] if deterministic encoding fails.
pub fn invocation_identity_digest(
    target: &DependencyInvocationCancellationTarget,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.invocation.identity.v1",
        &target.session_id,
        &target.run_id,
        &target.plugin_id,
        &target.plugin_version,
        &target.invocation_id,
        &target.operation_id,
        target.declaration_hash,
        target.request_hash,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn validate_invocation_target(
    target: &DependencyInvocationCancellationTarget,
    plugin: &LoadedPlugin,
    invocation_id: &str,
    operation_id: &str,
    request_hash: ContentHash,
    declaration_hash: Option<ContentHash>,
    authorization: &DependencyAuthorization,
) -> Result<(), PluginDependencyError> {
    if target.session_id != authorization.session_id
        || target.plugin_id != plugin.manifest.id
        || target.plugin_version != plugin.manifest.version
        || target.invocation_id != invocation_id
        || target.operation_id != operation_id
        || target.request_hash != request_hash
        || declaration_hash.is_some_and(|hash| target.declaration_hash != hash)
        || invocation_identity_digest(target)? != target.invocation_digest
    {
        return Err(PluginDependencyError::CancellationTargetMismatch);
    }
    Ok(())
}

fn plugin_interceptor_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    handler: &str,
    proposal_type: &str,
    proposal: &Value,
    readable_state: &Value,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.interceptor.request.v1",
        plugin_id,
        invocation_id,
        handler,
        proposal_type,
        proposal,
        readable_state,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

#[allow(clippy::too_many_arguments)]
fn plugin_node_executor_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    executor_id: &str,
    executor_version: &str,
    node_kind: &str,
    handler: &str,
    timeout_ms: u64,
    configuration_reference: ContentHash,
    input: &Value,
    readable_state: &Value,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-executor.request.v1",
        plugin_id,
        invocation_id,
        executor_id,
        executor_version,
        node_kind,
        handler,
        timeout_ms,
        configuration_reference,
        input,
        readable_state,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

#[allow(clippy::too_many_arguments)]
fn plugin_context_transform_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    transform_id: &str,
    transform_version: &str,
    lifecycle: DependencyContextTransformLifecycle,
    handler: &str,
    timeout_ms: u64,
    configuration_reference: ContentHash,
    input: &Value,
    readable_state: &Value,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.context-transform.request.v1",
        plugin_id,
        invocation_id,
        transform_id,
        transform_version,
        match lifecycle {
            DependencyContextTransformLifecycle::BeforeModelRequest => "before_model_request",
        },
        handler,
        timeout_ms,
        configuration_reference,
        input,
        readable_state,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

#[derive(Deserialize, Serialize)]
struct NormalizedPluginArtifactReference {
    artifact_id: String,
    content_hash: ContentHash,
    media_type: String,
    size_bytes: u64,
    security_classification: String,
}

#[derive(Deserialize, Serialize)]
struct NormalizedPluginCanonicalReference {
    kind: String,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<ContentHash>,
}

#[derive(Deserialize)]
struct NormalizedMemoryRetrieveInput {
    query: String,
    scopes: BTreeSet<String>,
    max_items: u32,
    max_bytes: u64,
    artifacts: Vec<NormalizedPluginArtifactReference>,
    references: Vec<NormalizedPluginCanonicalReference>,
    parameters: Value,
}

#[derive(Deserialize)]
struct NormalizedMemoryWriteInput {
    scope: String,
    boundary: String,
    value: Value,
    value_hash: ContentHash,
    artifacts: Vec<NormalizedPluginArtifactReference>,
    references: Vec<NormalizedPluginCanonicalReference>,
    security_classification: String,
    parameters: Value,
}

#[derive(Deserialize)]
struct NormalizedCompactionInput {
    projection: Value,
    projection_hash: ContentHash,
    required_references: Vec<NormalizedPluginCanonicalReference>,
    required_artifacts: Vec<NormalizedPluginArtifactReference>,
    preservation_requirements: BTreeSet<String>,
    max_replacement_bytes: u64,
    max_projection_tokens: u64,
    parameters: Value,
}

#[derive(Serialize)]
struct HashedMemoryRetrieveRequest<'a> {
    schema: &'static str,
    plugin_id: &'a str,
    plugin_version: &'a str,
    invocation_id: &'a str,
    operation_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    idempotency_key: &'a str,
    attempt: u8,
    provider_id: &'a str,
    provider_version: &'a str,
    handler: &'a str,
    timeout_ms: u64,
    idempotency: &'static str,
    query: &'a str,
    scopes: &'a BTreeSet<String>,
    max_items: u32,
    max_bytes: u64,
    artifacts: &'a [NormalizedPluginArtifactReference],
    references: &'a [NormalizedPluginCanonicalReference],
    parameters: &'a Value,
    readable_state: &'a Value,
}

#[derive(Serialize)]
struct HashedMemoryWriteRequest<'a> {
    schema: &'static str,
    plugin_id: &'a str,
    plugin_version: &'a str,
    invocation_id: &'a str,
    operation_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    idempotency_key: &'a str,
    attempt: u8,
    provider_id: &'a str,
    provider_version: &'a str,
    handler: &'a str,
    timeout_ms: u64,
    idempotency: &'static str,
    scope: &'a str,
    boundary: &'a str,
    value: &'a Value,
    value_hash: ContentHash,
    artifacts: &'a [NormalizedPluginArtifactReference],
    references: &'a [NormalizedPluginCanonicalReference],
    security_classification: &'a str,
    parameters: &'a Value,
    readable_state: &'a Value,
}

#[derive(Serialize)]
struct HashedCompactionRequest<'a> {
    schema: &'static str,
    plugin_id: &'a str,
    plugin_version: &'a str,
    invocation_id: &'a str,
    operation_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    idempotency_key: &'a str,
    attempt: u8,
    compactor_id: &'a str,
    compactor_version: &'a str,
    handler: &'a str,
    timeout_ms: u64,
    idempotency: &'static str,
    projection: &'a Value,
    projection_hash: ContentHash,
    required_references: &'a [NormalizedPluginCanonicalReference],
    required_artifacts: &'a [NormalizedPluginArtifactReference],
    preservation_requirements: &'a BTreeSet<String>,
    max_replacement_bytes: u64,
    max_projection_tokens: u64,
    parameters: &'a Value,
    readable_state: &'a Value,
}

fn operation_idempotency_name(value: DependencyOperationIdempotency) -> &'static str {
    match value {
        DependencyOperationIdempotency::Idempotent => "idempotent",
        DependencyOperationIdempotency::NonIdempotent => "non_idempotent",
    }
}

fn memory_retrieve_request_hash(
    request: &DependencyMemoryRetrieveRequest,
) -> Result<ContentHash, PluginDependencyError> {
    let input: NormalizedMemoryRetrieveInput = serde_json::from_value(request.request.clone())
        .map_err(|_| PluginDependencyError::Invalid)?;
    serde_json::to_vec(&HashedMemoryRetrieveRequest {
        schema: "agentmod.plugin.memory-retrieve.request.v2",
        plugin_id: &request.binding.plugin_id,
        plugin_version: &request.binding.plugin_version,
        invocation_id: &request.binding.invocation_id,
        operation_id: &request.binding.operation_id,
        session_id: &request.binding.session_id,
        run_id: &request.binding.run_id,
        node_id: request.binding.node_id.as_deref(),
        declaration_hash: request.binding.declaration_hash,
        configuration_reference: request.binding.configuration_reference,
        idempotency_key: &request.binding.idempotency_key,
        attempt: request.binding.attempt,
        provider_id: &request.provider_id,
        provider_version: &request.provider_version,
        handler: &request.handler,
        timeout_ms: request.timeout_ms,
        idempotency: operation_idempotency_name(request.idempotency),
        query: &input.query,
        scopes: &input.scopes,
        max_items: input.max_items,
        max_bytes: input.max_bytes,
        artifacts: &input.artifacts,
        references: &input.references,
        parameters: &input.parameters,
        readable_state: &request.readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn memory_write_request_hash(
    request: &DependencyMemoryWriteRequest,
) -> Result<ContentHash, PluginDependencyError> {
    let input: NormalizedMemoryWriteInput = serde_json::from_value(request.request.clone())
        .map_err(|_| PluginDependencyError::Invalid)?;
    serde_json::to_vec(&HashedMemoryWriteRequest {
        schema: "agentmod.plugin.memory-write.request.v2",
        plugin_id: &request.binding.plugin_id,
        plugin_version: &request.binding.plugin_version,
        invocation_id: &request.binding.invocation_id,
        operation_id: &request.binding.operation_id,
        session_id: &request.binding.session_id,
        run_id: &request.binding.run_id,
        node_id: request.binding.node_id.as_deref(),
        declaration_hash: request.binding.declaration_hash,
        configuration_reference: request.binding.configuration_reference,
        idempotency_key: &request.binding.idempotency_key,
        attempt: request.binding.attempt,
        provider_id: &request.provider_id,
        provider_version: &request.provider_version,
        handler: &request.handler,
        timeout_ms: request.timeout_ms,
        idempotency: operation_idempotency_name(request.idempotency),
        scope: &input.scope,
        boundary: &input.boundary,
        value: &input.value,
        value_hash: input.value_hash,
        artifacts: &input.artifacts,
        references: &input.references,
        security_classification: &input.security_classification,
        parameters: &input.parameters,
        readable_state: &request.readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn compaction_request_hash(
    request: &DependencyCompactionRequest,
) -> Result<ContentHash, PluginDependencyError> {
    let input: NormalizedCompactionInput = serde_json::from_value(request.request.clone())
        .map_err(|_| PluginDependencyError::Invalid)?;
    serde_json::to_vec(&HashedCompactionRequest {
        schema: "agentmod.plugin.compaction.request.v2",
        plugin_id: &request.binding.plugin_id,
        plugin_version: &request.binding.plugin_version,
        invocation_id: &request.binding.invocation_id,
        operation_id: &request.binding.operation_id,
        session_id: &request.binding.session_id,
        run_id: &request.binding.run_id,
        node_id: request.binding.node_id.as_deref(),
        declaration_hash: request.binding.declaration_hash,
        configuration_reference: request.binding.configuration_reference,
        idempotency_key: &request.binding.idempotency_key,
        attempt: request.binding.attempt,
        compactor_id: &request.compactor_id,
        compactor_version: &request.compactor_version,
        handler: &request.handler,
        timeout_ms: request.timeout_ms,
        idempotency: operation_idempotency_name(request.idempotency),
        projection: &input.projection,
        projection_hash: input.projection_hash,
        required_references: &input.required_references,
        required_artifacts: &input.required_artifacts,
        preservation_requirements: &input.preservation_requirements,
        max_replacement_bytes: input.max_replacement_bytes,
        max_projection_tokens: input.max_projection_tokens,
        parameters: &input.parameters,
        readable_state: &request.readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

/// Hashes the domain-separated cancellation action authorized by the runtime.
///
/// # Errors
///
/// Returns [`PluginDependencyError::Invalid`] if deterministic encoding fails.
pub fn cancellation_action_digest(
    target: &DependencyInvocationCancellationTarget,
    reason_code: &str,
    nonce: &str,
    idempotency_key: &str,
    cancellation_id: &str,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.invocation.cancel.v1",
        target,
        reason_code,
        nonce,
        idempotency_key,
        cancellation_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

/// Hashes every field in a signal-only cancellation receipt except its digest.
///
/// # Errors
///
/// Returns [`PluginDependencyError::Invalid`] if deterministic encoding fails.
pub fn cancellation_receipt_digest(
    receipt: &DependencyInvocationCancellationReceipt,
) -> Result<ContentHash, PluginDependencyError> {
    serde_json::to_vec(&(
        "agentmod.plugin.invocation.cancel.receipt.v1",
        &receipt.target,
        &receipt.reason_code,
        receipt.action_digest,
        &receipt.nonce,
        &receipt.idempotency_key,
        &receipt.cancellation_id,
        receipt.status,
        &receipt.receipt_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginDependencyError::Invalid)
}

fn exact_cancellation_replay(
    receipt: &DependencyInvocationCancellationReceipt,
    request: &DependencyCancelInvocationRequest,
) -> bool {
    receipt.target == request.target
        && receipt.reason_code == request.reason_code
        && receipt.action_digest == request.action_digest
        && receipt.nonce == request.nonce
        && receipt.idempotency_key == request.idempotency_key
        && receipt.cancellation_id == request.authorization.cancellation_id
}

fn validate_cancellation_request(
    request: &DependencyCancelInvocationRequest,
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
        request.authorization.owner_id.as_str(),
        request.authorization.session_id.as_str(),
        request.authorization.call_id.as_str(),
        request.authorization.cancellation_id.as_str(),
    ] {
        if value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
        {
            return Err(PluginDependencyError::Invalid);
        }
    }
    if request.authorization.grant.is_empty() || request.authorization.normalized_digest.len() != 64
    {
        return Err(PluginDependencyError::Invalid);
    }
    Ok(())
}

fn operation_cancellation_target(
    binding: &DependencyOperationBinding,
) -> Result<DependencyInvocationCancellationTarget, PluginDependencyError> {
    let mut target = DependencyInvocationCancellationTarget {
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        invocation_digest: ContentHash::digest(b"uninitialized"),
        operation_id: binding.operation_id.clone(),
        declaration_hash: binding.declaration_hash,
        request_hash: binding.request_hash,
    };
    target.invocation_digest = invocation_identity_digest(&target)?;
    Ok(target)
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
    /// Loaded immutable configuration drift.
    #[error("plugin configuration differs from the loaded immutable configuration")]
    ConfigurationDrift,
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
    /// Complete cancellation target did not match the active invocation.
    #[error("plugin cancellation target does not match the active invocation")]
    CancellationTargetMismatch,
    /// Cancellation idempotency key was reused for a different action.
    #[error("plugin cancellation idempotency identity conflicts with an existing receipt")]
    IdempotencyConflict,
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
    /// Consequential execution may have completed without an acceptable receipt.
    #[error("plugin execution may have completed without a terminal receipt")]
    Ambiguous,
    /// Bound.
    #[error("plugin response exceeded its bound")]
    ResponseTooLarge,
    /// State version.
    #[error("plugin state version is incompatible")]
    StateVersion,
    /// State.
    #[error("plugin state is corrupt")]
    StateCorrupt,
    /// Stale plugin-node state generation.
    #[error("plugin-node state generation is stale")]
    StaleStateGeneration,
    /// Conflicting plugin-node state idempotency identity.
    #[error("plugin-node state conflicts with an existing receipt")]
    StateConflict,
    /// External.
    #[error("plugin dependency operation failed")]
    External,
}

#[cfg(test)]
mod node_state_tests {
    use super::*;
    use agentmod_protocol_support::authorization::{
        AuthorizationClaims, AuthorizationKey, seal_authorization,
    };
    use serde::Serialize;
    use serde_json::json;
    use tempfile::TempDir;

    #[allow(
        clippy::too_many_lines,
        reason = "the isolated state fixture assembles one exact manifest and durable store"
    )]
    async fn fixture() -> (TempDir, IsolatedPluginDependency, ContentHash) {
        let root = TempDir::new().expect("root");
        let dependency = IsolatedPluginDependency::new(PluginDependencyConfig {
            runtime_api_version: String::from("1.0.0"),
            protocol_version: 5,
            available_capabilities: BTreeSet::new(),
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            authorization_key_hex: "07".repeat(32),
            state_root: root.path().join("state"),
            executable_roots: vec![root.path().to_owned()],
            observer_queue_capacity: 4,
            max_response_bytes: 1024 * 1024,
            rate_limit_per_minute: 100,
            max_restarts: 0,
            audit_capacity: 16,
        })
        .await
        .expect("dependency");
        let declaration = DependencyNodeExecutorDeclaration {
            executor_id: String::from("fixture.executor"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            node_kind: String::from("plugin_fixture"),
            handler: String::from("execute"),
            capabilities: BTreeSet::new(),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"object"}"#),
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            idempotency: DependencyNodeExecutorIdempotency::Idempotent,
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            state_scope: String::from("session"),
            external_effects: false,
        };
        let declaration_hash = ContentHash::digest(
            &serde_json::to_vec(&to_sdk_node_executor(&declaration).expect("SDK declaration"))
                .expect("declaration bytes"),
        );
        let manifest = DependencyManifest {
            schema_version: 1,
            id: String::from("fixture.plugin"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            category: String::from("extension"),
            scope: String::from("session"),
            class: DependencyPluginClass::Extension,
            entrypoint: DependencyEntrypoint {
                program: root.path().join("unused").to_string_lossy().into_owned(),
                arguments: Vec::new(),
            },
            required_capabilities: BTreeSet::new(),
            provided_capabilities: BTreeSet::new(),
            subscribed_events: BTreeSet::new(),
            read_authority: BTreeSet::new(),
            proposed_write_authority: BTreeSet::new(),
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            after: BTreeSet::new(),
            before: BTreeSet::new(),
            stage: 0,
            priority: 0,
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            state_migration_version: 1,
            configuration_schema: DependencyConfigurationSchema {
                id: String::from("fixture.config"),
                version: 1,
                required: false,
                inline_json: String::from(r#"{"type":"object"}"#),
            },
            node_executors: vec![declaration],
            context_transforms: Vec::new(),
            memory_providers: Vec::new(),
            compactors: Vec::new(),
        };
        dependency.plugins.lock().await.insert(
            String::from("fixture.plugin"),
            LoadedPlugin {
                manifest,
                configuration: json!({}),
                configuration_reference: configuration_reference(&json!({}))
                    .expect("configuration reference"),
                status: Arc::new(RwLock::new(DependencyPluginStatus::Active)),
                observer: None,
                observer_depth: Arc::new(AtomicU64::new(0)),
                observer_active: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
            },
        );
        persist_json(
            &state_path(&dependency.config.state_root, "fixture.plugin").expect("state path"),
            &PersistedState {
                version: 1,
                value: json!({}),
                lifecycle_state: default_lifecycle_state(),
                lifecycle_receipt: None,
                observer_deliveries: BTreeMap::new(),
                node_states: BTreeMap::new(),
                node_state_reads: BTreeMap::new(),
            },
        )
        .await
        .expect("initial state");
        (root, dependency, declaration_hash)
    }

    fn authorization<T: Serialize>(
        action: &str,
        operation: &T,
        call_id: &str,
        cancellation_id: &str,
    ) -> DependencyAuthorization {
        let digest =
            ContentHash::digest(&serde_json::to_vec(operation).expect("authorization operation"));
        let now = now_millis().expect("clock");
        let key = AuthorizationKey::from_bytes([7; 32]);
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: String::from("owner"),
                session: String::from("session-1"),
                call_id: call_id.to_owned(),
                action: action.to_owned(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(now),
                expires_at: TimestampMillis::new(now + 30_000),
                nonce: Uuid::now_v7().to_string(),
            },
            &key,
        )
        .expect("grant");
        DependencyAuthorization {
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            call_id: call_id.to_owned(),
            normalized_digest: digest.to_hex(),
            grant,
            cancellation_id: cancellation_id.to_owned(),
        }
    }

    fn cancellation_target(invocation_id: &str) -> DependencyInvocationCancellationTarget {
        let declaration_hash = ContentHash::digest(b"memory declaration");
        let request_hash = ContentHash::digest(b"memory request");
        let mut target = DependencyInvocationCancellationTarget {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: invocation_id.to_owned(),
            invocation_digest: ContentHash::digest(b"pending"),
            operation_id: String::from("memory-operation-1"),
            declaration_hash,
            request_hash,
        };
        target.invocation_digest =
            invocation_identity_digest(&target).expect("invocation identity");
        target
    }

    fn cancellation_request(
        target: DependencyInvocationCancellationTarget,
        reason_code: &str,
        nonce: &str,
        idempotency_key: &str,
        cancellation_id: &str,
    ) -> DependencyCancelInvocationRequest {
        let action_digest = cancellation_action_digest(
            &target,
            reason_code,
            nonce,
            idempotency_key,
            cancellation_id,
        )
        .expect("cancellation action");
        let now = now_millis().expect("clock");
        let call_id = format!("cancel-call-{}", Uuid::now_v7());
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: String::from("owner"),
                session: target.session_id.clone(),
                call_id: call_id.clone(),
                action: String::from("plugin.invocation.cancel"),
                normalized_digest: action_digest,
                issued_at: TimestampMillis::new(now),
                expires_at: TimestampMillis::new(now + 30_000),
                nonce: nonce.to_owned(),
            },
            &AuthorizationKey::from_bytes([7; 32]),
        )
        .expect("cancellation grant");
        DependencyCancelInvocationRequest {
            target: target.clone(),
            reason_code: reason_code.to_owned(),
            action_digest,
            nonce: nonce.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            authorization: DependencyAuthorization {
                owner_id: String::from("owner"),
                session_id: target.session_id,
                call_id,
                normalized_digest: action_digest.to_hex(),
                grant,
                cancellation_id: cancellation_id.to_owned(),
            },
        }
    }

    async fn restart_dependency(root: &TempDir, state_root: PathBuf) -> IsolatedPluginDependency {
        IsolatedPluginDependency::new(PluginDependencyConfig {
            runtime_api_version: String::from("1.0.0"),
            protocol_version: 8,
            available_capabilities: BTreeSet::new(),
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            authorization_key_hex: "07".repeat(32),
            state_root,
            executable_roots: vec![root.path().to_owned()],
            observer_queue_capacity: 4,
            max_response_bytes: 1024 * 1024,
            rate_limit_per_minute: 100,
            max_restarts: 0,
            audit_capacity: 16,
        })
        .await
        .expect("restart")
    }

    #[tokio::test]
    async fn cancellation_is_exact_grant_bound_idempotent_and_signal_only() {
        let (root, dependency, _declaration_hash) = fixture().await;
        let target = cancellation_target("memory-invocation-1");
        let token = CancellationToken::new();
        dependency.invocations.lock().await.insert(
            target.invocation_id.clone(),
            ActiveInvocation {
                target: target.clone(),
                cancellation: token.clone(),
            },
        );
        let request = cancellation_request(
            target.clone(),
            "user_cancelled",
            "cancel-nonce-1",
            "cancel-key-1",
            "cancellation-1",
        );
        let receipt = dependency
            .cancel_invocation(request.clone())
            .await
            .expect("exact cancellation");
        assert!(token.is_cancelled());
        assert_eq!(
            receipt.status,
            DependencyInvocationCancellationStatus::Signalled
        );
        assert_eq!(
            cancellation_receipt_digest(&receipt).expect("receipt digest"),
            receipt.receipt_digest
        );
        assert_eq!(
            dependency
                .cancel_invocation(request.clone())
                .await
                .expect("exact reconciliation"),
            receipt
        );

        let conflicting = cancellation_request(
            target.clone(),
            "timeout",
            "cancel-nonce-2",
            "cancel-key-1",
            "cancellation-2",
        );
        assert_eq!(
            dependency.cancel_invocation(conflicting).await,
            Err(PluginDependencyError::IdempotencyConflict)
        );

        let mut substituted = request.clone();
        substituted.target.run_id = String::from("run-2");
        assert_eq!(
            dependency.cancel_invocation(substituted).await,
            Err(PluginDependencyError::Authorization)
        );

        let replay = cancellation_request(
            target.clone(),
            "timeout",
            "cancel-nonce-1",
            "cancel-key-2",
            "cancellation-3",
        );
        assert_eq!(
            dependency.cancel_invocation(replay).await,
            Err(PluginDependencyError::Replay)
        );

        let terminal = cancellation_request(
            cancellation_target("already-terminal"),
            "timeout",
            "cancel-nonce-3",
            "cancel-key-3",
            "cancellation-4",
        );
        let terminal_receipt = dependency
            .cancel_invocation(terminal)
            .await
            .expect("terminal no-op");
        assert_eq!(
            terminal_receipt.status,
            DependencyInvocationCancellationStatus::AlreadyTerminal
        );
        let persisted = load_json::<PersistedState>(
            &state_path(&dependency.config.state_root, "fixture.plugin").expect("state path"),
        )
        .await
        .expect("state read")
        .expect("state");
        assert!(persisted.node_states.is_empty());
        assert!(persisted.node_state_reads.is_empty());

        let state_root = dependency.config.state_root.clone();
        drop(dependency);
        let restarted = restart_dependency(&root, state_root).await;
        assert_eq!(
            restarted
                .cancel_invocation(request)
                .await
                .expect("durable exact receipt"),
            receipt
        );
    }

    #[tokio::test]
    async fn cancellation_rejects_target_and_grant_substitution() {
        let (_root, dependency, _declaration_hash) = fixture().await;
        let target = cancellation_target("memory-invocation-2");
        let token = CancellationToken::new();
        dependency.invocations.lock().await.insert(
            target.invocation_id.clone(),
            ActiveInvocation {
                target: target.clone(),
                cancellation: token.clone(),
            },
        );
        let request = cancellation_request(
            target.clone(),
            "user_cancelled",
            "cancel-nonce-4",
            "cancel-key-4",
            "cancellation-5",
        );
        for mutate in 0_u8..7 {
            let mut substituted = request.clone();
            match mutate {
                0 => substituted.target.plugin_id = String::from("other.plugin"),
                1 => substituted.target.plugin_version = String::from("2.0.0"),
                2 => substituted.target.session_id = String::from("session-2"),
                3 => substituted.target.run_id = String::from("run-2"),
                4 => substituted.target.invocation_digest = ContentHash::digest(b"other"),
                5 => substituted.target.declaration_hash = ContentHash::digest(b"other"),
                _ => substituted.target.request_hash = ContentHash::digest(b"other"),
            }
            assert_eq!(
                dependency.cancel_invocation(substituted).await,
                Err(PluginDependencyError::Authorization)
            );
        }
        let mut grant_mismatch = request;
        grant_mismatch.authorization.normalized_digest = ContentHash::digest(b"other").to_hex();
        assert_eq!(
            dependency.cancel_invocation(grant_mismatch).await,
            Err(PluginDependencyError::Authorization)
        );

        let mut independently_authorized_substitution = target;
        independently_authorized_substitution.request_hash = ContentHash::digest(b"other request");
        let substituted = cancellation_request(
            independently_authorized_substitution,
            "user_cancelled",
            "cancel-nonce-5",
            "cancel-key-5",
            "cancellation-6",
        );
        assert_eq!(
            dependency.cancel_invocation(substituted).await,
            Err(PluginDependencyError::CancellationTargetMismatch)
        );
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn disable_cancels_registered_invocations_and_rejects_future_work() {
        let (_root, dependency, _declaration_hash) = fixture().await;
        let target = cancellation_target("disable-in-flight");
        let token = CancellationToken::new();
        dependency.invocations.lock().await.insert(
            target.invocation_id.clone(),
            ActiveInvocation {
                target: target.clone(),
                cancellation: token.clone(),
            },
        );
        let audit = dependency
            .disable(DependencyStateChangeRequest {
                plugin_id: String::from("fixture.plugin"),
                plugin_version: String::from("1.0.0"),
                configuration_reference: configuration_reference(&json!({}))
                    .expect("configuration reference"),
                reason: None,
                authorization: authorization(
                    "plugin.disable",
                    &(
                        String::from("fixture.plugin"),
                        String::from("1.0.0"),
                        configuration_reference(&json!({})).expect("configuration reference"),
                    ),
                    "disable-call-1",
                    "disable-cancellation-1",
                ),
            })
            .await
            .expect("disable");
        assert!(token.is_cancelled());
        assert_eq!(audit.operation, "disable");
        assert_eq!(audit.outcome, "disabled");
        let plugin = dependency.entry("fixture.plugin").await.expect("plugin");
        assert_eq!(
            *plugin.status.read().await,
            DependencyPluginStatus::Disabled
        );
        let configuration = json!({});
        assert!(matches!(
            dependency
                .invoke_worker(
                    &plugin,
                    Some(cancellation_target("after-disable")),
                    &WorkerRequest::Initialize {
                        configuration: &configuration,
                        state_version: 1,
                    },
                )
                .await,
            Err(PluginDependencyError::Inactive)
        ));
        let configuration_reference =
            configuration_reference(&json!({})).expect("configuration reference");
        let enabled = dependency
            .enable(DependencyStateChangeRequest {
                plugin_id: String::from("fixture.plugin"),
                plugin_version: String::from("1.0.0"),
                configuration_reference,
                reason: None,
                authorization: authorization(
                    "plugin.enable",
                    &(
                        String::from("fixture.plugin"),
                        String::from("1.0.0"),
                        configuration_reference,
                    ),
                    "enable-call-1",
                    "enable-cancellation-1",
                ),
            })
            .await
            .expect("enable");
        assert_eq!(enabled.operation, "enable");
        assert_eq!(enabled.outcome, "active");
        assert_eq!(*plugin.status.read().await, DependencyPluginStatus::Active);
    }

    #[tokio::test]
    async fn quarantine_binds_reason_and_cancels_registered_invocations() {
        let (_root, dependency, _declaration_hash) = fixture().await;
        let target = cancellation_target("quarantine-in-flight");
        let token = CancellationToken::new();
        dependency.invocations.lock().await.insert(
            target.invocation_id.clone(),
            ActiveInvocation {
                target,
                cancellation: token.clone(),
            },
        );
        let reason = Some(String::from("integrity_failure"));
        let audit = dependency
            .quarantine(DependencyStateChangeRequest {
                plugin_id: String::from("fixture.plugin"),
                plugin_version: String::from("1.0.0"),
                configuration_reference: configuration_reference(&json!({}))
                    .expect("configuration reference"),
                reason: reason.clone(),
                authorization: authorization(
                    "plugin.quarantine",
                    &(
                        String::from("fixture.plugin"),
                        String::from("1.0.0"),
                        configuration_reference(&json!({})).expect("configuration reference"),
                        &reason,
                    ),
                    "quarantine-call-1",
                    "quarantine-cancellation-1",
                ),
            })
            .await
            .expect("quarantine");
        assert!(token.is_cancelled());
        assert_eq!(audit.operation, "quarantine");
        assert_eq!(audit.outcome, "integrity_failure");
        let plugin = dependency.entry("fixture.plugin").await.expect("plugin");
        assert_eq!(
            *plugin.status.read().await,
            DependencyPluginStatus::Quarantined
        );
        let configuration_reference =
            configuration_reference(&json!({})).expect("configuration reference");
        let unquarantined = dependency
            .unquarantine(DependencyStateChangeRequest {
                plugin_id: String::from("fixture.plugin"),
                plugin_version: String::from("1.0.0"),
                configuration_reference,
                reason: None,
                authorization: authorization(
                    "plugin.unquarantine",
                    &(
                        String::from("fixture.plugin"),
                        String::from("1.0.0"),
                        configuration_reference,
                    ),
                    "unquarantine-call-1",
                    "unquarantine-cancellation-1",
                ),
            })
            .await
            .expect("unquarantine");
        assert_eq!(unquarantined.operation, "unquarantine");
        assert_eq!(unquarantined.outcome, "active");
        assert_eq!(*plugin.status.read().await, DependencyPluginStatus::Active);
    }

    #[tokio::test]
    async fn operation_binding_without_node_matches_protocol_hash_and_grant_shape() {
        let (_root, dependency, declaration_hash) = fixture().await;
        let binding = DependencyOperationBinding {
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: format!("plugin-automatic-memory-write:{}", "ab".repeat(32)),
            operation_id: String::from("memory-write"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: None,
            declaration_hash,
            configuration_reference: configuration_reference(&json!({}))
                .expect("configuration reference"),
            request_hash: ContentHash::digest(b"request"),
            idempotency_key: format!("plugin-automatic-memory-write-once:{}", "cd".repeat(32)),
            attempt: 1,
        };
        let encoded = serde_json::to_value(&binding).expect("operation binding");
        assert!(
            encoded.get("node_id").is_none(),
            "protocol bindings omit an absent node_id rather than hashing null"
        );

        let provider_id = String::from("fixture.memory");
        let provider_version = String::from("1.0.0");
        let handler = String::from("write");
        let request = json!({"value": "remember"});
        let readable_state = json!({});
        let cancellation_id = String::from("automatic-memory-cancellation");
        let authorization = authorization(
            "plugin.memory.write.invoke",
            &(
                &binding,
                &provider_id,
                &provider_version,
                &handler,
                1_000_u64,
                DependencyOperationIdempotency::Idempotent,
                &request,
                &readable_state,
                &cancellation_id,
            ),
            "automatic-memory-call",
            &cancellation_id,
        );

        assert_eq!(
            authorize_memory_operation(
                &dependency,
                "plugin.memory.write.invoke",
                &binding,
                &provider_id,
                &provider_version,
                &handler,
                1_000,
                DependencyOperationIdempotency::Idempotent,
                &request,
                &readable_state,
                &authorization,
            )
            .await,
            Ok(())
        );
    }

    fn state_request(
        declaration_hash: ContentHash,
        idempotency_key: &str,
        prior_generation: u64,
        prior_state_hash: Option<ContentHash>,
        state: Value,
    ) -> DependencyPersistNodeStateRequest {
        let state_hash =
            ContentHash::digest(&serde_json::to_vec(&state).expect("bounded fixture state"));
        let invocation_digest = ContentHash::digest(b"invocation");
        let configuration_reference =
            configuration_reference(&json!({})).expect("configuration reference");
        let request_hash = ContentHash::digest(
            &serde_json::to_vec(&(
                "agentmod.plugin.node-state.persist.request.v1",
                "fixture.plugin",
                "plugin-node:invocation",
                invocation_digest,
                "fixture.executor",
                "1.0.0",
                declaration_hash,
                configuration_reference,
                "session",
                prior_generation,
                prior_state_hash,
                &state,
                state_hash,
                idempotency_key,
            ))
            .expect("request identity"),
        );
        let mut cancellation_target = cancellation_target("plugin-node:invocation");
        cancellation_target.operation_id = String::from("fixture.executor:state-write");
        cancellation_target.declaration_hash = declaration_hash;
        cancellation_target.request_hash = request_hash;
        cancellation_target.invocation_digest =
            invocation_identity_digest(&cancellation_target).expect("cancellation identity");
        let action_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                "session-1",
                "fixture.plugin",
                "plugin-node:invocation",
                invocation_digest,
                "fixture.executor",
                "1.0.0",
                declaration_hash,
                configuration_reference,
                DependencyPluginNodeStateScope::Session,
                prior_generation,
                prior_state_hash,
                state_hash,
                idempotency_key,
            ))
            .expect("action identity"),
        );
        let nonce = String::from("state-nonce-1");
        let cancellation_id = String::from("cancel-state-1");
        let authorization_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                &cancellation_target,
                action_digest,
                &nonce,
                &cancellation_id,
                idempotency_key,
            ))
            .expect("authorization identity"),
        );
        let auth = authorization(
            "plugin.node_executor.persist_state",
            &(
                &cancellation_target,
                action_digest,
                &nonce,
                &cancellation_id,
                idempotency_key,
            ),
            &format!("call-{}", Uuid::now_v7()),
            &cancellation_id,
        );
        DependencyPersistNodeStateRequest {
            cancellation_target,
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest,
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference,
            state_scope: DependencyPluginNodeStateScope::Session,
            prior_generation,
            prior_state_hash,
            state,
            state_hash,
            action_digest,
            authorization_digest,
            nonce,
            idempotency_key: idempotency_key.to_owned(),
            authorization: auth,
        }
    }

    fn state_read_request(
        declaration_hash: ContentHash,
        invocation_id: &str,
        idempotency_key: &str,
        generation: u64,
        state_hash: ContentHash,
    ) -> DependencyLoadNodeStateRequest {
        let invocation_digest = ContentHash::digest(invocation_id.as_bytes());
        let configuration_reference =
            configuration_reference(&json!({})).expect("configuration reference");
        let request_hash = ContentHash::digest(
            &serde_json::to_vec(&(
                "agentmod.plugin.node-state.load.request.v1",
                "fixture.plugin",
                invocation_id,
                invocation_digest,
                "fixture.executor",
                "1.0.0",
                declaration_hash,
                configuration_reference,
                "session",
                generation,
                state_hash,
                idempotency_key,
            ))
            .expect("read request identity"),
        );
        let mut cancellation_target = cancellation_target(invocation_id);
        cancellation_target.operation_id = String::from("fixture.executor:state-read");
        cancellation_target.declaration_hash = declaration_hash;
        cancellation_target.request_hash = request_hash;
        cancellation_target.invocation_digest =
            invocation_identity_digest(&cancellation_target).expect("cancellation identity");
        let action_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                "session-1",
                "fixture.plugin",
                invocation_id,
                invocation_digest,
                "fixture.executor",
                "1.0.0",
                declaration_hash,
                configuration_reference,
                DependencyPluginNodeStateScope::Session,
                generation,
                state_hash,
                idempotency_key,
            ))
            .expect("read action identity"),
        );
        let nonce = String::from("state-read-nonce-1");
        let cancellation_id = String::from("cancel-state-read-1");
        let authorization_digest = ContentHash::digest(
            &serde_json::to_vec(&(
                &cancellation_target,
                action_digest,
                &nonce,
                &cancellation_id,
                idempotency_key,
            ))
            .expect("read authorization identity"),
        );
        let auth = authorization(
            "plugin.node_executor.load_state",
            &(
                &cancellation_target,
                action_digest,
                &nonce,
                &cancellation_id,
                idempotency_key,
            ),
            idempotency_key,
            &cancellation_id,
        );
        DependencyLoadNodeStateRequest {
            cancellation_target,
            plugin_id: String::from("fixture.plugin"),
            invocation_id: invocation_id.to_owned(),
            invocation_digest,
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference,
            state_scope: DependencyPluginNodeStateScope::Session,
            expected_generation: generation,
            expected_state_hash: state_hash,
            action_digest,
            authorization_digest,
            nonce,
            idempotency_key: idempotency_key.to_owned(),
            authorization: auth,
        }
    }

    #[tokio::test]
    async fn durable_state_cas_replays_exactly_and_rejects_conflict_and_stale_generation() {
        let (_root, dependency, declaration_hash) = fixture().await;
        let request = state_request(declaration_hash, "write-1", 0, None, json!({"cursor": 1}));
        let first = dependency
            .persist_node_state(request.clone())
            .await
            .expect("first state generation");
        let mut replay = request.clone();
        replay.authorization =
            state_request(declaration_hash, "write-1", 0, None, json!({"cursor": 1})).authorization;
        let second = dependency
            .persist_node_state(replay)
            .await
            .expect("exact replay");
        assert!(second.replayed);
        assert_eq!(first.receipt_id, second.receipt_id);
        assert_eq!(first.receipt_digest, second.receipt_digest);

        let conflict = state_request(declaration_hash, "write-1", 0, None, json!({"cursor": 2}));
        assert_eq!(
            dependency.persist_node_state(conflict).await,
            Err(PluginDependencyError::StateConflict)
        );
        let stale = state_request(declaration_hash, "write-2", 0, None, json!({"cursor": 2}));
        assert_eq!(
            dependency.persist_node_state(stale).await,
            Err(PluginDependencyError::StaleStateGeneration)
        );
        let update = state_request(
            declaration_hash,
            "write-2",
            1,
            Some(first.state_hash),
            json!({"cursor": 2}),
        );
        let updated = dependency
            .persist_node_state(update)
            .await
            .expect("second state generation");
        assert_eq!(updated.generation, 2);
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one state-read test preserves the full replay, substitution, cancellation, and audit sequence"
    )]
    async fn session_state_read_binds_later_invocation_and_replays_without_exposing_audit_value() {
        let (_root, dependency, declaration_hash) = fixture().await;
        let state = json!({"cursor": 7, "private": "transport-only"});
        let write = dependency
            .persist_node_state(state_request(
                declaration_hash,
                "write-read-fixture",
                0,
                None,
                state.clone(),
            ))
            .await
            .expect("session state");
        let request = state_read_request(
            declaration_hash,
            "plugin-node:later-invocation",
            "read-1",
            write.generation,
            write.state_hash,
        );
        let first = dependency
            .load_node_state(request.clone())
            .await
            .expect("first read");
        assert_eq!(first.state, state);
        assert!(!first.receipt.replayed);
        let mut replay = request;
        replay.authorization = state_read_request(
            declaration_hash,
            "plugin-node:later-invocation",
            "read-1",
            write.generation,
            write.state_hash,
        )
        .authorization;
        let second = dependency
            .load_node_state(replay)
            .await
            .expect("exact read replay");
        assert_eq!(second.state, first.state);
        assert!(second.receipt.replayed);
        assert_eq!(second.receipt.receipt_digest, first.receipt.receipt_digest);

        let mismatched_read = state_read_request(
            declaration_hash,
            "plugin-node:stale-invocation",
            "read-stale",
            write.generation,
            ContentHash::digest(b"substituted"),
        );
        assert_eq!(
            dependency.load_node_state(mismatched_read).await,
            Err(PluginDependencyError::StaleStateGeneration)
        );
        let audits = dependency.audits().await;
        assert!(
            audits
                .iter()
                .all(|audit| !format!("{audit:?}").contains("transport-only"))
        );
    }

    #[tokio::test]
    async fn missing_observer_receipt_is_sealed_ambiguous_once_and_never_redispatched() {
        let (_root, dependency, _declaration_hash) = fixture().await;
        let request = DependencyObservationRequest {
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("observer-restart-1"),
            handler: String::from("observe:tool.execution_completed"),
            event_type: String::from("tool.execution_completed"),
            event: json!({"event_id":"event-1","sequence":1}),
            authorization: DependencyAuthorization {
                owner_id: String::from("owner"),
                session_id: String::from("session-1"),
                call_id: String::from("observe-call-1"),
                normalized_digest: String::from("digest"),
                grant: String::from("grant"),
                cancellation_id: String::from("observer-cancel-1"),
            },
        };
        let request_hash = observer_delivery_request_hash(&request).expect("request hash");
        assert_eq!(
            dependency
                .begin_observer_delivery(&request, request_hash)
                .await
                .expect("persist pending"),
            None
        );
        let terminal = dependency
            .begin_observer_delivery(&request, request_hash)
            .await
            .expect("seal missing receipt")
            .expect("ambiguous terminal");
        assert_eq!(
            terminal.status,
            Some(DependencyObserverDeliveryStatus::Ambiguous)
        );
        assert!(terminal.receipt_id.is_some());
        assert!(terminal.receipt_digest.is_some());
        let replay = dependency
            .begin_observer_delivery(&request, request_hash)
            .await
            .expect("exact terminal replay")
            .expect("terminal");
        assert_eq!(replay, terminal);

        let mut substituted = request;
        substituted.authorization.cancellation_id = String::from("substituted");
        assert_eq!(
            dependency
                .begin_observer_delivery(&substituted, request_hash)
                .await,
            Err(PluginDependencyError::StateConflict)
        );
    }

    fn memory_operation(handler: &str) -> DependencyOperationDeclaration {
        DependencyOperationDeclaration {
            handler: handler.to_owned(),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"object"}"#),
            timeout_ms: 100,
            failure_policy: String::from("retry"),
            max_attempts: 2,
            retry_backoff_ms: 5,
            idempotency: DependencyOperationIdempotency::Idempotent,
            tool_permissions: vec![String::from("memory.read")],
            network_permissions: Vec::new(),
            state_scope: String::from("session"),
            external_effects: false,
        }
    }

    #[test]
    fn nonempty_wire_v6_memory_declaration_bytes_are_exact() {
        let provider = DependencyMemoryProviderDeclaration {
            provider_id: String::from("fixture.memory"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            capabilities: vec![String::from("memory.retrieve")],
            retrieve: memory_operation("retrieve"),
            write: None,
        };
        let encoded = String::from_utf8(serde_json::to_vec(&provider).expect("provider bytes"))
            .expect("utf-8");
        assert_eq!(
            encoded,
            concat!(
                r#"{"provider_id":"fixture.memory","version":"1.0.0","runtime_api":"^1.0","#,
                r#""capabilities":["memory.retrieve"],"retrieve":{"handler":"retrieve","#,
                r#""input_schema":"{\"type\":\"object\"}","output_schema":"{\"type\":\"object\"}","#,
                r#""timeout_ms":100,"failure_policy":{"kind":"retry","max_attempts":2,"backoff_ms":5},"#,
                r#""idempotency":"idempotent","required_permissions":{"tools":["memory.read"],"network":[]},"#,
                r#""state_scope":"session","external_effects":false}}"#
            )
        );
    }

    #[test]
    fn isolated_memory_response_rejects_unknown_fields() {
        let response = serde_json::from_str::<WorkerResponse>(
            r#"{"result":"memory_retrieved","binding":{"plugin_id":"fixture.plugin","plugin_version":"1.0.0","invocation_id":"invoke-1","operation_id":"retrieve-1","session_id":"session-1","run_id":"run-1","declaration_hash":"1111111111111111111111111111111111111111111111111111111111111111","configuration_reference":"2222222222222222222222222222222222222222222222222222222222222222","request_hash":"3333333333333333333333333333333333333333333333333333333333333333","idempotency_key":"key-1","attempt":1},"provider_id":"fixture.memory","provider_version":"1.0.0","items":[],"undeclared":true}"#,
        );
        assert!(response.is_err());
    }
}
