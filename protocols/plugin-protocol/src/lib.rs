//! Versioned wire contracts between the runtime and isolated plugin hosts.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current plugin-host wire protocol.
pub const CURRENT_PROTOCOL_VERSION: u16 = 10;

/// Maximum complete plugin-host command or response frame.
pub const MAX_PLUGIN_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum one inline schema declaration.
pub const MAX_PLUGIN_SCHEMA_BYTES: usize = 64 * 1024;
/// Maximum one inline operation payload.
pub const MAX_PLUGIN_INLINE_VALUE_BYTES: usize = 512 * 1024;
/// Maximum returned memory items in one proposal.
pub const MAX_PLUGIN_MEMORY_ITEMS: usize = 256;
/// Maximum typed references in one operation payload.
pub const MAX_PLUGIN_REFERENCES: usize = 256;
const MAX_PLUGIN_CAPABILITIES: usize = 256;
const MAX_PLUGIN_PERMISSIONS: usize = 256;
const MAX_PLUGIN_ATTEMPTS: u8 = 16;
const MAX_PLUGIN_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;
// `protocol-support` signs at most 4 KiB of claims, hex encodes that payload,
// and appends the token version, separators, and a 64-byte hexadecimal MAC.
const MAX_PLUGIN_AUTHORIZATION_GRANT_BYTES: usize = 4096 * 2 + 80;

/// Stable fail-closed validation classes for plugin wire contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginContractViolation {
    /// A required stable identifier is empty, too long, or contains invalid bytes.
    InvalidIdentifier,
    /// An implementation version is not a complete semantic version.
    InvalidVersion,
    /// A runtime API requirement is not a semantic-version requirement.
    InvalidRuntimeApi,
    /// An inline JSON schema is missing, malformed, or too large.
    InvalidSchema,
    /// A timeout or attempt count is outside protocol bounds.
    InvalidExecutionBound,
    /// Capabilities or permissions exceed protocol bounds.
    ExcessiveDeclarationItems,
    /// A pure operation declared effects or unsafe ambiguous retry behavior.
    UnsafeRecoveryDeclaration,
    /// An inline value exceeds its transport bound.
    PayloadTooLarge,
    /// A collection contains too many memory items or typed references.
    CollectionTooLarge,
    /// A request or response content hash does not bind the transmitted value.
    ContentHashMismatch,
    /// The complete serialized frame exceeds the plugin-host limit.
    FrameTooLarge,
    /// The response is not valid strict protocol JSON.
    MalformedResponse,
}

impl std::fmt::Display for PluginContractViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentifier => "invalid stable identifier",
            Self::InvalidVersion => "invalid complete semantic version",
            Self::InvalidRuntimeApi => "invalid runtime API requirement",
            Self::InvalidSchema => "invalid or oversized inline schema",
            Self::InvalidExecutionBound => "invalid timeout or attempt bound",
            Self::ExcessiveDeclarationItems => "too many declaration items",
            Self::UnsafeRecoveryDeclaration => "unsafe recovery declaration",
            Self::PayloadTooLarge => "inline payload exceeds protocol bound",
            Self::CollectionTooLarge => "collection exceeds protocol bound",
            Self::ContentHashMismatch => "content hash does not bind transmitted value",
            Self::FrameTooLarge => "plugin frame exceeds protocol bound",
            Self::MalformedResponse => "malformed plugin response",
        })
    }
}

impl std::error::Error for PluginContractViolation {}

/// Recovery declaration shared by memory and compaction operations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginOperationIdempotency {
    /// The exact invocation may be repeated with the same immutable identity.
    Idempotent,
    /// An ambiguous invocation must not be automatically repeated.
    NonIdempotent,
}

/// Failure policy in an exact memory or compaction declaration.
///
/// Its canonical serialized shape intentionally matches the plugin SDK
/// `FailurePolicy`; the protocol owns this boundary DTO while preserving the
/// one authoritative declaration hash.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum PluginOperationFailurePolicy {
    /// Reject the blocked operation.
    Reject,
    /// Cancel the blocked operation.
    Cancel,
    /// Disable the failed plugin.
    Disable,
    /// Continue without a result.
    Continue,
    /// Retry within bounded attempts and timeout.
    Retry {
        /// Total attempts including the first.
        max_attempts: u8,
        /// Delay between attempts.
        backoff_ms: u64,
    },
}

impl PluginOperationFailurePolicy {
    const fn max_attempts(&self) -> u8 {
        match self {
            Self::Retry { max_attempts, .. } => *max_attempts,
            Self::Reject | Self::Cancel | Self::Disable | Self::Continue => 1,
        }
    }
}

/// Required permissions in an exact memory or compaction declaration.
///
/// Vector order is part of the immutable SDK declaration identity.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginOperationPermissions {
    /// Stable tool or tool-group permission names.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Exact domains or wildcard subdomain patterns.
    #[serde(default)]
    pub network: Vec<String>,
}

/// State scope in an exact memory or compaction declaration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginOperationStateScope {
    /// One invocation.
    Invocation,
    /// One model call.
    ModelCall,
    /// One turn.
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

/// Exact isolated memory retrieval declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryRetrieveDeclaration {
    /// Stable isolated worker handler.
    pub handler: String,
    /// Bounded runtime-owned request schema.
    pub input_schema: String,
    /// Bounded proposed-memory collection schema.
    pub output_schema: String,
    /// Exact operation timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: PluginOperationFailurePolicy,
    /// Recovery declaration. Retrieval must be idempotent.
    pub idempotency: PluginOperationIdempotency,
    /// Required permissions.
    pub required_permissions: PluginOperationPermissions,
    /// Maximum readable state scope.
    pub state_scope: PluginOperationStateScope,
    /// Retrieval is pure and therefore must be false.
    pub external_effects: bool,
}

/// Exact isolated memory write declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryWriteDeclaration {
    /// Stable isolated worker handler.
    pub handler: String,
    /// Bounded approved write-request schema.
    pub input_schema: String,
    /// Bounded terminal provider-receipt schema.
    pub output_schema: String,
    /// Exact operation timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: PluginOperationFailurePolicy,
    /// Recovery declaration.
    pub idempotency: PluginOperationIdempotency,
    /// Required permissions.
    pub required_permissions: PluginOperationPermissions,
    /// Maximum readable state scope.
    pub state_scope: PluginOperationStateScope,
    /// Whether the approved write can perform external effects.
    pub external_effects: bool,
}

/// Exact plugin-provided memory implementation on the wire.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryProviderDeclaration {
    /// Stable provider implementation ID.
    pub provider_id: String,
    /// Exact provider semantic version.
    pub version: String,
    /// Provider runtime API requirement.
    pub runtime_api: String,
    /// Resolved provider capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Required pure retrieval operation.
    pub retrieve: PluginMemoryRetrieveDeclaration,
    /// Optional explicitly declared consequential write operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<PluginMemoryWriteDeclaration>,
}

impl PluginMemoryProviderDeclaration {
    /// Returns deterministic complete declaration bytes.
    ///
    /// Struct field order and every set's sorted order are stable. No
    /// configuration or live host state participates.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn declaration_hash_input(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Hashes the complete authoritative declaration.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn declaration_hash(&self) -> Result<ContentHash, serde_json::Error> {
        self.declaration_hash_input()
            .map(|bytes| ContentHash::digest(&bytes))
    }

    /// Validates stable identity, schemas, bounds, and recovery semantics.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed declarations.
    pub fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.provider_id)?;
        validate_version(&self.version)?;
        validate_runtime_api(&self.runtime_api)?;
        validate_declaration_collection(self.capabilities.len(), MAX_PLUGIN_CAPABILITIES)?;
        validate_operation(
            &self.retrieve.handler,
            &self.retrieve.input_schema,
            &self.retrieve.output_schema,
            self.retrieve.timeout_ms,
            &self.retrieve.failure_policy,
            &self.retrieve.required_permissions,
        )?;
        if self.retrieve.external_effects
            || self.retrieve.idempotency != PluginOperationIdempotency::Idempotent
        {
            return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
        }
        if let Some(write) = &self.write {
            validate_operation(
                &write.handler,
                &write.input_schema,
                &write.output_schema,
                write.timeout_ms,
                &write.failure_policy,
                &write.required_permissions,
            )?;
            if write.idempotency == PluginOperationIdempotency::NonIdempotent
                && write.failure_policy.max_attempts() != 1
            {
                return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
            }
        }
        Ok(())
    }
}

/// Exact plugin-provided provider-projection compactor on the wire.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompactorDeclaration {
    /// Stable compactor implementation ID.
    pub compactor_id: String,
    /// Exact compactor semantic version.
    pub version: String,
    /// Compactor runtime API requirement.
    pub runtime_api: String,
    /// Stable isolated worker handler.
    pub handler: String,
    /// Resolved compactor capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Bounded canonical projection input schema.
    pub input_schema: String,
    /// Bounded replacement proposal schema.
    pub output_schema: String,
    /// Exact operation timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: PluginOperationFailurePolicy,
    /// Recovery declaration. Compaction must be idempotent.
    pub idempotency: PluginOperationIdempotency,
    /// Required permissions.
    pub required_permissions: PluginOperationPermissions,
    /// Maximum readable state scope.
    pub state_scope: PluginOperationStateScope,
    /// Compaction is pure and therefore must be false.
    pub external_effects: bool,
}

impl PluginCompactorDeclaration {
    /// Returns deterministic complete declaration bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn declaration_hash_input(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Hashes the complete authoritative declaration.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn declaration_hash(&self) -> Result<ContentHash, serde_json::Error> {
        self.declaration_hash_input()
            .map(|bytes| ContentHash::digest(&bytes))
    }

    /// Validates stable identity, schemas, bounds, and pure recovery semantics.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed declarations.
    pub fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.compactor_id)?;
        validate_version(&self.version)?;
        validate_runtime_api(&self.runtime_api)?;
        validate_declaration_collection(self.capabilities.len(), MAX_PLUGIN_CAPABILITIES)?;
        validate_operation(
            &self.handler,
            &self.input_schema,
            &self.output_schema,
            self.timeout_ms,
            &self.failure_policy,
            &self.required_permissions,
        )?;
        if self.external_effects || self.idempotency != PluginOperationIdempotency::Idempotent {
            return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
        }
        Ok(())
    }
}

/// Lifecycle boundary supported by an isolated context transform.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformLifecycle {
    /// Transform the provider projection immediately before a model request.
    BeforeModelRequest,
}

/// Recovery declaration for an isolated context transform.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTransformIdempotency {
    /// The exact pure invocation may be safely repeated.
    Idempotent,
    /// An ambiguous invocation must not be automatically repeated.
    NonIdempotent,
}

/// Exact plugin-provided context-transform registration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginContextTransformDeclaration {
    /// Stable transform implementation ID.
    pub transform_id: String,
    /// Exact transform semantic version.
    pub version: String,
    /// Transform runtime API requirement.
    pub runtime_api: String,
    /// Isolated worker handler.
    pub handler: String,
    /// Exact lifecycle boundary.
    pub lifecycle: ContextTransformLifecycle,
    /// Resolved transform capabilities.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Bounded inline input JSON Schema.
    pub input_schema: String,
    /// Bounded inline proposal JSON Schema.
    pub output_schema: String,
    /// Per-invocation timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: String,
    /// Total attempts for retry policy.
    #[serde(default = "one")]
    pub max_attempts: u8,
    /// Delay between retry attempts.
    #[serde(default)]
    pub retry_backoff_ms: u64,
    /// Whether ambiguous execution may be retried.
    pub idempotency: ContextTransformIdempotency,
    /// Required tool permissions.
    #[serde(default)]
    pub tool_permissions: BTreeSet<String>,
    /// Required network permissions.
    #[serde(default)]
    pub network_permissions: BTreeSet<String>,
    /// Maximum readable state scope.
    pub state_scope: String,
    /// Whether external effects are possible.
    pub external_effects: bool,
}

/// Non-authoritative provider-projection proposal returned by a context transform.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginContextTransformProposal {
    /// Bounded typed replacement proposal; runtime logic remains authoritative.
    pub replacement: Value,
}

/// Immutable identity and request binding shared by plugin memory operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginOperationBinding {
    /// Exact allowed plugin ID.
    pub plugin_id: String,
    /// Exact allowed plugin semantic version.
    pub plugin_version: String,
    /// Unique invocation identity.
    pub invocation_id: String,
    /// Stable runtime-owned operation identity.
    pub operation_id: String,
    /// Exact session context.
    pub session_id: String,
    /// Exact graph/style run context.
    pub run_id: String,
    /// Exact graph node, when invoked by a node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Hash of the exact authoritative implementation declaration.
    pub declaration_hash: ContentHash,
    /// Hash of the exact immutable adapter configuration.
    pub configuration_reference: ContentHash,
    /// Hash of the typed request carried beside this binding.
    pub request_hash: ContentHash,
    /// Stable exact-request idempotency identity.
    pub idempotency_key: String,
    /// One-based invocation attempt.
    pub attempt: u8,
}

impl PluginOperationBinding {
    /// Returns deterministic binding bytes for action-digest construction.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Hashes the complete immutable operation binding.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn canonical_hash(&self) -> Result<ContentHash, serde_json::Error> {
        self.canonical_bytes()
            .map(|bytes| ContentHash::digest(&bytes))
    }

    fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.plugin_id)?;
        validate_version(&self.plugin_version)?;
        validate_identifier(&self.invocation_id)?;
        validate_identifier(&self.operation_id)?;
        validate_identifier(&self.session_id)?;
        validate_identifier(&self.run_id)?;
        if let Some(node_id) = &self.node_id {
            validate_identifier(node_id)?;
        }
        validate_identifier(&self.idempotency_key)?;
        if self.attempt == 0 || self.attempt > MAX_PLUGIN_ATTEMPTS {
            return Err(PluginContractViolation::InvalidExecutionBound);
        }
        Ok(())
    }
}

/// Canonical runtime reference kind visible to an isolated plugin operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginCanonicalReferenceKind {
    /// Immutable artifact.
    Artifact,
    /// Canonical node result.
    NodeResult,
    /// Canonical tool result.
    ToolResult,
    /// Canonical approval result.
    ApprovalResult,
    /// Durable continuation.
    Continuation,
    /// Child session.
    ChildSession,
}

/// Typed canonical reference; plugins cannot manufacture its envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCanonicalReference {
    /// Reference kind.
    pub kind: PluginCanonicalReferenceKind,
    /// Opaque runtime-owned identity.
    pub id: String,
    /// Exact referenced content hash when the kind has immutable content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<ContentHash>,
}

/// Bounded immutable artifact reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginArtifactReference {
    /// Opaque runtime artifact ID.
    pub artifact_id: String,
    /// Hash of immutable artifact bytes.
    pub content_hash: ContentHash,
    /// Declared media type.
    pub media_type: String,
    /// Exact artifact size.
    pub size_bytes: u64,
    /// Runtime-owned security classification.
    pub security_classification: PluginSecurityClassification,
}

/// Security classification carried on plugin memory values and artifacts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginSecurityClassification {
    /// Public data.
    Public,
    /// Runtime-internal data.
    Internal,
    /// User-private data.
    Private,
    /// Confidential data requiring explicit handling.
    Confidential,
}

/// Runtime-owned memory scope.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginMemoryScope {
    /// Current session.
    Session,
    /// Current project.
    Project,
    /// Current user.
    User,
    /// Entire runtime installation.
    Runtime,
}

/// Bounded typed memory retrieval request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryRetrieveRequest {
    /// Deterministically constructed runtime-owned query.
    pub query: String,
    /// Allowed retrieval scopes.
    pub scopes: BTreeSet<PluginMemoryScope>,
    /// Maximum returned item count.
    pub max_items: u32,
    /// Maximum total inline returned bytes.
    pub max_bytes: u64,
    /// Typed active artifacts exposed by policy.
    #[serde(default)]
    pub artifacts: Vec<PluginArtifactReference>,
    /// Other canonical references exposed by policy.
    #[serde(default)]
    pub references: Vec<PluginCanonicalReference>,
    /// Schema-validated provider-specific parameters.
    pub parameters: Value,
}

impl PluginMemoryRetrieveRequest {
    /// Hashes the exact typed request.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn content_hash(&self) -> Result<ContentHash, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| ContentHash::digest(&bytes))
    }

    fn validate(&self) -> Result<(), PluginContractViolation> {
        if self.query.len() > MAX_PLUGIN_INLINE_VALUE_BYTES
            || self.max_items == 0
            || usize::try_from(self.max_items).map_or(true, |count| count > MAX_PLUGIN_MEMORY_ITEMS)
            || self.max_bytes == 0
            || self.max_bytes > MAX_PLUGIN_INLINE_VALUE_BYTES as u64
        {
            return Err(PluginContractViolation::InvalidExecutionBound);
        }
        validate_references(&self.artifacts, &self.references)?;
        validate_inline_value(&self.parameters)
    }
}

/// One non-authoritative retrieved memory item proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryItemProposal {
    /// Provider-owned stable item ID.
    pub item_id: String,
    /// Proposed scope.
    pub scope: PluginMemoryScope,
    /// Bounded schema-validated item value.
    pub value: Value,
    /// Hash of `value`.
    pub value_hash: ContentHash,
    /// Runtime-readable artifact references proposed with the item.
    #[serde(default)]
    pub artifacts: Vec<PluginArtifactReference>,
    /// Other canonical references proposed with the item.
    #[serde(default)]
    pub references: Vec<PluginCanonicalReference>,
    /// Security classification which runtime policy may only strengthen.
    pub security_classification: PluginSecurityClassification,
    /// Bounded non-secret display metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Non-authoritative result of one plugin memory retrieval.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryRetrieveProposal {
    /// Exact invocation identity echoed by the isolated worker.
    pub binding: PluginOperationBinding,
    /// Exact provider implementation ID.
    pub provider_id: String,
    /// Exact provider implementation version.
    pub provider_version: String,
    /// Proposed memory items subject to runtime schema/policy validation.
    pub items: Vec<PluginMemoryItemProposal>,
}

impl PluginMemoryRetrieveProposal {
    fn validate(&self) -> Result<(), PluginContractViolation> {
        self.binding.validate()?;
        validate_identifier(&self.provider_id)?;
        validate_version(&self.provider_version)?;
        if self.items.len() > MAX_PLUGIN_MEMORY_ITEMS {
            return Err(PluginContractViolation::CollectionTooLarge);
        }
        for item in &self.items {
            validate_identifier(&item.item_id)?;
            validate_inline_value(&item.value)?;
            validate_value_hash(&item.value, item.value_hash)?;
            validate_references(&item.artifacts, &item.references)?;
            validate_metadata(&item.metadata)?;
        }
        Ok(())
    }
}

/// Runtime-owned memory write boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginMemoryWriteBoundary {
    /// Explicit graph or client request.
    Explicit,
    /// Successful user-turn completion.
    TurnCompletion,
    /// Successful bounded-iteration completion.
    IterationCompletion,
    /// Successful session completion.
    SessionCompletion,
}

/// Bounded approved memory write request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryWriteRequest {
    /// Runtime-approved scope.
    pub scope: PluginMemoryScope,
    /// Exact lifecycle boundary authorizing the write.
    pub boundary: PluginMemoryWriteBoundary,
    /// Bounded schema-validated content.
    pub value: Value,
    /// Hash of `value`.
    pub value_hash: ContentHash,
    /// Runtime-approved artifact references.
    #[serde(default)]
    pub artifacts: Vec<PluginArtifactReference>,
    /// Other runtime-approved canonical references.
    #[serde(default)]
    pub references: Vec<PluginCanonicalReference>,
    /// Runtime-owned security classification.
    pub security_classification: PluginSecurityClassification,
    /// Bounded non-secret provider parameters.
    pub parameters: Value,
}

impl PluginMemoryWriteRequest {
    /// Hashes the exact typed request.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn content_hash(&self) -> Result<ContentHash, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| ContentHash::digest(&bytes))
    }

    fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_inline_value(&self.value)?;
        validate_value_hash(&self.value, self.value_hash)?;
        validate_references(&self.artifacts, &self.references)?;
        validate_inline_value(&self.parameters)
    }
}

/// Non-authoritative provider receipt for an approved memory write.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMemoryWriteReceiptProposal {
    /// Exact invocation identity echoed by the isolated worker.
    pub binding: PluginOperationBinding,
    /// Exact provider implementation ID.
    pub provider_id: String,
    /// Exact provider implementation version.
    pub provider_version: String,
    /// Provider-owned terminal record identity.
    pub provider_record_id: String,
    /// Hash of the exact approved value accepted by the provider.
    pub value_hash: ContentHash,
    /// Provider receipt details validated against the output schema.
    pub receipt: Value,
}

impl PluginMemoryWriteReceiptProposal {
    fn validate(&self) -> Result<(), PluginContractViolation> {
        self.binding.validate()?;
        validate_identifier(&self.provider_id)?;
        validate_version(&self.provider_version)?;
        validate_identifier(&self.provider_record_id)?;
        validate_inline_value(&self.receipt)
    }
}

/// Bounded provider-projection compaction request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompactionRequest {
    /// Canonical provider projection to compact.
    pub projection: Value,
    /// Hash of `projection`.
    pub projection_hash: ContentHash,
    /// Runtime-owned records which the replacement must preserve.
    pub required_references: Vec<PluginCanonicalReference>,
    /// Runtime-owned artifacts which the replacement must preserve.
    pub required_artifacts: Vec<PluginArtifactReference>,
    /// Stable preservation requirement names from the immutable style.
    pub preservation_requirements: BTreeSet<String>,
    /// Hard maximum replacement bytes.
    pub max_replacement_bytes: u64,
    /// Hard maximum provider projection tokens.
    pub max_projection_tokens: u64,
    /// Schema-validated compactor parameters.
    pub parameters: Value,
}

impl PluginCompactionRequest {
    /// Hashes the exact typed request.
    ///
    /// # Errors
    ///
    /// Returns a serialization error only if JSON encoding fails.
    pub fn content_hash(&self) -> Result<ContentHash, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| ContentHash::digest(&bytes))
    }

    fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_inline_value(&self.projection)?;
        validate_value_hash(&self.projection, self.projection_hash)?;
        validate_references(&self.required_artifacts, &self.required_references)?;
        validate_declaration_collection(
            self.preservation_requirements.len(),
            MAX_PLUGIN_REFERENCES,
        )?;
        if self.max_replacement_bytes == 0
            || self.max_replacement_bytes > MAX_PLUGIN_INLINE_VALUE_BYTES as u64
            || self.max_projection_tokens == 0
        {
            return Err(PluginContractViolation::InvalidExecutionBound);
        }
        validate_inline_value(&self.parameters)
    }
}

/// Non-authoritative provider-projection compaction proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginCompactionProposal {
    /// Exact invocation identity echoed by the isolated worker.
    pub binding: PluginOperationBinding,
    /// Exact compactor implementation ID.
    pub compactor_id: String,
    /// Exact compactor implementation version.
    pub compactor_version: String,
    /// Proposed bounded provider projection.
    pub replacement: Value,
    /// Hash of `replacement`.
    pub replacement_hash: ContentHash,
    /// References explicitly preserved by the proposal.
    pub preserved_references: Vec<PluginCanonicalReference>,
    /// Artifacts explicitly preserved by the proposal.
    pub preserved_artifacts: Vec<PluginArtifactReference>,
}

impl PluginCompactionProposal {
    fn validate(&self) -> Result<(), PluginContractViolation> {
        self.binding.validate()?;
        validate_identifier(&self.compactor_id)?;
        validate_version(&self.compactor_version)?;
        validate_inline_value(&self.replacement)?;
        validate_value_hash(&self.replacement, self.replacement_hash)?;
        validate_references(&self.preserved_artifacts, &self.preserved_references)
    }
}

/// Recovery declaration for an isolated node executor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutorIdempotency {
    /// The exact invocation may be safely repeated.
    Idempotent,
    /// An ambiguous invocation must not be automatically repeated.
    NonIdempotent,
}

/// Exact plugin-provided node-executor registration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeExecutorDeclaration {
    /// Stable executor implementation ID.
    pub executor_id: String,
    /// Exact executor semantic version.
    pub version: String,
    /// Executor runtime API requirement.
    pub runtime_api: String,
    /// Serialized graph node kind.
    pub node_kind: String,
    /// Isolated worker handler.
    pub handler: String,
    /// Resolved executor capabilities.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Bounded inline input JSON Schema.
    pub input_schema: String,
    /// Bounded inline outcome JSON Schema.
    pub output_schema: String,
    /// Per-invocation timeout.
    pub timeout_ms: u64,
    /// Failure policy.
    pub failure_policy: String,
    /// Total attempts for retry policy.
    #[serde(default = "one")]
    pub max_attempts: u8,
    /// Delay between retry attempts.
    #[serde(default)]
    pub retry_backoff_ms: u64,
    /// Whether ambiguous execution may be retried.
    pub idempotency: NodeExecutorIdempotency,
    /// Required tool permissions.
    #[serde(default)]
    pub tool_permissions: BTreeSet<String>,
    /// Required network permissions.
    #[serde(default)]
    pub network_permissions: BTreeSet<String>,
    /// Maximum readable state scope.
    pub state_scope: String,
    /// Whether consequential actions may be proposed.
    pub external_effects: bool,
}

/// Runtime-action proposal returned by a plugin node.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeActionProposal {
    /// Declared runtime action class.
    pub kind: String,
    /// Bounded typed action payload.
    pub payload: Value,
}

/// Non-authoritative plugin node result validated by runtime logic.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeOutcomeProposal {
    /// Bounded node output.
    pub output: Value,
    /// State the plugin requires the runtime to preserve for later invocation.
    pub preserved_state: Value,
    /// Consequential runtime actions proposed for normal policy processing.
    #[serde(default)]
    pub proposed_actions: Vec<PluginNodeActionProposal>,
}

/// Durable scope for runtime-validated plugin-node state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginNodeStateScope {
    /// One exact invocation.
    Invocation,
    /// One model call.
    ModelCall,
    /// One runtime turn.
    Turn,
    /// One session.
    Session,
    /// One project.
    Project,
    /// One user.
    User,
    /// One runtime installation.
    Runtime,
}

const fn node_state_scope_name(scope: PluginNodeStateScope) -> &'static str {
    match scope {
        PluginNodeStateScope::Invocation => "invocation",
        PluginNodeStateScope::ModelCall => "model_call",
        PluginNodeStateScope::Turn => "turn",
        PluginNodeStateScope::Session => "session",
        PluginNodeStateScope::Project => "project",
        PluginNodeStateScope::User => "user",
        PluginNodeStateScope::Runtime => "runtime",
    }
}

/// Terminal receipt for one durable plugin-node state CAS operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeStateReceipt {
    /// Exact plugin ID.
    pub plugin_id: String,
    /// Exact node invocation ID.
    pub invocation_id: String,
    /// Exact node invocation digest.
    pub invocation_digest: String,
    /// Exact executor ID.
    pub executor_id: String,
    /// Exact executor version.
    pub executor_version: String,
    /// Exact executor declaration digest.
    pub executor_declaration_hash: String,
    /// Declared state scope.
    pub state_scope: PluginNodeStateScope,
    /// Required predecessor generation.
    pub prior_generation: u64,
    /// Committed generation.
    pub generation: u64,
    /// Hash of the committed bounded state.
    pub state_hash: String,
    /// Exact runtime action digest.
    pub action_digest: String,
    /// Exact authorization digest inside the keyed grant.
    pub authorization_digest: String,
    /// Stable exact-request idempotency identity.
    pub idempotency_key: String,
    /// Stable terminal receipt ID.
    pub receipt_id: String,
    /// Digest of this receipt identity.
    pub receipt_digest: String,
    /// Whether this response reconciled an already committed exact request.
    pub replayed: bool,
}

/// Terminal receipt for one exact authenticated plugin-node state read.
///
/// The bounded state value is carried separately in the response and is never
/// part of this receipt or the canonical runtime journal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginNodeStateReadReceipt {
    /// Exact plugin ID.
    pub plugin_id: String,
    /// Exact invocation requesting the state.
    pub invocation_id: String,
    /// Exact invocation digest.
    pub invocation_digest: String,
    /// Exact executor ID.
    pub executor_id: String,
    /// Exact executor version.
    pub executor_version: String,
    /// Exact executor declaration digest.
    pub executor_declaration_hash: String,
    /// Declared state scope.
    pub state_scope: PluginNodeStateScope,
    /// Required and returned generation.
    pub generation: u64,
    /// Required and returned state hash.
    pub state_hash: String,
    /// Exact state-read action digest.
    pub action_digest: String,
    /// Digest covered by the keyed authorization grant.
    pub authorization_digest: String,
    /// Stable exact-request idempotency identity.
    pub idempotency_key: String,
    /// Stable terminal receipt ID.
    pub receipt_id: String,
    /// Digest of this receipt identity.
    pub receipt_digest: String,
    /// Whether this response reconciled an already completed exact read.
    pub replayed: bool,
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
    /// Blocking/observer/tool/extension class.
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
    /// Exact graph-node executors exported by this plugin.
    #[serde(default)]
    pub node_executors: Vec<PluginNodeExecutorDeclaration>,
    /// Exact context transforms exported by this plugin.
    #[serde(default)]
    pub context_transforms: Vec<PluginContextTransformDeclaration>,
    /// Exact memory providers exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memory_providers: Vec<PluginMemoryProviderDeclaration>,
    /// Exact provider-projection compactors exported by this plugin.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compactors: Vec<PluginCompactorDeclaration>,
}

const fn one() -> u8 {
    1
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

/// One correlated runtime-to-plugin-host request frame.
///
/// Correlation is transport routing metadata only. It grants no authority and
/// is never substituted for the operation-specific authorization contract.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginRequestFrame {
    /// Unique bounded identity for one outstanding request on this connection.
    pub correlation_id: String,
    /// Exact versioned plugin command.
    pub command: PluginCommand,
}

impl PluginRequestFrame {
    /// Validates correlation, the nested command, and the complete frame bound.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed correlation, invalid commands, or oversized
    /// complete frames.
    pub fn validate_contract(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.correlation_id)?;
        self.command.validate_contract()?;
        validate_frame_size(self)
    }
}

/// Complete immutable identity of one running plugin invocation.
///
/// Cancellation is deliberately bound to the original session, run,
/// implementation, declaration, and request. An invocation ID alone is never
/// sufficient authority to signal an isolated operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationCancellationTarget {
    /// Exact session that owns the invocation.
    pub session_id: String,
    /// Exact graph/style run that owns the invocation.
    pub run_id: String,
    /// Exact loaded plugin ID.
    pub plugin_id: String,
    /// Exact loaded plugin semantic version.
    pub plugin_version: String,
    /// Exact invocation ID.
    pub invocation_id: String,
    /// Digest of the complete original invocation identity.
    pub invocation_digest: ContentHash,
    /// Stable runtime-owned operation ID.
    pub operation_id: String,
    /// Hash of the exact immutable implementation declaration.
    pub declaration_hash: ContentHash,
    /// Hash of the exact typed invocation request.
    pub request_hash: ContentHash,
}

impl PluginInvocationCancellationTarget {
    fn validate(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.session_id)?;
        validate_identifier(&self.run_id)?;
        validate_identifier(&self.plugin_id)?;
        validate_version(&self.plugin_version)?;
        validate_identifier(&self.invocation_id)?;
        validate_identifier(&self.operation_id)?;
        if plugin_invocation_identity_digest(
            &self.session_id,
            &self.run_id,
            &self.plugin_id,
            &self.plugin_version,
            &self.invocation_id,
            &self.operation_id,
            self.declaration_hash,
            self.request_hash,
        )
        .map_err(|_| PluginContractViolation::MalformedResponse)?
            != self.invocation_digest
        {
            return Err(PluginContractViolation::ContentHashMismatch);
        }
        Ok(())
    }
}

/// Computes the complete domain-separated invocation identity used as a
/// cancellation target.
///
/// # Errors
///
/// Returns a serialization error only if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_invocation_identity_digest(
    session_id: &str,
    run_id: &str,
    plugin_id: &str,
    plugin_version: &str,
    invocation_id: &str,
    operation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> Result<ContentHash, serde_json::Error> {
    serde_json::to_vec(&(
        "agentmod.plugin.invocation.identity.v1",
        session_id,
        run_id,
        plugin_id,
        plugin_version,
        invocation_id,
        operation_id,
        declaration_hash,
        request_hash,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Hashes the complete normalized interceptor invocation body.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
pub fn plugin_interceptor_invocation_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    handler: &str,
    proposal_type: &str,
    proposal: &Value,
    readable_state: &Value,
) -> Result<ContentHash, serde_json::Error> {
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
}

/// Hashes the complete normalized node-executor invocation body.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_node_executor_invocation_request_hash(
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
) -> Result<ContentHash, serde_json::Error> {
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
}

/// Hashes the complete normalized context-transform invocation body.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_context_transform_invocation_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    transform_id: &str,
    transform_version: &str,
    lifecycle: &str,
    handler: &str,
    timeout_ms: u64,
    configuration_reference: ContentHash,
    input: &Value,
    readable_state: &Value,
) -> Result<ContentHash, serde_json::Error> {
    serde_json::to_vec(&(
        "agentmod.plugin.context-transform.request.v1",
        plugin_id,
        invocation_id,
        transform_id,
        transform_version,
        lifecycle,
        handler,
        timeout_ms,
        configuration_reference,
        input,
        readable_state,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Hashes the complete normalized plugin-node state-write body.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_node_state_persist_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    invocation_digest: ContentHash,
    executor_id: &str,
    executor_version: &str,
    executor_declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    state_scope: &str,
    prior_generation: u64,
    prior_state_hash: Option<ContentHash>,
    state: &Value,
    state_hash: ContentHash,
    idempotency_key: &str,
) -> Result<ContentHash, serde_json::Error> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.persist.request.v1",
        plugin_id,
        invocation_id,
        invocation_digest,
        executor_id,
        executor_version,
        executor_declaration_hash,
        configuration_reference,
        state_scope,
        prior_generation,
        prior_state_hash,
        state,
        state_hash,
        idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Hashes the complete normalized plugin-node state-read body.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_node_state_load_request_hash(
    plugin_id: &str,
    invocation_id: &str,
    invocation_digest: ContentHash,
    executor_id: &str,
    executor_version: &str,
    executor_declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    state_scope: &str,
    expected_generation: u64,
    expected_state_hash: ContentHash,
    idempotency_key: &str,
) -> Result<ContentHash, serde_json::Error> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.load.request.v1",
        plugin_id,
        invocation_id,
        invocation_digest,
        executor_id,
        executor_version,
        executor_declaration_hash,
        configuration_reference,
        state_scope,
        expected_generation,
        expected_state_hash,
        idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
}

#[derive(Serialize)]
struct HashedPluginArtifactReference<'a> {
    artifact_id: &'a str,
    content_hash: &'a ContentHash,
    media_type: &'a str,
    size_bytes: u64,
    security_classification: &'static str,
}

#[derive(Serialize)]
struct HashedPluginCanonicalReference<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<&'a ContentHash>,
}

#[derive(Serialize)]
struct HashedPluginMemoryWriteRequest<'a> {
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
    scope: &'static str,
    boundary: &'static str,
    value: &'a Value,
    value_hash: ContentHash,
    artifacts: Vec<HashedPluginArtifactReference<'a>>,
    references: Vec<HashedPluginCanonicalReference<'a>>,
    security_classification: &'static str,
    parameters: &'a Value,
    readable_state: &'a Value,
}

#[derive(Serialize)]
struct HashedPluginMemoryRetrieveRequest<'a> {
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
    scopes: &'a BTreeSet<PluginMemoryScope>,
    max_items: u32,
    max_bytes: u64,
    artifacts: Vec<HashedPluginArtifactReference<'a>>,
    references: Vec<HashedPluginCanonicalReference<'a>>,
    parameters: &'a Value,
    readable_state: &'a Value,
}

#[derive(Serialize)]
struct HashedPluginCompactionRequest<'a> {
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
    required_references: Vec<HashedPluginCanonicalReference<'a>>,
    required_artifacts: Vec<HashedPluginArtifactReference<'a>>,
    preservation_requirements: &'a BTreeSet<String>,
    max_replacement_bytes: u64,
    max_projection_tokens: u64,
    parameters: &'a Value,
    readable_state: &'a Value,
}

/// Hashes the complete normalized memory-retrieval operation contract.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_memory_retrieve_request_hash(
    binding: &PluginOperationBinding,
    provider_id: &str,
    provider_version: &str,
    handler: &str,
    timeout_ms: u64,
    idempotency: PluginOperationIdempotency,
    request: &PluginMemoryRetrieveRequest,
    readable_state: &Value,
) -> Result<ContentHash, serde_json::Error> {
    let artifacts = request
        .artifacts
        .iter()
        .map(hashed_plugin_artifact_reference)
        .collect();
    let references = request
        .references
        .iter()
        .map(hashed_plugin_canonical_reference)
        .collect();
    serde_json::to_vec(&HashedPluginMemoryRetrieveRequest {
        schema: "agentmod.plugin.memory-retrieve.request.v2",
        plugin_id: &binding.plugin_id,
        plugin_version: &binding.plugin_version,
        invocation_id: &binding.invocation_id,
        operation_id: &binding.operation_id,
        session_id: &binding.session_id,
        run_id: &binding.run_id,
        node_id: binding.node_id.as_deref(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        idempotency_key: &binding.idempotency_key,
        attempt: binding.attempt,
        provider_id,
        provider_version,
        handler,
        timeout_ms,
        idempotency: plugin_operation_idempotency_name(idempotency),
        query: &request.query,
        scopes: &request.scopes,
        max_items: request.max_items,
        max_bytes: request.max_bytes,
        artifacts,
        references,
        parameters: &request.parameters,
        readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Hashes the complete normalized context-compaction operation contract.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_compaction_request_hash(
    binding: &PluginOperationBinding,
    compactor_id: &str,
    compactor_version: &str,
    handler: &str,
    timeout_ms: u64,
    idempotency: PluginOperationIdempotency,
    request: &PluginCompactionRequest,
    readable_state: &Value,
) -> Result<ContentHash, serde_json::Error> {
    let required_references = request
        .required_references
        .iter()
        .map(hashed_plugin_canonical_reference)
        .collect();
    let required_artifacts = request
        .required_artifacts
        .iter()
        .map(hashed_plugin_artifact_reference)
        .collect();
    serde_json::to_vec(&HashedPluginCompactionRequest {
        schema: "agentmod.plugin.compaction.request.v2",
        plugin_id: &binding.plugin_id,
        plugin_version: &binding.plugin_version,
        invocation_id: &binding.invocation_id,
        operation_id: &binding.operation_id,
        session_id: &binding.session_id,
        run_id: &binding.run_id,
        node_id: binding.node_id.as_deref(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        idempotency_key: &binding.idempotency_key,
        attempt: binding.attempt,
        compactor_id,
        compactor_version,
        handler,
        timeout_ms,
        idempotency: plugin_operation_idempotency_name(idempotency),
        projection: &request.projection,
        projection_hash: request.projection_hash,
        required_references,
        required_artifacts,
        preservation_requirements: &request.preservation_requirements,
        max_replacement_bytes: request.max_replacement_bytes,
        max_projection_tokens: request.max_projection_tokens,
        parameters: &request.parameters,
        readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Hashes the complete normalized memory-write operation contract.
///
/// # Errors
///
/// Returns a serialization error if deterministic JSON encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_memory_write_request_hash(
    binding: &PluginOperationBinding,
    provider_id: &str,
    provider_version: &str,
    handler: &str,
    timeout_ms: u64,
    idempotency: PluginOperationIdempotency,
    request: &PluginMemoryWriteRequest,
    readable_state: &Value,
) -> Result<ContentHash, serde_json::Error> {
    let artifacts = request
        .artifacts
        .iter()
        .map(hashed_plugin_artifact_reference)
        .collect();
    let references = request
        .references
        .iter()
        .map(hashed_plugin_canonical_reference)
        .collect();
    serde_json::to_vec(&HashedPluginMemoryWriteRequest {
        schema: "agentmod.plugin.memory-write.request.v2",
        plugin_id: &binding.plugin_id,
        plugin_version: &binding.plugin_version,
        invocation_id: &binding.invocation_id,
        operation_id: &binding.operation_id,
        session_id: &binding.session_id,
        run_id: &binding.run_id,
        node_id: binding.node_id.as_deref(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        idempotency_key: &binding.idempotency_key,
        attempt: binding.attempt,
        provider_id,
        provider_version,
        handler,
        timeout_ms,
        idempotency: plugin_operation_idempotency_name(idempotency),
        scope: plugin_memory_scope_name(request.scope),
        boundary: plugin_memory_write_boundary_name(request.boundary),
        value: &request.value,
        value_hash: request.value_hash,
        artifacts,
        references,
        security_classification: plugin_security_classification_name(
            request.security_classification,
        ),
        parameters: &request.parameters,
        readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
}

fn hashed_plugin_artifact_reference(
    reference: &PluginArtifactReference,
) -> HashedPluginArtifactReference<'_> {
    HashedPluginArtifactReference {
        artifact_id: &reference.artifact_id,
        content_hash: &reference.content_hash,
        media_type: &reference.media_type,
        size_bytes: reference.size_bytes,
        security_classification: plugin_security_classification_name(
            reference.security_classification,
        ),
    }
}

fn hashed_plugin_canonical_reference(
    reference: &PluginCanonicalReference,
) -> HashedPluginCanonicalReference<'_> {
    HashedPluginCanonicalReference {
        kind: plugin_canonical_reference_kind_name(reference.kind),
        id: &reference.id,
        content_hash: reference.content_hash.as_ref(),
    }
}

const fn plugin_operation_idempotency_name(value: PluginOperationIdempotency) -> &'static str {
    match value {
        PluginOperationIdempotency::Idempotent => "idempotent",
        PluginOperationIdempotency::NonIdempotent => "non_idempotent",
    }
}

const fn plugin_memory_scope_name(value: PluginMemoryScope) -> &'static str {
    match value {
        PluginMemoryScope::Session => "session",
        PluginMemoryScope::Project => "project",
        PluginMemoryScope::User => "user",
        PluginMemoryScope::Runtime => "runtime",
    }
}

const fn plugin_memory_write_boundary_name(value: PluginMemoryWriteBoundary) -> &'static str {
    match value {
        PluginMemoryWriteBoundary::Explicit => "explicit",
        PluginMemoryWriteBoundary::TurnCompletion => "turn_completion",
        PluginMemoryWriteBoundary::IterationCompletion => "iteration_completion",
        PluginMemoryWriteBoundary::SessionCompletion => "session_completion",
    }
}

const fn plugin_security_classification_name(value: PluginSecurityClassification) -> &'static str {
    match value {
        PluginSecurityClassification::Public => "public",
        PluginSecurityClassification::Internal => "internal",
        PluginSecurityClassification::Private => "private",
        PluginSecurityClassification::Confidential => "confidential",
    }
}

const fn plugin_canonical_reference_kind_name(value: PluginCanonicalReferenceKind) -> &'static str {
    match value {
        PluginCanonicalReferenceKind::Artifact => "artifact",
        PluginCanonicalReferenceKind::NodeResult => "node_result",
        PluginCanonicalReferenceKind::ToolResult => "tool_result",
        PluginCanonicalReferenceKind::ApprovalResult => "approval_result",
        PluginCanonicalReferenceKind::Continuation => "continuation",
        PluginCanonicalReferenceKind::ChildSession => "child_session",
    }
}

/// Outcome of delivering one authenticated cancellation signal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginInvocationCancellationStatus {
    /// The exact active invocation was found and its cancellation token was
    /// signalled.
    Signalled,
    /// The exact invocation was no longer active when the signal was handled.
    ///
    /// This is only a terminal no-op for cancellation delivery. It is not a
    /// terminal receipt for the original invocation and does not make an
    /// ambiguous non-idempotent effect safe to retry.
    AlreadyTerminal,
}

/// Durable proof of cancellation-signal handling.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginInvocationCancellationReceipt {
    /// Complete target that was checked before signalling.
    pub target: PluginInvocationCancellationTarget,
    /// Stable, non-secret cancellation reason classification.
    pub reason_code: String,
    /// Exact domain-separated action digest authorized by the grant.
    pub action_digest: ContentHash,
    /// Explicit nonce bound by both the action and keyed grant.
    pub nonce: String,
    /// Stable idempotency identity for this cancellation action.
    pub idempotency_key: String,
    /// Opaque cancellation lineage ID from the authorization envelope.
    pub cancellation_id: String,
    /// Whether a signal was delivered or the invocation was already terminal.
    pub status: PluginInvocationCancellationStatus,
    /// Stable receipt identity.
    pub receipt_id: String,
    /// Digest of every receipt field except this digest.
    pub receipt_digest: ContentHash,
}

impl PluginInvocationCancellationReceipt {
    fn validate(&self) -> Result<(), PluginContractViolation> {
        self.target.validate()?;
        validate_identifier(&self.reason_code)?;
        validate_identifier(&self.nonce)?;
        validate_identifier(&self.idempotency_key)?;
        validate_identifier(&self.cancellation_id)?;
        validate_identifier(&self.receipt_id)?;
        if plugin_invocation_cancellation_action_digest(
            &self.target,
            &self.reason_code,
            &self.nonce,
            &self.idempotency_key,
            &self.cancellation_id,
        )
        .map_err(|_| PluginContractViolation::MalformedResponse)?
            != self.action_digest
            || plugin_invocation_cancellation_receipt_digest(self)
                .map_err(|_| PluginContractViolation::MalformedResponse)?
                != self.receipt_digest
        {
            return Err(PluginContractViolation::ContentHashMismatch);
        }
        Ok(())
    }
}

/// Computes the exact domain-separated cancellation action digest.
///
/// # Errors
///
/// Returns a serialization error only if deterministic JSON encoding fails.
pub fn plugin_invocation_cancellation_action_digest(
    target: &PluginInvocationCancellationTarget,
    reason_code: &str,
    nonce: &str,
    idempotency_key: &str,
    cancellation_id: &str,
) -> Result<ContentHash, serde_json::Error> {
    serde_json::to_vec(&(
        "agentmod.plugin.invocation.cancel.v1",
        target,
        reason_code,
        nonce,
        idempotency_key,
        cancellation_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
}

/// Computes the exact domain-separated cancellation receipt digest.
///
/// # Errors
///
/// Returns a serialization error only if deterministic JSON encoding fails.
pub fn plugin_invocation_cancellation_receipt_digest(
    receipt: &PluginInvocationCancellationReceipt,
) -> Result<ContentHash, serde_json::Error> {
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
}

/// Runtime/plugin-host command.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "command",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
        /// Complete exact identity used for authenticated cancellation.
        cancellation_target: PluginInvocationCancellationTarget,
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
    /// Invoke one exact registered graph-node executor.
    InvokeNodeExecutor {
        /// Complete exact identity used for authenticated cancellation.
        cancellation_target: PluginInvocationCancellationTarget,
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Exact executor implementation ID.
        executor_id: String,
        /// Exact executor implementation version.
        executor_version: String,
        /// Serialized node kind.
        node_kind: String,
        /// Stable isolated worker handler.
        handler: String,
        /// Exact declaration-selected operation timeout.
        timeout_ms: u64,
        /// Exact activated-plugin configuration selected by the style binding.
        configuration_reference: String,
        /// Bounded typed node input.
        input: Value,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization over the exact invocation tuple.
        authorization: PluginAuthorization,
    },
    /// Invoke one exact registered provider-projection context transform.
    InvokeContextTransform {
        /// Complete exact identity used for authenticated cancellation.
        cancellation_target: PluginInvocationCancellationTarget,
        /// Loaded plugin ID.
        plugin_id: String,
        /// Unique invocation ID.
        invocation_id: String,
        /// Exact transform implementation ID.
        transform_id: String,
        /// Exact transform implementation version.
        transform_version: String,
        /// Exact lifecycle boundary.
        lifecycle: ContextTransformLifecycle,
        /// Stable isolated worker handler.
        handler: String,
        /// Exact declaration-selected operation timeout.
        timeout_ms: u64,
        /// Exact activated-plugin configuration selected by the style binding.
        configuration_reference: String,
        /// Bounded typed transform input.
        input: Value,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization over the exact invocation tuple.
        authorization: PluginAuthorization,
    },
    /// Invoke one exact pure memory retrieval operation.
    InvokeMemoryRetrieve {
        /// Immutable invocation/request binding.
        binding: PluginOperationBinding,
        /// Exact provider implementation ID.
        provider_id: String,
        /// Exact provider implementation version.
        provider_version: String,
        /// Stable isolated worker handler.
        handler: String,
        /// Exact declaration-selected operation timeout.
        timeout_ms: u64,
        /// Declared recovery behavior.
        idempotency: PluginOperationIdempotency,
        /// Bounded typed retrieval request.
        request: PluginMemoryRetrieveRequest,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization over the complete normalized invocation.
        authorization: PluginAuthorization,
    },
    /// Invoke one exact approved memory write operation.
    InvokeMemoryWrite {
        /// Immutable invocation/request binding.
        binding: PluginOperationBinding,
        /// Exact provider implementation ID.
        provider_id: String,
        /// Exact provider implementation version.
        provider_version: String,
        /// Stable isolated worker handler.
        handler: String,
        /// Exact declaration-selected operation timeout.
        timeout_ms: u64,
        /// Declared recovery behavior.
        idempotency: PluginOperationIdempotency,
        /// Bounded typed approved write request.
        request: PluginMemoryWriteRequest,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization over the complete normalized invocation.
        authorization: PluginAuthorization,
    },
    /// Invoke one exact pure provider-projection compactor.
    InvokeCompaction {
        /// Immutable invocation/request binding.
        binding: PluginOperationBinding,
        /// Exact compactor implementation ID.
        compactor_id: String,
        /// Exact compactor implementation version.
        compactor_version: String,
        /// Stable isolated worker handler.
        handler: String,
        /// Exact declaration-selected operation timeout.
        timeout_ms: u64,
        /// Declared recovery behavior.
        idempotency: PluginOperationIdempotency,
        /// Bounded typed compaction request.
        request: PluginCompactionRequest,
        /// Explicitly scoped readable state.
        readable_state: Value,
        /// Authorization over the complete normalized invocation.
        authorization: PluginAuthorization,
    },
    /// Persist runtime-validated plugin-node state using compare-and-swap.
    PersistNodeState {
        /// Complete exact identity used for authenticated cancellation.
        cancellation_target: PluginInvocationCancellationTarget,
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact node invocation ID.
        invocation_id: String,
        /// Exact node invocation digest.
        invocation_digest: String,
        /// Exact executor ID.
        executor_id: String,
        /// Exact executor version.
        executor_version: String,
        /// Exact executor declaration digest.
        executor_declaration_hash: String,
        /// Hash of the exact immutable node adapter configuration.
        configuration_reference: String,
        /// Declared state scope.
        state_scope: PluginNodeStateScope,
        /// Required predecessor generation.
        prior_generation: u64,
        /// Required predecessor state hash, absent only for generation zero.
        prior_state_hash: Option<String>,
        /// Bounded state previously validated by runtime logic.
        state: Value,
        /// Hash of `state`.
        state_hash: String,
        /// Exact state-change action digest.
        action_digest: String,
        /// Digest covered by the keyed authorization grant.
        authorization_digest: String,
        /// Stable state-operation nonce.
        nonce: String,
        /// Stable exact-request idempotency identity.
        idempotency_key: String,
        /// Authorization over the action digest, nonce, cancellation, and
        /// idempotency identity.
        authorization: PluginAuthorization,
    },
    /// Loads one exact bounded plugin-node state generation.
    LoadNodeState {
        /// Complete exact identity used for authenticated cancellation.
        cancellation_target: PluginInvocationCancellationTarget,
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact invocation requesting the state.
        invocation_id: String,
        /// Exact invocation digest.
        invocation_digest: String,
        /// Exact executor ID.
        executor_id: String,
        /// Exact executor version.
        executor_version: String,
        /// Exact executor declaration digest.
        executor_declaration_hash: String,
        /// Hash of the exact immutable node adapter configuration.
        configuration_reference: String,
        /// Declared state scope.
        state_scope: PluginNodeStateScope,
        /// Required generation derived from canonical runtime state.
        expected_generation: u64,
        /// Required state hash derived from canonical runtime state.
        expected_state_hash: String,
        /// Exact state-read action digest.
        action_digest: String,
        /// Digest covered by the keyed authorization grant.
        authorization_digest: String,
        /// Stable state-read nonce.
        nonce: String,
        /// Stable exact-request idempotency identity.
        idempotency_key: String,
        /// Authorization over the exact read identity.
        authorization: PluginAuthorization,
    },
    /// Authenticated cancellation of one exact running plugin invocation.
    CancelInvocation {
        /// Complete immutable invocation identity.
        target: PluginInvocationCancellationTarget,
        /// Stable, non-secret reason classification.
        reason_code: String,
        /// Exact domain-separated cancellation action digest.
        action_digest: ContentHash,
        /// Explicit nonce also required in the keyed grant claims.
        nonce: String,
        /// Stable exact-action idempotency identity.
        idempotency_key: String,
        /// Authorization for `plugin.invocation.cancel`.
        authorization: PluginAuthorization,
    },
    /// Disable without deleting persisted state.
    Disable {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact immutable plugin version.
        plugin_version: String,
        /// Exact immutable configuration reference.
        configuration_reference: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Re-enable a disabled plugin without changing its immutable binding.
    Enable {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact immutable plugin version.
        plugin_version: String,
        /// Exact immutable configuration reference.
        configuration_reference: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Quarantine a plugin after a policy or crash finding.
    Quarantine {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact immutable plugin version.
        plugin_version: String,
        /// Exact immutable configuration reference.
        configuration_reference: String,
        /// Redacted reason code.
        reason_code: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Release a quarantined plugin after an explicit management decision.
    Unquarantine {
        /// Loaded plugin ID.
        plugin_id: String,
        /// Exact immutable plugin version.
        plugin_version: String,
        /// Exact immutable configuration reference.
        configuration_reference: String,
        /// Authorization.
        authorization: PluginAuthorization,
    },
    /// Report plugin-host health and bounded audit state.
    Health,
}

impl PluginCommand {
    /// Validates frame, payload, identity, declaration, and recovery bindings.
    ///
    /// Existing operations receive the common complete-frame bound. The three
    /// typed memory/compaction operations additionally receive strict semantic
    /// validation.
    ///
    /// # Errors
    ///
    /// Fails closed when any bound or immutable binding is invalid.
    #[allow(
        clippy::too_many_lines,
        reason = "one protocol gate validates every exact command variant at the service boundary"
    )]
    pub fn validate_contract(&self) -> Result<(), PluginContractViolation> {
        validate_frame_size(self)?;
        match self {
            Self::Intercept {
                cancellation_target,
                plugin_id,
                invocation_id,
                handler,
                proposal_type,
                proposal,
                readable_state,
                authorization,
            } => validate_authenticated_invocation_target(
                cancellation_target,
                plugin_id,
                invocation_id,
                handler,
                plugin_interceptor_invocation_request_hash(
                    plugin_id,
                    invocation_id,
                    handler,
                    proposal_type,
                    proposal,
                    readable_state,
                )
                .map_err(|_| PluginContractViolation::MalformedResponse)?,
                authorization,
            )?,
            Self::InvokeNodeExecutor {
                cancellation_target,
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
                authorization,
            } => validate_authenticated_invocation_target(
                cancellation_target,
                plugin_id,
                invocation_id,
                executor_id,
                plugin_node_executor_invocation_request_hash(
                    plugin_id,
                    invocation_id,
                    executor_id,
                    executor_version,
                    node_kind,
                    handler,
                    *timeout_ms,
                    ContentHash::from_str(configuration_reference)
                        .map_err(|_| PluginContractViolation::ContentHashMismatch)?,
                    input,
                    readable_state,
                )
                .map_err(|_| PluginContractViolation::MalformedResponse)?,
                authorization,
            )?,
            Self::InvokeContextTransform {
                cancellation_target,
                plugin_id,
                invocation_id,
                transform_id,
                transform_version,
                lifecycle,
                handler,
                timeout_ms,
                configuration_reference,
                input,
                readable_state,
                authorization,
            } => validate_authenticated_invocation_target(
                cancellation_target,
                plugin_id,
                invocation_id,
                transform_id,
                plugin_context_transform_invocation_request_hash(
                    plugin_id,
                    invocation_id,
                    transform_id,
                    transform_version,
                    match lifecycle {
                        ContextTransformLifecycle::BeforeModelRequest => "before_model_request",
                    },
                    handler,
                    *timeout_ms,
                    ContentHash::from_str(configuration_reference)
                        .map_err(|_| PluginContractViolation::ContentHashMismatch)?,
                    input,
                    readable_state,
                )
                .map_err(|_| PluginContractViolation::MalformedResponse)?,
                authorization,
            )?,
            Self::PersistNodeState {
                cancellation_target,
                plugin_id,
                invocation_id,
                invocation_digest,
                executor_id,
                executor_version,
                executor_declaration_hash,
                configuration_reference,
                state_scope,
                prior_generation,
                prior_state_hash,
                state,
                state_hash,
                action_digest,
                authorization_digest: _authorization_digest,
                nonce: _nonce,
                idempotency_key,
                authorization,
            } => {
                let state_hash = ContentHash::from_str(state_hash)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let executor_declaration_hash = ContentHash::from_str(executor_declaration_hash)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let invocation_digest = ContentHash::from_str(invocation_digest)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let _action_digest = ContentHash::from_str(action_digest)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let configuration_reference = ContentHash::from_str(configuration_reference)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                if ContentHash::digest(
                    &serde_json::to_vec(state)
                        .map_err(|_| PluginContractViolation::MalformedResponse)?,
                ) != state_hash
                    || cancellation_target.declaration_hash != executor_declaration_hash
                {
                    return Err(PluginContractViolation::ContentHashMismatch);
                }
                validate_authenticated_invocation_target(
                    cancellation_target,
                    plugin_id,
                    invocation_id,
                    &format!("{executor_id}:state-write"),
                    plugin_node_state_persist_request_hash(
                        plugin_id,
                        invocation_id,
                        invocation_digest,
                        executor_id,
                        executor_version,
                        executor_declaration_hash,
                        configuration_reference,
                        node_state_scope_name(*state_scope),
                        *prior_generation,
                        prior_state_hash
                            .as_deref()
                            .map(ContentHash::from_str)
                            .transpose()
                            .map_err(|_| PluginContractViolation::ContentHashMismatch)?,
                        state,
                        state_hash,
                        idempotency_key,
                    )
                    .map_err(|_| PluginContractViolation::MalformedResponse)?,
                    authorization,
                )?;
            }
            Self::LoadNodeState {
                cancellation_target,
                plugin_id,
                invocation_id,
                invocation_digest,
                executor_id,
                executor_version,
                executor_declaration_hash,
                configuration_reference,
                state_scope,
                expected_generation,
                expected_state_hash,
                action_digest,
                authorization_digest: _authorization_digest,
                nonce: _nonce,
                idempotency_key,
                authorization,
            } => {
                let executor_declaration_hash = ContentHash::from_str(executor_declaration_hash)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let expected_state_hash = ContentHash::from_str(expected_state_hash)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let invocation_digest = ContentHash::from_str(invocation_digest)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let _action_digest = ContentHash::from_str(action_digest)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                let configuration_reference = ContentHash::from_str(configuration_reference)
                    .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
                if cancellation_target.declaration_hash != executor_declaration_hash {
                    return Err(PluginContractViolation::ContentHashMismatch);
                }
                validate_authenticated_invocation_target(
                    cancellation_target,
                    plugin_id,
                    invocation_id,
                    &format!("{executor_id}:state-read"),
                    plugin_node_state_load_request_hash(
                        plugin_id,
                        invocation_id,
                        invocation_digest,
                        executor_id,
                        executor_version,
                        executor_declaration_hash,
                        configuration_reference,
                        node_state_scope_name(*state_scope),
                        *expected_generation,
                        expected_state_hash,
                        idempotency_key,
                    )
                    .map_err(|_| PluginContractViolation::MalformedResponse)?,
                    authorization,
                )?;
            }
            Self::InvokeMemoryRetrieve {
                binding,
                provider_id,
                provider_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                validate_invocation_common(
                    binding,
                    provider_id,
                    provider_version,
                    handler,
                    plugin_memory_retrieve_request_hash(
                        binding,
                        provider_id,
                        provider_version,
                        handler,
                        *timeout_ms,
                        *idempotency,
                        request,
                        readable_state,
                    )
                    .map_err(|_| PluginContractViolation::MalformedResponse)?,
                    readable_state,
                    authorization,
                )?;
                request.validate()?;
                if *idempotency != PluginOperationIdempotency::Idempotent {
                    return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
                }
            }
            Self::InvokeMemoryWrite {
                binding,
                provider_id,
                provider_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                validate_invocation_common(
                    binding,
                    provider_id,
                    provider_version,
                    handler,
                    plugin_memory_write_request_hash(
                        binding,
                        provider_id,
                        provider_version,
                        handler,
                        *timeout_ms,
                        *idempotency,
                        request,
                        readable_state,
                    )
                    .map_err(|_| PluginContractViolation::MalformedResponse)?,
                    readable_state,
                    authorization,
                )?;
                request.validate()?;
                if *idempotency == PluginOperationIdempotency::NonIdempotent && binding.attempt != 1
                {
                    return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
                }
            }
            Self::InvokeCompaction {
                binding,
                compactor_id,
                compactor_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                validate_invocation_common(
                    binding,
                    compactor_id,
                    compactor_version,
                    handler,
                    plugin_compaction_request_hash(
                        binding,
                        compactor_id,
                        compactor_version,
                        handler,
                        *timeout_ms,
                        *idempotency,
                        request,
                        readable_state,
                    )
                    .map_err(|_| PluginContractViolation::MalformedResponse)?,
                    readable_state,
                    authorization,
                )?;
                request.validate()?;
                if *idempotency != PluginOperationIdempotency::Idempotent {
                    return Err(PluginContractViolation::UnsafeRecoveryDeclaration);
                }
            }
            Self::CancelInvocation {
                target,
                reason_code,
                action_digest,
                nonce,
                idempotency_key,
                authorization,
            } => validate_cancel_invocation(
                target,
                reason_code,
                *action_digest,
                nonce,
                idempotency_key,
                authorization,
            )?,
            Self::Disable {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            }
            | Self::Enable {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            }
            | Self::Unquarantine {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            } => validate_lifecycle_command(
                plugin_id,
                plugin_version,
                configuration_reference,
                None,
                authorization,
            )?,
            Self::Quarantine {
                plugin_id,
                plugin_version,
                configuration_reference,
                reason_code,
                authorization,
            } => validate_lifecycle_command(
                plugin_id,
                plugin_version,
                configuration_reference,
                Some(reason_code),
                authorization,
            )?,
            _ => {}
        }
        Ok(())
    }
}

fn validate_lifecycle_command(
    plugin_id: &str,
    plugin_version: &str,
    configuration_reference: &str,
    reason_code: Option<&String>,
    authorization: &PluginAuthorization,
) -> Result<(), PluginContractViolation> {
    validate_identifier(plugin_id)?;
    validate_version(plugin_version)?;
    ContentHash::from_str(configuration_reference)
        .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
    if let Some(reason_code) = reason_code {
        validate_identifier(reason_code)?;
    }
    validate_identifier(&authorization.owner_id)?;
    validate_identifier(&authorization.session_id)?;
    validate_identifier(&authorization.call_id)?;
    ContentHash::from_str(&authorization.normalized_digest)
        .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
    validate_authorization_grant(&authorization.grant)?;
    validate_identifier(&authorization.cancellation_id)
}

fn validate_authenticated_invocation_target(
    target: &PluginInvocationCancellationTarget,
    plugin_id: &str,
    invocation_id: &str,
    operation_id: &str,
    request_hash: ContentHash,
    authorization: &PluginAuthorization,
) -> Result<(), PluginContractViolation> {
    target.validate()?;
    validate_identifier(plugin_id)?;
    validate_identifier(invocation_id)?;
    validate_identifier(operation_id)?;
    validate_identifier(&authorization.owner_id)?;
    validate_identifier(&authorization.session_id)?;
    validate_identifier(&authorization.call_id)?;
    validate_identifier(&authorization.cancellation_id)?;
    if target.plugin_id != plugin_id
        || target.invocation_id != invocation_id
        || target.operation_id != operation_id
        || target.request_hash != request_hash
        || target.session_id != authorization.session_id
        || authorization.normalized_digest.is_empty()
        || authorization.grant.is_empty()
        || authorization.grant.len() > MAX_PLUGIN_AUTHORIZATION_GRANT_BYTES
    {
        return Err(PluginContractViolation::ContentHashMismatch);
    }
    Ok(())
}

fn validate_cancel_invocation(
    target: &PluginInvocationCancellationTarget,
    reason_code: &str,
    action_digest: ContentHash,
    nonce: &str,
    idempotency_key: &str,
    authorization: &PluginAuthorization,
) -> Result<(), PluginContractViolation> {
    target.validate()?;
    validate_identifier(reason_code)?;
    validate_identifier(nonce)?;
    validate_identifier(idempotency_key)?;
    validate_identifier(&authorization.owner_id)?;
    validate_identifier(&authorization.session_id)?;
    validate_identifier(&authorization.call_id)?;
    validate_identifier(&authorization.cancellation_id)?;
    if authorization.session_id != target.session_id
        || authorization.normalized_digest != action_digest.to_hex()
        || authorization.grant.is_empty()
        || authorization.grant.len() > MAX_PLUGIN_AUTHORIZATION_GRANT_BYTES
        || plugin_invocation_cancellation_action_digest(
            target,
            reason_code,
            nonce,
            idempotency_key,
            &authorization.cancellation_id,
        )
        .map_err(|_| PluginContractViolation::MalformedResponse)?
            != action_digest
    {
        return Err(PluginContractViolation::ContentHashMismatch);
    }
    Ok(())
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
    /// Stable outcome code.
    pub outcome: String,
    /// Attempt count.
    pub attempts: u8,
}

/// Plugin-host response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "result",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
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
    /// Plugin node returned a non-authoritative outcome proposal.
    NodeOutcome {
        /// Runtime-validated outcome proposal.
        proposal: PluginNodeOutcomeProposal,
        /// Audit result.
        audit: PluginAudit,
    },
    /// A context transform returned a non-authoritative replacement proposal.
    ContextTransformProposal {
        /// Runtime-owned validation must accept this proposal before use.
        proposal: PluginContextTransformProposal,
        /// Audit result.
        audit: PluginAudit,
    },
    /// A memory provider returned non-authoritative retrieved items.
    MemoryRetrieved {
        /// Runtime logic must validate identity, schema, scope, and policy.
        proposal: PluginMemoryRetrieveProposal,
        /// Audit result.
        audit: PluginAudit,
    },
    /// A memory provider returned a non-authoritative terminal write receipt.
    MemoryWritten {
        /// Runtime logic must validate the receipt against the approved write.
        receipt: PluginMemoryWriteReceiptProposal,
        /// Audit result.
        audit: PluginAudit,
    },
    /// A compactor returned a non-authoritative replacement projection.
    CompactionProposed {
        /// Runtime logic must validate preservation and replacement bounds.
        proposal: PluginCompactionProposal,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin-node state was durably committed or exactly reconciled.
    NodeStatePersisted {
        /// Exact terminal persistence receipt.
        receipt: Box<PluginNodeStateReceipt>,
        /// Redacted audit result.
        audit: PluginAudit,
    },
    /// Exact bounded plugin-node state was loaded.
    NodeStateLoaded {
        /// Raw bounded state. This value is transport-only.
        state: Value,
        /// Exact terminal read receipt.
        receipt: Box<PluginNodeStateReadReceipt>,
        /// Redacted audit result.
        audit: PluginAudit,
    },
    /// An exact authenticated cancellation signal was handled.
    InvocationCancelled {
        /// Durable signal-only cancellation receipt.
        receipt: Box<PluginInvocationCancellationReceipt>,
    },
    /// Observation reached a durable terminal delivery classification.
    Observation {
        /// Whether it entered the queue.
        accepted: bool,
        /// Current bounded queue depth.
        queue_depth: usize,
        /// Total dropped events for this plugin.
        dropped: u64,
        /// Terminal delivery classification.
        status: PluginObserverDeliveryStatus,
        /// Exact request hash.
        request_hash: String,
        /// Stable terminal receipt identity.
        receipt_id: String,
        /// Digest of the exact terminal receipt.
        receipt_digest: String,
        /// Whether the host returned an already-persisted exact receipt.
        replayed: bool,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Plugin lifecycle state changed.
    StateChanged {
        /// Plugin ID.
        plugin_id: String,
        /// `active`, `disabled`, or `quarantined`.
        state: String,
        /// Audit result.
        audit: PluginAudit,
    },
    /// Health projection.
    Health {
        /// Loaded plugin count.
        loaded: usize,
        /// Running invocation count.
        running: usize,
        /// Queued or actively executing observer workers.
        observer_pending: u64,
        /// Observer drops.
        observer_dropped: u64,
        /// Whether durable host state is fully flushed and terminal.
        state_flushed: bool,
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

/// Terminal observer delivery classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginObserverDeliveryStatus {
    /// Isolated observer worker returned its declared terminal result.
    Completed,
    /// Bounded queue or policy rejected delivery before worker dispatch.
    Rejected,
    /// Worker failed definitely before an ambiguous effect boundary.
    Failed,
    /// Worker execution may have crossed its effect boundary without receipt.
    Ambiguous,
}

/// One correlated plugin-host-to-runtime response frame.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginResponseFrame {
    /// Exact request correlation identity.
    pub correlation_id: String,
    /// Exact versioned plugin response.
    pub response: PluginResponse,
}

impl PluginResponseFrame {
    /// Validates correlation, nested response semantics, and complete size.
    ///
    /// # Errors
    ///
    /// Fails closed on malformed correlation, invalid response content, or an
    /// oversized complete frame.
    pub fn validate_contract(&self) -> Result<(), PluginContractViolation> {
        validate_identifier(&self.correlation_id)?;
        self.response.validate_contract()?;
        validate_frame_size(self)
    }
}

impl PluginResponse {
    /// Validates the complete response frame and typed proposal payload.
    ///
    /// # Errors
    ///
    /// Fails closed when any response field or bound is invalid.
    pub fn validate_contract(&self) -> Result<(), PluginContractViolation> {
        validate_frame_size(self)?;
        match self {
            Self::MemoryRetrieved { proposal, audit } => {
                proposal.validate()?;
                validate_audit_binding(audit, &proposal.binding, "memory_retrieve")
            }
            Self::MemoryWritten { receipt, audit } => {
                receipt.validate()?;
                validate_audit_binding(audit, &receipt.binding, "memory_write")
            }
            Self::CompactionProposed { proposal, audit } => {
                proposal.validate()?;
                validate_audit_binding(audit, &proposal.binding, "compaction")
            }
            Self::InvocationCancelled { receipt } => receipt.validate(),
            Self::Observation {
                accepted,
                status,
                request_hash,
                receipt_id,
                receipt_digest,
                audit,
                ..
            } => {
                validate_identifier(receipt_id)?;
                validate_identifier(&audit.plugin_id)?;
                let invocation_id = audit
                    .invocation_id
                    .as_deref()
                    .ok_or(PluginContractViolation::InvalidIdentifier)?;
                validate_identifier(invocation_id)?;
                if audit.operation != "observe"
                    || audit.attempts > 1
                    || (*status == PluginObserverDeliveryStatus::Rejected && *accepted)
                    || observer_delivery_receipt_digest(
                        &audit.plugin_id,
                        invocation_id,
                        request_hash,
                        *status,
                        receipt_id,
                    )? != *receipt_digest
                {
                    return Err(PluginContractViolation::ContentHashMismatch);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Computes the exact domain-separated observer delivery receipt digest.
///
/// # Errors
///
/// Returns a serialization error only if deterministic encoding fails.
pub fn observer_delivery_receipt_digest(
    plugin_id: &str,
    invocation_id: &str,
    request_hash: &str,
    status: PluginObserverDeliveryStatus,
    receipt_id: &str,
) -> Result<String, PluginContractViolation> {
    let request_hash = ContentHash::from_str(request_hash)
        .map_err(|_| PluginContractViolation::ContentHashMismatch)?;
    serde_json::to_vec(&(
        "agentmod.plugin.observer.delivery.receipt.v1",
        plugin_id,
        invocation_id,
        request_hash,
        status,
        receipt_id,
    ))
    .map(|bytes| ContentHash::digest(&bytes).to_hex())
    .map_err(|_| PluginContractViolation::MalformedResponse)
}

/// Strictly decodes and validates one complete plugin response frame.
///
/// This helper is deliberately separate from transport framing: authenticated
/// framing remains reusable, while operation-specific response acceptance is
/// fail closed.
///
/// # Errors
///
/// Returns a stable protocol violation for oversized, malformed, or
/// semantically invalid responses.
pub fn decode_bounded_response(bytes: &[u8]) -> Result<PluginResponse, PluginContractViolation> {
    if bytes.len() > MAX_PLUGIN_FRAME_BYTES {
        return Err(PluginContractViolation::FrameTooLarge);
    }
    let response = serde_json::from_slice::<PluginResponse>(bytes)
        .map_err(|_| PluginContractViolation::MalformedResponse)?;
    response.validate_contract()?;
    Ok(response)
}

/// Strictly decodes and validates one correlated request frame.
///
/// # Errors
///
/// Returns a stable violation for oversized, malformed, or semantically
/// invalid frames.
pub fn decode_bounded_request_frame(
    bytes: &[u8],
) -> Result<PluginRequestFrame, PluginContractViolation> {
    if bytes.len() > MAX_PLUGIN_FRAME_BYTES {
        return Err(PluginContractViolation::FrameTooLarge);
    }
    let frame = serde_json::from_slice::<PluginRequestFrame>(bytes)
        .map_err(|_| PluginContractViolation::MalformedResponse)?;
    frame.validate_contract()?;
    Ok(frame)
}

/// Strictly decodes and validates one correlated response frame.
///
/// # Errors
///
/// Returns a stable violation for oversized, malformed, or semantically
/// invalid frames.
pub fn decode_bounded_response_frame(
    bytes: &[u8],
) -> Result<PluginResponseFrame, PluginContractViolation> {
    if bytes.len() > MAX_PLUGIN_FRAME_BYTES {
        return Err(PluginContractViolation::FrameTooLarge);
    }
    let frame = serde_json::from_slice::<PluginResponseFrame>(bytes)
        .map_err(|_| PluginContractViolation::MalformedResponse)?;
    frame.validate_contract()?;
    Ok(frame)
}

fn validate_identifier(value: &str) -> Result<(), PluginContractViolation> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
    {
        return Err(PluginContractViolation::InvalidIdentifier);
    }
    Ok(())
}

fn validate_version(value: &str) -> Result<(), PluginContractViolation> {
    semver::Version::parse(value)
        .map(|_| ())
        .map_err(|_| PluginContractViolation::InvalidVersion)
}

fn validate_runtime_api(value: &str) -> Result<(), PluginContractViolation> {
    semver::VersionReq::parse(value)
        .map(|_| ())
        .map_err(|_| PluginContractViolation::InvalidRuntimeApi)
}

fn validate_schema(value: &str) -> Result<(), PluginContractViolation> {
    if value.is_empty() || value.len() > MAX_PLUGIN_SCHEMA_BYTES {
        return Err(PluginContractViolation::InvalidSchema);
    }
    let schema =
        serde_json::from_str::<Value>(value).map_err(|_| PluginContractViolation::InvalidSchema)?;
    if !schema.is_object() {
        return Err(PluginContractViolation::InvalidSchema);
    }
    Ok(())
}

fn validate_declaration_collection(
    actual: usize,
    maximum: usize,
) -> Result<(), PluginContractViolation> {
    if actual > maximum {
        Err(PluginContractViolation::ExcessiveDeclarationItems)
    } else {
        Ok(())
    }
}

fn validate_operation(
    handler: &str,
    input_schema: &str,
    output_schema: &str,
    timeout_ms: u64,
    failure_policy: &PluginOperationFailurePolicy,
    permissions: &PluginOperationPermissions,
) -> Result<(), PluginContractViolation> {
    validate_identifier(handler)?;
    validate_schema(input_schema)?;
    validate_schema(output_schema)?;
    let max_attempts = failure_policy.max_attempts();
    if timeout_ms == 0
        || timeout_ms > MAX_PLUGIN_TIMEOUT_MS
        || max_attempts == 0
        || max_attempts > MAX_PLUGIN_ATTEMPTS
    {
        return Err(PluginContractViolation::InvalidExecutionBound);
    }
    validate_declaration_collection(permissions.tools.len(), MAX_PLUGIN_PERMISSIONS)?;
    validate_declaration_collection(permissions.network.len(), MAX_PLUGIN_PERMISSIONS)
}

fn validate_inline_value(value: &Value) -> Result<(), PluginContractViolation> {
    let size = serde_json::to_vec(value)
        .map_err(|_| PluginContractViolation::MalformedResponse)?
        .len();
    if size > MAX_PLUGIN_INLINE_VALUE_BYTES {
        Err(PluginContractViolation::PayloadTooLarge)
    } else {
        Ok(())
    }
}

fn validate_value_hash(
    value: &Value,
    expected: ContentHash,
) -> Result<(), PluginContractViolation> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| PluginContractViolation::MalformedResponse)?;
    if ContentHash::digest(&bytes) == expected {
        Ok(())
    } else {
        Err(PluginContractViolation::ContentHashMismatch)
    }
}

fn validate_references(
    artifacts: &[PluginArtifactReference],
    references: &[PluginCanonicalReference],
) -> Result<(), PluginContractViolation> {
    if artifacts.len() > MAX_PLUGIN_REFERENCES || references.len() > MAX_PLUGIN_REFERENCES {
        return Err(PluginContractViolation::CollectionTooLarge);
    }
    for artifact in artifacts {
        validate_identifier(&artifact.artifact_id)?;
        if artifact.media_type.is_empty() || artifact.media_type.len() > 256 {
            return Err(PluginContractViolation::InvalidIdentifier);
        }
    }
    for reference in references {
        validate_identifier(&reference.id)?;
    }
    Ok(())
}

fn validate_metadata(metadata: &BTreeMap<String, String>) -> Result<(), PluginContractViolation> {
    if metadata.len() > MAX_PLUGIN_REFERENCES
        || metadata
            .iter()
            .any(|(key, value)| key.is_empty() || key.len() > 128 || value.len() > 1024)
    {
        return Err(PluginContractViolation::CollectionTooLarge);
    }
    Ok(())
}

fn validate_frame_size<T: Serialize>(value: &T) -> Result<(), PluginContractViolation> {
    let size = serde_json::to_vec(value)
        .map_err(|_| PluginContractViolation::MalformedResponse)?
        .len();
    if size > MAX_PLUGIN_FRAME_BYTES {
        Err(PluginContractViolation::FrameTooLarge)
    } else {
        Ok(())
    }
}

fn validate_invocation_common(
    binding: &PluginOperationBinding,
    implementation_id: &str,
    implementation_version: &str,
    handler: &str,
    request_hash: ContentHash,
    readable_state: &Value,
    authorization: &PluginAuthorization,
) -> Result<(), PluginContractViolation> {
    binding.validate()?;
    validate_identifier(implementation_id)?;
    validate_version(implementation_version)?;
    validate_identifier(handler)?;
    validate_inline_value(readable_state)?;
    if binding.request_hash != request_hash {
        return Err(PluginContractViolation::ContentHashMismatch);
    }
    if authorization.session_id != binding.session_id {
        return Err(PluginContractViolation::InvalidIdentifier);
    }
    validate_identifier(&authorization.owner_id)?;
    validate_identifier(&authorization.call_id)?;
    validate_identifier(&authorization.normalized_digest)?;
    validate_authorization_grant(&authorization.grant)?;
    validate_identifier(&authorization.cancellation_id)
}

fn validate_authorization_grant(value: &str) -> Result<(), PluginContractViolation> {
    if value.is_empty()
        || value.len() > MAX_PLUGIN_AUTHORIZATION_GRANT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(PluginContractViolation::InvalidIdentifier);
    }
    Ok(())
}

fn validate_audit_binding(
    audit: &PluginAudit,
    binding: &PluginOperationBinding,
    expected_operation: &str,
) -> Result<(), PluginContractViolation> {
    if audit.plugin_id != binding.plugin_id
        || audit.invocation_id.as_deref() != Some(binding.invocation_id.as_str())
        || audit.operation != expected_operation
        || audit.attempts != binding.attempt
    {
        return Err(PluginContractViolation::InvalidIdentifier);
    }
    validate_identifier(&audit.outcome)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn authorization() -> PluginAuthorization {
        PluginAuthorization {
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            call_id: String::from("call-1"),
            normalized_digest: String::from("digest"),
            grant: String::from("grant"),
            cancellation_id: String::from("cancel-1"),
        }
    }

    fn binding(request_hash: ContentHash) -> PluginOperationBinding {
        PluginOperationBinding {
            plugin_id: String::from("fixture.memory"),
            plugin_version: String::from("3.0.0"),
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

    fn retrieve_request() -> PluginMemoryRetrieveRequest {
        PluginMemoryRetrieveRequest {
            query: String::from("current runtime goal"),
            scopes: BTreeSet::from([PluginMemoryScope::Session, PluginMemoryScope::Project]),
            max_items: 8,
            max_bytes: 16 * 1024,
            artifacts: vec![PluginArtifactReference {
                artifact_id: String::from("artifact-1"),
                content_hash: ContentHash::digest(b"artifact"),
                media_type: String::from("text/plain"),
                size_bytes: 8,
                security_classification: PluginSecurityClassification::Private,
            }],
            references: vec![PluginCanonicalReference {
                kind: PluginCanonicalReferenceKind::NodeResult,
                id: String::from("node-result-1"),
                content_hash: Some(ContentHash::digest(b"node result")),
            }],
            parameters: json!({"namespace": "project"}),
        }
    }

    fn retrieve_binding(
        request: &PluginMemoryRetrieveRequest,
        readable_state: &Value,
        idempotency: PluginOperationIdempotency,
    ) -> PluginOperationBinding {
        let mut value = binding(ContentHash::from_bytes([0; 32]));
        value.request_hash = plugin_memory_retrieve_request_hash(
            &value,
            "fixture.memory",
            "1.2.3",
            "retrieve_memory",
            5_000,
            idempotency,
            request,
            readable_state,
        )
        .expect("retrieve request hash");
        value
    }

    fn write_binding(
        request: &PluginMemoryWriteRequest,
        readable_state: &Value,
    ) -> PluginOperationBinding {
        let mut value = binding(ContentHash::from_bytes([0; 32]));
        value.request_hash = plugin_memory_write_request_hash(
            &value,
            "fixture.memory",
            "1.2.3",
            "write_memory",
            5_000,
            PluginOperationIdempotency::NonIdempotent,
            request,
            readable_state,
        )
        .expect("write request hash");
        value
    }

    fn compaction_binding(
        request: &PluginCompactionRequest,
        readable_state: &Value,
    ) -> PluginOperationBinding {
        let mut value = binding(ContentHash::from_bytes([0; 32]));
        value.request_hash = plugin_compaction_request_hash(
            &value,
            "fixture.compactor",
            "2.0.0",
            "compact_projection",
            5_000,
            PluginOperationIdempotency::Idempotent,
            request,
            readable_state,
        )
        .expect("compaction request hash");
        value
    }

    fn memory_provider() -> PluginMemoryProviderDeclaration {
        PluginMemoryProviderDeclaration {
            provider_id: String::from("fixture.memory"),
            version: String::from("1.2.3"),
            runtime_api: String::from("^1.0"),
            capabilities: vec![String::from("memory.read"), String::from("memory.write")],
            retrieve: PluginMemoryRetrieveDeclaration {
                handler: String::from("retrieve_memory"),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 5_000,
                failure_policy: PluginOperationFailurePolicy::Retry {
                    max_attempts: 2,
                    backoff_ms: 10,
                },
                idempotency: PluginOperationIdempotency::Idempotent,
                required_permissions: PluginOperationPermissions::default(),
                state_scope: PluginOperationStateScope::Session,
                external_effects: false,
            },
            write: Some(PluginMemoryWriteDeclaration {
                handler: String::from("write_memory"),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 5_000,
                failure_policy: PluginOperationFailurePolicy::Reject,
                idempotency: PluginOperationIdempotency::NonIdempotent,
                required_permissions: PluginOperationPermissions::default(),
                state_scope: PluginOperationStateScope::Session,
                external_effects: true,
            }),
        }
    }

    fn compactor() -> PluginCompactorDeclaration {
        PluginCompactorDeclaration {
            compactor_id: String::from("fixture.compactor"),
            version: String::from("2.0.0"),
            runtime_api: String::from(">=1.0, <2.0"),
            handler: String::from("compact_projection"),
            capabilities: vec![String::from("context.compact")],
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"array"}"#),
            timeout_ms: 5_000,
            failure_policy: PluginOperationFailurePolicy::Retry {
                max_attempts: 2,
                backoff_ms: 10,
            },
            idempotency: PluginOperationIdempotency::Idempotent,
            required_permissions: PluginOperationPermissions::default(),
            state_scope: PluginOperationStateScope::Turn,
            external_effects: false,
        }
    }

    #[test]
    fn context_transform_command_round_trips_with_exact_identity() {
        let declaration_hash = ContentHash::digest(b"context declaration");
        let configuration_reference = ContentHash::digest(b"context configuration");
        let input = json!({"projection": [{"role": "user", "content": "hello"}]});
        let readable_state = json!({"classification": "private"});
        let request_hash = plugin_context_transform_invocation_request_hash(
            "fixture.context",
            "session-1:run-1:model-1",
            "fixture.redact",
            "1.0.0",
            "before_model_request",
            "redact_projection",
            5_000,
            configuration_reference,
            &input,
            &readable_state,
        )
        .expect("context request");
        let command = PluginCommand::InvokeContextTransform {
            cancellation_target: PluginInvocationCancellationTarget {
                session_id: String::from("session-1"),
                run_id: String::from("run-1"),
                plugin_id: String::from("fixture.context"),
                plugin_version: String::from("1.0.0"),
                invocation_id: String::from("session-1:run-1:model-1"),
                invocation_digest: plugin_invocation_identity_digest(
                    "session-1",
                    "run-1",
                    "fixture.context",
                    "1.0.0",
                    "session-1:run-1:model-1",
                    "fixture.redact",
                    declaration_hash,
                    request_hash,
                )
                .expect("invocation digest"),
                operation_id: String::from("fixture.redact"),
                declaration_hash,
                request_hash,
            },
            plugin_id: String::from("fixture.context"),
            invocation_id: String::from("session-1:run-1:model-1"),
            transform_id: String::from("fixture.redact"),
            transform_version: String::from("1.0.0"),
            lifecycle: ContextTransformLifecycle::BeforeModelRequest,
            handler: String::from("redact_projection"),
            timeout_ms: 5_000,
            configuration_reference: configuration_reference.to_hex(),
            input,
            readable_state,
            authorization: PluginAuthorization {
                owner_id: String::from("owner"),
                session_id: String::from("session-1"),
                call_id: String::from("call-1"),
                normalized_digest: String::from("digest"),
                grant: String::from("grant"),
                cancellation_id: String::from("cancel-1"),
            },
        };

        let encoded = serde_json::to_vec(&command).expect("encode transform command");
        let decoded: PluginCommand =
            serde_json::from_slice(&encoded).expect("decode transform command");

        assert_eq!(decoded, command);
    }

    #[test]
    fn context_transform_proposal_round_trips_as_typed_response() {
        let response = PluginResponse::ContextTransformProposal {
            proposal: PluginContextTransformProposal {
                replacement: json!([{"role": "user", "content": "[redacted]"}]),
            },
            audit: PluginAudit {
                plugin_id: String::from("fixture.context"),
                invocation_id: Some(String::from("invocation-1")),
                operation: String::from("context_transform"),
                outcome: String::from("completed"),
                attempts: 1,
            },
        };

        let encoded = serde_json::to_vec(&response).expect("encode transform proposal");
        let decoded: PluginResponse =
            serde_json::from_slice(&encoded).expect("decode transform proposal");

        assert_eq!(decoded, response);
    }

    #[test]
    fn memory_provider_declaration_hash_is_complete_and_deterministic() {
        let provider = memory_provider();
        provider.validate().expect("valid provider");
        let bytes = provider.declaration_hash_input().expect("canonical bytes");
        let hash = provider.declaration_hash().expect("declaration hash");

        let decoded: PluginMemoryProviderDeclaration =
            serde_json::from_slice(&bytes).expect("decode declaration");
        assert_eq!(decoded, provider);
        assert_eq!(hash, ContentHash::digest(&bytes));

        let mut reordered = provider.clone();
        reordered.capabilities = vec![String::from("memory.write"), String::from("memory.read")];
        assert_ne!(
            reordered.declaration_hash().expect("reordered hash"),
            hash,
            "authoritative SDK vector order participates in declaration identity"
        );

        let mut changed = provider;
        changed.write.as_mut().expect("write").external_effects = false;
        assert_ne!(
            changed.declaration_hash().expect("changed hash"),
            hash,
            "every declaration field must participate"
        );
    }

    #[test]
    fn declarations_fail_closed_on_unsafe_recovery_and_malformed_schema() {
        let mut provider = memory_provider();
        provider.retrieve.external_effects = true;
        assert_eq!(
            provider.validate(),
            Err(PluginContractViolation::UnsafeRecoveryDeclaration)
        );

        let mut provider = memory_provider();
        provider.write.as_mut().expect("write").failure_policy =
            PluginOperationFailurePolicy::Retry {
                max_attempts: 2,
                backoff_ms: 0,
            };
        assert_eq!(
            provider.validate(),
            Err(PluginContractViolation::UnsafeRecoveryDeclaration)
        );

        let mut provider = memory_provider();
        provider.retrieve.input_schema = String::from("[]");
        assert_eq!(
            provider.validate(),
            Err(PluginContractViolation::InvalidSchema)
        );

        let mut declaration = compactor();
        declaration.idempotency = PluginOperationIdempotency::NonIdempotent;
        assert_eq!(
            declaration.validate(),
            Err(PluginContractViolation::UnsafeRecoveryDeclaration)
        );
    }

    #[test]
    fn compactor_declaration_hash_binds_every_field() {
        let declaration = compactor();
        declaration.validate().expect("valid declaration");
        let expected = declaration.declaration_hash().expect("hash");

        let mut changed = declaration;
        changed.timeout_ms += 1;
        assert_ne!(changed.declaration_hash().expect("changed hash"), expected);
    }

    #[test]
    fn wire_declaration_hash_input_exactly_matches_authoritative_sdk() {
        let provider = memory_provider();
        let provider_bytes = provider.declaration_hash_input().expect("wire provider");
        let sdk_provider: agentmod_plugin_sdk::MemoryProviderManifest =
            serde_json::from_slice(&provider_bytes).expect("SDK-compatible provider");
        assert_eq!(
            sdk_provider
                .declaration_hash_input()
                .expect("SDK provider bytes"),
            provider_bytes
        );
        assert_eq!(
            provider.declaration_hash().expect("wire provider hash"),
            ContentHash::digest(
                &sdk_provider
                    .declaration_hash_input()
                    .expect("SDK provider bytes")
            )
        );

        let compactor = compactor();
        let compactor_bytes = compactor.declaration_hash_input().expect("wire compactor");
        let sdk_compactor: agentmod_plugin_sdk::CompactorManifest =
            serde_json::from_slice(&compactor_bytes).expect("SDK-compatible compactor");
        assert_eq!(
            sdk_compactor
                .declaration_hash_input()
                .expect("SDK compactor bytes"),
            compactor_bytes
        );
        assert_eq!(
            compactor.declaration_hash().expect("wire compactor hash"),
            ContentHash::digest(
                &sdk_compactor
                    .declaration_hash_input()
                    .expect("SDK compactor bytes")
            )
        );
    }

    #[test]
    fn legacy_manifest_defaults_new_declaration_collections() {
        let manifest: PluginManifest = serde_json::from_value(json!({
            "schema_version": 1,
            "id": "fixture.legacy",
            "version": "1.0.0",
            "runtime_api": "^1.0",
            "category": "observer",
            "scope": "session",
            "class": "observer",
            "entrypoint": {"program": "fixture", "arguments": []},
            "required_capabilities": [],
            "provided_capabilities": [],
            "subscribed_events": [],
            "read_authority": [],
            "proposed_write_authority": [],
            "tool_permissions": [],
            "network_permissions": [],
            "after": [],
            "before": [],
            "stage": 0,
            "priority": 0,
            "timeout_ms": 1000,
            "failure_policy": "continue",
            "max_attempts": 1,
            "retry_backoff_ms": 0,
            "state_migration_version": 1,
            "configuration_schema": {
                "id": "fixture",
                "version": 1,
                "required": false,
                "inline_json": "{}"
            }
        }))
        .expect("legacy manifest");

        assert!(manifest.node_executors.is_empty());
        assert!(manifest.context_transforms.is_empty());
        assert!(manifest.memory_providers.is_empty());
        assert!(manifest.compactors.is_empty());
        let encoded = serde_json::to_value(&manifest).expect("serialize legacy manifest");
        assert!(
            encoded.get("memory_providers").is_none() && encoded.get("compactors").is_none(),
            "empty wire-v6 declarations must not change the signed legacy manifest bytes"
        );
    }

    #[test]
    fn memory_retrieve_command_is_distinct_and_hash_bound() {
        let request = retrieve_request();
        let readable_state = json!({"generation": 4});
        let command = PluginCommand::InvokeMemoryRetrieve {
            binding: retrieve_binding(
                &request,
                &readable_state,
                PluginOperationIdempotency::Idempotent,
            ),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("retrieve_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::Idempotent,
            request,
            readable_state,
            authorization: authorization(),
        };
        command.validate_contract().expect("valid command");

        let encoded = serde_json::to_vec(&command).expect("encode");
        let decoded: PluginCommand = serde_json::from_slice(&encoded).expect("decode");
        assert_eq!(decoded, command);
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded)
                .expect("json")
                .get("command")
                .and_then(Value::as_str),
            Some("invoke_memory_retrieve")
        );
    }

    #[test]
    fn memory_write_full_hash_matches_runtime_logic_golden() {
        let binding = PluginOperationBinding {
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("2.0.0"),
            invocation_id: String::from(
                "plugin-automatic-memory-write:c169b03168fd4f77c72648046d21db4afa8d0009065318ddaec6ac8373a385f9",
            ),
            operation_id: String::from("fixture.memory"),
            session_id: String::from("00000000-0000-0000-0000-000000005151"),
            run_id: String::from("run-1"),
            node_id: None,
            declaration_hash: ContentHash::from_str(
                "73b3fcc63676a58d6cb44c5d21f3fbfcecc0b5216fa2a0bf5ba93c0166908284",
            )
            .expect("declaration hash"),
            configuration_reference: ContentHash::from_str(
                "bfe36838316fd548f475283db1ffd3dfa6cec917fb4d09d31bd42a8d66ce5174",
            )
            .expect("configuration hash"),
            request_hash: ContentHash::from_bytes([0; 32]),
            idempotency_key: String::from(
                "plugin-automatic-memory-write-once:c169b03168fd4f77c72648046d21db4afa8d0009065318ddaec6ac8373a385f9",
            ),
            attempt: 1,
        };
        let request = PluginMemoryWriteRequest {
            scope: PluginMemoryScope::Session,
            boundary: PluginMemoryWriteBoundary::TurnCompletion,
            value: Value::String(String::from("bounded plugin memory")),
            value_hash: ContentHash::from_str(
                "803dca15ce3768077781c780fe99b1c8423d6a0b9ebd90fff6f2a4e3da846840",
            )
            .expect("value hash"),
            artifacts: Vec::new(),
            references: Vec::new(),
            security_classification: PluginSecurityClassification::Internal,
            parameters: json!({}),
        };
        let hash = plugin_memory_write_request_hash(
            &binding,
            "fixture.memory",
            "1.0.0",
            "write",
            1_000,
            PluginOperationIdempotency::NonIdempotent,
            &request,
            &json!({}),
        )
        .expect("complete request hash");
        assert_eq!(
            hash.to_hex(),
            "696facbe7de78af68a107339722d8f6871602bdc8a4fea2d5c918d4d58b6eee3"
        );
    }

    #[test]
    fn memory_command_accepts_bounded_portable_authorization_grants() {
        let request = retrieve_request();
        let readable_state = json!({});
        let mut authorization = authorization();
        authorization.grant = format!("v1.{}.{}", "ab".repeat(512), "cd".repeat(32));
        assert!(
            authorization.grant.len() > 256,
            "the regression grant must exceed the stable-identifier bound"
        );
        let command = PluginCommand::InvokeMemoryRetrieve {
            binding: retrieve_binding(
                &request,
                &readable_state,
                PluginOperationIdempotency::Idempotent,
            ),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("retrieve_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::Idempotent,
            request,
            readable_state,
            authorization,
        };
        command
            .validate_contract()
            .expect("portable keyed grant within the authorization envelope");

        let mut oversized = command;
        let PluginCommand::InvokeMemoryRetrieve { authorization, .. } = &mut oversized else {
            unreachable!("memory retrieve fixture");
        };
        authorization.grant = "a".repeat(MAX_PLUGIN_AUTHORIZATION_GRANT_BYTES + 1);
        assert_eq!(
            oversized.validate_contract(),
            Err(PluginContractViolation::InvalidIdentifier)
        );
    }

    #[test]
    fn each_operation_has_a_separate_command_tag() {
        let value = json!({"memory": "approved"});
        let write_request = PluginMemoryWriteRequest {
            scope: PluginMemoryScope::Session,
            boundary: PluginMemoryWriteBoundary::Explicit,
            value: value.clone(),
            value_hash: ContentHash::digest(&serde_json::to_vec(&value).expect("canonical value")),
            artifacts: Vec::new(),
            references: Vec::new(),
            security_classification: PluginSecurityClassification::Private,
            parameters: json!({}),
        };
        let write_readable_state = json!({});
        let write = PluginCommand::InvokeMemoryWrite {
            binding: write_binding(&write_request, &write_readable_state),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("write_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::NonIdempotent,
            request: write_request,
            readable_state: write_readable_state,
            authorization: authorization(),
        };
        write.validate_contract().expect("valid write");

        let projection = json!([{"role": "user", "content": "hello"}]);
        let compaction_request = PluginCompactionRequest {
            projection: projection.clone(),
            projection_hash: ContentHash::digest(
                &serde_json::to_vec(&projection).expect("projection"),
            ),
            required_references: Vec::new(),
            required_artifacts: Vec::new(),
            preservation_requirements: BTreeSet::from([String::from("current_input")]),
            max_replacement_bytes: 64 * 1024,
            max_projection_tokens: 8_000,
            parameters: json!({}),
        };
        let compaction_readable_state = json!({});
        let compact = PluginCommand::InvokeCompaction {
            binding: compaction_binding(&compaction_request, &compaction_readable_state),
            compactor_id: String::from("fixture.compactor"),
            compactor_version: String::from("2.0.0"),
            handler: String::from("compact_projection"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::Idempotent,
            request: compaction_request,
            readable_state: compaction_readable_state,
            authorization: authorization(),
        };
        compact.validate_contract().expect("valid compaction");

        let write_tag = serde_json::to_value(write).expect("write JSON");
        let compact_tag = serde_json::to_value(compact).expect("compaction JSON");
        assert_eq!(write_tag["command"], "invoke_memory_write");
        assert_eq!(compact_tag["command"], "invoke_compaction");
        assert_ne!(write_tag["command"], "invoke_context_transform");
        assert_ne!(compact_tag["command"], "invoke_context_transform");
    }

    #[test]
    fn command_rejects_request_hash_drift_and_unsafe_pure_retry() {
        let request = retrieve_request();
        let readable_state = json!({});
        let command = PluginCommand::InvokeMemoryRetrieve {
            binding: binding(ContentHash::digest(b"wrong")),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("retrieve_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::Idempotent,
            request,
            readable_state,
            authorization: authorization(),
        };
        assert_eq!(
            command.validate_contract(),
            Err(PluginContractViolation::ContentHashMismatch)
        );

        let request = retrieve_request();
        let readable_state = json!({});
        let command = PluginCommand::InvokeMemoryRetrieve {
            binding: retrieve_binding(
                &request,
                &readable_state,
                PluginOperationIdempotency::NonIdempotent,
            ),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("retrieve_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::NonIdempotent,
            request,
            readable_state,
            authorization: authorization(),
        };
        assert_eq!(
            command.validate_contract(),
            Err(PluginContractViolation::UnsafeRecoveryDeclaration)
        );

        let value = json!({"memory": "approved"});
        let request = PluginMemoryWriteRequest {
            scope: PluginMemoryScope::Session,
            boundary: PluginMemoryWriteBoundary::Explicit,
            value: value.clone(),
            value_hash: ContentHash::digest(&serde_json::to_vec(&value).expect("canonical value")),
            artifacts: Vec::new(),
            references: Vec::new(),
            security_classification: PluginSecurityClassification::Private,
            parameters: json!({}),
        };
        let readable_state = json!({});
        let mut retry_binding = write_binding(&request, &readable_state);
        retry_binding.attempt = 2;
        retry_binding.request_hash = plugin_memory_write_request_hash(
            &retry_binding,
            "fixture.memory",
            "1.2.3",
            "write_memory",
            5_000,
            PluginOperationIdempotency::NonIdempotent,
            &request,
            &readable_state,
        )
        .expect("retry write hash");
        let command = PluginCommand::InvokeMemoryWrite {
            binding: retry_binding,
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.2.3"),
            handler: String::from("write_memory"),
            timeout_ms: 5_000,
            idempotency: PluginOperationIdempotency::NonIdempotent,
            request,
            readable_state,
            authorization: authorization(),
        };
        assert_eq!(
            command.validate_contract(),
            Err(PluginContractViolation::UnsafeRecoveryDeclaration),
            "non-idempotent writes cannot represent an automatic retry"
        );
    }

    #[test]
    fn typed_memory_response_round_trips_and_validates_echoed_identity() {
        let request = retrieve_request();
        let operation_binding = binding(request.content_hash().expect("request hash"));
        let value = json!({"fact": "runtime owns acceptance"});
        let response = PluginResponse::MemoryRetrieved {
            proposal: PluginMemoryRetrieveProposal {
                binding: operation_binding.clone(),
                provider_id: String::from("fixture.memory"),
                provider_version: String::from("1.2.3"),
                items: vec![PluginMemoryItemProposal {
                    item_id: String::from("memory-1"),
                    scope: PluginMemoryScope::Session,
                    value: value.clone(),
                    value_hash: ContentHash::digest(&serde_json::to_vec(&value).expect("value")),
                    artifacts: Vec::new(),
                    references: Vec::new(),
                    security_classification: PluginSecurityClassification::Private,
                    metadata: BTreeMap::from([(String::from("source"), String::from("fixture"))]),
                }],
            },
            audit: PluginAudit {
                plugin_id: operation_binding.plugin_id.clone(),
                invocation_id: Some(operation_binding.invocation_id.clone()),
                operation: String::from("memory_retrieve"),
                outcome: String::from("completed"),
                attempts: operation_binding.attempt,
            },
        };

        let bytes = serde_json::to_vec(&response).expect("response");
        assert_eq!(
            decode_bounded_response(&bytes).expect("strict response"),
            response
        );
    }

    #[test]
    fn malformed_plugin_results_fail_closed() {
        let request = retrieve_request();
        let operation_binding = binding(request.content_hash().expect("request hash"));
        let value = json!({"fact": "value"});
        let response = PluginResponse::MemoryRetrieved {
            proposal: PluginMemoryRetrieveProposal {
                binding: operation_binding.clone(),
                provider_id: String::from("fixture.memory"),
                provider_version: String::from("1.2.3"),
                items: vec![PluginMemoryItemProposal {
                    item_id: String::from("memory-1"),
                    scope: PluginMemoryScope::Session,
                    value,
                    value_hash: ContentHash::digest(b"wrong"),
                    artifacts: Vec::new(),
                    references: Vec::new(),
                    security_classification: PluginSecurityClassification::Private,
                    metadata: BTreeMap::new(),
                }],
            },
            audit: PluginAudit {
                plugin_id: operation_binding.plugin_id,
                invocation_id: Some(operation_binding.invocation_id),
                operation: String::from("memory_retrieve"),
                outcome: String::from("completed"),
                attempts: 1,
            },
        };
        let bytes = serde_json::to_vec(&response).expect("response");
        assert_eq!(
            decode_bounded_response(&bytes),
            Err(PluginContractViolation::ContentHashMismatch)
        );

        let unknown_field = br#"{
            "result":"memory_written",
            "value":{
                "receipt":{
                    "binding":{
                        "plugin_id":"fixture.memory",
                        "plugin_version":"3.0.0",
                        "invocation_id":"invocation-1",
                        "operation_id":"operation-1",
                        "session_id":"session-1",
                        "run_id":"run-1",
                        "declaration_hash":"d6c3ca310006006cbe4ea8505bd83846bcd858c29e7f6d7c65b25f140fdd2e1f",
                        "configuration_reference":"d6c3ca310006006cbe4ea8505bd83846bcd858c29e7f6d7c65b25f140fdd2e1f",
                        "request_hash":"d6c3ca310006006cbe4ea8505bd83846bcd858c29e7f6d7c65b25f140fdd2e1f",
                        "idempotency_key":"key-1",
                        "attempt":1
                    },
                    "provider_id":"fixture.memory",
                    "provider_version":"1.0.0",
                    "provider_record_id":"record-1",
                    "value_hash":"d6c3ca310006006cbe4ea8505bd83846bcd858c29e7f6d7c65b25f140fdd2e1f",
                    "receipt":{},
                    "forged":"field"
                },
                "audit":{
                    "plugin_id":"fixture.memory",
                    "invocation_id":"invocation-1",
                    "operation":"memory_write",
                    "outcome":"completed",
                    "attempts":1
                }
            }
        }"#;
        assert_eq!(
            decode_bounded_response(unknown_field),
            Err(PluginContractViolation::MalformedResponse)
        );

        let oversized = vec![b' '; MAX_PLUGIN_FRAME_BYTES + 1];
        assert_eq!(
            decode_bounded_response(&oversized),
            Err(PluginContractViolation::FrameTooLarge)
        );
    }

    fn cancellation_target() -> PluginInvocationCancellationTarget {
        let declaration_hash = ContentHash::digest(b"declaration");
        let request_hash = ContentHash::digest(b"request");
        PluginInvocationCancellationTarget {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.2.3"),
            invocation_id: String::from("invocation-1"),
            invocation_digest: plugin_invocation_identity_digest(
                "session-1",
                "run-1",
                "fixture.plugin",
                "1.2.3",
                "invocation-1",
                "operation-1",
                declaration_hash,
                request_hash,
            )
            .expect("invocation digest"),
            operation_id: String::from("operation-1"),
            declaration_hash,
            request_hash,
        }
    }

    #[test]
    fn protocol_v10_preserves_v9_exact_authenticated_cancellation() {
        assert_eq!(CURRENT_PROTOCOL_VERSION, 10);
        assert!(
            serde_json::from_value::<PluginCommand>(json!({
                "command": "cancel",
                "value": {"invocation_id": "invocation-1"}
            }))
            .is_err()
        );

        let target = cancellation_target();
        let action_digest = plugin_invocation_cancellation_action_digest(
            &target,
            "user_cancelled",
            "nonce-1",
            "cancel-key-1",
            "cancellation-1",
        )
        .expect("action digest");
        let command = PluginCommand::CancelInvocation {
            target: target.clone(),
            reason_code: String::from("user_cancelled"),
            action_digest,
            nonce: String::from("nonce-1"),
            idempotency_key: String::from("cancel-key-1"),
            authorization: PluginAuthorization {
                owner_id: String::from("owner-1"),
                session_id: target.session_id.clone(),
                call_id: String::from("call-1"),
                normalized_digest: action_digest.to_hex(),
                grant: String::from("bounded-grant"),
                cancellation_id: String::from("cancellation-1"),
            },
        };
        command.validate_contract().expect("exact cancellation");

        let mut substituted = command;
        if let PluginCommand::CancelInvocation { target, .. } = &mut substituted {
            target.run_id = String::from("run-2");
        }
        assert_eq!(
            substituted.validate_contract(),
            Err(PluginContractViolation::ContentHashMismatch)
        );
    }

    #[test]
    fn cancellation_receipt_is_strict_and_signal_only() {
        let target = cancellation_target();
        let action_digest = plugin_invocation_cancellation_action_digest(
            &target,
            "timeout",
            "nonce-2",
            "cancel-key-2",
            "cancellation-2",
        )
        .expect("action digest");
        let mut receipt = PluginInvocationCancellationReceipt {
            target,
            reason_code: String::from("timeout"),
            action_digest,
            nonce: String::from("nonce-2"),
            idempotency_key: String::from("cancel-key-2"),
            cancellation_id: String::from("cancellation-2"),
            status: PluginInvocationCancellationStatus::AlreadyTerminal,
            receipt_id: String::from("receipt-2"),
            receipt_digest: ContentHash::digest(b"pending"),
        };
        receipt.receipt_digest =
            plugin_invocation_cancellation_receipt_digest(&receipt).expect("receipt digest");
        let response = PluginResponse::InvocationCancelled {
            receipt: Box::new(receipt.clone()),
        };
        response.validate_contract().expect("valid receipt");

        receipt.target.plugin_id = String::from("other.plugin");
        assert_eq!(
            PluginResponse::InvocationCancelled {
                receipt: Box::new(receipt)
            }
            .validate_contract(),
            Err(PluginContractViolation::ContentHashMismatch)
        );
    }

    #[test]
    fn correlated_frames_round_trip_and_reject_correlation_substitution() {
        let request = PluginRequestFrame {
            correlation_id: String::from("correlation-1"),
            command: PluginCommand::Negotiate {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                runtime_api_version: String::from("1.0.0"),
                capabilities: BTreeSet::from([String::from("plugin_state")]),
            },
        };
        let request_bytes = serde_json::to_vec(&request).expect("request frame");
        assert_eq!(
            decode_bounded_request_frame(&request_bytes).expect("strict request frame"),
            request
        );

        let response = PluginResponseFrame {
            correlation_id: request.correlation_id.clone(),
            response: PluginResponse::Failed {
                code: String::from("fixture_failure"),
                message: String::from("bounded diagnostic"),
                retryable: false,
            },
        };
        let response_bytes = serde_json::to_vec(&response).expect("response frame");
        assert_eq!(
            decode_bounded_response_frame(&response_bytes).expect("strict response frame"),
            response
        );

        let substituted = serde_json::to_vec(&json!({
            "correlation_id":"correlation-1",
            "command":{
                "command":"negotiate",
                "protocol_version":CURRENT_PROTOCOL_VERSION,
                "runtime_api_version":"1.0.0",
                "capabilities":["plugin_state"]
            },
            "response":{"response":"health","loaded":0,"running":0,"observer_dropped":0}
        }))
        .expect("substituted frame");
        assert_eq!(
            decode_bounded_request_frame(&substituted),
            Err(PluginContractViolation::MalformedResponse)
        );
    }
}
