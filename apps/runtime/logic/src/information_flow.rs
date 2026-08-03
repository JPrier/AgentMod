//! Bounded runtime information-flow classification and secret detection.

use std::collections::BTreeSet;

use agentmod_primitives::ContentHash;

use crate::conversation::ConversationEntry;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum text inspected as one canonical information-flow value.
pub const MAX_INFORMATION_FLOW_TEXT_BYTES: usize = 64 * 1024;
/// Maximum exact source bytes hashed into one flow label.
pub const MAX_INFORMATION_FLOW_SOURCE_BYTES: usize = 1024 * 1024;
/// Maximum portable reference retained in automatic context or memory.
pub const MAX_INFORMATION_FLOW_REFERENCE_BYTES: usize = 1024;
/// Maximum stable identity retained in one flow envelope or source label.
pub const MAX_INFORMATION_FLOW_IDENTITY_BYTES: usize = 128;
/// Maximum independently labelled sources admitted to one flow decision.
pub const MAX_INFORMATION_FLOW_SOURCES: usize = 128;

/// Stable runtime-owned information-flow class.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationFlowClassification {
    /// Deliberately public content.
    Public,
    /// Runtime-internal non-user-private content.
    Internal,
    /// User-private content that may be retained only in its declared scope.
    Private,
    /// Sensitive instructions, tool data, or child/process state.
    Confidential,
    /// An opaque reference to secret data; never the secret value.
    SecretReference,
    /// Secret material that must not enter automatic memory.
    Secret,
}

impl InformationFlowClassification {
    /// Stable serialized name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Private => "private",
            Self::Confidential => "confidential",
            Self::SecretReference => "secret_reference",
            Self::Secret => "secret",
        }
    }

    /// Whether this is an ordinary lattice class rather than an orthogonal
    /// secret-reference capability or prohibited raw secret.
    #[must_use]
    pub const fn is_ordinary(self) -> bool {
        matches!(
            self,
            Self::Public | Self::Internal | Self::Private | Self::Confidential
        )
    }

    /// Explicit v1 ordinary-class join.
    #[must_use]
    pub const fn join_ordinary(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Public, value) | (value, Self::Public) if value.is_ordinary() => Some(value),
            (Self::Internal, Self::Internal) => Some(Self::Internal),
            (Self::Internal, Self::Private) | (Self::Private, Self::Internal | Self::Private) => {
                Some(Self::Private)
            }
            (Self::Internal | Self::Private | Self::Confidential, Self::Confidential)
            | (Self::Confidential, Self::Internal | Self::Private) => Some(Self::Confidential),
            _ => None,
        }
    }

    /// Whether this ordinary class is at least as restrictive as `source`.
    #[must_use]
    pub const fn dominates_ordinary(self, source: Self) -> bool {
        matches!(
            (self, source),
            (Self::Public, Self::Public)
                | (Self::Internal, Self::Public | Self::Internal)
                | (Self::Private, Self::Public | Self::Internal | Self::Private)
                | (
                    Self::Confidential,
                    Self::Public | Self::Internal | Self::Private | Self::Confidential
                )
        )
    }
}

/// Runtime-owned sink whose bounded clearance is enforced by the v1 kernel.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationFlowSink {
    /// Provider model request projection.
    ModelProjection,
    /// User-facing frontend projection.
    FrontendProjection,
    /// Isolated plugin invocation input.
    PluginInvocation,
    /// Consequential external-network output.
    ExternalNetwork,
    /// Automatic durable memory input.
    AutomaticMemory,
    /// Local tool invocation input.
    LocalTool,
    /// Artifact object content.
    Artifact,
    /// Child task or child-message envelope.
    ChildMessage,
}

/// Exact immutable source label consumed by one flow evaluation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InformationFlowSource {
    /// Stable bounded constituent identity.
    identity: String,
    /// Ordinary source classification.
    classification: InformationFlowClassification,
    /// True only when an exact dedicated `secret-ref:` field was validated.
    secret_reference: bool,
    /// Hash of the exact source bytes.
    value_hash: ContentHash,
    /// Deterministic hash of this complete label.
    source_hash: ContentHash,
}

/// Deterministic bounded flow envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InformationFlowEnvelope {
    /// Stable operation or destination identity.
    pub identity: String,
    /// Exact sink being evaluated.
    pub sink: InformationFlowSink,
    /// Declared destination classification.
    pub destination_classification: InformationFlowClassification,
    /// Join of every ordinary source class.
    pub joined_classification: InformationFlowClassification,
    /// Whether any source contains a validated dedicated secret reference.
    pub contains_secret_references: bool,
    /// Stable sorted hashes of all exact source labels.
    pub source_hashes: Vec<ContentHash>,
    /// Deterministic hash of the complete envelope.
    pub envelope_hash: ContentHash,
}

/// Stable fail-closed flow denial class.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InformationFlowDenialCode {
    /// Destination is not an ordinary v1 classification.
    InvalidDestinationClassification,
    /// Destination exceeds the sink's bounded clearance.
    SinkClearanceExceeded,
    /// Destination would weaken at least one source classification.
    DeclassificationProhibited,
    /// Sink does not admit secret-reference capabilities.
    SecretReferenceProhibited,
}

/// Deterministic flow decision. No v1 variant performs declassification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum InformationFlowDecision {
    /// Every source is within the sink and destination clearance.
    Allowed {
        /// Exact envelope evaluated by the decision.
        envelope_hash: ContentHash,
        /// Deterministic decision hash.
        decision_hash: ContentHash,
    },
    /// Flow failed closed with stable non-secret diagnostics.
    Denied {
        /// Exact envelope evaluated by the decision.
        envelope_hash: ContentHash,
        /// Stable denial class.
        code: InformationFlowDenialCode,
        /// Stable bounded non-secret explanation.
        reason: String,
        /// Sorted exact sources responsible for the denial.
        offending_source_hashes: Vec<ContentHash>,
        /// Deterministic decision hash.
        decision_hash: ContentHash,
    },
}

/// Stable bounded secret/reference finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InformationFlowFinding {
    /// The value exceeded the inspection bound and cannot be admitted.
    InspectionBoundExceeded,
    /// A credential name was paired with an assignment delimiter.
    CredentialAssignment,
    /// A provider-specific high-confidence credential prefix was present.
    CredentialPrefix,
    /// An authorization header or URI user-info credential was present.
    AuthorizationMaterial,
    /// A PEM or SSH private-key envelope was present.
    PrivateKeyMaterial,
    /// A compact signed token matched a bounded JWT shape.
    SignedToken,
    /// A value presented an operating-system/process/network handle.
    ExternalHandle,
    /// A portable reference was malformed or belonged to an unknown domain.
    InvalidReference,
    /// A control character made the value unsafe for canonical retention.
    ControlCharacter,
    /// A source supplied an unknown security class and cannot be retained.
    UnclassifiedSecret,
}

/// One explicit per-entry classification retained by context builders.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClassifiedConversationEntry {
    /// Canonical conversation entry ID.
    pub entry_id: String,
    /// Stable entry kind.
    pub entry_kind: String,
    /// Runtime-owned classification.
    pub classification: InformationFlowClassification,
}

/// A bounded automatic-memory projection with explicit per-entry classes.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ClassifiedAutomaticMemoryEntries {
    /// Canonical entries eligible for automatic memory.
    pub entries: Vec<ConversationEntry>,
    /// Exact classification record for every retained entry.
    pub information_flow: Vec<ClassifiedConversationEntry>,
}

/// Fail-closed information-flow validation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InformationFlowError {
    /// Bounded secret inspection rejected an entry.
    #[error("conversation entry `{entry_id}` contains prohibited sensitive material")]
    SensitiveEntry {
        /// Canonical entry ID.
        entry_id: String,
        /// Stable non-secret finding.
        finding: InformationFlowFinding,
    },
    /// A reference is not a bounded portable runtime reference.
    #[error("runtime reference is not a bounded portable reference")]
    InvalidReference(InformationFlowFinding),
    /// A source label is unbounded or does not carry an ordinary class.
    #[error("information-flow source label is invalid")]
    InvalidSource,
    /// Envelope identity, source count, or source uniqueness is invalid.
    #[error("information-flow envelope is invalid")]
    InvalidEnvelope,
    /// Canonical flow material could not be serialized for hashing.
    #[error("information-flow material could not be hashed")]
    Hash,
}

impl InformationFlowSink {
    /// Maximum ordinary classification admitted by this sink.
    #[must_use]
    pub const fn maximum_classification(self) -> InformationFlowClassification {
        match self {
            Self::ExternalNetwork => InformationFlowClassification::Internal,
            Self::AutomaticMemory => InformationFlowClassification::Private,
            Self::ModelProjection
            | Self::FrontendProjection
            | Self::PluginInvocation
            | Self::LocalTool
            | Self::Artifact
            | Self::ChildMessage => InformationFlowClassification::Confidential,
        }
    }

    /// Whether the sink admits exact secret references in dedicated fields.
    #[must_use]
    pub const fn permits_secret_references(self) -> bool {
        matches!(self, Self::LocalTool | Self::ChildMessage)
    }
}

impl InformationFlowSource {
    /// Stable constituent identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Ordinary source classification.
    #[must_use]
    pub const fn classification(&self) -> InformationFlowClassification {
        self.classification
    }

    /// Whether an exact dedicated secret reference was validated.
    #[must_use]
    pub const fn has_secret_reference(&self) -> bool {
        self.secret_reference
    }

    /// Hash of the exact labelled bytes.
    #[must_use]
    pub const fn value_hash(&self) -> ContentHash {
        self.value_hash
    }

    /// Deterministic complete source-label hash.
    #[must_use]
    pub const fn source_hash(&self) -> ContentHash {
        self.source_hash
    }

    /// Builds one exact source label from bounded source bytes.
    ///
    /// `secret_reference` is accepted only as a dedicated exact capability
    /// field. Ordinary bytes are never scanned for a suggestive substring.
    ///
    /// # Errors
    ///
    /// Rejects invalid identities, non-ordinary classes, and malformed
    /// dedicated secret references.
    pub fn from_bytes(
        identity: impl Into<String>,
        classification: InformationFlowClassification,
        value: &[u8],
        secret_reference: Option<&str>,
    ) -> Result<Self, InformationFlowError> {
        let identity = identity.into();
        if !valid_flow_identity(&identity)
            || !classification.is_ordinary()
            || value.len() > MAX_INFORMATION_FLOW_SOURCE_BYTES
        {
            return Err(InformationFlowError::InvalidSource);
        }
        let secret_reference_hash = secret_reference
            .map(|reference| {
                if is_exact_secret_reference(reference) {
                    Ok(ContentHash::digest(reference.as_bytes()))
                } else {
                    Err(InformationFlowError::InvalidReference(
                        InformationFlowFinding::InvalidReference,
                    ))
                }
            })
            .transpose()?;
        let value_hash = ContentHash::digest(value);
        let material = InformationFlowSourceMaterial {
            identity: &identity,
            classification,
            secret_reference: secret_reference.is_some(),
            secret_reference_hash,
            value_hash,
        };
        let source_hash = hash_flow_material(b"agentmod.information-flow.source@1\0", &material)?;
        Ok(Self {
            identity,
            classification,
            secret_reference: secret_reference.is_some(),
            value_hash,
            source_hash,
        })
    }
}

/// Evaluates one deterministic no-declassification flow envelope.
///
/// # Errors
///
/// Rejects malformed or over-bound envelope inputs. Policy failures are
/// returned as deterministic [`InformationFlowDecision::Denied`] values.
pub fn evaluate_information_flow(
    identity: impl Into<String>,
    sink: InformationFlowSink,
    destination_classification: InformationFlowClassification,
    sources: &[InformationFlowSource],
) -> Result<(InformationFlowEnvelope, InformationFlowDecision), InformationFlowError> {
    let identity = identity.into();
    if !valid_flow_identity(&identity)
        || sources.is_empty()
        || sources.len() > MAX_INFORMATION_FLOW_SOURCES
        || sources.iter().any(|source| {
            !valid_flow_identity(&source.identity) || !source.classification.is_ordinary()
        })
    {
        return Err(InformationFlowError::InvalidEnvelope);
    }
    let mut joined = InformationFlowClassification::Public;
    let mut source_hashes = Vec::with_capacity(sources.len());
    for source in sources {
        joined = joined
            .join_ordinary(source.classification)
            .ok_or(InformationFlowError::InvalidEnvelope)?;
        source_hashes.push(source.source_hash);
    }
    source_hashes.sort_by_key(|hash| hash.to_hex());
    if source_hashes.iter().collect::<BTreeSet<_>>().len() != source_hashes.len() {
        return Err(InformationFlowError::InvalidEnvelope);
    }
    let contains_secret_references = sources.iter().any(|source| source.secret_reference);
    let envelope_material = InformationFlowEnvelopeMaterial {
        identity: &identity,
        sink,
        destination_classification,
        joined_classification: joined,
        contains_secret_references,
        source_hashes: &source_hashes,
    };
    let envelope_hash = hash_flow_material(
        b"agentmod.information-flow.envelope@1\0",
        &envelope_material,
    )?;
    let envelope = InformationFlowEnvelope {
        identity,
        sink,
        destination_classification,
        joined_classification: joined,
        contains_secret_references,
        source_hashes,
        envelope_hash,
    };
    let decision = flow_decision(&envelope, sources)?;
    Ok((envelope, decision))
}

/// Validates the only v1 secret-reference capability syntax.
///
/// This test is exact and case-sensitive; `secret:`, URI-shaped values,
/// whitespace, control characters, and prefix near-misses are rejected.
#[must_use]
pub fn is_exact_secret_reference(reference: &str) -> bool {
    reference.len() <= MAX_INFORMATION_FLOW_REFERENCE_BYTES
        && reference
            .strip_prefix("secret-ref:")
            .is_some_and(valid_opaque_reference_value)
}

fn flow_decision(
    envelope: &InformationFlowEnvelope,
    sources: &[InformationFlowSource],
) -> Result<InformationFlowDecision, InformationFlowError> {
    if !envelope.destination_classification.is_ordinary() {
        return denied_flow_decision(
            envelope,
            InformationFlowDenialCode::InvalidDestinationClassification,
            "destination classification is not an ordinary v1 class",
            sources.iter().map(|source| source.source_hash),
        );
    }
    let maximum = envelope.sink.maximum_classification();
    if !maximum.dominates_ordinary(envelope.destination_classification)
        || !maximum.dominates_ordinary(envelope.joined_classification)
    {
        let offending = sources
            .iter()
            .filter(|source| !maximum.dominates_ordinary(source.classification))
            .map(|source| source.source_hash)
            .collect::<Vec<_>>();
        return denied_flow_decision(
            envelope,
            InformationFlowDenialCode::SinkClearanceExceeded,
            "source or destination exceeds the bounded sink clearance",
            if offending.is_empty() {
                sources.iter().map(|source| source.source_hash).collect()
            } else {
                offending
            },
        );
    }
    if !envelope
        .destination_classification
        .dominates_ordinary(envelope.joined_classification)
    {
        return denied_flow_decision(
            envelope,
            InformationFlowDenialCode::DeclassificationProhibited,
            "destination classification does not dominate every source",
            sources
                .iter()
                .filter(|source| {
                    !envelope
                        .destination_classification
                        .dominates_ordinary(source.classification)
                })
                .map(|source| source.source_hash),
        );
    }
    if envelope.contains_secret_references && !envelope.sink.permits_secret_references() {
        return denied_flow_decision(
            envelope,
            InformationFlowDenialCode::SecretReferenceProhibited,
            "sink does not admit secret-reference capabilities",
            sources
                .iter()
                .filter(|source| source.secret_reference)
                .map(|source| source.source_hash),
        );
    }
    let material = InformationFlowAllowedDecisionMaterial {
        decision: "allowed",
        envelope_hash: envelope.envelope_hash,
    };
    let decision_hash = hash_flow_material(b"agentmod.information-flow.decision@1\0", &material)?;
    Ok(InformationFlowDecision::Allowed {
        envelope_hash: envelope.envelope_hash,
        decision_hash,
    })
}

fn denied_flow_decision(
    envelope: &InformationFlowEnvelope,
    code: InformationFlowDenialCode,
    reason: &'static str,
    offending: impl IntoIterator<Item = ContentHash>,
) -> Result<InformationFlowDecision, InformationFlowError> {
    let mut offending_source_hashes = offending.into_iter().collect::<Vec<_>>();
    offending_source_hashes.sort_by_key(|hash| hash.to_hex());
    offending_source_hashes.dedup();
    let material = InformationFlowDeniedDecisionMaterial {
        decision: "denied",
        envelope_hash: envelope.envelope_hash,
        code,
        reason,
        offending_source_hashes: &offending_source_hashes,
    };
    let decision_hash = hash_flow_material(b"agentmod.information-flow.decision@1\0", &material)?;
    Ok(InformationFlowDecision::Denied {
        envelope_hash: envelope.envelope_hash,
        code,
        reason: reason.to_owned(),
        offending_source_hashes,
        decision_hash,
    })
}

fn valid_flow_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity.len() <= MAX_INFORMATION_FLOW_IDENTITY_BYTES
        && identity
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn hash_flow_material(
    domain: &[u8],
    material: &impl Serialize,
) -> Result<ContentHash, InformationFlowError> {
    let bytes = serde_json::to_vec(material).map_err(|_| InformationFlowError::Hash)?;
    Ok(ContentHash::digest(&[domain, bytes.as_slice()].concat()))
}

#[derive(Serialize)]
struct InformationFlowSourceMaterial<'a> {
    identity: &'a str,
    classification: InformationFlowClassification,
    secret_reference: bool,
    secret_reference_hash: Option<ContentHash>,
    value_hash: ContentHash,
}

#[derive(Serialize)]
struct InformationFlowEnvelopeMaterial<'a> {
    identity: &'a str,
    sink: InformationFlowSink,
    destination_classification: InformationFlowClassification,
    joined_classification: InformationFlowClassification,
    contains_secret_references: bool,
    source_hashes: &'a [ContentHash],
}

#[derive(Serialize)]
struct InformationFlowAllowedDecisionMaterial {
    decision: &'static str,
    envelope_hash: ContentHash,
}

#[derive(Serialize)]
struct InformationFlowDeniedDecisionMaterial<'a> {
    decision: &'static str,
    envelope_hash: ContentHash,
    code: InformationFlowDenialCode,
    reason: &'static str,
    offending_source_hashes: &'a [ContentHash],
}

/// Classifies a bounded canonical conversation entry.
///
/// User and assistant text are inspected before admission. Privileged entry
/// kinds receive an explicit class even when a caller later excludes them.
///
/// # Errors
///
/// Fails closed when inspected text contains secret material or exceeds the
/// bounded inspection window.
#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive entry-kind match makes every retained information-flow class explicit"
)]
pub fn classify_conversation_entry(
    entry: &ConversationEntry,
) -> Result<ClassifiedConversationEntry, InformationFlowError> {
    let (kind, classification, inspected) = match entry {
        ConversationEntry::SystemInstruction(text) => (
            "system_instruction",
            InformationFlowClassification::Confidential,
            Some(text.text.as_str()),
        ),
        ConversationEntry::UserMessage(text) => (
            "user_message",
            InformationFlowClassification::Private,
            Some(text.text.as_str()),
        ),
        ConversationEntry::AssistantMessage(text) => (
            "assistant_message",
            InformationFlowClassification::Internal,
            Some(text.text.as_str()),
        ),
        ConversationEntry::ProjectInstruction(text) => (
            "project_instruction",
            InformationFlowClassification::Confidential,
            Some(text.text.as_str()),
        ),
        ConversationEntry::UserInstruction(text) => (
            "user_instruction",
            InformationFlowClassification::Private,
            Some(text.text.as_str()),
        ),
        ConversationEntry::RuntimeAnnotation(text) => (
            "runtime_annotation",
            InformationFlowClassification::Internal,
            Some(text.text.as_str()),
        ),
        ConversationEntry::ToolCallRequest(_) => (
            "tool_call_request",
            InformationFlowClassification::Confidential,
            None,
        ),
        ConversationEntry::ToolResult(result) => (
            "tool_result",
            InformationFlowClassification::Confidential,
            Some(result.content.as_str()),
        ),
        ConversationEntry::Attachment(_)
        | ConversationEntry::Image(_)
        | ConversationEntry::ArtifactReference(_) => (
            "artifact_reference",
            InformationFlowClassification::Private,
            None,
        ),
        ConversationEntry::ContextSummary(summary) => (
            "context_summary",
            InformationFlowClassification::Private,
            Some(summary.text.as_str()),
        ),
        ConversationEntry::RetrievedMemory(memory) => {
            let classification = memory.typed_provenance.as_ref().map_or(
                InformationFlowClassification::Private,
                |provenance| match provenance.security_classification.as_str() {
                    "public" => InformationFlowClassification::Public,
                    "internal" => InformationFlowClassification::Internal,
                    "private" => InformationFlowClassification::Private,
                    "confidential" => InformationFlowClassification::Confidential,
                    "secret_reference" => InformationFlowClassification::SecretReference,
                    _ => InformationFlowClassification::Secret,
                },
            );
            (
                "retrieved_memory",
                classification,
                Some(memory.content.as_str()),
            )
        }
        ConversationEntry::ProviderVisibleMetadata(_) => (
            "provider_visible_metadata",
            InformationFlowClassification::Confidential,
            None,
        ),
        ConversationEntry::PendingTask(_) => (
            "pending_task",
            InformationFlowClassification::Confidential,
            None,
        ),
        ConversationEntry::ActiveProcessSummary(_) => (
            "active_process_summary",
            InformationFlowClassification::Confidential,
            None,
        ),
        ConversationEntry::ChildAgentHandoff(handoff) => (
            "child_agent_handoff",
            InformationFlowClassification::Confidential,
            Some(handoff.summary.as_str()),
        ),
    };
    if classification == InformationFlowClassification::Secret {
        return Err(InformationFlowError::SensitiveEntry {
            entry_id: entry.id().0.clone(),
            finding: InformationFlowFinding::UnclassifiedSecret,
        });
    }
    if let Some(text) = inspected
        && let Some(finding) = detect_sensitive_text(text)
    {
        return Err(InformationFlowError::SensitiveEntry {
            entry_id: entry.id().0.clone(),
            finding,
        });
    }
    Ok(ClassifiedConversationEntry {
        entry_id: entry.id().0.clone(),
        entry_kind: kind.to_owned(),
        classification,
    })
}

/// Builds the only conversation projection eligible for automatic memory.
///
/// The projection deliberately retains user and assistant messages only, but
/// still runs the runtime-owned bounded classifier over every retained entry.
/// A single secret or over-bound value rejects the complete projection.
///
/// # Errors
///
/// Fails closed when any retained entry cannot be safely classified.
pub fn classify_automatic_memory_entries<'a>(
    entries: impl IntoIterator<Item = &'a ConversationEntry>,
) -> Result<ClassifiedAutomaticMemoryEntries, InformationFlowError> {
    let mut retained = Vec::new();
    let mut information_flow = Vec::new();
    for entry in entries {
        if !matches!(
            entry,
            ConversationEntry::UserMessage(_) | ConversationEntry::AssistantMessage(_)
        ) {
            continue;
        }
        information_flow.push(classify_conversation_entry(entry)?);
        retained.push(entry.clone());
    }
    Ok(ClassifiedAutomaticMemoryEntries {
        entries: retained,
        information_flow,
    })
}

/// Detects high-confidence secret material using a bounded scan.
#[must_use]
pub fn detect_sensitive_text(text: &str) -> Option<InformationFlowFinding> {
    if text.len() > MAX_INFORMATION_FLOW_TEXT_BYTES {
        return Some(InformationFlowFinding::InspectionBoundExceeded);
    }
    if text.chars().any(|character| character == '\0') {
        return Some(InformationFlowFinding::ControlCharacter);
    }
    let lowercase = text.to_ascii_lowercase();
    if contains_external_handle(&lowercase) {
        return Some(InformationFlowFinding::ExternalHandle);
    }
    if [
        "-----begin private key-----",
        "-----begin rsa private key-----",
        "-----begin ec private key-----",
        "-----begin openssh private key-----",
        "ssh-private-key",
    ]
    .iter()
    .any(|marker| lowercase.contains(marker))
    {
        return Some(InformationFlowFinding::PrivateKeyMaterial);
    }
    if ["authorization:", "proxy-authorization:"]
        .iter()
        .any(|marker| lowercase.contains(marker))
        || contains_bearer_token(&lowercase)
        || contains_uri_user_info(&lowercase)
    {
        return Some(InformationFlowFinding::AuthorizationMaterial);
    }
    if [
        "password",
        "passwd",
        "api_key",
        "api-key",
        "apikey",
        "client_secret",
        "access_token",
        "refresh_token",
        "id_token",
        "secret_key",
        "secret_access_key",
        "aws_secret_access_key",
        "private_key",
    ]
    .iter()
    .any(|name| contains_sensitive_assignment(&lowercase, name))
    {
        return Some(InformationFlowFinding::CredentialAssignment);
    }
    if contains_credential_prefix(text) {
        return Some(InformationFlowFinding::CredentialPrefix);
    }
    if text
        .split(|character: char| character.is_whitespace() || matches!(character, '"' | '\''))
        .any(looks_like_jwt)
    {
        return Some(InformationFlowFinding::SignedToken);
    }
    None
}

/// Validates and classifies one opaque runtime reference.
///
/// # Errors
///
/// Rejects path/URL/pipe/process handles, control characters, unknown reference
/// domains, embedded credentials, and malformed digest-backed references.
pub fn classify_runtime_reference(
    reference: &str,
) -> Result<InformationFlowClassification, InformationFlowError> {
    if reference.trim().is_empty()
        || reference.len() > MAX_INFORMATION_FLOW_REFERENCE_BYTES
        || reference.chars().any(char::is_control)
    {
        return Err(InformationFlowError::InvalidReference(
            InformationFlowFinding::ControlCharacter,
        ));
    }
    let lowercase = reference.to_ascii_lowercase();
    if lowercase.contains("://")
        || lowercase.starts_with(r"\\.\pipe")
        || lowercase.starts_with("pipe:")
        || lowercase.starts_with("process:")
        || lowercase.starts_with("handle:")
        || lowercase.starts_with('/')
        || lowercase.starts_with("./")
        || lowercase.starts_with("../")
        || reference.contains('\\')
    {
        return Err(InformationFlowError::InvalidReference(
            InformationFlowFinding::ExternalHandle,
        ));
    }
    if let Some(finding) = detect_sensitive_text(reference) {
        return Err(InformationFlowError::InvalidReference(finding));
    }
    if let Some(value) = reference.strip_prefix("artifact:blake3:") {
        return exact_lower_hex(value, 64)
            // This class belongs only to the opaque reference token. Artifact
            // content requires an authoritative source label at its owning
            // command boundary and must never be inferred from reference shape.
            .then_some(InformationFlowClassification::Internal)
            .ok_or(InformationFlowError::InvalidReference(
                InformationFlowFinding::InvalidReference,
            ));
    }
    if let Some(value) = reference.strip_prefix("plugin-receipt:") {
        return exact_lower_hex(value, 64)
            .then_some(InformationFlowClassification::Internal)
            .ok_or(InformationFlowError::InvalidReference(
                InformationFlowFinding::InvalidReference,
            ));
    }
    if let Some(value) = reference.strip_prefix("secret-ref:") {
        return valid_opaque_reference_value(value)
            .then_some(InformationFlowClassification::SecretReference)
            .ok_or(InformationFlowError::InvalidReference(
                InformationFlowFinding::InvalidReference,
            ));
    }
    for prefix in [
        "provider-result:",
        "provider-tool-batch:",
        "tool-result:",
        "tool:",
        "node-result:",
        "approval-result:",
        "generic-approval:",
        "declarative-approval:",
        "child-result:",
        "child-wait:",
        "child-message:",
        "children:",
        "children-completed:",
        "context:",
        "loop:",
        "schedule-stored:",
        "schedule-resumed:",
        "user-space-event:",
    ] {
        if let Some(value) = reference.strip_prefix(prefix) {
            return valid_opaque_reference_value(value)
                .then_some(InformationFlowClassification::Internal)
                .ok_or(InformationFlowError::InvalidReference(
                    InformationFlowFinding::InvalidReference,
                ));
        }
    }
    if matches!(reference, "artifact-persisted" | "child-result") {
        return Ok(InformationFlowClassification::Internal);
    }
    Err(InformationFlowError::InvalidReference(
        InformationFlowFinding::InvalidReference,
    ))
}

fn contains_sensitive_assignment(text: &str, name: &str) -> bool {
    text.match_indices(name).any(|(index, _)| {
        if text[..index]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return false;
        }
        let suffix = text[index + name.len()..].trim_start();
        suffix.starts_with('=')
            || suffix.starts_with(':')
            || suffix.starts_with("=>")
            || suffix.starts_with('"') && suffix[1..].trim_start().starts_with(':')
    })
}

fn contains_bearer_token(text: &str) -> bool {
    text.match_indices("bearer ").any(|(index, _)| {
        let token = text[index + "bearer ".len()..]
            .split(|character: char| {
                character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ';')
            })
            .next()
            .unwrap_or_default();
        token.len() >= 16
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    })
}

fn contains_uri_user_info(text: &str) -> bool {
    text.split_whitespace().any(|token| {
        token
            .split_once("://")
            .and_then(|(_, authority)| authority.split('/').next())
            .is_some_and(|authority| {
                authority
                    .split_once('@')
                    .and_then(|(credentials, _)| credentials.split_once(':'))
                    .is_some_and(|(user, password)| !user.is_empty() && !password.is_empty())
            })
    })
}

fn contains_external_handle(text: &str) -> bool {
    text.split_whitespace().any(|token| {
        let token = token.trim_matches(|character: char| {
            matches!(
                character,
                '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        });
        token.contains("://")
            || token.starts_with(r"\\.\pipe")
            || token.starts_with("pipe:")
            || token.starts_with("process:")
            || token.starts_with("handle:")
            || token.len() > 1
                && (token.starts_with('/')
                    || token.starts_with("./")
                    || token.starts_with("../")
                    || token.starts_with(r"\\"))
            || token.as_bytes().get(1) == Some(&b':')
                && token
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic)
                && token
                    .as_bytes()
                    .get(2)
                    .is_some_and(|separator| matches!(separator, b'\\' | b'/'))
    })
}

fn contains_credential_prefix(text: &str) -> bool {
    text.split(|character: char| {
        character.is_whitespace()
            || matches!(character, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']')
    })
    .any(|token| {
        let trimmed = token.trim_matches(|character: char| {
            !character.is_ascii_alphanumeric() && !matches!(character, '_' | '-')
        });
        (trimmed.starts_with("AKIA")
            && trimmed.len() == 20
            && trimmed
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()))
            || (trimmed.starts_with("ASIA")
                && trimmed.len() == 20
                && trimmed
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()))
            || [
                "ghp_",
                "gho_",
                "ghu_",
                "ghs_",
                "github_pat_",
                "xoxb-",
                "xoxp-",
                "xoxa-",
            ]
            .iter()
            .any(|prefix| trimmed.starts_with(prefix) && trimmed.len() >= prefix.len() + 16)
            || (trimmed.starts_with("sk-") && trimmed.len() >= 24)
    })
}

fn looks_like_jwt(token: &str) -> bool {
    if token.len() < 32 || token.len() > 4096 || !token.starts_with("eyJ") {
        return false;
    }
    let mut segments = token.split('.');
    let Some(header) = segments.next() else {
        return false;
    };
    let Some(payload) = segments.next() else {
        return false;
    };
    let Some(signature) = segments.next() else {
        return false;
    };
    segments.next().is_none()
        && [header, payload, signature].iter().all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_opaque_reference_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_primitives::Sequence;

    use crate::conversation::{ConversationEntryId, TextEntry};

    fn text_entry(kind: &str, text: &str) -> ConversationEntry {
        let entry = TextEntry {
            id: ConversationEntryId(format!("{kind}:1")),
            text: text.to_owned(),
            source_sequence: Sequence::new(1).expect("sequence"),
        };
        match kind {
            "user" => ConversationEntry::UserMessage(entry),
            "assistant" => ConversationEntry::AssistantMessage(entry),
            _ => ConversationEntry::SystemInstruction(entry),
        }
    }

    fn source(
        identity: &str,
        classification: InformationFlowClassification,
    ) -> InformationFlowSource {
        InformationFlowSource::from_bytes(identity, classification, identity.as_bytes(), None)
            .expect("source")
    }

    fn secret_reference_source(identity: &str) -> InformationFlowSource {
        InformationFlowSource::from_bytes(
            identity,
            InformationFlowClassification::Confidential,
            b"opaque capability",
            Some("secret-ref:vault_record_17"),
        )
        .expect("secret reference source")
    }

    fn decision_for(
        sink: InformationFlowSink,
        destination: InformationFlowClassification,
        sources: &[InformationFlowSource],
    ) -> InformationFlowDecision {
        evaluate_information_flow("flow:test", sink, destination, sources)
            .expect("flow envelope")
            .1
    }

    #[test]
    fn ordinary_lattice_join_and_dominance_are_explicit_and_exhaustive() {
        use InformationFlowClassification::{
            Confidential, Internal, Private, Public, Secret, SecretReference,
        };

        let ordinary = [Public, Internal, Private, Confidential];
        for (left_index, left) in ordinary.iter().copied().enumerate() {
            for (right_index, right) in ordinary.iter().copied().enumerate() {
                let expected = ordinary[left_index.max(right_index)];
                assert_eq!(left.join_ordinary(right), Some(expected));
                assert_eq!(left.dominates_ordinary(right), left_index >= right_index);
            }
        }
        for orthogonal in [SecretReference, Secret] {
            for ordinary in ordinary {
                assert_eq!(orthogonal.join_ordinary(ordinary), None);
                assert_eq!(ordinary.join_ordinary(orthogonal), None);
                assert!(!orthogonal.dominates_ordinary(ordinary));
                assert!(!ordinary.dominates_ordinary(orthogonal));
            }
        }
    }

    #[test]
    fn every_sink_enforces_its_exact_clearance_and_reference_boundary() {
        use InformationFlowClassification::{Confidential, Internal, Private};

        for sink in [
            InformationFlowSink::ModelProjection,
            InformationFlowSink::FrontendProjection,
            InformationFlowSink::PluginInvocation,
            InformationFlowSink::Artifact,
        ] {
            assert!(matches!(
                decision_for(sink, Confidential, &[source("confidential", Confidential)]),
                InformationFlowDecision::Allowed { .. }
            ));
            assert!(matches!(
                decision_for(sink, Confidential, &[secret_reference_source("secret")]),
                InformationFlowDecision::Denied {
                    code: InformationFlowDenialCode::SecretReferenceProhibited,
                    ..
                }
            ));
        }
        assert!(matches!(
            decision_for(
                InformationFlowSink::ExternalNetwork,
                Internal,
                &[source("internal", Internal)]
            ),
            InformationFlowDecision::Allowed { .. }
        ));
        assert!(matches!(
            decision_for(
                InformationFlowSink::ExternalNetwork,
                Private,
                &[source("private", Private)]
            ),
            InformationFlowDecision::Denied {
                code: InformationFlowDenialCode::SinkClearanceExceeded,
                ..
            }
        ));
        assert!(matches!(
            decision_for(
                InformationFlowSink::AutomaticMemory,
                Private,
                &[source("private", Private)]
            ),
            InformationFlowDecision::Allowed { .. }
        ));
        assert!(matches!(
            decision_for(
                InformationFlowSink::AutomaticMemory,
                Confidential,
                &[source("confidential", Confidential)]
            ),
            InformationFlowDecision::Denied {
                code: InformationFlowDenialCode::SinkClearanceExceeded,
                ..
            }
        ));
        for sink in [
            InformationFlowSink::LocalTool,
            InformationFlowSink::ChildMessage,
        ] {
            assert!(matches!(
                decision_for(sink, Confidential, &[secret_reference_source("secret")]),
                InformationFlowDecision::Allowed { .. }
            ));
        }
    }

    #[test]
    fn exact_secret_reference_syntax_rejects_every_near_miss() {
        for valid in [
            "secret-ref:vault_record_17",
            "secret-ref:session-mcp:session_1:server_2:token:0",
        ] {
            assert!(is_exact_secret_reference(valid), "{valid}");
            InformationFlowSource::from_bytes(
                "secret",
                InformationFlowClassification::Confidential,
                valid.as_bytes(),
                Some(valid),
            )
            .expect(valid);
        }
        for invalid in [
            "secret:value",
            "secret-ref:",
            "Secret-ref:value",
            "secret-ref://vault/value",
            " secret-ref:value",
            "secret-ref:value ",
            "prefix-secret-ref:value",
            "secret-ref:value/path",
            "secret-ref:value\0tail",
        ] {
            assert!(!is_exact_secret_reference(invalid), "{invalid:?}");
            assert!(
                InformationFlowSource::from_bytes(
                    "secret",
                    InformationFlowClassification::Confidential,
                    invalid.as_bytes(),
                    Some(invalid),
                )
                .is_err()
            );
        }
        let ordinary = InformationFlowSource::from_bytes(
            "ordinary",
            InformationFlowClassification::Internal,
            b"prose mentioning secret-ref:value is not a dedicated capability field",
            None,
        )
        .expect("ordinary source");
        assert!(!ordinary.secret_reference);
    }

    #[test]
    fn deterministic_envelope_and_decision_hashes_ignore_source_input_order() {
        let first = source("first", InformationFlowClassification::Internal);
        let second = source("second", InformationFlowClassification::Private);
        let left = evaluate_information_flow(
            "flow:deterministic",
            InformationFlowSink::ChildMessage,
            InformationFlowClassification::Private,
            &[first.clone(), second.clone()],
        )
        .expect("left flow");
        let right = evaluate_information_flow(
            "flow:deterministic",
            InformationFlowSink::ChildMessage,
            InformationFlowClassification::Private,
            &[second, first],
        )
        .expect("right flow");
        assert_eq!(left, right);

        let denied_left = evaluate_information_flow(
            "flow:denied",
            InformationFlowSink::ChildMessage,
            InformationFlowClassification::Internal,
            &[
                source("first", InformationFlowClassification::Private),
                source("second", InformationFlowClassification::Confidential),
            ],
        )
        .expect("denied left");
        let denied_right = evaluate_information_flow(
            "flow:denied",
            InformationFlowSink::ChildMessage,
            InformationFlowClassification::Internal,
            &[
                source("second", InformationFlowClassification::Confidential),
                source("first", InformationFlowClassification::Private),
            ],
        )
        .expect("denied right");
        assert_eq!(denied_left, denied_right);
    }

    #[test]
    fn v1_never_implicitly_downgrades_or_declassifies() {
        let private = source("private", InformationFlowClassification::Private);
        let (envelope, decision) = evaluate_information_flow(
            "flow:no-downgrade",
            InformationFlowSink::ChildMessage,
            InformationFlowClassification::Internal,
            std::slice::from_ref(&private),
        )
        .expect("denied flow");
        assert_eq!(
            envelope.joined_classification,
            InformationFlowClassification::Private
        );
        assert_eq!(envelope.source_hashes, vec![private.source_hash]);
        assert!(matches!(
            decision,
            InformationFlowDecision::Denied {
                code: InformationFlowDenialCode::DeclassificationProhibited,
                offending_source_hashes,
                ..
            } if offending_source_hashes == vec![private.source_hash]
        ));
    }

    #[test]
    fn envelope_and_source_bounds_fail_closed() {
        assert!(
            InformationFlowSource::from_bytes(
                "x".repeat(MAX_INFORMATION_FLOW_IDENTITY_BYTES + 1),
                InformationFlowClassification::Internal,
                b"value",
                None,
            )
            .is_err()
        );
        assert!(
            InformationFlowSource::from_bytes(
                "source",
                InformationFlowClassification::SecretReference,
                b"value",
                Some("secret-ref:value"),
            )
            .is_err()
        );
        let over_bound = vec![0; MAX_INFORMATION_FLOW_SOURCE_BYTES + 1];
        assert!(
            InformationFlowSource::from_bytes(
                "source",
                InformationFlowClassification::Internal,
                &over_bound,
                None,
            )
            .is_err()
        );
        let sources = (0..=MAX_INFORMATION_FLOW_SOURCES)
            .map(|index| {
                source(
                    &format!("source:{index}"),
                    InformationFlowClassification::Public,
                )
            })
            .collect::<Vec<_>>();
        assert!(
            evaluate_information_flow(
                "flow:over-bound",
                InformationFlowSink::FrontendProjection,
                InformationFlowClassification::Public,
                &sources,
            )
            .is_err()
        );
        let duplicate = source("duplicate", InformationFlowClassification::Internal);
        assert!(
            evaluate_information_flow(
                "flow:duplicate",
                InformationFlowSink::ChildMessage,
                InformationFlowClassification::Internal,
                &[duplicate.clone(), duplicate],
            )
            .is_err()
        );
    }

    #[test]
    fn text_entries_receive_explicit_stable_classes() {
        assert_eq!(
            classify_conversation_entry(&text_entry("user", "bounded user text"))
                .expect("user classification")
                .classification,
            InformationFlowClassification::Private
        );
        assert_eq!(
            classify_conversation_entry(&text_entry("assistant", "bounded response"))
                .expect("assistant classification")
                .classification,
            InformationFlowClassification::Internal
        );
        assert_eq!(
            classify_conversation_entry(&text_entry("system", "bounded policy"))
                .expect("system classification")
                .classification,
            InformationFlowClassification::Confidential
        );
    }

    #[test]
    fn high_confidence_secret_shapes_fail_closed() {
        for secret in [
            "password = correct-horse-battery-staple",
            "Authorization: Bearer opaque-value",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
            "AKIAIOSFODNN7EXAMPLE",
            "ghp_0123456789abcdefghijklmnop",
            "xoxb-12345678901234567890",
            "sk-1234567890abcdefghijklmnop",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature",
            "https://user:password@example.invalid/path",
            "file:///private/runtime.sock",
            "../workspace/private.json",
            r"C:\Users\fixture\secret.txt",
            r"\\.\pipe\private-runtime",
            "HANDLE:0000000000000042",
        ] {
            assert!(
                classify_conversation_entry(&text_entry("user", secret)).is_err(),
                "{secret}"
            );
        }
    }

    #[test]
    fn ordinary_hyphenated_text_does_not_trigger_prefix_detection() {
        for ordinary in [
            "sketch a key-value diagram",
            "discuss task-token accounting",
            "the password policy changed",
            "this is a basic authorization overview",
            "a bearer instrument is a financial instrument",
            "the notpassword: field is ordinary prose",
            "authorization is required",
        ] {
            assert_eq!(detect_sensitive_text(ordinary), None, "{ordinary}");
        }
    }

    #[test]
    fn portable_references_are_allowlisted_and_external_handles_fail_closed() {
        let artifact = format!("artifact:blake3:{}", "a".repeat(64));
        assert_eq!(
            classify_runtime_reference(&artifact).expect("artifact"),
            InformationFlowClassification::Internal
        );
        assert_eq!(
            classify_runtime_reference("secret-ref:vault_record_17").expect("secret reference"),
            InformationFlowClassification::SecretReference
        );
        assert_eq!(
            classify_runtime_reference("provider-result:result_17").expect("result"),
            InformationFlowClassification::Internal
        );
        for reference in [
            "tool:call_17",
            "generic-approval:approval_17",
            "child-message:child_17:3",
            "schedule-resumed:schedule_17",
            "user-space-event:user.progress:9",
            "artifact-persisted",
        ] {
            assert_eq!(
                classify_runtime_reference(reference).expect(reference),
                InformationFlowClassification::Internal
            );
        }
        for invalid in [
            "file:///tmp/secret",
            "../workspace/file",
            r"\\.\pipe\handle",
            "https://example.invalid/object",
            "unknown:reference",
        ] {
            assert!(classify_runtime_reference(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn scan_bound_and_nul_are_rejected() {
        assert_eq!(
            detect_sensitive_text(&"x".repeat(MAX_INFORMATION_FLOW_TEXT_BYTES + 1)),
            Some(InformationFlowFinding::InspectionBoundExceeded)
        );
        assert_eq!(
            detect_sensitive_text("safe\0unsafe"),
            Some(InformationFlowFinding::ControlCharacter)
        );
    }

    #[test]
    fn automatic_memory_projection_is_per_entry_classified_and_fail_closed() {
        let user = text_entry("user", "bounded private goal");
        let assistant = text_entry("assistant", "bounded internal answer");
        let system = text_entry("system", "excluded privileged policy");
        let projection = classify_automatic_memory_entries([&system, &user, &assistant])
            .expect("classified projection");
        assert_eq!(projection.entries, vec![user, assistant]);
        assert_eq!(
            projection
                .information_flow
                .iter()
                .map(|entry| entry.classification)
                .collect::<Vec<_>>(),
            vec![
                InformationFlowClassification::Private,
                InformationFlowClassification::Internal
            ]
        );

        let secret = text_entry(
            "user",
            "Authorization: Bearer 0123456789abcdef0123456789abcdef",
        );
        assert!(classify_automatic_memory_entries([&secret]).is_err());
    }
}
