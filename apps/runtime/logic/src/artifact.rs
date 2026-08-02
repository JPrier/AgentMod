//! Runtime-owned immutable artifact proposal and persistence coordination.

use std::path::PathBuf;

use agentmod_event_pipeline::ActionCapabilities;
use agentmod_primitives::ContentHash;
use agentmod_runtime_data::artifact::{
    ArtifactDataError, ArtifactDataPort, ArtifactRetentionRecord, ArtifactSecurityRecord,
    InspectArtifactDataRequest, PersistArtifactDataRequest, PersistedArtifactDataRecord,
    ReadArtifactRangeDataRequest,
};
use thiserror::Error;

use crate::{
    action::{
        ActionProposal, ArtifactPersistenceAction, ArtifactPersistenceRetention,
        ArtifactPersistenceSecurity, ConsequentialAction, ProposalId,
    },
    harness::ProviderExecutionPolicy,
    interception::{InterceptionOutcome, intercept_action},
};

/// Maximum artifact content exposed by one runtime business read.
pub const MAX_ARTIFACT_RANGE_BYTES: u64 = 1024 * 1024;

/// Logic-owned exact bounded artifact range command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadArtifactRangeCommand {
    /// Session-scoped artifact store root selected by runtime orchestration.
    pub store_root: PathBuf,
    /// Exact immutable portable reference.
    pub artifact_reference: String,
    /// Canonically expected full-object digest.
    pub expected_content_hash: ContentHash,
    /// Canonically expected full-object size.
    pub expected_byte_size: u64,
    /// Zero-based byte offset.
    pub offset: u64,
    /// Exact bounded byte count.
    pub length: u64,
}

/// Logic-owned exact bounded artifact range result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadArtifactRangeResult {
    /// Exact requested bytes.
    pub bytes: Vec<u8>,
    /// Verified full-object byte size.
    pub artifact_bytes: u64,
    /// Verified full-object digest.
    pub content_hash: ContentHash,
}

/// Narrow logic port exposed to the runtime service boundary.
pub trait ArtifactReadLogicPort {
    /// Reads one exact range after validating canonical hash, size, and bounds.
    ///
    /// # Errors
    ///
    /// Fails closed for malformed, missing, corrupt, oversized, or substituted
    /// artifact content.
    fn read_artifact_range(
        &self,
        command: ReadArtifactRangeCommand,
    ) -> Result<ReadArtifactRangeResult, ArtifactReadError>;
}

/// Runtime-owned bounded immutable artifact reader.
#[derive(Clone)]
pub struct ArtifactReadLogic<D> {
    data: D,
}

impl<D> ArtifactReadLogic<D> {
    /// Creates bounded artifact read logic over the injected data boundary.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D: ArtifactDataPort> ArtifactReadLogicPort for ArtifactReadLogic<D> {
    fn read_artifact_range(
        &self,
        command: ReadArtifactRangeCommand,
    ) -> Result<ReadArtifactRangeResult, ArtifactReadError> {
        let expected_reference = format!("artifact:blake3:{}", command.expected_content_hash);
        let end = command
            .offset
            .checked_add(command.length)
            .ok_or(ArtifactReadError::InvalidRange)?;
        if command.store_root.as_os_str().is_empty()
            || command.artifact_reference != expected_reference
            || command.expected_byte_size == 0
            || command.length == 0
            || command.length > MAX_ARTIFACT_RANGE_BYTES
            || end > command.expected_byte_size
        {
            return Err(ArtifactReadError::InvalidRange);
        }
        let record = self
            .data
            .read_artifact_range(ReadArtifactRangeDataRequest {
                store_root: command.store_root,
                artifact_reference: command.artifact_reference,
                offset: command.offset,
                length: command.length,
            })
            .map_err(|error| match error {
                ArtifactDataError::NotFound => ArtifactReadError::NotFound,
                other => ArtifactReadError::Data(other),
            })?;
        if record.artifact_bytes != command.expected_byte_size
            || u64::try_from(record.bytes.len()).ok() != Some(command.length)
        {
            return Err(ArtifactReadError::Substituted);
        }
        Ok(ReadArtifactRangeResult {
            bytes: record.bytes,
            artifact_bytes: record.artifact_bytes,
            content_hash: command.expected_content_hash,
        })
    }
}

/// Bounded artifact range read failure.
#[derive(Debug, Error)]
pub enum ArtifactReadError {
    /// Hash, size, offset, or length violates the exact read contract.
    #[error("artifact range contract is invalid")]
    InvalidRange,
    /// The immutable artifact is missing.
    #[error("artifact was not found")]
    NotFound,
    /// The dependency result differs from the expected immutable object.
    #[error("artifact content was substituted")]
    Substituted,
    /// The data boundary failed.
    #[error("artifact read failed: {0}")]
    Data(#[source] ArtifactDataError),
}

/// Logic-owned immutable artifact persistence command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistArtifactCommand {
    /// Stable proposal identifier.
    pub proposal_id: String,
    /// Owning style identity.
    pub style: String,
    /// Safe workspace label.
    pub workspace: String,
    /// Session-scoped artifact store root.
    pub store_root: PathBuf,
    /// Canonical proposal event ID.
    pub creation_event: String,
    /// Producer identity.
    pub producer: String,
    /// Valid media type.
    pub mime_type: String,
    /// Exact approved bytes.
    pub bytes: Vec<u8>,
    /// Security handling classification.
    pub security: ArtifactSecurity,
    /// Retention policy.
    pub retention: ArtifactRetention,
}

/// Logic-owned security classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactSecurity {
    /// Ordinary workspace content.
    Standard,
    /// User-private content.
    Private,
    /// Secret-bearing content.
    Secret,
}

/// Logic-owned retention selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactRetention {
    /// Retain until explicit removal policy acts.
    Permanent,
    /// Retain with the owning session.
    Session,
    /// Retain until a portable Unix timestamp in milliseconds.
    UntilUnixMilliseconds(i64),
}

impl ArtifactRetention {
    const fn action(self) -> ArtifactPersistenceRetention {
        match self {
            Self::Permanent => ArtifactPersistenceRetention::Permanent,
            Self::Session => ArtifactPersistenceRetention::Session,
            Self::UntilUnixMilliseconds(expires_at_millis) => {
                ArtifactPersistenceRetention::UntilUnixMilliseconds { expires_at_millis }
            }
        }
    }
}

impl ArtifactSecurity {
    const fn action(self) -> ArtifactPersistenceSecurity {
        match self {
            Self::Standard => ArtifactPersistenceSecurity::Standard,
            Self::Private => ArtifactPersistenceSecurity::Private,
            Self::Secret => ArtifactPersistenceSecurity::Secret,
        }
    }
}

/// Prepared proposal which has not yet traversed blocking policy.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedArtifactPersistence {
    /// Original immutable proposal.
    pub original: ActionProposal,
    command: PersistArtifactCommand,
}

/// Authorized persistence request bound to the final action digest.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthorizedArtifactPersistence {
    /// Original immutable proposal.
    pub original: ActionProposal,
    /// Final executable proposal.
    pub executable: ActionProposal,
    command: PersistArtifactCommand,
}

/// Logic-owned immutable artifact result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedArtifact {
    /// Content-addressed artifact identity.
    pub artifact_id: String,
    /// Portable immutable reference.
    pub artifact_reference: String,
    /// Exact media type.
    pub mime_type: String,
    /// Exact byte count.
    pub byte_size: u64,
    /// Canonical event that first created this content-addressed object.
    pub creation_event: String,
    /// Original producer.
    pub producer: String,
    /// Exact security classification retained with the object.
    pub security: ArtifactSecurity,
    /// Exact retention contract retained with the object.
    pub retention: ArtifactRetention,
    /// Lowercase BLAKE3 digest.
    pub content_hash: String,
    /// Whether an identical immutable object was reused.
    pub deduplicated: bool,
}

/// Runtime artifact business logic over an injected data boundary.
#[derive(Clone)]
pub struct ArtifactPersistenceLogic<D> {
    data: D,
    policy: ProviderExecutionPolicy,
}

impl<D> ArtifactPersistenceLogic<D> {
    /// Creates artifact logic with the same mandatory blocking and permission
    /// chain used by model and tool actions.
    #[must_use]
    pub const fn new(data: D, policy: ProviderExecutionPolicy) -> Self {
        Self { data, policy }
    }

    /// Builds an immutable proposal without policy evaluation or persistence.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPersistenceError`] for invalid identifiers, paths, or
    /// empty/oversized content.
    pub fn prepare(
        &self,
        command: PersistArtifactCommand,
    ) -> Result<PreparedArtifactPersistence, ArtifactPersistenceError> {
        validate(&command)?;
        let byte_size =
            u64::try_from(command.bytes.len()).map_err(|_| ArtifactPersistenceError::Invalid)?;
        let original = ActionProposal {
            id: ProposalId(command.proposal_id.clone()),
            action: ConsequentialAction::ArtifactPersistence(ArtifactPersistenceAction {
                content_hash: ContentHash::digest(&command.bytes),
                mime_type: command.mime_type.clone(),
                byte_size,
                security: command.security.action(),
                retention: command.retention.action(),
            }),
            style: command.style.clone(),
            workspace: command.workspace.clone(),
            origin: command.producer.clone(),
        };
        Ok(PreparedArtifactPersistence { original, command })
    }

    /// Evaluates the prepared proposal through the mandatory blocking and
    /// permission chain.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPersistenceError`] for any non-approved outcome or an
    /// interceptor replacement which changes the exact bytes contract.
    pub async fn authorize_prepared(
        &self,
        prepared: PreparedArtifactPersistence,
    ) -> Result<AuthorizedArtifactPersistence, ArtifactPersistenceError> {
        let result = intercept_action(
            prepared.original.clone(),
            &self.policy.style_pipeline,
            &self.policy.plugin_pipeline,
            ActionCapabilities::all(),
            &self.policy.user_policy,
            &self.policy.mandatory_policy,
        )
        .await;
        let executable = match result.outcome {
            InterceptionOutcome::Approved { executable, .. } => executable,
            InterceptionOutcome::RequireApproval { reason, .. } => {
                return Err(ArtifactPersistenceError::ApprovalRequired(reason));
            }
            InterceptionOutcome::Rejected { reason } => {
                return Err(ArtifactPersistenceError::Rejected(reason));
            }
            InterceptionOutcome::Cancelled { reason } => {
                return Err(ArtifactPersistenceError::Cancelled(reason));
            }
            InterceptionOutcome::Deferred { .. }
            | InterceptionOutcome::Forked { .. }
            | InterceptionOutcome::Aborted { .. } => {
                return Err(ArtifactPersistenceError::UnsupportedDecision);
            }
        };
        let ConsequentialAction::ArtifactPersistence(action) = &executable.action else {
            return Err(ArtifactPersistenceError::InvalidInterceptionReplacement);
        };
        let exact = ArtifactPersistenceAction {
            content_hash: ContentHash::digest(&prepared.command.bytes),
            mime_type: prepared.command.mime_type.clone(),
            byte_size: u64::try_from(prepared.command.bytes.len())
                .map_err(|_| ArtifactPersistenceError::Invalid)?,
            security: prepared.command.security.action(),
            retention: prepared.command.retention.action(),
        };
        if action != &exact || executable != prepared.original {
            return Err(ArtifactPersistenceError::InvalidInterceptionReplacement);
        }
        Ok(AuthorizedArtifactPersistence {
            original: prepared.original,
            executable,
            command: prepared.command,
        })
    }

    /// Reconstructs the exact approved request from a canonical action digest
    /// without re-running blocking interceptors after restart.
    ///
    /// This is legal because this adapter rejects artifact interceptor
    /// replacements; the approved executable therefore equals the original
    /// proposal exactly.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPersistenceError`] when the reconstructed digest does
    /// not exactly match canonical approval state.
    pub fn restore_authorized(
        &self,
        command: PersistArtifactCommand,
        expected_digest: ContentHash,
    ) -> Result<AuthorizedArtifactPersistence, ArtifactPersistenceError> {
        let prepared = self.prepare(command)?;
        if prepared
            .original
            .digest()
            .map_err(|_| ArtifactPersistenceError::Invalid)?
            != expected_digest
        {
            return Err(ArtifactPersistenceError::InvalidReconciliation);
        }
        Ok(AuthorizedArtifactPersistence {
            executable: prepared.original.clone(),
            original: prepared.original,
            command: prepared.command,
        })
    }

    /// Persists one already-authorized exact request.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPersistenceError`] if the data boundary fails.
    pub fn persist_authorized(
        &self,
        authorized: AuthorizedArtifactPersistence,
    ) -> Result<PersistedArtifact, ArtifactPersistenceError>
    where
        D: ArtifactDataPort,
    {
        let expected = ExpectedPersistedArtifact::from_command(&authorized.command)?;
        let record = self
            .data
            .persist_artifact(PersistArtifactDataRequest {
                store_root: authorized.command.store_root,
                creation_event: authorized.command.creation_event,
                producer: authorized.command.producer,
                mime_type: authorized.command.mime_type,
                bytes: authorized.command.bytes,
                security: authorized.command.security.into(),
                retention: authorized.command.retention.into(),
            })
            .map_err(ArtifactPersistenceError::Data)?;
        expected.validate(&record)?;
        Ok(map_record(record))
    }

    /// Reconciles an exact content-addressed object after a dispatched write
    /// lost its canonical completion event.
    ///
    /// A missing object is returned as `None`; callers may safely retry the
    /// identical immutable write. Any present object must match the approved
    /// bytes contract exactly.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPersistenceError`] for corrupt or mismatched metadata.
    pub fn reconcile(
        &self,
        command: &PersistArtifactCommand,
    ) -> Result<Option<PersistedArtifact>, ArtifactPersistenceError>
    where
        D: ArtifactDataPort,
    {
        validate(command)?;
        let expected = ExpectedPersistedArtifact::from_command(command)?;
        let record = match self.data.inspect_artifact(InspectArtifactDataRequest {
            store_root: command.store_root.clone(),
            artifact_reference: format!("artifact:blake3:{}", expected.content_hash),
        }) {
            Ok(record) => record,
            Err(ArtifactDataError::NotFound) => return Ok(None),
            Err(error) => return Err(ArtifactPersistenceError::Data(error)),
        };
        expected.validate(&record)?;
        Ok(Some(map_record(record)))
    }
}

struct ExpectedPersistedArtifact {
    content_hash: String,
    mime_type: String,
    byte_size: u64,
    creation_event: String,
    producer: String,
    security: ArtifactSecurityRecord,
    retention: ArtifactRetentionRecord,
}

impl ExpectedPersistedArtifact {
    fn from_command(command: &PersistArtifactCommand) -> Result<Self, ArtifactPersistenceError> {
        Ok(Self {
            content_hash: ContentHash::digest(&command.bytes).to_hex(),
            mime_type: command.mime_type.clone(),
            byte_size: u64::try_from(command.bytes.len())
                .map_err(|_| ArtifactPersistenceError::Invalid)?,
            creation_event: command.creation_event.clone(),
            producer: command.producer.clone(),
            security: command.security.into(),
            retention: command.retention.into(),
        })
    }

    fn validate(
        &self,
        record: &PersistedArtifactDataRecord,
    ) -> Result<(), ArtifactPersistenceError> {
        let exact_provenance =
            record.creation_event == self.creation_event && record.producer == self.producer;
        if record.content_hash != self.content_hash
            || record.mime_type != self.mime_type
            || record.byte_size != self.byte_size
            || record.security != self.security
            || record.retention != self.retention
            || (!exact_provenance && !record.deduplicated)
        {
            return Err(ArtifactPersistenceError::InvalidReconciliation);
        }
        Ok(())
    }
}

fn validate(command: &PersistArtifactCommand) -> Result<(), ArtifactPersistenceError> {
    if command.proposal_id.trim().is_empty()
        || command.style.trim().is_empty()
        || command.workspace.trim().is_empty()
        || command.store_root.as_os_str().is_empty()
        || command.creation_event.trim().is_empty()
        || command.producer.trim().is_empty()
        || command.mime_type.trim().is_empty()
        || command.bytes.is_empty()
    {
        Err(ArtifactPersistenceError::Invalid)
    } else {
        Ok(())
    }
}

fn map_record(value: PersistedArtifactDataRecord) -> PersistedArtifact {
    PersistedArtifact {
        artifact_id: value.artifact_id,
        artifact_reference: value.artifact_reference,
        mime_type: value.mime_type,
        byte_size: value.byte_size,
        creation_event: value.creation_event,
        producer: value.producer,
        security: value.security.into(),
        retention: value.retention.into(),
        content_hash: value.content_hash,
        deduplicated: value.deduplicated,
    }
}

impl From<ArtifactSecurity> for ArtifactSecurityRecord {
    fn from(value: ArtifactSecurity) -> Self {
        match value {
            ArtifactSecurity::Standard => Self::Standard,
            ArtifactSecurity::Private => Self::Private,
            ArtifactSecurity::Secret => Self::Secret,
        }
    }
}

impl From<ArtifactRetention> for ArtifactRetentionRecord {
    fn from(value: ArtifactRetention) -> Self {
        match value {
            ArtifactRetention::Permanent => Self::Permanent,
            ArtifactRetention::Session => Self::Session,
            ArtifactRetention::UntilUnixMilliseconds(value) => Self::UntilUnixMilliseconds(value),
        }
    }
}

impl From<ArtifactSecurityRecord> for ArtifactSecurity {
    fn from(value: ArtifactSecurityRecord) -> Self {
        match value {
            ArtifactSecurityRecord::Standard => Self::Standard,
            ArtifactSecurityRecord::Private => Self::Private,
            ArtifactSecurityRecord::Secret => Self::Secret,
        }
    }
}

impl From<ArtifactRetentionRecord> for ArtifactRetention {
    fn from(value: ArtifactRetentionRecord) -> Self {
        match value {
            ArtifactRetentionRecord::Permanent => Self::Permanent,
            ArtifactRetentionRecord::Session => Self::Session,
            ArtifactRetentionRecord::UntilUnixMilliseconds(value) => {
                Self::UntilUnixMilliseconds(value)
            }
        }
    }
}

/// Artifact persistence business failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ArtifactPersistenceError {
    /// Logic-owned command is invalid.
    #[error("artifact persistence request is invalid")]
    Invalid,
    /// User approval is required.
    #[error("artifact persistence requires approval: {0}")]
    ApprovalRequired(String),
    /// Policy rejected persistence.
    #[error("artifact persistence was rejected: {0}")]
    Rejected(String),
    /// Policy cancelled persistence.
    #[error("artifact persistence was cancelled: {0}")]
    Cancelled(String),
    /// The selected pipeline decision is not supported by this node adapter.
    #[error("artifact persistence pipeline returned an unsupported decision")]
    UnsupportedDecision,
    /// An interceptor attempted to replace the exact immutable bytes contract.
    #[error("artifact persistence interceptor replacement is invalid")]
    InvalidInterceptionReplacement,
    /// Existing content-addressed metadata does not match the approved request.
    #[error("artifact reconciliation metadata is invalid")]
    InvalidReconciliation,
    /// Data persistence failed.
    #[error("artifact persistence data failed: {0}")]
    Data(ArtifactDataError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agentmod_event_pipeline::BlockingPipelineBuilder;
    use agentmod_runtime_data::artifact::RuntimeArtifactData;

    use crate::permission::{PermissionEffect, PermissionPolicy};

    use super::*;

    fn policy() -> ProviderExecutionPolicy {
        let pipeline = || {
            BlockingPipelineBuilder::<ActionProposal>::new()
                .compile()
                .expect("pipeline")
        };
        ProviderExecutionPolicy {
            style_pipeline: Arc::new(pipeline()),
            plugin_pipeline: Arc::new(pipeline()),
            user_policy: PermissionPolicy::new(
                "user",
                Vec::new(),
                PermissionEffect::Allow,
                "allowed",
            ),
            mandatory_policy: PermissionPolicy::new(
                "mandatory",
                Vec::new(),
                PermissionEffect::Allow,
                "allowed",
            ),
        }
    }

    fn command(
        root: &std::path::Path,
        proposal_id: &str,
        security: ArtifactSecurity,
        retention: ArtifactRetention,
    ) -> PersistArtifactCommand {
        PersistArtifactCommand {
            proposal_id: proposal_id.to_owned(),
            style: String::from("research-loop"),
            workspace: String::from("fixture"),
            store_root: root.join("artifacts"),
            creation_event: String::from("event-1"),
            producer: String::from("runtime.style"),
            mime_type: String::from("application/json"),
            bytes: br#"{"finding":"one"}"#.to_vec(),
            security,
            retention,
        }
    }

    #[tokio::test]
    async fn proposal_policy_and_data_bound_the_exact_bytes() {
        let root = tempfile::tempdir().expect("root");
        let logic = ArtifactPersistenceLogic::new(RuntimeArtifactData::first_party(), policy());
        let prepared = logic
            .prepare(command(
                root.path(),
                "artifact:node:1",
                ArtifactSecurity::Private,
                ArtifactRetention::Session,
            ))
            .expect("prepare");
        let approved = logic.authorize_prepared(prepared).await.expect("authorize");
        assert_eq!(approved.executable.action.kind(), "artifact_persistence");
        let persisted = logic.persist_authorized(approved).expect("persist");
        assert_eq!(persisted.byte_size, 17);
        assert!(persisted.artifact_reference.starts_with("artifact:blake3:"));
    }

    #[test]
    fn action_digest_binds_exact_expiry_and_security() {
        let root = tempfile::tempdir().expect("root");
        let logic = ArtifactPersistenceLogic::new(RuntimeArtifactData::first_party(), policy());
        let digest = |proposal_id, security, retention| {
            logic
                .prepare(command(root.path(), proposal_id, security, retention))
                .expect("prepare")
                .original
                .digest()
                .expect("digest")
        };

        let first_expiry = digest(
            "artifact:expiry",
            ArtifactSecurity::Private,
            ArtifactRetention::UntilUnixMilliseconds(1_900_000_000_000),
        );
        let second_expiry = digest(
            "artifact:expiry",
            ArtifactSecurity::Private,
            ArtifactRetention::UntilUnixMilliseconds(1_900_000_000_001),
        );
        let secret = digest(
            "artifact:expiry",
            ArtifactSecurity::Secret,
            ArtifactRetention::UntilUnixMilliseconds(1_900_000_000_000),
        );

        assert_ne!(first_expiry, second_expiry);
        assert_ne!(first_expiry, secret);
    }

    #[tokio::test]
    async fn same_content_with_weaker_existing_security_is_rejected() {
        let root = tempfile::tempdir().expect("root");
        let logic = ArtifactPersistenceLogic::new(RuntimeArtifactData::first_party(), policy());
        let weaker = logic
            .prepare(command(
                root.path(),
                "artifact:standard",
                ArtifactSecurity::Standard,
                ArtifactRetention::Session,
            ))
            .expect("prepare weaker");
        let weaker = logic
            .authorize_prepared(weaker)
            .await
            .expect("authorize weaker");
        logic
            .persist_authorized(weaker)
            .expect("persist weaker object");

        let secret = logic
            .prepare(command(
                root.path(),
                "artifact:secret",
                ArtifactSecurity::Secret,
                ArtifactRetention::Session,
            ))
            .expect("prepare secret");
        let secret = logic
            .authorize_prepared(secret)
            .await
            .expect("authorize secret");
        assert_eq!(
            logic.persist_authorized(secret),
            Err(ArtifactPersistenceError::InvalidReconciliation)
        );
    }

    #[tokio::test]
    async fn same_content_reuses_original_physical_provenance_for_new_canonical_reference() {
        let root = tempfile::tempdir().expect("root");
        let logic = ArtifactPersistenceLogic::new(RuntimeArtifactData::first_party(), policy());
        let first = logic
            .prepare(command(
                root.path(),
                "artifact:first",
                ArtifactSecurity::Private,
                ArtifactRetention::Session,
            ))
            .expect("prepare first");
        let first = logic
            .authorize_prepared(first)
            .await
            .expect("authorize first");
        let first = logic.persist_authorized(first).expect("persist first");
        assert!(!first.deduplicated);

        let mut repeated = command(
            root.path(),
            "artifact:second",
            ArtifactSecurity::Private,
            ArtifactRetention::Session,
        );
        repeated.creation_event = String::from("event-2");
        repeated.producer = String::from("runtime.style.iteration-2");
        let repeated = logic.prepare(repeated).expect("prepare repeated");
        let repeated = logic
            .authorize_prepared(repeated)
            .await
            .expect("authorize repeated");
        let repeated = logic
            .persist_authorized(repeated)
            .expect("reuse content-addressed object");

        assert!(repeated.deduplicated);
        assert_eq!(repeated.artifact_reference, first.artifact_reference);
        assert_eq!(repeated.creation_event, "event-1");
        assert_eq!(repeated.producer, "runtime.style");
    }

    #[test]
    fn bounded_read_validates_hash_size_and_range_and_fails_closed_when_missing() {
        let root = tempfile::tempdir().expect("root");
        let store_root = root.path().join("artifacts");
        let data = RuntimeArtifactData::first_party();
        let bytes = b"bounded artifact evidence".to_vec();
        let persisted = data
            .persist_artifact(PersistArtifactDataRequest {
                store_root: store_root.clone(),
                creation_event: String::from("event-read"),
                producer: String::from("runtime.style"),
                mime_type: String::from("text/plain"),
                bytes: bytes.clone(),
                security: ArtifactSecurityRecord::Private,
                retention: ArtifactRetentionRecord::Session,
            })
            .expect("persist read fixture");
        let reader = ArtifactReadLogic::new(data);
        let hash = ContentHash::digest(&bytes);

        let range = reader
            .read_artifact_range(ReadArtifactRangeCommand {
                store_root: store_root.clone(),
                artifact_reference: persisted.artifact_reference.clone(),
                expected_content_hash: hash,
                expected_byte_size: persisted.byte_size,
                offset: 8,
                length: 8,
            })
            .expect("bounded read");
        assert_eq!(range.bytes, b"artifact");
        assert_eq!(range.artifact_bytes, persisted.byte_size);
        assert_eq!(range.content_hash, hash);

        let wrong_hash = ContentHash::digest(b"different");
        assert!(matches!(
            reader.read_artifact_range(ReadArtifactRangeCommand {
                store_root: store_root.clone(),
                artifact_reference: persisted.artifact_reference.clone(),
                expected_content_hash: wrong_hash,
                expected_byte_size: persisted.byte_size,
                offset: 0,
                length: 1,
            }),
            Err(ArtifactReadError::InvalidRange)
        ));
        assert!(matches!(
            reader.read_artifact_range(ReadArtifactRangeCommand {
                store_root: store_root.clone(),
                artifact_reference: persisted.artifact_reference.clone(),
                expected_content_hash: hash,
                expected_byte_size: persisted.byte_size + 1,
                offset: 0,
                length: 1,
            }),
            Err(ArtifactReadError::Substituted)
        ));
        assert!(matches!(
            reader.read_artifact_range(ReadArtifactRangeCommand {
                store_root: store_root.clone(),
                artifact_reference: persisted.artifact_reference,
                expected_content_hash: hash,
                expected_byte_size: persisted.byte_size,
                offset: persisted.byte_size,
                length: 1,
            }),
            Err(ArtifactReadError::InvalidRange)
        ));

        let missing_hash = ContentHash::digest(b"missing");
        assert!(matches!(
            reader.read_artifact_range(ReadArtifactRangeCommand {
                store_root,
                artifact_reference: format!("artifact:blake3:{missing_hash}"),
                expected_content_hash: missing_hash,
                expected_byte_size: 1,
                offset: 0,
                length: 1,
            }),
            Err(ArtifactReadError::NotFound)
        ));
    }
}
