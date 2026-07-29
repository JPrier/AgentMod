//! Runtime-owned immutable artifact proposal and persistence coordination.

use std::path::PathBuf;

use agentmod_event_pipeline::ActionCapabilities;
use agentmod_primitives::ContentHash;
use agentmod_runtime_data::artifact::{
    ArtifactDataError, ArtifactDataPort, ArtifactRetentionRecord, ArtifactSecurityRecord,
    InspectArtifactDataRequest, PersistArtifactDataRequest, PersistedArtifactDataRecord,
};
use thiserror::Error;

use crate::{
    action::{ActionProposal, ArtifactPersistenceAction, ConsequentialAction, ProposalId},
    harness::ProviderExecutionPolicy,
    interception::{InterceptionOutcome, intercept_action},
};

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
    const fn label(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Session => "session",
            Self::UntilUnixMilliseconds(_) => "until_unix_milliseconds",
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
                retention: command.retention.label().to_owned(),
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
            retention: prepared.command.retention.label().to_owned(),
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
        self.data
            .persist_artifact(PersistArtifactDataRequest {
                store_root: authorized.command.store_root,
                creation_event: authorized.command.creation_event,
                producer: authorized.command.producer,
                mime_type: authorized.command.mime_type,
                bytes: authorized.command.bytes,
                security: authorized.command.security.into(),
                retention: authorized.command.retention.into(),
            })
            .map(map_record)
            .map_err(ArtifactPersistenceError::Data)
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
        let content_hash = ContentHash::digest(&command.bytes).to_hex();
        let record = match self.data.inspect_artifact(InspectArtifactDataRequest {
            store_root: command.store_root.clone(),
            artifact_reference: format!("artifact:blake3:{content_hash}"),
        }) {
            Ok(record) => record,
            Err(ArtifactDataError::NotFound) => return Ok(None),
            Err(error) => return Err(ArtifactPersistenceError::Data(error)),
        };
        if record.content_hash != content_hash
            || record.mime_type != command.mime_type
            || record.byte_size
                != u64::try_from(command.bytes.len())
                    .map_err(|_| ArtifactPersistenceError::Invalid)?
        {
            return Err(ArtifactPersistenceError::InvalidReconciliation);
        }
        Ok(Some(map_record(record)))
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

    #[tokio::test]
    async fn proposal_policy_and_data_bound_the_exact_bytes() {
        let root = tempfile::tempdir().expect("root");
        let logic = ArtifactPersistenceLogic::new(RuntimeArtifactData::first_party(), policy());
        let prepared = logic
            .prepare(PersistArtifactCommand {
                proposal_id: String::from("artifact:node:1"),
                style: String::from("research-loop"),
                workspace: String::from("fixture"),
                store_root: root.path().join("artifacts"),
                creation_event: String::from("event-1"),
                producer: String::from("runtime.style"),
                mime_type: String::from("application/json"),
                bytes: br#"{"finding":"one"}"#.to_vec(),
                security: ArtifactSecurity::Private,
                retention: ArtifactRetention::Session,
            })
            .expect("prepare");
        let approved = logic.authorize_prepared(prepared).await.expect("authorize");
        assert_eq!(approved.executable.action.kind(), "artifact_persistence");
        let persisted = logic.persist_authorized(approved).expect("persist");
        assert_eq!(persisted.byte_size, 17);
        assert!(persisted.artifact_reference.starts_with("artifact:blake3:"));
    }
}
