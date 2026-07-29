//! Business-facing immutable artifact persistence datasets.

use std::path::{Path, PathBuf};

use agentmod_runtime_dependency::artifact::{
    ArtifactDependencyError, ArtifactDependencyPort, DependencyAbortArtifactRequest,
    DependencyArtifactCompression, DependencyArtifactLimits, DependencyArtifactRetention,
    DependencyArtifactSecurity, DependencyFinalizeArtifactRequest,
    DependencyInspectArtifactRequest, DependencyStartArtifactRequest,
    DependencyWriteArtifactChunkRequest, LocalArtifactDependency,
};
use thiserror::Error;

const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_ARTIFACT_BYTES_U64: u64 = 16 * 1024 * 1024;

/// Data-owned artifact security classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactSecurityRecord {
    /// Ordinary workspace content.
    Standard,
    /// User-private content.
    Private,
    /// Secret-bearing content.
    Secret,
}

/// Data-owned artifact retention selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactRetentionRecord {
    /// Retain until an explicit removal policy acts.
    Permanent,
    /// Retain with the owning session.
    Session,
    /// Retain until a portable Unix timestamp in milliseconds.
    UntilUnixMilliseconds(i64),
}

/// Data-owned immutable artifact write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistArtifactDataRequest {
    /// Session-scoped artifact store root selected by runtime logic.
    pub store_root: PathBuf,
    /// Canonical event that proposed this persistence operation.
    pub creation_event: String,
    /// Runtime or plugin producer identity.
    pub producer: String,
    /// Valid media type.
    pub mime_type: String,
    /// Approved exact bytes.
    pub bytes: Vec<u8>,
    /// Security handling classification.
    pub security: ArtifactSecurityRecord,
    /// Retention policy.
    pub retention: ArtifactRetentionRecord,
}

/// Data-owned immutable artifact record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedArtifactDataRecord {
    /// Content-addressed artifact identity.
    pub artifact_id: String,
    /// Portable immutable reference.
    pub artifact_reference: String,
    /// Exact media type retained by the dependency.
    pub mime_type: String,
    /// Exact byte count.
    pub byte_size: u64,
    /// Canonical event that first created this content-addressed object.
    pub creation_event: String,
    /// Original producer.
    pub producer: String,
    /// Lowercase BLAKE3 content digest.
    pub content_hash: String,
    /// Whether an identical immutable object was reused.
    pub deduplicated: bool,
}

/// Data-owned immutable artifact inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectArtifactDataRequest {
    /// Session-scoped artifact store root.
    pub store_root: PathBuf,
    /// Exact portable immutable reference.
    pub artifact_reference: String,
}

/// Narrow artifact data interface consumed by runtime logic.
pub trait ArtifactDataPort {
    /// Persists exact approved bytes as one immutable content-addressed object.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactDataError`] for invalid data-layer input, dependency
    /// failure, or a malformed dependency record.
    fn persist_artifact(
        &self,
        request: PersistArtifactDataRequest,
    ) -> Result<PersistedArtifactDataRecord, ArtifactDataError>;

    /// Inspects an exact immutable content-addressed object.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactDataError`] for invalid references, missing objects,
    /// dependency failures, or malformed metadata.
    fn inspect_artifact(
        &self,
        request: InspectArtifactDataRequest,
    ) -> Result<PersistedArtifactDataRecord, ArtifactDataError>;
}

/// First-party artifact data router with explicit hard bounds.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeArtifactData {
    limits: DependencyArtifactLimits,
}

impl RuntimeArtifactData {
    /// Creates the bounded first-party router.
    #[must_use]
    pub const fn first_party() -> Self {
        Self {
            limits: DependencyArtifactLimits {
                max_chunk_bytes: DEFAULT_CHUNK_BYTES as u64,
                max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES_U64,
                max_range_bytes: DEFAULT_MAX_ARTIFACT_BYTES_U64,
            },
        }
    }

    fn store(&self, root: &Path) -> Result<LocalArtifactDependency, ArtifactDataError> {
        LocalArtifactDependency::new(root.to_owned(), self.limits)
            .map_err(ArtifactDataError::Dependency)
    }
}

impl Default for RuntimeArtifactData {
    fn default() -> Self {
        Self::first_party()
    }
}

impl ArtifactDataPort for RuntimeArtifactData {
    fn persist_artifact(
        &self,
        request: PersistArtifactDataRequest,
    ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
        if request.store_root.as_os_str().is_empty()
            || request.bytes.is_empty()
            || request.bytes.len() > DEFAULT_MAX_ARTIFACT_BYTES
        {
            return Err(ArtifactDataError::InvalidRequest);
        }
        let store = self.store(&request.store_root)?;
        let started = store
            .start(DependencyStartArtifactRequest {
                mime_type: request.mime_type.clone(),
                creation_event: request.creation_event.clone(),
                producer: request.producer.clone(),
                security: request.security.into(),
                compression: DependencyArtifactCompression::None,
                retention: request.retention.into(),
            })
            .map_err(ArtifactDataError::Dependency)?;
        for chunk in request.bytes.chunks(DEFAULT_CHUNK_BYTES) {
            if let Err(error) = store.write_chunk(DependencyWriteArtifactChunkRequest {
                write_id: started.write_id.clone(),
                bytes: chunk.to_vec(),
            }) {
                let _ = store.abort(DependencyAbortArtifactRequest {
                    write_id: started.write_id,
                });
                return Err(ArtifactDataError::Dependency(error));
            }
        }
        let finalized = store
            .finalize(DependencyFinalizeArtifactRequest {
                write_id: started.write_id,
            })
            .map_err(ArtifactDataError::Dependency)?;
        let metadata = finalized.metadata;
        if metadata.mime_type != request.mime_type
            || metadata.byte_size
                != u64::try_from(request.bytes.len())
                    .map_err(|_| ArtifactDataError::InvalidRequest)?
            || metadata.content_hash != blake3::hash(&request.bytes).to_hex().as_str()
        {
            return Err(ArtifactDataError::InvalidDependencyRecord);
        }
        Ok(PersistedArtifactDataRecord {
            artifact_id: metadata.artifact_id.as_str().to_owned(),
            artifact_reference: metadata.artifact_reference.as_str().to_owned(),
            mime_type: metadata.mime_type,
            byte_size: metadata.byte_size,
            creation_event: metadata.creation_event,
            producer: metadata.producer,
            content_hash: metadata.content_hash,
            deduplicated: finalized.deduplicated,
        })
    }

    fn inspect_artifact(
        &self,
        request: InspectArtifactDataRequest,
    ) -> Result<PersistedArtifactDataRecord, ArtifactDataError> {
        if request.store_root.as_os_str().is_empty() {
            return Err(ArtifactDataError::InvalidRequest);
        }
        let reference = agentmod_runtime_dependency::artifact::ArtifactReference::parse(
            request.artifact_reference,
        )
        .map_err(ArtifactDataError::Dependency)?;
        let metadata = self
            .store(&request.store_root)?
            .inspect(DependencyInspectArtifactRequest {
                artifact_reference: reference,
            })
            .map_err(|error| {
                if error == ArtifactDependencyError::ArtifactNotFound {
                    ArtifactDataError::NotFound
                } else {
                    ArtifactDataError::Dependency(error)
                }
            })?;
        Ok(PersistedArtifactDataRecord {
            artifact_id: metadata.artifact_id.as_str().to_owned(),
            artifact_reference: metadata.artifact_reference.as_str().to_owned(),
            mime_type: metadata.mime_type,
            byte_size: metadata.byte_size,
            creation_event: metadata.creation_event,
            producer: metadata.producer,
            content_hash: metadata.content_hash,
            deduplicated: true,
        })
    }
}

impl From<ArtifactSecurityRecord> for DependencyArtifactSecurity {
    fn from(value: ArtifactSecurityRecord) -> Self {
        match value {
            ArtifactSecurityRecord::Standard => Self::Standard,
            ArtifactSecurityRecord::Private => Self::Private,
            ArtifactSecurityRecord::Secret => Self::Secret,
        }
    }
}

impl From<ArtifactRetentionRecord> for DependencyArtifactRetention {
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

/// Artifact data-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ArtifactDataError {
    /// Request violates the data-layer hard bounds.
    #[error("artifact request is invalid")]
    InvalidRequest,
    /// Dependency returned metadata inconsistent with the approved bytes.
    #[error("artifact dependency returned an invalid record")]
    InvalidDependencyRecord,
    /// Requested immutable object does not exist.
    #[error("artifact was not found")]
    NotFound,
    /// External artifact storage failed.
    #[error("artifact dependency failed: {0}")]
    Dependency(ArtifactDependencyError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_and_deduplicates_exact_bounded_content() {
        let root = tempfile::tempdir().expect("root");
        let data = RuntimeArtifactData::first_party();
        let request = PersistArtifactDataRequest {
            store_root: root.path().join("artifacts"),
            creation_event: String::from("event-1"),
            producer: String::from("runtime.style"),
            mime_type: String::from("application/json"),
            bytes: br#"{"finding":"bounded"}"#.to_vec(),
            security: ArtifactSecurityRecord::Private,
            retention: ArtifactRetentionRecord::Session,
        };
        let first = data
            .persist_artifact(request.clone())
            .expect("first artifact");
        let second = data
            .persist_artifact(request)
            .expect("deduplicated artifact");
        assert!(!first.deduplicated);
        assert!(second.deduplicated);
        assert_eq!(first.artifact_reference, second.artifact_reference);
        assert_eq!(first.content_hash, second.content_hash);
    }

    #[test]
    fn rejects_empty_content_before_creating_a_store() {
        let root = tempfile::tempdir().expect("root");
        let result =
            RuntimeArtifactData::first_party().persist_artifact(PersistArtifactDataRequest {
                store_root: root.path().join("artifacts"),
                creation_event: String::from("event-1"),
                producer: String::from("runtime.style"),
                mime_type: String::from("application/json"),
                bytes: Vec::new(),
                security: ArtifactSecurityRecord::Private,
                retention: ArtifactRetentionRecord::Session,
            });
        assert_eq!(result, Err(ArtifactDataError::InvalidRequest));
        assert!(!root.path().join("artifacts").exists());
    }
}
