//! Content-addressed, transactional artifact storage adapter.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const INCOMPLETE_DIRECTORY: &str = ".incomplete";
const FINALIZING_DIRECTORY: &str = ".finalizing";
const OBJECTS_DIRECTORY: &str = "objects";
const CONTENT_FILE: &str = "content";
const PENDING_METADATA_FILE: &str = "pending.json";
const FINAL_METADATA_FILE: &str = "metadata.json";
const HASH_HEX_LENGTH: usize = 64;
const WRITE_ID_HEX_LENGTH: usize = 32;

/// Opaque validated artifact write transaction identifier.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactWriteId(String);

impl ArtifactWriteId {
    /// Parses a persisted dependency write identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactDependencyError::InvalidWriteId`] for unsafe text.
    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactDependencyError> {
        let value = value.into();
        if is_lower_hex(&value, WRITE_ID_HEX_LENGTH) {
            Ok(Self(value))
        } else {
            Err(ArtifactDependencyError::InvalidWriteId)
        }
    }

    /// Returns the validated opaque identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// BLAKE3 content identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactId(String);

impl ArtifactId {
    /// Returns the portable content identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated portable reference used for artifact range reads.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactReference(String);

impl ArtifactReference {
    /// Parses a persisted artifact reference.
    ///
    /// # Errors
    ///
    /// Rejects malformed references and all path-like or traversal text.
    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactDependencyError> {
        let value = value.into();
        artifact_hash_from_reference(&value)?;
        Ok(Self(value))
    }

    /// Returns the portable reference.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Security handling classification persisted with an artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyArtifactSecurity {
    /// Normal workspace content.
    Standard,
    /// Content requiring user-private handling.
    Private,
    /// Content which may contain credentials or equivalent secrets.
    Secret,
}

/// Compression representation of stored content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyArtifactCompression {
    /// Bytes are stored without compression.
    None,
    /// Bytes are already gzip encoded.
    Gzip,
    /// Bytes are already Zstandard encoded.
    Zstd,
}

/// Artifact retention policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyArtifactRetention {
    /// Retain until explicit removal policy acts.
    Permanent,
    /// Retain with the owning session.
    Session,
    /// Retain until the portable Unix timestamp in milliseconds.
    UntilUnixMilliseconds(i64),
}

/// Size limits enforced without trusting callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyArtifactLimits {
    /// Maximum accepted bytes in one write call.
    pub max_chunk_bytes: u64,
    /// Maximum total bytes in one artifact.
    pub max_artifact_bytes: u64,
    /// Maximum bytes returned by one range read.
    pub max_range_bytes: u64,
}

/// Dependency-owned request to start an artifact transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStartArtifactRequest {
    /// Valid MIME media type.
    pub mime_type: String,
    /// Event identifier which initiated creation.
    pub creation_event: String,
    /// Producing subsystem or plugin identifier.
    pub producer: String,
    /// Security handling classification.
    pub security: DependencyArtifactSecurity,
    /// Stored compression representation.
    pub compression: DependencyArtifactCompression,
    /// Retention policy.
    pub retention: DependencyArtifactRetention,
}

/// Successful transaction creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStartArtifactResponse {
    /// Opaque identifier required by subsequent transaction operations.
    pub write_id: ArtifactWriteId,
}

/// Dependency-owned bounded chunk request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyWriteArtifactChunkRequest {
    /// Active transaction.
    pub write_id: ArtifactWriteId,
    /// Chunk bytes copied directly to the transaction file.
    pub bytes: Vec<u8>,
}

/// Successful bounded chunk append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyWriteArtifactChunkResponse {
    /// Total transaction bytes after the append.
    pub total_bytes: u64,
}

/// Dependency-owned finalize request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyFinalizeArtifactRequest {
    /// Active transaction to finalize.
    pub write_id: ArtifactWriteId,
}

/// Immutable persisted artifact metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyArtifactMetadata {
    /// BLAKE3 content identity.
    pub artifact_id: ArtifactId,
    /// Portable artifact reference.
    pub artifact_reference: ArtifactReference,
    /// MIME media type.
    pub mime_type: String,
    /// Exact stored byte size.
    pub byte_size: u64,
    /// Event which originally created this content-addressed object.
    pub creation_event: String,
    /// Original producing subsystem.
    pub producer: String,
    /// Security handling classification.
    pub security: DependencyArtifactSecurity,
    /// Stored compression representation.
    pub compression: DependencyArtifactCompression,
    /// Retention policy.
    pub retention: DependencyArtifactRetention,
    /// Lowercase BLAKE3 digest.
    pub content_hash: String,
}

/// Successful finalize result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyFinalizeArtifactResponse {
    /// Canonical immutable metadata.
    pub metadata: DependencyArtifactMetadata,
    /// Whether identical content already existed.
    pub deduplicated: bool,
}

/// Dependency-owned abort request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAbortArtifactRequest {
    /// Active transaction to remove.
    pub write_id: ArtifactWriteId,
}

/// Dependency-owned range request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyReadArtifactRangeRequest {
    /// Validated artifact reference.
    pub artifact_reference: ArtifactReference,
    /// Zero-based byte offset.
    pub offset: u64,
    /// Exact requested byte count.
    pub length: u64,
}

/// Dependency-owned immutable artifact inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyInspectArtifactRequest {
    /// Validated portable artifact reference.
    pub artifact_reference: ArtifactReference,
}

/// Bounded artifact range response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyReadArtifactRangeResponse {
    /// Exact requested bytes.
    pub bytes: Vec<u8>,
    /// Full artifact byte size without loading the remainder.
    pub artifact_bytes: u64,
}

/// Incomplete transaction cleanup result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyCleanupArtifactsResponse {
    /// Number of transaction directories removed.
    pub removed_transactions: u64,
}

/// External artifact storage contract consumed by runtime data.
pub trait ArtifactDependencyPort {
    /// Starts a validated transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid metadata or storage failures.
    fn start(
        &self,
        request: DependencyStartArtifactRequest,
    ) -> Result<DependencyStartArtifactResponse, ArtifactDependencyError>;

    /// Appends one bounded chunk without retaining previous chunks in memory.
    ///
    /// # Errors
    ///
    /// Returns an error for missing transactions, empty/oversize chunks,
    /// total-size overflow, or storage failures.
    fn write_chunk(
        &self,
        request: DependencyWriteArtifactChunkRequest,
    ) -> Result<DependencyWriteArtifactChunkResponse, ArtifactDependencyError>;

    /// Atomically exposes an immutable content-addressed object.
    ///
    /// # Errors
    ///
    /// Returns an error for missing/corrupt transactions or storage failures.
    fn finalize(
        &self,
        request: DependencyFinalizeArtifactRequest,
    ) -> Result<DependencyFinalizeArtifactResponse, ArtifactDependencyError>;

    /// Removes an incomplete transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction is missing or cannot be removed.
    fn abort(&self, request: DependencyAbortArtifactRequest)
    -> Result<(), ArtifactDependencyError>;

    /// Reads an exact bounded range without loading the complete artifact.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid references/ranges, missing objects,
    /// corruption, or storage failures.
    fn read_range(
        &self,
        request: DependencyReadArtifactRangeRequest,
    ) -> Result<DependencyReadArtifactRangeResponse, ArtifactDependencyError>;

    /// Reads and validates immutable metadata without loading artifact content.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid references, missing objects, corruption, or
    /// storage failures.
    fn inspect(
        &self,
        request: DependencyInspectArtifactRequest,
    ) -> Result<DependencyArtifactMetadata, ArtifactDependencyError>;

    /// Removes all transactions not atomically finalized.
    ///
    /// # Errors
    ///
    /// Returns an error when cleanup cannot enumerate or remove transactions.
    fn cleanup_incomplete(
        &self,
    ) -> Result<DependencyCleanupArtifactsResponse, ArtifactDependencyError>;
}

/// Local filesystem content-addressed artifact implementation.
#[derive(Clone, Debug)]
pub struct LocalArtifactDependency {
    root: PathBuf,
    limits: DependencyArtifactLimits,
    post_finalize_delay: Duration,
}

impl LocalArtifactDependency {
    /// Creates the store directories and validates hard size limits.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/inconsistent limits or storage failures.
    pub fn new(
        root: PathBuf,
        limits: DependencyArtifactLimits,
    ) -> Result<Self, ArtifactDependencyError> {
        if root.as_os_str().is_empty()
            || limits.max_chunk_bytes == 0
            || limits.max_artifact_bytes == 0
            || limits.max_range_bytes == 0
            || limits.max_chunk_bytes > limits.max_artifact_bytes
        {
            return Err(ArtifactDependencyError::InvalidConfiguration);
        }
        fs::create_dir_all(root.join(INCOMPLETE_DIRECTORY)).map_err(io_error)?;
        fs::create_dir_all(root.join(FINALIZING_DIRECTORY)).map_err(io_error)?;
        fs::create_dir_all(root.join(OBJECTS_DIRECTORY)).map_err(io_error)?;
        Ok(Self {
            root,
            limits,
            post_finalize_delay: Duration::ZERO,
        })
    }

    /// Adds a post-finalize observation window used only by process crash-cut
    /// validation. The immutable object and its parent directory are durable
    /// before this delay begins.
    #[must_use]
    pub const fn with_post_finalize_delay(mut self, delay: Duration) -> Self {
        self.post_finalize_delay = delay;
        self
    }

    fn incomplete_path(&self, write_id: &ArtifactWriteId) -> PathBuf {
        self.root.join(INCOMPLETE_DIRECTORY).join(write_id.as_str())
    }

    fn finalizing_path(&self, write_id: &ArtifactWriteId) -> PathBuf {
        self.root.join(FINALIZING_DIRECTORY).join(write_id.as_str())
    }

    fn object_path(&self, hash: &str) -> PathBuf {
        self.root
            .join(OBJECTS_DIRECTORY)
            .join(&hash[..2])
            .join(hash)
    }
}

impl ArtifactDependencyPort for LocalArtifactDependency {
    fn start(
        &self,
        request: DependencyStartArtifactRequest,
    ) -> Result<DependencyStartArtifactResponse, ArtifactDependencyError> {
        validate_mime(&request.mime_type)?;
        validate_external_id("creation_event", &request.creation_event)?;
        validate_external_id("producer", &request.producer)?;
        let pending = StoredPendingMetadata::from_request(request);

        for _ in 0..8 {
            let write_id = ArtifactWriteId::parse(Uuid::now_v7().simple().to_string())?;
            let transaction = self.incomplete_path(&write_id);
            match fs::create_dir(&transaction) {
                Ok(()) => {
                    let result = initialize_transaction(&transaction, &pending);
                    if let Err(error) = result {
                        let _ = fs::remove_dir_all(&transaction);
                        return Err(error);
                    }
                    return Ok(DependencyStartArtifactResponse { write_id });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        Err(ArtifactDependencyError::WriteIdCollision)
    }

    fn write_chunk(
        &self,
        request: DependencyWriteArtifactChunkRequest,
    ) -> Result<DependencyWriteArtifactChunkResponse, ArtifactDependencyError> {
        if request.bytes.is_empty() {
            return Err(ArtifactDependencyError::EmptyChunk);
        }
        let chunk_bytes = u64::try_from(request.bytes.len())
            .map_err(|_| ArtifactDependencyError::SizeOverflow)?;
        if chunk_bytes > self.limits.max_chunk_bytes {
            return Err(ArtifactDependencyError::ChunkTooLarge {
                actual: chunk_bytes,
                maximum: self.limits.max_chunk_bytes,
            });
        }
        let content_path = self.incomplete_path(&request.write_id).join(CONTENT_FILE);
        let mut content = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&content_path)
            .map_err(transaction_io_error)?;
        content.lock_exclusive().map_err(io_error)?;
        let current = content.metadata().map_err(io_error)?.len();
        let total = current
            .checked_add(chunk_bytes)
            .ok_or(ArtifactDependencyError::SizeOverflow)?;
        if total > self.limits.max_artifact_bytes {
            fs2::FileExt::unlock(&content).map_err(io_error)?;
            return Err(ArtifactDependencyError::ArtifactTooLarge {
                attempted: total,
                maximum: self.limits.max_artifact_bytes,
            });
        }
        let write_result = content
            .write_all(&request.bytes)
            .and_then(|()| content.flush())
            .map_err(io_error);
        let unlock_result = fs2::FileExt::unlock(&content).map_err(io_error);
        write_result?;
        unlock_result?;
        Ok(DependencyWriteArtifactChunkResponse { total_bytes: total })
    }

    fn finalize(
        &self,
        request: DependencyFinalizeArtifactRequest,
    ) -> Result<DependencyFinalizeArtifactResponse, ArtifactDependencyError> {
        let incomplete = self.incomplete_path(&request.write_id);
        let finalizing = self.finalizing_path(&request.write_id);
        fs::rename(&incomplete, &finalizing).map_err(transaction_io_error)?;
        let result = finalize_transaction(self, &finalizing);
        if result.is_err() {
            let _ = fs::remove_dir_all(&finalizing);
        }
        if result.is_ok() && !self.post_finalize_delay.is_zero() {
            std::thread::sleep(self.post_finalize_delay);
        }
        result
    }

    fn abort(
        &self,
        request: DependencyAbortArtifactRequest,
    ) -> Result<(), ArtifactDependencyError> {
        fs::remove_dir_all(self.incomplete_path(&request.write_id)).map_err(transaction_io_error)
    }

    fn read_range(
        &self,
        request: DependencyReadArtifactRangeRequest,
    ) -> Result<DependencyReadArtifactRangeResponse, ArtifactDependencyError> {
        if request.length > self.limits.max_range_bytes {
            return Err(ArtifactDependencyError::RangeTooLarge {
                requested: request.length,
                maximum: self.limits.max_range_bytes,
            });
        }
        let hash = artifact_hash_from_reference(request.artifact_reference.as_str())?;
        let object = self.object_path(hash);
        let metadata = read_metadata(&object)?;
        let end = request
            .offset
            .checked_add(request.length)
            .ok_or(ArtifactDependencyError::InvalidRange)?;
        if request.offset > metadata.byte_size || end > metadata.byte_size {
            return Err(ArtifactDependencyError::InvalidRange);
        }
        let allocation = usize::try_from(request.length).map_err(|_| {
            ArtifactDependencyError::RangeTooLarge {
                requested: request.length,
                maximum: self.limits.max_range_bytes,
            }
        })?;
        let mut content = File::open(object.join(CONTENT_FILE)).map_err(artifact_io_error)?;
        content
            .seek(SeekFrom::Start(request.offset))
            .map_err(io_error)?;
        let mut bytes = vec![0; allocation];
        content.read_exact(&mut bytes).map_err(io_error)?;
        Ok(DependencyReadArtifactRangeResponse {
            bytes,
            artifact_bytes: metadata.byte_size,
        })
    }

    fn inspect(
        &self,
        request: DependencyInspectArtifactRequest,
    ) -> Result<DependencyArtifactMetadata, ArtifactDependencyError> {
        let hash = artifact_hash_from_reference(request.artifact_reference.as_str())?;
        read_metadata(&self.object_path(hash))
    }

    fn cleanup_incomplete(
        &self,
    ) -> Result<DependencyCleanupArtifactsResponse, ArtifactDependencyError> {
        let mut removed_transactions = 0_u64;
        for directory in [INCOMPLETE_DIRECTORY, FINALIZING_DIRECTORY] {
            let root = self.root.join(directory);
            for entry in fs::read_dir(&root).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if !entry.file_type().map_err(io_error)?.is_dir() {
                    return Err(ArtifactDependencyError::UnsafeStoreEntry);
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                ArtifactWriteId::parse(name)?;
                fs::remove_dir_all(entry.path()).map_err(io_error)?;
                removed_transactions = removed_transactions
                    .checked_add(1)
                    .ok_or(ArtifactDependencyError::SizeOverflow)?;
            }
        }
        Ok(DependencyCleanupArtifactsResponse {
            removed_transactions,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredPendingMetadata {
    mime_type: String,
    creation_event: String,
    producer: String,
    security: StoredSecurity,
    compression: StoredCompression,
    retention: StoredRetention,
}

impl StoredPendingMetadata {
    fn from_request(request: DependencyStartArtifactRequest) -> Self {
        Self {
            mime_type: request.mime_type,
            creation_event: request.creation_event,
            producer: request.producer,
            security: request.security.into(),
            compression: request.compression.into(),
            retention: request.retention.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredArtifactMetadata {
    schema_version: u16,
    artifact_id: String,
    artifact_reference: String,
    mime_type: String,
    byte_size: u64,
    creation_event: String,
    producer: String,
    security: StoredSecurity,
    compression: StoredCompression,
    retention: StoredRetention,
    content_hash: String,
}

impl StoredArtifactMetadata {
    fn into_dependency(self) -> Result<DependencyArtifactMetadata, ArtifactDependencyError> {
        let expected_id = format!("blake3:{}", self.content_hash);
        let expected_reference = format!("artifact:blake3:{}", self.content_hash);
        if self.schema_version != 1
            || !is_lower_hex(&self.content_hash, HASH_HEX_LENGTH)
            || self.artifact_id != expected_id
            || self.artifact_reference != expected_reference
        {
            return Err(ArtifactDependencyError::CorruptArtifact);
        }
        Ok(DependencyArtifactMetadata {
            artifact_id: ArtifactId(self.artifact_id),
            artifact_reference: ArtifactReference::parse(self.artifact_reference)?,
            mime_type: self.mime_type,
            byte_size: self.byte_size,
            creation_event: self.creation_event,
            producer: self.producer,
            security: self.security.into(),
            compression: self.compression.into(),
            retention: self.retention.into(),
            content_hash: self.content_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum StoredSecurity {
    Standard,
    Private,
    Secret,
}

impl From<DependencyArtifactSecurity> for StoredSecurity {
    fn from(value: DependencyArtifactSecurity) -> Self {
        match value {
            DependencyArtifactSecurity::Standard => Self::Standard,
            DependencyArtifactSecurity::Private => Self::Private,
            DependencyArtifactSecurity::Secret => Self::Secret,
        }
    }
}

impl From<StoredSecurity> for DependencyArtifactSecurity {
    fn from(value: StoredSecurity) -> Self {
        match value {
            StoredSecurity::Standard => Self::Standard,
            StoredSecurity::Private => Self::Private,
            StoredSecurity::Secret => Self::Secret,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum StoredCompression {
    None,
    Gzip,
    Zstd,
}

impl From<DependencyArtifactCompression> for StoredCompression {
    fn from(value: DependencyArtifactCompression) -> Self {
        match value {
            DependencyArtifactCompression::None => Self::None,
            DependencyArtifactCompression::Gzip => Self::Gzip,
            DependencyArtifactCompression::Zstd => Self::Zstd,
        }
    }
}

impl From<StoredCompression> for DependencyArtifactCompression {
    fn from(value: StoredCompression) -> Self {
        match value {
            StoredCompression::None => Self::None,
            StoredCompression::Gzip => Self::Gzip,
            StoredCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum StoredRetention {
    Permanent,
    Session,
    UntilUnixMilliseconds(i64),
}

impl From<DependencyArtifactRetention> for StoredRetention {
    fn from(value: DependencyArtifactRetention) -> Self {
        match value {
            DependencyArtifactRetention::Permanent => Self::Permanent,
            DependencyArtifactRetention::Session => Self::Session,
            DependencyArtifactRetention::UntilUnixMilliseconds(value) => {
                Self::UntilUnixMilliseconds(value)
            }
        }
    }
}

impl From<StoredRetention> for DependencyArtifactRetention {
    fn from(value: StoredRetention) -> Self {
        match value {
            StoredRetention::Permanent => Self::Permanent,
            StoredRetention::Session => Self::Session,
            StoredRetention::UntilUnixMilliseconds(value) => Self::UntilUnixMilliseconds(value),
        }
    }
}

fn initialize_transaction(
    transaction: &Path,
    pending: &StoredPendingMetadata,
) -> Result<(), ArtifactDependencyError> {
    File::create_new(transaction.join(CONTENT_FILE)).map_err(io_error)?;
    let encoded = serde_json::to_vec(pending)
        .map_err(|error| ArtifactDependencyError::Serialization(error.to_string()))?;
    let mut metadata =
        File::create_new(transaction.join(PENDING_METADATA_FILE)).map_err(io_error)?;
    metadata.write_all(&encoded).map_err(io_error)?;
    metadata.sync_all().map_err(io_error)
}

fn finalize_transaction(
    store: &LocalArtifactDependency,
    transaction: &Path,
) -> Result<DependencyFinalizeArtifactResponse, ArtifactDependencyError> {
    let pending_bytes = fs::read(transaction.join(PENDING_METADATA_FILE)).map_err(io_error)?;
    let pending: StoredPendingMetadata = serde_json::from_slice(&pending_bytes)
        .map_err(|error| ArtifactDependencyError::Serialization(error.to_string()))?;
    validate_mime(&pending.mime_type)?;
    validate_external_id("creation_event", &pending.creation_event)?;
    validate_external_id("producer", &pending.producer)?;
    let content_path = transaction.join(CONTENT_FILE);
    let (content_hash, byte_size) = hash_file(&content_path)?;
    if byte_size > store.limits.max_artifact_bytes {
        return Err(ArtifactDependencyError::ArtifactTooLarge {
            attempted: byte_size,
            maximum: store.limits.max_artifact_bytes,
        });
    }
    let artifact_id = format!("blake3:{content_hash}");
    let artifact_reference = format!("artifact:blake3:{content_hash}");
    let stored = StoredArtifactMetadata {
        schema_version: 1,
        artifact_id,
        artifact_reference,
        mime_type: pending.mime_type,
        byte_size,
        creation_event: pending.creation_event,
        producer: pending.producer,
        security: pending.security,
        compression: pending.compression,
        retention: pending.retention,
        content_hash: content_hash.clone(),
    };
    let target = store.object_path(&content_hash);
    if target.exists() {
        let metadata = read_metadata(&target)?;
        fs::remove_dir_all(transaction).map_err(io_error)?;
        return Ok(DependencyFinalizeArtifactResponse {
            metadata,
            deduplicated: true,
        });
    }

    let encoded = serde_json::to_vec(&stored)
        .map_err(|error| ArtifactDependencyError::Serialization(error.to_string()))?;
    let mut metadata = File::create_new(transaction.join(FINAL_METADATA_FILE)).map_err(io_error)?;
    metadata.write_all(&encoded).map_err(io_error)?;
    metadata.sync_all().map_err(io_error)?;
    drop(metadata);
    fs::remove_file(transaction.join(PENDING_METADATA_FILE))
        .map_err(|error| io_error_context("remove pending metadata", &error))?;
    let target_parent = target
        .parent()
        .ok_or(ArtifactDependencyError::UnsafeStoreEntry)?;
    fs::create_dir_all(target_parent)
        .map_err(|error| io_error_context("create object prefix", &error))?;
    let dependency_metadata = stored.clone().into_dependency()?;

    match fs::rename(transaction, &target) {
        Ok(()) => Ok(DependencyFinalizeArtifactResponse {
            metadata: dependency_metadata,
            deduplicated: false,
        }),
        Err(_error) if target.exists() => {
            fs::remove_dir_all(transaction).map_err(io_error)?;
            Ok(DependencyFinalizeArtifactResponse {
                metadata: read_metadata(&target)?,
                deduplicated: true,
            })
        }
        Err(error) => Err(io_error_context("rename finalized object", &error)),
    }
}

fn hash_file(path: &Path) -> Result<(String, u64), ArtifactDependencyError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    file.lock_exclusive().map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        total = total
            .checked_add(u64::try_from(count).map_err(|_| ArtifactDependencyError::SizeOverflow)?)
            .ok_or(ArtifactDependencyError::SizeOverflow)?;
    }
    fs2::FileExt::unlock(&file).map_err(io_error)?;
    Ok((hasher.finalize().to_hex().to_string(), total))
}

fn read_metadata(object: &Path) -> Result<DependencyArtifactMetadata, ArtifactDependencyError> {
    let bytes = fs::read(object.join(FINAL_METADATA_FILE)).map_err(artifact_io_error)?;
    let stored: StoredArtifactMetadata =
        serde_json::from_slice(&bytes).map_err(|_| ArtifactDependencyError::CorruptArtifact)?;
    let metadata = stored.into_dependency()?;
    let expected_directory = object
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ArtifactDependencyError::CorruptArtifact)?;
    if expected_directory != metadata.content_hash {
        return Err(ArtifactDependencyError::CorruptArtifact);
    }
    let (observed_hash, content_bytes) = hash_file(&object.join(CONTENT_FILE))?;
    if content_bytes != metadata.byte_size || observed_hash != metadata.content_hash {
        return Err(ArtifactDependencyError::CorruptArtifact);
    }
    Ok(metadata)
}

fn validate_mime(value: &str) -> Result<(), ArtifactDependencyError> {
    if value.is_empty()
        || value.len() > 255
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
        || value.split_once('/').is_none()
    {
        Err(ArtifactDependencyError::InvalidMetadata { field: "mime_type" })
    } else {
        Ok(())
    }
}

fn validate_external_id(field: &'static str, value: &str) -> Result<(), ArtifactDependencyError> {
    if value.is_empty()
        || value.len() > 128
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        Err(ArtifactDependencyError::InvalidMetadata { field })
    } else {
        Ok(())
    }
}

fn artifact_hash_from_reference(value: &str) -> Result<&str, ArtifactDependencyError> {
    let Some(hash) = value.strip_prefix("artifact:blake3:") else {
        return Err(ArtifactDependencyError::InvalidArtifactReference);
    };
    if is_lower_hex(hash, HASH_HEX_LENGTH) {
        Ok(hash)
    } else {
        Err(ArtifactDependencyError::InvalidArtifactReference)
    }
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn transaction_io_error(error: std::io::Error) -> ArtifactDependencyError {
    if error.kind() == std::io::ErrorKind::NotFound {
        ArtifactDependencyError::TransactionNotFound
    } else {
        io_error(error)
    }
}

fn artifact_io_error(error: std::io::Error) -> ArtifactDependencyError {
    if error.kind() == std::io::ErrorKind::NotFound {
        ArtifactDependencyError::ArtifactNotFound
    } else {
        io_error(error)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn io_error(error: std::io::Error) -> ArtifactDependencyError {
    ArtifactDependencyError::Storage(error.to_string())
}

fn io_error_context(context: &str, error: &std::io::Error) -> ArtifactDependencyError {
    ArtifactDependencyError::Storage(format!("{context}: {error}"))
}

/// Artifact external-adapter failure without leaking filesystem or serde types.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ArtifactDependencyError {
    /// Store root or limits are unsafe.
    #[error("artifact store configuration is invalid")]
    InvalidConfiguration,
    /// Write identifier is malformed or path-like.
    #[error("artifact write identifier is invalid")]
    InvalidWriteId,
    /// Portable artifact reference is malformed or path-like.
    #[error("artifact reference is invalid")]
    InvalidArtifactReference,
    /// Metadata failed dependency validation.
    #[error("artifact metadata field `{field}` is invalid")]
    InvalidMetadata {
        /// Invalid field name.
        field: &'static str,
    },
    /// An empty chunk was rejected.
    #[error("artifact chunk is empty")]
    EmptyChunk,
    /// One chunk exceeded its hard bound.
    #[error("artifact chunk has {actual} bytes; maximum is {maximum}")]
    ChunkTooLarge {
        /// Actual chunk bytes.
        actual: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// Transaction total exceeded its hard bound.
    #[error("artifact would have {attempted} bytes; maximum is {maximum}")]
    ArtifactTooLarge {
        /// Attempted total.
        attempted: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// Read range exceeded the per-call bound.
    #[error("artifact range requests {requested} bytes; maximum is {maximum}")]
    RangeTooLarge {
        /// Requested range bytes.
        requested: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// Range overflowed or exceeded artifact bounds.
    #[error("artifact range is outside content bounds")]
    InvalidRange,
    /// Transaction does not exist or has already reached a terminal state.
    #[error("artifact write transaction was not found")]
    TransactionNotFound,
    /// Finalized artifact does not exist.
    #[error("artifact was not found")]
    ArtifactNotFound,
    /// Stored content or metadata failed integrity validation.
    #[error("artifact storage is corrupt")]
    CorruptArtifact,
    /// Repeated generated identifiers collided.
    #[error("unable to allocate a unique artifact write identifier")]
    WriteIdCollision,
    /// Byte arithmetic exceeded platform limits.
    #[error("artifact size overflow")]
    SizeOverflow,
    /// Store contained an entry not owned by the adapter.
    #[error("artifact store contains an unsafe entry")]
    UnsafeStoreEntry,
    /// Serialization failed inside the dependency boundary.
    #[error("artifact serialization failed: {0}")]
    Serialization(String),
    /// Filesystem failure translated at the dependency boundary.
    #[error("artifact storage failed: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, LocalArtifactDependency) {
        let directory = tempfile::tempdir().expect("temporary artifact directory");
        let store = LocalArtifactDependency::new(
            directory.path().join("artifacts"),
            DependencyArtifactLimits {
                max_chunk_bytes: 8,
                max_artifact_bytes: 32,
                max_range_bytes: 16,
            },
        )
        .expect("artifact store");
        (directory, store)
    }

    fn start(store: &LocalArtifactDependency, creation_event: &str) -> ArtifactWriteId {
        store
            .start(DependencyStartArtifactRequest {
                mime_type: "text/plain; charset=utf-8".into(),
                creation_event: creation_event.into(),
                producer: "runtime-test".into(),
                security: DependencyArtifactSecurity::Private,
                compression: DependencyArtifactCompression::None,
                retention: DependencyArtifactRetention::Session,
            })
            .expect("start transaction")
            .write_id
    }

    fn write(store: &LocalArtifactDependency, write_id: &ArtifactWriteId, bytes: &[u8]) {
        store
            .write_chunk(DependencyWriteArtifactChunkRequest {
                write_id: write_id.clone(),
                bytes: bytes.to_vec(),
            })
            .expect("write chunk");
    }

    #[test]
    fn chunked_finalize_hashes_and_reads_ranges() {
        let (_directory, store) = store();
        let write_id = start(&store, "event-1");
        write(&store, &write_id, b"hello ");
        let response = store
            .write_chunk(DependencyWriteArtifactChunkRequest {
                write_id: write_id.clone(),
                bytes: b"world".to_vec(),
            })
            .expect("second chunk");
        assert_eq!(response.total_bytes, 11);

        let finalized = store
            .finalize(DependencyFinalizeArtifactRequest { write_id })
            .expect("finalize");
        assert!(!finalized.deduplicated);
        assert_eq!(finalized.metadata.byte_size, 11);
        assert_eq!(
            finalized.metadata.content_hash,
            blake3::hash(b"hello world").to_hex().to_string()
        );
        assert_eq!(finalized.metadata.creation_event, "event-1");
        assert_eq!(finalized.metadata.producer, "runtime-test");
        assert_eq!(
            finalized.metadata.security,
            DependencyArtifactSecurity::Private
        );

        let range = store
            .read_range(DependencyReadArtifactRangeRequest {
                artifact_reference: finalized.metadata.artifact_reference,
                offset: 6,
                length: 5,
            })
            .expect("bounded range");
        assert_eq!(range.bytes, b"world");
        assert_eq!(range.artifact_bytes, 11);
    }

    #[test]
    fn identical_content_is_deduplicated() {
        let (_directory, store) = store();
        let first = start(&store, "event-1");
        write(&store, &first, b"same");
        let first = store
            .finalize(DependencyFinalizeArtifactRequest { write_id: first })
            .expect("first finalize");

        let second = start(&store, "event-2");
        write(&store, &second, b"same");
        let second = store
            .finalize(DependencyFinalizeArtifactRequest { write_id: second })
            .expect("deduplicated finalize");

        assert!(second.deduplicated);
        assert_eq!(second.metadata.artifact_id, first.metadata.artifact_id);
        assert_eq!(
            second.metadata.artifact_reference,
            first.metadata.artifact_reference
        );
        assert_eq!(second.metadata.creation_event, "event-1");
    }

    #[test]
    fn abort_and_cleanup_leave_no_final_artifact() {
        let (_directory, store) = store();
        let aborted = start(&store, "event-abort");
        write(&store, &aborted, b"partial");
        store
            .abort(DependencyAbortArtifactRequest {
                write_id: aborted.clone(),
            })
            .expect("abort");
        assert_eq!(
            store.write_chunk(DependencyWriteArtifactChunkRequest {
                write_id: aborted,
                bytes: b"x".to_vec(),
            }),
            Err(ArtifactDependencyError::TransactionNotFound)
        );

        let incomplete = start(&store, "event-cleanup");
        write(&store, &incomplete, b"partial");
        let cleanup = store.cleanup_incomplete().expect("cleanup");
        assert_eq!(cleanup.removed_transactions, 1);
        assert!(object_directories(&store).is_empty());
    }

    #[test]
    fn invalid_ids_references_and_ranges_are_rejected() {
        let (_directory, store) = store();
        assert_eq!(
            ArtifactWriteId::parse("../escape"),
            Err(ArtifactDependencyError::InvalidWriteId)
        );
        assert_eq!(
            ArtifactReference::parse("artifact:blake3:../../escape"),
            Err(ArtifactDependencyError::InvalidArtifactReference)
        );

        let write_id = start(&store, "event-range");
        write(&store, &write_id, b"four");
        let artifact = store
            .finalize(DependencyFinalizeArtifactRequest { write_id })
            .expect("finalize");
        assert_eq!(
            store.read_range(DependencyReadArtifactRangeRequest {
                artifact_reference: artifact.metadata.artifact_reference,
                offset: 3,
                length: 2,
            }),
            Err(ArtifactDependencyError::InvalidRange)
        );
    }

    #[test]
    fn failed_finalize_exposes_no_partial_object() {
        let (_directory, store) = store();
        let write_id = start(&store, "event-corrupt");
        write(&store, &write_id, b"partial");
        fs::write(
            store.incomplete_path(&write_id).join(PENDING_METADATA_FILE),
            b"not-json",
        )
        .expect("corrupt pending fixture");

        assert!(matches!(
            store.finalize(DependencyFinalizeArtifactRequest { write_id }),
            Err(ArtifactDependencyError::Serialization(_))
        ));
        assert!(object_directories(&store).is_empty());
        assert_eq!(
            store.cleanup_incomplete().expect("cleanup"),
            DependencyCleanupArtifactsResponse {
                removed_transactions: 0
            }
        );
    }

    #[test]
    fn inspect_rejects_same_length_content_tampering() {
        let (_directory, store) = store();
        let write_id = start(&store, "event-tamper");
        write(&store, &write_id, b"original");
        let finalized = store
            .finalize(DependencyFinalizeArtifactRequest { write_id })
            .expect("finalize");
        let hash = finalized.metadata.content_hash.clone();
        fs::write(store.object_path(&hash).join(CONTENT_FILE), b"tampered")
            .expect("same-length tamper");

        assert_eq!(
            store.inspect(DependencyInspectArtifactRequest {
                artifact_reference: finalized.metadata.artifact_reference,
            }),
            Err(ArtifactDependencyError::CorruptArtifact)
        );
    }

    fn object_directories(store: &LocalArtifactDependency) -> Vec<PathBuf> {
        let mut objects = Vec::new();
        for prefix in fs::read_dir(store.root.join(OBJECTS_DIRECTORY)).expect("object root") {
            let prefix = prefix.expect("prefix entry");
            for object in fs::read_dir(prefix.path()).expect("prefix directory") {
                objects.push(object.expect("object entry").path());
            }
        }
        objects
    }
}
