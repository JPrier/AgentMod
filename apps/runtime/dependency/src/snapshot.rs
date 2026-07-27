//! Versioned, validated, immutable snapshot persistence.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;
use uuid::Uuid;

const SNAPSHOT_DIRECTORY: &str = "snapshots";
const MAGIC: &[u8; 8] = b"AMSNAP01";
const HEADER_BYTES: u64 = 8 + 2 + 4 + 8 + 64 + 64 + 8;
const CHECKSUM_BYTES: usize = 64;
const HASH_BYTES: usize = 64;

/// Hard storage bounds for snapshot files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencySnapshotLimits {
    /// Maximum normalized state bytes.
    pub max_state_bytes: u64,
    /// Maximum complete snapshot file bytes.
    pub max_file_bytes: u64,
}

/// Dependency request to persist one immutable snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPersistSnapshotRequest {
    /// Session directory containing the snapshot collection.
    pub session_directory: PathBuf,
    /// Snapshot binary schema version.
    pub schema_version: u16,
    /// Pure reducer implementation version.
    pub reducer_version: u32,
    /// Last event included in the state.
    pub event_sequence: u64,
    /// Checksum of the terminal included event.
    pub terminal_event_checksum: String,
    /// BLAKE3 digest of the normalized state bytes.
    pub state_content_hash: String,
    /// Normalized opaque state bytes.
    pub state_bytes: Vec<u8>,
}

/// Dependency-owned validated snapshot metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySnapshotMetadata {
    /// Immutable safe filename.
    pub snapshot_name: String,
    /// Snapshot binary schema version.
    pub schema_version: u16,
    /// Pure reducer implementation version.
    pub reducer_version: u32,
    /// Last included event sequence.
    pub event_sequence: u64,
    /// Terminal event checksum.
    pub terminal_event_checksum: String,
    /// BLAKE3 normalized state digest.
    pub state_content_hash: String,
    /// Complete persisted file bytes.
    pub snapshot_bytes: u64,
}

/// Successful immutable snapshot persist result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPersistSnapshotResponse {
    /// Validated persisted metadata.
    pub metadata: DependencySnapshotMetadata,
    /// Whether the identical immutable name already existed.
    pub deduplicated: bool,
}

/// Dependency request to scan one session's snapshots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScanSnapshotsRequest {
    /// Session directory containing snapshots.
    pub session_directory: PathBuf,
}

/// Safe description of an invalid snapshot entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyInvalidSnapshot {
    /// Direct child filename only, never a filesystem path.
    pub snapshot_name: String,
    /// Dependency-owned readable validation reason.
    pub reason: String,
}

/// Validated snapshot catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScanSnapshotsResponse {
    /// Valid metadata ordered by sequence then name ascending.
    pub valid: Vec<DependencySnapshotMetadata>,
    /// Invalid entries ignored by latest-valid selection.
    pub invalid: Vec<DependencyInvalidSnapshot>,
}

/// Dependency request to load one validated immutable name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLoadSnapshotRequest {
    /// Session directory containing snapshots.
    pub session_directory: PathBuf,
    /// Safe immutable filename obtained from a scan.
    pub snapshot_name: String,
}

/// Fully loaded bounded snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySnapshotRecord {
    /// Validated snapshot metadata.
    pub metadata: DependencySnapshotMetadata,
    /// Exact normalized state bytes.
    pub state_bytes: Vec<u8>,
}

/// Snapshot persistence abstraction consumed only by runtime data.
pub trait SnapshotDependencyPort {
    /// Atomically persists an immutable snapshot.
    ///
    /// # Errors
    ///
    /// Returns a dependency error for invalid metadata, hash/size mismatch,
    /// name conflict, or storage failure.
    fn persist_snapshot(
        &self,
        request: DependencyPersistSnapshotRequest,
    ) -> Result<DependencyPersistSnapshotResponse, SnapshotDependencyError>;

    /// Scans and validates snapshot headers and state hashes.
    ///
    /// Corrupt individual files are returned in `invalid`; directory access
    /// failures are returned as errors.
    ///
    /// # Errors
    ///
    /// Returns a dependency error when the snapshot directory cannot be read.
    fn scan_snapshots(
        &self,
        request: DependencyScanSnapshotsRequest,
    ) -> Result<DependencyScanSnapshotsResponse, SnapshotDependencyError>;

    /// Loads one validated immutable snapshot by safe name.
    ///
    /// # Errors
    ///
    /// Returns a dependency error for unsafe names, corruption, size limits,
    /// missing files, or storage failures.
    fn load_snapshot(
        &self,
        request: DependencyLoadSnapshotRequest,
    ) -> Result<DependencySnapshotRecord, SnapshotDependencyError>;

    /// Loads the highest-sequence structurally valid snapshot.
    ///
    /// # Errors
    ///
    /// Returns a dependency error when scanning or loading fails.
    fn load_latest_valid_snapshot(
        &self,
        request: DependencyScanSnapshotsRequest,
    ) -> Result<Option<DependencySnapshotRecord>, SnapshotDependencyError>;
}

/// Local immutable binary snapshot adapter.
#[derive(Clone, Debug)]
pub struct LocalSnapshotDependency {
    limits: DependencySnapshotLimits,
}

impl LocalSnapshotDependency {
    /// Creates a bounded snapshot adapter.
    ///
    /// # Errors
    ///
    /// Rejects zero or internally inconsistent limits.
    pub fn new(limits: DependencySnapshotLimits) -> Result<Self, SnapshotDependencyError> {
        if limits.max_state_bytes == 0
            || limits.max_file_bytes < HEADER_BYTES
            || limits
                .max_state_bytes
                .checked_add(HEADER_BYTES)
                .is_none_or(|maximum| maximum > limits.max_file_bytes)
        {
            Err(SnapshotDependencyError::InvalidLimits)
        } else {
            Ok(Self { limits })
        }
    }
}

impl SnapshotDependencyPort for LocalSnapshotDependency {
    fn persist_snapshot(
        &self,
        request: DependencyPersistSnapshotRequest,
    ) -> Result<DependencyPersistSnapshotResponse, SnapshotDependencyError> {
        validate_persist_request(&request, self.limits)?;
        let snapshot_name = snapshot_name(request.event_sequence, &request.state_content_hash);
        let directory = snapshot_directory(&request.session_directory)?;
        let destination = directory.join(&snapshot_name);
        let metadata = DependencySnapshotMetadata {
            snapshot_name: snapshot_name.clone(),
            schema_version: request.schema_version,
            reducer_version: request.reducer_version,
            event_sequence: request.event_sequence,
            terminal_event_checksum: request.terminal_event_checksum.clone(),
            state_content_hash: request.state_content_hash.clone(),
            snapshot_bytes: HEADER_BYTES
                + u64::try_from(request.state_bytes.len())
                    .map_err(|_| SnapshotDependencyError::SizeOverflow)?,
        };

        if destination.exists() {
            let existing = read_snapshot(&destination, self.limits, true)?;
            if existing.metadata == metadata && existing.state_bytes == request.state_bytes {
                return Ok(DependencyPersistSnapshotResponse {
                    metadata,
                    deduplicated: true,
                });
            }
            return Err(SnapshotDependencyError::ImmutableNameConflict);
        }

        let temporary_name = format!(".{}.{}.tmp", snapshot_name, Uuid::now_v7().simple());
        let temporary = directory.join(temporary_name);
        let result = write_snapshot(&temporary, &request).and_then(|()| {
            match fs::rename(&temporary, &destination) {
                Ok(()) => {
                    #[cfg(unix)]
                    sync_directory(&directory)?;
                    Ok(DependencyPersistSnapshotResponse {
                        metadata,
                        deduplicated: false,
                    })
                }
                Err(_error) if destination.exists() => {
                    let existing = read_snapshot(&destination, self.limits, true)?;
                    if existing.metadata == metadata && existing.state_bytes == request.state_bytes
                    {
                        fs::remove_file(&temporary).map_err(storage_error)?;
                        Ok(DependencyPersistSnapshotResponse {
                            metadata,
                            deduplicated: true,
                        })
                    } else {
                        Err(SnapshotDependencyError::ImmutableNameConflict)
                    }
                }
                Err(error) => Err(storage_error(error)),
            }
        });
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn scan_snapshots(
        &self,
        request: DependencyScanSnapshotsRequest,
    ) -> Result<DependencyScanSnapshotsResponse, SnapshotDependencyError> {
        let directory = snapshot_directory(&request.session_directory)?;
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        for entry in fs::read_dir(directory).map_err(storage_error)? {
            let entry = entry.map_err(storage_error)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if !entry.file_type().map_err(storage_error)?.is_file() || !is_snapshot_name(&name) {
                invalid.push(DependencyInvalidSnapshot {
                    snapshot_name: name,
                    reason: "entry is not an immutable snapshot file".into(),
                });
                continue;
            }
            match read_snapshot(&entry.path(), self.limits, false) {
                Ok(record) => valid.push(record.metadata),
                Err(error) => invalid.push(DependencyInvalidSnapshot {
                    snapshot_name: name,
                    reason: error.to_string(),
                }),
            }
        }
        valid.sort_by(|left, right| {
            left.event_sequence
                .cmp(&right.event_sequence)
                .then_with(|| left.snapshot_name.cmp(&right.snapshot_name))
        });
        invalid.sort_by(|left, right| left.snapshot_name.cmp(&right.snapshot_name));
        Ok(DependencyScanSnapshotsResponse { valid, invalid })
    }

    fn load_snapshot(
        &self,
        request: DependencyLoadSnapshotRequest,
    ) -> Result<DependencySnapshotRecord, SnapshotDependencyError> {
        if !is_snapshot_name(&request.snapshot_name) {
            return Err(SnapshotDependencyError::UnsafeSnapshotName);
        }
        let directory = snapshot_directory(&request.session_directory)?;
        read_snapshot(&directory.join(request.snapshot_name), self.limits, true)
    }

    fn load_latest_valid_snapshot(
        &self,
        request: DependencyScanSnapshotsRequest,
    ) -> Result<Option<DependencySnapshotRecord>, SnapshotDependencyError> {
        let scan = self.scan_snapshots(request.clone())?;
        let Some(metadata) = scan.valid.last() else {
            return Ok(None);
        };
        self.load_snapshot(DependencyLoadSnapshotRequest {
            session_directory: request.session_directory,
            snapshot_name: metadata.snapshot_name.clone(),
        })
        .map(Some)
    }
}

fn validate_persist_request(
    request: &DependencyPersistSnapshotRequest,
    limits: DependencySnapshotLimits,
) -> Result<(), SnapshotDependencyError> {
    if request.session_directory.as_os_str().is_empty()
        || request.schema_version == 0
        || request.reducer_version == 0
        || request.event_sequence == 0
    {
        return Err(SnapshotDependencyError::InvalidMetadata);
    }
    if !is_lower_hex(&request.terminal_event_checksum, CHECKSUM_BYTES) {
        return Err(SnapshotDependencyError::InvalidAnchor);
    }
    if !is_lower_hex(&request.state_content_hash, HASH_BYTES) {
        return Err(SnapshotDependencyError::InvalidStateHash);
    }
    let state_bytes = u64::try_from(request.state_bytes.len())
        .map_err(|_| SnapshotDependencyError::SizeOverflow)?;
    if state_bytes > limits.max_state_bytes
        || state_bytes
            .checked_add(HEADER_BYTES)
            .is_none_or(|bytes| bytes > limits.max_file_bytes)
    {
        return Err(SnapshotDependencyError::SnapshotTooLarge);
    }
    if blake3::hash(&request.state_bytes).to_hex().as_str() != request.state_content_hash {
        return Err(SnapshotDependencyError::InvalidStateHash);
    }
    Ok(())
}

fn write_snapshot(
    path: &Path,
    request: &DependencyPersistSnapshotRequest,
) -> Result<(), SnapshotDependencyError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(storage_error)?;
    file.write_all(MAGIC).map_err(storage_error)?;
    file.write_all(&request.schema_version.to_le_bytes())
        .map_err(storage_error)?;
    file.write_all(&request.reducer_version.to_le_bytes())
        .map_err(storage_error)?;
    file.write_all(&request.event_sequence.to_le_bytes())
        .map_err(storage_error)?;
    file.write_all(request.terminal_event_checksum.as_bytes())
        .map_err(storage_error)?;
    file.write_all(request.state_content_hash.as_bytes())
        .map_err(storage_error)?;
    let state_bytes = u64::try_from(request.state_bytes.len())
        .map_err(|_| SnapshotDependencyError::SizeOverflow)?;
    file.write_all(&state_bytes.to_le_bytes())
        .map_err(storage_error)?;
    file.write_all(&request.state_bytes)
        .map_err(storage_error)?;
    file.flush().map_err(storage_error)?;
    file.sync_all().map_err(storage_error)
}

fn read_snapshot(
    path: &Path,
    limits: DependencySnapshotLimits,
    load_state: bool,
) -> Result<DependencySnapshotRecord, SnapshotDependencyError> {
    let file_bytes = fs::metadata(path).map_err(snapshot_storage_error)?.len();
    if file_bytes < HEADER_BYTES || file_bytes > limits.max_file_bytes {
        return Err(SnapshotDependencyError::SnapshotTooLarge);
    }
    let mut file = File::open(path).map_err(snapshot_storage_error)?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic).map_err(corruption_error)?;
    if &magic != MAGIC {
        return Err(SnapshotDependencyError::CorruptSnapshot);
    }
    let schema_version = read_u16(&mut file)?;
    let reducer_version = read_u32(&mut file)?;
    let event_sequence = read_u64(&mut file)?;
    let terminal_event_checksum = read_ascii(&mut file, CHECKSUM_BYTES)?;
    let state_content_hash = read_ascii(&mut file, HASH_BYTES)?;
    let state_length = read_u64(&mut file)?;
    if schema_version == 0
        || reducer_version == 0
        || event_sequence == 0
        || !is_lower_hex(&terminal_event_checksum, CHECKSUM_BYTES)
        || !is_lower_hex(&state_content_hash, HASH_BYTES)
        || state_length > limits.max_state_bytes
        || state_length
            .checked_add(HEADER_BYTES)
            .is_none_or(|expected| expected != file_bytes)
    {
        return Err(SnapshotDependencyError::CorruptSnapshot);
    }

    let allocation =
        usize::try_from(state_length).map_err(|_| SnapshotDependencyError::SnapshotTooLarge)?;
    let mut state_bytes = if load_state {
        Vec::with_capacity(allocation)
    } else {
        Vec::new()
    };
    let mut hasher = blake3::Hasher::new();
    let mut remaining = state_length;
    let mut buffer = vec![0_u8; 64 * 1024];
    while remaining > 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| SnapshotDependencyError::SnapshotTooLarge)?;
        file.read_exact(&mut buffer[..count])
            .map_err(corruption_error)?;
        hasher.update(&buffer[..count]);
        if load_state {
            state_bytes.extend_from_slice(&buffer[..count]);
        }
        remaining -= u64::try_from(count).map_err(|_| SnapshotDependencyError::SizeOverflow)?;
    }
    if hasher.finalize().to_hex().as_str() != state_content_hash {
        return Err(SnapshotDependencyError::InvalidStateHash);
    }
    let current_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(SnapshotDependencyError::UnsafeSnapshotName)?
        .to_owned();
    if current_name != snapshot_name(event_sequence, &state_content_hash) {
        return Err(SnapshotDependencyError::CorruptSnapshot);
    }
    Ok(DependencySnapshotRecord {
        metadata: DependencySnapshotMetadata {
            snapshot_name: current_name,
            schema_version,
            reducer_version,
            event_sequence,
            terminal_event_checksum,
            state_content_hash,
            snapshot_bytes: file_bytes,
        },
        state_bytes,
    })
}

fn snapshot_directory(session_directory: &Path) -> Result<PathBuf, SnapshotDependencyError> {
    if session_directory.as_os_str().is_empty() {
        return Err(SnapshotDependencyError::InvalidMetadata);
    }
    let directory = session_directory.join(SNAPSHOT_DIRECTORY);
    fs::create_dir_all(&directory).map_err(storage_error)?;
    Ok(directory)
}

fn snapshot_name(sequence: u64, state_hash: &str) -> String {
    format!("snapshot-{sequence:020}-{state_hash}.bin")
}

fn is_snapshot_name(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("snapshot-") else {
        return false;
    };
    let Some((sequence, hash_and_extension)) = rest.split_once('-') else {
        return false;
    };
    let Some(hash) = hash_and_extension.strip_suffix(".bin") else {
        return false;
    };
    sequence.len() == 20
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && is_lower_hex(hash, HASH_BYTES)
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn read_u16(file: &mut File) -> Result<u16, SnapshotDependencyError> {
    let mut bytes = [0_u8; 2];
    file.read_exact(&mut bytes).map_err(corruption_error)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(file: &mut File) -> Result<u32, SnapshotDependencyError> {
    let mut bytes = [0_u8; 4];
    file.read_exact(&mut bytes).map_err(corruption_error)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(file: &mut File) -> Result<u64, SnapshotDependencyError> {
    let mut bytes = [0_u8; 8];
    file.read_exact(&mut bytes).map_err(corruption_error)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_ascii(file: &mut File, length: usize) -> Result<String, SnapshotDependencyError> {
    let mut bytes = vec![0_u8; length];
    file.read_exact(&mut bytes).map_err(corruption_error)?;
    String::from_utf8(bytes).map_err(|_| SnapshotDependencyError::CorruptSnapshot)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SnapshotDependencyError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)
}

fn corruption_error(_error: std::io::Error) -> SnapshotDependencyError {
    SnapshotDependencyError::CorruptSnapshot
}

fn snapshot_storage_error(error: std::io::Error) -> SnapshotDependencyError {
    if error.kind() == std::io::ErrorKind::NotFound {
        SnapshotDependencyError::SnapshotNotFound
    } else {
        storage_error(error)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn storage_error(error: std::io::Error) -> SnapshotDependencyError {
    SnapshotDependencyError::Storage(error.to_string())
}

/// Snapshot dependency failure without filesystem implementation types.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotDependencyError {
    /// Limits were zero or inconsistent.
    #[error("snapshot limits are invalid")]
    InvalidLimits,
    /// Required version, sequence, or session metadata is invalid.
    #[error("snapshot metadata is invalid")]
    InvalidMetadata,
    /// Terminal event checksum is malformed.
    #[error("snapshot terminal event anchor is invalid")]
    InvalidAnchor,
    /// State digest is malformed or does not match bytes.
    #[error("snapshot state content hash is invalid")]
    InvalidStateHash,
    /// Snapshot exceeds configured state or file bounds.
    #[error("snapshot exceeds configured size limits")]
    SnapshotTooLarge,
    /// Byte arithmetic overflowed.
    #[error("snapshot size overflow")]
    SizeOverflow,
    /// Safe immutable name already refers to different content or metadata.
    #[error("immutable snapshot name conflicts with existing content")]
    ImmutableNameConflict,
    /// Snapshot name is path-like or not generated by this adapter.
    #[error("snapshot name is unsafe")]
    UnsafeSnapshotName,
    /// Requested immutable snapshot is absent.
    #[error("snapshot was not found")]
    SnapshotNotFound,
    /// Snapshot framing, metadata, or filename is corrupt.
    #[error("snapshot is corrupt")]
    CorruptSnapshot,
    /// Filesystem failure translated inside dependency.
    #[error("snapshot storage failed: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter(max_state_bytes: u64) -> LocalSnapshotDependency {
        LocalSnapshotDependency::new(DependencySnapshotLimits {
            max_state_bytes,
            max_file_bytes: HEADER_BYTES + max_state_bytes,
        })
        .expect("snapshot adapter")
    }

    fn request(session: &Path, sequence: u64, state: &[u8]) -> DependencyPersistSnapshotRequest {
        DependencyPersistSnapshotRequest {
            session_directory: session.to_owned(),
            schema_version: 1,
            reducer_version: 3,
            event_sequence: sequence,
            terminal_event_checksum: blake3::hash(format!("event-{sequence}").as_bytes())
                .to_hex()
                .to_string(),
            state_content_hash: blake3::hash(state).to_hex().to_string(),
            state_bytes: state.to_vec(),
        }
    }

    #[test]
    fn roundtrip_and_latest_valid_selection() {
        let directory = tempfile::tempdir().expect("temp directory");
        let adapter = adapter(1024);
        let first = adapter
            .persist_snapshot(request(directory.path(), 4, b"{\"turn\":4}"))
            .expect("first snapshot");
        let latest = adapter
            .persist_snapshot(request(directory.path(), 9, b"{\"turn\":9}"))
            .expect("latest snapshot");
        assert!(!first.deduplicated);
        assert!(!latest.deduplicated);

        let loaded = adapter
            .load_latest_valid_snapshot(DependencyScanSnapshotsRequest {
                session_directory: directory.path().to_owned(),
            })
            .expect("latest selection")
            .expect("snapshot exists");
        assert_eq!(loaded.metadata.event_sequence, 9);
        assert_eq!(loaded.state_bytes, b"{\"turn\":9}");
    }

    #[test]
    fn deduplicates_exact_immutable_snapshot() {
        let directory = tempfile::tempdir().expect("temp directory");
        let adapter = adapter(1024);
        let request = request(directory.path(), 1, b"state");
        adapter
            .persist_snapshot(request.clone())
            .expect("first persist");
        assert!(
            adapter
                .persist_snapshot(request)
                .expect("deduplicated persist")
                .deduplicated
        );
    }

    #[test]
    fn corrupt_snapshot_is_reported_and_skipped() {
        let directory = tempfile::tempdir().expect("temp directory");
        let adapter = adapter(1024);
        let valid = adapter
            .persist_snapshot(request(directory.path(), 2, b"valid"))
            .expect("valid persist");
        let corrupt_hash = blake3::hash(b"broken").to_hex();
        let corrupt_name = snapshot_name(10, corrupt_hash.as_str());
        fs::write(
            directory.path().join(SNAPSHOT_DIRECTORY).join(corrupt_name),
            b"partial",
        )
        .expect("corrupt fixture");

        let scan = adapter
            .scan_snapshots(DependencyScanSnapshotsRequest {
                session_directory: directory.path().to_owned(),
            })
            .expect("scan");
        assert_eq!(scan.valid, vec![valid.metadata]);
        assert_eq!(scan.invalid.len(), 1);
        assert_eq!(
            adapter
                .load_latest_valid_snapshot(DependencyScanSnapshotsRequest {
                    session_directory: directory.path().to_owned(),
                })
                .expect("latest")
                .expect("valid remains")
                .state_bytes,
            b"valid"
        );
    }

    #[test]
    fn invalid_hash_and_size_leave_no_partial_snapshot() {
        let directory = tempfile::tempdir().expect("temp directory");
        let adapter = adapter(4);
        let mut invalid_hash = request(directory.path(), 1, b"four");
        invalid_hash.state_content_hash = "0".repeat(HASH_BYTES);
        assert_eq!(
            adapter.persist_snapshot(invalid_hash),
            Err(SnapshotDependencyError::InvalidStateHash)
        );
        assert_eq!(
            adapter.persist_snapshot(request(directory.path(), 2, b"oversize")),
            Err(SnapshotDependencyError::SnapshotTooLarge)
        );
        let snapshot_directory = directory.path().join(SNAPSHOT_DIRECTORY);
        if snapshot_directory.exists() {
            let entries: Vec<_> = fs::read_dir(snapshot_directory)
                .expect("snapshot directory")
                .collect();
            assert!(entries.is_empty());
        }
    }

    #[test]
    fn unsafe_name_and_invalid_anchor_are_rejected() {
        let directory = tempfile::tempdir().expect("temp directory");
        let adapter = adapter(1024);
        assert_eq!(
            adapter.load_snapshot(DependencyLoadSnapshotRequest {
                session_directory: directory.path().to_owned(),
                snapshot_name: "../events.jsonl".into(),
            }),
            Err(SnapshotDependencyError::UnsafeSnapshotName)
        );
        let mut invalid_anchor = request(directory.path(), 1, b"state");
        invalid_anchor.terminal_event_checksum = "../journal".into();
        assert_eq!(
            adapter.persist_snapshot(invalid_anchor),
            Err(SnapshotDependencyError::InvalidAnchor)
        );
    }
}
