//! Durable filesystem adapter for resume-once continuations.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use agentmod_primitives::SessionId;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const SCHEMA_VERSION: u32 = 2;
const MAX_RECORD_BYTES: u64 = 1024 * 1024;

/// Dependency-owned continuation state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyContinuationState {
    /// Awaiting a resolution.
    Pending,
    /// Claimed for execution by an approved resolution.
    Resumed,
    /// Permanently denied.
    Cancelled,
    /// Expired before resolution.
    Expired,
}

/// Dependency-owned durable continuation record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyContinuationRecord {
    /// Session directory scope beneath the configured sessions root.
    pub session_id: String,
    /// Opaque identifier, restricted to a portable filename alphabet.
    pub id: String,
    /// Current durable state.
    pub state: DependencyContinuationState,
    /// Serialized wake condition owned by the data boundary.
    pub wake_condition_json: Vec<u8>,
    /// Serialized pending-action payload owned by the data boundary.
    pub payload_json: Vec<u8>,
    /// Optional portable expiry in Unix milliseconds.
    pub expires_at_millis: Option<i64>,
}

/// Create request at the dependency boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateContinuationRequest {
    /// Complete initial record.
    pub record: DependencyContinuationRecord,
}

/// Atomic transition request at the dependency boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyTransitionContinuationRequest {
    /// Session containing the continuation.
    pub session_id: String,
    /// Continuation to claim.
    pub id: String,
    /// Required current state.
    pub expected: DependencyContinuationState,
    /// Desired terminal state.
    pub target: DependencyContinuationState,
}

/// Result of an idempotent compare-and-set transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyTransitionContinuationResponse {
    /// Whether this request changed durable state.
    pub transitioned: bool,
    /// State after the operation.
    pub current: DependencyContinuationState,
    /// Serialized pending action associated with the transition.
    pub payload_json: Vec<u8>,
}

/// Narrow persistence interface consumed by runtime data.
pub trait ContinuationDependencyPort {
    /// Creates a new pending continuation without overwriting an existing ID.
    ///
    /// An exact replay is accepted so a caller can safely retry after losing
    /// the response. Reusing the ID for different content remains an error.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDependencyError`] for invalid input or storage failure.
    fn create_continuation(
        &self,
        request: DependencyCreateContinuationRequest,
    ) -> Result<(), ContinuationDependencyError>;

    /// Loads a continuation by ID.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDependencyError`] for missing, corrupt, or inaccessible data.
    fn load_continuation(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError>;

    /// Atomically transitions a continuation if it still has the expected state.
    ///
    /// Repeating a request whose target is already current is an idempotent success
    /// with `transitioned == false`.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDependencyError`] for invalid transitions or storage failure.
    fn transition_continuation(
        &self,
        request: DependencyTransitionContinuationRequest,
    ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError>;

    /// Lists a bounded set of continuations across all session scopes.
    ///
    /// This is used only to recover the exact session-bound graph wait behind
    /// an opaque cancellation token. Implementations must fail closed rather
    /// than return a truncated result.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationDependencyError`] when bounded cross-session
    /// lookup is unsupported or storage cannot be read safely.
    fn list_continuations(
        &self,
        _limit: u32,
    ) -> Result<Vec<DependencyContinuationRecord>, ContinuationDependencyError> {
        Err(ContinuationDependencyError::LookupUnsupported)
    }
}

/// Filesystem-backed continuation store rooted at one session's continuation directory.
#[derive(Clone, Debug)]
pub struct FileContinuationDependency {
    root: PathBuf,
}

impl FileContinuationDependency {
    /// Creates a store. Directories are created lazily.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn continuation_root(&self, session_id: &str) -> Result<PathBuf, ContinuationDependencyError> {
        validate_id(session_id)?;
        Ok(self.root.join(session_id).join("continuations"))
    }

    fn record_path(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<PathBuf, ContinuationDependencyError> {
        validate_id(id)?;
        Ok(self
            .continuation_root(session_id)?
            .join(format!("{id}.json")))
    }

    fn lock_path(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<PathBuf, ContinuationDependencyError> {
        validate_id(id)?;
        Ok(self
            .continuation_root(session_id)?
            .join(format!("{id}.lock")))
    }

    fn with_lock<T>(
        &self,
        session_id: &str,
        id: &str,
        operation: impl FnOnce() -> Result<T, ContinuationDependencyError>,
    ) -> Result<T, ContinuationDependencyError> {
        let continuation_root = self.continuation_root(session_id)?;
        fs::create_dir_all(&continuation_root).map_err(ContinuationDependencyError::Io)?;
        let lock_path = self.lock_path(session_id, id)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(ContinuationDependencyError::Io)?;
        lock.lock_exclusive()
            .map_err(ContinuationDependencyError::Io)?;
        let result = operation();
        FileExt::unlock(&lock).map_err(ContinuationDependencyError::Io)?;
        result
    }

    fn read_record(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
        let path = self.record_path(session_id, id)?;
        let mut file = File::open(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ContinuationDependencyError::NotFound(id.to_owned())
            } else {
                ContinuationDependencyError::Io(error)
            }
        })?;
        let length = file
            .metadata()
            .map_err(ContinuationDependencyError::Io)?
            .len();
        if length > MAX_RECORD_BYTES {
            return Err(ContinuationDependencyError::RecordTooLarge(length));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| ContinuationDependencyError::RecordTooLarge(length))?;
        let mut bytes = Vec::with_capacity(capacity);
        file.read_to_end(&mut bytes)
            .map_err(ContinuationDependencyError::Io)?;
        let stored: StoredContinuation =
            serde_json::from_slice(&bytes).map_err(ContinuationDependencyError::Serialization)?;
        stored.verify(session_id, id)
    }

    fn write_new(
        &self,
        record: &DependencyContinuationRecord,
    ) -> Result<(), ContinuationDependencyError> {
        let path = self.record_path(&record.session_id, &record.id)?;
        let bytes = StoredContinuation::from_record(record)?.encode()?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    ContinuationDependencyError::AlreadyExists(record.id.clone())
                } else {
                    ContinuationDependencyError::Io(error)
                }
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(ContinuationDependencyError::Io)
    }

    fn replace(
        &self,
        record: &DependencyContinuationRecord,
    ) -> Result<(), ContinuationDependencyError> {
        let continuation_root = self.continuation_root(&record.session_id)?;
        let path = self.record_path(&record.session_id, &record.id)?;
        let temporary = continuation_root.join(format!("{}.tmp", record.id));
        let bytes = StoredContinuation::from_record(record)?.encode()?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(ContinuationDependencyError::Io)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(ContinuationDependencyError::Io)?;
        fs::rename(&temporary, &path).map_err(ContinuationDependencyError::Io)?;
        sync_directory(&continuation_root)
    }
}

impl ContinuationDependencyPort for FileContinuationDependency {
    fn create_continuation(
        &self,
        request: DependencyCreateContinuationRequest,
    ) -> Result<(), ContinuationDependencyError> {
        if request.record.state != DependencyContinuationState::Pending {
            return Err(ContinuationDependencyError::InvalidInitialState);
        }
        validate_id(&request.record.session_id)?;
        let id = request.record.id.clone();
        self.with_lock(&request.record.session_id, &id, || {
            if self.record_path(&request.record.session_id, &id)?.exists() {
                let existing = self.read_record(&request.record.session_id, &id)?;
                return if existing == request.record {
                    Ok(())
                } else {
                    Err(ContinuationDependencyError::AlreadyExists(id.clone()))
                };
            }
            self.write_new(&request.record)
        })
    }

    fn load_continuation(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
        self.with_lock(session_id, id, || self.read_record(session_id, id))
    }

    fn transition_continuation(
        &self,
        request: DependencyTransitionContinuationRequest,
    ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError> {
        if request.expected != DependencyContinuationState::Pending
            || request.target == DependencyContinuationState::Pending
        {
            return Err(ContinuationDependencyError::InvalidTransition);
        }
        validate_id(&request.session_id)?;
        self.with_lock(&request.session_id, &request.id, || {
            let mut record = self.read_record(&request.session_id, &request.id)?;
            if record.state == request.target {
                return Ok(DependencyTransitionContinuationResponse {
                    transitioned: false,
                    current: record.state,
                    payload_json: record.payload_json,
                });
            }
            if record.state != request.expected {
                return Err(ContinuationDependencyError::StateConflict {
                    current: record.state,
                    requested: request.target,
                });
            }
            record.state = request.target;
            self.replace(&record)?;
            Ok(DependencyTransitionContinuationResponse {
                transitioned: true,
                current: record.state,
                payload_json: record.payload_json,
            })
        })
    }

    fn list_continuations(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyContinuationRecord>, ContinuationDependencyError> {
        if limit == 0 || limit > 4096 {
            return Err(ContinuationDependencyError::InvalidLookupLimit);
        }
        let root = match fs::read_dir(&self.root) {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(ContinuationDependencyError::Io(error)),
        };
        let mut identities = Vec::new();
        for session in root {
            let session = session.map_err(ContinuationDependencyError::Io)?;
            if !session
                .file_type()
                .map_err(ContinuationDependencyError::Io)?
                .is_dir()
            {
                continue;
            }
            let session_name = session.file_name();
            let Some(session_id) = session_name.to_str() else {
                continue;
            };
            if SessionId::from_str(session_id).is_err() {
                continue;
            }
            let session_id = session_id.to_owned();
            let continuation_root = session.path().join("continuations");
            let entries = match fs::read_dir(continuation_root) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(ContinuationDependencyError::Io(error)),
            };
            for entry in entries {
                let entry = entry.map_err(ContinuationDependencyError::Io)?;
                if !entry
                    .file_type()
                    .map_err(ContinuationDependencyError::Io)?
                    .is_file()
                    || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
                {
                    continue;
                }
                let id = entry
                    .path()
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .ok_or(ContinuationDependencyError::InvalidId)?
                    .to_owned();
                validate_id(&id)?;
                identities.push((session_id.clone(), id));
            }
        }
        identities.sort();
        let limit =
            usize::try_from(limit).map_err(|_| ContinuationDependencyError::InvalidLookupLimit)?;
        let mut records = Vec::new();
        for (session_id, id) in identities {
            let record = self.read_record(&session_id, &id)?;
            if records.len() == limit {
                return Err(ContinuationDependencyError::LookupLimitExceeded);
            }
            records.push(record);
        }
        Ok(records)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct StoredContinuation {
    schema_version: u32,
    session_id: String,
    id: String,
    state: DependencyContinuationState,
    wake_condition_json: Vec<u8>,
    payload_json: Vec<u8>,
    expires_at_millis: Option<i64>,
    checksum: String,
}

#[derive(Serialize)]
struct ChecksumBody<'a> {
    schema_version: u32,
    session_id: &'a str,
    id: &'a str,
    state: DependencyContinuationState,
    wake_condition_json: &'a [u8],
    payload_json: &'a [u8],
    expires_at_millis: Option<i64>,
}

impl StoredContinuation {
    fn from_record(
        record: &DependencyContinuationRecord,
    ) -> Result<Self, ContinuationDependencyError> {
        validate_id(&record.id)?;
        validate_id(&record.session_id)?;
        let mut stored = Self {
            schema_version: SCHEMA_VERSION,
            session_id: record.session_id.clone(),
            id: record.id.clone(),
            state: record.state,
            wake_condition_json: record.wake_condition_json.clone(),
            payload_json: record.payload_json.clone(),
            expires_at_millis: record.expires_at_millis,
            checksum: String::new(),
        };
        stored.checksum = stored.calculate_checksum()?;
        Ok(stored)
    }

    fn calculate_checksum(&self) -> Result<String, ContinuationDependencyError> {
        let body = ChecksumBody {
            schema_version: self.schema_version,
            session_id: &self.session_id,
            id: &self.id,
            state: self.state,
            wake_condition_json: &self.wake_condition_json,
            payload_json: &self.payload_json,
            expires_at_millis: self.expires_at_millis,
        };
        let bytes =
            serde_json::to_vec(&body).map_err(ContinuationDependencyError::Serialization)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    fn encode(&self) -> Result<Vec<u8>, ContinuationDependencyError> {
        serde_json::to_vec(self).map_err(ContinuationDependencyError::Serialization)
    }

    fn verify(
        self,
        expected_session_id: &str,
        expected_id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ContinuationDependencyError::UnsupportedSchema(
                self.schema_version,
            ));
        }
        if self.session_id != expected_session_id
            || self.id != expected_id
            || self.checksum != self.calculate_checksum()?
        {
            return Err(ContinuationDependencyError::Integrity);
        }
        Ok(DependencyContinuationRecord {
            session_id: self.session_id,
            id: self.id,
            state: self.state,
            wake_condition_json: self.wake_condition_json,
            payload_json: self.payload_json,
            expires_at_millis: self.expires_at_millis,
        })
    }
}

fn validate_id(id: &str) -> Result<(), ContinuationDependencyError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(ContinuationDependencyError::InvalidId)
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), ContinuationDependencyError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(ContinuationDependencyError::Io)
}

#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(_path: &Path) -> Result<(), ContinuationDependencyError> {
    Ok(())
}

/// Durable continuation adapter failure.
#[derive(Debug, Error)]
pub enum ContinuationDependencyError {
    /// Identifier cannot safely become a local filename.
    #[error("continuation identifier is invalid")]
    InvalidId,
    /// Continuations must be created pending.
    #[error("continuation initial state must be pending")]
    InvalidInitialState,
    /// Only pending-to-terminal transitions are valid.
    #[error("continuation transition is invalid")]
    InvalidTransition,
    /// ID is already durable.
    #[error("continuation already exists: {0}")]
    AlreadyExists(String),
    /// ID does not exist.
    #[error("continuation not found: {0}")]
    NotFound(String),
    /// Durable state no longer matches the compare-and-set request.
    #[error("continuation is {current:?}; cannot transition to {requested:?}")]
    StateConflict {
        /// Current durable state.
        current: DependencyContinuationState,
        /// Requested state.
        requested: DependencyContinuationState,
    },
    /// Record exceeds its hard bound.
    #[error("continuation record is too large: {0} bytes")]
    RecordTooLarge(u64),
    /// Schema is not supported by this runtime.
    #[error("unsupported continuation schema version: {0}")]
    UnsupportedSchema(u32),
    /// Record identity or checksum is invalid.
    #[error("continuation integrity validation failed")]
    Integrity,
    /// Filesystem operation failed.
    #[error("continuation storage failed: {0}")]
    Io(#[source] std::io::Error),
    /// JSON encoding or decoding failed.
    #[error("continuation serialization failed: {0}")]
    Serialization(#[source] serde_json::Error),
    /// This dependency does not support bounded continuation lookup.
    #[error("continuation lookup is unsupported")]
    LookupUnsupported,
    /// Lookup limit is outside the safe supported range.
    #[error("continuation lookup limit is invalid")]
    InvalidLookupLimit,
    /// More pending continuations exist than the bounded lookup can inspect.
    #[error("continuation lookup limit exceeded")]
    LookupLimitExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(id: &str) -> DependencyContinuationRecord {
        DependencyContinuationRecord {
            session_id: "session_1".into(),
            id: id.into(),
            state: DependencyContinuationState::Pending,
            wake_condition_json: br#"{"kind":"manual"}"#.to_vec(),
            payload_json: br#"{"kind":"fixture"}"#.to_vec(),
            expires_at_millis: None,
        }
    }

    #[test]
    fn transition_is_durable_and_idempotent() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: pending("approval_1"),
            })
            .expect("create");
        let request = DependencyTransitionContinuationRequest {
            session_id: "session_1".into(),
            id: "approval_1".into(),
            expected: DependencyContinuationState::Pending,
            target: DependencyContinuationState::Resumed,
        };
        assert!(
            store
                .transition_continuation(request.clone())
                .expect("transition")
                .transitioned
        );
        assert!(
            !store
                .transition_continuation(request)
                .expect("duplicate")
                .transitioned
        );
        assert_eq!(
            FileContinuationDependency::new(directory.path().into())
                .load_continuation("session_1", "approval_1")
                .expect("load after restart")
                .state,
            DependencyContinuationState::Resumed
        );
    }

    #[test]
    fn cancelled_and_expired_terminal_transitions_are_durable_and_idempotent() {
        for (id, target) in [
            ("cancelled_1", DependencyContinuationState::Cancelled),
            ("expired_1", DependencyContinuationState::Expired),
        ] {
            let directory = tempfile::tempdir().expect("temp directory");
            let store = FileContinuationDependency::new(directory.path().into());
            store
                .create_continuation(DependencyCreateContinuationRequest {
                    record: pending(id),
                })
                .expect("create");
            let request = DependencyTransitionContinuationRequest {
                session_id: "session_1".into(),
                id: id.into(),
                expected: DependencyContinuationState::Pending,
                target,
            };
            let first = store
                .transition_continuation(request.clone())
                .expect("terminal transition");
            assert!(first.transitioned);
            assert_eq!(first.current, target);
            assert_eq!(first.payload_json, br#"{"kind":"fixture"}"#);
            let duplicate = store
                .transition_continuation(request)
                .expect("idempotent duplicate");
            assert!(!duplicate.transitioned);
            assert_eq!(duplicate.current, target);
            assert_eq!(
                FileContinuationDependency::new(directory.path().into())
                    .load_continuation("session_1", id)
                    .expect("load after restart")
                    .state,
                target
            );
        }
    }

    #[test]
    fn bounded_cross_session_lookup_is_deterministic_and_never_truncates() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        let mut first = pending("wait_b");
        first.session_id = uuid::Uuid::from_u128(1).to_string();
        let mut second = pending("wait_a");
        second.session_id = uuid::Uuid::from_u128(2).to_string();
        for record in [first.clone(), second.clone()] {
            store
                .create_continuation(DependencyCreateContinuationRequest { record })
                .expect("create");
        }
        assert!(matches!(
            store.list_continuations(1),
            Err(ContinuationDependencyError::LookupLimitExceeded)
        ));
        assert_eq!(
            FileContinuationDependency::new(directory.path().into())
                .list_continuations(2)
                .expect("lookup after restart"),
            vec![first, second]
        );
    }

    #[test]
    fn cross_session_lookup_ignores_internal_roots_but_rejects_corrupt_session_records() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        for reserved in [".process-hosts", ".workspace-leases", "not-a-session"] {
            let continuation_root = directory.path().join(reserved).join("continuations");
            fs::create_dir_all(&continuation_root).expect("reserved continuation root");
            fs::write(continuation_root.join("corrupt.json"), b"not json")
                .expect("reserved corrupt record");
        }
        let mut record = pending("valid_wait");
        record.session_id = uuid::Uuid::from_u128(3).to_string();
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: record.clone(),
            })
            .expect("valid continuation");
        assert_eq!(
            store.list_continuations(1).expect("bounded lookup"),
            vec![record.clone()]
        );

        fs::write(
            directory
                .path()
                .join(&record.session_id)
                .join("continuations")
                .join("corrupt.json"),
            b"not json",
        )
        .expect("valid-session corrupt record");
        assert!(matches!(
            store.list_continuations(2),
            Err(ContinuationDependencyError::Serialization(_))
        ));
    }

    #[test]
    fn rejects_path_like_identifiers_and_conflicting_duplicate_creation() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        assert!(matches!(
            store.create_continuation(DependencyCreateContinuationRequest {
                record: pending("../escape")
            }),
            Err(ContinuationDependencyError::InvalidId)
        ));
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: pending("same"),
            })
            .expect("first create");
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: pending("same"),
            })
            .expect("exact replay");
        let mut conflicting = pending("same");
        conflicting.payload_json = br#"{"kind":"different"}"#.to_vec();
        assert!(matches!(
            store.create_continuation(DependencyCreateContinuationRequest {
                record: conflicting
            }),
            Err(ContinuationDependencyError::AlreadyExists(_))
        ));
    }

    #[test]
    fn detects_record_tampering() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: pending("tamper"),
            })
            .expect("create");
        let path = directory
            .path()
            .join("session_1")
            .join("continuations")
            .join("tamper.json");
        let mut text = fs::read_to_string(&path).expect("read");
        text = text.replace("pending", "resumed");
        fs::write(path, text).expect("tamper");
        assert!(matches!(
            store.load_continuation("session_1", "tamper"),
            Err(ContinuationDependencyError::Integrity)
        ));
    }

    #[test]
    fn preserves_typed_graph_wait_payload_bytes_across_restart() {
        let directory = tempfile::tempdir().expect("temp directory");
        let store = FileContinuationDependency::new(directory.path().into());
        let mut record = pending("graph_wait_1");
        record.wake_condition_json = br#"{"kind":"at","value":123}"#.to_vec();
        record.payload_json = br#"{"kind":"graph_node_wait","value":{"session_id":"session_1","run_id":"run_1","branch_path":["root"],"node_id":"delay","executor_id":"runtime.delay","executor_version":"1.0.0","executor_source":{"kind":"runtime"},"execution_boundary":"runtime_logic","adapter_configuration_reference":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","execution_plan_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"loop_iteration":0,"step":2,"transition_target_node_id":"after_delay","compiled_transition_reference":"transition_hash","schedule_id":"schedule_1","cancellation_token":"token_1","cancellation_reference":"cancel_ref_1"}}"#.to_vec();
        store
            .create_continuation(DependencyCreateContinuationRequest {
                record: record.clone(),
            })
            .expect("create graph wait");
        let restored = FileContinuationDependency::new(directory.path().into())
            .load_continuation("session_1", "graph_wait_1")
            .expect("load graph wait after restart");
        assert_eq!(restored, record);
    }
}
