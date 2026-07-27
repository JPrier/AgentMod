//! Atomic filesystem session-directory and metadata adapter.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{CausationId, CorrelationId, EventId, SessionId, TimestampMillis};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::journal::{
    DependencyAppendJournalRequest, DependencyDurability, JournalDependencyPort,
    JsonlJournalDependency,
};

const METADATA_LIMIT: u64 = 64 * 1024;
const BRANCH_ARTIFACT_LIMIT: usize = 16 * 1024 * 1024;
const SCHEMA_VERSION: u32 = 1;

/// IDs and normalized workspace obtained from external system dependencies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPreparedSession {
    /// New canonical session identifier.
    pub session_id: SessionId,
    /// Initial event identifier.
    pub event_id: EventId,
    /// Initial correlation identifier.
    pub correlation_id: CorrelationId,
    /// Initial causation identifier.
    pub causation_id: CausationId,
    /// Dependency clock value.
    pub timestamp: TimestampMillis,
    /// Canonical existing workspace path.
    pub normalized_workspace: PathBuf,
}

/// Preparation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyPrepareSessionRequest {
    /// User-selected workspace.
    pub workspace: PathBuf,
}

/// Atomic session creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateSessionRequest {
    /// Sessions storage root.
    pub sessions_root: PathBuf,
    /// Prepared external values.
    pub prepared: DependencyPreparedSession,
    /// Explicit validated style.
    pub style: String,
    /// Canonical initial event JSON.
    pub initial_event_json: Vec<u8>,
}

/// Created session dependency response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreatedSession {
    /// Final durable directory.
    pub session_directory: PathBuf,
}

/// One complete canonical event supplied for atomic branch creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchEvent {
    /// Strict child-session sequence.
    pub sequence: u64,
    /// Fresh child-session event identifier.
    pub event_id: String,
    /// Sealed canonical envelope bytes.
    pub event_json: Vec<u8>,
}

/// One immutable artifact staged atomically with a child session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyBranchArtifact {
    /// Opaque UUID identity used by canonical conversation entries.
    pub artifact_id: String,
    /// Exact BLAKE3 hash rendered as lowercase hex.
    pub content_hash: String,
    /// Stable media type.
    pub mime_type: String,
    /// Canonical child event that first references this artifact.
    pub creation_event: String,
    /// Complete bounded artifact bytes.
    pub bytes: Vec<u8>,
}

/// Atomic branch creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateBranchRequest {
    /// Sessions storage root.
    pub sessions_root: PathBuf,
    /// Prepared child identity and normalized workspace.
    pub prepared: DependencyPreparedSession,
    /// Explicit validated child style.
    pub style: String,
    /// Immutable parent session identifier.
    pub parent_session_id: String,
    /// Inclusive parent sequence used to construct the child.
    pub fork_sequence: u64,
    /// Complete child journal, starting at sequence one.
    pub events: Vec<DependencyBranchEvent>,
    /// Immutable artifacts committed with the child before its atomic rename.
    pub artifacts: Vec<DependencyBranchArtifact>,
}

/// Session listing request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyListSessionsRequest {
    /// Sessions storage root.
    pub sessions_root: PathBuf,
    /// Strict result bound.
    pub limit: usize,
}

/// Dependency-owned lightweight metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySessionMetadata {
    /// Parsed session identifier text.
    pub session_id: String,
    /// Safe workspace label.
    pub workspace: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence from metadata.
    pub sequence: u64,
    /// Lifecycle label.
    pub state: String,
    /// Creation timestamp for stable ordering.
    pub created_at_millis: i64,
    /// Parent session for a branch.
    pub parent_session_id: Option<String>,
    /// Inclusive parent fork point.
    pub fork_sequence: Option<u64>,
}

/// Narrow session catalog dependency used by runtime data.
pub trait SessionCatalogDependencyPort {
    /// Canonicalizes the workspace and obtains IDs/time.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCatalogDependencyError`] when the workspace or external
    /// ID/clock sources are unavailable.
    fn prepare_session(
        &self,
        request: DependencyPrepareSessionRequest,
    ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError>;

    /// Atomically creates the required session directory and initial journal.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCatalogDependencyError`] for invalid roots, collisions,
    /// journal failures, or filesystem failures.
    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError>;

    /// Atomically creates a child session with a complete remapped journal.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCatalogDependencyError`] when ancestry, event ordering,
    /// persistence, or the final atomic rename fails.
    fn create_branch(
        &self,
        request: DependencyCreateBranchRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError>;

    /// Reads bounded metadata without loading conversations.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCatalogDependencyError`] when the root cannot be
    /// enumerated. Invalid individual metadata records are skipped.
    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError>;
}

/// Local filesystem session catalog.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileSessionCatalogDependency;

impl SessionCatalogDependencyPort for FileSessionCatalogDependency {
    fn prepare_session(
        &self,
        request: DependencyPrepareSessionRequest,
    ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError> {
        let normalized_workspace = request.workspace.canonicalize().map_err(|error| {
            SessionCatalogDependencyError::WorkspaceUnavailable(error.to_string())
        })?;
        if !normalized_workspace.is_dir() {
            return Err(SessionCatalogDependencyError::WorkspaceNotDirectory);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SessionCatalogDependencyError::ClockBeforeEpoch)?;
        let millis = i64::try_from(now.as_millis())
            .map_err(|_| SessionCatalogDependencyError::ClockOverflow)?;
        Ok(DependencyPreparedSession {
            session_id: SessionId::from_uuid(Uuid::now_v7()),
            event_id: EventId::from_uuid(Uuid::now_v7()),
            correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
            causation_id: CausationId::from_uuid(Uuid::now_v7()),
            timestamp: TimestampMillis::new(millis),
            normalized_workspace,
        })
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        validate_root(&request.sessions_root)?;
        fs::create_dir_all(&request.sessions_root).map_err(map_io)?;
        let final_directory = request
            .sessions_root
            .join(request.prepared.session_id.to_string());
        if final_directory.exists() {
            return Err(SessionCatalogDependencyError::AlreadyExists);
        }
        let temporary = request
            .sessions_root
            .join(format!(".creating-{}", request.prepared.session_id));
        fs::create_dir(&temporary).map_err(map_io)?;
        if let Err(error) = populate_directory(&temporary, &request) {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &final_directory).map_err(map_io)?;
        sync_directory(&request.sessions_root)?;
        Ok(DependencyCreatedSession {
            session_directory: final_directory,
        })
    }

    fn create_branch(
        &self,
        request: DependencyCreateBranchRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        validate_root(&request.sessions_root)?;
        validate_branch(&request)?;
        fs::create_dir_all(&request.sessions_root).map_err(map_io)?;
        let final_directory = request
            .sessions_root
            .join(request.prepared.session_id.to_string());
        if final_directory.exists() {
            return Err(SessionCatalogDependencyError::AlreadyExists);
        }
        let temporary = request
            .sessions_root
            .join(format!(".creating-{}", request.prepared.session_id));
        fs::create_dir(&temporary).map_err(map_io)?;
        if let Err(error) = populate_branch_directory(&temporary, &request) {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &final_directory).map_err(map_io)?;
        sync_directory(&request.sessions_root)?;
        Ok(DependencyCreatedSession {
            session_directory: final_directory,
        })
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError> {
        if request.limit == 0 || !request.sessions_root.exists() {
            return Ok(vec![]);
        }
        let mut records = Vec::new();
        for entry in fs::read_dir(&request.sessions_root).map_err(map_io)? {
            let entry = entry.map_err(map_io)?;
            if !entry.file_type().map_err(map_io)?.is_dir()
                || entry.file_name().to_string_lossy().starts_with('.')
            {
                continue;
            }
            if let Ok(mut metadata) = read_metadata(&entry.path().join("metadata.json")) {
                if let Ok(journal) =
                    JsonlJournalDependency.scan(crate::journal::DependencyScanJournalRequest {
                        session_directory: entry.path(),
                    })
                    && let Some(tail) = journal.records.last()
                {
                    metadata.sequence = tail.sequence;
                }
                records.push(metadata.into());
            }
        }
        records.sort_by(|left: &DependencySessionMetadata, right| {
            right
                .created_at_millis
                .cmp(&left.created_at_millis)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        records.truncate(request.limit);
        Ok(records)
    }
}

impl SessionCatalogDependencyPort for crate::LocalRuntimeDependencies {
    fn prepare_session(
        &self,
        request: DependencyPrepareSessionRequest,
    ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.prepare_session(request)
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.create_session(request)
    }

    fn create_branch(
        &self,
        request: DependencyCreateBranchRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.create_branch(request)
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.list_sessions(request)
    }
}

fn populate_directory(
    temporary: &Path,
    request: &DependencyCreateSessionRequest,
) -> Result<(), SessionCatalogDependencyError> {
    create_session_subdirectories(temporary)?;
    let workspace = request.prepared.normalized_workspace.to_string_lossy();
    let metadata = StoredMetadata {
        schema_version: SCHEMA_VERSION,
        session_id: request.prepared.session_id.to_string(),
        workspace: workspace.as_ref(),
        style: &request.style,
        sequence: 1,
        state: "active",
        created_at_millis: request.prepared.timestamp.get(),
        parent_session_id: None,
        fork_sequence: None,
    };
    write_session_descriptors(temporary, &metadata, workspace.as_ref(), &request.style)?;
    JsonlJournalDependency
        .append(DependencyAppendJournalRequest {
            session_directory: temporary.to_owned(),
            sequence: 1,
            event_id: request.prepared.event_id.to_string(),
            event_json: request.initial_event_json.clone(),
            durability: DependencyDurability::Full,
        })
        .map_err(|error| SessionCatalogDependencyError::Journal(error.to_string()))?;
    sync_directory(temporary)
}

fn create_session_subdirectories(temporary: &Path) -> Result<(), SessionCatalogDependencyError> {
    for directory in [
        "continuations",
        "snapshots",
        "artifacts",
        "process-logs",
        "branches",
    ] {
        fs::create_dir(temporary.join(directory)).map_err(map_io)?;
    }
    Ok(())
}

fn write_session_descriptors(
    temporary: &Path,
    metadata: &StoredMetadata<'_>,
    workspace: &str,
    style: &str,
) -> Result<(), SessionCatalogDependencyError> {
    atomic_json(temporary.join("metadata.json"), &metadata)?;
    atomic_json(
        temporary.join("workspace.json"),
        &serde_json::json!({"schema_version": SCHEMA_VERSION, "path": workspace}),
    )?;
    atomic_json(
        temporary.join("style.json"),
        &serde_json::json!({"schema_version": SCHEMA_VERSION, "id": style}),
    )?;
    atomic_json(
        temporary.join("style.lock"),
        &serde_json::json!({"schema_version": SCHEMA_VERSION, "id": style}),
    )?;
    Ok(())
}

fn populate_branch_directory(
    temporary: &Path,
    request: &DependencyCreateBranchRequest,
) -> Result<(), SessionCatalogDependencyError> {
    create_session_subdirectories(temporary)?;
    let workspace = request.prepared.normalized_workspace.to_string_lossy();
    let metadata = StoredMetadata {
        schema_version: SCHEMA_VERSION,
        session_id: request.prepared.session_id.to_string(),
        workspace: workspace.as_ref(),
        style: &request.style,
        sequence: u64::try_from(request.events.len())
            .map_err(|_| SessionCatalogDependencyError::SequenceOverflow)?,
        state: "active",
        created_at_millis: request.prepared.timestamp.get(),
        parent_session_id: Some(&request.parent_session_id),
        fork_sequence: Some(request.fork_sequence),
    };
    write_session_descriptors(temporary, &metadata, workspace.as_ref(), &request.style)?;
    for artifact in &request.artifacts {
        let artifact_directory = temporary.join("artifacts");
        let path = artifact_directory.join(format!("{}.json", artifact.artifact_id));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(map_io)?;
        file.write_all(&artifact.bytes).map_err(map_io)?;
        file.sync_all().map_err(map_io)?;
        atomic_json(
            artifact_directory.join(format!("{}.metadata.json", artifact.artifact_id)),
            &serde_json::json!({
                "schema_version": 1,
                "artifact_id": artifact.artifact_id,
                "content_hash": artifact.content_hash,
                "mime_type": artifact.mime_type,
                "byte_size": artifact.bytes.len(),
                "creation_event": artifact.creation_event,
                "producer": "runtime.branch",
                "security": "private",
                "compression": "none",
                "retention": "session"
            }),
        )?;
    }
    for event in &request.events {
        JsonlJournalDependency
            .append(DependencyAppendJournalRequest {
                session_directory: temporary.to_owned(),
                sequence: event.sequence,
                event_id: event.event_id.clone(),
                event_json: event.event_json.clone(),
                durability: DependencyDurability::Full,
            })
            .map_err(|error| SessionCatalogDependencyError::Journal(error.to_string()))?;
    }
    sync_directory(temporary)
}

fn validate_branch(
    request: &DependencyCreateBranchRequest,
) -> Result<(), SessionCatalogDependencyError> {
    if request.parent_session_id.is_empty() || request.fork_sequence == 0 {
        return Err(SessionCatalogDependencyError::InvalidBranchAncestry);
    }
    if request.events.len() < 2 {
        return Err(SessionCatalogDependencyError::InvalidBranchEvents);
    }
    for artifact in &request.artifacts {
        if Uuid::parse_str(&artifact.artifact_id).is_err()
            || Uuid::parse_str(&artifact.creation_event).is_err()
            || artifact.mime_type != "application/vnd.agentmod.branch-context+json"
            || artifact.bytes.is_empty()
            || artifact.bytes.len() > BRANCH_ARTIFACT_LIMIT
            || artifact.content_hash != blake3::hash(&artifact.bytes).to_hex().as_str()
        {
            return Err(SessionCatalogDependencyError::InvalidBranchArtifact);
        }
    }
    for (index, event) in request.events.iter().enumerate() {
        let expected = u64::try_from(index)
            .map_err(|_| SessionCatalogDependencyError::SequenceOverflow)?
            .checked_add(1)
            .ok_or(SessionCatalogDependencyError::SequenceOverflow)?;
        if event.sequence != expected || event.event_id.is_empty() || event.event_json.is_empty() {
            return Err(SessionCatalogDependencyError::InvalidBranchEvents);
        }
    }
    Ok(())
}

fn atomic_json(path: PathBuf, value: &impl Serialize) -> Result<(), SessionCatalogDependencyError> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| SessionCatalogDependencyError::Serialization(error.to_string()))?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(map_io)?;
    file.write_all(&bytes).map_err(map_io)?;
    file.sync_all().map_err(map_io)?;
    fs::rename(temporary, path).map_err(map_io)
}

fn read_metadata(path: &Path) -> Result<StoredMetadataOwned, SessionCatalogDependencyError> {
    let file = File::open(path).map_err(map_io)?;
    if file.metadata().map_err(map_io)?.len() > METADATA_LIMIT {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let mut bytes = Vec::new();
    file.take(METADATA_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(map_io)?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let metadata: StoredMetadataOwned = serde_json::from_slice(&bytes)
        .map_err(|error| SessionCatalogDependencyError::Serialization(error.to_string()))?;
    if metadata.schema_version != SCHEMA_VERSION {
        return Err(SessionCatalogDependencyError::UnsupportedSchema);
    }
    Ok(metadata)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SessionCatalogDependencyError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(map_io)
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(_path: &Path) -> Result<(), SessionCatalogDependencyError> {
    // Opening a directory as `std::fs::File` is denied on Windows. Every file
    // is individually synchronized before the final same-volume rename.
    Ok(())
}

fn validate_root(root: &Path) -> Result<(), SessionCatalogDependencyError> {
    if root.as_os_str().is_empty() {
        return Err(SessionCatalogDependencyError::InvalidRoot);
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn map_io(error: std::io::Error) -> SessionCatalogDependencyError {
    SessionCatalogDependencyError::Io(error.to_string())
}

#[derive(Serialize)]
struct StoredMetadata<'a> {
    schema_version: u32,
    session_id: String,
    workspace: &'a str,
    style: &'a str,
    sequence: u64,
    state: &'a str,
    created_at_millis: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_sequence: Option<u64>,
}

#[derive(Deserialize)]
struct StoredMetadataOwned {
    schema_version: u32,
    session_id: String,
    workspace: String,
    style: String,
    sequence: u64,
    state: String,
    created_at_millis: i64,
    #[serde(default)]
    parent_session_id: Option<String>,
    #[serde(default)]
    fork_sequence: Option<u64>,
}

impl From<StoredMetadataOwned> for DependencySessionMetadata {
    fn from(value: StoredMetadataOwned) -> Self {
        Self {
            session_id: value.session_id,
            workspace: value.workspace,
            style: value.style,
            sequence: value.sequence,
            state: value.state,
            created_at_millis: value.created_at_millis,
            parent_session_id: value.parent_session_id,
            fork_sequence: value.fork_sequence,
        }
    }
}

/// Session catalog adapter failure with external details redacted.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionCatalogDependencyError {
    /// Workspace cannot be resolved.
    #[error("workspace is unavailable: {0}")]
    WorkspaceUnavailable(String),
    /// Workspace is not a directory.
    #[error("workspace is not a directory")]
    WorkspaceNotDirectory,
    /// System clock is earlier than the Unix epoch.
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    /// System clock cannot be represented.
    #[error("system clock value overflow")]
    ClockOverflow,
    /// Sessions root is empty.
    #[error("sessions root is invalid")]
    InvalidRoot,
    /// Session ID already exists.
    #[error("session already exists")]
    AlreadyExists,
    /// External filesystem operation failed.
    #[error("session filesystem operation failed: {0}")]
    Io(String),
    /// Initial event journal failed.
    #[error("initial event journal failed: {0}")]
    Journal(String),
    /// Metadata serialization failed.
    #[error("session metadata serialization failed: {0}")]
    Serialization(String),
    /// Metadata exceeded its fixed bound.
    #[error("session metadata exceeds size limit")]
    MetadataTooLarge,
    /// Metadata schema is unsupported.
    #[error("session metadata schema is unsupported")]
    UnsupportedSchema,
    /// Branch ancestry was absent or invalid.
    #[error("session branch ancestry is invalid")]
    InvalidBranchAncestry,
    /// Branch journal events were empty or non-monotonic.
    #[error("session branch event set is invalid")]
    InvalidBranchEvents,
    /// Branch artifact identity, hash, media type, or size was invalid.
    #[error("session branch artifact is invalid")]
    InvalidBranchArtifact,
    /// Branch event count could not be represented.
    #[error("session branch sequence overflow")]
    SequenceOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(workspace: &Path) -> DependencyPreparedSession {
        DependencyPreparedSession {
            session_id: SessionId::from_uuid(Uuid::from_u128(1)),
            event_id: EventId::from_uuid(Uuid::from_u128(2)),
            correlation_id: CorrelationId::from_uuid(Uuid::from_u128(3)),
            causation_id: CausationId::from_uuid(Uuid::from_u128(4)),
            timestamp: TimestampMillis::new(100),
            normalized_workspace: workspace.to_owned(),
        }
    }

    #[test]
    fn creates_required_tree_and_lists_without_loading_history() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let adapter = FileSessionCatalogDependency;
        let created = adapter
            .create_session(DependencyCreateSessionRequest {
                sessions_root: root.path().join("sessions"),
                prepared: prepared(workspace.path()),
                style: String::from("persistent-chat"),
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
            })
            .expect("create");
        for required in [
            "metadata.json",
            "events.jsonl",
            "style.json",
            "style.lock",
            "workspace.json",
            "continuations",
            "snapshots",
            "artifacts",
            "process-logs",
            "branches",
        ] {
            assert!(
                created.session_directory.join(required).exists(),
                "{required}"
            );
        }
        let listed = adapter
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: root.path().join("sessions"),
                limit: 10,
            })
            .expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].style, "persistent-chat");
        assert_eq!(listed[0].sequence, 1);
    }

    #[test]
    fn listing_uses_verified_journal_tail_instead_of_stale_metadata_hint() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let adapter = FileSessionCatalogDependency;
        let created = adapter
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: prepared(workspace.path()),
                style: String::from("persistent-chat"),
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
            })
            .expect("create");
        JsonlJournalDependency
            .append(DependencyAppendJournalRequest {
                session_directory: created.session_directory,
                sequence: 2,
                event_id: Uuid::from_u128(9).to_string(),
                event_json: br#"{"fixture":"continued"}"#.to_vec(),
                durability: DependencyDurability::Data,
            })
            .expect("append");

        let listed = adapter
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: sessions,
                limit: 10,
            })
            .expect("list");

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].sequence, 2);
    }

    #[test]
    fn preparation_canonicalizes_an_existing_directory() {
        let workspace = tempfile::tempdir().expect("workspace");
        let result = FileSessionCatalogDependency
            .prepare_session(DependencyPrepareSessionRequest {
                workspace: workspace.path().to_owned(),
            })
            .expect("prepare");
        assert_eq!(
            result.normalized_workspace,
            workspace.path().canonicalize().expect("canonical")
        );
    }

    #[test]
    fn branch_artifact_is_hash_validated_and_committed_with_atomic_tree() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let bytes = br#"{"schema_version":1,"history":["complete"]}"#.to_vec();
        let artifact_id = Uuid::from_u128(9).to_string();
        let request = DependencyCreateBranchRequest {
            sessions_root: root.path().join("sessions"),
            prepared: prepared(workspace.path()),
            style: String::from("persistent-chat"),
            parent_session_id: Uuid::from_u128(8).to_string(),
            fork_sequence: 7,
            events: vec![
                DependencyBranchEvent {
                    sequence: 1,
                    event_id: Uuid::from_u128(10).to_string(),
                    event_json: br#"{"event":1}"#.to_vec(),
                },
                DependencyBranchEvent {
                    sequence: 2,
                    event_id: Uuid::from_u128(11).to_string(),
                    event_json: br#"{"event":2}"#.to_vec(),
                },
            ],
            artifacts: vec![DependencyBranchArtifact {
                artifact_id: artifact_id.clone(),
                content_hash: blake3::hash(&bytes).to_hex().to_string(),
                mime_type: String::from("application/vnd.agentmod.branch-context+json"),
                creation_event: Uuid::from_u128(13).to_string(),
                bytes: bytes.clone(),
            }],
        };
        let created = FileSessionCatalogDependency
            .create_branch(request.clone())
            .expect("branch");
        assert_eq!(
            fs::read(
                created
                    .session_directory
                    .join("artifacts")
                    .join(format!("{artifact_id}.json"))
            )
            .expect("artifact"),
            bytes
        );
        let metadata: serde_json::Value = serde_json::from_slice(
            &fs::read(
                created
                    .session_directory
                    .join("artifacts")
                    .join(format!("{artifact_id}.metadata.json")),
            )
            .expect("metadata"),
        )
        .expect("metadata json");
        assert_eq!(metadata["security"], "private");
        assert_eq!(metadata["retention"], "session");

        let mut invalid = request;
        invalid.prepared.session_id = SessionId::from_uuid(Uuid::from_u128(12));
        invalid.artifacts[0].content_hash = String::from("00");
        assert_eq!(
            FileSessionCatalogDependency.create_branch(invalid),
            Err(SessionCatalogDependencyError::InvalidBranchArtifact)
        );
    }
}
