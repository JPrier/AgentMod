//! Atomic filesystem session-directory and metadata adapter.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{
    CausationId, ContentHash, CorrelationId, EventId, SessionId, TimestampMillis,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use fs2::FileExt;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::journal::{
    DependencyAppendJournalRequest, DependencyDurability, DependencyScanJournalRequest,
    JournalDependencyPort, JsonlJournalDependency,
};

const METADATA_LIMIT_BYTES: usize = 2 * 1024 * 1024;
const METADATA_LIMIT_U64: u64 = 2 * 1024 * 1024;
const BRANCH_ARTIFACT_LIMIT: usize = 16 * 1024 * 1024;
const SCHEMA_VERSION: u32 = 2;
const CHILD_MESSAGE_EVENT_LIMIT: usize = 256 * 1024;
const CHILD_MESSAGE_FIELD_LIMIT: usize = 512;
const CHILD_MESSAGE_ARTIFACT_LIMIT: usize = 64;
const CHILD_MESSAGE_LOCK_FILE: &str = "child-message.append.lock";
const MCP_KEY_FILE: &str = ".session-mcp.key";
const MCP_BOOTSTRAP_FILE: &str = "mcp-bootstrap.enc.json";
const MCP_KEY_BYTES: usize = 32;
const MCP_NONCE_BYTES: usize = 24;
const MCP_BOOTSTRAP_LIMIT: usize = 512 * 1024;

/// Exact immutable parent link that authorizes writes to a worker journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyChildParentLink {
    /// Runtime-managed parent session.
    pub parent_session_id: String,
    /// Parent action that created the worker.
    pub parent_action_sequence: u64,
    /// Parent graph node that owns the worker.
    pub parent_graph_node_id: String,
    /// Runtime-owned child task identity.
    pub task_id: String,
}

/// Exact child journal tail expected before a message is appended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyChildJournalHead {
    /// Last committed child-journal sequence.
    pub sequence: u64,
    /// Checksum of that exact journal record.
    pub checksum: String,
}

/// Atomic append-or-replay request for one canonical child-message receipt.
///
/// `message_id` is intentionally the canonical event ID. This binds duplicate
/// detection to the existing child journal instead of introducing a second
/// receipt registry. `message_sequence` is the canonical child-journal sequence
/// assigned to the receipt and must immediately follow `expected_head`.
///
/// The immutable child journal link is the storage-layer authorization proof:
/// this dependency verifies its exact parent session, creation action, graph
/// node, and task. Current graph-policy authorization remains a runtime-logic
/// responsibility and is deliberately not inferred here from catalog metadata.
/// Missing catalog state or an unverifiable immutable link fails closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAppendChildMessageRequest {
    /// Root containing canonical session directories.
    pub sessions_root: PathBuf,
    /// Exact immutable parent/child ownership link.
    pub parent_link: DependencyChildParentLink,
    /// Target worker session.
    pub child_session_id: String,
    /// Stable message identity and canonical event ID.
    pub message_id: String,
    /// Canonical sequence assigned to this message receipt.
    pub message_sequence: u64,
    /// Exact child tail observed before dispatch.
    pub expected_head: DependencyChildJournalHead,
    /// Sealed canonical event envelope bytes.
    pub canonical_event_json: Vec<u8>,
    /// Expected canonical envelope checksum.
    pub canonical_event_checksum: String,
    /// Hash of the bounded typed payload projection.
    pub payload_hash: String,
    /// Hash of the ordered canonical artifact-reference projection.
    pub artifact_references_hash: String,
}

/// Durable receipt returned for a fresh append or exact duplicate replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyChildMessageReceipt {
    /// Whether an existing exact receipt was returned instead of appending.
    pub replayed: bool,
    /// Canonical child-journal sequence containing the receipt.
    pub sequence: u64,
    /// Canonical event/message identity.
    pub message_id: String,
    /// Canonical envelope checksum supplied by the runtime data boundary.
    pub canonical_event_checksum: String,
    /// Complete journal-frame checksum.
    pub journal_checksum: String,
    /// Previous frame checksum, when the receipt is not the first record.
    pub previous_journal_checksum: Option<String>,
    /// Byte offset at which the receipt frame begins.
    pub offset: u64,
    /// Verified child-journal byte length after the receipt.
    pub journal_bytes: u64,
}

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
    /// Canonical immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Canonical initial event JSON.
    pub initial_event_json: Vec<u8>,
    /// Exact transient MCP configuration; diagnostics are always redacted.
    pub mcp_configuration: Option<DependencySensitiveMcpConfiguration>,
}

/// Dependency-owned sensitive MCP configuration wrapper.
#[derive(Clone, Eq, PartialEq)]
pub struct DependencySensitiveMcpConfiguration(pub String);

impl From<String> for DependencySensitiveMcpConfiguration {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl fmt::Debug for DependencySensitiveMcpConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DependencySensitiveMcpConfiguration(<redacted>)")
    }
}

#[derive(Deserialize, Serialize)]
struct EncryptedSessionMcpBootstrap {
    schema_version: u16,
    session_id: String,
    declaration_hash: String,
    binding_hash: String,
    nonce_base64: String,
    ciphertext_base64: String,
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
    /// Canonical immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Immutable parent session identifier.
    pub parent_session_id: String,
    /// Inclusive parent sequence used to construct the child.
    pub fork_sequence: u64,
    /// Exact MCP bootstrap handling selected by runtime logic.
    pub mcp_bootstrap: DependencyBranchMcpBootstrap,
    /// Complete child journal, starting at sequence one.
    pub events: Vec<DependencyBranchEvent>,
    /// Immutable artifacts committed with the child before its atomic rename.
    pub artifacts: Vec<DependencyBranchArtifact>,
}

/// Dependency-owned MCP bootstrap disposition for an atomic branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyBranchMcpBootstrap {
    /// The target immutable binding must not name or contain an MCP bootstrap.
    None,
    /// Authenticate the exact source and re-encrypt it for the target session.
    InheritExact {
        /// Source session whose immutable binding and bootstrap must agree.
        source_session_id: String,
    },
}

/// Atomic runtime-managed worker-session creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCreateChildSessionRequest {
    /// Sessions storage root.
    pub sessions_root: PathBuf,
    /// Prepared child identity and normalized workspace.
    pub prepared: DependencyPreparedSession,
    /// Explicit validated child style.
    pub style: String,
    /// Canonical immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Runtime-managed parent session.
    pub parent_session_id: String,
    /// Canonical parent proposal sequence.
    pub parent_action_sequence: u64,
    /// Parent graph node that owns the child.
    pub parent_graph_node_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Exact MCP bootstrap handling selected by runtime logic.
    pub mcp_bootstrap: DependencyBranchMcpBootstrap,
    /// Complete child journal, starting at sequence one.
    pub events: Vec<DependencyBranchEvent>,
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
    /// Parent session for a runtime-managed worker.
    pub child_parent_session_id: Option<String>,
    /// Parent proposal sequence used to reconcile this worker.
    pub child_parent_action_sequence: Option<u64>,
    /// Runtime-owned task identity.
    pub child_task_id: Option<String>,
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

    /// Atomically creates a runtime-managed worker session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCatalogDependencyError`] when the request is invalid
    /// or the atomic filesystem creation cannot complete.
    fn create_child_session(
        &self,
        request: DependencyCreateChildSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError>;

    /// Atomically appends, or exactly replays, a canonical child-message receipt
    /// in the worker's existing journal.
    ///
    /// # Errors
    ///
    /// Returns [`ChildMessageDependencyError`] when ownership, the expected
    /// journal head, frame bounds, lifecycle preconditions, or duplicate
    /// identity are invalid.
    fn append_child_message(
        &self,
        request: DependencyAppendChildMessageRequest,
    ) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError>;

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

    fn create_child_session(
        &self,
        request: DependencyCreateChildSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        validate_root(&request.sessions_root)?;
        validate_child_session(&request)?;
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
        if let Err(error) = populate_child_directory(&temporary, &request) {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &final_directory).map_err(map_io)?;
        sync_directory(&request.sessions_root)?;
        Ok(DependencyCreatedSession {
            session_directory: final_directory,
        })
    }

    fn append_child_message(
        &self,
        request: DependencyAppendChildMessageRequest,
    ) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
        append_child_message_file(&request)
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

    fn create_child_session(
        &self,
        request: DependencyCreateChildSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        FileSessionCatalogDependency.create_child_session(request)
    }

    fn append_child_message(
        &self,
        request: DependencyAppendChildMessageRequest,
    ) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
        FileSessionCatalogDependency.append_child_message(request)
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
    let descriptors = parse_style_descriptors(
        &request.style,
        &request.style_binding_json,
        &request.style_manifest_json,
        &request.compiled_style_json,
    )?;
    let workspace = request.prepared.normalized_workspace.to_string_lossy();
    let metadata = StoredMetadata {
        schema_version: SCHEMA_VERSION,
        session_id: request.prepared.session_id.to_string(),
        workspace: workspace.as_ref(),
        style: &request.style,
        style_binding: Some(&descriptors.binding),
        sequence: 1,
        state: "active",
        created_at_millis: request.prepared.timestamp.get(),
        parent_session_id: None,
        fork_sequence: None,
        child_parent_session_id: None,
        child_parent_action_sequence: None,
        child_task_id: None,
    };
    write_session_descriptors(temporary, &metadata, workspace.as_ref(), &descriptors)?;
    write_session_mcp_bootstrap(temporary, request, &descriptors.binding)?;
    JsonlJournalDependency
        .append(DependencyAppendJournalRequest {
            session_directory: temporary.to_owned(),
            sequence: 1,
            expected_head_event_id: None,
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
    style: &ParsedStyleDescriptors,
) -> Result<(), SessionCatalogDependencyError> {
    atomic_json(temporary.join("metadata.json"), &metadata)?;
    atomic_json(
        temporary.join("workspace.json"),
        &serde_json::json!({"schema_version": SCHEMA_VERSION, "path": workspace}),
    )?;
    atomic_json(
        temporary.join("style.json"),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "binding": style.binding,
            "manifest": style.manifest
        }),
    )?;
    atomic_json(
        temporary.join("style.lock"),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "binding": style.binding,
            "compiled": style.compiled
        }),
    )?;
    Ok(())
}

fn write_session_mcp_bootstrap(
    temporary: &Path,
    request: &DependencyCreateSessionRequest,
    binding: &serde_json::Value,
) -> Result<(), SessionCatalogDependencyError> {
    let Some(mcp) = binding.get("mcp") else {
        return if request.mcp_configuration.is_none() {
            Ok(())
        } else {
            Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
        };
    };
    let configuration_reference = mcp
        .get("configuration_reference")
        .and_then(serde_json::Value::as_str);
    let Some(configuration) = request.mcp_configuration.as_ref() else {
        if configuration_reference.is_some() {
            return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
        }
        return Ok(());
    };
    if configuration_reference.is_none() || configuration.0.len() > MCP_BOOTSTRAP_LIMIT {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let declaration_hash = mcp
        .get("declaration_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let plaintext: serde_json::Value = serde_json::from_str(&configuration.0)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if plaintext
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        != Some(request.prepared.session_id.to_string().as_str())
        || plaintext
            .get("declaration_hash")
            .and_then(serde_json::Value::as_str)
            != Some(declaration_hash)
        || plaintext
            .get("servers")
            .and_then(serde_json::Value::as_array)
            .is_none_or(Vec::is_empty)
    {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let binding_bytes = serde_json::to_vec(mcp)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let binding_hash = ContentHash::digest(&binding_bytes).to_hex();
    let aad = mcp_bootstrap_aad(
        &request.prepared.session_id.to_string(),
        declaration_hash,
        &binding_hash,
    )?;
    let key = load_or_create_mcp_key(&request.sessions_root)?;
    let cipher = XChaCha20Poly1305::new((&key).into());
    let mut nonce = [0_u8; MCP_NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: configuration.0.as_bytes(),
                aad: &aad,
            },
        )
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let encrypted = EncryptedSessionMcpBootstrap {
        schema_version: 1,
        session_id: request.prepared.session_id.to_string(),
        declaration_hash: declaration_hash.to_owned(),
        binding_hash,
        nonce_base64: BASE64.encode(nonce),
        ciphertext_base64: BASE64.encode(ciphertext),
    };
    let bytes = serde_json::to_vec(&encrypted)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    write_private_file(&temporary.join(MCP_BOOTSTRAP_FILE), &bytes)
}

fn write_branch_mcp_bootstrap(
    temporary: &Path,
    request: &DependencyCreateBranchRequest,
    binding: &serde_json::Value,
) -> Result<(), SessionCatalogDependencyError> {
    let target_mcp = binding.get("mcp");
    match &request.mcp_bootstrap {
        DependencyBranchMcpBootstrap::None => {
            ensure_no_session_mcp_bootstrap(temporary, target_mcp)
        }
        DependencyBranchMcpBootstrap::InheritExact { source_session_id } => {
            if source_session_id != &request.parent_session_id
                || SessionId::from_str(source_session_id).is_err()
            {
                return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
            }
            let target_session_id = request.prepared.session_id.to_string();
            inherit_session_mcp_bootstrap(&InheritSessionMcpBootstrapRequest {
                sessions_root: &request.sessions_root,
                temporary,
                source_session_id,
                target_session_id: &target_session_id,
                target_mcp,
            })
        }
    }
}

fn write_child_mcp_bootstrap(
    temporary: &Path,
    request: &DependencyCreateChildSessionRequest,
    binding: &serde_json::Value,
) -> Result<(), SessionCatalogDependencyError> {
    let target_mcp = binding.get("mcp");
    match &request.mcp_bootstrap {
        DependencyBranchMcpBootstrap::None => {
            ensure_no_session_mcp_bootstrap(temporary, target_mcp)
        }
        DependencyBranchMcpBootstrap::InheritExact { source_session_id } => {
            if source_session_id != &request.parent_session_id
                || SessionId::from_str(source_session_id).is_err()
            {
                return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
            }
            let target_session_id = request.prepared.session_id.to_string();
            inherit_session_mcp_bootstrap(&InheritSessionMcpBootstrapRequest {
                sessions_root: &request.sessions_root,
                temporary,
                source_session_id,
                target_session_id: &target_session_id,
                target_mcp,
            })
        }
    }
}

fn ensure_no_session_mcp_bootstrap(
    temporary: &Path,
    target_mcp: Option<&serde_json::Value>,
) -> Result<(), SessionCatalogDependencyError> {
    if target_mcp.is_some_and(|mcp| {
        mcp.get("configuration_reference")
            .and_then(serde_json::Value::as_str)
            .is_some()
            || mcp
                .get("servers")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|servers| !servers.is_empty())
    }) || temporary.join(MCP_BOOTSTRAP_FILE).exists()
    {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    Ok(())
}

struct InheritSessionMcpBootstrapRequest<'a> {
    sessions_root: &'a Path,
    temporary: &'a Path,
    source_session_id: &'a str,
    target_session_id: &'a str,
    target_mcp: Option<&'a serde_json::Value>,
}

#[allow(
    clippy::too_many_lines,
    reason = "exact source authentication and target re-encryption remain one fail-closed atomic operation"
)]
fn inherit_session_mcp_bootstrap(
    request: &InheritSessionMcpBootstrapRequest<'_>,
) -> Result<(), SessionCatalogDependencyError> {
    if SessionId::from_str(request.source_session_id).is_err()
        || SessionId::from_str(request.target_session_id).is_err()
        || request.source_session_id == request.target_session_id
    {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let target_mcp = request
        .target_mcp
        .filter(|mcp| {
            mcp.get("configuration_reference")
                .and_then(serde_json::Value::as_str)
                .is_some()
        })
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let source_style_lock = fs::read(
        request
            .sessions_root
            .join(request.source_session_id)
            .join("style.lock"),
    )
    .map_err(map_io)?;
    if source_style_lock.len() > METADATA_LIMIT_BYTES {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let source_style_lock: serde_json::Value = serde_json::from_slice(&source_style_lock)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let source_mcp = source_style_lock
        .get("binding")
        .and_then(|binding| binding.get("mcp"))
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if source_mcp != target_mcp {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let mut plaintext =
        load_session_mcp_bootstrap_document(request.sessions_root, request.source_session_id)?
            .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    plaintext
        .as_object_mut()
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?
        .insert(
            String::from("session_id"),
            serde_json::Value::String(request.target_session_id.to_owned()),
        );
    let plaintext = serde_json::to_vec(&plaintext)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if plaintext.len() > MCP_BOOTSTRAP_LIMIT {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let declaration_hash = target_mcp
        .get("declaration_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let binding_hash = ContentHash::digest(
        &serde_json::to_vec(target_mcp)
            .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?,
    )
    .to_hex();
    let aad = mcp_bootstrap_aad(request.target_session_id, declaration_hash, &binding_hash)?;
    let key = load_existing_mcp_key(request.sessions_root)?;
    let mut nonce = [0_u8; MCP_NONCE_BYTES];
    rand::rng().fill_bytes(&mut nonce);
    let ciphertext = XChaCha20Poly1305::new((&key).into())
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let encrypted = EncryptedSessionMcpBootstrap {
        schema_version: 1,
        session_id: request.target_session_id.to_owned(),
        declaration_hash: declaration_hash.to_owned(),
        binding_hash,
        nonce_base64: BASE64.encode(nonce),
        ciphertext_base64: BASE64.encode(ciphertext),
    };
    let bytes = serde_json::to_vec(&encrypted)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    write_private_file(&request.temporary.join(MCP_BOOTSTRAP_FILE), &bytes)
}

fn mcp_bootstrap_aad(
    session_id: &str,
    declaration_hash: &str,
    binding_hash: &str,
) -> Result<Vec<u8>, SessionCatalogDependencyError> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "session_id": session_id,
        "declaration_hash": declaration_hash,
        "binding_hash": binding_hash,
    }))
    .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)
}

fn load_or_create_mcp_key(
    sessions_root: &Path,
) -> Result<[u8; MCP_KEY_BYTES], SessionCatalogDependencyError> {
    let path = sessions_root.join(MCP_KEY_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(map_io)?;
    file.lock_exclusive().map_err(map_io)?;
    file.seek(SeekFrom::Start(0)).map_err(map_io)?;
    let mut stored = Vec::new();
    file.read_to_end(&mut stored).map_err(map_io)?;
    let result = if stored.is_empty() {
        let mut key = [0_u8; MCP_KEY_BYTES];
        rand::rng().fill_bytes(&mut key);
        file.seek(SeekFrom::Start(0)).map_err(map_io)?;
        file.write_all(&key).map_err(map_io)?;
        file.sync_all().map_err(map_io)?;
        set_private_permissions(&path)?;
        Ok(key)
    } else if stored.len() == MCP_KEY_BYTES {
        let mut key = [0_u8; MCP_KEY_BYTES];
        key.copy_from_slice(&stored);
        Ok(key)
    } else {
        Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
    };
    FileExt::unlock(&file).map_err(map_io)?;
    result
}

/// Loads and authenticates the exact MCP bootstrap bound into one session.
///
/// The returned JSON is the host's bounded server array. Missing, substituted,
/// or tampered configuration fails closed whenever the immutable binding names
/// an encrypted configuration reference.
///
/// # Errors
///
/// Returns a classified catalog error when the binding, key, encrypted payload,
/// or authenticated metadata is missing, malformed, substituted, or unavailable.
pub fn load_session_mcp_bootstrap(
    sessions_root: &Path,
    session_id: &str,
) -> Result<Option<String>, SessionCatalogDependencyError> {
    let Some(plaintext) = load_session_mcp_bootstrap_document(sessions_root, session_id)? else {
        return Ok(None);
    };
    let servers = plaintext
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .filter(|servers| !servers.is_empty())
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    serde_json::to_string(servers)
        .map(Some)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)
}

fn load_session_mcp_bootstrap_document(
    sessions_root: &Path,
    session_id: &str,
) -> Result<Option<serde_json::Value>, SessionCatalogDependencyError> {
    let session_directory = sessions_root.join(session_id);
    let style_lock = fs::read(session_directory.join("style.lock")).map_err(map_io)?;
    if style_lock.len() > METADATA_LIMIT_BYTES {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let style_lock: serde_json::Value = serde_json::from_slice(&style_lock)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let mcp = style_lock
        .get("binding")
        .and_then(|binding| binding.get("mcp"));
    let encrypted_path = session_directory.join(MCP_BOOTSTRAP_FILE);
    let Some(mcp) = mcp else {
        return if encrypted_path.exists() {
            Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
        } else {
            Ok(None)
        };
    };
    let configuration_reference = mcp
        .get("configuration_reference")
        .and_then(serde_json::Value::as_str);
    if configuration_reference.is_none() {
        if encrypted_path.exists() {
            return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
        }
        return Ok(None);
    }
    let declaration_hash = mcp
        .get("declaration_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let binding_hash = ContentHash::digest(
        &serde_json::to_vec(mcp)
            .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?,
    )
    .to_hex();
    let encrypted_bytes = fs::read(encrypted_path).map_err(map_io)?;
    if encrypted_bytes.len() > MCP_BOOTSTRAP_LIMIT {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let encrypted: EncryptedSessionMcpBootstrap = serde_json::from_slice(&encrypted_bytes)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if encrypted.schema_version != 1
        || encrypted.session_id != session_id
        || encrypted.declaration_hash != declaration_hash
        || encrypted.binding_hash != binding_hash
        || encrypted.nonce_base64.len() > MCP_NONCE_BYTES * 2
        || encrypted.ciphertext_base64.len() > MCP_BOOTSTRAP_LIMIT * 2
    {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let nonce = BASE64
        .decode(encrypted.nonce_base64)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if nonce.len() != MCP_NONCE_BYTES {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let ciphertext = BASE64
        .decode(encrypted.ciphertext_base64)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    let key = load_existing_mcp_key(sessions_root)?;
    let aad = mcp_bootstrap_aad(session_id, declaration_hash, &binding_hash)?;
    let plaintext = XChaCha20Poly1305::new((&key).into())
        .decrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if plaintext.len() > MCP_BOOTSTRAP_LIMIT {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let plaintext: serde_json::Value = serde_json::from_slice(&plaintext)
        .map_err(|_| SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    if plaintext
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        != Some(session_id)
        || plaintext
            .get("declaration_hash")
            .and_then(serde_json::Value::as_str)
            != Some(declaration_hash)
    {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    plaintext
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .filter(|servers| !servers.is_empty())
        .ok_or(SessionCatalogDependencyError::InvalidMcpConfiguration)?;
    Ok(Some(plaintext))
}

fn load_existing_mcp_key(
    sessions_root: &Path,
) -> Result<[u8; MCP_KEY_BYTES], SessionCatalogDependencyError> {
    let stored = fs::read(sessions_root.join(MCP_KEY_FILE)).map_err(map_io)?;
    if stored.len() != MCP_KEY_BYTES {
        return Err(SessionCatalogDependencyError::InvalidMcpConfiguration);
    }
    let mut key = [0_u8; MCP_KEY_BYTES];
    key.copy_from_slice(&stored);
    Ok(key)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), SessionCatalogDependencyError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(map_io)?;
    file.write_all(bytes).map_err(map_io)?;
    file.sync_all().map_err(map_io)?;
    set_private_permissions(path)
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<(), SessionCatalogDependencyError> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(map_io)
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
fn set_private_permissions(_path: &Path) -> Result<(), SessionCatalogDependencyError> {
    Ok(())
}

fn populate_branch_directory(
    temporary: &Path,
    request: &DependencyCreateBranchRequest,
) -> Result<(), SessionCatalogDependencyError> {
    create_session_subdirectories(temporary)?;
    let descriptors = parse_style_descriptors(
        &request.style,
        &request.style_binding_json,
        &request.style_manifest_json,
        &request.compiled_style_json,
    )?;
    let workspace = request.prepared.normalized_workspace.to_string_lossy();
    let metadata = StoredMetadata {
        schema_version: SCHEMA_VERSION,
        session_id: request.prepared.session_id.to_string(),
        workspace: workspace.as_ref(),
        style: &request.style,
        style_binding: Some(&descriptors.binding),
        sequence: u64::try_from(request.events.len())
            .map_err(|_| SessionCatalogDependencyError::SequenceOverflow)?,
        state: "active",
        created_at_millis: request.prepared.timestamp.get(),
        parent_session_id: Some(&request.parent_session_id),
        fork_sequence: Some(request.fork_sequence),
        child_parent_session_id: None,
        child_parent_action_sequence: None,
        child_task_id: None,
    };
    write_session_descriptors(temporary, &metadata, workspace.as_ref(), &descriptors)?;
    write_branch_mcp_bootstrap(temporary, request, &descriptors.binding)?;
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
                expected_head_event_id: None,
                event_id: event.event_id.clone(),
                event_json: event.event_json.clone(),
                durability: DependencyDurability::Full,
            })
            .map_err(|error| SessionCatalogDependencyError::Journal(error.to_string()))?;
    }
    sync_directory(temporary)
}

fn populate_child_directory(
    temporary: &Path,
    request: &DependencyCreateChildSessionRequest,
) -> Result<(), SessionCatalogDependencyError> {
    create_session_subdirectories(temporary)?;
    let descriptors = parse_style_descriptors(
        &request.style,
        &request.style_binding_json,
        &request.style_manifest_json,
        &request.compiled_style_json,
    )?;
    let workspace = request.prepared.normalized_workspace.to_string_lossy();
    let metadata = StoredMetadata {
        schema_version: SCHEMA_VERSION,
        session_id: request.prepared.session_id.to_string(),
        workspace: workspace.as_ref(),
        style: &request.style,
        style_binding: Some(&descriptors.binding),
        sequence: u64::try_from(request.events.len())
            .map_err(|_| SessionCatalogDependencyError::SequenceOverflow)?,
        state: "active",
        created_at_millis: request.prepared.timestamp.get(),
        parent_session_id: None,
        fork_sequence: None,
        child_parent_session_id: Some(&request.parent_session_id),
        child_parent_action_sequence: Some(request.parent_action_sequence),
        child_task_id: Some(&request.task_id),
    };
    write_session_descriptors(temporary, &metadata, workspace.as_ref(), &descriptors)?;
    write_child_mcp_bootstrap(temporary, request, &descriptors.binding)?;
    for event in &request.events {
        JsonlJournalDependency
            .append(DependencyAppendJournalRequest {
                session_directory: temporary.to_owned(),
                sequence: event.sequence,
                expected_head_event_id: None,
                event_id: event.event_id.clone(),
                event_json: event.event_json.clone(),
                durability: DependencyDurability::Full,
            })
            .map_err(|error| SessionCatalogDependencyError::Journal(error.to_string()))?;
    }
    sync_directory(temporary)
}

fn validate_child_session(
    request: &DependencyCreateChildSessionRequest,
) -> Result<(), SessionCatalogDependencyError> {
    if request.parent_session_id.is_empty()
        || request.parent_action_sequence == 0
        || request.parent_graph_node_id.trim().is_empty()
        || request.task_id.trim().is_empty()
        || request.events.len() != 3
    {
        return Err(SessionCatalogDependencyError::InvalidChildSession);
    }
    for (index, event) in request.events.iter().enumerate() {
        let expected = u64::try_from(index)
            .map_err(|_| SessionCatalogDependencyError::SequenceOverflow)?
            .checked_add(1)
            .ok_or(SessionCatalogDependencyError::SequenceOverflow)?;
        if event.sequence != expected || event.event_id.is_empty() || event.event_json.is_empty() {
            return Err(SessionCatalogDependencyError::InvalidChildSession);
        }
    }
    Ok(())
}

/// Appends a child-message receipt under a narrow per-child operation lock.
///
/// The actual record remains an ordinary checksummed `events.jsonl` frame. The
/// operation lock only serializes append-or-replay requests for the same child;
/// the journal's own exclusive lock remains the final sequence/checksum gate for
/// all runtime event writers.
fn append_child_message_file(
    request: &DependencyAppendChildMessageRequest,
) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
    append_child_message_file_with_locked_hook(request, |_| Ok(()))
}

fn append_child_message_file_with_locked_hook(
    request: &DependencyAppendChildMessageRequest,
    locked_hook: impl FnOnce(&Path) -> Result<(), ChildMessageDependencyError>,
) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
    validate_child_message_request(request)?;
    let child_directory = request.sessions_root.join(&request.child_session_id);
    let parent_directory = request
        .sessions_root
        .join(&request.parent_link.parent_session_id);

    let lock_path = child_directory.join(CHILD_MESSAGE_LOCK_FILE);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|error| child_message_io(&error))?;
    lock.lock_exclusive()
        .map_err(|error| child_message_io(&error))?;
    let result = locked_hook(&child_directory)
        .and_then(|()| append_child_message_locked(request, &parent_directory, &child_directory));
    let unlock_result = FileExt::unlock(&lock).map_err(|error| child_message_io(&error));
    match (result, unlock_result) {
        (Ok(receipt), Ok(())) => Ok(receipt),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

fn append_child_message_locked(
    request: &DependencyAppendChildMessageRequest,
    parent_directory: &Path,
    child_directory: &Path,
) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
    validate_message_session_directory(
        parent_directory,
        &request.parent_link.parent_session_id,
        None,
        false,
    )?;
    validate_message_session_directory(
        child_directory,
        &request.child_session_id,
        Some(&request.parent_link),
        false,
    )?;
    let scan = JsonlJournalDependency
        .scan(DependencyScanJournalRequest {
            session_directory: child_directory.to_owned(),
        })
        .map_err(map_child_message_journal)?;
    validate_child_parent_link_event(&scan.records, &request.parent_link)?;

    if let Some(existing) = scan
        .records
        .iter()
        .find(|record| record.event_id == request.message_id)
    {
        let receipt = existing_child_message_receipt(existing, request, scan.valid_bytes)?;
        update_child_metadata_sequence(child_directory, receipt.sequence)?;
        return Ok(receipt);
    }

    validate_message_session_directory(
        parent_directory,
        &request.parent_link.parent_session_id,
        None,
        true,
    )?;
    validate_message_session_directory(
        child_directory,
        &request.child_session_id,
        Some(&request.parent_link),
        true,
    )?;
    let tail = scan
        .records
        .last()
        .ok_or(ChildMessageDependencyError::MissingChildHead)?;
    if tail.sequence != request.expected_head.sequence
        || tail.checksum != request.expected_head.checksum
    {
        return Err(ChildMessageDependencyError::StaleChildHead {
            expected_sequence: request.expected_head.sequence,
            actual_sequence: tail.sequence,
        });
    }
    let required_sequence = tail
        .sequence
        .checked_add(1)
        .ok_or(ChildMessageDependencyError::SequenceOverflow)?;
    if request.message_sequence != required_sequence {
        return Err(ChildMessageDependencyError::MessageSequenceMismatch {
            expected: required_sequence,
            actual: request.message_sequence,
        });
    }

    // Re-read both catalog records under the child operation lock immediately
    // before the journal CAS. A concurrent canonical journal writer is still
    // rejected by `JsonlJournalDependency::append`; this second catalog check
    // closes the lifecycle/ownership window that previously existed before the
    // operation lock was acquired.
    validate_message_session_directory(
        parent_directory,
        &request.parent_link.parent_session_id,
        None,
        true,
    )?;
    validate_message_session_directory(
        child_directory,
        &request.child_session_id,
        Some(&request.parent_link),
        true,
    )?;
    let appended = JsonlJournalDependency
        .append(DependencyAppendJournalRequest {
            session_directory: child_directory.to_owned(),
            sequence: request.message_sequence,
            expected_head_event_id: None,
            event_id: request.message_id.clone(),
            event_json: request.canonical_event_json.clone(),
            durability: DependencyDurability::Full,
        })
        .map_err(map_child_message_journal)?;
    let receipt = DependencyChildMessageReceipt {
        replayed: false,
        sequence: request.message_sequence,
        message_id: request.message_id.clone(),
        canonical_event_checksum: request.canonical_event_checksum.clone(),
        journal_checksum: appended.checksum,
        previous_journal_checksum: Some(request.expected_head.checksum.clone()),
        offset: appended.offset,
        journal_bytes: appended.journal_bytes,
    };
    update_child_metadata_sequence(child_directory, receipt.sequence)?;
    Ok(receipt)
}

fn validate_child_parent_link_event(
    records: &[crate::journal::DependencyJournalRecord],
    expected: &DependencyChildParentLink,
) -> Result<(), ChildMessageDependencyError> {
    let linked = records
        .iter()
        .find(|record| record.sequence == 2)
        .ok_or(ChildMessageDependencyError::ParentLinkMismatch)?;
    let event: serde_json::Value = serde_json::from_slice(&linked.event_json)
        .map_err(|_| ChildMessageDependencyError::ParentLinkMismatch)?;
    let payload = event
        .get("payload")
        .and_then(serde_json::Value::as_object)
        .ok_or(ChildMessageDependencyError::ParentLinkMismatch)?;
    let body = payload
        .get("payload")
        .and_then(serde_json::Value::as_object)
        .filter(|_| {
            payload.get("event").and_then(serde_json::Value::as_str) == Some("child_session_linked")
        })
        .ok_or(ChildMessageDependencyError::ParentLinkMismatch)?;
    if body
        .get("parent_session_id")
        .and_then(serde_json::Value::as_str)
        != Some(expected.parent_session_id.as_str())
        || body
            .get("parent_action_sequence")
            .and_then(serde_json::Value::as_u64)
            != Some(expected.parent_action_sequence)
        || body
            .get("parent_graph_node_id")
            .and_then(serde_json::Value::as_str)
            != Some(expected.parent_graph_node_id.as_str())
        || body.get("task_id").and_then(serde_json::Value::as_str)
            != Some(expected.task_id.as_str())
    {
        return Err(ChildMessageDependencyError::ParentLinkMismatch);
    }
    Ok(())
}

fn update_child_metadata_sequence(
    child_directory: &Path,
    sequence: u64,
) -> Result<(), ChildMessageDependencyError> {
    let mut metadata = read_metadata(&child_directory.join("metadata.json"))
        .map_err(|_| ChildMessageDependencyError::InvalidSessionMetadata)?;
    if metadata.sequence >= sequence {
        return Ok(());
    }
    metadata.sequence = sequence;
    atomic_json(child_directory.join("metadata.json"), &metadata)
        .map_err(|error| ChildMessageDependencyError::Io(error.to_string()))?;
    sync_directory(child_directory)
        .map_err(|error| ChildMessageDependencyError::Io(error.to_string()))
}

fn existing_child_message_receipt(
    existing: &crate::journal::DependencyJournalRecord,
    request: &DependencyAppendChildMessageRequest,
    journal_bytes: u64,
) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
    let existing_event: serde_json::Value = serde_json::from_slice(&existing.event_json)
        .map_err(|_| ChildMessageDependencyError::ConflictingDuplicate)?;
    let requested_event: serde_json::Value = serde_json::from_slice(&request.canonical_event_json)
        .map_err(|_| ChildMessageDependencyError::InvalidCanonicalEvent)?;
    if existing.sequence != request.message_sequence || existing_event != requested_event {
        return Err(ChildMessageDependencyError::ConflictingDuplicate);
    }
    Ok(DependencyChildMessageReceipt {
        replayed: true,
        sequence: existing.sequence,
        message_id: request.message_id.clone(),
        canonical_event_checksum: request.canonical_event_checksum.clone(),
        journal_checksum: existing.checksum.clone(),
        previous_journal_checksum: existing.previous_checksum.clone(),
        offset: existing.offset,
        journal_bytes,
    })
}

fn validate_child_message_request(
    request: &DependencyAppendChildMessageRequest,
) -> Result<(), ChildMessageDependencyError> {
    if request.sessions_root.as_os_str().is_empty()
        || request.canonical_event_json.is_empty()
        || request.canonical_event_json.len() > CHILD_MESSAGE_EVENT_LIMIT
        || request.message_sequence == 0
        || request.expected_head.sequence == 0
        || request.parent_link.parent_action_sequence == 0
        || request.parent_link.parent_graph_node_id.trim().is_empty()
        || request.parent_link.task_id.trim().is_empty()
        || request.parent_link.parent_graph_node_id.len() > CHILD_MESSAGE_FIELD_LIMIT
        || request.parent_link.task_id.len() > CHILD_MESSAGE_FIELD_LIMIT
        || request.message_id.len() > CHILD_MESSAGE_FIELD_LIMIT
    {
        return Err(ChildMessageDependencyError::InvalidRequest);
    }
    let parent = SessionId::from_str(&request.parent_link.parent_session_id)
        .map_err(|_| ChildMessageDependencyError::InvalidRequest)?;
    let child = SessionId::from_str(&request.child_session_id)
        .map_err(|_| ChildMessageDependencyError::InvalidRequest)?;
    if parent == child
        || EventId::from_str(&request.message_id).is_err()
        || !valid_hash(&request.expected_head.checksum)
        || !valid_hash(&request.canonical_event_checksum)
        || !valid_hash(&request.payload_hash)
        || !valid_hash(&request.artifact_references_hash)
    {
        return Err(ChildMessageDependencyError::InvalidRequest);
    }
    validate_canonical_child_message_event(request)
}

fn validate_canonical_child_message_event(
    request: &DependencyAppendChildMessageRequest,
) -> Result<(), ChildMessageDependencyError> {
    let event: serde_json::Value = serde_json::from_slice(&request.canonical_event_json)
        .map_err(|_| ChildMessageDependencyError::InvalidCanonicalEvent)?;
    let object = event
        .as_object()
        .ok_or(ChildMessageDependencyError::InvalidCanonicalEvent)?;
    let metadata = object
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .ok_or(ChildMessageDependencyError::InvalidCanonicalEvent)?;
    if metadata.get("event_id").and_then(serde_json::Value::as_str) != Some(&request.message_id)
        || metadata.get("sequence").and_then(serde_json::Value::as_u64)
            != Some(request.message_sequence)
        || metadata
            .get("event_type")
            .and_then(serde_json::Value::as_str)
            != Some("child_agent.message_received")
        || metadata
            .get("classification")
            .and_then(serde_json::Value::as_str)
            != Some("committed")
        || object
            .get("integrity_checksum")
            .and_then(serde_json::Value::as_str)
            != Some(&request.canonical_event_checksum)
    {
        return Err(ChildMessageDependencyError::InvalidCanonicalEvent);
    }
    let scope = metadata
        .get("scope")
        .and_then(serde_json::Value::as_object)
        .ok_or(ChildMessageDependencyError::InvalidCanonicalEvent)?;
    if scope.get("kind").and_then(serde_json::Value::as_str) != Some("session")
        || scope.get("id").and_then(serde_json::Value::as_str) != Some(&request.child_session_id)
    {
        return Err(ChildMessageDependencyError::InvalidCanonicalEvent);
    }
    let artifacts = metadata
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or(ChildMessageDependencyError::InvalidCanonicalEvent)?;
    if artifacts.len() > CHILD_MESSAGE_ARTIFACT_LIMIT {
        return Err(ChildMessageDependencyError::InvalidCanonicalEvent);
    }
    let payload = object
        .get("payload")
        .ok_or(ChildMessageDependencyError::InvalidCanonicalEvent)?;
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|_| ChildMessageDependencyError::InvalidCanonicalEvent)?;
    let artifact_bytes = serde_json::to_vec(artifacts)
        .map_err(|_| ChildMessageDependencyError::InvalidCanonicalEvent)?;
    if ContentHash::digest(&payload_bytes).to_string() != request.payload_hash
        || ContentHash::digest(&artifact_bytes).to_string() != request.artifact_references_hash
    {
        return Err(ChildMessageDependencyError::InvalidCanonicalEvent);
    }
    Ok(())
}

fn validate_message_session_directory(
    directory: &Path,
    expected_session_id: &str,
    expected_child_link: Option<&DependencyChildParentLink>,
    require_active: bool,
) -> Result<(), ChildMessageDependencyError> {
    let metadata =
        read_metadata(&directory.join("metadata.json")).map_err(|error| match error {
            SessionCatalogDependencyError::Io(_) => ChildMessageDependencyError::SessionUnavailable,
            _ => ChildMessageDependencyError::InvalidSessionMetadata,
        })?;
    if metadata.session_id != expected_session_id {
        return Err(ChildMessageDependencyError::InvalidSessionMetadata);
    }
    if require_active && metadata.state != "active" {
        return Err(ChildMessageDependencyError::ChildNotWritable);
    }
    if let Some(link) = expected_child_link
        && (metadata.child_parent_session_id.as_deref() != Some(link.parent_session_id.as_str())
            || metadata.child_parent_action_sequence != Some(link.parent_action_sequence)
            || metadata.child_task_id.as_deref() != Some(link.task_id.as_str()))
    {
        return Err(ChildMessageDependencyError::ChildNotWritable);
    }
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    ContentHash::from_str(value).is_ok()
}

fn map_child_message_journal(
    error: crate::journal::JournalDependencyError,
) -> ChildMessageDependencyError {
    match error {
        crate::journal::JournalDependencyError::SequenceMismatch { .. }
        | crate::journal::JournalDependencyError::HeadEventIdMismatch { .. } => {
            ChildMessageDependencyError::ConcurrentJournalAdvance
        }
        crate::journal::JournalDependencyError::SequenceOverflow => {
            ChildMessageDependencyError::SequenceOverflow
        }
        error => ChildMessageDependencyError::Journal(error.to_string()),
    }
}

fn child_message_io(error: &std::io::Error) -> ChildMessageDependencyError {
    ChildMessageDependencyError::Io(error.to_string())
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

struct ParsedStyleDescriptors {
    binding: serde_json::Value,
    manifest: serde_json::Value,
    compiled: serde_json::Value,
}

fn parse_style_descriptors(
    style_id: &str,
    binding_json: &str,
    manifest_json: &str,
    compiled_json: &str,
) -> Result<ParsedStyleDescriptors, SessionCatalogDependencyError> {
    if style_id.is_empty()
        || binding_json.len() > METADATA_LIMIT_BYTES
        || manifest_json.len() > METADATA_LIMIT_BYTES
        || compiled_json.len() > METADATA_LIMIT_BYTES
    {
        return Err(SessionCatalogDependencyError::InvalidStyleBinding);
    }
    let binding: serde_json::Value = serde_json::from_str(binding_json)
        .map_err(|_| SessionCatalogDependencyError::InvalidStyleBinding)?;
    let manifest: serde_json::Value = serde_json::from_str(manifest_json)
        .map_err(|_| SessionCatalogDependencyError::InvalidStyleBinding)?;
    let compiled: serde_json::Value = serde_json::from_str(compiled_json)
        .map_err(|_| SessionCatalogDependencyError::InvalidStyleBinding)?;
    let bound_id = binding
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidStyleBinding)?;
    let manifest_hash = binding
        .get("content_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidStyleBinding)?;
    let compiled_hash = binding
        .get("compiled_style_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or(SessionCatalogDependencyError::InvalidStyleBinding)?;
    if bound_id != style_id
        || ContentHash::digest(manifest_json.as_bytes()).to_string() != manifest_hash
        || ContentHash::digest(compiled_json.as_bytes()).to_string() != compiled_hash
    {
        return Err(SessionCatalogDependencyError::InvalidStyleBinding);
    }
    Ok(ParsedStyleDescriptors {
        binding,
        manifest,
        compiled,
    })
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
    if file.metadata().map_err(map_io)?.len() > METADATA_LIMIT_U64 {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let mut bytes = Vec::new();
    file.take(METADATA_LIMIT_U64 + 1)
        .read_to_end(&mut bytes)
        .map_err(map_io)?;
    if bytes.len() > METADATA_LIMIT_BYTES {
        return Err(SessionCatalogDependencyError::MetadataTooLarge);
    }
    let metadata: StoredMetadataOwned = serde_json::from_slice(&bytes)
        .map_err(|error| SessionCatalogDependencyError::Serialization(error.to_string()))?;
    if !matches!(metadata.schema_version, 1 | SCHEMA_VERSION) {
        return Err(SessionCatalogDependencyError::UnsupportedSchema);
    }
    if metadata.schema_version == SCHEMA_VERSION && metadata.style_binding.is_none() {
        return Err(SessionCatalogDependencyError::InvalidStyleBinding);
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
    #[serde(skip_serializing_if = "Option::is_none")]
    style_binding: Option<&'a serde_json::Value>,
    sequence: u64,
    state: &'a str,
    created_at_millis: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_parent_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_parent_action_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_task_id: Option<&'a str>,
}

#[derive(Deserialize, Serialize)]
struct StoredMetadataOwned {
    schema_version: u32,
    session_id: String,
    workspace: String,
    style: String,
    #[serde(default)]
    style_binding: Option<serde_json::Value>,
    sequence: u64,
    state: String,
    created_at_millis: i64,
    #[serde(default)]
    parent_session_id: Option<String>,
    #[serde(default)]
    fork_sequence: Option<u64>,
    #[serde(default)]
    child_parent_session_id: Option<String>,
    #[serde(default)]
    child_parent_action_sequence: Option<u64>,
    #[serde(default)]
    child_task_id: Option<String>,
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
            child_parent_session_id: value.child_parent_session_id,
            child_parent_action_sequence: value.child_parent_action_sequence,
            child_task_id: value.child_task_id,
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
    /// Per-session MCP declaration or encrypted bootstrap failed validation.
    #[error("session MCP configuration is invalid or unavailable")]
    InvalidMcpConfiguration,
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
    /// Worker ownership or initial journal is invalid.
    #[error("child session creation request is invalid")]
    InvalidChildSession,
    /// Branch artifact identity, hash, media type, or size was invalid.
    #[error("session branch artifact is invalid")]
    InvalidBranchArtifact,
    /// Branch event count could not be represented.
    #[error("session branch sequence overflow")]
    SequenceOverflow,
    /// Style binding, manifest, or compiled descriptor was inconsistent.
    #[error("session style binding is invalid")]
    InvalidStyleBinding,
}

/// Stable failure classification for a child-message append-or-replay.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ChildMessageDependencyError {
    /// Request fields, IDs, hashes, or bounds were invalid.
    #[error("child message request is invalid")]
    InvalidRequest,
    /// The sealed canonical event did not match the child-message receipt contract.
    #[error("child message canonical event is invalid")]
    InvalidCanonicalEvent,
    /// The required parent or child catalog directory is unavailable.
    #[error("child message session is unavailable")]
    SessionUnavailable,
    /// Parent or child metadata did not match the expected immutable identity.
    #[error("child message session metadata is invalid")]
    InvalidSessionMetadata,
    /// The canonical child-session link event disagreed with the supplied immutable parent link.
    #[error("child message parent link does not match the child journal")]
    ParentLinkMismatch,
    /// The parent or worker is no longer active, or the worker is not owned by
    /// the supplied immutable parent link.
    #[error("child message parent or child session is not writable")]
    ChildNotWritable,
    /// No verified child journal head existed.
    #[error("child message child journal has no head")]
    MissingChildHead,
    /// The supplied head no longer identifies the current child tail.
    #[error(
        "child message child journal head is stale (expected sequence {expected_sequence}, actual {actual_sequence})"
    )]
    StaleChildHead {
        /// Expected tail sequence.
        expected_sequence: u64,
        /// Actual verified tail sequence.
        actual_sequence: u64,
    },
    /// The requested receipt sequence did not immediately follow the verified tail.
    #[error("child message sequence mismatch (expected {expected}, actual {actual})")]
    MessageSequenceMismatch {
        /// Required next sequence.
        expected: u64,
        /// Requested message sequence.
        actual: u64,
    },
    /// The same message ID already identifies different canonical bytes or sequence.
    #[error("child message duplicate conflicts with the existing receipt")]
    ConflictingDuplicate,
    /// Another writer advanced the shared canonical journal between validation and append.
    #[error("child message journal advanced concurrently")]
    ConcurrentJournalAdvance,
    /// Sequence arithmetic overflowed.
    #[error("child message sequence overflow")]
    SequenceOverflow,
    /// The existing canonical journal rejected the append.
    #[error("child message journal failed: {0}")]
    Journal(String),
    /// The filesystem operation failed.
    #[error("child message filesystem operation failed: {0}")]
    Io(String),
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

    fn style_documents() -> (String, String, String) {
        let manifest = String::from(r#"{"id":"persistent-chat"}"#);
        let compiled = String::from(r#"{"id":"persistent-chat","entry":"respond"}"#);
        let binding = serde_json::json!({
            "id": "persistent-chat",
            "content_hash": ContentHash::digest(manifest.as_bytes()),
            "compiled_style_hash": ContentHash::digest(compiled.as_bytes())
        })
        .to_string();
        (binding, manifest, compiled)
    }

    fn mcp_style_documents(
        session_id: SessionId,
        secret: &str,
    ) -> (String, String, String, DependencySensitiveMcpConfiguration) {
        let manifest = String::from(r#"{"id":"persistent-chat"}"#);
        let compiled = String::from(r#"{"id":"persistent-chat","entry":"respond"}"#);
        let declaration_hash = ContentHash::digest(secret.as_bytes());
        let binding = serde_json::json!({
            "id": "persistent-chat",
            "content_hash": ContentHash::digest(manifest.as_bytes()),
            "compiled_style_hash": ContentHash::digest(compiled.as_bytes()),
            "mcp": {
                "schema_version": 1,
                "declaration_hash": declaration_hash,
                "configuration_reference": format!(
                    "session-mcp:blake3:{}",
                    declaration_hash.to_hex()
                ),
                "servers": [{
                    "id": "fixture",
                    "display_name": "fixture",
                    "transport": {
                        "transport": "stdio",
                        "program": "/absolute/mcp",
                        "arguments": [],
                        "environment": [{
                            "name": "FIXTURE_TOKEN",
                            "secret_reference": "secret-ref:fixture",
                            "value_hash": ContentHash::digest(secret.as_bytes())
                        }]
                    }
                }]
            }
        })
        .to_string();
        let configuration = serde_json::json!({
            "schema_version": 1,
            "session_id": session_id,
            "declaration_hash": declaration_hash,
            "servers": [{
                "id": "fixture",
                "display_name": "fixture",
                "active": true,
                "transport": "stdio",
                "program": "/absolute/mcp",
                "arguments": [],
                "environment": {"FIXTURE_TOKEN": secret}
            }]
        })
        .to_string()
        .into();
        (binding, manifest, compiled, configuration)
    }

    fn prepared_with(workspace: &Path, seed: u128) -> DependencyPreparedSession {
        DependencyPreparedSession {
            session_id: SessionId::from_uuid(Uuid::from_u128(seed)),
            event_id: EventId::from_uuid(Uuid::from_u128(seed + 1)),
            correlation_id: CorrelationId::from_uuid(Uuid::from_u128(seed + 2)),
            causation_id: CausationId::from_uuid(Uuid::from_u128(seed + 3)),
            timestamp: TimestampMillis::new(100),
            normalized_workspace: workspace.to_owned(),
        }
    }

    fn child_message_fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        PathBuf,
        SessionId,
        SessionId,
    ) {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let parent = prepared_with(workspace.path(), 100);
        let child = prepared_with(workspace.path(), 200);
        let adapter = FileSessionCatalogDependency;
        let (binding, manifest, compiled) = style_documents();
        adapter
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: parent.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                initial_event_json: br#"{"fixture":"parent"}"#.to_vec(),
                mcp_configuration: None,
            })
            .expect("parent");
        let (binding, manifest, compiled) = style_documents();
        let linked = serde_json::json!({
            "metadata": {"sequence": 2_u64},
            "payload": {
                "event": "child_session_linked",
                "payload": {
                    "parent_session_id": parent.session_id,
                    "parent_action_sequence": 17_u64,
                    "parent_graph_node_id": "send-message",
                    "task_id": "task-1"
                }
            },
            "integrity_checksum": ContentHash::digest(b"linked")
        });
        let workspace_lease = serde_json::json!({
            "metadata": {"sequence": 3_u64},
            "payload": {
                "event": "child_session_workspace_lease_bound",
                "payload": {
                    "parent_session_id": parent.session_id,
                    "parent_action_sequence": 17_u64,
                    "parent_graph_node_id": "send-message",
                    "task_id": "task-1",
                    "mode": "shared_read_only"
                }
            },
            "integrity_checksum": ContentHash::digest(b"workspace-lease")
        });
        adapter
            .create_child_session(DependencyCreateChildSessionRequest {
                sessions_root: sessions.clone(),
                prepared: child.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                parent_session_id: parent.session_id.to_string(),
                parent_action_sequence: 17,
                parent_graph_node_id: String::from("send-message"),
                task_id: String::from("task-1"),
                mcp_bootstrap: DependencyBranchMcpBootstrap::None,
                events: vec![
                    DependencyBranchEvent {
                        sequence: 1,
                        event_id: EventId::from_uuid(Uuid::from_u128(210)).to_string(),
                        event_json: br#"{"fixture":"child"}"#.to_vec(),
                    },
                    DependencyBranchEvent {
                        sequence: 2,
                        event_id: EventId::from_uuid(Uuid::from_u128(211)).to_string(),
                        event_json: serde_json::to_vec(&linked).expect("linked json"),
                    },
                    DependencyBranchEvent {
                        sequence: 3,
                        event_id: EventId::from_uuid(Uuid::from_u128(212)).to_string(),
                        event_json: serde_json::to_vec(&workspace_lease)
                            .expect("workspace lease json"),
                    },
                ],
            })
            .expect("child");
        (
            root,
            workspace,
            sessions,
            parent.session_id,
            child.session_id,
        )
    }

    fn child_message_request(
        sessions: PathBuf,
        parent_session_id: SessionId,
        child_session_id: SessionId,
        message_id: EventId,
        body: &str,
    ) -> DependencyAppendChildMessageRequest {
        let scan = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: sessions.join(child_session_id.to_string()),
            })
            .expect("scan child");
        let head = scan.records.last().expect("head");
        let payload = serde_json::json!({"kind":"instruction","body":body});
        let artifacts = serde_json::json!([]);
        let envelope_checksum = ContentHash::digest(b"message-envelope");
        let event = serde_json::json!({
            "metadata": {
                "event_id": message_id,
                "scope": {"kind":"session", "id": child_session_id},
                "sequence": 4_u64,
                "event_type": "child_agent.message_received",
                "classification": "committed",
                "artifacts": artifacts
            },
            "payload": payload,
            "integrity_checksum": envelope_checksum
        });
        let payload_bytes = serde_json::to_vec(&event["payload"]).expect("payload");
        let artifact_bytes =
            serde_json::to_vec(&event["metadata"]["artifacts"]).expect("artifacts");
        DependencyAppendChildMessageRequest {
            sessions_root: sessions,
            parent_link: DependencyChildParentLink {
                parent_session_id: parent_session_id.to_string(),
                parent_action_sequence: 17,
                parent_graph_node_id: String::from("send-message"),
                task_id: String::from("task-1"),
            },
            child_session_id: child_session_id.to_string(),
            message_id: message_id.to_string(),
            message_sequence: 4,
            expected_head: DependencyChildJournalHead {
                sequence: head.sequence,
                checksum: head.checksum.clone(),
            },
            canonical_event_json: serde_json::to_vec(&event).expect("event"),
            canonical_event_checksum: envelope_checksum.to_string(),
            payload_hash: ContentHash::digest(&payload_bytes).to_string(),
            artifact_references_hash: ContentHash::digest(&artifact_bytes).to_string(),
        }
    }

    #[test]
    fn child_message_append_replays_exact_duplicate_and_persists_across_reopen() {
        let (_root, _workspace, sessions, parent, child) = child_message_fixture();
        let request = child_message_request(
            sessions.clone(),
            parent,
            child,
            EventId::from_uuid(Uuid::from_u128(300)),
            "continue",
        );
        let first = FileSessionCatalogDependency
            .append_child_message(request.clone())
            .expect("first append");
        assert!(!first.replayed);
        assert_eq!(first.sequence, 4);

        let child_directory = sessions.join(child.to_string());
        let metadata_path = child_directory.join("metadata.json");
        let mut terminal_metadata = read_metadata(&metadata_path).expect("child metadata");
        terminal_metadata.state = String::from("completed");
        atomic_json(metadata_path, &terminal_metadata).expect("mark child terminal");
        let replay = FileSessionCatalogDependency
            .append_child_message(request)
            .expect("exact replay remains safe after child termination");
        assert!(replay.replayed);
        assert_eq!(replay.journal_checksum, first.journal_checksum);
        assert_eq!(replay.sequence, first.sequence);

        let listed = FileSessionCatalogDependency
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: sessions.clone(),
                limit: 10,
            })
            .expect("list");
        assert_eq!(
            listed
                .iter()
                .find(|record| record.session_id == child.to_string())
                .unwrap()
                .sequence,
            4
        );
        let metadata =
            read_metadata(&child_directory.join("metadata.json")).expect("persisted metadata");
        assert_eq!(metadata.sequence, 4);
        let reopened = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: child_directory,
            })
            .expect("reopen journal");
        assert_eq!(reopened.records.len(), 4);
        assert_eq!(reopened.records[3].event_id, first.message_id);
    }

    #[test]
    fn child_message_rejects_conflicting_duplicate_and_wrong_identity_or_head() {
        let (_root, _workspace, sessions, parent, child) = child_message_fixture();
        let request = child_message_request(
            sessions.clone(),
            parent,
            child,
            EventId::from_uuid(Uuid::from_u128(301)),
            "continue",
        );
        let mut wrong_head = request.clone();
        wrong_head.expected_head.sequence = 1;
        wrong_head.expected_head.checksum = ContentHash::digest(b"wrong-head").to_string();
        assert!(matches!(
            FileSessionCatalogDependency.append_child_message(wrong_head),
            Err(ChildMessageDependencyError::StaleChildHead { .. })
        ));
        FileSessionCatalogDependency
            .append_child_message(request.clone())
            .expect("first append");

        let mut conflict = request.clone();
        let mut event: serde_json::Value =
            serde_json::from_slice(&conflict.canonical_event_json).expect("event");
        event["payload"]["body"] = serde_json::json!("different");
        let payload = serde_json::to_vec(&event["payload"]).expect("payload");
        conflict.payload_hash = ContentHash::digest(&payload).to_string();
        conflict.canonical_event_json = serde_json::to_vec(&event).expect("event");
        assert_eq!(
            FileSessionCatalogDependency.append_child_message(conflict),
            Err(ChildMessageDependencyError::ConflictingDuplicate)
        );

        let mut wrong_parent = request.clone();
        wrong_parent.parent_link.parent_graph_node_id = String::from("wrong-node");
        assert_eq!(
            FileSessionCatalogDependency.append_child_message(wrong_parent),
            Err(ChildMessageDependencyError::ParentLinkMismatch)
        );

        let mut unknown_parent = request.clone();
        unknown_parent.parent_link.parent_session_id =
            SessionId::from_uuid(Uuid::from_u128(302)).to_string();
        assert_eq!(
            FileSessionCatalogDependency.append_child_message(unknown_parent),
            Err(ChildMessageDependencyError::SessionUnavailable)
        );

        let mut wrong_child = request;
        wrong_child.child_session_id = parent.to_string();
        assert_eq!(
            FileSessionCatalogDependency.append_child_message(wrong_child),
            Err(ChildMessageDependencyError::InvalidRequest)
        );
    }

    #[test]
    fn child_message_revalidates_lifecycle_after_acquiring_operation_lock() {
        let (_root, _workspace, sessions, parent, child) = child_message_fixture();
        let request = child_message_request(
            sessions.clone(),
            parent,
            child,
            EventId::from_uuid(Uuid::from_u128(303)),
            "must not be delivered",
        );

        let result = append_child_message_file_with_locked_hook(&request, |child_directory| {
            let metadata_path = child_directory.join("metadata.json");
            let mut metadata = read_metadata(&metadata_path)
                .map_err(|_| ChildMessageDependencyError::InvalidSessionMetadata)?;
            metadata.state = String::from("cancelled");
            atomic_json(metadata_path, &metadata)
                .map_err(|error| ChildMessageDependencyError::Io(error.to_string()))
        });

        assert_eq!(result, Err(ChildMessageDependencyError::ChildNotWritable));
        let reopened = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: sessions.join(child.to_string()),
            })
            .expect("reopen child journal");
        assert_eq!(reopened.records.len(), 3);
        assert!(
            reopened
                .records
                .iter()
                .all(|record| record.event_id != request.message_id)
        );
    }

    #[test]
    fn child_message_retry_repairs_metadata_after_append_update_crash_cut() {
        let (_root, _workspace, sessions, parent, child) = child_message_fixture();
        let request = child_message_request(
            sessions.clone(),
            parent,
            child,
            EventId::from_uuid(Uuid::from_u128(304)),
            "recover exactly once",
        );
        let child_directory = sessions.join(child.to_string());
        let metadata_temporary = child_directory.join("metadata.tmp");
        std::fs::create_dir(&metadata_temporary).expect("block metadata replacement");

        assert!(matches!(
            FileSessionCatalogDependency.append_child_message(request.clone()),
            Err(ChildMessageDependencyError::Io(_))
        ));
        let after_failed_metadata_update = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: child_directory.clone(),
            })
            .expect("message append survived metadata failure");
        assert_eq!(after_failed_metadata_update.records.len(), 4);
        let durable = after_failed_metadata_update
            .records
            .last()
            .expect("receipt");
        assert_eq!(durable.event_id, request.message_id);
        let durable_checksum = durable.checksum.clone();
        assert_eq!(
            read_metadata(&child_directory.join("metadata.json"))
                .expect("stale metadata")
                .sequence,
            3
        );

        std::fs::remove_dir(&metadata_temporary).expect("unblock metadata replacement");
        let replay = FileSessionCatalogDependency
            .append_child_message(request)
            .expect("recover existing durable receipt");
        assert!(replay.replayed);
        assert_eq!(replay.sequence, 4);
        assert_eq!(replay.journal_checksum, durable_checksum);
        assert_eq!(
            read_metadata(&child_directory.join("metadata.json"))
                .expect("reconciled metadata")
                .sequence,
            4
        );
        let reopened = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: child_directory,
            })
            .expect("reopen reconciled journal");
        assert_eq!(reopened.records.len(), 4);
        assert_eq!(reopened.records[3].event_id, replay.message_id);
    }

    #[test]
    fn creates_required_tree_and_lists_without_loading_history() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let adapter = FileSessionCatalogDependency;
        let (style_binding_json, style_manifest_json, compiled_style_json) = style_documents();
        let created = adapter
            .create_session(DependencyCreateSessionRequest {
                sessions_root: root.path().join("sessions"),
                prepared: prepared(workspace.path()),
                style: String::from("persistent-chat"),
                style_binding_json,
                style_manifest_json,
                compiled_style_json,
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
                mcp_configuration: None,
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
    fn session_mcp_bootstrap_is_encrypted_bound_and_tamper_closed() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let prepared = prepared(workspace.path());
        let secret = "never persist this MCP token in plaintext";
        let (binding, manifest, compiled, configuration) =
            mcp_style_documents(prepared.session_id, secret);
        let created = FileSessionCatalogDependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: prepared.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
                mcp_configuration: Some(configuration),
            })
            .expect("MCP session");
        let encrypted_path = created.session_directory.join(MCP_BOOTSTRAP_FILE);
        let encrypted = fs::read_to_string(&encrypted_path).expect("encrypted bootstrap");
        assert!(!encrypted.contains(secret));
        assert!(
            !fs::read_to_string(created.session_directory.join("style.lock"))
                .expect("style lock")
                .contains(secret)
        );
        let loaded = load_session_mcp_bootstrap(&sessions, &prepared.session_id.to_string())
            .expect("authenticated bootstrap")
            .expect("bound servers");
        assert!(loaded.contains(secret));

        let mut tampered: serde_json::Value =
            serde_json::from_str(&encrypted).expect("encrypted JSON");
        tampered["ciphertext_base64"] = serde_json::Value::String(BASE64.encode(b"substitution"));
        fs::write(
            encrypted_path,
            serde_json::to_vec(&tampered).expect("tampered JSON"),
        )
        .expect("tamper fixture");
        assert!(matches!(
            load_session_mcp_bootstrap(&sessions, &prepared.session_id.to_string()),
            Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
        ));
    }

    #[tokio::test]
    async fn mcp_tool_dispatch_classifies_tampered_bootstrap_as_invalid_configuration() {
        use crate::tool::{
            DependencyToolCommand, ProcessToolHostDependency, ToolHostDependencyConfig,
            ToolHostDependencyError, ToolHostDependencyPort, ToolHostKind,
        };

        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let prepared = prepared(workspace.path());
        let (binding, manifest, compiled, configuration) =
            mcp_style_documents(prepared.session_id, "dispatch-only MCP secret");
        let created = FileSessionCatalogDependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: prepared.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
                mcp_configuration: Some(configuration),
            })
            .expect("MCP session");
        let encrypted_path = created.session_directory.join(MCP_BOOTSTRAP_FILE);
        let mut tampered: serde_json::Value =
            serde_json::from_slice(&fs::read(&encrypted_path).expect("encrypted bootstrap"))
                .expect("encrypted JSON");
        tampered["ciphertext_base64"] = serde_json::Value::String(BASE64.encode(b"substitution"));
        fs::write(
            encrypted_path,
            serde_json::to_vec(&tampered).expect("tampered JSON"),
        )
        .expect("tamper fixture");
        let dependency = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Mcp,
            program: workspace
                .path()
                .join("must-not-be-invoked")
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: String::from("runtime-test"),
            state_root: Some(sessions),
            maximum_frame_bytes: 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(1),
            authorization_key: [7; 32],
        })
        .expect("MCP dependency");

        let error = dependency
            .execute(DependencyToolCommand {
                execution_id: String::from("execution-1"),
                receipt_only: false,
                session_id: prepared.session_id.to_string(),
                workspace: workspace.path().to_owned(),
                call_id: String::from("call-1"),
                tool: String::from("mcp.invoke"),
                arguments: serde_json::json!({
                    "server_id": "fixture",
                    "kind": "tool",
                    "name": "echo",
                    "arguments": {"value":"hello"}
                }),
                cancellation_id: Uuid::from_u128(99).to_string(),
                workspace_authorization: None,
            })
            .await
            .expect_err("tampered bootstrap must fail before process spawn");

        assert_eq!(error, ToolHostDependencyError::InvalidConfiguration);
    }

    #[test]
    fn branch_mcp_bootstrap_is_authenticated_and_reencrypted_for_child_identity() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let parent = prepared_with(workspace.path(), 400);
        let child = prepared_with(workspace.path(), 410);
        let secret = "branch-only MCP secret";
        let (binding, manifest, compiled, configuration) =
            mcp_style_documents(parent.session_id, secret);
        let parent_created = FileSessionCatalogDependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: parent.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding.clone(),
                style_manifest_json: manifest.clone(),
                compiled_style_json: compiled.clone(),
                initial_event_json: br#"{"fixture":"parent"}"#.to_vec(),
                mcp_configuration: Some(configuration),
            })
            .expect("parent MCP session");
        let parent_encrypted = fs::read(parent_created.session_directory.join(MCP_BOOTSTRAP_FILE))
            .expect("parent encrypted bootstrap");
        let branch = FileSessionCatalogDependency
            .create_branch(DependencyCreateBranchRequest {
                sessions_root: sessions.clone(),
                prepared: child.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                parent_session_id: parent.session_id.to_string(),
                fork_sequence: 1,
                mcp_bootstrap: DependencyBranchMcpBootstrap::InheritExact {
                    source_session_id: parent.session_id.to_string(),
                },
                events: vec![
                    DependencyBranchEvent {
                        sequence: 1,
                        event_id: child.event_id.to_string(),
                        event_json: br#"{"fixture":"branch-created"}"#.to_vec(),
                    },
                    DependencyBranchEvent {
                        sequence: 2,
                        event_id: Uuid::from_u128(420).to_string(),
                        event_json: br#"{"fixture":"branch-linked"}"#.to_vec(),
                    },
                ],
                artifacts: Vec::new(),
            })
            .expect("branch with inherited MCP bootstrap");
        let child_encrypted = fs::read(branch.session_directory.join(MCP_BOOTSTRAP_FILE))
            .expect("child encrypted bootstrap");
        assert_ne!(
            parent_encrypted, child_encrypted,
            "fresh nonce/AAD required"
        );
        assert!(!String::from_utf8_lossy(&child_encrypted).contains(secret));
        assert!(
            !fs::read_to_string(branch.session_directory.join("style.lock"))
                .expect("child style lock")
                .contains(secret)
        );
        let loaded = load_session_mcp_bootstrap(&sessions, &child.session_id.to_string())
            .expect("authenticated child bootstrap")
            .expect("inherited servers");
        assert!(loaded.contains(secret));
    }

    #[test]
    fn child_mcp_bootstrap_is_authenticated_and_reencrypted_before_atomic_create() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let parent = prepared_with(workspace.path(), 430);
        let child = prepared_with(workspace.path(), 440);
        let secret = "child-only MCP secret";
        let (binding, manifest, compiled, configuration) =
            mcp_style_documents(parent.session_id, secret);
        let parent_created = FileSessionCatalogDependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: parent.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding.clone(),
                style_manifest_json: manifest.clone(),
                compiled_style_json: compiled.clone(),
                initial_event_json: br#"{"fixture":"parent"}"#.to_vec(),
                mcp_configuration: Some(configuration),
            })
            .expect("parent MCP session");
        let parent_encrypted = fs::read(parent_created.session_directory.join(MCP_BOOTSTRAP_FILE))
            .expect("parent encrypted bootstrap");
        let created = FileSessionCatalogDependency
            .create_child_session(DependencyCreateChildSessionRequest {
                sessions_root: sessions.clone(),
                prepared: child.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding,
                style_manifest_json: manifest,
                compiled_style_json: compiled,
                parent_session_id: parent.session_id.to_string(),
                parent_action_sequence: 17,
                parent_graph_node_id: String::from("spawn-workers"),
                task_id: String::from("task-1"),
                mcp_bootstrap: DependencyBranchMcpBootstrap::InheritExact {
                    source_session_id: parent.session_id.to_string(),
                },
                events: (0_u128..3)
                    .map(|index| DependencyBranchEvent {
                        sequence: u64::try_from(index + 1).expect("sequence"),
                        event_id: Uuid::from_u128(450 + index).to_string(),
                        event_json: format!(r#"{{"fixture":{index}}}"#).into_bytes(),
                    })
                    .collect(),
            })
            .expect("child with inherited MCP bootstrap");
        let child_encrypted = fs::read(created.session_directory.join(MCP_BOOTSTRAP_FILE))
            .expect("child encrypted bootstrap");
        assert_ne!(
            parent_encrypted, child_encrypted,
            "fresh nonce/AAD required"
        );
        assert!(!String::from_utf8_lossy(&child_encrypted).contains(secret));
        let loaded = load_session_mcp_bootstrap(&sessions, &child.session_id.to_string())
            .expect("authenticated child bootstrap")
            .expect("inherited servers");
        assert!(loaded.contains(secret));
    }

    #[test]
    fn branch_mcp_bootstrap_fails_closed_for_none_missing_or_substituted_key() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let parent = prepared_with(workspace.path(), 500);
        let (binding, manifest, compiled, configuration) =
            mcp_style_documents(parent.session_id, "fail-closed MCP secret");
        let parent_created = FileSessionCatalogDependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: parent.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding.clone(),
                style_manifest_json: manifest.clone(),
                compiled_style_json: compiled.clone(),
                initial_event_json: br#"{"fixture":"parent"}"#.to_vec(),
                mcp_configuration: Some(configuration),
            })
            .expect("parent MCP session");
        let request = |child: DependencyPreparedSession,
                       disposition: DependencyBranchMcpBootstrap| {
            DependencyCreateBranchRequest {
                sessions_root: sessions.clone(),
                prepared: child.clone(),
                style: String::from("persistent-chat"),
                style_binding_json: binding.clone(),
                style_manifest_json: manifest.clone(),
                compiled_style_json: compiled.clone(),
                parent_session_id: parent.session_id.to_string(),
                fork_sequence: 1,
                mcp_bootstrap: disposition,
                events: vec![
                    DependencyBranchEvent {
                        sequence: 1,
                        event_id: child.event_id.to_string(),
                        event_json: br#"{"fixture":"created"}"#.to_vec(),
                    },
                    DependencyBranchEvent {
                        sequence: 2,
                        event_id: Uuid::from_u128(
                            child.session_id.into_uuid().as_u128().saturating_add(9),
                        )
                        .to_string(),
                        event_json: br#"{"fixture":"linked"}"#.to_vec(),
                    },
                ],
                artifacts: Vec::new(),
            }
        };
        assert!(matches!(
            FileSessionCatalogDependency.create_branch(request(
                prepared_with(workspace.path(), 510),
                DependencyBranchMcpBootstrap::None,
            )),
            Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
        ));

        let key_path = sessions.join(MCP_KEY_FILE);
        let key = fs::read(&key_path).expect("MCP key");
        fs::write(&key_path, [0_u8; MCP_KEY_BYTES]).expect("substitute key");
        assert!(matches!(
            FileSessionCatalogDependency.create_branch(request(
                prepared_with(workspace.path(), 520),
                DependencyBranchMcpBootstrap::InheritExact {
                    source_session_id: parent.session_id.to_string(),
                },
            )),
            Err(SessionCatalogDependencyError::InvalidMcpConfiguration)
        ));
        fs::write(&key_path, key).expect("restore key");
        fs::remove_file(parent_created.session_directory.join(MCP_BOOTSTRAP_FILE))
            .expect("remove parent bootstrap");
        assert!(
            FileSessionCatalogDependency
                .create_branch(request(
                    prepared_with(workspace.path(), 530),
                    DependencyBranchMcpBootstrap::InheritExact {
                        source_session_id: parent.session_id.to_string(),
                    },
                ))
                .is_err()
        );
    }

    #[test]
    fn listing_uses_verified_journal_tail_instead_of_stale_metadata_hint() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let adapter = FileSessionCatalogDependency;
        let (style_binding_json, style_manifest_json, compiled_style_json) = style_documents();
        let created = adapter
            .create_session(DependencyCreateSessionRequest {
                sessions_root: sessions.clone(),
                prepared: prepared(workspace.path()),
                style: String::from("persistent-chat"),
                style_binding_json,
                style_manifest_json,
                compiled_style_json,
                initial_event_json: br#"{"fixture":true}"#.to_vec(),
                mcp_configuration: None,
            })
            .expect("create");
        JsonlJournalDependency
            .append(DependencyAppendJournalRequest {
                session_directory: created.session_directory,
                sequence: 2,
                expected_head_event_id: None,
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
        let (style_binding_json, style_manifest_json, compiled_style_json) = style_documents();
        let request = DependencyCreateBranchRequest {
            sessions_root: root.path().join("sessions"),
            prepared: prepared(workspace.path()),
            style: String::from("persistent-chat"),
            style_binding_json,
            style_manifest_json,
            compiled_style_json,
            parent_session_id: Uuid::from_u128(8).to_string(),
            fork_sequence: 7,
            mcp_bootstrap: DependencyBranchMcpBootstrap::None,
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

    #[test]
    fn child_session_is_distinct_from_branch_and_catalogued_by_parent_proposal() {
        let root = tempfile::tempdir().expect("root");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions = root.path().join("sessions");
        let (style_binding_json, style_manifest_json, compiled_style_json) = style_documents();
        let request = DependencyCreateChildSessionRequest {
            sessions_root: sessions.clone(),
            prepared: prepared(workspace.path()),
            style: String::from("persistent-chat"),
            style_binding_json,
            style_manifest_json,
            compiled_style_json,
            parent_session_id: Uuid::from_u128(8).to_string(),
            parent_action_sequence: 17,
            parent_graph_node_id: String::from("spawn-workers"),
            task_id: String::from("task-1"),
            mcp_bootstrap: DependencyBranchMcpBootstrap::None,
            events: vec![
                DependencyBranchEvent {
                    sequence: 1,
                    event_id: Uuid::from_u128(10).to_string(),
                    event_json: br#"{"event":"session.created"}"#.to_vec(),
                },
                DependencyBranchEvent {
                    sequence: 2,
                    event_id: Uuid::from_u128(11).to_string(),
                    event_json: br#"{"event":"child_session.linked"}"#.to_vec(),
                },
                DependencyBranchEvent {
                    sequence: 3,
                    event_id: Uuid::from_u128(12).to_string(),
                    event_json: br#"{"event":"child_session.workspace_lease_bound"}"#.to_vec(),
                },
            ],
        };

        FileSessionCatalogDependency
            .create_child_session(request.clone())
            .expect("child");
        let listed = FileSessionCatalogDependency
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: sessions,
                limit: 10,
            })
            .expect("list");

        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].child_parent_session_id.as_deref(),
            Some(request.parent_session_id.as_str())
        );
        assert_eq!(listed[0].child_parent_action_sequence, Some(17));
        assert_eq!(listed[0].child_task_id.as_deref(), Some("task-1"));
        assert!(listed[0].parent_session_id.is_none());
        assert!(listed[0].fork_sequence.is_none());

        let mut invalid = request;
        invalid.prepared.session_id = SessionId::from_uuid(Uuid::from_u128(13));
        invalid.events[1].sequence = 3;
        assert_eq!(
            FileSessionCatalogDependency.create_child_session(invalid),
            Err(SessionCatalogDependencyError::InvalidChildSession)
        );
    }
}
