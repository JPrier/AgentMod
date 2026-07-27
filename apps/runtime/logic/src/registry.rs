//! Session creation and dormant-catalog business use cases.

use std::path::PathBuf;

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{Sequence, SessionId, Version};
use agentmod_runtime_data::registry::{
    CreateSessionDataRequest, ListSessionsDataRequest, PrepareSessionDataRequest,
    PreparedSessionDataRecord, SessionRegistryDataError, SessionRegistryDataPort,
};
use thiserror::Error;

use crate::session::{RuntimeCommittedEvent, SessionCreatedEvent};

const MAX_SESSION_LIST: usize = 1_000;
const MAX_STYLE_LENGTH: usize = 128;

/// Logic-owned create command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionCommand {
    /// Sessions root selected by bootstrap configuration.
    pub sessions_root: PathBuf,
    /// User-selected workspace path.
    pub workspace: PathBuf,
    /// Explicit top-level style ID.
    pub style: String,
}

/// Logic-owned create result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionResult {
    /// New session identifier.
    pub session_id: SessionId,
    /// Durable directory.
    pub session_directory: PathBuf,
}

/// Logic-owned list command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListSessionsCommand {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Caller-requested maximum.
    pub limit: usize,
}

/// Logic-owned summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSummaryResult {
    /// Session ID.
    pub id: SessionId,
    /// Workspace display label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence.
    pub sequence: Sequence,
    /// Lifecycle label.
    pub state: String,
}

/// Narrow session registry use-case interface.
pub trait SessionRegistryLogicPort {
    /// Creates a complete initial durable session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryLogicError`] for invalid business input or
    /// failed durable creation.
    fn create_session(
        &self,
        command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, SessionRegistryLogicError>;

    /// Lists dormant metadata without loading conversations.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryLogicError`] for invalid configuration or data.
    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, SessionRegistryLogicError>;
}

impl<D> SessionRegistryLogicPort for super::RuntimeLogic<D>
where
    D: SessionRegistryDataPort,
{
    fn create_session(
        &self,
        command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, SessionRegistryLogicError> {
        validate_create(&command)?;
        let prepared = self
            .data
            .prepare(PrepareSessionDataRequest {
                workspace: command.workspace,
            })
            .map_err(SessionRegistryLogicError::Data)?;
        let event = initial_event(&prepared, &command.style)?;
        let event_json = serde_json::to_vec(&event)
            .map_err(|_| SessionRegistryLogicError::InitialEventSerialization)?;
        let session_id = prepared.session_id;
        let created = self
            .data
            .create(CreateSessionDataRequest {
                sessions_root: command.sessions_root,
                prepared,
                style: command.style,
                initial_event_json: event_json,
            })
            .map_err(SessionRegistryLogicError::Data)?;
        Ok(CreateSessionResult {
            session_id,
            session_directory: created.session_directory,
        })
    }

    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, SessionRegistryLogicError> {
        if command.sessions_root.as_os_str().is_empty() {
            return Err(SessionRegistryLogicError::InvalidSessionsRoot);
        }
        let limit = command.limit.min(MAX_SESSION_LIST);
        self.data
            .list(ListSessionsDataRequest {
                sessions_root: command.sessions_root,
                limit,
            })
            .map_err(SessionRegistryLogicError::Data)?
            .into_iter()
            .map(|record| {
                Ok(SessionSummaryResult {
                    id: record.id,
                    workspace_label: record.workspace,
                    style: record.style,
                    sequence: Sequence::new(record.sequence)
                        .map_err(|_| SessionRegistryLogicError::InvalidSequence)?,
                    state: record.state,
                })
            })
            .collect()
    }
}

fn validate_create(command: &CreateSessionCommand) -> Result<(), SessionRegistryLogicError> {
    if command.sessions_root.as_os_str().is_empty() {
        return Err(SessionRegistryLogicError::InvalidSessionsRoot);
    }
    if command.workspace.as_os_str().is_empty() {
        return Err(SessionRegistryLogicError::InvalidWorkspace);
    }
    if command.style.is_empty()
        || command.style.len() > MAX_STYLE_LENGTH
        || !command
            .style
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SessionRegistryLogicError::InvalidStyle);
    }
    Ok(())
}

fn initial_event(
    prepared: &PreparedSessionDataRecord,
    style: &str,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, SessionRegistryLogicError> {
    EventEnvelope::seal(
        EventMetadata {
            event_id: prepared.event_id,
            scope: EventScope::Session(prepared.session_id),
            sequence: Sequence::FIRST,
            timestamp: prepared.timestamp,
            event_type: String::from("session.created"),
            event_version: Version::new(1, 0),
            correlation_id: prepared.correlation_id,
            causation_id: prepared.causation_id,
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: vec![],
            classification: EventClassification::Committed,
        },
        RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
            workspace: prepared.normalized_workspace.to_string_lossy().into_owned(),
            style: style.to_owned(),
        }),
    )
    .map_err(|_| SessionRegistryLogicError::InitialEventSerialization)
}

/// Session registry business failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionRegistryLogicError {
    /// Configured storage root is empty.
    #[error("sessions root is invalid")]
    InvalidSessionsRoot,
    /// Workspace selection is empty.
    #[error("workspace selection is invalid")]
    InvalidWorkspace,
    /// Style ID has unsafe syntax or length.
    #[error("session style identifier is invalid")]
    InvalidStyle,
    /// Data operation failed.
    #[error("session registry data failed: {0}")]
    Data(SessionRegistryDataError),
    /// Initial event could not be serialized.
    #[error("initial session event could not be serialized")]
    InitialEventSerialization,
    /// Data returned sequence zero.
    #[error("session registry returned an invalid sequence")]
    InvalidSequence,
}
