//! Business-facing session catalog datasets.

use std::{path::PathBuf, str::FromStr};

use agentmod_primitives::{CausationId, CorrelationId, EventId, SessionId, TimestampMillis};
use agentmod_runtime_dependency::registry::{
    DependencyBranchArtifact, DependencyBranchEvent, DependencyCreateBranchRequest,
    DependencyCreateChildSessionRequest, DependencyCreateSessionRequest,
    DependencyListSessionsRequest, DependencyPrepareSessionRequest, DependencyPreparedSession,
    FileSessionCatalogDependency, SessionCatalogDependencyError, SessionCatalogDependencyPort,
};
use thiserror::Error;

/// Data request to normalize a workspace and allocate stable primitives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareSessionDataRequest {
    /// Workspace selected by logic.
    pub workspace: PathBuf,
}

/// Prepared data record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSessionDataRecord {
    /// Session identifier.
    pub session_id: SessionId,
    /// Creation event identifier.
    pub event_id: EventId,
    /// Correlation identifier.
    pub correlation_id: CorrelationId,
    /// Causation identifier.
    pub causation_id: CausationId,
    /// External clock value.
    pub timestamp: TimestampMillis,
    /// Dependency-normalized workspace.
    pub normalized_workspace: PathBuf,
}

/// Data request to atomically persist a new session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionDataRequest {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Prepared record.
    pub prepared: PreparedSessionDataRecord,
    /// Explicit style.
    pub style: String,
    /// Canonical logic-owned immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Canonical initial event bytes.
    pub initial_event_json: Vec<u8>,
}

/// Created data record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedSessionDataRecord {
    /// Durable directory.
    pub session_directory: PathBuf,
}

/// One sealed child event for atomic branch creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchEventDataRecord {
    /// Strict child sequence.
    pub sequence: u64,
    /// Fresh child event identifier.
    pub event_id: String,
    /// Sealed canonical envelope bytes.
    pub event_json: Vec<u8>,
}

/// One immutable child-session artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchArtifactDataRecord {
    /// Logic-allocated opaque artifact identity.
    pub artifact_id: String,
    /// Exact content hash.
    pub content_hash: String,
    /// Stable media type.
    pub mime_type: String,
    /// Canonical event which first references the artifact.
    pub creation_event: String,
    /// Complete bounded bytes.
    pub bytes: Vec<u8>,
}

/// Data request for atomic branch persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateBranchDataRequest {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Prepared child identity and workspace.
    pub prepared: PreparedSessionDataRecord,
    /// Explicit child style.
    pub style: String,
    /// Canonical logic-owned immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Immutable parent identifier.
    pub parent_session_id: SessionId,
    /// Inclusive source fork point.
    pub fork_sequence: u64,
    /// Complete child journal.
    pub events: Vec<BranchEventDataRecord>,
    /// Immutable artifacts atomically staged with the child.
    pub artifacts: Vec<BranchArtifactDataRecord>,
}

/// Data request for atomic runtime-managed worker creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateChildSessionDataRequest {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Prepared child identity and normalized workspace.
    pub prepared: PreparedSessionDataRecord,
    /// Explicit child style.
    pub style: String,
    /// Canonical immutable binding JSON.
    pub style_binding_json: String,
    /// Canonical selected manifest JSON.
    pub style_manifest_json: String,
    /// Canonical compiled descriptor JSON.
    pub compiled_style_json: String,
    /// Runtime-managed parent session.
    pub parent_session_id: SessionId,
    /// Canonical parent proposal sequence.
    pub parent_action_sequence: u64,
    /// Parent graph node that owns the child.
    pub parent_graph_node_id: String,
    /// Runtime-owned task identity.
    pub task_id: String,
    /// Complete sealed child journal.
    pub events: Vec<BranchEventDataRecord>,
}

/// Listing request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListSessionsDataRequest {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Strict result bound.
    pub limit: usize,
}

/// Lightweight session catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSummaryDataRecord {
    /// Session identifier.
    pub id: SessionId,
    /// Workspace display string.
    pub workspace: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence.
    pub sequence: u64,
    /// Lifecycle label.
    pub state: String,
    /// Creation time for ordering.
    pub created_at_millis: i64,
    /// Parent session for a branch.
    pub parent_session_id: Option<SessionId>,
    /// Inclusive parent sequence used to create a branch.
    pub fork_sequence: Option<u64>,
    /// Parent session for a runtime-managed worker.
    pub child_parent_session_id: Option<SessionId>,
    /// Parent proposal sequence used to reconcile a worker.
    pub child_parent_action_sequence: Option<u64>,
    /// Runtime-owned child task.
    pub child_task_id: Option<String>,
}

/// Narrow session catalog data interface.
pub trait SessionRegistryDataPort {
    /// Prepares a session using the selected dependency.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryDataError`] when normalization or allocation fails.
    fn prepare(
        &self,
        request: PrepareSessionDataRequest,
    ) -> Result<PreparedSessionDataRecord, SessionRegistryDataError>;

    /// Atomically persists the initial session dataset.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryDataError`] when the external catalog cannot
    /// atomically create the session.
    fn create(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError>;

    /// Atomically persists a replay-derived branch.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryDataError`] when dependency mapping or atomic
    /// persistence fails.
    fn create_branch(
        &self,
        request: CreateBranchDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError>;

    /// Atomically persists a runtime-managed worker session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryDataError`] when dependency mapping or
    /// persistence fails.
    fn create_child_session(
        &self,
        request: CreateChildSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError>;

    /// Lists lightweight records without loading conversations.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryDataError`] for dependency failures or invalid
    /// normalized records.
    fn list(
        &self,
        request: ListSessionsDataRequest,
    ) -> Result<Vec<SessionSummaryDataRecord>, SessionRegistryDataError>;
}

/// Session registry router over one dependency implementation.
#[derive(Clone, Debug)]
pub struct SessionRegistryData<D> {
    dependency: D,
}

impl<D> SessionRegistryData<D> {
    /// Creates a registry data router.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D> SessionRegistryDataPort for SessionRegistryData<D>
where
    D: SessionCatalogDependencyPort,
{
    fn prepare(
        &self,
        request: PrepareSessionDataRequest,
    ) -> Result<PreparedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .prepare_session(DependencyPrepareSessionRequest {
                workspace: request.workspace,
            })
            .map(from_dependency_prepared)
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: request.sessions_root,
                prepared: to_dependency_prepared(request.prepared),
                style: request.style,
                style_binding_json: request.style_binding_json,
                style_manifest_json: request.style_manifest_json,
                compiled_style_json: request.compiled_style_json,
                initial_event_json: request.initial_event_json,
            })
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create_branch(
        &self,
        request: CreateBranchDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_branch(to_dependency_branch(request))
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create_child_session(
        &self,
        request: CreateChildSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_child_session(to_dependency_child(request))
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn list(
        &self,
        request: ListSessionsDataRequest,
    ) -> Result<Vec<SessionSummaryDataRecord>, SessionRegistryDataError> {
        self.dependency
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: request.sessions_root,
                limit: request.limit,
            })
            .map_err(SessionRegistryDataError::Dependency)?
            .into_iter()
            .map(|record| {
                let id = SessionId::from_str(&record.session_id)
                    .map_err(|_| SessionRegistryDataError::InvalidSessionId)?;
                if record.sequence == 0 {
                    return Err(SessionRegistryDataError::InvalidSequence);
                }
                Ok(SessionSummaryDataRecord {
                    id,
                    workspace: record.workspace,
                    style: record.style,
                    sequence: record.sequence,
                    state: record.state,
                    created_at_millis: record.created_at_millis,
                    parent_session_id: record
                        .parent_session_id
                        .as_deref()
                        .map(SessionId::from_str)
                        .transpose()
                        .map_err(|_| SessionRegistryDataError::InvalidSessionId)?,
                    fork_sequence: record.fork_sequence,
                    child_parent_session_id: record
                        .child_parent_session_id
                        .as_deref()
                        .map(SessionId::from_str)
                        .transpose()
                        .map_err(|_| SessionRegistryDataError::InvalidSessionId)?,
                    child_parent_action_sequence: record.child_parent_action_sequence,
                    child_task_id: record.child_task_id,
                })
            })
            .collect()
    }
}

impl<D> SessionRegistryDataPort for super::RuntimeData<D>
where
    D: SessionCatalogDependencyPort,
{
    fn prepare(
        &self,
        request: PrepareSessionDataRequest,
    ) -> Result<PreparedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .prepare_session(DependencyPrepareSessionRequest {
                workspace: request.workspace,
            })
            .map(from_dependency_prepared)
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_session(DependencyCreateSessionRequest {
                sessions_root: request.sessions_root,
                prepared: to_dependency_prepared(request.prepared),
                style: request.style,
                style_binding_json: request.style_binding_json,
                style_manifest_json: request.style_manifest_json,
                compiled_style_json: request.compiled_style_json,
                initial_event_json: request.initial_event_json,
            })
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create_branch(
        &self,
        request: CreateBranchDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_branch(to_dependency_branch(request))
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn create_child_session(
        &self,
        request: CreateChildSessionDataRequest,
    ) -> Result<CreatedSessionDataRecord, SessionRegistryDataError> {
        self.dependency
            .create_child_session(to_dependency_child(request))
            .map(|created| CreatedSessionDataRecord {
                session_directory: created.session_directory,
            })
            .map_err(SessionRegistryDataError::Dependency)
    }

    fn list(
        &self,
        request: ListSessionsDataRequest,
    ) -> Result<Vec<SessionSummaryDataRecord>, SessionRegistryDataError> {
        self.dependency
            .list_sessions(DependencyListSessionsRequest {
                sessions_root: request.sessions_root,
                limit: request.limit,
            })
            .map_err(SessionRegistryDataError::Dependency)?
            .into_iter()
            .map(|record| {
                let id = SessionId::from_str(&record.session_id)
                    .map_err(|_| SessionRegistryDataError::InvalidSessionId)?;
                if record.sequence == 0 {
                    return Err(SessionRegistryDataError::InvalidSequence);
                }
                Ok(SessionSummaryDataRecord {
                    id,
                    workspace: record.workspace,
                    style: record.style,
                    sequence: record.sequence,
                    state: record.state,
                    created_at_millis: record.created_at_millis,
                    parent_session_id: record
                        .parent_session_id
                        .as_deref()
                        .map(SessionId::from_str)
                        .transpose()
                        .map_err(|_| SessionRegistryDataError::InvalidSessionId)?,
                    fork_sequence: record.fork_sequence,
                    child_parent_session_id: record
                        .child_parent_session_id
                        .as_deref()
                        .map(SessionId::from_str)
                        .transpose()
                        .map_err(|_| SessionRegistryDataError::InvalidSessionId)?,
                    child_parent_action_sequence: record.child_parent_action_sequence,
                    child_task_id: record.child_task_id,
                })
            })
            .collect()
    }
}

fn from_dependency_prepared(value: DependencyPreparedSession) -> PreparedSessionDataRecord {
    PreparedSessionDataRecord {
        session_id: value.session_id,
        event_id: value.event_id,
        correlation_id: value.correlation_id,
        causation_id: value.causation_id,
        timestamp: value.timestamp,
        normalized_workspace: value.normalized_workspace,
    }
}

fn to_dependency_branch(request: CreateBranchDataRequest) -> DependencyCreateBranchRequest {
    DependencyCreateBranchRequest {
        sessions_root: request.sessions_root,
        prepared: to_dependency_prepared(request.prepared),
        style: request.style,
        style_binding_json: request.style_binding_json,
        style_manifest_json: request.style_manifest_json,
        compiled_style_json: request.compiled_style_json,
        parent_session_id: request.parent_session_id.to_string(),
        fork_sequence: request.fork_sequence,
        events: request
            .events
            .into_iter()
            .map(|event| DependencyBranchEvent {
                sequence: event.sequence,
                event_id: event.event_id,
                event_json: event.event_json,
            })
            .collect(),
        artifacts: request
            .artifacts
            .into_iter()
            .map(|artifact| DependencyBranchArtifact {
                artifact_id: artifact.artifact_id,
                content_hash: artifact.content_hash,
                mime_type: artifact.mime_type,
                creation_event: artifact.creation_event,
                bytes: artifact.bytes,
            })
            .collect(),
    }
}

fn to_dependency_child(
    request: CreateChildSessionDataRequest,
) -> DependencyCreateChildSessionRequest {
    DependencyCreateChildSessionRequest {
        sessions_root: request.sessions_root,
        prepared: to_dependency_prepared(request.prepared),
        style: request.style,
        style_binding_json: request.style_binding_json,
        style_manifest_json: request.style_manifest_json,
        compiled_style_json: request.compiled_style_json,
        parent_session_id: request.parent_session_id.to_string(),
        parent_action_sequence: request.parent_action_sequence,
        parent_graph_node_id: request.parent_graph_node_id,
        task_id: request.task_id,
        events: request
            .events
            .into_iter()
            .map(|event| DependencyBranchEvent {
                sequence: event.sequence,
                event_id: event.event_id,
                event_json: event.event_json,
            })
            .collect(),
    }
}

fn to_dependency_prepared(value: PreparedSessionDataRecord) -> DependencyPreparedSession {
    DependencyPreparedSession {
        session_id: value.session_id,
        event_id: value.event_id,
        correlation_id: value.correlation_id,
        causation_id: value.causation_id,
        timestamp: value.timestamp,
        normalized_workspace: value.normalized_workspace,
    }
}

/// Data-layer session registry failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionRegistryDataError {
    /// External catalog adapter failed.
    #[error("session catalog dependency failed: {0}")]
    Dependency(SessionCatalogDependencyError),
    /// Dependency returned an invalid identifier.
    #[error("session catalog returned an invalid session identifier")]
    InvalidSessionId,
    /// Dependency returned sequence zero.
    #[error("session catalog returned an invalid sequence")]
    InvalidSequence,
}

/// Constructs the first-party file-backed session data router.
#[must_use]
pub fn file_session_registry() -> SessionRegistryData<FileSessionCatalogDependency> {
    SessionRegistryData::new(FileSessionCatalogDependency)
}
